package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"syscall"
	"time"

	"go.uber.org/zap/zapcore"

	"storj.io/utp-go"
	"storj.io/utp-go/utp_file"

	"go.uber.org/zap"
)

var (
	logger *zap.SugaredLogger
)

func main() {
	if len(os.Args) < 3 {
		_, _ = fmt.Fprintf(os.Stderr, `usage: %s dest-addr file-to-send

   dest-addr: destination node to connect to, in the form <host>:<port>
   file-to-send: the file to upload

`, os.Args[0])
		os.Exit(1)
	}

	dest := os.Args[1]
	fileName := os.Args[2]

	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zap.InfoLevel)
	logConfig.Encoding = "console"
	logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	plainLogger, err := logConfig.Build()
	if err != nil {
		panic(err)
	}
	logger = plainLogger.Sugar()

	logger.Infof("connecting to %s", dest)
	logger.Infof("sending %q", fileName)

	dataFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed to open source: %v", err)
	}
	defer func() { _ = dataFile.Close() }()
	fileSize, err := dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		log.Fatalf("could not determine size of input size: %v", err)
	}
	_, err = dataFile.Seek(0, io.SeekStart)
	if err != nil {
		log.Fatalf("could not seek to beginning of file: %v", err)
	}
	if fileSize == 0 {
		log.Fatalf("file is 0 bytes")
	}

	var sm utp_file.UDPSocketManager

	udpSock, err := utp_file.MakeSocket(":0")
	if err != nil {
		log.Fatalf("failed to make socket: %v", err)
	}
	sm.SetSocket(udpSock)

	udpAddr, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		log.Fatalf("could not resolve destination %q: %v", dest, err)
	}

	s := utp.Create(logger, cbSendTo, &sm, udpAddr)
	s.SetSockOpt(syscall.SO_SNDBUF, 100*300)
	logger.Infof("creating socket %p", s)

	callbacks := utp.CallbackTable{
		OnRead:     cbRead,
		OnWrite:    func(_ interface{}, data []byte) { fillBuffer(s, dataFile, data) },
		GetRBSize:  cbGetRBSize,
		OnState:    func(_ interface{}, state utp.State) { handleStateChange(s, state, dataFile, fileSize) },
		OnError:    func(_ interface{}, err error) { handleError(s, err) },
		OnOverhead: nil,
	}
	s.SetCallbacks(&callbacks, s)

	logger.Infof("connecting socket %p", s)
	s.Connect()

	lastSent := 0
	lastTime := time.Now()

	for s != nil {
		err := sm.Select(50000)
		if err != nil {
			log.Fatalf("failed to run Select(): %v", err)
		}
		utp.CheckTimeouts()
		curTime := time.Now()
		if curTime.After(lastTime.Add(time.Second)) {
			rate := float64(sm.TotalSent()-lastSent) / curTime.Sub(lastTime).Seconds()
			lastSent = sm.TotalSent()
			lastTime = curTime
			fmt.Printf("\r[%d] sent: %d/%d  %.1f bytes/s  ", curTime.Unix(), lastSent, fileSize, rate)
		}
	}
}

func cbSendTo(userdata interface{}, p []byte, addr *net.UDPAddr) {
	sm := userdata.(*utp_file.UDPSocketManager)
	sm.Send(p, addr)
}

func cbRead(userdata interface{}, data []byte) {
	logger.Infof("got data from peer? %x", data)
}

func cbGetRBSize(userdata interface{}) int {
	// this side doesn't have to worry about the receive window getting smaller
	return 0
}

func handleError(conn *utp.Socket, err error) {
	logger.Infof("socket error: %s\n", err)
	conn.Close()
}

func fillBuffer(conn *utp.Socket, dataFile *os.File, data []byte) {
	pos := 0
	for pos < len(data) {
		n, err := dataFile.Read(data[pos:])
		if err != nil {
			logger.Infof("failed to read from datafile: %v", err)
			conn.Close()
		}
		pos += n
	}
}

func handleStateChange(conn *utp.Socket, state utp.State, file io.Seeker, totalSize int64) {
	if state == utp.StateConnect || state == utp.StateWritable {
		curPos, _ := file.Seek(0, io.SeekCurrent)
		if conn.Write(int(totalSize - curPos)) {
			logger.Infof("upload complete")
			conn.Close()
		}
	} else if state == utp.StateDestroying {
		conn.Close()
	}
}
