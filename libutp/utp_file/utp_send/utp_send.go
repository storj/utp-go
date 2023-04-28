// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// This is a port of a file in the C++ libutp library as found in the Transmission app.
// Copyright (c) 2010 BitTorrent, Inc.

package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"storj.io/utp-go/libutp"
	"storj.io/utp-go/libutp/utp_file"
)

var (
	logger *zap.Logger

	debug = flag.Bool("debug", false, "Enable debug logging")
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		_, _ = fmt.Fprintf(os.Stderr, `usage: %s dest-addr file-to-send

   dest-addr: destination node to connect to, in the form <host>:<port>
   file-to-send: the file to upload

`, os.Args[0])
		os.Exit(1)
	}

	dest := args[0]
	fileName := args[1]

	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zap.InfoLevel)
	if *debug {
		logConfig.Level.SetLevel(-10)
	}
	logConfig.Encoding = "console"
	logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	var err error
	logger, err = logConfig.Build()
	if err != nil {
		panic(err)
	}

	logger.Info("connecting", zap.String("dest-addr", dest))
	logger.Info("sending", zap.String("source-file", fileName))

	dataFile, err := os.Open(fileName)
	if err != nil {
		logger.Fatal("failed to open source", zap.Error(err))
	}
	defer func() { _ = dataFile.Close() }()
	fileSize, err := dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		logger.Fatal("could not determine size of input size", zap.Error(err))
	}
	_, err = dataFile.Seek(0, io.SeekStart)
	if err != nil {
		logger.Fatal("could not seek to beginning of file", zap.Error(err))
	}
	if fileSize == 0 {
		logger.Fatal("file is 0 bytes")
	}

	sm := utp_file.NewUDPSocketManager(zapr.NewLogger(logger))

	udpSock, err := utp_file.MakeSocket(":0")
	if err != nil {
		logger.Fatal("failed to make socket", zap.Error(err))
	}
	sm.SetSocket(udpSock)

	udpAddr, err := net.ResolveUDPAddr("udp", dest)
	if err != nil {
		logger.Fatal("could not resolve destination", zap.String("dest", dest), zap.Error(err))
	}

	s, err := sm.Create(cbSendTo, sm, udpAddr)
	if err != nil {
		logger.Fatal("could not connect", zap.Stringer("dest-addr", udpAddr), zap.Error(err))
	}
	s.SetSockOpt(syscall.SO_SNDBUF, 100*300)
	logger.Sugar().Infof("creating socket %p", s)

	totalSent := 0
	done := false
	callbacks := libutp.CallbackTable{
		OnRead:     cbRead,
		OnWrite:    func(_ interface{}, data []byte) { fillBuffer(s, dataFile, data, &totalSent) },
		GetRBSize:  cbGetRBSize,
		OnState:    func(_ interface{}, state libutp.State) { done = handleStateChange(s, state, dataFile, fileSize) },
		OnError:    func(_ interface{}, err error) { handleError(s, err) },
		OnOverhead: nil,
	}
	s.SetCallbacks(&callbacks, s)

	logger.Sugar().Infof("connecting socket %p", s)
	s.Connect()

	lastSent := 0
	lastTime := time.Now()

	for !done {
		err := sm.Select(50000 * time.Microsecond)
		if err != nil {
			logger.Fatal("failed to run Select()", zap.Error(err))
		}
		sm.CheckTimeouts()
		curTime := time.Now()
		if curTime.After(lastTime.Add(time.Second)) {
			rate := float64(totalSent-lastSent) / curTime.Sub(lastTime).Seconds()
			lastSent = totalSent
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
	logger.Info("got data from peer?", zap.ByteString("data", data))
}

func cbGetRBSize(userdata interface{}) int {
	// this side doesn't have to worry about the receive window getting smaller
	return 0
}

func handleError(conn *libutp.Socket, err error) {
	logger.Error("socket error", zap.Error(err))
	if err := conn.Close(); err != nil {
		logger.Error("could not close µTP socket", zap.Error(err))
	}
}

func fillBuffer(conn *libutp.Socket, dataFile *os.File, data []byte, totalSent *int) {
	n, err := dataFile.Read(data)
	if err != nil {
		logger.Error("failed to read from datafile", zap.Error(err))
		if err := conn.Close(); err != nil {
			logger.Error("could not close µTP socket", zap.Error(err))
		}
	} else {
		*totalSent += n
	}
}

func handleStateChange(conn *libutp.Socket, state libutp.State, file io.Seeker, totalSize int64) bool {
	switch state {
	case libutp.StateConnect, libutp.StateWritable:
		curPos, _ := file.Seek(0, io.SeekCurrent)
		if conn.Write(int(totalSize - curPos)) {
			logger.Info("upload complete")
			if err := conn.Close(); err != nil {
				logger.Error("could not close socket", zap.Error(err))
			}
		}
	case libutp.StateDestroying:
		return true
	}
	return false
}
