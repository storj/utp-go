// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"storj.io/utp-go"
	"storj.io/utp-go/utp_file"
)

var (
	logger *zap.SugaredLogger

	debug = flag.Bool("debug", false, "Enable debug logging")
)

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		_, _ = fmt.Fprintf(os.Stderr, `usage: %s listen-addr file-to-send

   listen-addr: address to listen on, in the form [<host>]:<port>
   file-to-write: where to write the received file

`, os.Args[0])
		os.Exit(1)
	}

	startTime := time.Now()

	listenAddr := args[0]
	fileName := args[1]

	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zap.InfoLevel)

	if *debug {
		logConfig.Level.SetLevel(zap.DebugLevel)
	}
	logConfig.Encoding = "console"
	logConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	plainLogger, err := logConfig.Build()
	if err != nil {
		panic(err)
	}
	logger = plainLogger.Sugar()

	logger.Infof("listening on %s", listenAddr)
	logger.Infof("saving to %s", fileName)

	destFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0664)
	if err != nil {
		logger.Fatalf("could not open destination file for writing: %v", err)
	}
	defer func() {
		err := destFile.Close()
		if err != nil {
			logger.Fatalf("failed to close destination file: %v", err)
		}
	}()

	fsr, err := newFileStreamReceiver(logger, listenAddr, destFile)
	defer func() {
		if err := fsr.Close(); err != nil {
			logger.Errorf("failed to close fileStreamReceiver: %v", err)
		}
	}()

	lastRecv := 0
	lastTime := time.Now()

	for !fsr.done {
		err := fsr.sm.Select(50000 * time.Microsecond)
		if err != nil {
			logger.Fatalf("failed to run select: %v", err)
		}
		fsr.sm.CheckTimeouts()
		curTime := time.Now()
		if curTime.After(lastTime.Add(1000 * time.Millisecond)) {
			rate := float64((fsr.totalRecv-lastRecv)*1000) / float64(curTime.Sub(lastTime).Milliseconds())
			lastRecv = fsr.totalRecv
			lastTime = curTime
			fmt.Printf("\r[%d] recv: %d  %.1f bytes/s  ", curTime.Sub(startTime).Milliseconds(), fsr.totalRecv, rate)
		}
	}

	fmt.Printf("\nreceived: %d bytes\n", fsr.totalRecv)
}

type fileStreamReceiver struct {
	sm             *utp_file.UDPSocketManager
	logger         *zap.SugaredLogger
	fileDest       io.Writer
	totalRecv      int
	connectionSeen bool
	done           bool
}

func newFileStreamReceiver(logger *zap.SugaredLogger, listenAddr string, fileDest io.Writer) (*fileStreamReceiver, error) {
	sock, err := utp_file.MakeSocket(listenAddr)
	if err != nil {
		err = fmt.Errorf("could not listen on %q: %w", listenAddr, err)
		return nil, err
	}
	sm := utp_file.NewUDPSocketManager(logger)
	sm.SetSocket(sock)

	fsr := &fileStreamReceiver{
		logger:   logger,
		sm:       sm,
		fileDest: fileDest,
	}
	sm.OnIncomingConnection = fsr.receiveNewConnection

	return fsr, nil
}

func (fsr *fileStreamReceiver) receiveNewConnection(s *utp.Socket) error {
	if fsr.connectionSeen {
		return errors.New("no more connections allowed")
	}
	s.SetCallbacks(&utp.CallbackTable{
		OnRead:     fsr.doRead,
		OnWrite:    fsr.doWrite,
		GetRBSize:  fsr.doGetRBSize,
		OnState:    fsr.doStateChange,
		OnError:    fsr.handleError,
		OnOverhead: nil,
	}, s)
	fsr.connectionSeen = true
	return nil
}

func (fsr *fileStreamReceiver) doRead(_ interface{}, b []byte) {
	n, err := fsr.fileDest.Write(b)
	if err != nil {
		fsr.logger.Errorf("failed to write to destination file: %v", err)
		fsr.done = true
		return
	}
	if n < len(b) {
		fsr.logger.Errorf("could not write full packet to destination file! %d<%d", n, len(b))
		fsr.done = true
		return
	}
	fsr.totalRecv += len(b)
}

func (fsr *fileStreamReceiver) doWrite(_ interface{}, _ []byte) {
	fsr.logger.Errorf("got doWrite call on receiving side, which is not expected")
	fsr.done = true
}

func (fsr *fileStreamReceiver) doGetRBSize(_ interface{}) int {
	// always pretend the buffer is empty, since we write straight to the disk anyway
	return 0
}

func (fsr *fileStreamReceiver) doStateChange(_ interface{}, state utp.State) {
	switch state {
	case utp.StateEOF:
		fsr.logger.Debugf("entered state EOF; done with transfer")
		fsr.done = true
	case utp.StateDestroying:
		fsr.logger.Debugf("entered state Destroying; done with transfer")
		fsr.done = true
	}
}

func (fsr *fileStreamReceiver) handleError(_ interface{}, err error) {
	fsr.logger.Errorf("got socket error: %v", err)
	fsr.done = true
}

func (fsr *fileStreamReceiver) Close() error {
	return fsr.sm.Close()
}
