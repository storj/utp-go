// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// This is a port of a file in the C++ libutp library as found in the Transmission app.
// Copyright (c) 2010 BitTorrent, Inc.

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
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
	var err error
	logger, err = logConfig.Build()
	if err != nil {
		panic(err)
	}

	logger.Info("listening", zap.String("address", listenAddr))
	logger.Info("saving to file", zap.String("dest-file", fileName))

	destFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0664)
	if err != nil {
		logger.Fatal("could not open destination file for writing", zap.Error(err))
	}
	defer func() {
		err := destFile.Close()
		if err != nil {
			logger.Fatal("failed to close destination file", zap.Error(err))
		}
	}()

	fsr, err := newFileStreamReceiver(logger, listenAddr, destFile)
	if err != nil {
		logger.Fatal("could not create new fileStreamReceiver", zap.Error(err))
	}
	defer func() {
		if err := fsr.Close(); err != nil {
			logger.Error("failed to close fileStreamReceiver", zap.Error(err))
		}
	}()

	lastRecv := 0
	lastTime := time.Now()

	for !fsr.done {
		err := fsr.sm.Select(50000 * time.Microsecond)
		if err != nil {
			logger.Fatal("failed to run select", zap.Error(err))
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
	logger         *zap.Logger
	fileDest       io.Writer
	totalRecv      int
	connectionSeen bool
	done           bool
}

func newFileStreamReceiver(logger *zap.Logger, listenAddr string, fileDest io.Writer) (*fileStreamReceiver, error) {
	sock, err := utp_file.MakeSocket(listenAddr)
	if err != nil {
		err = fmt.Errorf("could not listen on %q: %w", listenAddr, err)
		return nil, err
	}
	sm := utp_file.NewUDPSocketManager(zapr.NewLogger(logger))
	sm.SetSocket(sock)

	fsr := &fileStreamReceiver{
		logger:   logger,
		sm:       sm,
		fileDest: fileDest,
	}
	sm.OnIncomingConnection = fsr.receiveNewConnection

	return fsr, nil
}

func (fsr *fileStreamReceiver) receiveNewConnection(s *libutp.Socket) error {
	if fsr.connectionSeen {
		return errors.New("no more connections allowed")
	}
	s.SetCallbacks(&libutp.CallbackTable{
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
		fsr.logger.Error("failed to write to destination file", zap.Error(err))
		fsr.done = true
		return
	}
	if n < len(b) {
		fsr.logger.Error("could not write full packet to destination file!", zap.Int("written", n), zap.Int("full-len", len(b)))
		fsr.done = true
		return
	}
	fsr.totalRecv += len(b)
}

func (fsr *fileStreamReceiver) doWrite(_ interface{}, _ []byte) {
	fsr.logger.Error("got doWrite call on receiving side, which is not expected")
	fsr.done = true
}

func (fsr *fileStreamReceiver) doGetRBSize(_ interface{}) int {
	// always pretend the buffer is empty, since we write straight to the disk anyway
	return 0
}

func (fsr *fileStreamReceiver) doStateChange(_ interface{}, state libutp.State) {
	switch state {
	case libutp.StateEOF:
		fsr.logger.Debug("entered state EOF; done with transfer")
		fsr.done = true
	case libutp.StateDestroying:
		fsr.logger.Debug("entered state Destroying; done with transfer")
		fsr.done = true
	}
}

func (fsr *fileStreamReceiver) handleError(_ interface{}, err error) {
	fsr.logger.Error("got socket error", zap.Error(err))
	fsr.done = true
}

func (fsr *fileStreamReceiver) Close() error {
	return fsr.sm.Close()
}
