// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp

import (
	"net"
	"syscall"
)

const (
	IP_DONTFRAG   = 28 // in bsd/netinet/in.h as of xnu 7195.50.7.100.1
	IPV6_DONTFRAG = 62 // in bsd/netinet6/in6.h
)

func systemSetupUDPSocket(sm *socketManager) error {
	option := IP_DONTFRAG
	level := syscall.IPPROTO_IP
	socket := sm.udpSocket
	if socket.LocalAddr().(*net.UDPAddr).IP.To4() == nil {
		option = IPV6_DONTFRAG
		level = syscall.IPPROTO_IPV6
	}
	sc, err := socket.SyscallConn()
	if err != nil {
		return err
	}
	callErr := sc.Control(func(fd uintptr) {
		err = syscall.SetsockoptInt(int(fd), level, option, 1)
	})
	if callErr != nil {
		return callErr
	}
	if err != nil {
		// Setting DONTFRAG failed; I think Mac OSes older than 11.3 Big Sur
		// do not support the IPv4 IP_DONTFRAG (but I haven't tested this). We
		// might lose some performance due to IP fragmentation, but we can
		// carry on.
		sm.logger.Info("could not set DONTFRAG option on UDP socket",
			"error", err.Error())
	}
	return nil
}

func processUDPErrorQueue(sm *socketManager) {
}
