// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func systemSetupUDPSocket(sm *socketManager) error {
	sc, err := sm.udpSocket.SyscallConn()
	if err != nil {
		return err
	}
	callErr := sc.Control(func(fd uintptr) {
		// enable path mtu discovery, which (at least for non-SOCK_STREAM
		// sockets) forces the don't-fragment flag on for all outgoing packets.
		err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_DO)
		if err != nil {
			// not sure why this would happen, but we can carry on without it
			sm.logger.Error(err, "could not set IP_MTU_DISCOVER option on UDP socket")
		}

		// enable extended reliable error message passing; this will allow
		// us to get calculated MTU updates per host.
		err = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_RECVERR, 1)
		if err != nil {
			// again, this doesn't need to cause total failure, but we might
			// up losing a lot of performance to ip fragmentation
			sm.logger.Error(err, "could not enable error message queue with IP_RECVERR on UDP socket")
		}
	})
	if callErr != nil {
		return callErr
	}
	return nil
}

func processUDPErrorQueue(sm *socketManager) {
	sc, err := sm.udpSocket.SyscallConn()
	if err != nil {
		sm.logger.Error(err, "could not access SyscallConn interface of udp socket??")
		return
	}
	oob := make([]byte, 1024)
	callErr := sc.Control(func(fd uintptr) {
		// see documentation for IP_RECVERR in Linux's ip(7) man page.
		for {
			_, oobn, flags, sockAddr, err := syscall.Recvmsg(int(fd), nil, oob, syscall.MSG_ERRQUEUE | syscall.MSG_DONTWAIT)
			cmsgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
			if err != nil {
				sm.logger.Error(err, "could not parse socket control messages")
				return
			}
			if flags & syscall.MSG_CTRUNC != 0 {
				sm.logger.Error(nil, "Received error queue data from udp socket was truncated! Need to make that buffer bigger")
			}

			// I don't think this will be a useful bit of information, if we
			// even get anything here, but let's check and see
			var srcAddr net.Addr
			switch addr := sockAddr.(type) {
			case *syscall.SockaddrInet4:
				srcAddr = &net.UDPAddr{IP: addr.Addr[:], Port: addr.Port}
			case *syscall.SockaddrInet6:
				srcAddr = &net.UDPAddr{IP: addr.Addr[:], Port: addr.Port}
			default:
				sm.logger.Error(nil, "source address from recvmsg is of type %T: %v", srcAddr, srcAddr)
			}

			for _, cmsg := range cmsgs {
				if cmsg.Header.Level != syscall.IPPROTO_IP || cmsg.Header.Type != syscall.MSG_ERRQUEUE {
					sm.logger.Info("got unexpected out-of-band data on udp socket error queue",
						"header-type", cmsg.Header.Type, "header-level", cmsg.Header.Level,
						"data", fmt.Sprintf("%x", cmsg.Data))
					continue
				}
				errData := (*unix.SockExtendedErr)(unsafe.Pointer(&cmsg.Data[0]))
				// origin, type, and code are interesting, but not sure how best to filter. just log
				if errData.Errno != uint32(syscall.EMSGSIZE) {
					// is that the right thing to expect for mtu updates? log
					continue
				}
				offender := cmsg.Data[unsafe.Sizeof(unix.SockExtendedErr{}):] // RawSockaddr?
				newMTU := errData.Info
				sm.adjustMTUFor(offender, newMTU)
			}
		}
	})
	if callErr != nil {
		sm.logger.Error(err, "could not process UDP error queue")
	}
}
