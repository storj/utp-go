// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp_file

import (
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"

	"storj.io/utp-go/libutp"
)

const MaxOutgoingQueueSize = 32

func MakeSocket(addr string) (*net.UDPConn, error) {
	sock, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	udpSock, ok := sock.(*net.UDPConn)
	if !ok {
		return nil, fmt.Errorf("ListenPacket returned a %T instead of *net.UDPConn", udpSock)
	}

	// Mark to hold a couple of megabytes
	const size = 2 * 1024 * 1024

	err = udpSock.SetReadBuffer(size)
	if err != nil {
		return nil, fmt.Errorf("could not set read buffer size: %w", err)
	}
	err = udpSock.SetWriteBuffer(size)
	if err != nil {
		return nil, fmt.Errorf("could not set write buffer size: %w", err)
	}
	return udpSock, nil
}

type UDPOutgoing struct {
	to  *net.UDPAddr
	mem []byte
}

type UDPSocketManager struct {
	*libutp.SocketMultiplexer
	socket               *net.UDPConn
	outQueue             []UDPOutgoing
	Logger               logr.Logger
	OnIncomingConnection func(*libutp.Socket) error
}

func NewUDPSocketManager(logger logr.Logger) *UDPSocketManager {
	return &UDPSocketManager{
		SocketMultiplexer: libutp.NewSocketMultiplexer(logger, nil),
		Logger:            logger,
	}
}

func (usm *UDPSocketManager) SetSocket(sock *net.UDPConn) {
	if usm.socket != nil && usm.socket != sock {
		if err := usm.socket.Close(); err != nil {
			usm.Logger.Error(err, "failed to close old UDP socket during SetSocket")
		}
	}
	usm.socket = sock
}

func (usm *UDPSocketManager) Flush() {
	for len(usm.outQueue) > 0 {
		uo := usm.outQueue[0]

		usm.Logger.V(1).Info("Flush->WriteTo", "contents", fmt.Sprintf("%x", uo.mem), "len", len(uo.mem))
		_, err := usm.socket.WriteToUDP(uo.mem, uo.to)
		if err != nil {
			usm.Logger.Error(err, "sendto failed")
			break
		}
	}
}

func (usm *UDPSocketManager) Select(blockTime time.Duration) error {
	socketRawConn, err := usm.socket.SyscallConn()
	if err != nil {
		return err
	}
	var selectErr error
	controlErr := socketRawConn.Control(func(socketFd uintptr) {
		selectErr = usm.performSelect(blockTime, int32(socketFd))
	})
	if controlErr != nil {
		return controlErr
	}
	return selectErr
}

func (usm *UDPSocketManager) performSelect(blockTime time.Duration, socketFd int32) error {
	timeoutTime := time.Now().Add(blockTime)
	var fds [1]unix.PollFd
	for {
		fds[0] = unix.PollFd{Fd: socketFd, Events: unix.POLLIN}
		n, err := unix.Poll(fds[:], int(time.Until(timeoutTime).Milliseconds()))
		if err != nil || n == 0 {
			if err == syscall.EINTR {
				continue
			}
			return err
		}
		break
	}
	usm.Flush()
	if fds[0].Revents&unix.POLLIN != 0 {
		var buffer [8192]byte
		for {
			receivedBytes, srcAddr, err := usm.socket.ReadFromUDP(buffer[:])
			if err != nil {
				// ECONNRESET - On a UDP-datagram socket
				// this error indicates a previous send operation
				// resulted in an ICMP Port Unreachable message.
				if err == syscall.ECONNRESET {
					// (storj): do we have a way to know which previous send
					// operation? or a way to tie it to an existing connection,
					// so we can pass the error on?
					usm.Logger.Error(nil, "got ECONNRESET from udp socket")
					continue
				}
				// EMSGSIZE - The message was too large to fit into
				// the buffer pointed to by the buf parameter and was
				// truncated.
				if err == syscall.EMSGSIZE {
					// (storj): this seems like a big huge hairy deal. the code
					// shouldn't allow this to happen, and if it does, won't
					// all subsequent traffic be potentially wrong?
					usm.Logger.Error(nil, "got EMSGSIZE from udp socket")
					continue
				}
				// any other error (such as EWOULDBLOCK) results in breaking the loop
				break
			}

			// Lookup the right UTP socket that can handle this message
			if !usm.IsIncomingUTP(gotIncomingConnection, sendTo, usm, buffer[:receivedBytes], srcAddr) {
				usm.Logger.V(1).Info("received a non-??TP packet on UDP port", "source-addr", srcAddr)
			}
			break
		}
	}

	if fds[0].Revents&unix.POLLERR != 0 {
		usm.Logger.Error(nil, "error condition on socket manager socket")
	}
	return nil
}

func sendTo(userdata interface{}, p []byte, addr *net.UDPAddr) {
	userdata.(*UDPSocketManager).Send(p, addr)
}

func (usm *UDPSocketManager) Send(p []byte, addr *net.UDPAddr) {
	if len(p) > int(libutp.GetUDPMTU(addr)) {
		panic("given packet is too big")
	}

	var err error
	if len(usm.outQueue) == 0 {
		usm.Logger.V(1).Info("Send->WriteTo", "contents", fmt.Sprintf("%x", p), "len", len(p))
		_, err = usm.socket.WriteToUDP(p, addr)
		if err != nil {
			usm.Logger.Error(err, "sendto failed")
		}
	}
	if len(usm.outQueue) > 0 || err != nil {
		// Buffer a packet.
		if len(usm.outQueue) >= MaxOutgoingQueueSize {
			usm.Logger.Error(nil, "no room to buffer outgoing packet")
		} else {
			memCopy := make([]byte, len(p))
			copy(memCopy, p)
			usm.outQueue = append(usm.outQueue, UDPOutgoing{to: addr, mem: memCopy})
			usm.Logger.Error(nil, "buffering packet: %d", len(usm.outQueue))
		}
	}
}

func (usm *UDPSocketManager) Close() error {
	err := usm.socket.Close()
	usm.socket = nil
	usm.Logger = nil
	return err
}

var NotAcceptingConnections = errors.New("not accepting connections")

func gotIncomingConnection(userdata interface{}, socket *libutp.Socket) {
	usm := userdata.(*UDPSocketManager)
	usm.Logger.V(1).Info("incoming connection received from %v", socket.GetPeerName())
	var err error
	if usm.OnIncomingConnection != nil {
		err = usm.OnIncomingConnection(socket)
	} else {
		err = NotAcceptingConnections
	}
	if err != nil {
		usm.Logger.Error(err, "rejecting connection")
		if closeErr := socket.Close(); closeErr != nil {
			usm.Logger.Error(closeErr, "could not close new socket")
		}
	}
}
