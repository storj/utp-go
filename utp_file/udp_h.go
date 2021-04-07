package utp_file

import (
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"storj.io/utp-go"
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
	totalSent int
	count     int
	socket    *net.UDPConn
	outQueue  []UDPOutgoing
	Logger    utp.CompatibleLogger
}

func NewUDPSocketManager(logger utp.CompatibleLogger) *UDPSocketManager {
	return &UDPSocketManager{Logger: logger}
}

func (usm *UDPSocketManager) SetSocket(sock *net.UDPConn) {
	if usm.socket != nil && usm.socket != sock {
		_ = usm.socket.Close()
	}
	usm.socket = sock
}

func (usm *UDPSocketManager) Flush() {
	for len(usm.outQueue) > 0 {
		uo := usm.outQueue[0]

		usm.Logger.Debugf("Flush->WriteTo(%x) len=%d", uo.mem, len(uo.mem))
		_, err := usm.socket.WriteTo(uo.mem, uo.to)
		if err != nil {
			usm.Logger.Infof("sendto failed: %v", err)
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
		fds[0] = unix.PollFd{Fd: int32(socketFd), Events: unix.POLLIN}
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
			usm.Logger.Debugf("data available to read")
			receivedBytes, srcAddr, err := usm.socket.ReadFromUDP(buffer[:])
			usm.Logger.Debugf("got %d bytes from %v (%v)", receivedBytes, srcAddr, err)
			if err != nil {
				// ECONNRESET - On a UDP-datagram socket
				// this error indicates a previous send operation
				// resulted in an ICMP Port Unreachable message.
				if err == syscall.ECONNRESET {
					continue
				}
				// EMSGSIZE - The message was too large to fit into
				// the buffer pointed to by the buf parameter and was
				// truncated.
				if err == syscall.EMSGSIZE {
					continue
				}
				// any other error (such as EWOULDBLOCK) results in breaking the loop
				break
			}

			// Lookup the right UTP socket that can handle this message
			wasUTP := utp.IsIncomingUTP(usm.Logger, nil, sendTo, usm, buffer[:receivedBytes], srcAddr)
			if wasUTP {
				usm.Logger.Debugf("that was a utp packet")
			} else {
				usm.Logger.Debugf("that was NOT a utp packet")
			}
			break
		}
	}

	if fds[0].Revents&unix.POLLERR != 0 {
		usm.Logger.Errorf("error condition on socket manager socket")
	}
	return nil
}

func sendTo(userdata interface{}, p []byte, addr *net.UDPAddr) {
	userdata.(*UDPSocketManager).Send(p, addr)
}

func (usm *UDPSocketManager) Send(p []byte, addr *net.UDPAddr) {
	if len(p) > int(utp.GetUDPMTU(addr)) {
		panic("given packet is too big")
	}

	var err error
	if len(usm.outQueue) == 0 {
		usm.Logger.Debugf("Send->WriteTo(%x) len=%d", p, len(p))
		_, err = usm.socket.WriteTo(p, addr)
		if err != nil {
			usm.Logger.Infof("sendto failed: %v", err)
		}
	}
	if len(usm.outQueue) > 0 || err != nil {
		// Buffer a packet.
		if len(usm.outQueue) >= MaxOutgoingQueueSize {
			usm.Logger.Infof("no room to buffer outgoing packet")
		} else {
			memCopy := make([]byte, len(p))
			copy(memCopy, p)
			usm.outQueue = append(usm.outQueue, UDPOutgoing{to: addr, mem: memCopy})
			usm.Logger.Infof("buffering packet: %d", len(usm.outQueue))
		}
	}
}

func (usm *UDPSocketManager) TotalSent() int {
	return usm.totalSent
}
