package utp_file

import (
	"fmt"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"storj.io/go-utp"
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

func (usm *UDPSocketManager) SetSocket(sock *net.UDPConn) {
	if usm.socket != nil && usm.socket != sock {
		_ = usm.socket.Close()
	}
	usm.socket = sock
}

func (usm *UDPSocketManager) Flush() {
	for len(usm.outQueue) > 0 {
		uo := usm.outQueue[0]

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
	controlErr := socketRawConn.Control(func(socketFd uintptr) {
		pfd := unix.PollFd{Fd: int32(socketFd), Events: unix.POLLIN}
		var n int
		n, err = unix.Poll([]unix.PollFd{pfd}, int(blockTime / time.Millisecond))
		if err != nil || n == 0 {
			return
		}
		usm.Flush()
		if pfd.Revents & unix.POLLIN != 0 {
			var buffer [8192]byte
			for {
				receivedBytes, srcAddr, err := usm.socket.ReadFromUDP(buffer[:])
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
				if utp.IsIncomingUTP(usm.Logger, nil, sendTo, usm, buffer[:receivedBytes], srcAddr) {
					continue
				}
			}
		}

		if pfd.Revents & unix.POLLERR != 0 {
			usm.Logger.Errorf("error condition on socket manager socket")
		}
	})
	if controlErr != nil {
		return controlErr
	}
	return err
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
