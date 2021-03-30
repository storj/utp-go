// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package utp

import "net"

// Used to set sockopt on a uTP socket to set the version of uTP
// to use for outgoing connections. This can only be called before
// the uTP socket is connected
const SO_UTPVERSION = 99

type State int

const (
	// socket has reveived syn-ack (notification only for outgoing connection completion)
	// this implies writability
	StateConnect State = 1
	// socket is able to send more data
	StateWritable = 2
	// connection closed
	StateEOF = 3
	// socket is being destroyed, meaning all data has been sent if possible.
	// it is not valid to refer to the socket after this state change occurs
	StateDestroying = 4
)

type OnReadCallback func(userdata interface{}, bytes []byte)

// The uTP socket layer calls this to fill the outgoing buffer with bytes.
// The uTP layer takes responsibility that those bytes will be delivered.
type OnWriteCallback func(userdata interface{}, bytes []byte)

// The uTP socket layer calls this to retrieve number of bytes currently in read buffer
type GetRBSizeCallback func(userdata interface{}) int

// The uTP socket layer calls this whenever the socket becomes writable.
type OnStateChangeCallback func(userdata interface{}, state State)

// The uTP socket layer calls this when an error occurs on the socket.
// These errors currently include ECONNREFUSED, ECONNRESET and ETIMEDOUT, but
// could eventually include any BSD socket error.
type OnErrorCallback func(userdata interface{}, err error)

// The uTP socket layer calls this to report overhead statistics
type OnOverheadCallback func(userdata interface{}, send bool, count int, bandwidthType BandwidthType)

type CallbackTable struct {
	OnRead     OnReadCallback
	OnWrite    OnWriteCallback
	GetRBSize  GetRBSizeCallback
	OnState    OnStateChangeCallback
	OnError    OnErrorCallback
	OnOverhead OnOverheadCallback
}

// The uTP socket layer calls this when a new incoming uTP connection is established
// this implies writability
type GotIncomingConnection func(userdata interface{}, s *Socket)

// The uTP socket layer calls this to send UDP packets
type PacketSendCallback func(userdata interface{}, p []byte, addr *net.UDPAddr)

// Statistics collected for a particular Socket
type Stats struct {
	NBytesRecv uint64 // total bytes received
	NBytesXmit uint64 // total bytes transmitted
	ReXmit     uint32 // retransmit counter
	FastReXmit uint32 // fast retransmit counter
	NXmit      uint32 // transmit counter
	NRecv      uint32 // receive counter (total)
	NDupRecv   uint32 // duplicate receive counter
}

type GlobalStats struct {
	// total packets recieved less than 300/600/1200/MTU bytes fpr all connections (global)
	NumRawRecv [5]uint32
	// total packets sent less than 300/600/1200/MTU bytes for all connections (global)
	NumRawSend [5]uint32
}
