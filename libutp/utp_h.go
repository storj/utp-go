// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package libutp

import "net"

// SO_UTPVERSION is the option name used to set the version of µTP
// to use in outgoing connections. This can only be called before
// the µTP socket is connected.
//
// Example:
//
//     conn := base.Create(sendToCallback, sendToUserdata, destAddr)
//     conn.SetSockopt(libutp.SO_UTPVERSION, 1)  // µTP version 1
//
const SO_UTPVERSION = 99

// State represents the state of a µTP socket. It is important to distinguish
// this type from connState, which is the state of a µTP _connection_. State
// conveys what a socket is prepared to do, while connState indicates a point
// in the µTP protocol state diagram. For most purposes, code using utp-go
// will only need to deal with State.
type State int

const (
	// StateConnect is the state wherein a socket has received syn-ack
	// (notification only for outgoing connection completion) (although I don't
	// know exactly what the previous parenthetical means). This state implies
	// writability.
	StateConnect State = 1
	// StateWritable is the state wherein a socket is able to send more data.
	StateWritable = 2
	// StateEOF is the state used for a socket when its connection is closed.
	StateEOF = 3
	// StateDestroying indicates that the socket is being destroyed, meaning
	// all data has been sent if possible. It is not valid to refer to the
	// socket after this state change occurs.
	StateDestroying = 4
)

func (s State) String() string {
	return stateNames[s]
}

// OnReadCallback is the type of callback to be used when a socket has received
// application data from the peer. The callback will be provided with the
// userdata parameter that was given when the callback was set (using
// (*Socket).SetCallbacks()). The callback is expected to process or buffer all
// of the data in the 'bytes' slice. Other methods on the Socket object, such
// as (*Socket).Write(), may be called while the callback is running, if
// appropriate.
type OnReadCallback func(userdata interface{}, bytes []byte)

// OnWriteCallback is the type of callback to be used when a socket is ready to
// send more application data to the peer. The application will already have
// indicated that it _wants_ to send some amount of data using the
// (*Socket).Write() method, but the data to be sent is not determined until
// this callback is made. The callback will be provided with the userdata
// parameter that was given when the callback was set (using
// (*Socket).SetCallbacks()). The callback is expected to fill in the entire
// 'bytes' slice. (It should be guaranteed that the slice will not be greater
// in size than the amount of application that is ready to be sent, as
// indicated by the last call to (*Socket).Write().)
//
// Once received, the µTP layer takes responsibility for delivering those
// bytes.
type OnWriteCallback func(userdata interface{}, bytes []byte)

// GetRBSizeCallback is the type of callback to be used when a socket needs to
// know the current size of the read buffer (how many bytes have been received
// by OnReadCallback, but have not yet been processed). This is used to
// determine the size of the read window (how many bytes our side is willing to
// accept from the remote all at once). If your OnReadCallback does not buffer
// any bytes, but instead always processes bytes immediately, this method
// can simply always return 0. The callback will be provided with the userdata
// parameter that was given when the callback was set (using
// (*Socket).SetCallbacks()).
type GetRBSizeCallback func(userdata interface{}) int

// OnStateChangeCallback is the type of callback to be used when a socket
// changes its socket state. The callback will be provided with the userdata
// parameter that was given when the callback was set (using
// (*Socket).SetCallbacks()). The 'state' parameter will hold the new state
// of the Socket. It may be important to provide this callback in order to
// know when a connection is established, and a Socket can accept data, or
// when a connection has been closed.
type OnStateChangeCallback func(userdata interface{}, state State)

// OnErrorCallback is the type of callback to be used when something goes
// wrong with a connection. Currently, the possible errors which cause an
// OnErrorCallback are syscall.ECONNREFUSED, syscall.ECONNRESET, and
// syscall.ETIMEDOUT. All of these probably need to be considered fatal
// errors in the context of the connection.
//
// The callback will be provided with the userdata parameter that was given
// when the callback was set (using (*Socket).SetCallbacks()). The callback
// is responsible for calling (*Socket).Close() if appropriate to tear down
// the connection.
type OnErrorCallback func(userdata interface{}, err error)

// OnOverheadCallback is the type of callback to be used when any data is sent
// over the network. Each byte is accounted by µTP into one of the
// BandwidthType data classes, and when sent, the use of that class of data is
// reported to the application using this OnOverheadCallback. The callback will
// be provided with the userdata parameter that was given when the callback was
// set (using (*Socket).SetCallbacks()). This callback can be used to maintain
// statistics about the amount of overhead used to communicate with µTP.
//
// (Use of the PayloadBandwidth BandwidthType is also reported to this callback,
// although strictly speaking it is not really "overhead".)
type OnOverheadCallback func(userdata interface{}, send bool, count int, bandwidthType BandwidthType)

// CallbackTable contains a table of callbacks to be used by a Socket. Each
// Socket can have a different set of callbacks to call, if needed. Any members
// of the table can be left as nil, if you do not need that callback to be
// made.
//
// A CallbackTable is assigned to a Socket by use of the SetCallbacks() method.
type CallbackTable struct {
	// OnRead is the OnReadCallback to be used by this socket.
	OnRead OnReadCallback
	// OnWrite is the OnWriteCallback to be used by this socket.
	OnWrite OnWriteCallback
	// GetRBSize is the GetRBSizeCallback to be used by this socket.
	GetRBSize GetRBSizeCallback
	// OnState is the OnStateChangeCallback to be used by this socket.
	OnState OnStateChangeCallback
	// OnError is the OnErrorCallback to be used by this socket.
	OnError OnErrorCallback
	// OnOverhead is the OnOverheadCallback to be used by this socket.
	OnOverhead OnOverheadCallback
}

// GotIncomingConnection is the type of callback to be called when a new
// incoming µTP connection has been established by the socket layer. The Socket
// for the new connection is passed as 's'. The new Socket can be considered
// connected and writable immediately.
//
// The callback will be provided with the userdata parameter that was given
// along with this callback in the corresponding call to IsIncomingUTP(). The
// callback is responsible for getting the Socket ready to do work; typically,
// this involves setting callbacks for it with (*Socket).SetCallbacks().
//
// If you want to reject the new connection, it may be closed immediately with
// (*Socket).Close().
type GotIncomingConnection func(userdata interface{}, s *Socket)

// PacketSendCallback is the type of callback to be used when the µTP socket
// layer wants to transmit data over the underlying packet connection (which
// is probably UDP). The callback will be provided with the userdata parameter
// that was given along with this callback in the corresponding call to
// IsIncomingUTP() or Create(). The callback is responsible for actually
// transmitting the packet to the destination address.
//
// The callback is allowed to buffer the outgoing data instead of transmitting
// it immediately, if desired, but the data should be transmitted as soon as
// feasible.
type PacketSendCallback func(userdata interface{}, p []byte, addr *net.UDPAddr)

// Stats collects statistics for a particular Socket when utp-go is built with
// the 'utpstats' build tag. The (*Socket).GetStats() method will be made
// available when that build tag is present, and it will return a current copy
// of the Stats table for that socket.
//
// When the utpstats build tag is not used, no per-socket statistics will be
// collected.
type Stats struct {
	NBytesRecv uint64 // total bytes received
	NBytesXmit uint64 // total bytes transmitted
	ReXmit     uint32 // retransmit counter
	FastReXmit uint32 // fast retransmit counter
	NXmit      uint32 // transmit counter
	NRecv      uint32 // receive counter (total)
	NDupRecv   uint32 // duplicate receive counter
}

// GlobalStats encapsulates statistics pertaining to all use of utp-go since
// the library was loaded. A snapshot of these statistics can be accessed with
// the libutp.GetGlobalStats() function.
//
// Statistics in this struct are kept as totals for each of 5 different size
// buckets:
//
// bucket[0] :  size >    0 && size <=   23 bytes
// bucket[1] :  size >   23 && size <=  373 bytes
// bucket[2] :  size >  373 && size <=  723 bytes
// bucket[3] :  size >  723 && size <= 1400 bytes
// bucket[4] :  size > 1400 && size <   MTU bytes
//
// The size of the packet header is included in the packet size for this
// purpose. The packet header is either 20 or 23 bytes, depending on the µTP
// protocol version in use.
type GlobalStats struct {
	// NumRawRecv keeps a total of all packets received in each size bucket.
	NumRawRecv [5]uint32
	// NumRawSend keeps a total of all packets sent in each size bucket.
	NumRawSend [5]uint32
}
