// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

// This is a port of a file in the C++ libutp library as found in the Transmission app.
// Copyright (c) 2010 BitTorrent, Inc.

package libutp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"go.uber.org/zap"
)

const (
	// maxCWndIncreaseBytesPerRTT gives the number of bytes to increase max
	// window size by, per RTT. This is scaled down linearly proportional to
	// offTarget. I.e. if all packets in one window have 0 delay, window size
	// will increase by this number. Typically it's less. TCP increases one MSS
	// per RTT, which is 1500.
	maxCWndIncreaseBytesPerRTT = 3000
	// curDelaySize controls the amount of history to be kept for the
	// curDelay measurements on a connection.
	curDelaySize = 3
	// delayBaseHistory controls the amount of history to be kept for the
	// delayBase measurements on a connection. Experiments suggest that a clock
	// skew of 10 ms per 325 seconds is not impossible. The delayBase parameter
	// will effectively be reset every delayBaseHistory minutes. The clock skew
	// is dealt with by observing the delay base in the other direction, and
	// adjusting our own upwards if the opposite direction delay base keeps
	// going down.
	delayBaseHistory = 13 // minutes
	// maxWindowDecay controls the maximum amount of acceptable window decay
	// before the µTP socket layer will decide to decay maxWindow.
	maxWindowDecay = 100 // ms

	// reorderBufferMaxSize is the size of the reorder buffer.
	reorderBufferMaxSize = 511
	// outgoingBufferMaxSize is the size of the outgoing buffer.
	outgoingBufferMaxSize = 511

	// packetSize is a size of a subdivision of a packet that doesn't appear to
	// have a proper name in µTP. Maybe a better name than packetSize would be
	// "chunkSize". Window sizes and send quotas are tracked in units of
	// packetSize bytes.
	packetSize = 350

	// minWindowSize is the minimum maxWindow value. The maxWindow parameter
	// can never drop below this value.
	minWindowSize = 10

	// when window sizes are smaller than one packetSize, this
	// will pace the packets to average at the given window size
	// if it's not set, it will simply not send anything until
	// there's a timeout
	//
	// code handling the `usePacketPacing = false` case has been
	// removed in this Go transliteration of µTP. Keeping this
	// constant here so that we can find it in the future and
	// know which value we're using implicitly.
	//
	//	usePacketPacing = true

	// duplicateAcksBeforeResend controls whether a packet is resent when some
	// number of duplicate acks have been received.
	duplicateAcksBeforeResend = 3

	delayedAckByteThreshold = 2400 // bytes
	delayedAckTimeThreshold = 100  // milliseconds

	rstInfoTimeout = 10000
	rstInfoLimit   = 1000

	// 29 seconds determined from measuring many home NAT devices.
	keepaliveInterval = 29000

	seqNumberMask = 0xFFFF
	ackNumberMask = 0xFFFF
)

func divRoundUp(num, denom uint32) uint32 {
	return (num + denom - 1) / denom
}

// SocketMultiplexer coordinates µTP sockets that are sharing a single underlying
// PacketConn.
type SocketMultiplexer struct {
	logger *zap.Logger

	// this must return a positive and monotonically increasing time value. it
	// is used to assign timestamps in microseconds and in milliseconds to
	// various packet fields and timers. It must be thread-safe and reentrant.
	packetTimeCallback func() time.Duration

	rstInfo   []rstInfo
	socketMap map[string]*Socket
}

// NewSocketMultiplexer creates a new instance of SocketMultiplexer.
func NewSocketMultiplexer(logger *zap.Logger, packetTimeCallback func() time.Duration) *SocketMultiplexer {
	if packetTimeCallback == nil {
		createTime := time.Now()
		packetTimeCallback = func() time.Duration { return time.Since(createTime) }
	}
	return &SocketMultiplexer{
		logger:             logger,
		packetTimeCallback: packetTimeCallback,
		socketMap:          make(map[string]*Socket),
	}
}

func removeRstInfo(ril []rstInfo, index int) ([]rstInfo, bool) {
	dumbAssert(index < len(ril))
	c := len(ril) - 1
	itemToMove := ril[c]
	ril = ril[:c]
	if index != c {
		ril[index] = itemToMove
		return ril, true
	}
	return ril, false
}

type rstInfo struct {
	addr      *net.UDPAddr
	connID    uint32
	timestamp uint32
	ackNum    uint16
}

// these packet sizes are including the µTP header which
// is either 20 or 23 bytes depending on version.
const (
	packetSizeEmptyBucket = 0
	packetSizeEmpty       = 23
	packetSizeSmallBucket = 1
	packetSizeSmall       = 373
	packetSizeMidBucket   = 2
	packetSizeMid         = 723
	packetSizeBigBucket   = 3
	packetSizeBig         = 1400
	packetSizeHugeBucket  = 4
)

// SetPacketTimeCallback sets the packet time callback. The callback must
// return a positive and monotonically increasing time value. it is used to
// assign timestamps in microseconds and in milliseconds to various packet
// fields and timers.
//
// This must not be called after any packets have been received or sent on
// the underlying PacketConn.
func (mx *SocketMultiplexer) SetPacketTimeCallback(cb func() time.Duration) {
	mx.packetTimeCallback = cb
}

// packetHeader is an interface that can be fulfilled by packet headers in
// all supported µTP versions. This interface reduces the amount of nearly-
// duplicate code that needs to exist to support multiple packet header
// versions.
//
// (storj): we'll use this in place of reinterpret-casting memory in buffers.
type packetHeader interface {
	getVersion() int8
	setVersion(v int8)
	getConnID() uint32
	setConnID(id uint32)
	getPacketTime() uint64
	setPacketTime(packetTime uint64)
	getReplyMicro() uint32
	setReplyMicro(s uint32)
	getPacketType() packetFlag
	setPacketType(t packetFlag)
	getSequenceNumber() uint16
	setSequenceNumber(seqNum uint16)
	getAckNumber() uint16
	setAckNumber(ackNum uint16)
	getExt() int8
	setExt(ext int8)
	getWindowSize() int
	setWindowSize(ws int)
	encodedSize() int
	encodeToBytes(b []byte) error
	decodeFromBytes(b []byte) error
}

type packetAckHeader interface {
	packetHeader
	setAcks(m uint32)
	setExtNext(n uint8)
	setExtLen(n uint8)
}

// packetFormat is the structure of packet headers in µTP version 0.
//
// Use big-endian when encoding to buffer or wire.
type packetFormat struct {
	// connection ID
	connID     uint32
	tvSec      uint32
	tvUSec     uint32
	replyMicro uint32
	// receive window size in packetSize chunks
	windowSize byte
	// Type of the first extension header
	ext byte
	// Flags
	flags byte
	// Sequence number
	seqNum uint16
	// Acknowledgment number
	ackNum uint16
}

const sizeofPacketFormat = 23

func (pf *packetFormat) encodedSize() int {
	return sizeofPacketFormat
}

func (pf *packetFormat) encodeToBytes(b []byte) error {
	if len(b) < sizeofPacketFormat {
		return errors.New("buffer too small for object")
	}
	// this is finicky, but binary.Write is just too slow for this fast-path code.
	binary.BigEndian.PutUint32(b[0:4], pf.connID)
	binary.BigEndian.PutUint32(b[4:8], pf.tvSec)
	binary.BigEndian.PutUint32(b[8:12], pf.tvUSec)
	binary.BigEndian.PutUint32(b[12:16], pf.replyMicro)
	b[16] = pf.windowSize
	b[17] = pf.ext
	b[18] = pf.flags
	binary.BigEndian.PutUint16(b[19:21], pf.seqNum)
	binary.BigEndian.PutUint16(b[21:23], pf.ackNum)
	return nil
}

func (pf *packetFormat) decodeFromBytes(b []byte) error {
	if len(b) < sizeofPacketFormat {
		return errors.New("buffer too small for object")
	}
	// this is finicky, but binary.Read is just too slow for this fast-path code.
	pf.connID = binary.BigEndian.Uint32(b[0:4])
	pf.tvSec = binary.BigEndian.Uint32(b[4:8])
	pf.tvUSec = binary.BigEndian.Uint32(b[8:12])
	pf.replyMicro = binary.BigEndian.Uint32(b[12:16])
	pf.windowSize = b[16]
	pf.ext = b[17]
	pf.flags = b[18]
	pf.seqNum = binary.BigEndian.Uint16(b[19:21])
	pf.ackNum = binary.BigEndian.Uint16(b[21:23])
	return nil
}

func (pf *packetFormat) getVersion() int8 {
	return 0
}

func (pf *packetFormat) setVersion(v int8) {
	if v != 0 {
		panic("can not set version of packetFormat to anything but 0!")
	}
}

func (pf *packetFormat) getConnID() uint32 {
	return pf.connID
}

func (pf *packetFormat) setConnID(connID uint32) {
	pf.connID = connID
}

func (pf *packetFormat) getPacketTime() uint64 {
	return uint64(pf.tvSec)*1000000 + uint64(pf.tvUSec)
}

func (pf *packetFormat) setPacketTime(packetTime uint64) {
	pf.tvSec = uint32(packetTime / 1000000)
	pf.tvUSec = uint32(packetTime % 1000000)
}

func (pf *packetFormat) getReplyMicro() uint32 {
	return pf.replyMicro
}

func (pf *packetFormat) setReplyMicro(s uint32) {
	pf.replyMicro = s
}

func (pf *packetFormat) getPacketType() packetFlag {
	return packetFlag(pf.flags)
}

func (pf *packetFormat) setPacketType(t packetFlag) {
	pf.flags = byte(t)
}

func (pf *packetFormat) getSequenceNumber() uint16 {
	return pf.seqNum
}

func (pf *packetFormat) setSequenceNumber(n uint16) {
	pf.seqNum = n
}

func (pf *packetFormat) getAckNumber() uint16 {
	return pf.ackNum
}

func (pf *packetFormat) setAckNumber(n uint16) {
	pf.ackNum = n
}

func (pf *packetFormat) getExt() int8 {
	return int8(pf.ext)
}

func (pf *packetFormat) setExt(ext int8) {
	pf.ext = byte(ext)
}

func (pf *packetFormat) getWindowSize() int {
	return int(pf.windowSize) * packetSize
}

func (pf *packetFormat) setWindowSize(ws int) {
	pf.windowSize = byte(divRoundUp(uint32(ws), packetSize))
}

// use big-endian when encoding to buffer or wire.
type packetFormatAck struct {
	packetFormat
	extNext byte
	extLen  byte
	acks    [4]byte
}

const sizeofPacketFormatAck = sizeofPacketFormat + 6

func (pfa *packetFormatAck) encodedSize() int {
	return sizeofPacketFormatAck
}

func (pfa *packetFormatAck) encodeToBytes(b []byte) error {
	if len(b) < sizeofPacketFormatAck {
		return errors.New("buffer too small for object")
	}
	err := pfa.packetFormat.encodeToBytes(b[:sizeofPacketFormat])
	if err != nil {
		return err
	}
	b[sizeofPacketFormat] = pfa.extNext
	b[sizeofPacketFormat+1] = pfa.extLen
	for i, ackByte := range pfa.acks {
		b[sizeofPacketFormat+2+i] = ackByte
	}
	return nil
}

func (pfa *packetFormatAck) setAcks(m uint32) {
	pfa.acks[0] = byte(m & 0xff)
	pfa.acks[1] = byte((m >> 8) & 0xff)
	pfa.acks[2] = byte((m >> 16) & 0xff)
	pfa.acks[3] = byte((m >> 24) & 0xff)
}

func (pfa *packetFormatAck) setExtNext(n uint8) {
	pfa.extNext = n
}

func (pfa *packetFormatAck) setExtLen(n uint8) {
	pfa.extLen = n
}

type packetFormatExtensions struct {
	packetFormatAck
	// (storj): this is meant to overlay and extend packetFormatAck.acks as
	// a [8]byte. Instead, we'll just embed packetFormatAck directly and
	// add the extra bytes as a new field.
	extensions2 [4]byte
}

const sizeofPacketFormatExtensions = sizeofPacketFormatAck + 4

func (*packetFormatExtensions) encodedSize() int {
	return sizeofPacketFormatExtensions
}

func (pf *packetFormatExtensions) encodeToBytes(b []byte) error {
	if len(b) < sizeofPacketFormatExtensions {
		return errors.New("buffer too small for object")
	}
	err := pf.packetFormatAck.encodeToBytes(b[:sizeofPacketFormatAck])
	if err != nil {
		return err
	}
	for i, extByte := range pf.extensions2 {
		b[sizeofPacketFormatAck+i] = extByte
	}
	return nil
}

// packetFormatV1 is the structure of packet headers in µTP version 1.
//
// Use big-endian when encoding to buffer or wire.
type packetFormatV1 struct {
	// packet type (4 high bits)
	// protocol version (4 low bits)
	verType byte

	// Type of the first extension header
	ext byte
	// connection ID
	connID     uint16
	tvUSec     uint32
	replyMicro uint32
	// receive window size in bytes
	windowSize uint32
	// Sequence number
	seqNum uint16
	// Acknowledgment number
	ackNum uint16
}

const sizeofPacketFormatV1 = 20

func (pf *packetFormatV1) encodedSize() int {
	return sizeofPacketFormatV1
}

func (pf *packetFormatV1) encodeToBytes(b []byte) error {
	if len(b) < sizeofPacketFormatV1 {
		return errors.New("buffer too small for object")
	}
	// this is finicky, but binary.Write is just too slow for this fast-path code.
	b[0] = pf.verType
	b[1] = pf.ext
	binary.BigEndian.PutUint16(b[2:4], pf.connID)
	binary.BigEndian.PutUint32(b[4:8], pf.tvUSec)
	binary.BigEndian.PutUint32(b[8:12], pf.replyMicro)
	binary.BigEndian.PutUint32(b[12:16], pf.windowSize)
	binary.BigEndian.PutUint16(b[16:18], pf.seqNum)
	binary.BigEndian.PutUint16(b[18:20], pf.ackNum)
	return nil
}

func (pf *packetFormatV1) decodeFromBytes(b []byte) error {
	if len(b) < sizeofPacketFormatV1 {
		return errors.New("buffer too small for object")
	}
	// this is finicky, but binary.Read is just too slow for this fast-path code.
	pf.verType = b[0]
	pf.ext = b[1]
	pf.connID = binary.BigEndian.Uint16(b[2:4])
	pf.tvUSec = binary.BigEndian.Uint32(b[4:8])
	pf.replyMicro = binary.BigEndian.Uint32(b[8:12])
	pf.windowSize = binary.BigEndian.Uint32(b[12:16])
	pf.seqNum = binary.BigEndian.Uint16(b[16:18])
	pf.ackNum = binary.BigEndian.Uint16(b[18:20])
	return nil
}

func (pf *packetFormatV1) getVersion() int8 {
	return int8(pf.verType & 0xf)
}

func (pf *packetFormatV1) setVersion(v int8) {
	pf.verType = (pf.verType & 0xf0) | (byte(v) & 0xf)
}

func (pf *packetFormatV1) getPacketType() packetFlag {
	return packetFlag(pf.verType >> 4)
}

func (pf *packetFormatV1) setPacketType(t packetFlag) {
	pf.verType = (pf.verType & 0xf) | (byte(t) << 4)
}

func (pf *packetFormatV1) getConnID() uint32 {
	return uint32(pf.connID)
}

func (pf *packetFormatV1) setConnID(connID uint32) {
	pf.connID = uint16(connID)
}

func (pf *packetFormatV1) getPacketTime() uint64 {
	return uint64(pf.tvUSec)
}

func (pf *packetFormatV1) setPacketTime(t uint64) {
	// (storj): this truncation causes the time field to wrap around in
	// about 72 minutes. should be enough to measure a round-trip delay tho
	pf.tvUSec = uint32(t)
}

func (pf *packetFormatV1) getReplyMicro() uint32 {
	return pf.replyMicro
}

func (pf *packetFormatV1) setReplyMicro(s uint32) {
	pf.replyMicro = s
}

func (pf *packetFormatV1) getSequenceNumber() uint16 {
	return pf.seqNum
}

func (pf *packetFormatV1) setSequenceNumber(n uint16) {
	pf.seqNum = n
}

func (pf *packetFormatV1) getAckNumber() uint16 {
	return pf.ackNum
}

func (pf *packetFormatV1) setAckNumber(n uint16) {
	pf.ackNum = n
}

func (pf *packetFormatV1) getExt() int8 {
	return int8(pf.ext)
}

func (pf *packetFormatV1) setExt(ext int8) {
	pf.ext = byte(ext)
}

func (pf *packetFormatV1) getWindowSize() int {
	return int(pf.windowSize)
}

func (pf *packetFormatV1) setWindowSize(ws int) {
	pf.windowSize = uint32(ws)
}

// use big-endian when encoding to buffer or wire.
type packetFormatAckV1 struct {
	packetFormatV1
	extNext byte
	extLen  byte
	acks    [4]byte
}

const sizeofPacketFormatAckV1 = sizeofPacketFormatV1 + 6

func (pfa *packetFormatAckV1) encodedSize() int {
	return sizeofPacketFormatAckV1
}

func (pfa *packetFormatAckV1) encodeToBytes(b []byte) error {
	if len(b) < sizeofPacketFormatAckV1 {
		return errors.New("buffer too small for object")
	}
	err := pfa.packetFormatV1.encodeToBytes(b[:sizeofPacketFormatV1])
	if err != nil {
		return err
	}
	b[sizeofPacketFormatV1] = pfa.extNext
	b[sizeofPacketFormatV1+1] = pfa.extLen
	for i, ackByte := range pfa.acks {
		b[sizeofPacketFormatV1+2+i] = ackByte
	}
	return nil
}

func (pfa *packetFormatAckV1) setAcks(m uint32) {
	pfa.acks[0] = byte(m & 0xff)
	pfa.acks[1] = byte((m >> 8) & 0xff)
	pfa.acks[2] = byte((m >> 16) & 0xff)
	pfa.acks[3] = byte((m >> 24) & 0xff)
}

func (pfa *packetFormatAckV1) setExtNext(n uint8) {
	pfa.extNext = n
}

func (pfa *packetFormatAckV1) setExtLen(n uint8) {
	pfa.extLen = n
}

// use big-endian when encoding to buffer or wire.
type packetFormatExtensionsV1 struct {
	packetFormatAckV1
	// (storj): this is meant to overlay and extend packetFormatAckV1.acks as
	// a [8]byte. Instead, we'll just embed packetFormatAckV1 directly and
	// add the extra bytes as a new field.
	extensions2 [4]byte
}

const sizeofPacketFormatExtensionsV1 = sizeofPacketFormatAckV1 + 4

func (*packetFormatExtensionsV1) encodedSize() int {
	return sizeofPacketFormatExtensionsV1
}

func (pf *packetFormatExtensionsV1) encodeToBytes(b []byte) error {
	if len(b) < sizeofPacketFormatExtensionsV1 {
		return errors.New("buffer too small for object")
	}
	err := pf.packetFormatAckV1.encodeToBytes(b[:sizeofPacketFormatAckV1])
	if err != nil {
		return err
	}
	for i, extByte := range pf.extensions2 {
		b[sizeofPacketFormatAckV1+i] = extByte
	}
	return nil
}

type packetFlag int

const (
	stData  packetFlag = 0 // Data packet.
	stFin   packetFlag = 1 // Finalize the connection. This is the last packet.
	stState packetFlag = 2 // State packet. Used to transmit an ACK with no data.
	stReset packetFlag = 3 // Terminate connection forcefully.
	stSyn   packetFlag = 4 // Connect SYN

	stNumStates = 5 // used for bounds checking
)

var flagNames = []string{
	"STData", "STFin", "STState", "STReset", "STSyn",
}

func (fn packetFlag) String() string {
	return flagNames[fn]
}

type connState int

const (
	csIdle          connState = 0
	csSynSent       connState = 1
	csConnected     connState = 2
	csConnectedFull connState = 3
	csGotFin        connState = 4
	csDestroyDelay  connState = 5
	csFinSent       connState = 6
	csReset         connState = 7
	csDestroy       connState = 8
)

var connStateNames = []string{
	"csIdle", "csSynSent", "csConnected", "csConnectedFull", "csGotFin", "csDestroyDelay", "csFinSent", "csReset", "csDestroy",
}

func (cs connState) String() string {
	return connStateNames[cs]
}

type outgoingPacket struct {
	// length should normally be equal to len(data)
	length int
	// payload will normally be equal to len(data)-headerSize
	payload int
	// timeSent gives a sent timestamp in microseconds
	timeSent      uint64
	transmissions uint32
	needResend    bool
	header        packetHeader
	// data holds header data as well as payload
	data []byte
}

var globalStats GlobalStats

func noRead(interface{}, []byte)                       {}
func noWrite(interface{}, []byte)                      {}
func noRBSize(interface{}) int                         { return 0 }
func noState(interface{}, State)                       {}
func noError(interface{}, error)                       {}
func noOverhead(interface{}, bool, int, BandwidthType) {}

type sizableCircularBuffer struct {
	// This is the mask. Since it's always a power of 2, adding 1 to this value will return the size.
	mask int
	// This is the elements that the circular buffer points to
	elements [][]byte
}

func (scb *sizableCircularBuffer) get(i int) []byte {
	return scb.elements[i&scb.mask]
}

func (scb *sizableCircularBuffer) put(i int, data []byte) {
	scb.elements[i&scb.mask] = data
}

func (scb *sizableCircularBuffer) ensureSize(item, index int) {
	if index > scb.mask {
		scb.grow(item, index)
	}
}

func (scb *sizableCircularBuffer) size() int {
	return scb.mask + 1
}

// Item contains the element we want to make space for
// index is the index in the list.
func (scb *sizableCircularBuffer) grow(item, index int) {
	// Figure out the new size.
	size := scb.mask + 1
	for {
		size *= 2
		if index < size {
			break
		}
	}

	// Allocate the new buffer
	buf := make([][]byte, size)

	size--

	// Copy elements from the old buffer to the new buffer
	for i := 0; i <= scb.mask; i++ {
		buf[(item-index+i)&size] = scb.get(item - index + i)
	}

	// Swap to the newly allocated buffer
	scb.mask = size
	scb.elements = buf
}

// sizableCircularBufferOutgoing is exactly the same as
// sizableCircularBuffer, except it has an array of outgoingPacket pointers
// instead of an array of byte slices. Hell's bells, generics really can not
// come soon enough.
type sizableCircularBufferOutgoing struct {
	// This is the mask. Since it's always a power of 2, adding 1 to this value will return the size.
	mask int
	// This is the elements that the circular buffer points to
	elements []*outgoingPacket
}

func (scb *sizableCircularBufferOutgoing) get(i int) *outgoingPacket {
	return scb.elements[i&scb.mask]
}

func (scb *sizableCircularBufferOutgoing) put(i int, elem *outgoingPacket) {
	scb.elements[i&scb.mask] = elem
}

func (scb *sizableCircularBufferOutgoing) ensureSize(item, index int) {
	if index > scb.mask {
		scb.grow(item, index)
	}
}

func (scb *sizableCircularBufferOutgoing) size() int {
	return scb.mask + 1
}

// Item contains the element we want to make space for
// index is the index in the list.
func (scb *sizableCircularBufferOutgoing) grow(item, index int) {
	// Figure out the new size.
	size := scb.mask + 1
	for {
		size *= 2
		if index < size {
			break
		}
	}

	// Allocate the new buffer
	buf := make([]*outgoingPacket, size)

	size--

	// Copy elements from the old buffer to the new buffer
	for i := 0; i <= scb.mask; i++ {
		buf[(item-index+i)&size] = scb.get(item - index + i)
	}

	// Swap to the newly allocated buffer
	scb.mask = size
	scb.elements = buf
}

// compare if lhs is less than rhs, taking wrapping
// into account. if lhs is close to math.MaxUint32 and rhs
// is close to 0, lhs is assumed to have wrapped and
// considered smaller.
func wrappingCompareLess(lhs, rhs uint32) bool {
	// distance walking from lhs to rhs, downwards
	distDown := lhs - rhs
	// distance walking from lhs to rhs, upwards
	distUp := rhs - lhs

	// if the distance walking up is shorter, lhs
	// is less than rhs. If the distance walking down
	// is shorter, then rhs is less than lhs
	return distUp < distDown
}

type delayHist struct {
	delayBase uint32

	// this is the history of delay samples,
	// normalized by using the delayBase. These
	// values are always greater than 0 and measures
	// the queuing delay in microseconds
	curDelayHist [curDelaySize]uint32
	curDelayIdx  int

	// this is the history of delayBase. It's
	// a number that doesn't have an absolute meaning
	// only relative. It doesn't make sense to initialize
	// it to anything other than values relative to
	// what's been seen in the real world.
	delayBaseHist [delayBaseHistory]uint32
	delayBaseIdx  int
	// the time when we last stepped the delayBaseIdx
	delayBaseTime uint32

	delayBaseInitialized bool
}

func (dh *delayHist) clear(currentMS uint32) {
	dh.delayBaseInitialized = false
	dh.delayBase = 0
	dh.curDelayIdx = 0
	dh.delayBaseIdx = 0
	dh.delayBaseTime = currentMS
	for i := 0; i < curDelaySize; i++ {
		dh.curDelayHist[i] = 0
	}
	for i := 0; i < delayBaseHistory; i++ {
		dh.delayBaseHist[i] = 0
	}
}

func (dh *delayHist) shift(offset uint32) {
	// the offset should never be "negative"
	// assert(offset < 0x10000000)

	// increase all of our base delays by this amount
	// this is used to take clock skew into account
	// by observing the other side's changes in its baseDelay
	for i := 0; i < delayBaseHistory; i++ {
		dh.delayBaseHist[i] += offset
	}
	dh.delayBase += offset
}

func (dh *delayHist) addSample(sample uint32, currentMS uint32) {
	// The two clocks (in the two peers) are assumed not to
	// progress at the exact same rate. They are assumed to be
	// drifting, which causes the delay samples to contain
	// a systematic error, either they are under-
	// estimated or over-estimated. This is why we update the
	// delayBase every two minutes, to adjust for this.

	// This means the values will keep drifting and eventually wrap.
	// We can cross the wrapping boundary in two directions, either
	// going up, crossing the highest value, or going down, crossing 0.

	// if the delayBase is close to the max value and sample actually
	// wrapped on the other end we would see something like this:
	// delayBase = 0xffffff00, sample = 0x00000400
	// sample - delayBase = 0x500 which is the correct difference

	// if the delayBase is instead close to 0, and we got an even lower
	// sample (that will eventually update the delayBase), we may see
	// something like this:
	// delayBase = 0x00000400, sample = 0xffffff00
	// sample - delayBase = 0xfffffb00
	// this needs to be interpreted as a negative number and the actual
	// recorded delay should be 0.

	// It is important that all arithmetic that assume wrapping
	// is done with unsigned integers. Signed integers are not guaranteed
	// to wrap the way unsigned integers do. At least GCC takes advantage
	// of this relaxed rule and won't necessarily wrap signed ints.

	// remove the clock offset and propagation delay.
	// delay base is min of the sample and the current
	// delay base. This min-operation is subject to wrapping
	// and care needs to be taken to correctly choose the
	// true minimum.

	// specifically the problem case is when delayBase is very small
	// and sample is very large (because it wrapped past zero), sample
	// needs to be considered the smaller

	if !dh.delayBaseInitialized {
		// delayBase being 0 suggests that we haven't initialized
		// it or its history with any real measurements yet. Initialize
		// everything with this sample.
		for i := 0; i < delayBaseHistory; i++ {
			// if we don't have a value, set it to the current sample
			dh.delayBaseHist[i] = sample
			continue
		}
		dh.delayBase = sample
		dh.delayBaseInitialized = true
	}

	if wrappingCompareLess(sample, dh.delayBaseHist[dh.delayBaseIdx]) {
		// sample is smaller than the current delayBaseHist entry
		// update it
		dh.delayBaseHist[dh.delayBaseIdx] = sample
	}

	// is sample lower than delayBase? If so, update delayBase
	if wrappingCompareLess(sample, dh.delayBase) {
		// sample is smaller than the current delayBase
		// update it
		dh.delayBase = sample
	}

	// this operation may wrap, and is supposed to
	delay := sample - dh.delayBase
	// sanity check. If this is triggered, something fishy is going on
	// it means the measured sample was greater than 32 seconds!
	//	assert(delay < 0x2000000)

	dh.curDelayHist[dh.curDelayIdx] = delay
	dh.curDelayIdx = (dh.curDelayIdx + 1) % curDelaySize

	// once every minute
	if currentMS-dh.delayBaseTime > 60*1000 {
		dh.delayBaseTime = currentMS
		dh.delayBaseIdx = (dh.delayBaseIdx + 1) % delayBaseHistory
		// clear up the new delay base history spot by initializing
		// it to the current sample, then update it
		dh.delayBaseHist[dh.delayBaseIdx] = sample
		dh.delayBase = dh.delayBaseHist[0]
		// Assign the lowest delay in the last 2 minutes to delayBase
		for i := 0; i < delayBaseHistory; i++ {
			if wrappingCompareLess(dh.delayBaseHist[i], dh.delayBase) {
				dh.delayBase = dh.delayBaseHist[i]
			}
		}
	}
}

func (dh *delayHist) getValue() uint32 {
	value := uint32(math.MaxUint32)
	for i := 0; i < curDelaySize; i++ {
		value = minUint32(dh.curDelayHist[i], value)
	}
	// value could be MaxUint32 if we have no samples yet...
	return value
}

// Socket represents a µTP socket, which roughly corresponds to one connection
// to an internet peer. It is important to distinguish µTP sockets from UDP
// sockets; although µTP may be used over UDP, there is not a 1-1 correlation
// from µTP sockets to UDP sockets.
//
// Sockets are created using the mx.Create() method (for outgoing connections)
// or in the course of processing an incoming packet with mx.IsIncomingUTP()
// (for incoming connections). In the latter case, the socket object will be
// provided to your code by way of the GotIncomingConnection callback.
//
// Sockets created directly with Create() will need to have the appropriate
// callbacks registered (with SetCallbacks()) before they can initiate the
// outgoing connection. To initiate the outgoing connection, use the Connect()
// method.
//
// Sockets received by way of a GotIncomingConnection callback represent
// incoming connections, and should not have Connect() called on them.
//
// When your code wants to send data on a Socket, use the Write() method to
// indicate the amount of data that is ready to be sent. At the appropriate
// time, the Socket will use the OnWriteCallback to collect the actual data.
// The OnWriteCallback will only accept a limited amount of data; enough to
// fill a single packet. If there is more data than that to be sent, it is
// appropriate to call Write() again with the new total amount (the amount
// given to Write() is considered the _total_ amount of data which is ready
// to be written; it is not added to the amount previously indicated).
//
// When data is received from the other end of the connection, the Socket
// will call its OnReadCallback to pass the received data on to your code.
//
// If a connection times out or is rejected or reset by the peer, the Socket
// will call its OnErrorCallback.
//
// When a connection becomes writable (connected enough to accept outgoing
// data), or a connection is closed, the Socket's OnStateChangeCallback will be
// called.
//
// Your code must call mx.CheckTimeouts() periodically; as often as desired to
// give the µTP code a chance to notice that a socket has timed out. No
// background threads remain running to manage µTP state; µTP code is only run
// when your code calls into it (usually via mx.IsIncomingUTP(),
// mx.CheckTimeouts(), or the Socket methods).
//
// See the documentation on the various types of callback to learn more.
type Socket struct {
	addr       *net.UDPAddr
	addrString string

	reorderCount uint16
	duplicateAck byte

	// the number of bytes we've received but not acked yet
	bytesSinceAck int

	// the number of packets in the send queue. Packets that haven't
	// yet been sent count as well as packets marked as needing resend
	// the oldest un-acked packet in the send queue is seqNum - curWindowPackets
	curWindowPackets uint16

	// how much of the window is used, number of bytes in-flight
	// packets that have not yet been sent do not count, packets
	// that are marked as needing to be re-sent (due to a timeout)
	// don't count either
	curWindow int
	// maximum window size, in bytes
	maxWindow int
	// SO_SNDBUF setting, in bytes
	optSendBufferSize int
	// SO_RCVBUF setting, in bytes
	optRecvBufferSize int

	// Is a FIN packet in the reassembly buffer?
	gotFin bool
	// Timeout procedure
	fastTimeout bool

	// max receive window for other end, in bytes
	maxWindowUser int
	// 0 = original µTP header, 1 = second revision
	version int8
	state   connState
	// TickCount when we last decayed window (wraps)
	lastRWinDecay int32

	// the sequence number of the FIN packet. This field is only set
	// when we have received a FIN, and the flag field has the FIN flag set.
	// it is used to know when it is safe to destroy the socket, we must have
	// received all packets up to this sequence number first.
	eofPacket uint16

	// All sequence numbers up to including this have been properly received
	// by us
	ackNum uint16
	// This is the sequence number for the next packet to be sent.
	seqNum uint16

	timeoutSeqNum uint16

	// This is the sequence number of the next packet we're allowed to
	// do a fast resend with. This makes sure we only do a fast-resend
	// once per packet. We can resend the packet with this sequence number
	// or any later packet (with a higher sequence number).
	fastResendSeqNum uint16

	replyMicro uint32

	// the time when we need to send another ack. If there's
	// nothing to ack, this is a very large number
	ackTime uint32

	lastGotPacket      uint32
	lastSentPacket     uint32
	lastMeasuredDelay  uint32
	lastMaxedOutWindow uint32

	// the last time we added send quota to the connection
	// when adding send quota, this is subtracted from the
	// current time multiplied by maxWindow / rtt
	// which is the current allowed send rate.
	lastSendQuota int32

	// the number of bytes we are allowed to send on
	// this connection. If this is more than one packet
	// size when we run out of data to send, it is clamped
	// to the packet size
	// this value is multiplied by 100 in order to get
	// higher accuracy when dealing with low rates
	sendQuota int32

	sendToCB       PacketSendCallback
	sendToUserdata interface{}
	callbackTable  CallbackTable // called "func" in utp.cpp
	userdata       interface{}

	// Round trip time
	rtt uint
	// Round trip time variance
	rttVariance uint
	// Round trip timeout
	rto               uint
	rtoHist           delayHist
	retransmitTimeout uint
	// The RTO timer will timeout here.
	rtoTimeout uint
	// When the window size is set to zero, start this timer. It will send a new packet every 30secs.
	zeroWindowTime uint32

	connSeed uint32
	// Connection ID for packets I receive
	connIDRecv uint32
	// Connection ID for packets I send
	connIDSend uint32
	// Last rcv window we advertised, in bytes
	lastReceiveWindow int

	outHist   delayHist
	theirHist delayHist

	// extension bytes from SYN packet
	extensions [8]byte

	inbuf  sizableCircularBuffer
	outbuf sizableCircularBufferOutgoing

	// Public stats, returned by GetStats(). Not collected unless built
	// with the utpstats build tag.
	stats *Stats

	packetTimeCallback func() time.Duration

	logger *zap.Logger
}

func (s *Socket) getCurrentMS() uint32 {
	return uint32(s.packetTimeCallback().Milliseconds())
}

func (s *Socket) getMicroseconds() uint64 {
	return uint64(s.packetTimeCallback().Microseconds())
}

// Calculates the current receive window.
func (s *Socket) getRcvWindow() int {
	// If we don't have a connection (such as during connection
	// establishment), always act as if we have an empty buffer.
	if s.userdata == nil {
		return s.optRecvBufferSize
	}

	// Trim window down according to what's already in buffer.
	numBuf := s.callbackTable.GetRBSize(s.userdata)
	dumbAssert(numBuf >= 0)
	if s.optRecvBufferSize > numBuf {
		return s.optRecvBufferSize - numBuf
	}
	return 0
}

// Test if we're ready to decay maxWindow
// XXX this breaks when spaced by > math.MaxInt32/2, which is 49
// days; the failure mode in that case is we do an extra decay
// or fail to do one when we really shouldn't.
func (s *Socket) canDecayWin(msec int32) bool {
	return msec-s.lastRWinDecay >= maxWindowDecay
}

// If we can, decay max window. Returns true if we actually did so.
func (s *Socket) maybeDecayWin(currentMS uint32) {
	if s.canDecayWin(int32(currentMS)) {
		// TCP uses 0.5
		s.maxWindow = int(float64(s.maxWindow) * .5) // (storj): why not us.maxWindow /= 2?
		s.lastRWinDecay = int32(currentMS)
		if s.maxWindow < minWindowSize {
			s.maxWindow = minWindowSize
		}
	}
}

func (s *Socket) getHeaderSize() int {
	if s.version == 0 {
		return sizeofPacketFormat
	}
	return sizeofPacketFormatV1
}

func (s *Socket) getHeaderExtensionsSize() int {
	if s.version == 0 {
		return sizeofPacketFormatExtensions
	}
	return sizeofPacketFormatExtensionsV1
}

func (s *Socket) sentAck(currentMS uint32) {
	s.ackTime = currentMS + 0x70000000
	s.bytesSinceAck = 0
}

// GetUDPMTU returns the maximum size of UDP packets that may be sent using
// this Socket.
func (s *Socket) GetUDPMTU() int {
	return int(GetUDPMTU(s.addr))
}

// GetUDPOverhead returns the number of bytes of overhead that apply to each
// UDP packet sent (the size of a UDP header plus IPv4 or IPv6 overhead).
func (s *Socket) GetUDPOverhead() int {
	return int(getUDPOverhead(s.addr))
}

// NOTE(storj): As far as I can tell, nothing in the library or in any of the
// code that uses the library cares about this function. -thepaul
/*
func (us *Socket) getGlobalUTPBytesSent() uint64 {
	return GetGlobalUTPBytesSent(us.addr)
}
*/

// GetOverhead returns the number of bytes of overhead that apply to each µTP
// packet sent (GetUDPOverhead() plus the size of the µTP packet header for the
// µTP protocol version in use for this connection).
func (s *Socket) GetOverhead() int {
	return s.GetUDPOverhead() + s.getHeaderSize()
}

func registerSentPacket(length int) {
	if length <= packetSizeMid {
		if length <= packetSizeEmpty {
			atomic.AddUint32(&globalStats.NumRawSend[packetSizeEmptyBucket], 1)
		} else if length <= packetSizeSmall {
			atomic.AddUint32(&globalStats.NumRawSend[packetSizeSmallBucket], 1)
		} else {
			atomic.AddUint32(&globalStats.NumRawSend[packetSizeMidBucket], 1)
		}
	} else {
		if length <= packetSizeBig {
			atomic.AddUint32(&globalStats.NumRawSend[packetSizeBigBucket], 1)
		} else {
			atomic.AddUint32(&globalStats.NumRawSend[packetSizeHugeBucket], 1)
		}
	}
}

func sendToAddr(sendToProc PacketSendCallback, sendToUserdata interface{}, p []byte, addr *net.UDPAddr) {
	registerSentPacket(len(p))
	sendToProc(sendToUserdata, p, addr)
}

// we'll expect "data" to contain all the data for the packet's payload,
// _along with_ enough empty space at the beginning to hold the packet
// header (that is, data[0:b.encodedSize()] will be overwritten). This is
// done partially in order to avoid copies and partially to remain close
// to the structure of the original C++ code.
func (s *Socket) sendData(b packetHeader, data []byte, bwType BandwidthType, currentMS uint32) {
	// time stamp this packet with local time, the stamp goes into
	// the header of every packet at the 8th byte for 8 bytes :
	// two integers, check packet.h for more
	packetTime := s.getMicroseconds()

	b.setPacketTime(packetTime)
	b.setReplyMicro(s.replyMicro)

	s.lastSentPacket = currentMS

	headerSize := b.encodedSize()
	err := b.encodeToBytes(data[0:headerSize])
	if err != nil {
		panic(err)
	}
	s.stats.transmitted(len(data))

	if s.userdata != nil {
		var n int
		if bwType == PayloadBandwidth {
			// if this packet carries payload, just
			// count the header as overhead
			bwType = HeaderOverhead
			n = s.GetOverhead()
		} else {
			n = len(data) + s.GetUDPOverhead()
		}
		s.callbackTable.OnOverhead(s.userdata, true, n, bwType)
	}

	flags := b.getPacketType()
	seqNum := b.getSequenceNumber()
	ackNum := b.getAckNumber()

	s.logger.Debug("send-data", zap.Int("len", len(data)), zap.Uint32("id", s.connIDSend), zap.Uint64("timestamp", packetTime), zap.Uint32("reply_micro", s.replyMicro), zap.Stringer("flags", flags), zap.Uint16("seq_nr", seqNum), zap.Uint16("ack_nr", ackNum))

	sendToAddr(s.sendToCB, s.sendToUserdata, data, s.addr)
}

func (s *Socket) sendAck(synack bool, currentMS uint32) {
	var pa packetAckHeader
	if s.version == 0 {
		pa = &packetFormatAck{}
	} else {
		pa = &packetFormatAckV1{}
	}
	s.lastReceiveWindow = s.getRcvWindow()
	pa.setVersion(s.version)
	pa.setConnID(s.connIDSend)
	pa.setPacketType(stState)
	pa.setAckNumber(s.ackNum)
	pa.setSequenceNumber(s.seqNum)
	pa.setExt(0)
	pa.setWindowSize(s.lastReceiveWindow)

	// we never need to send EACK for connections
	// that are shutting down
	if s.reorderCount != 0 && s.state < csGotFin {
		// if reorder count > 0, send an EACK.
		// reorder count should always be 0
		// for synacks, so this should not be
		// as synack
		dumbAssert(!synack)
		pa.setExt(1)
		pa.setExtNext(0)
		pa.setExtLen(4)
		var m uint32

		// reorder count should only be non-zero
		// if the packet ackNum + 1 has not yet
		// been received
		dumbAssert(s.inbuf.get(int(s.ackNum)+1) == nil)
		window := minInt(14+16, s.inbuf.size())
		// Generate bit mask of segments received.
		for i := 0; i < window; i++ {
			if s.inbuf.get(int(s.ackNum)+i+2) != nil {
				m |= 1 << i
				s.logger.Debug("EACK packet", zap.Int("ack_nr", int(s.ackNum)+i+2))
			}
		}
		pa.setAcks(m)
		s.logger.Debug("Sending EACK", zap.Uint16("ack_nr", s.ackNum), zap.Uint32("id", s.connIDSend), zap.Uint32("bits", m))
	} else if synack {
		// we only send "extensions" in response to SYN
		// and the reorder count is 0 in that state

		s.logger.Debug("Sending ACK with extension bits", zap.Uint16("ack_nr", s.ackNum), zap.Uint32("id", s.connIDSend))
		switch pfa := pa.(type) {
		case *packetFormatAck:
			pa = &packetFormatExtensions{packetFormatAck: *pfa}
		case *packetFormatAckV1:
			pa = &packetFormatExtensionsV1{packetFormatAckV1: *pfa}
		}
		pa.setExt(2)
		pa.setExtNext(0)
		pa.setExtLen(8)
	} else {
		s.logger.Debug("Sending ACK", zap.Uint16("ack_nr", s.ackNum), zap.Uint32("id", s.connIDSend))
	}

	s.sentAck(currentMS)
	packetData := make([]byte, pa.encodedSize())
	s.sendData(pa, packetData, AckOverhead, currentMS)
}

func (s *Socket) sendKeepAlive(currentMS uint32) {
	s.ackNum--
	s.logger.Debug("Sending KeepAlive ACK", zap.Uint16("ack_nr", s.ackNum), zap.Uint32("id", s.connIDSend))
	s.sendAck(false, currentMS)
	s.ackNum++
}

func sendRST(logger *zap.Logger, sendToProc PacketSendCallback, sendToUserdata interface{}, addr *net.UDPAddr, connIDSend uint32, ackNum uint16, seqNum uint16, version int8) {
	var p packetHeader
	if version == 0 {
		p = &packetFormat{}
	} else {
		p = &packetFormatV1{}
	}
	p.setVersion(version)
	p.setConnID(connIDSend)
	p.setAckNumber(ackNum)
	p.setSequenceNumber(seqNum)
	p.setPacketType(stReset)
	p.setExt(0)
	p.setWindowSize(0)

	packetData := make([]byte, p.encodedSize())
	err := p.encodeToBytes(packetData)
	if err != nil {
		panic(err)
	}

	logger.Debug("sending RST", zap.Uint32("id", connIDSend), zap.Uint16("seq_nr", seqNum), zap.Uint16("ack_nr", ackNum))
	logger.Debug("send", zap.Int("len", len(packetData)), zap.Uint32("id", connIDSend))
	sendToAddr(sendToProc, sendToUserdata, packetData, addr)
}

func (s *Socket) sendPacket(pkt *outgoingPacket, currentMS uint32) {
	// only count against the quota the first time we
	// send the packet. Don't enforce quota when closing
	// a socket. Only enforce the quota when we're sending
	// at slow rates (max window < packet size)
	maxSend := minInt(s.maxWindow, minInt(s.optSendBufferSize, s.maxWindowUser))

	if pkt.transmissions == 0 || pkt.needResend {
		s.curWindow += pkt.payload
	}

	maxPacketSize := s.GetPacketSize()
	if pkt.transmissions == 0 && maxSend < maxPacketSize {
		dumbAssert(s.state == csFinSent || int32(pkt.payload) <= s.sendQuota/100)
		s.sendQuota -= int32(pkt.payload * 100)
	}

	pkt.needResend = false

	pkt.header.setAckNumber(s.ackNum)
	pkt.timeSent = s.getMicroseconds()
	pkt.transmissions++
	s.sentAck(currentMS)
	bwType := PayloadBandwidth
	if s.state == csSynSent {
		bwType = ConnectOverhead
	} else if pkt.transmissions != 1 {
		bwType = RetransmitOverhead
	}
	s.sendData(pkt.header, pkt.data, bwType, currentMS)
}

func (s *Socket) isWritable(toWrite int, currentMS uint32) bool {
	// return true if it's OK to stuff another packet into the
	// outgoing queue. Since we may be using packet pacing, we
	// might not actually send the packet right away to affect the
	// curWindow. The only thing that happens when we add another
	// packet is that curWindowPackets is increased.
	maxSend := minInt(s.maxWindow, minInt(s.optSendBufferSize, s.maxWindowUser))

	maxPacketSize := s.GetPacketSize()

	if s.curWindow+maxPacketSize >= s.maxWindow {
		s.lastMaxedOutWindow = currentMS
	}

	s.logger.Debug("isWritable(start)", zap.Int("toWrite", toWrite), zap.Int("curWindow", s.curWindow), zap.Int("maxWindow", s.maxWindow), zap.Int32("sendQuota", s.sendQuota), zap.Uint16("curWindowPackets", s.curWindowPackets), zap.Int("maxPacketSize", maxPacketSize), zap.Int("maxSend", maxSend))

	// if we don't have enough quota, we can't write regardless
	if s.sendQuota/100 < int32(toWrite) {
		s.logger.Debug("isWritable=false: sendQuota/100 < toWrite", zap.Int32("sendQuota", s.sendQuota), zap.Int("toWrite", toWrite))
		return false
	}

	// subtract one to save space for the FIN packet
	if s.curWindowPackets >= outgoingBufferMaxSize-1 {
		s.logger.Debug("isWritable=false: curWindowPackets > outgoingBufferMaxSize-1", zap.Uint16("curWindowPackets", s.curWindowPackets), zap.Int("outgoingBufferMaxSize", outgoingBufferMaxSize))
		return false
	}

	// if sending another packet would not make the window exceed
	// the maxWindow, we can write
	if s.curWindow+maxPacketSize <= maxSend {
		s.logger.Debug("isWritable=true: curWindow + maxPacketSize <= maxSend", zap.Int("curWindow", s.curWindow), zap.Int("maxPacketSize", maxPacketSize), zap.Int("maxSend", maxSend))
		return true
	}

	// if the window size is less than a packet, and we have enough
	// quota to send a packet, we can write, even though it would
	// make the window exceed the max size
	// the last condition is needed to not put too many packets
	// in the send buffer. curWindow isn't updated until we flush
	// the send buffer, so we need to take the number of packets
	// into account
	if s.maxWindow < toWrite && s.curWindow < s.maxWindow && s.curWindowPackets == 0 {
		s.logger.Debug("isWritable=true: curWindow < maxWindow < toWrite and curWindowPackets=0", zap.Int("curWindow", s.curWindow), zap.Int("maxWindow", s.maxWindow), zap.Int("toWrite", toWrite))
		return true
	}

	if !(s.maxWindow < toWrite && s.curWindow < s.maxWindow) {
		s.logger.Debug("isWritable=false: curWindow >= maxWindow or maxWindow >= toWrite", zap.Int("curWindow", s.curWindow), zap.Int("maxWindow", s.maxWindow), zap.Int("toWrite", toWrite))
	} else {
		s.logger.Debug("isWritable=false", zap.Uint16("curWindowPackets", s.curWindowPackets))
	}
	return false
}

func (s *Socket) flushPackets(currentMS uint32) bool {
	maxPacketSize := s.GetPacketSize()

	// send packets that are waiting on the pacer to be sent
	// i has to be an unsigned 16 bit counter to wrap correctly
	// signed types are not guaranteed to wrap the way you expect
	for i := s.seqNum - s.curWindowPackets; i != s.seqNum; i++ {
		pkt := s.outbuf.get(int(i))

		if pkt == nil || (pkt.transmissions > 0 && pkt.needResend == false) {
			continue
		}
		// have we run out of quota?
		if !s.isWritable(pkt.payload, currentMS) {
			return true
		}

		// Nagle check
		// don't send the last packet if we have one packet in-flight
		// and the current packet is still smaller than maxPacketSize.
		if i != ((s.seqNum-1)&ackNumberMask) || s.curWindowPackets == 1 || pkt.payload >= maxPacketSize {
			s.sendPacket(pkt, currentMS)

			// No need to send another ack if there is nothing to reorder.
			if s.reorderCount == 0 {
				s.sentAck(currentMS)
			}
		}
	}
	return false
}

func (s *Socket) writeOutgoingPacket(payload int, flags packetFlag, currentMS uint32) {
	// Setup initial timeout timer
	if s.curWindowPackets == 0 {
		s.retransmitTimeout = s.rto
		s.rtoTimeout = uint(currentMS) + s.retransmitTimeout
		dumbAssert(s.curWindow == 0)
	}

	maxPacketSize := s.GetPacketSize()
	for {
		dumbAssert(s.curWindowPackets < outgoingBufferMaxSize)
		dumbAssert(flags == stData || flags == stFin)

		var added int
		var pkt *outgoingPacket

		if s.curWindowPackets > 0 {
			pkt = s.outbuf.get(int(s.seqNum) - 1)
		}

		headerSize := s.getHeaderSize()
		doAppend := true

		// if there's any room left in the last packet in the window
		// and it hasn't been sent yet, fill that frame first
		if payload != 0 && pkt != nil && pkt.transmissions == 0 && pkt.payload < maxPacketSize {
			// Use the previous unsent packet
			added = minInt(payload+pkt.payload, maxInt(maxPacketSize, pkt.payload)) - pkt.payload

			// make the data buffer have enough room for the added data
			if cap(pkt.data) < len(pkt.data)+added {
				newBuf := make([]byte, len(pkt.data)+added)
				copy(newBuf[:len(pkt.data)], pkt.data)
				pkt.data = newBuf
			} else {
				pkt.data = pkt.data[0:(len(pkt.data) + added)]
			}
			doAppend = false
			dumbAssert(!pkt.needResend)
		} else {
			// Create the packet to send.
			added = payload
			pkt = &outgoingPacket{
				payload:       0,
				transmissions: 0,
				needResend:    false,
			}
			pkt.data = make([]byte, headerSize+added)
			if s.version == 0 {
				pkt.header = &packetFormat{}
			} else {
				pkt.header = &packetFormatV1{}
			}
			pkt.header.setVersion(s.version)
		}

		if added > 0 {
			// Fill the new section with data from the upper layer.
			s.callbackTable.OnWrite(s.userdata, pkt.data[headerSize+pkt.payload:])
		}
		pkt.payload += added
		pkt.length = headerSize + pkt.payload

		s.lastReceiveWindow = s.getRcvWindow()

		header := pkt.header
		header.setVersion(s.version)
		header.setConnID(s.connIDSend)
		header.setWindowSize(s.lastReceiveWindow)
		header.setExt(0)
		header.setAckNumber(s.ackNum)
		header.setPacketType(flags)

		if doAppend {
			// Remember the message in the outgoing queue.
			s.outbuf.ensureSize(int(s.seqNum), int(s.curWindowPackets))
			s.outbuf.put(int(s.seqNum), pkt)
			header.setSequenceNumber(s.seqNum)
			s.seqNum++
			s.curWindowPackets++
		}

		payload -= added

		if payload == 0 {
			break
		}
	}

	s.flushPackets(currentMS)
}

func (s *Socket) updateSendQuota(currentMS uint32) {
	dt := int32(currentMS) - s.lastSendQuota
	if dt == 0 {
		return
	}
	s.lastSendQuota = int32(currentMS)
	delayBase := s.rtoHist.delayBase
	if delayBase == 0 {
		delayBase = 50
	}
	maxWindow := int32(s.maxWindow)
	add := maxWindow * dt * 100 / int32(delayBase)
	if add > maxWindow*100 && add > maxCWndIncreaseBytesPerRTT*100 {
		add = maxWindow
	}
	s.sendQuota += add
	s.logger.Debug("(*Socket).updateSendQuota", zap.Int32("dt", dt), zap.Uint32("rtt", delayBase), zap.Int32("max_window", maxWindow), zap.Int32("quota", s.sendQuota/100))
}

func (s *Socket) checkTimeouts(currentMS uint32) {
	s.checkInvariants()

	// this invariant should always be true
	dumbAssert(s.curWindowPackets == 0 || s.outbuf.get(int(s.seqNum)-int(s.curWindowPackets)) != nil)

	s.logger.Debug("CheckTimeouts", zap.Int("timeout", int(s.rtoTimeout)-int(currentMS)), zap.Int("max_window", s.maxWindow), zap.Int("cur_window", s.curWindow), zap.Int32("quota", s.sendQuota/100), zap.Stringer("state", s.state), zap.Uint16("cur_window_packets", s.curWindowPackets), zap.Int("bytes_since_ack", s.bytesSinceAck), zap.Uint32("ack_time", currentMS-s.ackTime))

	s.updateSendQuota(currentMS)
	s.flushPackets(currentMS)

	// In case the new send quota made it possible to send another packet
	// Mark the socket as writable. If we don't use pacing, the send
	// quota does not affect if the socket is writeable
	// if we don't use packet pacing, the writable event is triggered
	// whenever the curWindow falls below the maxWindow, so we don't
	// need this check then
	if s.state == csConnectedFull && s.isWritable(s.GetPacketSize(), currentMS) {
		s.state = csConnected
		s.logger.Debug("Socket writable", zap.Int("max_window", s.maxWindow), zap.Int("cur_window", s.curWindow), zap.Int32("quota", s.sendQuota/100), zap.Int("packet_size", s.GetPacketSize()))
		s.callbackTable.OnState(s.userdata, StateWritable)
	}

	switch s.state {
	case csSynSent, csConnectedFull, csConnected, csFinSent:
		// Reset max window...
		if int(currentMS)-int(s.zeroWindowTime) >= 0 && s.maxWindowUser == 0 {
			s.maxWindowUser = packetSize
		}

		if int(currentMS)-int(s.rtoTimeout) >= 0 && s.curWindowPackets > 0 && s.rtoTimeout > 0 {

			/*
				// (commented out in original C++ code; unclear why)
				pkt := us.outbuf.get(int(us.seqNum)-int(us.curWindowPackets))
				// If there were a lot of retransmissions, force recomputation of round trip time
				if pkt.transmissions >= 4 {
					us.rtt = 0
				}
			*/

			// Increase RTO
			newTimeout := s.retransmitTimeout * 2
			if newTimeout >= 30000 || (s.state == csSynSent && newTimeout > 6000) {
				// more than 30 seconds with no reply. kill it.
				// if we haven't even connected yet, give up sooner. 6 seconds
				// means 2 tries at the following timeouts: 3, 6 seconds
				if s.state == csFinSent {
					s.state = csDestroy
				} else {
					s.state = csReset
				}
				s.callbackTable.OnError(s.userdata, syscall.ETIMEDOUT)
				goto getout
			}

			s.retransmitTimeout = newTimeout
			s.rtoTimeout = uint(currentMS) + newTimeout

			// On Timeout
			s.duplicateAck = 0

			// rate = min_rate
			s.maxWindow = s.GetPacketSize()
			s.sendQuota = maxInt32(int32(s.maxWindow)*100, s.sendQuota)

			// every packet should be considered lost
			for i := 0; i < int(s.curWindowPackets); i++ {
				pkt := s.outbuf.get(int(s.seqNum) - i - 1)
				if pkt == nil || pkt.transmissions == 0 || pkt.needResend {
					continue
				}
				pkt.needResend = true
				dumbAssert(s.curWindow >= pkt.payload)
				s.curWindow -= pkt.payload
			}

			// used in parse_log.py
			s.logger.Info("Packet timeout. Resend", zap.Uint16("seq_nr", s.seqNum-s.curWindowPackets), zap.Uint("timeout", s.retransmitTimeout), zap.Int("max_window", s.maxWindow))

			s.fastTimeout = true
			s.timeoutSeqNum = s.seqNum

			if s.curWindowPackets > 0 {
				pkt := s.outbuf.get(int(s.seqNum) - int(s.curWindowPackets))
				dumbAssert(pkt != nil)
				s.sendQuota = maxInt32(int32(pkt.length)*100, s.sendQuota)

				// Re-send the packet.
				s.sendPacket(pkt, currentMS)
			}
		}

		// Mark the socket as writable
		if s.state == csConnectedFull && s.isWritable(s.GetPacketSize(), currentMS) {
			s.state = csConnected
			s.logger.Debug("Socket writable", zap.Int("max_window", s.maxWindow), zap.Int("cur_window", s.curWindow), zap.Int32("quota", s.sendQuota/100), zap.Int("packet_size", s.GetPacketSize()))
			s.callbackTable.OnState(s.userdata, StateWritable)
		}

		if s.state >= csConnected && s.state <= csFinSent {
			// Send acknowledgment packets periodically, or when the threshold is reached
			if s.bytesSinceAck > delayedAckByteThreshold ||
				int(currentMS)-int(s.ackTime) >= 0 {
				s.sendAck(false, currentMS)
			}

			if (currentMS - s.lastSentPacket) >= keepaliveInterval {
				s.sendKeepAlive(currentMS)
			}
		}

	// Close?
	case csGotFin, csDestroyDelay:
		if int(currentMS)-int(s.rtoTimeout) >= 0 {
			if s.state == csDestroyDelay {
				s.state = csDestroy
			} else {
				s.state = csReset
			}
			if s.curWindowPackets > 0 && s.userdata != nil {
				s.callbackTable.OnError(s.userdata, syscall.ECONNRESET)
			}
		}

	// prevent compiler warning
	case csIdle, csReset, csDestroy:
	}

getout:
	// make sure we don't accumulate quota when we don't have
	// anything to send
	limit := maxInt32(int32(s.maxWindow)/2, 5*int32(s.GetPacketSize())) * 100
	if s.sendQuota > limit {
		s.sendQuota = limit
	}
}

func (s *Socket) ackPacket(seq uint16, currentMS uint32) int {
	pkt := s.outbuf.get(int(seq))

	// the packet has already been acked (or not sent)
	if pkt == nil {
		s.logger.Debug("got ack (already acked, or packet never sent)", zap.Uint16("for", seq))
		return 1
	}

	// can't ack packets that haven't been sent yet!
	if pkt.transmissions == 0 {
		s.logger.Debug("got ack (never sent)", zap.Uint16("for", seq), zap.Int("pkt_size", pkt.payload), zap.Bool("need_resend", pkt.needResend))
		return 2
	}

	s.logger.Debug("got ack", zap.Uint16("for", seq), zap.Int("pkt_size", pkt.payload), zap.Bool("need_resend", pkt.needResend))

	s.outbuf.put(int(seq), nil)

	// if we never re-sent the packet, update the RTT estimate
	if pkt.transmissions == 1 {
		// Estimate the round trip time.
		ertt := uint32((s.getMicroseconds() - pkt.timeSent) / 1000)
		if s.rtt == 0 {
			// First round trip time sample
			s.rtt = uint(ertt)
			s.rttVariance = uint(ertt / 2)
			// sanity check. rtt should never be more than 6 seconds
			//			assert(us.rtt < 6000)
		} else {
			// Compute new round trip times
			delta := int(s.rtt) - int(ertt)
			s.rttVariance = uint(int(s.rttVariance) + (abs(delta)-int(s.rttVariance))/4)
			s.rtt = s.rtt - s.rtt/8 + uint(ertt)/8
			// sanity check. rtt should never be more than 6 seconds
			//			assert(us.rtt < 6000)
			s.rtoHist.addSample(ertt, currentMS)
		}
		s.rto = maxUint(s.rtt+s.rttVariance*4, 500)
		s.logger.Debug("update rtt estimate", zap.Uint32("rtt", ertt), zap.Uint("avg", s.rtt), zap.Uint("var", s.rttVariance), zap.Uint("rto", s.rto))
	}
	s.retransmitTimeout = s.rto
	s.rtoTimeout = uint(currentMS) + s.rto
	// if needResend is set, this packet has already
	// been considered timed-out, and is not included in
	// the curWindow anymore
	if !pkt.needResend {
		dumbAssert(s.curWindow >= pkt.payload)
		s.curWindow -= pkt.payload
	}
	return 0
}

func (s *Socket) selectiveAckBytes(base uint, mask []byte, minRTT *int64) int {
	if s.curWindowPackets == 0 {
		return 0
	}

	ackedBytes := 0
	bits := len(mask)*8 + 1

	for {
		bits--
		if bits < -1 {
			break
		}

		v := uint(int(base) + bits)

		// ignore bits that haven't been sent yet
		// see comment in Socket::selectiveAck
		if ((uint(s.seqNum) - v - 1) & ackNumberMask) >= uint(s.curWindowPackets-1) {
			continue
		}

		// ignore bits that represents packets we haven't sent yet
		// or packets that have already been acked
		pkt := s.outbuf.get(int(v))
		if pkt == nil || pkt.transmissions == 0 {
			continue
		}

		// Count the number of segments that were successfully received past it.
		if bits >= 0 && bits < len(mask)*8 && (mask[bits>>3]&(1<<(bits&7))) != 0 {
			dumbAssert(pkt.payload >= 0)
			ackedBytes += pkt.payload
			*minRTT = minInt64(*minRTT, int64(s.getMicroseconds()-pkt.timeSent))
			continue
		}
	}
	return ackedBytes
}

const maxEAck = 128

func (s *Socket) selectiveAck(base uint16, mask []byte, currentMS uint32) {
	if s.curWindowPackets == 0 {
		return
	}

	// the range is inclusive [0, 31] bits
	bits := len(mask) * 8

	count := 0

	// resends is a stack of sequence numbers we need to resend. Since we
	// iterate in reverse over the acked packets, at the end, the top packets
	// are the ones we want to resend
	var resends [maxEAck]int
	nr := 0

	s.logger.Debug("Got EACK", zap.ByteString("mask", mask), zap.Uint16("base", base))
	for {
		bits--
		if bits < -1 {
			break
		}

		// we're iterating over the bits from higher sequence numbers
		// to lower (kind of in reverse order, which might not be very
		// intuitive)
		v := int(base) + bits

		// ignore bits that haven't been sent yet
		// and bits that fall below the ACKed sequence number
		// this can happen if an EACK message gets
		// reordered and arrives after a packet that ACKs up past
		// the base for this EACK message

		// this is essentially the same as:
		// if v >= seqNum || v <= seqNum - curWindowPackets
		// but it takes wrapping into account

		// if v == seqNum the -1 will make it wrap. if v > seqNum
		// it will also wrap (since it will fall further below 0)
		// and be > curWindowPackets.
		// if v == seqNum - curWindowPackets, the result will be
		// seqNum - (seqNum - curWindowPackets) - 1
		// == seqNum - seqNum + curWindowPackets - 1
		// == curWindowPackets - 1 which will be caught by the
		// test. If v < seqNum - curWindowPackets the result will grow
		// fall further outside of the curWindowPackets range.

		// sequence number space:
		//
		//     rejected <   accepted   > rejected
		// <============+--------------+============>
		//              ^              ^
		//              |              |
		//        (seqNum-wnd)         seqNum

		if ((int(s.seqNum) - v - 1) & ackNumberMask) >= (int(s.curWindowPackets) - 1) {
			continue
		}

		// this counts as a duplicate ack, even though we might have
		// received an ack for this packet previously (in another EACK
		// message for instance)
		bitSet := bits >= 0 && (mask[bits>>3]&(1<<(bits&7)) != 0)

		// if this packet is acked, it counts towards the duplicate ack counter
		if bitSet {
			count++
		}

		// ignore bits that represents packets we haven't sent yet
		// or packets that have already been acked
		pkt := s.outbuf.get(v)
		if pkt == nil || pkt.transmissions == 0 {
			transmissions := uint32(0)
			msg := "(already acked?)"
			if pkt != nil {
				transmissions = pkt.transmissions
				msg = "(not sent yet?)"
			}
			s.logger.Debug("skipping", zap.Int("v", v), zap.Uint32("transmissions", transmissions), zap.String("msg", msg))
			continue
		}

		// Count the number of segments that were successfully received past it.
		if bitSet {
			// the selective ack should never ACK the packet we're waiting for to decrement curWindowPackets
			dumbAssert((v & s.outbuf.mask) != (int(s.seqNum-s.curWindowPackets) & s.outbuf.mask))
			s.ackPacket(uint16(v), currentMS)
			continue
		}

		// Resend segments
		// if count is less than our re-send limit, we haven't seen enough
		// acked packets in front of this one to warrant a re-send.
		// if count == 0, we're still going through the tail of zeroes
		if ((v-int(s.fastResendSeqNum))&ackNumberMask) <= outgoingBufferMaxSize &&
			count >= duplicateAcksBeforeResend &&
			s.duplicateAck < duplicateAcksBeforeResend {

			// resends is a stack, and we're mostly interested in the top of it
			// if we're full, just throw away the lower half
			if nr >= maxEAck-2 {
				copy(resends[0:(maxEAck/2)], resends[(maxEAck/2):])
				nr -= maxEAck / 2
			}
			resends[nr] = v
			nr++
			s.logger.Debug("no ack", zap.Int("for", v))
		} else {
			s.logger.Debug("not resending", zap.Int("v", v), zap.Int("count", count), zap.Uint8("dup_ack", s.duplicateAck), zap.Uint16("fast_resend_seq_nr", s.fastResendSeqNum))
		}
	}

	if ((base-1-s.fastResendSeqNum)&ackNumberMask) <= outgoingBufferMaxSize &&
		count >= duplicateAcksBeforeResend {
		// if we get enough duplicate acks to start
		// resending, the first packet we should resend
		// is base-1
		resends[nr] = (int(base) - 1) & ackNumberMask
		nr++
	} else {
		s.logger.Debug("not resending (not enough dup acks)", zap.Uint16("base", base), zap.Int("count", count), zap.Uint8("dup_ack", s.duplicateAck), zap.Uint16("fast_resend_seq_nr", s.fastResendSeqNum))
	}

	backOff := false
	i := 0
	for nr > 0 {
		nr--
		v := uint(resends[nr])
		// don't consider the tail of 0:es to be lost packets
		// only un-acked packets with acked packets after should
		// be considered lost
		pkt := s.outbuf.get(int(v))

		// this may be an old (re-ordered) packet, and some of the
		// packets in here may have been acked already. In which
		// case they will not be in the send queue anymore
		if pkt == nil {
			continue
		}

		// used in parse_log.py
		s.logger.Info("Packet lost. Resending", zap.Uint("seq_nr", v))

		// On Loss
		backOff = true

		s.stats.packetLost()
		s.sendPacket(pkt, currentMS)
		s.fastResendSeqNum = uint16(v + 1)

		// Re-send max 4 packets.
		i++
		if i >= 4 {
			break
		}
	}

	if backOff {
		s.maybeDecayWin(currentMS)
	}

	s.duplicateAck = byte(count)
}

func (s *Socket) applyLEDBATControl(bytesAcked int, actualDelay uint32, minRTT int64, currentMS uint32) {
	// the delay can never be greater than the rtt. The minRTT
	// variable is the RTT in microseconds

	dumbAssert(minRTT >= 0)
	ourDelay := int32(minUint32(s.outHist.getValue(), uint32(minRTT)))
	dumbAssert(ourDelay != math.MaxInt32)
	dumbAssert(ourDelay >= 0)

	delaySample(s.addr, int(ourDelay/1000))

	// This test the connection under heavy load from foreground
	// traffic. Pretend that our delays are very high to force the
	// connection to use sub-packet size window sizes
	// us.ourDelay *= 4

	// target is microseconds
	target := congestionControlTarget
	if target <= 0 {
		target = 100000
	}

	offTarget := float64(target - int(ourDelay))

	// this is the same as:
	//
	//    (min(offTarget, target) / target) * (bytesAcked / maxWindow) * maxCWndIncreaseBytesPerRTT
	//
	// so, it's scaling the max increase by the fraction of the window this ack represents, and the fraction
	// of the target delay the current delay represents.
	// The min() around offTarget protects against crazy values of ourDelay, which may happen when th
	// timestamps wraps, or by just having a malicious peer sending garbage. This caps the increase
	// of the window size to maxCWndIncreaseBytesPerRTT per rtt.
	// as for large negative numbers, this direction is already capped at the min packet size further down
	// the min around the bytesAcked protects against the case where the window size was recently
	// shrunk and the number of acked bytes exceeds that. This is considered no more than one full
	// window, in order to keep the gain within sane boundaries.

	dumbAssert(bytesAcked > 0)
	windowFactor := float64(minInt(bytesAcked, s.maxWindow)) / float64(maxInt(s.maxWindow, bytesAcked))
	delayFactor := offTarget / float64(target)
	scaledGain := maxCWndIncreaseBytesPerRTT * windowFactor * delayFactor

	// since maxCWndIncreaseBytesPerRTT is a cap on how much the window size (maxWindow)
	// may increase per RTT, we may not increase the window size more than that proportional
	// to the number of bytes that were acked, so that once one window has been acked (one rtt)
	// the increase limit is not exceeded
	// the +1. is to allow for floating point imprecision
	dumbAssert(scaledGain <= 1.+maxCWndIncreaseBytesPerRTT*float64(minInt(bytesAcked, s.maxWindow))/float64(maxInt(s.maxWindow, bytesAcked)))

	if scaledGain > 0 && currentMS-s.lastMaxedOutWindow > 300 {
		// if it was more than 300 milliseconds since we tried to send a packet
		// and stopped because we hit the max window, we're most likely rate
		// limited (which prevents us from ever hitting the window size)
		// if this is the case, we cannot let the maxWindow grow indefinitely
		scaledGain = 0
	}

	if int(scaledGain)+s.maxWindow < minWindowSize {
		s.maxWindow = minWindowSize
	} else {
		s.maxWindow += int(scaledGain)
	}

	// make sure that the congestion window is below max
	// make sure that we don't shrink our window too small
	s.maxWindow = clamp(s.maxWindow, minWindowSize, s.optSendBufferSize)

	// used in parse_log.py
	showDelayBase := uint32(50)
	if s.rtoHist.delayBase != 0 {
		showDelayBase = s.rtoHist.delayBase
	}
	s.logger.Debug("ledbat control applied",
		zap.Uint32("actual_delay", actualDelay),
		zap.Int32("our_delay", ourDelay/1000),
		zap.Uint32("their_delay", s.theirHist.getValue()/1000),
		zap.Int("off_target", int(offTarget)/1000),
		zap.Int("max_window", s.maxWindow),
		zap.Uint32("delay_base", s.outHist.delayBase),
		zap.Uint32("delay_sum", (uint32(ourDelay)+s.theirHist.getValue())/1000),
		zap.Int("target_delay", target/1000),
		zap.Int("acked_bytes", bytesAcked),
		zap.Int("cur_window", s.curWindow-bytesAcked),
		zap.Float64("scaled_gain", scaledGain),
		zap.Uint("rtt", s.rtt),
		zap.Int("rate", s.maxWindow*1000/int(showDelayBase)),
		zap.Int32("quota", s.sendQuota/100),
		zap.Int("wnduser", s.maxWindowUser),
		zap.Uint("rto", s.rto),
		zap.Uint("timeout", s.rtoTimeout-uint(currentMS)),
		zap.Uint64("get_microseconds", s.getMicroseconds()),
		zap.Uint16("cur_window_packets", s.curWindowPackets),
		zap.Int("packet_size", s.GetPacketSize()),
		zap.Uint32("their_delay_base", s.theirHist.delayBase),
		zap.Uint32("their_actual_delay", s.theirHist.delayBase+s.theirHist.getValue()))
}

func registerRecvPacket(conn *Socket, length int) {
	conn.stats.packetReceived(length)

	if length <= packetSizeMid {
		if length <= packetSizeEmpty {
			atomic.AddUint32(&globalStats.NumRawRecv[packetSizeEmptyBucket], 1)
		} else if length <= packetSizeSmall {
			atomic.AddUint32(&globalStats.NumRawRecv[packetSizeSmallBucket], 1)
		} else {
			atomic.AddUint32(&globalStats.NumRawRecv[packetSizeMidBucket], 1)
		}
	} else {
		if length <= packetSizeBig {
			atomic.AddUint32(&globalStats.NumRawRecv[packetSizeBigBucket], 1)
		} else {
			atomic.AddUint32(&globalStats.NumRawRecv[packetSizeHugeBucket], 1)
		}
	}
}

// GetPacketSize returns the max number of bytes of payload the µTP connection
// is allowed to send at a time.
func (s *Socket) GetPacketSize() int {
	headerSize := s.getHeaderSize()

	mtu := s.GetUDPMTU()

	if DynamicPacketSizeEnabled {
		maxPacketSize := getMaxPacketSize()
		return minInt(mtu-headerSize, maxPacketSize)
	}
	return mtu - headerSize
}

// processIncoming processes an incoming packet.
//
// syn is true if this is the first packet received. It will cut off parsing
// as soon as the header is done. Returns the amount of payload bytes
// successfully read from the packet.
func (mx *SocketMultiplexer) processIncoming(conn *Socket, packet []byte, syn bool, currentMS uint32) int {
	registerRecvPacket(conn, len(packet))

	conn.updateSendQuota(currentMS)

	var p packetHeader
	var err error
	if conn.version == 0 {
		var pf packetFormat
		err = pf.decodeFromBytes(packet)
		p = &pf
	} else {
		var pf1 packetFormatV1
		err = pf1.decodeFromBytes(packet)
		p = &pf1
	}
	if err != nil {
		panic(err)
	}

	packetEnd := len(packet)

	pkSeqNum := p.getSequenceNumber()
	pkAckNum := p.getAckNumber()
	pkFlags := p.getPacketType()

	if pkFlags >= stNumStates {
		return 0
	}

	conn.logger.Debug("Got incoming", zap.Stringer("flags", pkFlags), zap.Uint16("seq_nr", pkSeqNum), zap.Uint16("ack_nr", pkAckNum), zap.Stringer("state", conn.state), zap.Int8("version", conn.version), zap.Uint64("timestamp", p.getPacketTime()), zap.Uint32("reply_micro", p.getReplyMicro()))

	// mark receipt time
	receiptTime := conn.getMicroseconds()

	// RSTs are handled earlier, since the connID matches the send id not the recv id
	dumbAssert(pkFlags != stReset)

	// TODO: maybe send a STReset if we're in csReset?

	var selackPtr int

	// Unpack UTP packet options
	// Data pointer
	data := conn.getHeaderSize()
	if conn.getHeaderSize() > packetEnd {
		conn.logger.Debug("Invalid packet size (less than header size)")
		return 0
	}
	// Skip the extension headers
	extension := p.getExt()
	if extension != 0 {
		for {
			// Verify that the packet is valid.
			data += 2

			if (packetEnd-data) < 0 || (packetEnd-data) < int(packet[data-1]) {
				conn.logger.Debug("Invalid len of extensions")
				return 0
			}

			switch extension {
			case 1: // Selective Acknowledgment
				selackPtr = data
			case 2: // extension bits
				if packet[data-1] != 8 {
					conn.logger.Debug("Invalid len of extension bits header")
					return 0
				}
				copy(conn.extensions[:], packet[data:data+8])
				conn.logger.Debug("got extension bits", zap.ByteString("bits", conn.extensions[:]))
			}
			extension = int8(packet[data-2])
			data += int(packet[data-1])

			if extension == 0 {
				break
			}
		}
	}

	if conn.state == csSynSent {
		// if this is a syn-ack, initialize our ackNum
		// to match the sequence number we got from
		// the other end
		conn.ackNum = (pkSeqNum - 1) & seqNumberMask
	}

	currentMS = mx.getCurrentMS()
	conn.lastGotPacket = currentMS

	if syn {
		return 0
	}

	// seqNum is the number of packets past the expected
	// packet this is. ackNum is the last acked, seqNum is the
	// current. Subtracting 1 makes 0 mean "this is the next
	// expected packet".
	seqNum := (pkSeqNum - conn.ackNum - 1) & seqNumberMask

	// Getting an invalid sequence number?
	if seqNum >= reorderBufferMaxSize {
		if seqNum >= (seqNumberMask+1)-reorderBufferMaxSize && pkFlags != stState {
			conn.ackTime = currentMS + minUint32(conn.ackTime-currentMS, delayedAckTimeThreshold)
		}
		conn.logger.Debug("Got old Packet/Ack!", zap.Uint16("pkSeqNum", pkSeqNum), zap.Uint16("ackNum", conn.ackNum), zap.Uint16("seqNum", seqNum))
		return 0
	}

	// Process acknowledgment
	// acks is the number of packets that was acked
	acks := (pkAckNum - (conn.seqNum - 1 - conn.curWindowPackets)) & ackNumberMask

	// this happens when we receive an old ack nr
	if acks > conn.curWindowPackets {
		acks = 0
	}

	// if we get the same ackNum as in the last packet
	// increase the duplicateAck counter, otherwise reset
	// it to 0
	if conn.curWindowPackets > 0 {
		if pkAckNum == ((conn.seqNum-conn.curWindowPackets-1)&ackNumberMask) &&
			conn.curWindowPackets > 0 {
			// conn.duplicateAck++
		} else {
			conn.duplicateAck = 0
		}

		// TODO: if duplicateAck == duplicateAckBeforeResend
		// and fastResendSeqNum <= ackNum + 1
		//    resend ackNum + 1
	}

	// figure out how many bytes were acked
	var ackedBytes int

	// the minimum rtt of all acks
	// this is the upper limit on the delay we get back
	// from the other peer. Our delay cannot exceed
	// the rtt of the packet. If it does, clamp it.
	// this is done in applyLEDBATControl()
	var minRTT int64 = math.MaxInt64

	for i := 0; i < int(acks); i++ {
		seq := int(conn.seqNum) - int(conn.curWindowPackets) + i
		pkt := conn.outbuf.get(seq)
		if pkt == nil || pkt.transmissions == 0 {
			continue
		}
		dumbAssert(pkt.payload >= 0)
		ackedBytes += pkt.payload
		minRTT = minInt64(minRTT, int64(conn.getMicroseconds()-pkt.timeSent))
	}

	// count bytes acked by EACK
	if selackPtr != 0 {
		selackLen := int(packet[selackPtr-1])
		ackedBytes += conn.selectiveAckBytes(uint(pkAckNum+2)&ackNumberMask,
			packet[selackPtr:selackPtr+selackLen], &minRTT)
	}

	conn.logger.Debug("ack state", zap.Uint16("acks", acks), zap.Int("acked_bytes", ackedBytes), zap.Uint16("seq_nr", conn.seqNum), zap.Int("cur_window", conn.curWindow), zap.Uint16("cur_window_packets", conn.curWindowPackets), zap.Uint16("relative_seqnr", seqNum), zap.Int("max_window", conn.maxWindow), zap.Int64("min_rtt", minRTT/1000), zap.Uint("rtt", conn.rtt))

	packetTime := p.getPacketTime()
	conn.lastMeasuredDelay = currentMS

	// get delay in both directions
	// record the delay to report back
	var theirDelay uint32
	if packetTime != 0 {
		theirDelay = uint32(receiptTime - packetTime)
	}
	conn.replyMicro = theirDelay
	prevDelayBase := conn.theirHist.delayBase
	if theirDelay != 0 {
		conn.theirHist.addSample(theirDelay, currentMS)
	}

	// if their new delay base is less than their previous one
	// we should shift our delay base in the other direction in order
	// to take the clock skew into account
	if prevDelayBase != 0 && wrappingCompareLess(conn.theirHist.delayBase, prevDelayBase) {
		// never adjust more than 10 milliseconds
		if prevDelayBase-conn.theirHist.delayBase <= 10000 {
			conn.outHist.shift(prevDelayBase - conn.theirHist.delayBase)
		}
	}

	var actualDelay uint32
	replyMicro := p.getReplyMicro()
	if replyMicro != math.MaxInt32 {
		actualDelay = replyMicro
	}

	// if the actual delay is 0, it means the other end
	// hasn't received a sample from us yet, and doesn't
	// know what it is. We can't update out history unless
	// we have a true measured sample
	if actualDelay != 0 {
		conn.outHist.addSample(actualDelay, currentMS)
	}

	// if our new delay base is less than our previous one
	// we should shift the other end's delay base in the other
	// direction in order to take the clock skew into account
	// This is commented out because it creates bad interactions
	// with our adjustment in the other direction. We don't really
	// need our estimates of the other peer to be very accurate
	// anyway. The problem with shifting here is that we're more
	// likely shift it back later because of a low latency. This
	// second shift back would cause us to shift our delay base
	// which then gets into a death spiral of shifting delay bases
	/*
		prevDelayBase = conn.outHist.delayBase
		if prevDelayBase != 0 &&
			wrappingCompareLess(conn.ourHist.delayBase, prevDelayBase) {
			// never adjust more than 10 milliseconds
			if prevDelayBase - conn.ourHist.delayBase <= 10000 {
				conn.theirHist.Shift(prevDelayBase - conn.ourHist.delayBase)
			}
		}
	*/

	// if the delay estimate exceeds the RTT, adjust the baseDelay to
	// compensate
	if conn.outHist.getValue() > uint32(minRTT) {
		conn.outHist.shift(conn.outHist.getValue() - uint32(minRTT))
	}

	// only apply the congestion controller on acks
	// if we don't have a delay measurement, there's
	// no point in invoking the congestion control
	if actualDelay != 0 && ackedBytes >= 1 {
		conn.applyLEDBATControl(ackedBytes, actualDelay, minRTT, currentMS)
	}

	// sanity check, the other end should never ack packets
	// past the point we've sent
	if acks <= conn.curWindowPackets {
		conn.maxWindowUser = p.getWindowSize()

		// If max user window is set to 0, then we startup a timer
		// That will reset it to packetSize after 15 seconds.
		if conn.maxWindowUser == 0 {
			// Reset maxWindowUser to packetSize every 15 seconds.
			conn.zeroWindowTime = currentMS + 15000
		}

		// Respond to connect message
		// Switch to CONNECTED state.
		if conn.state == csSynSent {
			conn.state = csConnected
			conn.callbackTable.OnState(conn.userdata, StateConnect)

			// We've sent a fin, and everything was ACKed (including the FIN),
			// it's safe to destroy the socket. curWindowPackets == acks
			// means that this packet acked all the remaining packets that
			// were in-flight.
		} else if conn.state == csFinSent && conn.curWindowPackets == acks {
			conn.state = csDestroy
		}

		// Update fast resend counter
		if wrappingCompareLess(uint32(conn.fastResendSeqNum), uint32((pkAckNum+1)&ackNumberMask)) {
			conn.fastResendSeqNum = pkAckNum + 1
		}

		conn.logger.Debug("fast_resend update", zap.Uint16("fast_resend_seq_nr", conn.fastResendSeqNum))

		for i := uint16(0); i < acks; i++ {
			ackStatus := conn.ackPacket(conn.seqNum-conn.curWindowPackets, currentMS)
			// if ackStatus is 0, the packet was acked.
			// if ackStatus is 1, it means that the packet had already been acked
			// if it's 2, the packet has not been sent yet
			// We need to break this loop in the latter case. This could potentially
			// happen if we get an ackNum that does not exceed what we have stuffed
			// into the outgoing buffer, but does exceed what we have sent
			if ackStatus == 2 {
				conn.checkNoTransmissions()
				break
			}
			conn.curWindowPackets--
		}
		conn.checkNoWindow()

		// packets in front of this may have been acked by a
		// selective ack (EACK). Keep decreasing the window packet size
		// until we hit a packet that is still waiting to be acked
		// in the send queue
		// this is especially likely to happen when the other end
		// has the EACK send bug older versions of µTP had
		for conn.curWindowPackets > 0 && conn.outbuf.get(int(conn.seqNum)-int(conn.curWindowPackets)) == nil {
			conn.curWindowPackets--
		}
		conn.checkNoWindow()

		// this invariant should always be true
		dumbAssert(conn.curWindowPackets == 0 || conn.outbuf.get(int(conn.seqNum)-int(conn.curWindowPackets)) != nil)

		// flush Nagle
		if conn.curWindowPackets == 1 {
			pkt := conn.outbuf.get(int(conn.seqNum) - 1)
			// do we still have quota?
			if pkt.transmissions == 0 && conn.sendQuota/100 >= int32(pkt.length) {
				conn.sendPacket(pkt, currentMS)

				// No need to send another ack if there is nothing to reorder.
				if conn.reorderCount == 0 {
					conn.sentAck(currentMS)
				}
			}
		}

		// Fast timeout-retry
		if conn.fastTimeout {
			conn.logger.Debug("Fast timeout?", zap.Int("curWindow", conn.curWindow), zap.Uint16("seqNum", conn.seqNum), zap.Uint16("timeoutSeqNum", conn.timeoutSeqNum))
			// if the fastResendSeqNum is not pointing to the oldest outstanding packet, it suggests that we've already
			// resent the packet that timed out, and we should leave the fast-timeout mode.
			if ((conn.seqNum - conn.curWindowPackets) & ackNumberMask) != conn.fastResendSeqNum {
				conn.fastTimeout = false
			} else {
				// resend the oldest packet and increment fastResendSeqNum
				// to not allow another fast resend on it again
				pkt := conn.outbuf.get(int(conn.seqNum) - int(conn.curWindowPackets))
				if pkt != nil && pkt.transmissions > 0 {
					conn.logger.Debug("Packet fast timeout-retry", zap.Uint16("seqNum", conn.seqNum), zap.Uint16("curWindowPackets", conn.curWindowPackets))
					conn.stats.fastTransmitted()
					conn.fastResendSeqNum++
					conn.sendPacket(pkt, currentMS)
				}
			}
		}
	}

	// Process selective acknowledgement
	if selackPtr != 0 {
		selackLen := int(packet[selackPtr-1])
		conn.selectiveAck(pkAckNum+2, packet[selackPtr:selackPtr+selackLen], currentMS)
	}

	// this invariant should always be true
	dumbAssert(conn.curWindowPackets == 0 || conn.outbuf.get(int(conn.seqNum)-int(conn.curWindowPackets)) != nil)

	conn.logger.Debug("selacks processed", zap.Uint16("acks", acks), zap.Int("acked_bytes", ackedBytes), zap.Uint16("seq_nr", conn.seqNum), zap.Int("cur_window", conn.curWindow), zap.Uint16("cur_window_packets", conn.curWindowPackets), zap.Int32("quota", conn.sendQuota/100))

	// In case the ack dropped the current window below
	// the maxWindow size, Mark the socket as writable
	if conn.state == csConnectedFull && conn.isWritable(conn.GetPacketSize(), currentMS) {
		conn.state = csConnected
		conn.logger.Debug("Socket writable", zap.Int("max_window", conn.maxWindow), zap.Int("cur_window", conn.curWindow), zap.Int32("quota", conn.sendQuota/100), zap.Int("packet_size", conn.GetPacketSize()))
		conn.callbackTable.OnState(conn.userdata, StateWritable)
	}

	if pkFlags == stState {
		// This is a state packet only.
		return 0
	}

	// The connection is not in a state that can accept data?
	if conn.state != csConnected &&
		conn.state != csConnectedFull &&
		conn.state != csFinSent {
		return 0
	}

	// Is this a finalize packet?
	if pkFlags == stFin && !conn.gotFin {
		conn.logger.Debug("Got FIN", zap.Uint16("eof_pkt", pkSeqNum))
		conn.gotFin = true
		conn.eofPacket = pkSeqNum
		// at this point, it is possible for the
		// other end to have sent packets with
		// sequence numbers higher than seqNum.
		// if this is the case, our reorderCount
		// is out of sync. This case is dealt with
		// when we re-order and hit the eofPacket.
		// we'll just ignore any packets with
		// sequence numbers past this
	}

	// Getting an in-order packet?
	if seqNum == 0 {
		count := packetEnd - data
		if count > 0 && conn.state != csFinSent {
			conn.logger.Debug("Got Data", zap.Int("len", count), zap.Int("rb", conn.callbackTable.GetRBSize(conn.userdata)))
			// Post bytes to the upper layer
			conn.callbackTable.OnRead(conn.userdata, packet[data:data+count])
		}
		conn.ackNum++
		conn.bytesSinceAck += count

		// Check if the next packet has been received too, but waiting
		// in the reorder buffer.
		for {
			if conn.gotFin && conn.eofPacket == conn.ackNum {
				if conn.state != csFinSent {
					conn.state = csGotFin
					conn.rtoTimeout = uint(currentMS) + minUint(conn.rto*3, 60)

					conn.logger.Debug("Posting EOF")
					conn.callbackTable.OnState(conn.userdata, StateEOF)
				}

				// if the other end wants to close, ack immediately
				conn.sendAck(false, currentMS)

				// reorderCount is not necessarily 0 at this point.
				// even though it is most of the time, the other end
				// may have sent packets with higher sequence numbers
				// than what later end up being eofPacket
				// since we have received all packets up to eofPacket
				// just ignore the ones after it.
				conn.reorderCount = 0
			}

			// Quick get-out in case there is nothing to reorder
			if conn.reorderCount == 0 {
				break
			}

			// Check if there are additional buffers in the reorder buffers
			// that need delivery.
			b := conn.inbuf.get(int(conn.ackNum) + 1)
			if b == nil {
				break
			}
			conn.inbuf.put(int(conn.ackNum)+1, nil)
			if len(b) > 0 && conn.state != csFinSent {
				// Pass the bytes to the upper layer
				conn.callbackTable.OnRead(conn.userdata, b)
			}
			conn.ackNum++
			conn.bytesSinceAck += len(b)

			dumbAssert(conn.reorderCount > 0)
			conn.reorderCount--
		}

		// start the delayed ACK timer
		conn.ackTime = currentMS + minUint32(conn.ackTime-currentMS, delayedAckTimeThreshold)
	} else {
		// Getting an out of order packet.
		// The packet needs to be remembered and rearranged later.

		// if we have received a FIN packet, and the EOF-sequence number
		// is lower than the sequence number of the packet we just received
		// something is wrong.
		if conn.gotFin && pkSeqNum > conn.eofPacket {
			conn.logger.Debug("Got an invalid packet sequence number, past EOF", zap.Uint16("reorder_count", conn.reorderCount), zap.Int("len", packetEnd-data), zap.Int("rb", conn.callbackTable.GetRBSize(conn.userdata)))
			return 0
		}

		// if the sequence number is entirely off the expected
		// one, just drop it. We can't allocate buffer space in
		// the inbuf entirely based on untrusted input
		if seqNum > 0x3ff {
			conn.logger.Debug("Got an invalid packet sequence number, too far off", zap.Uint16("reorder_count", conn.reorderCount), zap.Int("len", packetEnd-data), zap.Int("rb", conn.callbackTable.GetRBSize(conn.userdata)))
			return 0
		}

		// we need to grow the circle buffer before we
		// check if the packet is already in here, so that
		// we don't end up looking at an older packet (since
		// the indices wraps around).
		conn.inbuf.ensureSize(int(pkSeqNum)+1, int(seqNum+1))

		// Has this packet already been received? (i.e. a duplicate)
		// If that is the case, just discard it.
		if conn.inbuf.get(int(pkSeqNum)) != nil {
			conn.stats.duplicateReceived()
			return 0
		}

		// Allocate memory to fit the packet that needs to re-ordered
		mem := make([]byte, packetEnd-data)
		copy(mem, packet[data:packetEnd])

		// Insert into reorder buffer and increment the count
		// of # of packets to be reordered.
		// we add one to seqNum in order to leave the last
		// entry empty, that way the assert in sendAck
		// is valid. we have to add one to seqNum too, in order
		// to make the circular buffer grow around the correct
		// point (which is conn.ackNum + 1).
		dumbAssert(conn.inbuf.get(int(pkSeqNum)) == nil)
		dumbAssert((int(pkSeqNum) & conn.inbuf.mask) != (int(conn.ackNum+1) & conn.inbuf.mask))
		conn.inbuf.put(int(pkSeqNum), mem)
		conn.reorderCount++

		conn.logger.Debug("Got out of order data", zap.Uint16("reorder_count", conn.reorderCount), zap.Int("len", packetEnd-data), zap.Int("rb", conn.callbackTable.GetRBSize(conn.userdata)))

		// Setup so the partial ACK message will get sent immediately.
		conn.ackTime = currentMS + minUint32(conn.ackTime-currentMS, 1)
	}

	// If ackTime or bytesSinceAck indicate that we need to send and ack, send one
	// here instead of waiting for the timer to trigger
	conn.logger.Debug("check if ack necessary", zap.Int("bytes_since_ack", conn.bytesSinceAck), zap.Uint32("ack_time", currentMS-conn.ackTime))
	if conn.state == csConnected || conn.state == csConnectedFull {
		if conn.bytesSinceAck > delayedAckByteThreshold ||
			(int)(currentMS-conn.ackTime) >= 0 {
			conn.sendAck(false, currentMS)
		}
	}
	return packetEnd - data
}

// this is at best a wild guess.
func detectVersion(packetBytes []byte) int8 {
	ver := int8(packetBytes[0] & 0xf)
	pType := packetFlag(packetBytes[0] >> 4)
	ext := packetBytes[1]
	if ver == 1 && pType < stNumStates && ext < 3 {
		return 1
	}
	return 0
}

func (mx *SocketMultiplexer) removeFromTracking(conn *Socket) {
	conn.logger.Debug("Killing socket")

	foundConn, ok := mx.socketMap[conn.addrString]
	if ok {
		dumbAssert(foundConn == conn)
		delete(mx.socketMap, conn.addrString)
	}

	if ok {
		conn.callbackTable.OnState(conn.userdata, StateDestroying)
		conn.SetCallbacks(nil, nil)
	}
}

// Create a µTP socket for communication with a peer at the given address.
func (mx *SocketMultiplexer) Create(sendToCB PacketSendCallback, sendToUserdata interface{}, addr *net.UDPAddr) (*Socket, error) {
	conn := &Socket{logger: mx.logger, packetTimeCallback: mx.packetTimeCallback}

	currentMS := conn.getCurrentMS()

	conn.SetCallbacks(nil, nil)
	conn.outHist.clear(currentMS)
	conn.theirHist.clear(currentMS)
	conn.rto = 3000
	conn.rttVariance = 800
	conn.seqNum = 1
	conn.ackNum = 0
	conn.maxWindowUser = 255 * packetSize
	conn.addr = addr
	conn.addrString = addr.String()
	conn.sendToCB = sendToCB
	conn.sendToUserdata = sendToUserdata
	conn.ackTime = currentMS + 0x70000000
	conn.lastGotPacket = currentMS
	conn.lastSentPacket = currentMS
	conn.lastMeasuredDelay = currentMS + 0x70000000
	conn.lastRWinDecay = int32(currentMS) - maxWindowDecay
	conn.lastSendQuota = int32(currentMS)
	conn.sendQuota = packetSize * 100
	conn.curWindowPackets = 0
	conn.fastResendSeqNum = conn.seqNum

	// default to version 1
	conn.SetSockOpt(SO_UTPVERSION, 1)

	// we need to fit one packet in the window
	// when we start the connection
	conn.maxWindow = conn.GetPacketSize()
	conn.state = csIdle

	conn.outbuf.mask = 15
	conn.inbuf.mask = 15

	conn.outbuf.elements = make([]*outgoingPacket, 16)
	conn.inbuf.elements = make([][]byte, 16)

	var err error

	_, found := mx.socketMap[conn.addrString]
	if found {
		// we can only have one Socket with a given same remote address
		// associated with our underlying PacketConn (as we can only identify/
		// distinguish connections by the 4-tuple of {local-ip, local-port,
		// remote-ip, remote-port}).
		err = &net.OpError{
			Op:   "create",
			Net:  "utp",
			Addr: conn.addr,
			Err:  syscall.EADDRINUSE,
		}
		conn = nil
	} else {
		mx.socketMap[conn.addrString] = conn
	}

	return conn, err
}

// SetCallbacks assigns a table of callbacks to this Socket. If any
// callbacks were set previously, they are discarded.
//
// This must be called before Connect, or mass hysteria could result.
func (s *Socket) SetCallbacks(funcs *CallbackTable, userdata interface{}) {
	if funcs == nil {
		funcs = &CallbackTable{}
	}
	if funcs.OnRead == nil {
		funcs.OnRead = noRead
	}
	if funcs.OnWrite == nil {
		funcs.OnWrite = noWrite
	}
	if funcs.OnState == nil {
		funcs.OnState = noState
	}
	if funcs.OnOverhead == nil {
		funcs.OnOverhead = noOverhead
	}
	if funcs.OnError == nil {
		funcs.OnError = noError
	}
	if funcs.GetRBSize == nil {
		funcs.GetRBSize = noRBSize
	}
	s.callbackTable = *funcs
	s.userdata = userdata
}

// SetSockOpt sets certain socket options on this µTP socket. This is intended
// to work similarly to the setsockopt() system call as used with UDP and TCP
// sockets, but it is not really the same thing; this only affects send and
// receive buffer sizes (SO_SNDBUF and SO_RCVBUF), and the µTP version to be
// used with this Socket (SO_UTPVERSION).
//
// For incoming connections, the µTP version is determined by the initiating
// host, so setting SO_UTPVERSION only has an effect for outgoing connections
// before Connect() has been called.
//
// The return value indicates whether or not the setting was valid.
func (s *Socket) SetSockOpt(opt, val int) bool {
	switch opt {
	case syscall.SO_SNDBUF:
		dumbAssert(val >= 1)
		s.optSendBufferSize = val
		return true
	case syscall.SO_RCVBUF:
		s.optRecvBufferSize = val
		return true
	case SO_UTPVERSION:
		dumbAssert(s.state == csIdle)
		if s.state != csIdle {
			// too late
			return false
		}
		if s.version == 1 && val == 0 {
			s.replyMicro = math.MaxInt32
			s.optRecvBufferSize = 200 * 1024
			s.optSendBufferSize = outgoingBufferMaxSize * packetSize
		} else if s.version == 0 && val == 1 {
			s.replyMicro = 0
			s.optRecvBufferSize = 3*1024*1024 + 512*1024
			s.optSendBufferSize = s.optRecvBufferSize
		}
		s.version = int8(val)
		return true
	}

	return false
}

// SetLogger sets a new logger for the socket. If logger is nil, a no-op
// logger will be used.
func (s *Socket) SetLogger(logger *zap.Logger) {
	s.logger = logger
}

// Connect initiates connection to the associated remote address. The remote
// address was given when the Socket was created.
//
// Connect should only be called on outgoing connections; Socket objects
// received by GotIncomingConnection callbacks are incoming connections and
// do not need to be "connected".
//
// There is no return value indication of success here; the connection will not
// be fully established until the Socket's OnStateChangeCallback is called with
// state=StateConnect or state=StateWritable.
func (s *Socket) Connect() {
	dumbAssert(s.state == csIdle)
	dumbAssert(s.curWindowPackets == 0)
	dumbAssert(s.outbuf.get(int(s.seqNum)) == nil)

	s.state = csSynSent

	currentMS := s.getCurrentMS()

	// Create and send a connect message
	connSeed := randomUint32()

	// we identify newer versions by setting the
	// first two bytes to 0x0001
	if s.version > 0 {
		connSeed &= 0xffff
	}

	// used in parse_log.py
	s.logger.Info("UTP_Connect", zap.Uint32("conn_seed", connSeed), zap.Int("packet_size", packetSize), zap.Duration("target_delay", congestionControlTarget*time.Microsecond), zap.Int("delay_history", curDelaySize), zap.Int("delay_base_history", delayBaseHistory))

	// Setup initial timeout timer.
	s.retransmitTimeout = 3000
	s.rtoTimeout = uint(currentMS) + s.retransmitTimeout
	s.lastReceiveWindow = s.getRcvWindow()

	s.connSeed = connSeed
	s.connIDRecv = connSeed
	s.connIDSend = connSeed + 1
	// if you need compatibility with 1.8.1, use this. it increases attackability though.
	// conn.seqNum = 1
	s.seqNum = uint16(randomUint32())

	// Create the connect packet.
	headerExtSize := s.getHeaderExtensionsSize()

	pkt := &outgoingPacket{
		payload:       0,
		transmissions: 0,
		length:        headerExtSize,
		needResend:    false,
		data:          make([]byte, headerExtSize),
	}

	var pa packetAckHeader
	if s.version == 0 {
		pfe := &packetFormatExtensions{}
		pa = pfe
	} else {
		pfe := &packetFormatExtensionsV1{}
		pa = pfe
	}
	pkt.header = pa
	pa.setVersion(s.version)

	// SYN packets are special, and have the receive ID in the connID field,
	// instead of connIDSend.
	pa.setConnID(s.connIDRecv)
	pa.setExt(2)
	pa.setPacketType(stSyn)
	pa.setWindowSize(s.lastReceiveWindow)
	pa.setSequenceNumber(s.seqNum)
	pa.setExtNext(0)
	pa.setExtLen(8)

	// s.logger.Debug("Sending connect", zap.Stringer("address", s.addr), zap.Uint32("conn_seed", connSeed))

	// Remember the message in the outgoing queue.
	s.outbuf.ensureSize(int(s.seqNum), int(s.curWindowPackets))
	s.outbuf.put(int(s.seqNum), pkt)
	s.seqNum++
	s.curWindowPackets++

	s.sendPacket(pkt, currentMS)
}

// IsIncomingUTP passes a UDP packet into the µTP processing layer. If the
// provided packet appears to be µTP traffic, it will be associated with the
// appropriate existing connection or it will begin negotiation of a new
// connection.
//
// The packet data should be passed in the 'buffer' parameter, and the source
// address that sent the data should be given in 'toAddr'.
//
// The returned boolean value indicates whether the provided packet data did
// indeed appear to be µTP traffic. If it was not, the caller might want to
// do something else with it.
//
// If a new connection is being initiated, a new Socket object will be created
// and passed to the provided GotIncomingConnection callback.
func (mx *SocketMultiplexer) IsIncomingUTP(incomingCB GotIncomingConnection, sendToCB PacketSendCallback, sendToUserdata interface{}, buffer []byte, toAddr *net.UDPAddr) bool {
	if len(buffer) < minInt(sizeofPacketFormat, sizeofPacketFormatV1) {
		mx.logger.Debug("recv packet too small", zap.Int("len", len(buffer)))
		return false
	}
	currentMS := mx.getCurrentMS()

	version := detectVersion(buffer)
	var ph packetHeader
	if version == 0 {
		ph = &packetFormat{}
	} else {
		ph = &packetFormatV1{}
	}
	err := ph.decodeFromBytes(buffer)
	if err != nil {
		mx.logger.Debug("recv packet too small for apparent format version", zap.Int("len", len(buffer)), zap.Int8("utp-version", version))
		return false
	}
	id := ph.getConnID()

	mx.logger.Debug("recv", zap.Int("len", len(buffer)), zap.Uint32("id", id), zap.Uint16("seq_nr", ph.getSequenceNumber()), zap.Uint16("ack_nr", ph.getAckNumber()))

	flags := ph.getPacketType()
	for _, conn := range mx.socketMap {
		// conn.logger.Debug("Examining Socket", zap.Stringer("source", conn.addr), zap.Stringer("dest", toAddr), zap.Uint32("conn_seed", conn.connSeed), zap.Uint32("conn_id_send", conn.connIDSend), zap.Uint32("conn_id_recv", conn.connIDRecv), zap.Uint32("id", id))
		if conn.addr.Port != toAddr.Port {
			continue
		}
		if !conn.addr.IP.Equal(toAddr.IP) {
			continue
		}

		if flags == stReset && (conn.connIDSend == id || conn.connIDRecv == id) {
			conn.logger.Debug("recv RST for existing connection")
			if conn.userdata == nil || conn.state == csFinSent {
				conn.state = csDestroy
			} else {
				conn.state = csReset
			}
			if conn.userdata != nil {
				conn.callbackTable.OnOverhead(conn.userdata, false, len(buffer)+conn.GetUDPOverhead(),
					CloseOverhead)
				socketErr := syscall.ECONNRESET
				if conn.state == csSynSent {
					socketErr = syscall.ECONNREFUSED
				}
				conn.callbackTable.OnError(conn.userdata, socketErr)
			}
			return true
		} else if flags != stSyn && conn.connIDRecv == id {
			mx.logger.Debug("recv processing")
			read := mx.processIncoming(conn, buffer, false, currentMS)
			if conn.userdata != nil {
				conn.callbackTable.OnOverhead(conn.userdata, false,
					(len(buffer)-read)+conn.GetUDPOverhead(),
					HeaderOverhead)
			}
			return true
		}
	}

	if flags == stReset {
		mx.logger.Debug("recv RST for unknown connection")
		return true
	}

	seqNum := ph.getSequenceNumber()
	if flags != stSyn {
		for i := 0; i < len(mx.rstInfo); i++ {
			if mx.rstInfo[i].connID != id {
				continue
			}
			if mx.rstInfo[i].addr.Port != toAddr.Port {
				continue
			}
			if !mx.rstInfo[i].addr.IP.Equal(toAddr.IP) {
				continue
			}
			if seqNum != mx.rstInfo[i].ackNum {
				continue
			}
			mx.rstInfo[i].timestamp = mx.getCurrentMS()
			mx.logger.Debug("recv not sending RST to non-SYN (stored)")
			return true
		}
		if len(mx.rstInfo) > rstInfoLimit {
			mx.logger.Debug("recv not sending RST to non-SYN (limit stored)", zap.Int("limit", len(mx.rstInfo)))
			return true
		}
		mx.logger.Debug("recv send RST to non-SYN", zap.Int("stored", len(mx.rstInfo)))
		mx.rstInfo = append(mx.rstInfo, rstInfo{})
		r := &mx.rstInfo[len(mx.rstInfo)-1]
		r.addr = toAddr
		r.connID = id
		r.ackNum = seqNum
		r.timestamp = mx.getCurrentMS()

		sendRST(mx.logger, sendToCB, sendToUserdata, toAddr, id, seqNum, uint16(randomUint32()), version)
		return true
	}

	if incomingCB != nil {
		mx.logger.Debug("Incoming connection", zap.Int8("utp-version", version))

		// Create a new UTP socket to handle this new connection
		conn, err := mx.Create(sendToCB, sendToUserdata, toAddr)
		if err != nil {
			mx.logger.Error("synchronous connections?", zap.Error(err))
			return true
		}
		// Need to track this value to be able to detect duplicate CONNECTs
		conn.connSeed = id
		// This is value that identifies this connection for them.
		conn.connIDSend = id
		// This is value that identifies this connection for us.
		conn.connIDRecv = id + 1
		conn.ackNum = seqNum
		conn.seqNum = uint16(randomUint32())
		conn.fastResendSeqNum = conn.seqNum

		conn.SetSockOpt(SO_UTPVERSION, int(version))
		conn.state = csConnected

		read := mx.processIncoming(conn, buffer, true, currentMS)

		conn.logger.Debug("recv send connect ACK")
		conn.sendAck(true, currentMS)

		incomingCB(sendToUserdata, conn)

		// we report overhead after incomingCB, because the callbacks are setup now
		if conn.userdata != nil {
			// SYN
			conn.callbackTable.OnOverhead(conn.userdata, false, len(buffer)-read+conn.GetUDPOverhead(),
				HeaderOverhead)
			// SYNACK
			conn.callbackTable.OnOverhead(conn.userdata, true, conn.GetOverhead(),
				AckOverhead)
		}
	}

	return true
}

// HandleICMP tells the µTP system to "process an ICMP received UDP packet."
// Why was a UDP packet received with ICMP? I don't know. It looks like the
// assumption here is that the ICMP packet is indicating an error with a sent
// UDP packet, so I suppose it is expecting ICMP messages like "Time exceeded"
// or "Destination unreachable".
func (mx *SocketMultiplexer) HandleICMP(buffer []byte, toAddr *net.UDPAddr) bool {
	// Want the whole packet so we have connection ID
	if len(buffer) < minInt(sizeofPacketFormat, sizeofPacketFormatV1) {
		return false
	}

	version := detectVersion(buffer)
	var ph packetHeader
	if version == 0 {
		ph = &packetFormat{}
	} else {
		ph = &packetFormatV1{}
	}
	err := ph.decodeFromBytes(buffer)
	if err != nil {
		// should be impossible because of the length check above
		panic(err)
	}
	id := ph.getConnID()

	for _, conn := range mx.socketMap {
		if conn.addr.IP.Equal(toAddr.IP) && conn.addr.Port == toAddr.Port && conn.connIDRecv == id {
			// Don't pass on errors for idle/closed connections
			if conn.state != csIdle {
				if conn.userdata == nil || conn.state == csFinSent {
					conn.logger.Debug("icmp packet causing socket destruction")
					conn.state = csDestroy
				} else {
					conn.state = csReset
				}
				if conn.userdata != nil {
					socketErr := syscall.ECONNRESET
					if conn.state == csSynSent {
						socketErr = syscall.ECONNREFUSED
					}
					conn.logger.Debug("icmp packet causing error on socket", zap.Int("errno", int(socketErr)), zap.Error(socketErr))
					conn.callbackTable.OnError(conn.userdata, socketErr)
				}
			}
			return true
		}
	}
	return false
}

// Write indicates that the specified amount of data is ready to be sent.
//
// The actual bytes to be sent are not passed to the socket at this point; the
// Socket depends on the calling code to keep track of the data buffers until
// it is ready to assemble packets. This minimizes the amount of copying that
// must be done, and gives the caller a great deal of control over how data is
// to be buffered.
//
// As much data as can be sent immediately (subject to window sizes) will be
// collected with the Socket's OnWriteCallback and passed on in packetized form
// to the Socket's PacketSendCallback before Write() returns. The caller may
// need to call Write() again, when the Socket is able to write more, with the
// new total amount of data ready to be sent. Subsequent calls to Write() are
// not additive; each call provides a new _total_ amount of data ready to be
// sent.
//
// The best way to know when the Socket is writable again appears to be by way
// of its OnStateChangeCallback. That callback can be called with state=
// StateWritable many times in succession. Therefore, it might be a good idea
// to call Write() every time the OnStateChangeCallback is called with
// StateWritable or StateConnected, if there actually is any data to send.
func (s *Socket) Write(numBytes int) bool {
	if s.state != csConnected {
		s.logger.Debug("Write n bytes = false (not csConnected)", zap.Int("n", numBytes))
		return false
	}
	param := numBytes

	currentMS := s.getCurrentMS()

	s.updateSendQuota(currentMS)

	// don't send unless it will all fit in the window
	maxPacketSize := s.GetPacketSize()
	numToSend := minInt(numBytes, maxPacketSize)
	for s.isWritable(numToSend, currentMS) {
		// Send an outgoing packet.
		// Also add it to the outgoing of packets that have been sent but not ACKed.

		if numToSend == 0 {
			s.logger.Debug("Write n bytes = true", zap.Int("n", param))
			return true
		}
		numBytes -= numToSend

		s.logger.Debug("Sending packet", zap.Uint16("seq_nr", s.seqNum), zap.Uint16("ack_nr", s.ackNum), zap.Int("cur_window", s.curWindow), zap.Int("max_window", s.maxWindow), zap.Int("max_window_user", s.maxWindowUser), zap.Int("rcv_win", s.lastReceiveWindow), zap.Int("size", numToSend), zap.Int32("quota", s.sendQuota/100), zap.Uint16("cur_window_packets", s.curWindowPackets))
		s.writeOutgoingPacket(numToSend, stData, currentMS)
		numToSend = minInt(numBytes, maxPacketSize)
	}

	// mark the socket as not being writable.
	s.state = csConnectedFull
	s.logger.Debug("Write n bytes = false", zap.Int("n", numBytes))
	return false
}

// RBDrained notifies the Socket that the read buffer has been exhausted. This
// prompts the sending of an immediate ACK if appropriate, so that more data
// can arrive as soon as possible.
func (s *Socket) RBDrained() {
	currentMS := s.getCurrentMS()
	rcvwin := s.getRcvWindow()

	if rcvwin > s.lastReceiveWindow {
		// If last window was 0 send ACK immediately, otherwise should set timer
		if s.lastReceiveWindow == 0 {
			s.sendAck(false, currentMS)
		} else {
			s.ackTime = currentMS + minUint32(s.ackTime-currentMS, delayedAckTimeThreshold)
		}
	}
}

// CheckTimeouts checks for timeout expiration on all current µTP sockets, and
// closes connections or causes state transitions as appropriate.
//
// It should be called by the governing code often enough that timeouts can be
// noticed reasonably quickly. The ideal call frequency will depend on your
// particular situation, but for development purposes, once every 50ms seems to
// work well.
func (mx *SocketMultiplexer) CheckTimeouts() {
	currentMS := mx.getCurrentMS()

	for i := 0; i < len(mx.rstInfo); i++ {
		if int(currentMS)-int(mx.rstInfo[i].timestamp) >= rstInfoTimeout {
			mx.rstInfo, _ = removeRstInfo(mx.rstInfo, i)
			i--
		}
	}

	socketList := make([]*Socket, 0, len(mx.socketMap))
	for _, conn := range mx.socketMap {
		socketList = append(socketList, conn)
	}
	for _, conn := range socketList {
		conn.checkTimeouts(currentMS)

		// (storj): this was added by thepaul without a terribly good
		// understanding of all the interconnected workings here. this _seems_
		// to avoid a messy situation where both sides send a FIN at about the
		// same time, and one of them ends up waiting in this csFinSent state
		// for 30s until it times out waiting for.. not sure what.
		if conn.state == csFinSent && conn.gotFin {
			conn.state = csDestroy
		}

		// Check if the object was deleted
		if conn.state == csDestroy {
			conn.logger.Debug("Destroying")
			mx.removeFromTracking(conn)
		}
	}
}

func (mx *SocketMultiplexer) getCurrentMS() uint32 {
	return uint32(mx.packetTimeCallback().Milliseconds())
}

// GetPeerName returns the UDP address of the remote end of the connection.
func (s *Socket) GetPeerName() *net.UDPAddr {
	return s.addr
}

// GetDelays returns the currently measured delay values for the connection.
func (s *Socket) GetDelays() (ours int32, theirs int32, age uint32) {
	currentMS := s.getCurrentMS()
	return int32(s.outHist.getValue()), int32(s.theirHist.getValue()), currentMS - s.lastMeasuredDelay
}

// GetGlobalStats returns a snapshot of the current GlobalStats counts.
func GetGlobalStats() GlobalStats {
	var r GlobalStats
	for i := range r.NumRawRecv {
		r.NumRawRecv[i] = atomic.LoadUint32(&globalStats.NumRawRecv[i])
	}
	for i := range r.NumRawSend {
		r.NumRawSend[i] = atomic.LoadUint32(&globalStats.NumRawSend[i])
	}
	return r
}

// Close closes the UTP socket.
//
// It is not valid to issue commands for this socket after it is closed.
// This does not actually destroy the socket until outstanding data is sent, at which
// point the socket will change to the StateDestroying state.
func (s *Socket) Close() error {
	if s.state == csDestroyDelay || s.state == csFinSent || s.state == csDestroy {
		return fmt.Errorf("can not close socket in state %s", s.state.String())
	}
	s.logger.Debug("Close", zap.Stringer("state", s.state))

	switch s.state {
	case csConnected, csConnectedFull:
		currentMS := s.getCurrentMS()
		s.state = csFinSent
		s.writeOutgoingPacket(0, stFin, currentMS)

	case csSynSent:
		currentMS := s.getCurrentMS()
		s.rtoTimeout = uint(currentMS) + minUint(s.rto*2, 60)
		fallthrough
	case csGotFin:
		s.state = csDestroyDelay

	default:
		s.state = csDestroy
	}
	return nil
}
