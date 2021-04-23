// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package utp

import (
	"fmt"
	"math/rand"
	"net"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testFlag int

const (
	useUTPv1 testFlag = 1 << iota
	simulatePacketLoss
	simulatePacketReorder
	heavyLoss
)

const bufferSizeMax = 1024 * 20

func TestWrappingCompareLess(t *testing.T) {
	assert.Equal(t, true, wrappingCompareLess(0xfffffff0, 0xffffffff))
	assert.Equal(t, false, wrappingCompareLess(0xffffffff, 0xfffffff0))
	assert.Equal(t, false, wrappingCompareLess(0xfff, 0xfffffff0))
	assert.Equal(t, true, wrappingCompareLess(0xfffffff0, 0xfff))
	assert.Equal(t, true, wrappingCompareLess(0x0, 0x1))
	assert.Equal(t, false, wrappingCompareLess(0x1, 0x0))
	assert.Equal(t, false, wrappingCompareLess(0x1, 0x1))
}

func TestTransfer(t *testing.T) {
	testTransfer(t, 0)
}

func TestTransferWithSimulatedPacketLoss(t *testing.T) {
	testTransfer(t, simulatePacketLoss)
}

func TestTransferWithSimulatedPacketLossAndReorder(t *testing.T) {
	testTransfer(t, simulatePacketLoss|simulatePacketReorder)
}

func TestTransferWithHeavySimulatedPacketLossAndReorder(t *testing.T) {
	testTransfer(t, simulatePacketLoss|simulatePacketReorder|heavyLoss)
}

func TestTransferWithSimulatedPacketReorder(t *testing.T) {
	testTransfer(t, simulatePacketReorder)
}

func TestUTPV1Transfer(t *testing.T) {
	testTransfer(t, useUTPv1)
}

func TestUTPV1TransferWithSimulatedPacketLoss(t *testing.T) {
	testTransfer(t, useUTPv1|simulatePacketLoss)
}

func TestUTPV1TransferWithSimulatedPacketLossAndReorder(t *testing.T) {
	testTransfer(t, useUTPv1|simulatePacketLoss|simulatePacketReorder)
}

func TestUTPV1TransferWithHeavySimulatedPacketLossAndReorder(t *testing.T) {
	testTransfer(t, useUTPv1|simulatePacketLoss|simulatePacketReorder|heavyLoss)
}

func TestUTPV1TransferWithSimulatedPacketReorder(t *testing.T) {
	testTransfer(t, useUTPv1|simulatePacketReorder)
}

type testLogger struct {
	t            testing.TB
	errorsLogged []string
}

func (tl *testLogger) Infof(tmpl string, args ...interface{}) {
	tl.t.Logf(tmpl, args...)
}

func (tl *testLogger) Debugf(string, ...interface{}) {}

func (tl *testLogger) Errorf(tmpl string, args ...interface{}) {
	msg := fmt.Sprintf(tmpl, args...)
	tl.errorsLogged = append(tl.errorsLogged, msg)
	tl.t.Logf(msg)
}

func testTransfer(t testing.TB, flags testFlag) {
	ts := newTestScenario(t)

	if flags&useUTPv1 != 0 {
		ts.senderSocket.sock.SetSockOpt(SO_UTPVERSION, 1)
	} else {
		ts.senderSocket.sock.SetSockOpt(SO_UTPVERSION, 0)
	}

	if flags&simulatePacketLoss != 0 {
		ts.sendManager.dropOnePacketEvery(33)
		ts.recvManager.dropOnePacketEvery(47)

		if flags&heavyLoss != 0 {
			ts.sendManager.dropOnePacketEvery(7)
			ts.recvManager.dropOnePacketEvery(13)
		}
	}

	if flags&simulatePacketReorder != 0 {
		ts.sendManager.reorderOnePacketEvery(27)
		ts.recvManager.reorderOnePacketEvery(23)
	}

	ts.senderSocket.sock.Connect()

	for i := 0; i < 1500; i++ {
		ts.tick(t)
		if ts.senderSocket.connected && ts.incomingSocket != nil {
			break
		}
	}
	require.NotNil(t, ts.incomingSocket)
	require.True(t, ts.senderSocket.connected)

	buffer := make([]byte, 16*1024)
	for i := 0; i < len(buffer); i++ {
		buffer[i] = byte(i & 0xff)
	}

	sendTarget := 10 * 16 * 1024

	written := ts.senderSocket.write(buffer)
	require.Greater(t, written, 0)

	for i := 0; i < 20000; i++ {
		ts.tick(t)
		if ts.incomingSocket.readBytes == sendTarget {
			break
		}
		if written < sendTarget && ts.senderSocket.writable {
			offset := written % (16 * 1024)
			written += ts.senderSocket.write(buffer[offset:])
		}
	}
	require.Equal(t, written, ts.incomingSocket.readBytes)

	ts.senderSocket.close()

	for i := 0; i < 1500; i++ {
		ts.tick(t)
		if !ts.incomingSocket.connected {
			break
		}
	}
	require.False(t, ts.incomingSocket.connected)

	ts.incomingSocket.close()

	// we know at this point that the sender sent all the data and the receiver got EOF.
	// shutdown might be disrupted by dropped packets, so ignore RSTs
	if flags&simulatePacketLoss != 0 {
		ts.senderSocket.ignoreReset = true
		ts.incomingSocket.ignoreReset = true
	}

	for i := 0; i < 1500; i++ {
		ts.tick(t)
		if ts.senderSocket.destroyed {
			break
		}
	}
	require.True(t, ts.senderSocket.destroyed)

	for i := 0; i < 1500; i++ {
		ts.tick(t)
		if ts.incomingSocket.destroyed {
			break
		}
	}
	require.True(t, ts.incomingSocket.destroyed)
}

type testScenario struct {
	sendManager udpManager
	recvManager udpManager

	incomingSocket *testUTPSocket
	senderSocket   *testUTPSocket
	mx             *SocketMultiplexer

	tickCounter int
	fakeTime    time.Duration
}

func newTestScenario(t testing.TB) *testScenario {
	scenario := &testScenario{}
	scenario.mx = NewSocketMultiplexer(&testLogger{t: t}, nil)
	scenario.mx.SetPacketTimeCallback(scenario.getTime)

	scenario.sendManager = udpManager{
		mx:       scenario.mx,
		myAddr:   net.UDPAddr{IP: net.IP{1, 2, 3, 4}, Port: 5},
		getTime:  scenario.getTime,
		receiver: &scenario.recvManager,
	}
	scenario.recvManager = udpManager{
		mx:       scenario.mx,
		myAddr:   net.UDPAddr{IP: net.IP{1, 2, 3, 4}, Port: 6},
		getTime:  scenario.getTime,
		receiver: &scenario.sendManager,
	}

	sock, err := scenario.mx.Create(testSendToProc, &scenario.sendManager, &scenario.recvManager.myAddr)
	require.NoError(t, err)
	scenario.senderSocket = newTestUTPSocket(t, sock)
	return scenario
}

func (ts *testScenario) getTime() time.Duration {
	return ts.fakeTime
}

func (ts *testScenario) tick(t testing.TB) {
	ts.tickCounter++
	if ts.tickCounter == 10 {
		ts.tickCounter = 0
		ts.mx.CheckTimeouts()
	}
	ts.sendManager.flush(t, ts)
	ts.recvManager.flush(t, ts)

	ts.fakeTime += 5 * time.Millisecond
}

type testUDPOutgoing struct {
	timestamp time.Duration
	addr      net.UDPAddr
	mem       []byte
}

type udpManager struct {
	mx     *SocketMultiplexer
	myAddr net.UDPAddr

	receiver    *udpManager
	lossCounter int
	lossEvery   int

	reorderCounter int
	reorderEvery   int

	sendBuffer []testUDPOutgoing

	getTime func() time.Duration
}

func (um *udpManager) bind(receiver *udpManager) {
	um.receiver = receiver
}

func (um *udpManager) send(buf []byte, addr *net.UDPAddr) {
	if um.lossEvery > 0 && um.lossCounter == um.lossEvery {
		um.lossCounter = 0
		return
	}
	um.lossCounter++

	delay := time.Millisecond * time.Duration(10+rand.Int()%30)

	um.reorderCounter++
	if um.reorderCounter >= um.reorderEvery && um.reorderEvery > 0 {
		delay = time.Millisecond * 9
		um.reorderCounter = 0
	}

	um.sendBuffer = append(um.sendBuffer, testUDPOutgoing{
		timestamp: um.getTime() + delay,
		addr:      *addr,
		mem:       buf,
	})
}

func (um *udpManager) dropOnePacketEvery(everyCount int) {
	um.lossEvery = everyCount
}

func (um *udpManager) reorderOnePacketEvery(everyCount int) {
	um.reorderEvery = everyCount
}

func (um *udpManager) flush(t testing.TB, ts *testScenario) {
	sort.Slice(um.sendBuffer, func(i, j int) bool {
		return packetTimestampLess(um.sendBuffer[i], um.sendBuffer[j])
	})

	for len(um.sendBuffer) > 0 {
		uo := um.sendBuffer[0]
		if uo.timestamp > um.getTime() {
			break
		}
		if um.receiver != nil {
			um.mx.IsIncomingUTP(func(_ interface{}, conn *Socket) {
				ts.incomingCallback(t, conn)
			}, testSendToProc, um.receiver, uo.mem, &um.myAddr)
		}
		um.sendBuffer = um.sendBuffer[1:]
	}
}

func packetTimestampLess(packet1 testUDPOutgoing, packet2 testUDPOutgoing) bool {
	return packet1.timestamp < packet2.timestamp
}

func (ts *testScenario) incomingCallback(t testing.TB, conn *Socket) {
	if ts.incomingSocket != nil {
		panic("incomingSocket already set!")
	}
	ts.incomingSocket = newTestUTPSocket(t, conn)
	ts.incomingSocket.connected = true
	ts.incomingSocket.writable = true
}

func testSendToProc(userdata interface{}, buf []byte, addr *net.UDPAddr) {
	um := userdata.(*udpManager)
	um.send(buf, addr)
}

type testUTPSocket struct {
	t testing.TB

	sock        *Socket
	buf         []byte
	readBytes   int
	connected   bool
	readable    bool
	writable    bool
	ignoreReset bool
	destroyed   bool
}

func newTestUTPSocket(t testing.TB, sock *Socket) *testUTPSocket {
	us := &testUTPSocket{
		t:    t,
		buf:  make([]byte, 0, bufferSizeMax),
		sock: sock,
	}
	sock.SetCallbacks(&CallbackTable{
		OnRead:  us.utpRead,
		OnWrite: us.onUTPWrite,
		OnState: us.onUTPState,
		OnError: us.onUTPError,
	}, us)
	return us
}

func (us *testUTPSocket) utpRead(userdata interface{}, buf []byte) {
	us.readBytes += len(buf)
	// TODO: assert the bytes we receive matches the pattern we sent
}

func (us *testUTPSocket) onUTPWrite(userdata interface{}, buf []byte) {
	copy(buf, us.buf[:len(buf)])
	us.buf = us.buf[len(buf):]
}

func (us *testUTPSocket) flushWrite() {
	if !us.writable {
		return
	}
	if len(us.buf) == 0 {
		return
	}

	us.writable = us.sock.Write(len(us.buf))
}

func (us *testUTPSocket) onUTPState(userdata interface{}, state State) {
	switch state {
	case StateConnect:
		require.False(us.t, us.destroyed)
		us.connected = true
		us.writable = true
		us.flushWrite()
	case StateWritable:
		require.True(us.t, us.connected)
		require.False(us.t, us.destroyed)
		us.writable = true
		us.flushWrite()
	case StateDestroying:
		require.False(us.t, us.destroyed)
		us.connected = false
		us.readable = false
		us.writable = false
		us.destroyed = true
		us.sock = nil
	case StateEOF:
		require.True(us.t, us.connected)
		require.False(us.t, us.destroyed)
		us.readable = false
		us.connected = false
	}
}

func (us *testUTPSocket) onUTPError(userdata interface{}, err error) {
	us.t.Logf("UTP ERROR for socket %s: %v", us.sock.addr.String(), err)
	if !us.ignoreReset || err == syscall.ECONNRESET {
		us.t.Fatalf("should not have gotten error: %v", err)
	}
	us.close()
}

func (us *testUTPSocket) write(buf []byte) int {
	require.LessOrEqual(us.t, len(us.buf), bufferSizeMax)
	free := bufferSizeMax - len(us.buf)
	toWrite := free
	if len(buf) < free {
		toWrite = len(buf)
	}
	if toWrite == 0 {
		return 0
	}
	us.buf = append(us.buf, buf[:toWrite]...)
	us.flushWrite()
	return toWrite
}

func (us *testUTPSocket) close() {
	err := us.sock.Close()
	require.NoError(us.t, err)
}
