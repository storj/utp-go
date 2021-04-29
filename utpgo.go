// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package utp

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"syscall"
	"time"

	"storj.io/utp-go/libutp"
)

// Buffer for data before it gets to µTP (there is another "send buffer" in
// the libutp code, but it is for managing flow control window sizes.
const (
	readBufferSize  = 200000
	writeBufferSize = 200000
)

//var noopLogger = libutp.NoopLogger{}
var noopLogger = DumbLogger{prefix: "utp"}

type DumbLogger struct {
	prefix string
}

func (d *DumbLogger) Infof(tmpl string, args ...interface{}) {
	log.Printf("[INFO : %s] %s", d.prefix, fmt.Sprintf(tmpl, args...))
}

func (d *DumbLogger) Debugf(tmpl string, args ...interface{}) {
	log.Printf("[DEBUG : %s] %s", d.prefix, fmt.Sprintf(tmpl, args...))
}

func (d *DumbLogger) Errorf(tmpl string, args ...interface{}) {
	log.Printf("[ERROR : %s] %s", d.prefix, fmt.Sprintf(tmpl, args...))
}

type Conn struct {
	utpSocket

	baseConn *libutp.Socket

	// set to true if the socket will close once the write buffer is empty
	willClose bool

	// set to true once the socket starts closing down
	closing bool

	// readBuffer tracks data that has been read on a particular Conn, but
	// not yet consumed by the application.
	//
	// When new data is added to the buffer, the state condition variable will
	// be signaled, to wake up any goroutines waiting for read data. This
	// should not be necessary when removing data from the buffer, because the
	// µTP receive window should provide the backpressure when the read buffer
	// gets full enough.
	readBuffer []byte

	// writeBuffer tracks data that needs to be sent on this Conn, which is
	// not yet ingested by µTP.
	//
	// When new data is added to the buffer, a send should also be done on the
	// corresponding socketManager's wantsToWrite channel, to wake up the
	// managing goroutine.
	//
	// When data is removed from the buffer for sending, the state condition
	// variable should be signaled to wake up any goroutines waiting to add to
	// the buffer because it was full.
	writeBuffer []byte
}

type Listener struct {
	utpSocket

	acceptChan <-chan *Conn
}

// utpSocket is shared functionality between Conn and Listener.
type utpSocket struct {
	localAddr net.UDPAddr

	// manager is shared by all sockets using the same local address
	// (for outgoing connections, only the one connection, but for incoming
	// connections, this includes all connections received by the associated
	// listening socket). It is reference-counted, and thus will only be
	// cleaned up entirely when the last related socket is closed.
	manager *socketManager

	// changes to baseConnClosed, opError, closing, connecting, or other state
	// variables in Conn or Listener (i.e., the read and write buffers, etc)
	// should all be protected with this lock.
	stateLock sync.Mutex
	stateCond sync.Cond
	// Set when baseConn has been entirely closed (got state=StateDestroying).
	// A broadcast on stateCond will be issued when this is set.
	baseConnClosed bool
	// Set when Close() has been called on this instance.
	closing bool
	// Once set, all further Write/Read operations should fail with this error.
	// Broadcasts on stateCond, readBuffer.cond, and writeBuffer.cond will be
	// issued when this is set.
	opError error
	// Set to true while waiting for a connection to complete (got
	// state=StateConnect). A broadcast on stateCond will be issued when this
	// is set.
	connecting bool
}

func Dial(network string, address string) (net.Conn, error) {
	switch network {
	case "utp", "utp4", "utp6":
		udpNetwork := "udp" + network[3:]
		rAddr, err := net.ResolveUDPAddr(udpNetwork, address)
		if err != nil {
			return nil, err
		}
		return DialUTP(network, nil, rAddr)
	}
	return net.Dial(network, address)
}

func DialUTP(network string, laddr, raddr *net.UDPAddr) (*Conn, error) {
	manager, err := newSocketManager(network, laddr, raddr)
	if err != nil {
		return nil, err
	}
	localAddr := manager.LocalAddr().(*net.UDPAddr)

	utpConn := &Conn{
		utpSocket: utpSocket{
			localAddr:  *localAddr,
			manager:    manager,
			connecting: true,
		},
		readBuffer:  make([]byte, 0, readBufferSize),
		writeBuffer: make([]byte, 0, writeBufferSize),
	}
	utpConn.stateCond.L = &utpConn.stateLock
	// thread-safe here, because no other goroutines could have a handle to
	// this mx yet.
	utpConn.baseConn, err = manager.mx.Create(packetSendCallback, manager, raddr)
	if err != nil {
		return nil, err
	}
	utpConn.baseConn.SetCallbacks(&libutp.CallbackTable{
		OnRead:    onReadCallback,
		OnWrite:   onWriteCallback,
		GetRBSize: getRBSizeCallback,
		OnState:   onStateCallback,
		OnError:   onErrorCallback,
	}, utpConn)

	manager.start()

	func() {
		// now that the manager's goroutines have started, we do need
		// concurrency protection
		manager.baseConnLock.Lock()
		defer manager.baseConnLock.Unlock()
		utpConn.baseConn.Connect()
	}()

	utpConn.stateLock.Lock()
	for utpConn.connecting {
		utpConn.stateCond.Wait()
	}
	err = utpConn.opError
	utpConn.stateLock.Unlock()

	if err != nil {
		_ = utpConn.Close()
		return nil, err
	}
	return utpConn, nil
}

func Listen(network string, addr string) (net.Listener, error) {
	switch network {
	case "utp", "utp4", "utp6":
		udpAddr, err := net.ResolveUDPAddr("udp"+network[3:], addr)
		if err != nil {
			return nil, err
		}
		return ListenUTP(network, udpAddr)
	}
	return net.Listen(network, addr)
}

func ListenUTP(network string, localAddr *net.UDPAddr) (*Listener, error) {
	manager, err := newSocketManager(network, localAddr, nil)
	if err != nil {
		return nil, err
	}
	localAddr = manager.LocalAddr().(*net.UDPAddr)
	utpListener := &Listener{
		utpSocket: utpSocket{
			localAddr: *localAddr,
			manager:   manager,
		},
		acceptChan: manager.acceptChan,
	}
	utpListener.stateCond.L = &utpListener.stateLock
	manager.start()
	return utpListener, nil
}

func (u *Conn) Close() error {
	// indicate our desire to close; once buffers are flushed, we can continue
	u.stateLock.Lock()
	u.willClose = true
	u.stateLock.Unlock()

	u.stateLock.Lock()
	alreadyClosedOrClosing := u.closing
	u.closing = true
	u.stateLock.Unlock()
	if alreadyClosedOrClosing {
		return errors.New("multiple calls to Close() not allowed")
	}
	err := func() error {
		// yes, even libutp.(*UTPSocket).Close() needs concurrency protection;
		// it may end up invoking callbacks
		u.manager.baseConnLock.Lock()
		defer u.manager.baseConnLock.Unlock()
		return u.baseConn.Close()
	}()
	socketCloseErr := u.utpSocket.Close()

	// if err was already set, this one is likely to be more helpful/interesting.
	if socketCloseErr != nil {
		err = socketCloseErr
	}
	return err
}

func (u *Conn) Read(buf []byte) (n int, err error) {
	u.stateLock.Lock()
	defer u.stateLock.Unlock()
	if u.opError != nil {
		return 0, u.opError
	}
	for len(u.readBuffer) < len(buf) {
		u.stateCond.Wait()
		if u.opError != nil {
			return 0, u.opError
		}
	}
	copy(buf, u.readBuffer[:len(buf)])
	remaining := len(u.readBuffer) - len(buf)
	copy(u.readBuffer[:remaining], u.readBuffer[len(buf):])
	u.readBuffer = u.readBuffer[:remaining]
	return len(buf), nil
}

func (u *Conn) Write(buf []byte) (n int, err error) {
	u.stateLock.Lock()
	defer u.stateLock.Unlock()
	if u.opError != nil {
		return 0, u.opError
	}
	for cap(u.writeBuffer)-len(u.writeBuffer) < len(buf) {
		u.stateCond.Wait()
		if u.opError != nil {
			return 0, u.opError
		}
	}
	oldLen := len(u.writeBuffer)
	u.writeBuffer = u.writeBuffer[:oldLen+len(buf)]
	copy(u.writeBuffer[oldLen:], buf)

	u.manager.wantsToWrite <- struct{}{}
	return len(buf), nil
}

func (u *Conn) LocalAddr() net.Addr {
	return u.Addr()
}

func (u *Conn) RemoteAddr() net.Addr {
	// thread-safe
	return u.baseConn.GetPeerName()
}

func (u *Conn) SetReadDeadline(t time.Time) error {
	// TODO
	return errors.New("not supported yet")
}

func (u *Conn) SetWriteDeadline(t time.Time) error {
	// TODO
	return errors.New("not supported yet")
}

func (u *Conn) SetDeadline(t time.Time) error {
	// TODO
	return errors.New("not supported yet")
}

var _ net.Conn = &Conn{}

func (u *Listener) AcceptUTP() (*Conn, error) {
	newConn, ok := <-u.acceptChan
	if ok {
		return newConn, nil
	}
	return nil, u.opError
}

func (u *Listener) Accept() (net.Conn, error) {
	return u.AcceptUTP()
}

func (u *Listener) Close() error {
	u.baseConnClosed = true
	return u.utpSocket.Close()
}

var _ net.Listener = &Listener{}

func (u *utpSocket) Close() (err error) {
	u.setOpError(net.ErrClosed)
	// wait for socket to enter StateDestroying
	u.stateLock.Lock()
	for !u.baseConnClosed {
		u.stateCond.Wait()
	}
	if u.manager != nil {
		err = u.manager.decrementReferences()
		u.manager = nil
	}
	u.stateLock.Unlock()
	return err
}

func (u *utpSocket) setOpError(err error) {
	if err == nil {
		return
	}
	u.stateLock.Lock()
	defer u.stateLock.Unlock()

	u.connecting = false
	// keep the first error if this is called multiple times
	if u.opError == nil {
		u.opError = err
		u.stateCond.Broadcast()
	}
}

func (u *utpSocket) Addr() net.Addr {
	localAddr := u.localAddr // copy
	return &localAddr
}

type receivedMessage struct {
	destAddr net.UDPAddr
	data     []byte
}

type socketManager struct {
	mx        *libutp.SocketMultiplexer
	udpSocket *net.UDPConn

	// this lock should be held when invoking any libutp functions or methods
	// that are not thread-safe or which themselves might invoke callbacks
	// (that is, nearly all libutp functions or methods). It can be assumed
	// that this lock is held in callbacks.
	baseConnLock sync.Mutex

	refCountLock sync.Mutex
	refCount     int

	// closeChan is a channel that should be closed once Conn.Close() has
	// been called. Indicates that the managing goroutine should clean up and
	// return the close error on closeErr.
	closeChan chan struct{}
	// closeErr is a channel on which the managing goroutine will return any
	// errors from a close operation when all is complete.
	closeErr chan error
	// set to true when closing down (set before closeChan is closed)
	closing bool

	// the udp reader goroutine will do a write to this channel when new
	// packets have been received for processing by the µTP machinery.
	incomingPacket chan receivedMessage
	// when processing of an item from incomingPacket is complete, a send will
	// be done on this channel in response.
	incomingPacketDone chan struct{}

	// an empty object is written to this whenever there are new packets to be
	// sent out (but only if the write queue was empty beforehand)
	wantsToWrite chan struct{}

	// to be allocated with a buffer the size of the intended backlog
	acceptChan chan *Conn

	// just a way to accumulate errors in sending or receiving on the UDP
	// socket; this may cause future Write/Read method calls to return the
	// error in the future
	socketErrors     []error
	socketErrorsLock sync.Mutex

	pollInterval time.Duration
}

const (
	defaultUTPConnBacklogSize = 5
)

func newSocketManager(network string, laddr, raddr *net.UDPAddr) (*socketManager, error) {
	switch network {
	case "utp", "utp4", "utp6":
	default:
		op := "dial"
		if raddr == nil {
			op = "listen"
		}
		return nil, &net.OpError{Op: op, Net: network, Source: laddr, Addr: raddr, Err: net.UnknownNetworkError(network)}
	}

	udpNetwork := "udp" + network[3:]
	// thread-safe here; don't need baseConnLock
	mx := libutp.NewSocketMultiplexer(&noopLogger, nil)

	udpSocket, err := net.ListenUDP(udpNetwork, laddr)
	if err != nil {
		return nil, err
	}

	sm := &socketManager{
		mx:                 mx,
		udpSocket:          udpSocket,
		refCount:           1,
		closeChan:          make(chan struct{}),
		closeErr:           make(chan error),
		incomingPacket:     make(chan receivedMessage),
		incomingPacketDone: make(chan struct{}),
		wantsToWrite:       make(chan struct{}, 1),
		acceptChan:         make(chan *Conn, defaultUTPConnBacklogSize),
		pollInterval:       5 * time.Millisecond,
	}
	return sm, nil
}

func (sm *socketManager) start() {
	go sm.socketManagement()
	go sm.udpMessageReceiver()
}

func (sm *socketManager) LocalAddr() net.Addr {
	return sm.udpSocket.LocalAddr()
}

func (sm *socketManager) socketManagement() {
	// when these deferreds are run, all attached Conn instances should be
	// closed already
	defer close(sm.wantsToWrite)
	defer close(sm.incomingPacketDone)

	timer := time.NewTimer(sm.pollInterval)
	defer timer.Stop()
	for {
		timer.Reset(sm.pollInterval)
		select {
		case <-sm.closeChan:
			sm.internalClose()
			return
		case packet := <-sm.incomingPacket:
			sm.processIncomingPacket(packet.data, &packet.destAddr)
		case <-timer.C:
		case <-sm.wantsToWrite:
		}
		sm.checkTimeouts()
	}
}

func (sm *socketManager) processIncomingPacket(data []byte, destAddr *net.UDPAddr) {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.IsIncomingUTP(gotIncomingConnectionCallback, packetSendCallback, sm, data, destAddr)
	sm.incomingPacketDone <- struct{}{}
}

func (sm *socketManager) checkTimeouts() {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.CheckTimeouts()
}

func (sm *socketManager) internalClose() {
	err := sm.udpSocket.Close()
	sm.mx = nil
	sm.closeErr <- err
	close(sm.closeErr)
	close(sm.acceptChan)
}

func (sm *socketManager) incrementReferences() {
	sm.refCountLock.Lock()
	sm.refCount++
	sm.refCountLock.Unlock()
}

func (sm *socketManager) decrementReferences() error {
	sm.refCountLock.Lock()
	defer sm.refCountLock.Unlock()
	sm.refCount--
	if sm.refCount == 0 {
		sm.closing = true
		close(sm.closeChan)
		return <-sm.closeErr
	}
	if sm.refCount < 0 {
		return errors.New("socketManager closed too many times!")
	}
	return nil
}

func (sm *socketManager) udpMessageReceiver() {
	defer close(sm.incomingPacket)

	// thread-safe; don't need baseConnLock
	maxSize := libutp.GetUDPMTU(sm.LocalAddr().(*net.UDPAddr))
	b := make([]byte, maxSize)
	for {
		n, addr, err := sm.udpSocket.ReadFromUDP(b)
		if err != nil {
			if sm.closing {
				return
			}
			sm.registerSocketError(err)
			continue
		}
		log.Printf("UDP RECEIVED %d bytes from %s to %s", n, addr.String(), sm.udpSocket.LocalAddr().String())
		msg := receivedMessage{
			destAddr: *addr,
			data:     b[:n],
		}
		select {
		case sm.incomingPacket <- msg:
			// wait until processing on that packet is done, so we can (a) keep
			// backpressure on incoming data, and (b) reuse the b buffer
			select {
			case <-sm.incomingPacketDone:
			case <-sm.closeChan:
				return
			}
		case <-sm.closeChan:
			return
		}
	}
}

func (sm *socketManager) registerSocketError(err error) {
	sm.socketErrorsLock.Lock()
	defer sm.socketErrorsLock.Unlock()
	log.Printf("SOCKET ERROR ON %s: %v", sm.udpSocket.LocalAddr(), err)
	sm.socketErrors = append(sm.socketErrors, err)
}

func gotIncomingConnectionCallback(userdata interface{}, newBaseConn *libutp.Socket) {
	sm := userdata.(*socketManager)
	remoteAddr := sm.udpSocket.RemoteAddr()
	if remoteAddr != nil {
		// this is not a listening-mode socket! we'll reject this spurious packet
		_ = newBaseConn.Close()
		return
	}
	sm.incrementReferences()

	newUTPConn := &Conn{
		utpSocket: utpSocket{
			localAddr: *sm.LocalAddr().(*net.UDPAddr),
			manager:   sm,
		},
		baseConn:    newBaseConn,
		readBuffer:  make([]byte, 0, readBufferSize),
		writeBuffer: make([]byte, 0, writeBufferSize),
	}
	newUTPConn.stateCond.L = &newUTPConn.stateLock
	newBaseConn.SetCallbacks(&libutp.CallbackTable{
		OnRead:    onReadCallback,
		OnWrite:   onWriteCallback,
		GetRBSize: getRBSizeCallback,
		OnState:   onStateCallback,
		OnError:   onErrorCallback,
	}, newUTPConn)
	select {
	case sm.acceptChan <- newUTPConn:
		// it's the socketManager's problem now
	default:
		// The accept backlog is full; drop this new connection. (This will
		// decref the socket manager as appropriate.)
		_ = newUTPConn.Close()
	}
}

func (u *Conn) addToReadBuffer(data []byte) error {
	u.stateLock.Lock()
	defer u.stateLock.Unlock()

	bufferRoom := cap(u.readBuffer) - len(u.readBuffer)
	if bufferRoom < len(data) {
		return fmt.Errorf("queue has size %d, %d bytes available, but appending %d bytes", len(u.readBuffer), bufferRoom, len(data))
	}
	oldLen := len(u.readBuffer)
	u.readBuffer = u.readBuffer[:oldLen+len(data)]
	copy(u.readBuffer[oldLen:], data)

	u.stateCond.Signal()
	return nil
}

func (u *Conn) readOutFromWriteBuffer(buf []byte) error {
	u.stateLock.Lock()
	defer u.stateLock.Unlock()

	if len(u.writeBuffer) < len(buf) {
		return fmt.Errorf("asked for %d bytes, but write buffer only has %d bytes available", len(buf), len(u.writeBuffer))
	}

	copy(buf, u.writeBuffer[:len(buf)])
	remainder := len(u.writeBuffer) - len(buf)
	copy(u.writeBuffer[:remainder], u.writeBuffer[len(buf):])
	u.writeBuffer = u.writeBuffer[:remainder]

	u.stateCond.Signal()
	return nil
}

func packetSendCallback(userdata interface{}, buf []byte, addr *net.UDPAddr) {
	sm := userdata.(*socketManager)
	log.Printf("UDP SENDING %d bytes from %s to %s", len(buf), sm.udpSocket.LocalAddr().String(), addr.String())
	_, err := sm.udpSocket.WriteToUDP(buf, addr)
	if err != nil {
		sm.registerSocketError(err)
	}
}

func onReadCallback(userdata interface{}, buf []byte) {
	u := userdata.(*Conn)
	err := u.addToReadBuffer(buf)
	if err != nil {
		// not enough room in the read buffer. perhaps here we should close
		// u.baseConn? or is libutp supposed to make this case impossible,
		// assuming the read buffer size is as expected?
		u.setOpError(fmt.Errorf("could not add to read buffer: %w", err))
	}
}

func onWriteCallback(userdata interface{}, buf []byte) {
	u := userdata.(*Conn)
	err := u.readOutFromWriteBuffer(buf)
	if err != nil {
		u.setOpError(fmt.Errorf("onWrite callback: %w", err))
	}
}

func getRBSizeCallback(userdata interface{}) int {
	u := userdata.(*Conn)
	u.stateLock.Lock()
	defer u.stateLock.Unlock()
	return len(u.readBuffer)
}

// the baseConnLock should already be held when this callback is entered
func onStateCallback(userdata interface{}, state libutp.State) {
	u := userdata.(*Conn)
	switch state {
	case libutp.StateConnect, libutp.StateWritable:
		u.stateLock.Lock()
		if u.connecting {
			u.connecting = false
			u.stateCond.Broadcast()
		}
		writeAmount := len(u.writeBuffer)
		u.stateLock.Unlock()
		if writeAmount > 0 {
			u.baseConn.Write(writeAmount)
		}
	case libutp.StateEOF:
		u.setOpError(&net.OpError{
			Op:     "?",
			Net:    u.localAddr.Network(),
			Source: u.LocalAddr(),
			Addr:   u.baseConn.GetPeerName(),
			Err:    syscall.ECONNRESET,
		})
	case libutp.StateDestroying:
		u.stateLock.Lock()
		u.baseConnClosed = true
		u.baseConn = nil
		u.stateCond.Broadcast()
		u.stateLock.Unlock()
	}
}

// the baseConnLock should already be held when this callback is entered
func onErrorCallback(userdata interface{}, err error) {
	u := userdata.(*Conn)
	u.setOpError(err)
}
