// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package utp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"

	"storj.io/utp-go/buffers"
	"storj.io/utp-go/libutp"
)

// Buffer for data before it gets to µTP (there is another "send buffer" in
// the libutp code, but it is for managing flow control window sizes).
const (
	readBufferSize  = 200000
	writeBufferSize = 200000
)

var noopLogger = logr.Discard()

// Addr represents a µTP address.
type Addr net.UDPAddr

// Network returns the network of the address ("utp").
func (a *Addr) Network() string { return "utp" }

// String returns the address formatted as a string.
func (a *Addr) String() string { return (*net.UDPAddr)(a).String() }

// Conn represents a µTP connection.
type Conn struct {
	utpSocket

	logger logr.Logger

	baseConn *libutp.Socket

	// set to true if the socket will close once the write buffer is empty
	willClose bool

	// set to true once the libutp-layer Close has been called
	libutpClosed bool

	// set to true when the socket has been closed by the remote side (or the
	// conn has experienced a timeout or other fatal error)
	remoteIsDone bool

	// set to true if a read call is pending
	readPending bool

	// set to true if a write call is pending
	writePending bool

	// closed when Close() is called
	closeChan chan struct{}

	// closed when baseConn has entered StateDestroying
	baseConnDestroyed chan struct{}

	// readBuffer tracks data that has been read on a particular Conn, but
	// not yet consumed by the application.
	readBuffer *buffers.SyncCircularBuffer

	// writeBuffer tracks data that needs to be sent on this Conn, which
	// has not yet been collected by µTP.
	writeBuffer *buffers.SyncCircularBuffer

	readDeadline  time.Time
	writeDeadline time.Time

	// Set to true while waiting for a connection to complete (got
	// state=StateConnect). The connectChan channel will be closed once this
	// is set.
	connecting  bool
	connectChan chan struct{}
}

// Listener represents a listening µTP socket.
type Listener struct {
	utpSocket

	acceptChan <-chan *Conn
}

// utpSocket is shared functionality between Conn and Listener.
type utpSocket struct {
	localAddr *net.UDPAddr

	// manager is shared by all sockets using the same local address
	// (for outgoing connections, only the one connection, but for incoming
	// connections, this includes all connections received by the associated
	// listening socket). It is reference-counted, and thus will only be
	// cleaned up entirely when the last related socket is closed.
	manager *socketManager

	// changes to encounteredError, manager, or other state variables in Conn
	// or Listener should all be protected with this lock. If it must be
	// acquired at the same time as manager.baseConnLock, the
	// manager.baseConnLock must be acquired first.
	stateLock sync.Mutex

	// Once set, all further Write/Read operations should fail with this error.
	encounteredError error
}

// Dial attempts to make an outgoing µTP connection to the given address. It is
// analogous to net.Dial.
func Dial(network, address string) (net.Conn, error) {
	return DialOptions(network, address)
}

// DialContext attempts to make an outgoing µTP connection to the given address.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return DialOptions(network, address, WithContext(ctx))
}

// DialOptions attempts to make an outgoing µTP connection to the given address
// with the given options.
func DialOptions(network, address string, options ...ConnectOption) (net.Conn, error) {
	switch network {
	case "utp", "utp4", "utp6":
	default:
		return nil, fmt.Errorf("network %s not supported", network)
	}
	rAddr, err := ResolveUTPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return DialUTPOptions(network, nil, rAddr, options...)
}

// DialUTP attempts to make an outgoing µTP connection with the given local
// and remote address endpoints. It is analogous to net.DialUDP.
func DialUTP(network string, localAddr, remoteAddr *Addr) (net.Conn, error) {
	return DialUTPOptions(network, localAddr, remoteAddr)
}

// DialUTPOptions attempts to make an outgoing µTP connection with the given
// local and remote address endpoints and the given options.
func DialUTPOptions(network string, localAddr, remoteAddr *Addr, options ...ConnectOption) (net.Conn, error) {
	s := utpDialState{
		logger:    noopLogger,
		ctx:       context.Background(),
		tlsConfig: nil,
	}
	for _, opt := range options {
		opt.apply(&s)
	}
	conn, err := dial(s.ctx, s.logger, network, localAddr, remoteAddr)
	if err != nil {
		return nil, err
	}
	if s.tlsConfig != nil {
		return tls.Client(conn, s.tlsConfig), nil
	}
	return conn, nil
}

func dial(ctx context.Context, logger logr.Logger, network string, localAddr, remoteAddr *Addr) (*Conn, error) {
	managerLogger := logger.WithValues("remote-addr", remoteAddr.String())
	manager, err := newSocketManager(managerLogger, network, (*net.UDPAddr)(localAddr), (*net.UDPAddr)(remoteAddr))
	if err != nil {
		return nil, err
	}
	localUDPAddr := manager.LocalAddr().(*net.UDPAddr)
	// different from managerLogger in case local addr interface and/or port
	// has been clarified
	connLogger := logger.WithValues("local-addr", localUDPAddr.String(), "remote-addr", remoteAddr.String(), "dir", "out")

	utpConn := &Conn{
		utpSocket: utpSocket{
			localAddr: localUDPAddr,
			manager:   manager,
		},
		logger:            connLogger.WithName("utp-conn"),
		connecting:        true,
		connectChan:       make(chan struct{}),
		closeChan:         make(chan struct{}),
		baseConnDestroyed: make(chan struct{}),
		readBuffer:        buffers.NewSyncBuffer(readBufferSize),
		writeBuffer:       buffers.NewSyncBuffer(writeBufferSize),
	}
	connLogger.V(10).Info("creating outgoing socket")
	// thread-safe here, because no other goroutines could have a handle to
	// this mx yet.
	utpConn.baseConn, err = manager.mx.Create(packetSendCallback, manager, (*net.UDPAddr)(remoteAddr))
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
	utpConn.baseConn.SetLogger(connLogger.WithName("utp-socket"))
	utpConn.baseConn.SetSockOpt(syscall.SO_RCVBUF, readBufferSize)

	manager.start()

	func() {
		// now that the manager's goroutines have started, we do need
		// concurrency protection
		manager.baseConnLock.Lock()
		defer manager.baseConnLock.Unlock()
		connLogger.V(10).Info("initiating libutp-level Connect()")
		utpConn.baseConn.Connect()
	}()

	select {
	case <-ctx.Done():
		_ = utpConn.Close()
		return nil, ctx.Err()
	case <-utpConn.connectChan:
	}

	// connection operation is complete, successful or not; record any error met
	utpConn.stateLock.Lock()
	err = utpConn.encounteredError
	utpConn.stateLock.Unlock()
	if err != nil {
		_ = utpConn.Close()
		return nil, utpConn.makeOpError("dial", err)
	}
	return utpConn, nil
}

// Listen creates a listening µTP socket on the local network address. It is
// analogous to net.Listen.
func Listen(network string, addr string) (net.Listener, error) {
	return ListenOptions(network, addr)
}

// ListenOptions creates a listening µTP socket on the local network address with
// the given options.
func ListenOptions(network, addr string, options ...ConnectOption) (net.Listener, error) {
	s := utpDialState{
		logger: noopLogger,
	}
	for _, opt := range options {
		opt.apply(&s)
	}
	switch network {
	case "utp", "utp4", "utp6":
	default:
		return nil, fmt.Errorf("network %s not supported", network)
	}
	udpAddr, err := ResolveUTPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	listener, err := listen(s.logger, network, udpAddr)
	if err != nil {
		return nil, err
	}
	if s.tlsConfig != nil {
		return tls.NewListener(listener, s.tlsConfig), nil
	}
	return listener, nil
}

// ListenUTP creates a listening µTP socket on the local network address. It is
// analogous to net.ListenUDP.
func ListenUTP(network string, localAddr *Addr) (*Listener, error) {
	return listen(noopLogger, network, localAddr)
}

// ListenUTPOptions creates a listening µTP socket on the given local network
// address and with the given options.
func ListenUTPOptions(network string, localAddr *Addr, options ...ConnectOption) (*Listener, error) {
	s := utpDialState{
		logger: noopLogger,
	}
	for _, opt := range options {
		opt.apply(&s)
	}
	return listen(s.logger, network, localAddr)
}

func listen(logger logr.Logger, network string, localAddr *Addr) (*Listener, error) {
	manager, err := newSocketManager(logger, network, (*net.UDPAddr)(localAddr), nil)
	if err != nil {
		return nil, err
	}
	udpLocalAddr := manager.LocalAddr().(*net.UDPAddr)
	utpListener := &Listener{
		utpSocket: utpSocket{
			localAddr: udpLocalAddr,
			manager:   manager,
		},
		acceptChan: manager.acceptChan,
	}
	manager.start()
	return utpListener, nil
}

type utpDialState struct {
	logger    logr.Logger
	ctx       context.Context
	tlsConfig *tls.Config
}

// ConnectOption is the interface which connection options should implement.
type ConnectOption interface {
	apply(s *utpDialState)
}

type optionLogger struct {
	logger logr.Logger
}

func (o *optionLogger) apply(s *utpDialState) {
	s.logger = o.logger
}

// WithLogger creates a connection option which specifies a logger to be
// attached to the connection. The logger will receive debugging messages
// about the socket.
func WithLogger(logger logr.Logger) ConnectOption {
	return &optionLogger{logger: logger}
}

type optionContext struct {
	ctx context.Context
}

func (o *optionContext) apply(s *utpDialState) {
	s.ctx = o.ctx
}

// WithContext creates a connection option which specifies a context to be
// attached to the connection. If the context is closed, the dial operation
// will be canceled.
func WithContext(ctx context.Context) ConnectOption {
	return &optionContext{ctx: ctx}
}

type optionTLS struct {
	tlsConfig *tls.Config
}

func (o *optionTLS) apply(s *utpDialState) {
	s.tlsConfig = o.tlsConfig
}

// WithTLS creates a connection option which specifies a TLS configuration
// structure to be attached to the connection. If specified, a TLS layer
// will be established on the connection before Dial returns.
func WithTLS(tlsConfig *tls.Config) ConnectOption {
	return &optionTLS{tlsConfig: tlsConfig}
}

// Close closes a connection.
func (c *Conn) Close() error {
	// indicate our desire to close; once buffers are flushed, we can continue
	c.stateLock.Lock()
	if c.willClose {
		c.stateLock.Unlock()
		return errors.New("multiple calls to Close() not allowed")
	}
	c.willClose = true
	c.stateLock.Unlock()

	// wait for write buffer to be flushed
	c.writeBuffer.FlushAndClose()

	// if there are still any blocked reads, shut them down
	c.readBuffer.Close()

	// close baseConn
	err := func() error {
		// yes, even libutp.(*UTPSocket).Close() needs concurrency protection;
		// it may end up invoking callbacks
		c.manager.baseConnLock.Lock()
		defer c.manager.baseConnLock.Unlock()
		c.logger.V(10).Info("closing baseConn")
		c.libutpClosed = true
		return c.baseConn.Close()
	}()

	// wait for socket to enter StateDestroying
	<-c.baseConnDestroyed

	c.setEncounteredError(net.ErrClosed)
	socketCloseErr := c.utpSocket.Close()

	// even if err was already set, this one is likely to be more helpful/interesting.
	if socketCloseErr != nil {
		err = socketCloseErr
	}
	return err
}

// SetLogger sets the logger to be used by a connection. The logger will receive
// debugging information about the socket.
func (c *Conn) SetLogger(logger logr.Logger) {
	c.baseConn.SetLogger(logger)
}

// Read reads from a Conn.
func (c *Conn) Read(buf []byte) (n int, err error) {
	return c.ReadContext(context.Background(), buf)
}

func (c *Conn) stateEnterRead() error {
	switch {
	case c.readPending:
		return buffers.ErrReaderAlreadyWaiting
	case c.willClose:
		return c.makeOpError("read", net.ErrClosed)
	case c.remoteIsDone && c.readBuffer.SpaceUsed() == 0:
		return c.makeOpError("read", c.encounteredError)
	}
	c.readPending = true
	return nil
}

// ReadContext reads from a Conn.
func (c *Conn) ReadContext(ctx context.Context, buf []byte) (n int, err error) {
	c.stateLock.Lock()
	encounteredErr := c.encounteredError
	deadline := c.readDeadline
	err = c.stateEnterRead()
	c.stateLock.Unlock()

	if err != nil {
		return 0, err
	}
	defer func() {
		c.stateLock.Lock()
		defer c.stateLock.Unlock()
		c.readPending = false
	}()
	if !deadline.IsZero() {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	for {
		var ok bool
		n, ok = c.readBuffer.TryConsume(buf)
		if ok {
			if n == 0 {
				return 0, io.EOF
			}
			c.manager.baseConnLock.Lock()
			c.baseConn.RBDrained()
			c.manager.baseConnLock.Unlock()
			return n, nil
		}
		if encounteredErr != nil {
			return 0, c.makeOpError("read", encounteredErr)
		}
		waitChan, cancelWait, err := c.readBuffer.WaitForBytesChan(1)
		if err != nil {
			return 0, err
		}
		select {
		case <-ctx.Done():
			cancelWait()
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				// transform deadline error to os.ErrDeadlineExceeded as per
				// net.Conn specification
				err = c.makeOpError("read", os.ErrDeadlineExceeded)
			}
			return 0, err
		case <-c.closeChan:
			cancelWait()
			return 0, c.makeOpError("read", net.ErrClosed)
		case <-waitChan:
		}
	}
}

// Write writes to a Conn.
func (c *Conn) Write(buf []byte) (n int, err error) {
	return c.WriteContext(context.Background(), buf)
}

// WriteContext writes to a Conn.
func (c *Conn) WriteContext(ctx context.Context, buf []byte) (n int, err error) {
	c.stateLock.Lock()
	if c.writePending {
		c.stateLock.Unlock()
		return 0, buffers.ErrWriterAlreadyWaiting
	}
	c.writePending = true
	deadline := c.writeDeadline
	c.stateLock.Unlock()

	if err != nil {
		if errors.Is(err, io.EOF) {
			// remote side closed connection cleanly, and µTP in/out streams
			// are not independently closeable. Doesn't make sense to return
			// an EOF from a Write method, so..
			err = c.makeOpError("write", syscall.ECONNRESET)
		} else if errors.Is(err, net.ErrClosed) {
			err = c.makeOpError("write", net.ErrClosed)
		}
		return 0, err
	}

	defer func() {
		c.stateLock.Lock()
		defer c.stateLock.Unlock()
		c.writePending = false
	}()

	if !deadline.IsZero() {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	for {
		c.stateLock.Lock()
		willClose := c.willClose
		remoteIsDone := c.remoteIsDone
		encounteredError := c.encounteredError
		c.stateLock.Unlock()
		if willClose {
			return 0, c.makeOpError("write", net.ErrClosed)
		}
		if remoteIsDone {
			return 0, c.makeOpError("write", encounteredError)
		}

		if ok := c.writeBuffer.TryAppend(buf); ok {
			// make sure µTP knows about the new bytes. this might be a bit
			// confusing, but it doesn't matter if other writes occur between
			// the TryAppend() above and the acquisition of the baseConnLock
			// below. All that matters is that (a) there is at least one call
			// to baseConn.Write scheduled to be made after this point (without
			// undue blocking); (b) baseConnLock is held when that Write call
			// is made; and (c) the amount of data in the write buffer does not
			// decrease between the SpaceUsed() call and the start of the next
			// call to onWriteCallback.
			func() {
				c.manager.baseConnLock.Lock()
				defer c.manager.baseConnLock.Unlock()

				amount := c.writeBuffer.SpaceUsed()
				c.logger.V(10).Info("informing libutp layer of data for writing", "len", amount)
				c.baseConn.Write(amount)
			}()

			return len(buf), nil
		}

		waitChan, cancelWait, err := c.writeBuffer.WaitForSpaceChan(len(buf))
		if err != nil {
			if errors.Is(err, buffers.ErrIsClosed) {
				err = c.makeOpError("write", c.encounteredError)
			}
			return 0, err
		}

		// couldn't write the data yet; wait until we can, or until we hit the
		// timeout, or until the conn is closed.
		select {
		case <-ctx.Done():
			cancelWait()
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				// transform deadline error to os.ErrDeadlineExceeded as per
				// net.Conn specification
				err = c.makeOpError("write", os.ErrDeadlineExceeded)
			}
			return 0, err
		case <-c.closeChan:
			cancelWait()
			return 0, c.makeOpError("write", net.ErrClosed)
		case <-waitChan:
		}
	}
}

// RemoteAddr returns the address of the connection peer.
func (c *Conn) RemoteAddr() net.Addr {
	// GetPeerName is thread-safe
	return (*Addr)(c.baseConn.GetPeerName())
}

// SetReadDeadline sets a read deadline for future read operations.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.readDeadline = t
	return nil
}

// SetWriteDeadline sets a write deadline for future write operations.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.writeDeadline = t
	return nil
}

// SetDeadline sets a deadline for future read and write operations.
func (c *Conn) SetDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.writeDeadline = t
	c.readDeadline = t
	return nil
}

func (c *Conn) makeOpError(op string, err error) error {
	opErr := c.utpSocket.makeOpError(op, err).(*net.OpError) //nolint: errorlint
	opErr.Source = opErr.Addr
	opErr.Addr = c.RemoteAddr()
	return opErr
}

var _ net.Conn = &Conn{}

// AcceptUTPContext accepts a new µTP connection on a listening socket.
func (l *Listener) AcceptUTPContext(ctx context.Context) (*Conn, error) {
	select {
	case newConn, ok := <-l.acceptChan:
		if ok {
			return newConn, nil
		}
		err := l.encounteredError
		if err == nil {
			err = l.makeOpError("accept", net.ErrClosed)
		}
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// AcceptUTP accepts a new µTP connection on a listening socket.
func (l *Listener) AcceptUTP() (*Conn, error) {
	return l.AcceptUTPContext(context.Background())
}

// Accept accepts a new µTP connection on a listening socket.
func (l *Listener) Accept() (net.Conn, error) {
	return l.AcceptUTP()
}

// AcceptContext accepts a new µTP connection on a listening socket.
func (l *Listener) AcceptContext(ctx context.Context) (net.Conn, error) {
	return l.AcceptUTPContext(ctx)
}

// Close closes a Listener.
func (l *Listener) Close() error {
	return l.utpSocket.Close()
}

// Addr returns the local address of a Listener.
func (l *Listener) Addr() net.Addr {
	return l.utpSocket.LocalAddr()
}

var _ net.Listener = &Listener{}

func (u *utpSocket) makeOpError(op string, err error) error {
	return &net.OpError{
		Op:     op,
		Net:    "utp",
		Source: nil,
		Addr:   u.LocalAddr(),
		Err:    err,
	}
}

func (u *utpSocket) Close() (err error) {
	u.stateLock.Lock()
	if u.manager != nil {
		err = u.manager.decrementReferences()
		u.manager = nil
	}
	u.stateLock.Unlock()
	return err
}

func (c *Conn) setEncounteredError(err error) {
	if err == nil {
		return
	}
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	// keep the first error if this is called multiple times
	if c.encounteredError == nil {
		c.encounteredError = err
	}
	if c.connecting {
		c.connecting = false
		close(c.connectChan)
	}
}

func (u *utpSocket) LocalAddr() net.Addr {
	return (*Addr)(u.localAddr)
}

type socketManager struct {
	mx        *libutp.SocketMultiplexer
	logger    logr.Logger
	udpSocket *net.UDPConn

	// this lock should be held when invoking any libutp functions or methods
	// that are not thread-safe or which themselves might invoke callbacks
	// (that is, nearly all libutp functions or methods). It can be assumed
	// that this lock is held in callbacks.
	baseConnLock sync.Mutex

	refCountLock sync.Mutex
	refCount     int

	// cancelManagement is a cancel function that should be called to close
	// down the socket management goroutines. The main managing goroutine
	// should clean up and return any close error on closeErr.
	cancelManagement func()
	// closeErr is a channel on which the managing goroutine will return any
	// errors from a close operation when all is complete.
	closeErr chan error

	// to be allocated with a buffer the size of the intended backlog. There
	// can be at most one utpSocket able to receive on this channel (one
	// Listener for any given UDP socket).
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

func newSocketManager(logger logr.Logger, network string, localAddr, remoteAddr *net.UDPAddr) (*socketManager, error) {
	switch network {
	case "utp", "utp4", "utp6":
	default:
		op := "dial"
		if remoteAddr == nil {
			op = "listen"
		}
		return nil, &net.OpError{Op: op, Net: network, Source: localAddr, Addr: remoteAddr, Err: net.UnknownNetworkError(network)}
	}

	udpNetwork := "udp" + network[3:]
	udpSocket, err := net.ListenUDP(udpNetwork, localAddr)
	if err != nil {
		return nil, err
	}

	// thread-safe here; don't need baseConnLock
	mx := libutp.NewSocketMultiplexer(logger.WithName("mx").WithValues("local-addr", udpSocket.LocalAddr().String()), nil)

	sm := &socketManager{
		mx:           mx,
		logger:       logger.WithName("manager").WithValues("local-addr", udpSocket.LocalAddr().String()),
		udpSocket:    udpSocket,
		refCount:     1,
		closeErr:     make(chan error),
		acceptChan:   make(chan *Conn, defaultUTPConnBacklogSize),
		pollInterval: 5 * time.Millisecond,
	}
	return sm, nil
}

func (sm *socketManager) start() {
	ctx, cancel := context.WithCancel(context.Background())
	sm.cancelManagement = cancel

	managementLabels := pprof.Labels(
		"name", "socket-management", "udp-socket", sm.udpSocket.LocalAddr().String())
	receiverLabels := pprof.Labels(
		"name", "udp-receiver", "udp-socket", sm.udpSocket.LocalAddr().String())
	go func() {
		pprof.Do(ctx, managementLabels, sm.socketManagement)
	}()
	go func() {
		pprof.Do(ctx, receiverLabels, sm.udpMessageReceiver)
	}()
}

func (sm *socketManager) LocalAddr() net.Addr {
	return sm.udpSocket.LocalAddr()
}

func (sm *socketManager) socketManagement(ctx context.Context) {
	timer := time.NewTimer(sm.pollInterval)
	defer timer.Stop()
	for {
		timer.Reset(sm.pollInterval)
		select {
		case <-ctx.Done():
			// at this point, all attached Conn instances should be
			// closed already
			sm.internalClose()
			return
		case <-timer.C:
		}
		sm.checkTimeouts()
	}
}

func (sm *socketManager) processIncomingPacket(data []byte, destAddr *net.UDPAddr) {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.IsIncomingUTP(gotIncomingConnectionCallback, packetSendCallback, sm, data, destAddr)
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
		sm.logger.V(1).Info("closing socketManager")
		sm.cancelManagement()
		return <-sm.closeErr
	}
	if sm.refCount < 0 {
		return errors.New("socketManager closed too many times")
	}
	return nil
}

func (sm *socketManager) udpMessageReceiver(ctx context.Context) {
	// thread-safe; don't need baseConnLock for GetUDPMTU
	bufSize := libutp.GetUDPMTU(sm.LocalAddr().(*net.UDPAddr))
	// It turns out GetUDPMTU is frequently wrong, and when it gives us a lower
	// number than the real MTU, and the other side is sending bigger packets,
	// then we end up not being able to read the full packets. Start with a
	// receive buffer twice as big as we thought we might need, and increase it
	// further from there if needed.
	bufSize *= 2
	sm.logger.V(0).Info("udp message receiver started", "receive-buf-size", bufSize)
	b := make([]byte, bufSize)
	for {
		n, _, flags, addr, err := sm.udpSocket.ReadMsgUDP(b, nil)
		if err != nil {
			if ctx.Err() != nil {
				// we expect an error here; the socket has been closed; it's fine
				return
			}
			sm.registerSocketError(err)
			continue
		}
		if flags&msg_trunc != 0 {
			// we didn't get the whole packet. don't pass it on to µTP; it
			// won't recognize the truncation and will pretend like that's
			// all the data there is. let the packet loss detection stuff
			// do its part instead.
			continue
		}
		sm.logger.V(10).Info("udp received bytes", "len", n)
		sm.processIncomingPacket(b[:n], addr)
	}
}

func (sm *socketManager) registerSocketError(err error) {
	sm.socketErrorsLock.Lock()
	defer sm.socketErrorsLock.Unlock()
	sm.logger.Error(err, "socket error")
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

	connLogger := sm.logger.WithName("utp-socket").WithValues("dir", "in", "remote-addr", newBaseConn.GetPeerName().String())
	newUTPConn := &Conn{
		utpSocket: utpSocket{
			localAddr: sm.LocalAddr().(*net.UDPAddr),
			manager:   sm,
		},
		logger:            connLogger,
		baseConn:          newBaseConn,
		closeChan:         make(chan struct{}),
		baseConnDestroyed: make(chan struct{}),
		readBuffer:        buffers.NewSyncBuffer(readBufferSize),
		writeBuffer:       buffers.NewSyncBuffer(writeBufferSize),
	}
	newBaseConn.SetCallbacks(&libutp.CallbackTable{
		OnRead:    onReadCallback,
		OnWrite:   onWriteCallback,
		GetRBSize: getRBSizeCallback,
		OnState:   onStateCallback,
		OnError:   onErrorCallback,
	}, newUTPConn)
	sm.logger.V(1).Info("accepted new connection", "remote-addr", newUTPConn.RemoteAddr().String())
	newUTPConn.baseConn.SetSockOpt(syscall.SO_RCVBUF, readBufferSize)
	select {
	case sm.acceptChan <- newUTPConn:
		// it's the socketManager's problem now
	default:
		sm.logger.Info("dropping new connection because full backlog", "remote-addr", newUTPConn.RemoteAddr())
		// The accept backlog is full; drop this new connection. We can't call
		// (*Conn).Close() from here, because the baseConnLock is already held.
		// Fortunately, most of the steps done there aren't necessary here
		// because we have never exposed this instance to the user.
		_ = newUTPConn.baseConn.Close()
		// This step will decref the socketManager back to where it was before
		// this instance was created.
		_ = newUTPConn.manager.decrementReferences()
		newUTPConn.manager = nil
	}
}

func packetSendCallback(userdata interface{}, buf []byte, addr *net.UDPAddr) {
	sm := userdata.(*socketManager)
	sm.logger.V(10).Info("udp sending bytes", "len", len(buf))
	_, err := sm.udpSocket.WriteToUDP(buf, addr)
	if err != nil {
		sm.registerSocketError(err)
	}
}

func onReadCallback(userdata interface{}, buf []byte) {
	c := userdata.(*Conn)
	c.stateLock.Lock()
	c.stateDebugLogLocked("entering onReadCallback", "got-bytes", len(buf))
	isClosing := c.willClose
	c.stateLock.Unlock()

	if isClosing {
		// the local side has closed the connection; they don't want any additional data
		return
	}

	if ok := c.readBuffer.TryAppend(buf); !ok {
		// I think this should not happen; the flow control mechanism should
		// keep us from getting more data than the receive buffer can hold.
		used := c.readBuffer.SpaceUsed()
		avail := c.readBuffer.SpaceAvailable()
		c.logger.Error(nil, "receive buffer overflow", "buffer-size", used+avail, "buffer-holds", c.readBuffer.SpaceUsed(), "new-data", len(buf))
		panic("receive buffer overflow")
	}
	c.stateDebugLog("finishing onReadCallback")
}

func onWriteCallback(userdata interface{}, buf []byte) {
	c := userdata.(*Conn)
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	c.stateDebugLogLocked("entering onWriteCallback", "accepting-bytes", len(buf))
	ok := c.writeBuffer.TryConsumeFull(buf)
	if !ok {
		// I think this should not happen; this callback should only be called
		// with data less than or equal to the number we pass in with
		// libutp.(*Socket).Write(). That gets passed in under the
		// baseConnLock, and this gets called under that same lock, so it also
		// shouldn't be possible for something to pull data from the write
		// buffer between that point and this point.
		panic("send buffer underflow")
	}
	c.stateDebugLogLocked("finishing onWriteCallback")
}

func getRBSizeCallback(userdata interface{}) int {
	c := userdata.(*Conn)
	return c.readBuffer.SpaceUsed()
}

func (c *Conn) onConnectOrWritable(state libutp.State) {
	c.stateLock.Lock()
	c.stateDebugLogLocked("entering onConnectOrWritable", "libutp-state", state)
	if c.connecting {
		c.connecting = false
		close(c.connectChan)
	}
	c.stateLock.Unlock()

	if writeAmount := c.writeBuffer.SpaceUsed(); writeAmount > 0 {
		c.logger.V(10).Info("initiating write to libutp layer", "len", writeAmount)
		c.baseConn.Write(writeAmount)
	} else {
		c.logger.V(10).Info("nothing to write")
	}

	c.stateDebugLog("finishing onConnectOrWritable")
}

func (c *Conn) onConnectionFailure(err error) {
	c.stateDebugLog("entering onConnectionFailure", "err-text", err.Error())

	// mark EOF as encountered error, so that it gets returned from
	// subsequent Read calls
	c.setEncounteredError(err)
	// clear out write buffer; we won't be able to send it now. If a call
	// to Close() is already waiting, we don't need to make it wait any
	// longer
	c.writeBuffer.Close()
	// this will allow any pending reads to complete (as short reads)
	c.readBuffer.CloseForWrites()

	c.stateDebugLog("finishing onConnectionFailure")
}

// the baseConnLock should already be held when this callback is entered.
func onStateCallback(userdata interface{}, state libutp.State) {
	c := userdata.(*Conn)
	switch state {
	case libutp.StateConnect, libutp.StateWritable:
		c.onConnectOrWritable(state)
	case libutp.StateEOF:
		c.onConnectionFailure(io.EOF)
	case libutp.StateDestroying:
		close(c.baseConnDestroyed)
	}
}

// This could be ECONNRESET, ECONNREFUSED, or ETIMEDOUT.
//
// the baseConnLock should already be held when this callback is entered.
func onErrorCallback(userdata interface{}, err error) {
	c := userdata.(*Conn)
	c.logger.Error(err, "onError callback from libutp layer")

	// we have to treat this like a total connection failure
	c.onConnectionFailure(err)

	// and we have to cover a corner case where this error was encountered
	// _during_ the libutp Close() call- in this case, libutp would sit
	// forever and never get to StateDestroying, so we have to prod it again.
	if c.libutpClosed {
		if err := c.baseConn.Close(); err != nil {
			c.logger.Error(err, "error from libutp layer Close()")
		}
	}
}

// ResolveUTPAddr resolves a µTP address. It is analogous to net.ResolveUDPAddr.
func ResolveUTPAddr(network, address string) (*Addr, error) {
	switch network {
	case "utp", "utp4", "utp6":
		udpNetwork := "udp" + network[3:]
		udpAddr, err := net.ResolveUDPAddr(udpNetwork, address)
		if err != nil {
			return nil, err
		}
		return (*Addr)(udpAddr), nil
	}
	return nil, net.UnknownNetworkError(network)
}
