// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package buffers

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrIsClosed indicates a sync buffer is closed.
	ErrIsClosed = errors.New("sync buffer is closed")
	// ErrReaderAlreadyWaiting indicates a reader is already waiting.
	ErrReaderAlreadyWaiting = errors.New("a reader is already waiting")
	// ErrWriterAlreadyWaiting indicates a writer is already waiting.
	ErrWriterAlreadyWaiting = errors.New("a writer is already waiting")
)

// SyncCircularBuffer represents a synchronous circular buffer. It can be
// treated like a pipe: write to it, read from it, wait for data to be written,
// wait for data to be read ("consumed"), wait for space to become available
// for a write operation, and so on.
//
// Once data is consumed from the buffer, its space is released.
type SyncCircularBuffer struct {
	lock   sync.Mutex
	buffer []byte

	readWaiter       chan struct{}
	readSizeTrigger  int
	writeWaiter      chan struct{}
	writeSizeTrigger int

	start int
	end   int
	wraps bool

	closedForWrites bool
	closedForReads  bool
}

// NewSyncBuffer creates a new synchronous circular buffer.
func NewSyncBuffer(size int) *SyncCircularBuffer {
	return &SyncCircularBuffer{
		buffer: make([]byte, size),
	}
}

// WaitForBytesChan returns a channel that will be written to after n bytes have
// been written to the buffer and are available for reading. It is possible for
// another goroutine to read the bytes before the goroutine that is waiting on
// the channel, so the waiter may need to call this in a loop between read
// attempts.
//
// Only one goroutine can hold the read-waiting channel at a time.
func (sb *SyncCircularBuffer) WaitForBytesChan(n int) (c <-chan struct{}, cancelWait func(), err error) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.readWaiter != nil {
		return nil, nil, ErrReaderAlreadyWaiting
	}
	rw := make(chan struct{}, 1)
	if sb.spaceUsed() >= n {
		rw <- struct{}{}
		close(rw)
		return rw, func() {}, nil
	}
	sb.readWaiter = rw
	sb.readSizeTrigger = n
	return sb.readWaiter, func() { sb.cancelReadWait(rw) }, nil
}

// WaitForSpaceChan returns a channel that will be written to when there are n
// bytes of space available for writing. It is possible for another goroutine to
// write and use up those bytes before the goroutine that is waiting on the
// channel, so the waiter may need to call this in a loop between write
// attempts.
//
// Only one goroutine can hold the write-waiting channel at a time.
func (sb *SyncCircularBuffer) WaitForSpaceChan(n int) (c <-chan struct{}, cancelWait func(), err error) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.closedForWrites {
		return nil, nil, ErrIsClosed
	}
	return sb.waitForSpaceChan(n)
}

func (sb *SyncCircularBuffer) waitForSpaceChan(n int) (c <-chan struct{}, cancelWait func(), err error) {
	if sb.writeWaiter != nil {
		return nil, nil, ErrWriterAlreadyWaiting
	}
	ww := make(chan struct{}, 1)
	if sb.spaceAvailable() >= n {
		ww <- struct{}{}
		close(ww)
		return ww, func() {}, nil
	}
	sb.writeWaiter = ww
	sb.writeSizeTrigger = n
	return sb.writeWaiter, func() { sb.cancelWriteWait(ww) }, nil
}

func (sb *SyncCircularBuffer) cancelWriteWait(waitChan <-chan struct{}) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.writeWaiter != nil && sb.writeWaiter == waitChan {
		sb.writeWaiter = nil
	}
}

func (sb *SyncCircularBuffer) cancelReadWait(waitChan <-chan struct{}) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.readWaiter != nil && sb.readWaiter == waitChan {
		sb.readWaiter = nil
	}
}

// Append synchronously writes to the buffer, blocking if necessary until there
// is enough space in the buffer for the data to be written all at once.
func (sb *SyncCircularBuffer) Append(ctx context.Context, data []byte) error {
	for {
		if ok := sb.TryAppend(data); ok {
			return nil
		}
		if sb.closedForWrites {
			return ErrIsClosed
		}

		waitForSpace, cancelWait, err := sb.WaitForSpaceChan(len(data))
		if err != nil {
			// something is already waiting to append to this buffer
			return err
		}
		select {
		case <-ctx.Done():
			cancelWait()
			return ctx.Err()
		case _, ok := <-waitForSpace:
			if !ok {
				return ErrIsClosed
			}
		}
	}
}

// Consume synchronously reads from the buffer, blocking if necessary until
// there are bytes available to read. This may read less than len(data) bytes,
// if the full requested amount is not available.
func (sb *SyncCircularBuffer) Consume(ctx context.Context, data []byte) (n int, err error) {
	for {
		if n, ok := sb.TryConsume(data); ok {
			return n, nil
		}
		if sb.closedForReads {
			return 0, ErrIsClosed
		}
		waitChan, cancelWait, err := sb.WaitForBytesChan(1)
		if err != nil {
			// something is already waiting to read from this buffer
			return 0, err
		}
		select {
		case <-ctx.Done():
			cancelWait()
			return 0, ctx.Err()
		case _, ok := <-waitChan:
			if !ok {
				return 0, ErrIsClosed
			}
		}
	}
}

// ConsumeFull synchronously reads from the buffer, blocking if necessary until
// there are enough bytes available to read and fill the whole data slice.
func (sb *SyncCircularBuffer) ConsumeFull(ctx context.Context, data []byte) error {
	for {
		if ok := sb.TryConsumeFull(data); ok {
			return nil
		}
		if sb.closedForReads {
			return ErrIsClosed
		}

		waitChan, cancelWait, err := sb.WaitForBytesChan(len(data))
		if err != nil {
			// something is already waiting to read from this buffer
			return err
		}
		select {
		case <-ctx.Done():
			cancelWait()
			return ctx.Err()
		case _, ok := <-waitChan:
			if !ok {
				return ErrIsClosed
			}
		}
	}
}

// TryAppend tries to write to the buffer. Does not block.
//
// If there is not enough space to write data all at once, the buffer is
// untouched, and false is returned. Data is always written atomically as a
// whole slice.
func (sb *SyncCircularBuffer) TryAppend(data []byte) (ok bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.closedForWrites {
		return false
	}
	if sb.spaceAvailable() < len(data) {
		return false
	}

	if !sb.wraps {
		bytesToCopy := len(sb.buffer) - sb.end
		if len(data) < bytesToCopy {
			bytesToCopy = len(data)
		}
		copy(sb.buffer[sb.end:sb.end+bytesToCopy], data[:bytesToCopy])
		data = data[bytesToCopy:]
		sb.end += bytesToCopy
		if sb.end == len(sb.buffer) {
			sb.end = 0
			sb.wraps = true
		}
	}
	if sb.wraps && len(data) > 0 {
		if len(data) > sb.start-sb.end {
			panic(fmt.Sprintf("internal error: %d too big (start=%d, end=%d, size=%d, wraps=%v)", len(data), sb.start, sb.end, len(sb.buffer), sb.wraps))
		}
		copy(sb.buffer[sb.end:sb.end+len(data)], data)
		sb.end += len(data)
	}
	if sb.readWaiter != nil {
		if sb.spaceUsed() >= sb.readSizeTrigger {
			rw := sb.readWaiter
			sb.readWaiter = nil
			rw <- struct{}{}
			close(rw)
		}
	}
	return true
}

// TryConsume attempts to consume enough bytes from the buffer to fill the
// given byte slice. Does not block.
//
// If there were _any_ bytes available in the buffer, up to len(data), they
// are consumed and placed in the data buffer. The number of bytes consumed
// is returned as n, and ok is true.
//
// The ok value is returned as false when this buffer is closed for reads
// or when there are no bytes currently available.
func (sb *SyncCircularBuffer) TryConsume(data []byte) (n int, ok bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.closedForReads {
		return 0, false
	}
	haveBytes := sb.spaceUsed()
	if haveBytes == 0 {
		if sb.closedForWrites {
			return 0, true
		}
		return 0, false
	}
	if len(data) > haveBytes {
		// do a short read
		data = data[:haveBytes]
	}

	sb.popFromBuffer(data)
	return len(data), true
}

// TryConsumeFull attempts to consume enough bytes from the buffer to fill the
// given byte slice. Does not block.
//
// If there were enough bytes available to fill data, returns true. Otherwise,
// the data buffer is left untouched and false is returned.
func (sb *SyncCircularBuffer) TryConsumeFull(data []byte) (ok bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.closedForReads {
		return false
	}
	if sb.spaceUsed() < len(data) {
		return false
	}

	sb.popFromBuffer(data)
	return true
}

func (sb *SyncCircularBuffer) popFromBuffer(data []byte) {
	if sb.wraps {
		bytesToCopy := len(sb.buffer) - sb.start
		if len(data) < bytesToCopy {
			bytesToCopy = len(data)
		}
		copy(data[:bytesToCopy], sb.buffer[sb.start:sb.start+bytesToCopy])
		data = data[bytesToCopy:]
		sb.start += bytesToCopy
		if sb.start == len(sb.buffer) {
			sb.start = 0
			sb.wraps = false
		}
	}
	if !sb.wraps && len(data) > 0 {
		if len(data) > sb.end-sb.start {
			panic(fmt.Sprintf("internal error: don't have %d bytes avail (start=%d, end=%d, size=%d, wraps=%v)", len(data), sb.start, sb.end, len(sb.buffer), sb.wraps))
		}
		copy(data, sb.buffer[sb.start:sb.start+len(data)])
		sb.start += len(data)
	}
	if sb.writeWaiter != nil {
		if sb.spaceAvailable() >= sb.writeSizeTrigger {
			ww := sb.writeWaiter
			sb.writeWaiter = nil
			ww <- struct{}{}
			close(ww)
		}
	}
}

// lock must be held before calling.
func (sb *SyncCircularBuffer) cancelWaiters() {
	if sb.writeWaiter != nil {
		// cancel any pending write
		close(sb.writeWaiter)
		sb.writeWaiter = nil
	}
	if sb.readWaiter != nil {
		// cancel any pending reads
		close(sb.readWaiter)
		sb.readWaiter = nil
	}
}

// FlushAndClose flushes the buffer. This involves closing the buffer for
// writes, waiting until all data has been read, and then closing the
// buffer entirely. A goroutine waiting to write data to the buffer
// will have ErrIsClosed returned.
func (sb *SyncCircularBuffer) FlushAndClose() {
	var waitChan <-chan struct{}
	func() {
		sb.lock.Lock()
		defer sb.lock.Unlock()

		if sb.writeWaiter != nil {
			// cancel any pending write
			close(sb.writeWaiter)
			sb.writeWaiter = nil
		}
		sb.closedForWrites = true
		// (but we remain open for reads for now)

		// model this as waiting for space for a write the size of the entire buffer
		var err error
		waitChan, _, err = sb.waitForSpaceChan(len(sb.buffer))
		if err != nil {
			// wait, how can something be waiting to write again
			panic(err)
		}
	}()
	<-waitChan
	sb.Close()
}

// Clear clears the contents of a buffer, canceling any waiters and resetting
// the buffer back to its zero state.
func (sb *SyncCircularBuffer) Clear() {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	sb.cancelWaiters()

	sb.start = 0
	sb.end = 0
	sb.wraps = false
	sb.writeSizeTrigger = 0
	sb.readSizeTrigger = 0
	sb.closedForReads = false
	sb.closedForWrites = false

	for i := 0; i < len(sb.buffer); i++ {
		sb.buffer[i] = 0
	}
}

// Close closes the buffer for both reading and writing. Any waiters are
// canceled and will have ErrIsClosed returned. Further attempts to read
// or write will also have ErrIsClosed returned.
func (sb *SyncCircularBuffer) Close() {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	sb.cancelWaiters()

	sb.closedForReads = true
	sb.closedForWrites = true
}

// CloseForWrites closes the buffer for writes. Any goroutine waiting to write
// or attempting to write after this point will have ErrIsClosed returned. Reads
// will still be allowed.
func (sb *SyncCircularBuffer) CloseForWrites() {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	// cancel both types of waiter. Canceling writes because they will no longer
	// be allowed, and cancel reads because if any read doesn't already have
	// enough data, it will never succeed without writes.
	sb.cancelWaiters()

	sb.closedForWrites = true
	// but remain open for reads in case there is anything left in the buffer.
}

// SpaceAvailable returns a snapshot of the amount of space available in the
// buffer. Since the caller does not hold the lock, it is possible that the
// amount of space has changed by the time this function returns. The returned
// value should be treated as advisory only, unless the caller has other means
// of arranging for both reads and writes to be disallowed.
func (sb *SyncCircularBuffer) SpaceAvailable() int {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	return sb.spaceAvailable()
}

func (sb *SyncCircularBuffer) spaceAvailable() int {
	if sb.wraps {
		return sb.start - sb.end
	}
	return len(sb.buffer) - sb.end + sb.start
}

// SpaceUsed returns a snapshot of the amount of space used in the buffer.
// Since the caller does not hold the lock, it is possible that the amount of
// space used has changed by the time this function returns. The returned
// value should be treated as advisory only, unless the caller has other means
// of arranging for both reads and writes to be disallowed.
func (sb *SyncCircularBuffer) SpaceUsed() int {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	return sb.spaceUsed()
}

func (sb *SyncCircularBuffer) spaceUsed() int {
	if sb.wraps {
		return len(sb.buffer) + sb.end - sb.start
	}
	return sb.end - sb.start
}
