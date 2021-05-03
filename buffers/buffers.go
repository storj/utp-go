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
	IsClosedErr = errors.New("sync buffer is closed")
	ReaderAlreadyWaitingErr = errors.New("a reader is already waiting")
	WriterAlreadyWaitingErr = errors.New("a writer is already waiting")
)

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
}

func NewSyncBuffer(size int) *SyncCircularBuffer {
	return &SyncCircularBuffer{
		buffer: make([]byte, size),
	}
}

func (sb *SyncCircularBuffer) WaitForBytesChan(n int) (c <-chan struct{}, cancelWait func(), err error) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.readWaiter != nil {
		return nil, nil, ReaderAlreadyWaitingErr
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

func (sb *SyncCircularBuffer) WaitForSpaceChan(n int) (c <-chan struct{}, cancelWait func(), err error) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	return sb.waitForSpaceChan(n)
}

func (sb *SyncCircularBuffer) waitForSpaceChan(n int) (c <-chan struct{}, cancelWait func(), err error) {
	if sb.writeWaiter != nil {
		return nil, nil, WriterAlreadyWaitingErr
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

func (sb *SyncCircularBuffer) Append(ctx context.Context, data []byte) error {
	for {
		ok := sb.TryAppend(data)
		if ok {
			return nil
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
		case _, ok = <-waitForSpace:
			if !ok {
				return IsClosedErr
			}
		}
	}
}

func (sb *SyncCircularBuffer) Consume(ctx context.Context, data []byte) (n int, err error) {
	for {
		if n, ok := sb.TryConsume(data); ok {
			return n, nil
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
				return 0, IsClosedErr
			}
		}
	}
}

func (sb *SyncCircularBuffer) ConsumeFull(ctx context.Context, data []byte) error {
	for {
		ok := sb.TryConsumeFull(data)
		if ok {
			return nil
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
		case _, ok = <-waitChan:
			if !ok {
				return IsClosedErr
			}
		}
	}
}

func (sb *SyncCircularBuffer) TryAppend(data []byte) (ok bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

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

func (sb *SyncCircularBuffer) TryConsume(data []byte) (n int, ok bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	haveBytes := sb.spaceUsed()
	if haveBytes == 0 {
		return 0, false
	}
	if len(data) > haveBytes {
		// do a short read
		data = data[:haveBytes]
	}

	sb.popFromBuffer(data)
	return len(data), true
}

func (sb *SyncCircularBuffer) TryConsumeFull(data []byte) (ok bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()

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
		// model this as waiting for a write the size of the entire buffer
		var err error
		waitChan, _, err = sb.waitForSpaceChan(len(sb.buffer))
		if err != nil {
			// wait, how can something be waiting to write again
			panic(err)
		}
	}()
	<-waitChan
}

func (sb *SyncCircularBuffer) Close() {
	sb.lock.Lock()
	defer sb.lock.Unlock()

	if sb.readWaiter != nil {
		close(sb.readWaiter)
		sb.readWaiter = nil
	}
	if sb.writeWaiter != nil {
		close(sb.writeWaiter)
		sb.writeWaiter = nil
	}
}

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
