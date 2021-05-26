// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

//+build utpstatedebug

package utp

import "fmt"

// ReadSideState represents the possible read-side states that a conn can be in.
type ReadSideState int

const (
	ReadConnecting             ReadSideState = iota // connecting
	ReadIdleConn                                    // nothing to do but wait
	ReadHasReadData                                 // have some data from remote waiting on user to read it
	ReadIdleClosing                                 // closed on local side, waiting for ack from remote
	ReadAwaitingData                                // user wants to read data, but no data available yet
	ReadAwaitingAccept                              // some data is available for a pending read call, but it hasn't accepted the data yet
	ReadDrainingReads                               // have some unread data from remote, also closed by remote
	ReadDrainingAwaitingAccept                      // have some unread data from remote, also closed by remote, also there is a pending read call that hasn't accepted its data yet
	ReadClosedByPeer                                // closed on remote side, waiting for close on local
	ReadClosed                                      // all shut down and done
)

// WriteSideState represents the possible write-side states that a Conn can be in.
type WriteSideState int

const (
	WriteConnecting     WriteSideState = iota // connecting
	WriteIdleConn                             // nothing to do but wait
	WriteCallPending                          // a write call has been made but the data is not queued yet
	WriteHasWriteData                         // there is some data in the write queue for sending
	WriteDataAndPending                       // there is data in the write queue and a pending write call wants to add more
	WriteIdleClosing                          // closed on local side, waiting for ack from remote
	WriteClosedByPeer                         // closed on remote side, waiting for close on local
	WriteDrainingWrites                       // closed on local side, but we have some data to be sent first
	WriteClosed                               // all shut down and done
)

func (st ReadSideState) String() string {
	switch st {
	case ReadConnecting:
		return "READ-CONNECTING"
	case ReadIdleConn:
		return "READ-IDLE"
	case ReadHasReadData:
		return "READ-HAS-READ-DATA"
	case ReadIdleClosing:
		return "READ-IDLE-CLOSING"
	case ReadAwaitingData:
		return "READ-AWAITING-DATA"
	case ReadAwaitingAccept:
		return "READ-AWAITING-ACCEPT"
	case ReadDrainingReads:
		return "READ-DRAINING-READS"
	case ReadDrainingAwaitingAccept:
		return "READ-DRAINING-AWAITING-ACCEPT"
	case ReadClosedByPeer:
		return "READ-CLOSED-BY-PEER"
	case ReadClosed:
		return "READ-CLOSED"
	}
	return fmt.Sprintf("UNKNOWN READ STATE %d", st)
}

func (st WriteSideState) String() string {
	switch st {
	case WriteConnecting:
		return "WRITE-CONNECTING"
	case WriteCallPending:
		return "WRITE-CALL-PENDING"
	case WriteIdleConn:
		return "WRITE-IDLE"
	case WriteHasWriteData:
		return "WRITE-HAS-WRITE-DATA"
	case WriteDataAndPending:
		return "WRITE-DATA-AND-PENDING"
	case WriteIdleClosing:
		return "WRITE-IDLE-CLOSING"
	case WriteClosedByPeer:
		return "WRITE-CLOSED-BY-PEER"
	case WriteDrainingWrites:
		return "WRITE-DRAINING-WRITES"
	case WriteClosed:
		return "WRITE-CLOSED"
	}
	return fmt.Sprintf("UNKNOWN WRITE STATE %d", st)
}

func (c *Conn) getReadStatus() ReadSideState {
	readBufferEmpty := (c.readBuffer.SpaceUsed() == 0)
	switch {
	case c.connecting:
		return ReadConnecting
	case c.willClose && c.remoteIsDone:
		return ReadClosed
	case c.willClose:
		return ReadIdleClosing
	case !c.readPending && readBufferEmpty && !c.remoteIsDone:
		return ReadIdleConn
	case !c.readPending && !readBufferEmpty && !c.remoteIsDone:
		return ReadHasReadData
	case c.readPending && readBufferEmpty && !c.remoteIsDone:
		return ReadAwaitingData
	case c.readPending && !readBufferEmpty && !c.remoteIsDone:
		return ReadAwaitingAccept
	case !c.readPending && readBufferEmpty && c.remoteIsDone:
		return ReadClosedByPeer
	case !c.readPending && !readBufferEmpty && c.remoteIsDone:
		return ReadDrainingReads
	case c.readPending && !readBufferEmpty && c.remoteIsDone:
		return ReadDrainingAwaitingAccept
	}
	c.logger.Error(nil, "unexpected read state",
		"connecting", c.connecting,
		"willClose", c.willClose,
		"remoteIsDone", c.remoteIsDone,
		"readPending", c.readPending,
		"readBufferEmpty", readBufferEmpty)
	return ReadSideState(-1)
}

func (c *Conn) getWriteStatus() WriteSideState {
	writeBufferEmpty := (c.writeBuffer.SpaceUsed() == 0)
	switch {
	case c.connecting:
		return WriteConnecting
	case c.remoteIsDone && c.willClose:
		return WriteClosed
	case c.remoteIsDone && !c.willClose:
		return WriteClosedByPeer
	case c.willClose && !c.writePending && !writeBufferEmpty:
		return WriteDrainingWrites
	case c.willClose && !c.writePending && writeBufferEmpty:
		return WriteIdleClosing
	case !c.willClose && c.writePending && writeBufferEmpty:
		return WriteCallPending
	case !c.willClose && c.writePending && !writeBufferEmpty:
		return WriteDataAndPending
	case !c.willClose && !c.writePending && writeBufferEmpty:
		return WriteIdleConn
	case !c.willClose && !c.writePending && !writeBufferEmpty:
		return WriteHasWriteData
	}
	c.logger.Error(nil, "unexpected write state",
		"connecting", c.connecting,
		"willClose", c.willClose,
		"remoteIsDone", c.remoteIsDone,
		"writePending", c.writePending,
		"writeBufferEmpty", writeBufferEmpty)
	return WriteSideState(-1)
}

func (c *Conn) stateDebugLog(msg string, keys ...interface{}) {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	c.stateDebugLogLocked(msg, keys...)
}

func (c *Conn) stateDebugLogLocked(msg string, keys ...interface{}) {
	logger := c.logger.V(10)
	if logger.Enabled() {
		keys = append(keys, "read-status", c.getReadStatus(), "write-status", c.getWriteStatus())
		logger.Info(msg, keys...)
	}
}
