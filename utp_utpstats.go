// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

//+build utpdebug

package utp

func (s *Stats) transmitted(length int) {
	s.NBytesXmit += uint64(length)
	s.NXmit++
}

func (s *Stats) packetLost() {
	s.ReXmit++
}

func (s *Stats) packetReceived(length int) {
	s.NRecv++
	s.NBytesRecv += len
}

func (s *Stats) fastTransmitted() {
	s.FastReXmit++
}

func (s *Stats) duplicateReceived() {
	s.NDupRecv++
}

func (s *Socket) checkInvariants() {
	if s.reorderCount > 0 {
		assert(s.inbuf.get(s.ackNum+1) == nil)
	}

	var outstandingBytes int
	for i := 0; i < s.curWindowPackets; i++ {
		pkt := s.outbuf.get(s.seqNum - i - 1)
		if pkt == nil || pkt.transmissions == 0 || pkt.needResend {
			continue
		}
		outstandingBytes += pkt.payload
	}
	assert(outstandingBytes == curWindow)
}

func (s *Socket) checkNoTransmissions() {
	pkt := s.conn.outbuf.get(int(conn.seq_nr) - int(conn.cur_window_packets))
	assert(pkt.transmissions == 0)
}

func (s *Socket) checkNoWindow() {
	if s.conn.cur_window_packets == 0 {
		assert(conn.curWindow == 0)
	}
}

func (s *Socket) GetStats() Stats {
	return *s.stats
}
