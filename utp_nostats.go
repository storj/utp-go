//+build !utpdebug

package utp

func (s *Stats) transmitted(length int)    {}
func (s *Stats) packetLost()               {}
func (s *Stats) packetReceived(length int) {}
func (s *Stats) fastTransmitted()          {}
func (s *Stats) duplicateReceived()        {}

func (s *Socket) checkInvariants()      {}
func (s *Socket) checkNoTransmissions() {}
func (s *Socket) checkNoWindow()        {}
