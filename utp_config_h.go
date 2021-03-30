package utp

const (
	congestionControlTarget  = (100 * 1000) // us
	rateCheckInterval        = 10000        // ms

	DynamicPacketSizeEnabled = false
	DynamicPacketSizeFactor  = 2
)

type BandwidthType int

const (
	PayloadBandwidth BandwidthType = iota
	ConnectOverhead
	CloseOverhead
	AckOverhead
	HeaderOverhead
	RetransmitOverhead
)
