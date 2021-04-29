// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package libutp

const (
	congestionControlTarget = (100 * 1000) // us
	rateCheckInterval       = 10000        // ms

	// DynamicPacketSizeEnabled controls whether this µTP endpoint will limit
	// outgoing packets to a maximum of getMaxPacketSize() bytes. It is very
	// unclear why this would be necessary, because packets are already limited
	// to a maximum of GetUDPMTU() bytes, and the result of that function is
	// always less than the result of getMaxPacketSize() bytes.
	//
	// Really, this parameter is probably vestigial and has no current purpose.
	DynamicPacketSizeEnabled = false
	// DynamicPacketSizeFactor is entirely unused and is also probably
	// vestigial, related to DynamicPacketSizeEnabled.
	DynamicPacketSizeFactor = 2
)

// BandwidthType represents different classes of data which may be exchanged
// in the course of µTP connections. Every byte in a µTP packet is classified
// using one of these, and every byte that is not PayloadBandwidth is a form
// of overhead. A base.OnOverhead callback can be used with Socket objects to
// keep track of how much overhead of each type is getting used.
type BandwidthType int

const (
	// PayloadBandwidth is the class of data which was directly specified by
	// the application layer. That is to say, this is application data.`
	PayloadBandwidth BandwidthType = iota
	// ConnectOverhead is the class of data used for bytes which are used to
	// negotiate connections with a remote peer.
	ConnectOverhead
	// CloseOverhead is the class of data used for bytes which are used to
	// indicate that a connection should be closed.
	CloseOverhead
	// AckOverhead is the class of data used to communicate acknowledgement
	// of data received.
	AckOverhead
	// HeaderOverhead is the class of data used for bytes in µTP packet
	// headers.
	HeaderOverhead
	// RetransmitOverhead is the class of data used for bytes that are
	// retransmissions of data previously sent as PayloadBandwidth.
	RetransmitOverhead
)
