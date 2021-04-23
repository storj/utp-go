// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp

import (
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
)

const (
	ethernetMTU     = 1500
	ipv4HeaderSize  = 20
	ipv6HeaderSize  = 40
	udpHeaderSize   = 8
	greHeaderSize   = 24
	pppoeHeaderSize = 8
	mppeHeaderSize  = 2

	fudgeHeaderSize = 36
	teredoMTU       = 1280

	udpIPv4Overhead   = ipv4HeaderSize + udpHeaderSize
	udpIPv6Overhead   = ipv6HeaderSize + udpHeaderSize
	udpTeredoOverhead = udpIPv4Overhead + udpIPv6Overhead

	udpIPv4MTU   = ethernetMTU - ipv4HeaderSize - udpHeaderSize - greHeaderSize - pppoeHeaderSize - mppeHeaderSize - fudgeHeaderSize
	udpIPv6MTU   = ethernetMTU - ipv6HeaderSize - udpHeaderSize - greHeaderSize - pppoeHeaderSize - mppeHeaderSize - fudgeHeaderSize
	udpTeredoMTU = teredoMTU - ipv6HeaderSize - udpHeaderSize
)

// GetUDPMTU returns a best guess as to the MTU (maximum transmission unit) on
// the network to which the specified address belongs (IPv4 or IPv6).
func GetUDPMTU(addr *net.UDPAddr) uint16 {
	// Since we don't know the local address of the interface,
	// be conservative and assume all IPv6 connections are Teredo.
	if isIPv6(addr.IP) {
		return udpTeredoMTU
	}
	return udpIPv4MTU
}

func getUDPOverhead(addr *net.UDPAddr) uint16 {
	// Since we don't know the local address of the interface,
	// be conservative and assume all IPv6 connections are Teredo.
	if isIPv6(addr.IP) {
		return udpTeredoOverhead
	}
	return udpIPv4Overhead
}

func randomUint32() uint32 {
	var buf [4]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic("can't read from random source: " + err.Error())
	}
	return binary.LittleEndian.Uint32(buf[:])
}

func getMaxPacketSize() int { return 1500 }

func delaySample(addr *net.UDPAddr, sampleMS int) {}

func isIPv6(ip net.IP) bool {
	return ip.To4() == nil
}
