package utp

import (
	"crypto/tls"
	"net"
)

// DialTLS connects to the given network address using net.Dial or utp.Dial as
// appropriate and then initiates a TLS handshake, returning the resulting TLS
// connection. DialTLS interprets a nil configuration as equivalent to the zero
// configuration; see the documentation of tls.Config for the details.
func DialTLS(network, addr string, config *tls.Config) (*tls.Conn, error) {
	return DialTLSOptions(network, addr, config)
}

// DialTLSOptions connects to the given network address using net.Dial or
// utp.Dial as appropriate and then initiates a TLS handshake, returning the
// resulting TLS connection. DialTLS interprets a nil configuration as
// equivalent to the zero configuration; see the documentation of tls.Config
// for the details.
func DialTLSOptions(network, addr string, config *tls.Config, options ...ConnectOption) (*tls.Conn, error) {
	utpAddr, err := ResolveUTPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	utpConn, err := DialUTPOptions(network, nil, utpAddr, options...)
	if err != nil {
		return nil, err
	}
	return tls.Client(utpConn, config), nil
}

// ListenTLS creates a TLS listener accepting connections on the given network
// address using net.Listen or utp.Listen as appropriate. The configuration
// config must be non-nil and must include at least one certificate or else
// set GetCertificate.
func ListenTLS(network, laddr string, config *tls.Config) (net.Listener, error) {
	return ListenTLSOptions(network, laddr, config)
}

// ListenTLSOptions creates a TLS listener accepting connections on the given
// network address using net.Listen or utp.Listen as appropriate. The
// configuration config must be non-nil and must include at least one
// certificate or else set GetCertificate.
func ListenTLSOptions(network, laddr string, config *tls.Config, options ...ConnectOption) (net.Listener, error) {
	utpAddr, err := ResolveUTPAddr(network, laddr)
	if err != nil {
		return nil, err
	}
	listener, err := ListenUTPOptions(network, utpAddr, options...)
	if err != nil {
		return nil, err
	}
	return tls.NewListener(listener, config), nil
}
