// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha512"
	"fmt"
	"io"
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"storj.io/utp-go"
)

const repeats = 20

func TestUTPConn(t *testing.T) {
	l := newTestServer(t)

	var group errgroup.Group
	group.Go(func() error {
		for {
			newConn, err := l.Accept()
			if err != nil {
				return err
			}
			group.Go(func() error {
				return handleConn(newConn)
			})
		}
	})
	group.Go(func() error {
		for i := 0; i < repeats; i++ {
			if err := makeConn(l.Addr()); err != nil {
				return err
			}
		}
		return l.Close()
	})
	err := group.Wait()
	require.NoError(t, err)
}

func newTestServer(tb testing.TB) *utp.Listener {
	server, err := utp.Listen("utp", "127.0.0.1:0")
	require.NoError(tb, err)
	utpListener, ok := server.(*utp.Listener)
	require.Truef(tb, ok, "expecting type *utp.Listener but got %T", server)
	log.Printf("now listening on %s", server.Addr().String())
	return utpListener
}

func handleConn(conn net.Conn) (err error) {
	defer func() {
		closeErr := conn.Close()
		if err == nil {
			err = closeErr
		}
	}()

	buf := make([]byte, 2048)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return err
	}
	sig := make([]byte, sha512.Size)
	_, err = io.ReadFull(conn, sig)
	if err != nil {
		return err
	}
	hashOfData := sha512.Sum512(buf)
	if bytes.Compare(hashOfData[:], sig) != 0 {
		return fmt.Errorf("hashes do not match: %x != %x", hashOfData, sig)
	}
	return nil
}

func makeConn(addr net.Addr) (err error) {
	conn, err := utp.Dial("utp", addr.String())
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	// do the things
	data := make([]byte, 2048)
	_, err = io.ReadFull(rand.Reader, data)
	if err != nil {
		return err
	}
	hashOfData := sha512.Sum512(data)
	both := append(data, hashOfData[:]...)
	n, err := conn.Write(both)
	if err != nil {
		return err
	}
	if n < len(both) {
		return fmt.Errorf("short read: %d < %d", n, len(both))
	}
	return nil
}
