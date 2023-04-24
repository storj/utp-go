package utp_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/zapr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"

	"storj.io/utp-go"
)

const (
	keyPEM = `-----BEGIN PRIVATE KEY-----
MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBALCuLILT4iHWiOr9
sXgvAJQrF1deNixfmW9x3qH9otAyBpgVrE2u9gf+UWuykvLQAT27HqdJ5eTaApbp
oTfz5HRq4WV659POcZJnB6BaaNGvA2i0JD8WrgXV2JmMFtT4nRlIv2yd8RX/LZ7x
tCWNFYmoHFs/MAQ1M52CJLA0DY9tAgMBAAECgYBo+lSAN505NduMpMh5/JN/dksc
ImJV40errCD4Z1gCFHdOjjIexkJxZW7DawtdMrJKF5CTHZl3bQH04URlloi6lk4w
WhfrjwYHx8VvKIEEejGzSnSqGcqn1pW4n18BEMTVqHF/WCqiEfmejovF/fYnYiGg
0ljvUxnx/rlkWWJ6EQJBAOFnfrq8dNtKGxupg3o1AeGcQWUi6NGXXqsVB/DcOnM9
Iv6ybMkZ+kSYJLE5Rjy7amd18+R+P7ZmVveqdLyD8R8CQQDIqZYfY3pAvuriMs8T
sMSa5g1udOG7eNW0UAaKleCNET1VwijsJw/4W3aRuNL+22o5GJ4u83+tGi/KLCm5
f3HzAkEAtiMoL4KrNqu6He8rM6vzmjfmS/Aai4oyUDJNWV7LyGTli0PoTdQ0/Aqo
06BBVj/nKjUQ4Fj36M7nhXdynwZK5wJAaFQT43n7JBKfWMAF/jzX25lkvlsyyiAH
LFq3K/LE71NZSm9Ki427tesH+LfZq/w0fD8ab1rWtQ96bWkMwI9MlQJBAKwRHQbV
NY6wfipOv3ew8Gdnc6nIkGSDUNU57qOxjW+0MNTlrBQVDqKbeiSkALACNjB/OF9C
grSVQqRG3JdWQOU=
-----END PRIVATE KEY-----`

	certPEM = `-----BEGIN CERTIFICATE-----
MIICmzCCAgSgAwIBAgIJAPqc9qysvigFMA0GCSqGSIb3DQEBCwUAMFsxCzAJBgNV
BAYTAkZKMQ0wCwYDVQQIDARSZXdhMRAwDgYDVQQHDAdOYXVzb3JpMRgwFgYDVQQK
DA9QYXRlbERoYWFiYUNhZmUxETAPBgNVBAMMCGFiY2QuZWZnMB4XDTIxMDUwNDAz
NDgzOFoXDTIxMDYwMzAzNDgzOFowWzELMAkGA1UEBhMCRkoxDTALBgNVBAgMBFJl
d2ExEDAOBgNVBAcMB05hdXNvcmkxGDAWBgNVBAoMD1BhdGVsRGhhYWJhQ2FmZTER
MA8GA1UEAwwIYWJjZC5lZmcwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBALCu
LILT4iHWiOr9sXgvAJQrF1deNixfmW9x3qH9otAyBpgVrE2u9gf+UWuykvLQAT27
HqdJ5eTaApbpoTfz5HRq4WV659POcZJnB6BaaNGvA2i0JD8WrgXV2JmMFtT4nRlI
v2yd8RX/LZ7xtCWNFYmoHFs/MAQ1M52CJLA0DY9tAgMBAAGjZzBlMAsGA1UdDwQE
AwIEMDATBgNVHSUEDDAKBggrBgEFBQcDATBBBgNVHREEOjA4gghhYmNkLmVmZ4IS
d29ybGRkb21pbmF0aW9uLmZqghhpbGx1bWluYXRpLmhhbGZsaWZlMy5jb20wDQYJ
KoZIhvcNAQELBQADgYEAb0KvbeIP4/SAsntBOKUmPs2gyNVCJVuzJ0pRgtTWxF75
9I6We1Qse1vYG6QJrEUcy541qyVUTwwrBhU8cR1lmxo+eOAt5GAuq+i4bvnV3m48
9Yn4mGrnTskJOQwPuPb+4n59YrYJYfNz/9Mdmyi+CTeby9qCYsgxRvj5RCDqbIM=
-----END CERTIFICATE-----`

	tlsTestRepeats = 20
	sekritMessage  = "hello"
)

func TestTLSOverUTPInParallel(t *testing.T) {
	x509cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM([]byte(certPEM))

	validTime := time.Date(2021, 05, 18, 3, 50, 13, 0, time.UTC)

	serverConfig := tls.Config{
		Certificates: []tls.Certificate{x509cert},
		MinVersion:   tls.VersionTLS13,
		Time:         func() time.Time { return validTime },
	}
	clientConfig := tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS13,
		ServerName: "abcd.efg",
		Time:       func() time.Time { return validTime },
	}
	addrChan := make(chan *utp.Addr, 1)

	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.Level(logLevel)))
	compatibleLogger := zapr.NewLogger(logger)

	group := newLabeledErrgroup(context.Background())

	group.Go(func(ctx context.Context) (err error) {
		server, err := utp.ListenTLSOptions("utp", "127.0.0.1:0", &serverConfig, utp.WithLogger(compatibleLogger))
		if err != nil {
			logger.Error("could not listen", zap.Error(err))
			return err
		}
		addrChan <- server.Addr().(*utp.Addr)

		// arrange for the listener to be closed either on return or on context
		// close, whichever is first
		defer closeOnContextCancel(ctx, logger, server)(&err)

		for i := 0; i < tlsTestRepeats; i++ {
			incoming, err := server.Accept()
			if err != nil {
				logger.Error("could not accept", zap.Error(err))
				return err
			}

			group.Go(func(ctx context.Context) (err error) {
				defer closeOnContextCancel(ctx, logger, incoming)(&err)
				return handleTLSConn(incoming)
			}, "task", "handleConn", "i", strconv.Itoa(i))
		}
		return nil
	}, "task", "listen")

	addr := <-addrChan
	for i := 0; i < tlsTestRepeats; i++ {
		group.Go(func(ctx context.Context) (err error) {
			client, err := utp.DialTLSOptions(addr.Network(), addr.String(), &clientConfig, utp.WithLogger(compatibleLogger))
			if err != nil {
				return err
			}

			defer closeOnContextCancel(ctx, logger, client)(&err)

			err = client.Handshake()
			require.NoError(t, err)

			message, err := io.ReadAll(client)
			assert.NoError(t, err)
			assert.Equal(t, sekritMessage, string(message))

			return nil
		}, "task", "client", "i", strconv.Itoa(i))
	}

	err = group.Wait()
	require.NoError(t, err)
}

func handleTLSConn(incoming net.Conn) error {
	tlsConn := incoming.(*tls.Conn)
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}

	connState := tlsConn.ConnectionState()
	if connState.Version != tls.VersionTLS13 {
		return fmt.Errorf("bad tls version %d != VersionTLS13", connState.Version)
	}

	_, err = tlsConn.Write([]byte(sekritMessage))
	if err != nil {
		return err
	}

	return nil
}

type closeable interface {
	Close() error
}

func closeOnContextCancel(ctx context.Context, logger *zap.Logger, x closeable) func(*error) {
	var (
		once       sync.Once
		closeErr   error
		cancelChan = make(chan struct{})
	)
	go func() {
		select {
		case <-ctx.Done():
			once.Do(func() {
				logger.Debug("context closed. closing object", zap.Error(ctx.Err()), zap.String("obj-type", fmt.Sprintf("%T", x)))
				closeErr = x.Close()
			})
		case <-cancelChan:
		}
	}()
	return func(errPointer *error) {
		close(cancelChan)
		once.Do(func() {
			logger.Debug("function complete. closing object", zap.Error(*errPointer), zap.String("obj-type", fmt.Sprintf("%T", x)))
			closeErr = x.Close()
		})
		if *errPointer != nil {
			*errPointer = closeErr
		}
	}
}
