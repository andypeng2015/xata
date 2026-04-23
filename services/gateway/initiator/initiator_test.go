package initiator

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace/noop"

	"xata/services/gateway/session"

	"github.com/elastic/go-concert/ctxtool"
	"github.com/elastic/go-concert/unison"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
)

var certificate = must(generateTestCertificate())

var testTracer = noop.NewTracerProvider().Tracer("")

const testServerName = "testdb.example.com"

func TestInitiator_Connect_TLSViaPGProtocol(t *testing.T) {
	testInitiatorConnect(t, connectTLSViaPostgres)
}

func TestInitiator_Connect_TLSDirect(t *testing.T) {
	testInitiatorConnect(t, connectTLSViaDirect)
}

func testInitiatorConnect(t *testing.T, connector func(addr string) (*pgx.Conn, error)) {
	gwListener := listen(t)
	serverListener := listen(t)

	ctx := newTestLogger(t).WithContext(context.Background())
	tg := unison.TaskGroupWithCancel(ctx)

	var gotServerName string
	dialer := session.NewClusterDialer(
		session.ClusterDialerConfiguration{
			ReactivateTimeout:   50 * time.Second,
			StatusCheckInterval: 2 * time.Second,
		},
	)

	resolver := session.ResolverFunc(func(ctx context.Context, serverName string) (*session.Branch, error) {
		gotServerName = serverName
		return &session.Branch{
			ID:      "testdb",
			Address: serverListener.Addr().String(),
		}, nil
	})

	proxy := session.NewProxy(testTracer, resolver, dialer.Dial, nil)
	initiator := must(New(testTracer, proxy, certificate))
	tg.Go(gatewayMain(gwListener, initiator))

	tg.Go(func(ctx context.Context) error {
		logger := zerolog.Ctx(ctx).With().Str("component", "server").Logger()
		ctx = logger.WithContext(ctx)

		logger.Debug().Msg("Starting server")
		defer logger.Debug().Msg("Server stopped")

		err := runServer(ctx, serverListener, func(ctx context.Context, conn net.Conn) error {
			if err := startupTestServerConn(ctx, conn, func(startupMessage *pgproto3.StartupMessage, password string) error {
				logger.Debug().Msg("Authenticated")
				return nil
			}); err != nil {
				return err
			}

			// wait for shutdown
			<-ctx.Done()
			return nil
		})
		if err != nil {
			return fmt.Errorf("server error: %w", err)
		}
		return nil
	})

	logger := zerolog.Ctx(ctx).With().Str("component", "client").Logger()

	logger.Debug().Msg("Connecting to proxy")
	conn, err := connector(gwListener.Addr().String())
	require.NoError(t, err, "failed to connect to proxy")

	logger.Debug().Msg("Connected to proxy")
	conn.Close(context.Background())
	logger.Debug().Msg("Closed connection to proxy")

	logger.Debug().Msg("Stopping task group")
	err = tg.Stop()
	logger.Debug().Msg("Stopped task group")

	require.NoError(t, err, "Unexpected error in server tasks")
	require.Equal(t, testServerName, gotServerName, "server name mismatch")
}

func TestInitiator_Cancellation_TLSViaPGProtocol(t *testing.T) {
	testInitiatorCancellation(t, connectTLSViaPostgres)
}

func TestInitiator_Cancellation_TLSDirect(t *testing.T) {
	testInitiatorCancellation(t, connectTLSViaDirect)
}

func testInitiatorCancellation(t *testing.T,
	connector func(addr string) (*pgx.Conn, error),
) {
	serverListener := listen(t)
	gwListener := listen(t)

	logger := newTestLogger(t)
	ctx := logger.WithContext(context.Background())

	tg := unison.TaskGroupWithCancel(ctx)

	dialer := session.NewClusterDialer(
		session.ClusterDialerConfiguration{
			ReactivateTimeout:   50 * time.Second,
			StatusCheckInterval: 2 * time.Second,
		},
	)

	resolver := session.ResolverFunc(func(ctx context.Context, serverName string) (*session.Branch, error) {
		return &session.Branch{
			ID:      "testdb",
			Address: serverListener.Addr().String(),
		}, nil
	})

	proxy := session.NewProxy(testTracer, resolver, dialer.Dial, nil)

	initiator := must(New(testTracer, proxy, certificate))

	tg.Go(gatewayMain(gwListener, initiator))

	var serverCancelled bool
	wgAwaitCancel := make(chan struct{})
	tg.Go(func(ctx context.Context) error {
		logger := zerolog.Ctx(ctx).With().Str("component", "server").Logger()
		ctx = logger.WithContext(ctx)

		ctx, cancel := ctxtool.WithFunc(ctx, func() {
			serverListener.Close()
		})
		defer cancel()

		// Accept first connection that we want to 'cancel'
		logger.Debug().Msg("Waiting for first connection")
		conn1, err := serverListener.Accept()
		if err != nil {
			return logError(ctx, fmt.Errorf("failed to accept connection: %w", err))
		}
		defer conn1.Close()
		logger.Debug().Msg("Accepted first connection")

		logger.Debug().Msg("Starting test server connection")
		if err := startupTestServerConn(ctx, conn1, func(startupMessage *pgproto3.StartupMessage, password string) error {
			return nil
		}); err != nil {
			return logError(ctx, fmt.Errorf("failed to handle startup: %w", err))
		}
		logger.Debug().Msg("Test server connection started")

		// read statement from client to simulate server side locking
		logger.Debug().Msg("Waiting for client message")
		backend1 := pgproto3.NewBackend(conn1, conn1)
		_, err = backend1.Receive()
		if err != nil {
			return logError(ctx, fmt.Errorf("failed to receive message: %w", err))
		}
		logger.Debug().Msg("Received client message, signaling cancel")
		close(wgAwaitCancel)

		// wait for second connection doing the 'cancel' message
		logger.Debug().Msg("Waiting for cancel connection")
		conn2, err := serverListener.Accept()
		if err != nil {
			return logError(ctx, fmt.Errorf("failed to accept connection: %w", err))
		}
		defer conn2.Close()
		logger.Debug().Msg("Accepted cancel connection")

		backend2 := pgproto3.NewBackend(conn2, conn2)
		msg, err := backend2.ReceiveStartupMessage()
		if err != nil {
			return logError(ctx, fmt.Errorf("failed to receive message: %w", err))
		}

		if _, ok := msg.(*pgproto3.CancelRequest); !ok {
			return logError(ctx, fmt.Errorf("expected cancel message, got %T", msg))
		}
		logger.Debug().Msg("Received cancel request")
		conn2.Close() // done cancellation
		serverCancelled = true

		// send error response to client waiting for the query to finish
		logger.Debug().Msg("Sending error response to client")
		backend1.Send(&pgproto3.ErrorResponse{Code: pgerrcode.QueryCanceled, Message: "Query Cancelled"})
		backend1.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend1.Flush(); err != nil {
			return logError(ctx, fmt.Errorf("failed to flush error response: %w", err))
		}
		logger.Debug().Msg("Error response sent to client")

		// wait for session to be cancelled
		logger.Debug().Msg("Waiting for context cancellation")
		<-ctx.Done()
		logger.Debug().Msg("Context cancelled")
		return nil
	})

	clientLogger := zerolog.Ctx(ctx).With().Str("component", "client").Str("client_id", "1").Logger()
	clientLogger.Debug().Msg("Connecting to proxy")
	client, err := connector(gwListener.Addr().String())
	require.NoError(t, err, "failed to connect to proxy")

	var clientCancelled bool
	var wgClientCancelled sync.WaitGroup
	wgClientCancelled.Add(1)
	tg.Go(func(ctx context.Context) error {
		logger := clientLogger
		ctx = clientLogger.WithContext(ctx)

		defer func() {
			// logger.Debug().Msg("Closing client")
			wgClientCancelled.Done()
			client.Close(ctx)
			logger.Debug().Msg("Client stopped")
		}()

		logger.Debug().Msg("Executing query")
		_, err := client.Exec(ctx, "SELECT 1")
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.QueryCanceled {
				clientCancelled = true
				return nil
			}
			return logError(ctx, fmt.Errorf("failed to execute query: %w", err))
		}
		return nil
	})

	logger.Debug().Msg("Waiting for cancel request from server")
	<-wgAwaitCancel
	logger.Debug().Msg("Sending cancel request to client")
	require.NoError(t, pgCancelRequest(ctx, client))

	// logger.Debug().Msg("Waiting for client to finish")
	wgClientCancelled.Wait()

	// logger.Debug().Msg("Stopping background tasks")
	err = tg.Stop()
	// logger.Debug().Msg("Background tasks stopped")

	require.NoError(t, err)
	require.True(t, serverCancelled, "server was not cancelled")
	require.True(t, clientCancelled, "client was not cancelled")
}

func TestInitiator_Err_InvalidServerName(t *testing.T) {
	gwListener := listen(t)

	dialer := session.NewClusterDialer(
		session.ClusterDialerConfiguration{
			ReactivateTimeout:   50 * time.Second,
			StatusCheckInterval: 2 * time.Second,
		},
	)

	resolver := session.ResolverFunc(func(ctx context.Context, serverName string) (*session.Branch, error) {
		return nil, fmt.Errorf("invalid server name")
	})

	proxy := session.NewProxy(testTracer, resolver, dialer.Dial, nil)

	initiator := must(New(testTracer, proxy, certificate))
	ctx := newTestLogger(t).WithContext(context.Background())

	tg := unison.TaskGroupWithCancel(ctx)
	tg.Go(gatewayMain(gwListener, initiator))

	conn, err := pgConnect(ConnConfig{
		Addr:           gwListener.Addr().String(),
		User:           "postgres",
		Password:       "pass",
		Dbname:         "db",
		SSLMode:        "require",
		SSLNegotiation: "postgres",
	})
	if err == nil {
		conn.Close(context.Background())
		t.Fatal("expected client error, got none")
	}

	err = tg.Stop()
	require.Error(t, err, "expected initiator error, got none")
}

type ConnConfig struct {
	Addr           string
	User           string
	Password       string
	Dbname         string
	SSLMode        string
	SSLNegotiation string
	ServerName     string
}

func pgConnect(config ConnConfig) (*pgx.Conn, error) {
	connstr := pgConnString(config)
	pgConfig, err := pgx.ParseConfig(connstr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if pgConfig.TLSConfig != nil {
		pgConfig.TLSConfig.ServerName = config.ServerName
	} else {
		return nil, fmt.Errorf("no TLS config")
	}

	return pgx.ConnectConfig(context.Background(), pgConfig)
}

func pgCancelRequest(ctx context.Context, conn *pgx.Conn) error {
	logger := zerolog.Ctx(ctx).With().Str("component", "cancelClient").Logger()

	// issue: https://github.com/jackc/pgx/issues/2340
	//
	// pgx does not properly encrypt the cancel request. In that case
	// clients will receive an error.
	// Client based on libpq (and maybe others) do use an encrypted
	// connection to send the cancel request. We simulate that behavior
	// here.
	// Once fixed, we can use `client.PgConn().CancelRequest(ctx)` instead.

	defer logger.Debug().Msg("Cancel request sent")

	logger.Debug().Msg("Dialing cancel connection")
	connConfig := conn.Config()
	addr := fmt.Sprintf("%v:%v", connConfig.Host, connConfig.Port)
	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to dial cancel connection: %w", err)
	}
	defer tcpConn.Close()

	cancelConn := tcpConn
	if connConfig.TLSConfig != nil {
		var tlsConn *tls.Conn

		if connConfig.SSLNegotiation == "direct" {
			tlsConn = tls.Client(tcpConn, connConfig.TLSConfig)
		} else {
			var rawBuffer [64]byte
			sslReq := &pgproto3.SSLRequest{}
			buf := must(sslReq.Encode(rawBuffer[:0]))
			must(tcpConn.Write(buf))

			// read 1 byte
			must(tcpConn.Read(rawBuffer[:1]))

			tlsConn = tls.Client(tcpConn, connConfig.TLSConfig)
		}

		if err := tlsConn.HandshakeContext(context.Background()); err != nil {
			return fmt.Errorf("failed to handshake cancel connection: %w", err)
		}
		cancelConn = tlsConn
	}

	logger.Debug().Msg("Sending cancel request")
	frontend := pgproto3.NewFrontend(cancelConn, cancelConn)
	frontend.Send(&pgproto3.CancelRequest{SecretKey: make([]byte, 4)})
	if err := frontend.Flush(); err != nil {
		return logError(ctx, fmt.Errorf("flush cancel request: %w", err))
	}
	return nil
}

func pgConnString(config ConnConfig) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s&sslnegotiation=%s",
		config.User, config.Password, config.Addr, config.Dbname, config.SSLMode, config.SSLNegotiation)
}

func runServer(ctx context.Context, listener net.Listener, callback func(ctx context.Context, conn net.Conn) error) error {
	tg := unison.TaskGroupWithCancel(ctx)
	tg.OnQuit = unison.ContinueOnErrors

	_, cancel := ctxtool.WithFunc(ctx, func() {
		listener.Close()
	})
	defer cancel()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
			conn, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
					break loop
				}
				return fmt.Errorf("failed to accept connection: %w (%T)", err, err)
			}

			tg.Go(func(ctx context.Context) error {
				defer conn.Close()
				return callback(ctx, conn)
			})
		}
	}

	return tg.Stop()
}

func gatewayMain(listener net.Listener, initiator Initiator) func(context.Context) error {
	return func(ctx context.Context) error {
		logger := zerolog.Ctx(ctx).With().Str("component", "gateway").Logger()
		ctx = logger.WithContext(ctx)

		logger.Debug().Msg("Starting gateway")
		defer logger.Debug().Msg("Gateway stopped")

		var clientID atomic.Int32
		err := runServer(ctx, listener, func(ctx context.Context, conn net.Conn) error {
			id := clientID.Add(1)
			logger := zerolog.Ctx(ctx).With().
				Str("client_id", fmt.Sprintf("%d", id)).
				Logger()
			ctx = logger.WithContext(ctx)

			logger.Debug().Msg("Initiating session")
			defer logger.Debug().Msg("Session stopped")

			session, err := initiator.InitSession(ctx, "test", conn)
			if err != nil {
				logger.Debug().Msgf("Failed to create session: %v", err)
				return fmt.Errorf("failed to create session: %w", err)
			}
			if session == nil {
				logger.Debug().Msg("No active session, closing connection")
				return nil
			}

			return session.ServeSQLSession(ctx)
		})
		if err != nil {
			return fmt.Errorf("gateway error: %w", err)
		}
		return nil
	}
}

func startupTestServerConn(
	ctx context.Context,
	conn net.Conn,
	authCallback func(startupMessage *pgproto3.StartupMessage, password string) error,
) error {
	logger := zerolog.Ctx(ctx)

	backend := pgproto3.NewBackend(conn, conn)

	logger.Debug().Msg("Waiting for startup message")
	msg, err := backend.ReceiveStartupMessage()
	if err != nil {
		return logError(ctx, fmt.Errorf("failed to receive startup message: %w", err))
	}
	logger.Debug().Msgf("Test server received startup message: %+v", msg)

	startupMsg, ok := msg.(*pgproto3.StartupMessage)
	if !ok {
		return logError(ctx, fmt.Errorf("expected startup message, got %T", startupMsg))
	}

	logger.Debug().Msg("Request password authentication")
	backend.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := backend.Flush(); err != nil {
		return logError(ctx, fmt.Errorf("failed to flush authentication message: %w", err))
	}

	// await password message
	logger.Debug().Msg("Waiting for password message")
	msg, err = backend.Receive()
	if err != nil {
		return logError(ctx, fmt.Errorf("failed to receive password message: %w", err))
	}
	logger.Debug().Msgf("Test server received password message: %+v", msg)

	pwdMsg, ok := msg.(*pgproto3.PasswordMessage)
	if !ok {
		return logError(ctx, fmt.Errorf("expected password message, got %T", pwdMsg))
	}

	logger.Debug().Msg("Authenticating")
	if err := authCallback(startupMsg, pwdMsg.Password); err != nil {
		return logError(ctx, fmt.Errorf("failed to authenticate: %w", err))
	}

	logger.Debug().Msg("Sending authentication OK + ready for query")
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return logError(ctx, fmt.Errorf("failed to flush authentication OK message: %w", err))
	}

	return nil
}

func logError(ctx context.Context, err error) error {
	zerolog.Ctx(ctx).Error().Msg(err.Error())
	return err
}

func listen(t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() {
		listener.Close()
	})
	return listener
}

func connectTLSViaPostgres(addr string) (*pgx.Conn, error) {
	return pgConnect(ConnConfig{
		Addr:           addr,
		User:           "postgres",
		Password:       "pass",
		Dbname:         "db",
		SSLMode:        "require",
		SSLNegotiation: "postgres",
		ServerName:     testServerName,
	})
}

func connectTLSViaDirect(addr string) (*pgx.Conn, error) {
	return pgConnect(ConnConfig{
		Addr:           addr,
		User:           "postgres",
		Password:       "pass",
		Dbname:         "db",
		SSLMode:        "require",
		SSLNegotiation: "direct",
		ServerName:     testServerName,
	})
}

func generateTestCertificate() (*tls.Certificate, error) {
	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Create a self-signed certificate
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test Organization"},
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().Add(24 * time.Hour), // Valid for 1 day
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	// Encode the certificate and private key to PEM format
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})

	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	// Create a tls.Certificate from the PEM-encoded data
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to create X509 key pair: %w", err)
	}

	return &cert, nil
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type testWriter struct {
	T   *testing.T
	out io.Writer
}

// Write writes the log message to testing.T.
func (tw testWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSuffix(string(p), "\n")
	tw.T.Log(msg)
	if tw.out != nil {
		tw.out.Write([]byte(msg))
		tw.out.Write([]byte("\n"))
	}
	return len(p), nil
}

var logStderr bool

// NewTestLogger creates a new zerolog logger that writes to testing.T.
func newTestLogger(t *testing.T) zerolog.Logger {
	writer := testWriter{T: t}
	if logStderr {
		writer.out = os.Stderr
	}

	return zerolog.New(writer).With().Timestamp().Logger()
}

func TestMain(m *testing.M) {
	flag.BoolVar(&logStderr, "log-stderr", false, "log to stderr in addition to test output")
	flag.Parse()
	os.Exit(m.Run())
}
