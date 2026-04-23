package session_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"xata/services/gateway/session"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

var testTracer = noop.NewTracerProvider().Tracer("test")

// Test helpers

// mockResolver is a test helper that captures resolver calls and allows configurable behavior
type mockResolver struct {
	ServerName string
	Address    string
	Branch     string
	Error      error
	CallCount  int
}

func (m *mockResolver) Resolve(ctx context.Context, serverName string) (*session.Branch, error) {
	m.ServerName = serverName
	m.CallCount++
	if m.Error != nil {
		return nil, m.Error
	}
	return &session.Branch{Address: m.Address, ID: m.Branch}, nil
}

// createStartupMessage creates a valid PostgreSQL startup message for testing
func createStartupMessage() *pgproto3.StartupMessage {
	return &pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters: map[string]string{
			"user":     "testuser",
			"database": "testdb",
		},
	}
}

// createCancelRequest creates a valid PostgreSQL cancel request for testing
func createCancelRequest() *pgproto3.CancelRequest {
	return &pgproto3.CancelRequest{
		ProcessID: 12345,
		SecretKey: []byte{0x00, 0x01, 0x09, 0x32},
	}
}

type dialerCall struct {
	Network string
	Address string
}

type mockDialer struct {
	Conn       net.Conn
	Err        error
	CallAction func()
	Call       *dialerCall // nil if not called yet
}

func (m *mockDialer) Dial(ctx context.Context, network string, branch *session.Branch) (net.Conn, error) {
	m.Call = &dialerCall{Network: network, Address: branch.Address}

	if m.CallAction != nil {
		m.CallAction()
	}

	if m.Err != nil {
		return nil, m.Err
	}
	return m.Conn, nil
}

// newClientConn creates a net.Conn for use as inboundConn in tests.
func newClientConn(t *testing.T) net.Conn {
	t.Helper()
	conn, _ := net.Pipe()
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestProxy_CreateBackendSession_Success(t *testing.T) {
	// Connection setup diagram:
	//
	// clientConn ←——————————→ (unused in this test)
	//
	// proxyToServerConn ←——————————→ serverSideConn
	//      ↑                              ↑
	//    proxy writes                test reads
	//
	// Flow:
	// 1. Mock dialer returns proxyToServerConn when proxy calls dial()
	// 2. Proxy writes startup message to proxyToServerConn
	// 3. Test reads from serverSideConn (the other end of the pipe)
	// 4. No race condition - clear separation of concerns

	clientConn := newClientConn(t)

	proxyToServerConn, serverSideConn := net.Pipe()
	defer proxyToServerConn.Close()
	defer serverSideConn.Close()

	// Mock resolver that returns valid address and branch name
	resolver := &mockResolver{Address: "mock-address:5432", Branch: "branch1"}
	md := &mockDialer{Conn: proxyToServerConn}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	// Create a valid startup message
	startupMessage := createStartupMessage()

	// Create a goroutine to handle the server side
	type serverResult struct {
		receivedMsg *pgproto3.StartupMessage
		err         error
	}
	serverResultCh := make(chan serverResult, 1)

	go func() {
		// Read the startup message that should be sent by the proxy
		backend := pgproto3.NewBackend(serverSideConn, serverSideConn)
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			serverResultCh <- serverResult{err: err}
			return
		}

		// Check if we received the correct message type
		receivedStartup, ok := msg.(*pgproto3.StartupMessage)
		if !ok {
			serverResultCh <- serverResult{err: fmt.Errorf("expected StartupMessage, got %T", msg)}
			return
		}

		serverResultCh <- serverResult{receivedMsg: receivedStartup, err: nil}
	}()

	// Test the CreateBackendSession method
	ctx := context.Background()
	session, err := proxy.CreateBackendSession(ctx, "branch1.example.com", clientConn, startupMessage)

	// Verify successful session creation
	require.NoError(t, err)
	require.NotNil(t, session)

	// Verify resolver was called with correct server name
	require.Equal(t, "branch1.example.com", resolver.ServerName)
	require.Equal(t, 1, resolver.CallCount)

	// Wait for server side to complete and validate results
	result := <-serverResultCh
	require.NoError(t, result.err)
	require.NotNil(t, result.receivedMsg)
	require.Equal(t, startupMessage, result.receivedMsg)

	require.NotNil(t, md.Call)
	require.Equal(t, "tcp", md.Call.Network)
	require.Equal(t, "mock-address:5432", md.Call.Address)
}

func TestProxy_CreateBackendSession_ResolverError(t *testing.T) {
	clientConn := newClientConn(t)

	// Mock resolver that returns an error
	testError := fmt.Errorf("invalid server name")
	var dialerCalled bool
	resolver := &mockResolver{Error: testError}
	md := &mockDialer{Err: errors.New("dialer should not be called when resolver fails")}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	// Test the CreateBackendSession method with invalid server name
	ctx := context.Background()
	session, err := proxy.CreateBackendSession(ctx, "invalid.example.com", clientConn, createStartupMessage())

	// Verify that resolver error is propagated
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid server name")
	require.Nil(t, session)

	// Verify resolver was called with correct server name but dialer was not
	require.Equal(t, "invalid.example.com", resolver.ServerName)
	require.False(t, dialerCalled, "dialer should not be called when resolver fails")

	require.Nil(t, md.Call)
}

func TestProxy_CreateBackendSession_NetworkDialError(t *testing.T) {
	clientConn := newClientConn(t)

	// Mock resolver that returns valid address but unreachable
	resolver := &mockResolver{Address: "unreachable.address:9999", Branch: "branch1"}
	// Mock dialer that simulates network connection failure
	md := &mockDialer{Err: errors.New("connection refused")}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	// Test the CreateBackendSession method
	ctx := context.Background()
	session, err := proxy.CreateBackendSession(ctx, "test.example.com", clientConn, createStartupMessage())

	// Verify that network error is propagated
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
	require.Nil(t, session)

	// Verify both resolver and dialer were called with correct parameters
	require.Equal(t, "test.example.com", resolver.ServerName)
	require.NotNil(t, md.Call)
	require.Equal(t, "tcp", md.Call.Network)
	require.Equal(t, "unreachable.address:9999", md.Call.Address)
}

func TestProxy_CreateBackendSession_SendMessageError(t *testing.T) {
	clientConn := newClientConn(t)

	// Create a connection pair where we'll close the server side immediately
	// to simulate send message failure
	proxyToServerConn, serverSideConn := net.Pipe()
	defer proxyToServerConn.Close()

	// Mock dialer that returns a connection that we'll close immediately
	md := &mockDialer{Conn: proxyToServerConn, CallAction: func() {
		serverSideConn.Close()
	}}

	// Mock resolver that returns valid address
	proxy := session.NewProxy(testTracer, &mockResolver{Address: "valid.address:5432", Branch: "branch1"}, md.Dial, nil)

	// Test the CreateBackendSession method
	ctx := context.Background()
	session, err := proxy.CreateBackendSession(ctx, "test.example.com", clientConn, createStartupMessage())

	// Verify that send message error is propagated with appropriate wrapping
	require.Error(t, err)
	require.Contains(t, err.Error(), "send startup message")
	require.Nil(t, session)

	// Verify that the connection was properly closed (connection cleanup)
	// We can test this by trying to write to the closed connection
	_, writeErr := proxyToServerConn.Write([]byte("test"))
	require.Error(t, writeErr, "connection should be closed after send message failure")

	require.NotNil(t, md.Call)
	require.Equal(t, "tcp", md.Call.Network)
	require.Equal(t, "valid.address:5432", md.Call.Address)
}

func TestProxy_CancelSession_Success(t *testing.T) {
	// Connection setup diagram:
	//
	// proxyToServerConn ←——————————→ serverSideConn
	//      ↑                              ↑
	//    proxy writes                test reads
	//
	// Flow:
	// 1. Mock dialer returns proxyToServerConn when proxy calls dial()
	// 2. Proxy writes cancel request to proxyToServerConn
	// 3. Proxy immediately closes proxyToServerConn
	// 4. Test reads from serverSideConn to verify cancel request was sent

	clientConn := newClientConn(t)

	proxyToServerConn, serverSideConn := net.Pipe()
	defer proxyToServerConn.Close()
	defer serverSideConn.Close()

	// Mock resolver that returns valid address and branch name
	resolver := &mockResolver{Address: "mock-address:5432", Branch: "branch1"}
	// Mock dialer that returns the proxy-to-server connection
	md := &mockDialer{Conn: proxyToServerConn}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	// Create a valid cancel request
	cancelRequest := createCancelRequest()

	// Create a goroutine to handle the server side
	type serverResult struct {
		receivedMsg *pgproto3.CancelRequest
		err         error
	}
	serverResultCh := make(chan serverResult, 1)

	go func() {
		// Read the cancel request that should be sent by the proxy
		backend := pgproto3.NewBackend(serverSideConn, serverSideConn)
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			serverResultCh <- serverResult{err: err}
			return
		}

		// Check if we received the correct message type
		receivedCancel, ok := msg.(*pgproto3.CancelRequest)
		if !ok {
			serverResultCh <- serverResult{err: fmt.Errorf("expected CancelRequest, got %T", msg)}
			return
		}

		serverResultCh <- serverResult{receivedMsg: receivedCancel, err: nil}
	}()

	// Test the CancelSession method
	ctx := context.Background()
	err := proxy.CancelSession(ctx, "branch1.example.com", clientConn, cancelRequest)

	// Verify successful cancellation (no error returned)
	require.NoError(t, err)

	// Verify resolver was called with correct server name
	require.Equal(t, "branch1.example.com", resolver.ServerName)

	// Wait for server side to complete and validate results
	result := <-serverResultCh
	require.NoError(t, result.err)
	require.NotNil(t, result.receivedMsg)
	require.Equal(t, cancelRequest, result.receivedMsg)

	require.NotNil(t, md.Call)
	require.Equal(t, "tcp", md.Call.Network)
	require.Equal(t, "mock-address:5432", md.Call.Address)
}

func TestProxy_CancelSession_ResolverError(t *testing.T) {
	clientConn := newClientConn(t)
	resolver := &mockResolver{Error: fmt.Errorf("resolver failed")}
	md := &mockDialer{Err: errors.New("dialer should not be called when resolver fails")}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	ctx := context.Background()
	err := proxy.CancelSession(ctx, "bad.example.com", clientConn, createCancelRequest())

	require.Error(t, err)
	require.Contains(t, err.Error(), "resolver failed")
	require.Equal(t, "bad.example.com", resolver.ServerName)
	require.Equal(t, 1, resolver.CallCount)
	// Dialer should not be called
	require.Nil(t, md.Call)
}

func TestProxy_CancelSession_NetworkDialError(t *testing.T) {
	clientConn := newClientConn(t)
	resolver := &mockResolver{Address: "unreachable.address:9999", Branch: "branch1"}
	md := &mockDialer{Err: fmt.Errorf("connection refused")}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	ctx := context.Background()
	err := proxy.CancelSession(ctx, "branch1.example.com", clientConn, createCancelRequest())

	require.Error(t, err)
	require.Contains(t, err.Error(), "connection refused")
	require.Equal(t, "branch1.example.com", resolver.ServerName)
	require.Equal(t, 1, resolver.CallCount)
	require.NotNil(t, md.Call)
	require.Equal(t, "tcp", md.Call.Network)
	require.Equal(t, "unreachable.address:9999", md.Call.Address)
}

func TestProxy_CancelSession_SendMessageError(t *testing.T) {
	clientConn := newClientConn(t)
	resolver := &mockResolver{Address: "valid.address:5432", Branch: "branch1"}
	proxyToServerConn, serverSideConn := net.Pipe()
	defer proxyToServerConn.Close()
	// serverSideConn will be closed in the dialer action to simulate send failure

	md := &mockDialer{
		Conn:       proxyToServerConn,
		CallAction: func() { serverSideConn.Close() },
	}
	proxy := session.NewProxy(testTracer, resolver, md.Dial, nil)

	ctx := context.Background()
	err := proxy.CancelSession(ctx, "branch1.example.com", clientConn, createCancelRequest())

	require.Error(t, err)
	require.Contains(t, err.Error(), "send startup message")
	require.Equal(t, "branch1.example.com", resolver.ServerName)
	require.Equal(t, 1, resolver.CallCount)
	require.NotNil(t, md.Call)
	require.Equal(t, "tcp", md.Call.Network)
	require.Equal(t, "valid.address:5432", md.Call.Address)

	// Verify that the connection was properly closed (connection cleanup)
	_, writeErr := proxyToServerConn.Write([]byte("test"))
	require.Error(t, writeErr, "connection should be closed after send message failure")
}

// mockIPFilter implements session.IPFilter for testing
type mockIPFilter struct {
	allowed bool
}

func (m *mockIPFilter) IsAllowed(string, string) bool { return m.allowed }

func TestCheckIPAllowed(t *testing.T) {
	tests := map[string]struct {
		filter  session.IPFilter
		wantErr error
	}{
		"nil filter allows all": {
			filter:  nil,
			wantErr: nil,
		},
		"filter allows": {
			filter:  &mockIPFilter{allowed: true},
			wantErr: nil,
		},
		"filter denies": {
			filter:  &mockIPFilter{allowed: false},
			wantErr: session.ErrIPNotAllowed,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := session.CheckIPAllowed(tc.filter, "branch-1", "10.0.0.1:5432")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestProxy_IPFilter(t *testing.T) {
	tests := map[string]struct {
		allowed    bool
		wantErr    error
		wantDialed bool
	}{
		"denied": {
			allowed:    false,
			wantErr:    session.ErrIPNotAllowed,
			wantDialed: false,
		},
		"allowed": {
			allowed:    true,
			wantErr:    nil,
			wantDialed: true,
		},
	}

	setup := func(t *testing.T, allowed bool) (*session.Proxy, *mockDialer) {
		t.Helper()
		proxyToServerConn, serverSideConn := net.Pipe()
		t.Cleanup(func() { proxyToServerConn.Close(); serverSideConn.Close() })

		md := &mockDialer{Conn: proxyToServerConn}
		if !allowed {
			md = &mockDialer{Err: errors.New("should not be called")}
		}

		go func() {
			backend := pgproto3.NewBackend(serverSideConn, serverSideConn)
			backend.ReceiveStartupMessage()
		}()

		proxy := session.NewProxy(testTracer,
			&mockResolver{Address: "mock-address:5432", Branch: "branch1"},
			md.Dial,
			&mockIPFilter{allowed: allowed},
		)
		return proxy, md
	}

	for name, tc := range tests {
		t.Run("CreateBackendSession/"+name, func(t *testing.T) {
			proxy, md := setup(t, tc.allowed)
			clientConn := newClientConn(t)

			sess, err := proxy.CreateBackendSession(context.Background(), "branch1.example.com", clientConn, createStartupMessage())

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, sess)
			} else {
				require.NoError(t, err)
				require.NotNil(t, sess)
			}
			require.Equal(t, tc.wantDialed, md.Call != nil)
		})

		t.Run("CancelSession/"+name, func(t *testing.T) {
			proxy, md := setup(t, tc.allowed)
			clientConn := newClientConn(t)

			err := proxy.CancelSession(context.Background(), "branch1.example.com", clientConn, createCancelRequest())

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantDialed, md.Call != nil)
		})
	}
}
