package serverless

import (
	"context"
	"encoding/binary"
	"net"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"xata/services/gateway/metrics"
	"xata/services/gateway/session"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestSSLRequestCodeDetection(t *testing.T) {
	tests := map[string]struct {
		data     []byte
		wantCode uint32
		isSSL    bool
		isGSSEnc bool
	}{
		"SSL request": {
			data:     makeSpecialMessage(sslRequestCode),
			wantCode: 80877103,
			isSSL:    true,
		},
		"GSS encryption request": {
			data:     makeSpecialMessage(gssEncRequestCode),
			wantCode: 80877104,
			isGSSEnc: true,
		},
		"startup message": {
			data:  makeSpecialMessage(196608), // protocol version 3.0
			isSSL: false,
		},
		"too short": {
			data:  []byte{0, 0, 0, 8},
			isSSL: false,
		},
		"too long": {
			data:  make([]byte, 100),
			isSSL: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			isSSLOrGSS := false
			if len(tc.data) == 8 {
				code := binary.BigEndian.Uint32(tc.data[4:8])
				isSSLOrGSS = code == sslRequestCode || code == gssEncRequestCode

				if tc.wantCode != 0 {
					require.Equal(t, tc.wantCode, code)
				}
			}

			if tc.isSSL || tc.isGSSEnc {
				require.True(t, isSSLOrGSS)
			} else {
				require.False(t, isSSLOrGSS)
			}
		})
	}
}

func makeSpecialMessage(code uint32) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], 8)    // length
	binary.BigEndian.PutUint32(data[4:8], code) // code
	return data
}

// wsTestEnv holds the shared state for WebSocket handler tests.
type wsTestEnv struct {
	ws  *websocket.Conn
	ctx context.Context
}

// setupWebSocketTest creates a WebSocket handler wired to a mock PostgreSQL
// server at pgAddr, performs the startup handshake (including reading
// AuthenticationOk), and returns the ready-to-use environment.
func setupWebSocketTest(t *testing.T, pgAddr string) *wsTestEnv {
	t.Helper()

	wsURL := setupWSServer(t, pgAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)

	// Send startup message (PostgreSQL protocol version 3.0)
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: 196608, // 3.0
		Parameters: map[string]string{
			"user":     "testuser",
			"database": "testdb",
		},
	}
	startupData, err := startupMsg.Encode(nil)
	require.NoError(t, err)
	err = ws.Write(ctx, websocket.MessageBinary, startupData)
	require.NoError(t, err)

	// Read AuthenticationOk
	_, data, err := ws.Read(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	return &wsTestEnv{ws: ws, ctx: ctx}
}

func TestWebSocketHandler_ClientEndClosesConnection(t *testing.T) {
	// Start a mock PostgreSQL server
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	pgServerDone := make(chan struct{})
	go func() {
		defer close(pgServerDone)
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)

		// Receive startup message
		_, err = backend.ReceiveStartupMessage()
		if err != nil {
			t.Logf("mock pg: receive startup: %v", err)
			return
		}

		// Send auth OK and ready for query
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			t.Logf("mock pg: flush auth: %v", err)
			return
		}

		// Wait for Terminate message
		for {
			msg, err := backend.Receive()
			if err != nil {
				t.Logf("mock pg: receive: %v", err)
				return
			}
			if _, ok := msg.(*pgproto3.Terminate); ok {
				t.Log("mock pg: received Terminate, closing connection")
				return
			}
		}
	}()

	env := setupWebSocketTest(t, pgListener.Addr().String())

	// Send Terminate message (this is what client.end() does)
	terminateMsg := &pgproto3.Terminate{}
	terminateData, err := terminateMsg.Encode(nil)
	require.NoError(t, err)
	err = env.ws.Write(env.ctx, websocket.MessageBinary, terminateData)
	require.NoError(t, err)

	// The WebSocket should close within a reasonable time
	closeDone := make(chan error, 1)
	go func() {
		_, _, err := env.ws.Read(env.ctx)
		closeDone <- err
	}()

	select {
	case err := <-closeDone:
		t.Logf("WebSocket closed with: %v", err)
		require.Error(t, err, "expected WebSocket to be closed after Terminate")
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket did not close within timeout - client.end() would hang")
	}

	select {
	case <-pgServerDone:
	case <-time.After(time.Second):
		t.Log("mock PostgreSQL server did not finish in time")
	}
}

func TestExtractHostFromStartup(t *testing.T) {
	tests := map[string]struct {
		data []byte
		want string
	}{
		"with host parameter": {
			data: func() []byte {
				msg := &pgproto3.StartupMessage{
					ProtocolVersion: 196608,
					Parameters: map[string]string{
						"user":     "testuser",
						"database": "testdb",
						"host":     "my-branch.example.com",
					},
				}
				data, _ := msg.Encode(nil)
				return data
			}(),
			want: "my-branch.example.com",
		},
		"without host parameter": {
			data: func() []byte {
				msg := &pgproto3.StartupMessage{
					ProtocolVersion: 196608,
					Parameters: map[string]string{
						"user":     "testuser",
						"database": "testdb",
					},
				}
				data, _ := msg.Encode(nil)
				return data
			}(),
			want: "",
		},
		"too short": {
			data: []byte{0, 0, 0, 4},
			want: "",
		},
		"empty": {
			data: nil,
			want: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := extractStartupParam(tc.data, "host")
			require.Equal(t, tc.want, got)
		})
	}
}

type mockIPFilter struct {
	allowed bool
}

func (m *mockIPFilter) IsAllowed(string, string) bool { return m.allowed }

// setupWSServer creates an echo HTTP server with the WebSocket handler
// wired to the given pgAddr and returns the WebSocket URL.
func setupWSServer(t *testing.T, pgAddr string, opts ...func(*handler)) string {
	t.Helper()

	tracer := tracenoop.NewTracerProvider().Tracer("")
	gwMetrics, err := metrics.New(noop.NewMeterProvider().Meter(""))
	require.NoError(t, err)

	resolver := session.ResolverFunc(func(_ context.Context, _ string) (*session.Branch, error) {
		return &session.Branch{ID: "test-branch", Address: pgAddr}, nil
	})

	dialer := session.NewClusterDialer(session.ClusterDialerConfiguration{
		ReactivateTimeout:   time.Second,
		StatusCheckInterval: 100 * time.Millisecond,
	})

	h := &handler{resolver: resolver, dialer: dialer, tracer: tracer, metrics: gwMetrics}
	for _, opt := range opts {
		opt(h)
	}
	e := echo.New()
	e.GET("/v2", h.Websocket)
	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return "ws" + srv.URL[4:] + "/v2"
}

func withIPFilter(f session.IPFilter) func(*handler) {
	return func(h *handler) { h.ipFilter = f }
}

func TestWebSocketHandler_PipelineConnect_CleartextAuth(t *testing.T) {
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	const password = "secret"
	pgDone := make(chan struct{})
	go func() {
		defer close(pgDone)
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)
		_, err = backend.ReceiveStartupMessage()
		if err != nil {
			t.Logf("mock pg: receive startup: %v", err)
			return
		}

		// Request cleartext password
		backend.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := backend.Flush(); err != nil {
			t.Logf("mock pg: flush auth request: %v", err)
			return
		}

		backend.SetAuthType(pgproto3.AuthTypeCleartextPassword)
		msg, err := backend.Receive()
		if err != nil {
			t.Logf("mock pg: receive password: %v", err)
			return
		}
		pwMsg, ok := msg.(*pgproto3.PasswordMessage)
		if !ok {
			t.Logf("mock pg: expected PasswordMessage, got %T", msg)
			return
		}
		if pwMsg.Password != password {
			t.Logf("mock pg: wrong password: %q", pwMsg.Password)
			return
		}

		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
		backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		backend.Flush()
	}()

	wsURL := setupWSServer(t, pgListener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer ws.CloseNow()

	// Build coalesced startup + password
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}
	startupData, err := startupMsg.Encode(nil)
	require.NoError(t, err)
	pwMsg := &pgproto3.PasswordMessage{Password: password}
	pwData, err := pwMsg.Encode(nil)
	require.NoError(t, err)

	// Send coalesced in one WebSocket message
	coalesced := slices.Concat(startupData, pwData)
	err = ws.Write(ctx, websocket.MessageBinary, coalesced)
	require.NoError(t, err)

	// Read auth response — all PG messages arrive in a single WebSocket frame
	gotAuthOk := false
	gotReady := false
	for !gotReady {
		_, data, err := ws.Read(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, data)

		for len(data) > 0 {
			require.GreaterOrEqual(t, len(data), 5, "truncated PG message header")
			msgType := data[0]
			msgLen := binary.BigEndian.Uint32(data[1:5])
			totalLen := 1 + int(msgLen)
			require.GreaterOrEqual(t, len(data), totalLen, "truncated PG message body")

			switch msgType {
			case 'R': // Authentication
				authType := binary.BigEndian.Uint32(data[5:9])
				require.Equal(t, uint32(0), authType, "expected AuthenticationOk")
				gotAuthOk = true
			case 'S': // ParameterStatus
			case 'K': // BackendKeyData
			case 'Z': // ReadyForQuery
				gotReady = true
			default:
				t.Fatalf("unexpected message type %c", msgType)
			}
			data = data[totalLen:]
		}
	}
	require.True(t, gotAuthOk)

	<-pgDone
}

func TestWebSocketHandler_PipelineConnect_CleartextAuth_WithQuery(t *testing.T) {
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	const password = "secret"
	pgDone := make(chan struct{})
	go func() {
		defer close(pgDone)
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)
		_, err = backend.ReceiveStartupMessage()
		if err != nil {
			t.Logf("mock pg: receive startup: %v", err)
			return
		}

		// Request cleartext password
		backend.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := backend.Flush(); err != nil {
			t.Logf("mock pg: flush auth request: %v", err)
			return
		}

		backend.SetAuthType(pgproto3.AuthTypeCleartextPassword)
		msg, err := backend.Receive()
		if err != nil {
			t.Logf("mock pg: receive password: %v", err)
			return
		}
		pwMsg, ok := msg.(*pgproto3.PasswordMessage)
		if !ok {
			t.Logf("mock pg: expected PasswordMessage, got %T", msg)
			return
		}
		if pwMsg.Password != password {
			t.Logf("mock pg: wrong password: %q", pwMsg.Password)
			return
		}

		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
		backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			t.Logf("mock pg: flush auth response: %v", err)
			return
		}

		// Receive the pipelined query (Parse/Bind/Describe/Execute/Sync)
		for {
			msg, err := backend.Receive()
			if err != nil {
				t.Logf("mock pg: receive query: %v", err)
				return
			}
			if _, ok := msg.(*pgproto3.Sync); ok {
				break
			}
		}

		// Respond with query result
		backend.Send(&pgproto3.ParseComplete{})
		backend.Send(&pgproto3.BindComplete{})
		backend.Send(&pgproto3.RowDescription{
			Fields: []pgproto3.FieldDescription{{Name: []byte("num"), DataTypeOID: 23, DataTypeSize: 4, Format: 0}},
		})
		backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}})
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if err := backend.Flush(); err != nil {
			t.Logf("mock pg: flush query response: %v", err)
			return
		}
	}()

	wsURL := setupWSServer(t, pgListener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer ws.CloseNow()

	// Build coalesced startup + password + query (Parse/Bind/Describe/Execute/Sync)
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}
	startupData, err := startupMsg.Encode(nil)
	require.NoError(t, err)

	pwMsg := &pgproto3.PasswordMessage{Password: password}
	pwData, err := pwMsg.Encode(nil)
	require.NoError(t, err)

	// Build extended query protocol messages
	parseMsg := &pgproto3.Parse{Query: "SELECT 1 as num"}
	bindMsg := &pgproto3.Bind{}
	descMsg := &pgproto3.Describe{ObjectType: 'P'}
	execMsg := &pgproto3.Execute{}
	syncMsg := &pgproto3.Sync{}

	var queryData []byte
	queryData, _ = parseMsg.Encode(queryData)
	queryData, _ = bindMsg.Encode(queryData)
	queryData, _ = descMsg.Encode(queryData)
	queryData, _ = execMsg.Encode(queryData)
	queryData, _ = syncMsg.Encode(queryData)

	// Send everything coalesced in one WebSocket message
	coalesced := slices.Concat(startupData, pwData, queryData)
	err = ws.Write(ctx, websocket.MessageBinary, coalesced)
	require.NoError(t, err)

	// Read auth response + query result
	gotAuthOk := false
	gotReady := false
	gotDataRow := false
	readyCount := 0
	for readyCount < 2 {
		_, data, err := ws.Read(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, data)

		for len(data) > 0 {
			require.GreaterOrEqual(t, len(data), 5, "truncated PG message header")
			msgType := data[0]
			msgLen := binary.BigEndian.Uint32(data[1:5])
			totalLen := 1 + int(msgLen)
			require.GreaterOrEqual(t, len(data), totalLen, "truncated PG message body")

			switch msgType {
			case 'R': // Authentication
				authType := binary.BigEndian.Uint32(data[5:9])
				require.Equal(t, uint32(0), authType, "expected AuthenticationOk")
				gotAuthOk = true
			case 'S': // ParameterStatus
			case 'K': // BackendKeyData
			case 'Z': // ReadyForQuery
				readyCount++
				gotReady = true
			case '1': // ParseComplete
			case '2': // BindComplete
			case 'T': // RowDescription
			case 'D': // DataRow
				gotDataRow = true
			case 'C': // CommandComplete
			default:
				t.Fatalf("unexpected message type %c", msgType)
			}
			data = data[totalLen:]
		}
	}
	require.True(t, gotAuthOk, "did not receive AuthenticationOk")
	require.True(t, gotReady, "did not receive ReadyForQuery")
	require.True(t, gotDataRow, "did not receive DataRow — pipelined query was not forwarded")

	<-pgDone
}

func TestWebSocketHandler_PipelineConnect_TrustAuth(t *testing.T) {
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	pgDone := make(chan struct{})
	go func() {
		defer close(pgDone)
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)
		_, err = backend.ReceiveStartupMessage()
		if err != nil {
			t.Logf("mock pg: receive startup: %v", err)
			return
		}

		// Trust auth - immediate AuthOk
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		backend.Flush()
	}()

	wsURL := setupWSServer(t, pgListener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer ws.CloseNow()

	// Build coalesced startup + password (password should be ignored for trust)
	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}
	startupData, err := startupMsg.Encode(nil)
	require.NoError(t, err)
	pwMsg := &pgproto3.PasswordMessage{Password: "unused"}
	pwData, err := pwMsg.Encode(nil)
	require.NoError(t, err)

	coalesced := slices.Concat(startupData, pwData)
	err = ws.Write(ctx, websocket.MessageBinary, coalesced)
	require.NoError(t, err)

	// Should still get AuthOk and ReadyForQuery
	gotReady := false
	for !gotReady {
		_, data, err := ws.Read(ctx)
		require.NoError(t, err)

		for len(data) > 0 {
			require.GreaterOrEqual(t, len(data), 5, "truncated PG message header")
			msgType := data[0]
			msgLen := binary.BigEndian.Uint32(data[1:5])
			totalLen := 1 + int(msgLen)
			require.GreaterOrEqual(t, len(data), totalLen, "truncated PG message body")

			switch msgType {
			case 'R', 'S', 'K':
			case 'Z':
				gotReady = true
			default:
				t.Fatalf("unexpected message type %c", msgType)
			}
			data = data[totalLen:]
		}
	}

	<-pgDone
}

func TestWebSocketHandler_NoPipeline_StillWorks(t *testing.T) {
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	pgDone := make(chan struct{})
	go func() {
		defer close(pgDone)
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)
		_, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}

		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		backend.Flush()

		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}
			if _, ok := msg.(*pgproto3.Terminate); ok {
				return
			}
		}
	}()

	// Use the standard (non-pipelined) test helper
	env := setupWebSocketTest(t, pgListener.Addr().String())

	// Send Terminate to close cleanly
	terminateData, err := (&pgproto3.Terminate{}).Encode(nil)
	require.NoError(t, err)
	err = env.ws.Write(env.ctx, websocket.MessageBinary, terminateData)
	require.NoError(t, err)

	closeDone := make(chan error, 1)
	go func() {
		_, _, err := env.ws.Read(env.ctx)
		closeDone <- err
	}()

	select {
	case err := <-closeDone:
		require.Error(t, err, "expected WebSocket to close after Terminate")
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket did not close")
	}

	select {
	case <-pgDone:
	case <-time.After(time.Second):
	}
}

func TestWebSocketHandler_PostgresCloseClosesWebSocket(t *testing.T) {
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	go func() {
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)

		_, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}

		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		backend.Flush()

		// Close immediately to simulate PostgreSQL closing the connection
		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}()

	env := setupWebSocketTest(t, pgListener.Addr().String())

	closeDone := make(chan error, 1)
	go func() {
		for {
			_, _, err := env.ws.Read(env.ctx)
			if err != nil {
				closeDone <- err
				return
			}
		}
	}()

	select {
	case err := <-closeDone:
		t.Logf("WebSocket closed with: %v", err)
		require.Error(t, err, "expected WebSocket to be closed when PostgreSQL closes")
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket did not close when PostgreSQL closed - would cause client to hang")
	}
}

func TestWebSocketHandler_IPFilterDenied(t *testing.T) {
	wsURL := setupWSServer(t, "127.0.0.1:0", withIPFilter(&mockIPFilter{allowed: false}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer ws.CloseNow()

	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}
	startupData, err := startupMsg.Encode(nil)
	require.NoError(t, err)
	err = ws.Write(ctx, websocket.MessageBinary, startupData)
	require.NoError(t, err)

	// Connection should be closed with policy violation
	_, _, err = ws.Read(ctx)
	require.Error(t, err)
	var closeErr websocket.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, websocket.StatusPolicyViolation, closeErr.Code)
}

func TestWebSocketHandler_IPFilterAllowed(t *testing.T) {
	pgListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer pgListener.Close()

	go func() {
		conn, err := pgListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		backend := pgproto3.NewBackend(conn, conn)
		_, _ = backend.ReceiveStartupMessage()
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		backend.Flush()
	}()

	wsURL := setupWSServer(t, pgListener.Addr().String(), withIPFilter(&mockIPFilter{allowed: true}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, wsURL, nil)
	require.NoError(t, err)
	defer ws.CloseNow()

	startupMsg := &pgproto3.StartupMessage{
		ProtocolVersion: 196608,
		Parameters:      map[string]string{"user": "testuser", "database": "testdb"},
	}
	startupData, err := startupMsg.Encode(nil)
	require.NoError(t, err)
	err = ws.Write(ctx, websocket.MessageBinary, startupData)
	require.NoError(t, err)

	// Should receive auth response — connection was allowed through the filter
	_, data, err := ws.Read(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	ws.Close(websocket.StatusNormalClosure, "")
}
