package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type testCodec struct{}

func (testCodec) Decode(_ []byte) (interface{}, error) { return nil, nil }
func (testCodec) Encode(v interface{}) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return []byte("ok"), nil
}

func TestSendWithoutCodecReturnsError(t *testing.T) {
	c := &Connection{
		server: &WebsocketServer{},
		send:   make(chan []byte, 1),
		done:   make(chan struct{}),
	}
	if err := c.Send([]byte("x")); !errors.Is(err, ErrCodecNotConfigured) {
		t.Fatalf("expected ErrCodecNotConfigured, got=%v", err)
	}
}

func TestSendOnClosedConnectionReturnsError(t *testing.T) {
	done := make(chan struct{})
	close(done)
	c := &Connection{
		server: NewWebsocketServer(":0", WithCodec(testCodec{})),
		send:   make(chan []byte),
		done:   done,
	}
	if err := c.Send([]byte("x")); !errors.Is(err, ErrConnectionClosed) {
		t.Fatalf("expected ErrConnectionClosed, got=%v", err)
	}
}

func TestShutdownContextWithoutStart(t *testing.T) {
	s := NewWebsocketServer(":0")
	if err := s.ShutdownContext(context.Background()); err != nil {
		t.Fatalf("ShutdownContext should not fail when server not started, got=%v", err)
	}
}

func TestWithReadLimitAndSendBufferFallbackToDefault(t *testing.T) {
	s := NewWebsocketServer(":0", WithReadLimit(0), WithSendBuffer(-1))
	if s.opts.readLimit != defaultReadLimit {
		t.Fatalf("expected default readLimit=%d, got=%d", defaultReadLimit, s.opts.readLimit)
	}
	if s.opts.sendBuffer != defaultSendBuffer {
		t.Fatalf("expected default sendBuffer=%d, got=%d", defaultSendBuffer, s.opts.sendBuffer)
	}
}

func TestWithLoggerOnlyAffectsCurrentInstance(t *testing.T) {
	logger1 := &testLogger{}
	logger2 := &testLogger{}

	s1 := NewWebsocketServer(":0", WithLogger(logger1))
	s2 := NewWebsocketServer(":0", WithLogger(logger2))

	if s1.Logger() != logger1 {
		t.Fatal("expected s1 logger to be logger1")
	}
	if s2.Logger() != logger2 {
		t.Fatal("expected s2 logger to be logger2")
	}
}

type passthroughCodec struct{}

func (passthroughCodec) Decode(data []byte) (interface{}, error) { return data, nil }
func (passthroughCodec) Encode(v interface{}) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected payload type: %T", v)
	}
	return b, nil
}

type echoHandler struct {
	BaseEventHandler
	connected chan struct{}
	received  chan []byte
}

func (h *echoHandler) OnConnect(_ IConnection) {
	select {
	case h.connected <- struct{}{}:
	default:
	}
}

func (h *echoHandler) OnMessage(conn IConnection, msg interface{}) {
	b, ok := msg.([]byte)
	if !ok {
		return
	}
	select {
	case h.received <- append([]byte(nil), b...):
	default:
	}
	_ = conn.Send(append([]byte("echo:"), b...))
}

type panicThenContinueHandler struct {
	BaseEventHandler
	count         atomic.Int32
	secondHandled chan struct{}
}

func (h *panicThenContinueHandler) OnMessage(_ IConnection, msg interface{}) {
	if h.count.Add(1) == 1 {
		panic("panic on first message")
	}
	b, _ := msg.([]byte)
	if string(b) == "second" {
		select {
		case h.secondHandled <- struct{}{}:
		default:
		}
	}
}

type disconnectNotifyHandler struct {
	BaseEventHandler
	disconnected chan error
}

func (h *disconnectNotifyHandler) OnDisconnect(_ IConnection, err error) {
	select {
	case h.disconnected <- err:
	default:
	}
}

type serverPushOnConnectHandler struct {
	BaseEventHandler
	payload []byte
}

func (h *serverPushOnConnectHandler) OnConnect(conn IConnection) {
	_ = conn.Send(h.payload)
}

type concurrentEchoHandler struct {
	BaseEventHandler
	received atomic.Int64
}

func (h *concurrentEchoHandler) OnMessage(conn IConnection, msg interface{}) {
	b, ok := msg.([]byte)
	if !ok {
		return
	}
	h.received.Add(1)
	_ = conn.Send(append([]byte("echo:"), b...))
}

func TestRealClientConnectAndEcho(t *testing.T) {
	addr := testFreeAddr(t)
	handler := &echoHandler{
		connected: make(chan struct{}, 1),
		received:  make(chan []byte, 1),
	}

	s := NewWebsocketServer(
		addr,
		WithCodec(passthroughCodec{}),
		WithEventHandler(handler),
	)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- s.Start()
	}()

	client := dialWSWithRetry(t, "ws://"+addr+"/")
	defer client.Close()
	defer shutdownServerForTest(t, s, serverDone)

	waitSignal(t, handler.connected, "server OnConnect")

	if err := client.WriteMessage(websocket.BinaryMessage, []byte("hello")); err != nil {
		t.Fatalf("client write failed: %v", err)
	}

	waitBytes(t, handler.received, []byte("hello"), "server received payload")

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, data, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if msgType != websocket.BinaryMessage {
		t.Fatalf("expected BinaryMessage, got=%d", msgType)
	}
	if string(data) != "echo:hello" {
		t.Fatalf("unexpected echo payload: %q", string(data))
	}
}

func TestRealClientPanicInOnMessageContinuesLoop(t *testing.T) {
	addr := testFreeAddr(t)
	handler := &panicThenContinueHandler{
		secondHandled: make(chan struct{}, 1),
	}

	s := NewWebsocketServer(
		addr,
		WithCodec(passthroughCodec{}),
		WithEventHandler(handler),
	)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- s.Start()
	}()

	client := dialWSWithRetry(t, "ws://"+addr+"/")
	defer client.Close()
	defer shutdownServerForTest(t, s, serverDone)

	if err := client.WriteMessage(websocket.BinaryMessage, []byte("first")); err != nil {
		t.Fatalf("client write first message failed: %v", err)
	}
	if err := client.WriteMessage(websocket.BinaryMessage, []byte("second")); err != nil {
		t.Fatalf("client write second message failed: %v", err)
	}

	waitSignal(t, handler.secondHandled, "second message handled")
}

func TestRealClientCloseTriggersOnDisconnect(t *testing.T) {
	addr := testFreeAddr(t)
	handler := &disconnectNotifyHandler{
		disconnected: make(chan error, 1),
	}

	s := NewWebsocketServer(
		addr,
		WithCodec(passthroughCodec{}),
		WithEventHandler(handler),
	)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- s.Start()
	}()

	client := dialWSWithRetry(t, "ws://"+addr+"/")
	defer shutdownServerForTest(t, s, serverDone)

	if err := client.Close(); err != nil {
		t.Fatalf("client close failed: %v", err)
	}

	select {
	case err := <-handler.disconnected:
		if err == nil {
			t.Fatal("expected non-nil disconnect error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for OnDisconnect callback")
	}
}

func TestRealClientReceivesServerPushOnConnect(t *testing.T) {
	addr := testFreeAddr(t)
	handler := &serverPushOnConnectHandler{
		payload: []byte("welcome"),
	}

	s := NewWebsocketServer(
		addr,
		WithCodec(passthroughCodec{}),
		WithEventHandler(handler),
	)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- s.Start()
	}()

	client := dialWSWithRetry(t, "ws://"+addr+"/")
	defer client.Close()
	defer shutdownServerForTest(t, s, serverDone)

	_ = client.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, data, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("client read push failed: %v", err)
	}
	if msgType != websocket.BinaryMessage {
		t.Fatalf("expected BinaryMessage, got=%d", msgType)
	}
	if string(data) != "welcome" {
		t.Fatalf("unexpected push payload: %q", string(data))
	}
}

func TestConcurrentClientsEchoStress(t *testing.T) {
	addr := testFreeAddr(t)
	handler := &concurrentEchoHandler{}

	s := NewWebsocketServer(
		addr,
		WithCodec(passthroughCodec{}),
		WithEventHandler(handler),
		WithSendBuffer(512),
	)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- s.Start()
	}()
	defer shutdownServerForTest(t, s, serverDone)

	const (
		clientCount       = 20
		messagesPerClient = 10
	)
	totalMessages := clientCount * messagesPerClient

	var wg sync.WaitGroup
	errCh := make(chan error, clientCount)

	for i := 0; i < clientCount; i++ {
		clientID := i
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := dialWSWithRetry(t, "ws://"+addr+"/")
			defer client.Close()
			_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))

			for j := 0; j < messagesPerClient; j++ {
				payload := fmt.Sprintf("c%d-m%d", clientID, j)
				if err := client.WriteMessage(websocket.BinaryMessage, []byte(payload)); err != nil {
					errCh <- fmt.Errorf("write failed client=%d msg=%d err=%w", clientID, j, err)
					return
				}

				msgType, data, err := client.ReadMessage()
				if err != nil {
					errCh <- fmt.Errorf("read failed client=%d msg=%d err=%w", clientID, j, err)
					return
				}
				if msgType != websocket.BinaryMessage {
					errCh <- fmt.Errorf("unexpected msg type client=%d msg=%d got=%d", clientID, j, msgType)
					return
				}

				expected := "echo:" + payload
				if string(data) != expected {
					errCh <- fmt.Errorf("unexpected echo client=%d msg=%d got=%q want=%q", clientID, j, string(data), expected)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	got := int(handler.received.Load())
	if got != totalMessages {
		t.Fatalf("unexpected server received count: got=%d want=%d", got, totalMessages)
	}
}

func BenchmarkWebsocketEchoRunParallel(b *testing.B) {
	addr := testFreeAddr(b)
	handler := &concurrentEchoHandler{}

	s := NewWebsocketServer(
		addr,
		WithCodec(passthroughCodec{}),
		WithEventHandler(handler),
		WithSendBuffer(1024),
	)

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- s.Start()
	}()
	b.Cleanup(func() {
		shutdownServerForTest(b, s, serverDone)
	})

	var seq atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		client := dialWSWithRetry(b, "ws://"+addr+"/")
		defer client.Close()

		for pb.Next() {
			id := seq.Add(1)
			payload := fmt.Sprintf("bench-%d", id)

			if err := client.WriteMessage(websocket.BinaryMessage, []byte(payload)); err != nil {
				b.Errorf("benchmark write failed: %v", err)
				return
			}
			_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))

			msgType, data, err := client.ReadMessage()
			if err != nil {
				b.Errorf("benchmark read failed: %v", err)
				return
			}
			if msgType != websocket.BinaryMessage {
				b.Errorf("benchmark unexpected message type: %d", msgType)
				return
			}

			expected := "echo:" + payload
			if string(data) != expected {
				b.Errorf("benchmark unexpected echo: got=%q want=%q", string(data), expected)
				return
			}
		}
	})
}

func testFreeAddr(t testing.TB) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral addr failed: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func dialWSWithRetry(t testing.TB, url string) *websocket.Conn {
	t.Helper()

	var lastErr error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("dial websocket timeout: %v", lastErr)
	return nil
}

func shutdownServerForTest(t testing.TB, s *WebsocketServer, serverDone <-chan error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := s.ShutdownContext(ctx); err != nil {
		t.Fatalf("shutdown server failed: %v", err)
	}

	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("server returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not exit after shutdown")
	}
}

func waitSignal(t testing.TB, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", name)
	}
}

func waitBytes(t testing.TB, ch <-chan []byte, want []byte, name string) {
	t.Helper()
	select {
	case got := <-ch:
		if string(got) != string(want) {
			t.Fatalf("%s mismatch: got=%q want=%q", name, string(got), string(want))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", name)
	}
}

type testLogger struct{}

func (testLogger) Debugf(string, ...any) {}
func (testLogger) Infof(string, ...any)  {}
func (testLogger) Warnf(string, ...any)  {}
func (testLogger) Errorf(string, ...any) {}
