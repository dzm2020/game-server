package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type datagramEchoHandler struct {
	received atomic.Int64
}

func (h *datagramEchoHandler) OnConnect(conn IConnection) error {
	return nil
}

func (h *datagramEchoHandler) OnMessage(conn IConnection, data []byte) (int, error) {
	copyData := append([]byte(nil), data...)
	if err := conn.Send(copyData); err != nil {
		return len(data), err
	}
	h.received.Add(1)
	return len(data), nil
}

func (h *datagramEchoHandler) OnClose(conn IConnection, err error) {}

func startUDPServer(tb testing.TB, handler IHandler, options ServerOptions) (*UDPServer, string) {
	tb.Helper()
	srv, err := NewServer(handler, "udp://127.0.0.1:0", normalization(options))
	if err != nil {
		tb.Fatalf("create udp server failed: %v", err)
	}
	udpSrv, ok := srv.(*UDPServer)
	if !ok {
		tb.Fatalf("server type mismatch: %T", srv)
	}
	if err := udpSrv.Start(); err != nil {
		tb.Fatalf("start udp server failed: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		udpSrv.Shutdown(ctx)
	})
	return udpSrv, udpSrv.conn.LocalAddr().String()
}

func reserveTCPAddr(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve tcp addr failed: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func startWebSocketServer(tb testing.TB, handler IHandler, options ServerOptions) (*WebSocketServer, string) {
	tb.Helper()
	addr := reserveTCPAddr(tb)
	srv, err := NewServer(handler, "ws://"+addr+"/ws", normalization(options))
	if err != nil {
		tb.Fatalf("create websocket server failed: %v", err)
	}
	wsSrv, ok := srv.(*WebSocketServer)
	if !ok {
		tb.Fatalf("server type mismatch: %T", srv)
	}
	if err := wsSrv.Start(); err != nil {
		tb.Fatalf("start websocket server failed: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		wsSrv.Shutdown(ctx)
	})

	wsURL := (&url.URL{Scheme: "ws", Host: addr, Path: "/ws"}).String()
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, _, dialErr := websocket.DefaultDialer.Dial(wsURL, nil)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			tb.Fatalf("wait websocket ready timeout: %v", dialErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return wsSrv, wsURL
}

func TestUDP_NoPacketLoss_MultiConnections(t *testing.T) {
	if os.Getenv("NETWORK_STRESS") != "1" {
		t.Skip("set NETWORK_STRESS=1 to run udp packet-loss stress test")
	}

	handler := &datagramEchoHandler{}
	opts := ServerOptions{
		SendChanSize:        8192,
		HeartIntervalSecond: 30,
		UdpOptions: UdpServerOptions{
			ReadChanSize: 4096,
			SendChanSize: 8192,
		},
	}
	_, addr := startUDPServer(t, handler, opts)

	const (
		clientCount      = 128
		messagePerClient = 200
		payloadSize      = 128
	)

	var wg sync.WaitGroup
	errCh := make(chan error, clientCount)

	for i := 0; i < clientCount; i++ {
		clientID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			raddr, err := net.ResolveUDPAddr("udp", addr)
			if err != nil {
				errCh <- fmt.Errorf("resolve udp addr failed client=%d err=%w", clientID, err)
				return
			}
			conn, err := net.DialUDP("udp", nil, raddr)
			if err != nil {
				errCh <- fmt.Errorf("udp dial failed client=%d err=%w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			for seq := 0; seq < messagePerClient; seq++ {
				payload := makePayload(clientID, seq, payloadSize)
				if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
					errCh <- fmt.Errorf("set write deadline failed client=%d err=%w", clientID, err)
					return
				}
				if _, err := conn.Write(payload); err != nil {
					errCh <- fmt.Errorf("udp write failed client=%d seq=%d err=%w", clientID, seq, err)
					return
				}
				buf := make([]byte, payloadSize)
				if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					errCh <- fmt.Errorf("set read deadline failed client=%d err=%w", clientID, err)
					return
				}
				n, readErr := conn.Read(buf)
				if readErr != nil {
					errCh <- fmt.Errorf("udp read failed client=%d seq=%d err=%w", clientID, seq, readErr)
					return
				}
				resp := buf[:n]
				if !bytes.Equal(resp, payload) {
					errCh <- fmt.Errorf("udp echo mismatch client=%d seq=%d", clientID, seq)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("udp packet-loss check failed: %v", err)
		}
	}

	expected := int64(clientCount * messagePerClient)
	if got := handler.received.Load(); got != expected {
		t.Fatalf("udp server receive count mismatch: got=%d want=%d", got, expected)
	}
}

func TestWebSocket_NoPacketLoss_MultiConnections(t *testing.T) {
	if os.Getenv("NETWORK_STRESS") != "1" {
		t.Skip("set NETWORK_STRESS=1 to run websocket packet-loss stress test")
	}

	handler := &datagramEchoHandler{}
	opts := ServerOptions{
		SendChanSize:        8192,
		HeartIntervalSecond: 30,
	}
	_, wsURL := startWebSocketServer(t, handler, opts)

	const (
		clientCount      = 128
		messagePerClient = 200
		payloadSize      = 128
	)

	var wg sync.WaitGroup
	errCh := make(chan error, clientCount)

	for i := 0; i < clientCount; i++ {
		clientID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				errCh <- fmt.Errorf("websocket dial failed client=%d err=%w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			for seq := 0; seq < messagePerClient; seq++ {
				payload := makePayload(clientID, seq, payloadSize)
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
					errCh <- fmt.Errorf("websocket write failed client=%d seq=%d err=%w", clientID, seq, err)
					return
				}

				_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
				msgType, resp, readErr := conn.ReadMessage()
				if readErr != nil {
					errCh <- fmt.Errorf("websocket read failed client=%d seq=%d err=%w", clientID, seq, readErr)
					return
				}
				if msgType != websocket.BinaryMessage || !bytes.Equal(resp, payload) {
					errCh <- fmt.Errorf("websocket echo mismatch client=%d seq=%d", clientID, seq)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("websocket packet-loss check failed: %v", err)
		}
	}

	expected := int64(clientCount * messagePerClient)
	if got := handler.received.Load(); got != expected {
		t.Fatalf("websocket server receive count mismatch: got=%d want=%d", got, expected)
	}
}

func makeDatagramPayload(clientID, seq, size int) []byte {
	if size < 8 {
		size = 8
	}
	data := make([]byte, size)
	binary.BigEndian.PutUint32(data[:4], uint32(clientID))
	binary.BigEndian.PutUint32(data[4:8], uint32(seq))
	fill := byte((clientID + seq) % 241)
	for i := 8; i < len(data); i++ {
		data[i] = fill
	}
	return data
}

func benchmarkDatagramServerThroughput(
	b *testing.B,
	benchName string,
	startFn func(testing.TB, IHandler, ServerOptions) (string, func()),
	payloadLen int,
	connections int,
) {
	b.Run(benchName, func(b *testing.B) {
		handler := &datagramEchoHandler{}
		opts := ServerOptions{
			SendChanSize:        16384,
			HeartIntervalSecond: 30,
			UdpOptions: UdpServerOptions{
				ReadChanSize: 8192,
				SendChanSize: 16384,
			},
		}
		addrOrURL, shutdown := startFn(b, handler, opts)
		defer shutdown()

		b.SetBytes(int64(payloadLen))
		b.ReportAllocs()

		var (
			ops     atomic.Int64
			wg      sync.WaitGroup
			readyWG sync.WaitGroup
			errMu   sync.Mutex
			errs    []error
		)
		startCh := make(chan struct{})

		recordErr := func(err error) {
			if err == nil {
				return
			}
			errMu.Lock()
			if len(errs) < 8 {
				errs = append(errs, err)
			}
			errMu.Unlock()
		}

		readyWG.Add(connections)
		for workerID := 0; workerID < connections; workerID++ {
			workerID := workerID
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, createErr := net.DialTimeout("udp", addrOrURL, 5*time.Second)
				if createErr != nil {
					recordErr(fmt.Errorf("udp dial failed worker=%d err=%w", workerID, createErr))
					readyWG.Done()
					return
				}
				defer func() { _ = conn.Close() }()
				readyWG.Done()
				<-startCh

				buf := make([]byte, payloadLen)
				seq := 0
				for {
					cur := int(ops.Add(1))
					if cur > b.N {
						return
					}
					payload := makeDatagramPayload(workerID, seq, payloadLen)
					seq++
					_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
					if _, err := conn.Write(payload); err != nil {
						recordErr(fmt.Errorf("udp write failed worker=%d err=%w", workerID, err))
						return
					}

					_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
					n, err := conn.Read(buf)
					if err != nil {
						recordErr(fmt.Errorf("udp read failed worker=%d err=%w", workerID, err))
						return
					}
					if !bytes.Equal(buf[:n], payload) {
						recordErr(fmt.Errorf("udp echo mismatch worker=%d", workerID))
						return
					}
				}
			}()
		}

		readyWG.Wait()
		b.ResetTimer()
		close(startCh)
		wg.Wait()
		b.StopTimer()

		if len(errs) > 0 {
			for _, err := range errs {
				b.Error(err)
			}
		}
	})
}

func benchmarkWebSocketServerThroughput(
	b *testing.B,
	benchName string,
	payloadLen int,
	connections int,
) {
	b.Run(benchName, func(b *testing.B) {
		handler := &datagramEchoHandler{}
		opts := ServerOptions{
			SendChanSize:        16384,
			HeartIntervalSecond: 30,
		}
		_, wsURL := startWebSocketServer(b, handler, opts)

		b.SetBytes(int64(payloadLen))
		b.ReportAllocs()

		var (
			ops     atomic.Int64
			wg      sync.WaitGroup
			readyWG sync.WaitGroup
			errMu   sync.Mutex
			errs    []error
		)
		startCh := make(chan struct{})

		recordErr := func(err error) {
			if err == nil {
				return
			}
			errMu.Lock()
			if len(errs) < 8 {
				errs = append(errs, err)
			}
			errMu.Unlock()
		}

		readyWG.Add(connections)
		for workerID := 0; workerID < connections; workerID++ {
			workerID := workerID
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					recordErr(fmt.Errorf("websocket dial failed worker=%d err=%w", workerID, err))
					readyWG.Done()
					return
				}
				defer func() { _ = conn.Close() }()
				readyWG.Done()
				<-startCh

				seq := 0
				for {
					cur := int(ops.Add(1))
					if cur > b.N {
						return
					}
					payload := makeDatagramPayload(workerID, seq, payloadLen)
					seq++

					_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
					if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
						recordErr(fmt.Errorf("websocket write failed worker=%d err=%w", workerID, err))
						return
					}
					_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
					msgType, resp, readErr := conn.ReadMessage()
					if readErr != nil {
						recordErr(fmt.Errorf("websocket read failed worker=%d err=%w", workerID, readErr))
						return
					}
					if msgType != websocket.BinaryMessage || !bytes.Equal(resp, payload) {
						recordErr(fmt.Errorf("websocket echo mismatch worker=%d", workerID))
						return
					}
				}
			}()
		}

		readyWG.Wait()
		b.ResetTimer()
		close(startCh)
		wg.Wait()
		b.StopTimer()

		if len(errs) > 0 {
			for _, err := range errs {
				b.Error(err)
			}
		}
	})
}

func BenchmarkUDPServerThroughput(b *testing.B) {
	cases := []struct {
		name        string
		payloadLen  int
		connections int
		heavy       bool
	}{
		{name: "conn-100_payload-128B", payloadLen: 128, connections: 100},
		{name: "conn-100_payload-512B", payloadLen: 512, connections: 100},
		{name: "conn-500_payload-128B", payloadLen: 128, connections: 500, heavy: true},
		{name: "conn-500_payload-512B", payloadLen: 512, connections: 500, heavy: true},
	}

	for _, tc := range cases {
		tc := tc
		if tc.heavy && os.Getenv("NETWORK_BENCH_HEAVY") != "1" {
			continue
		}
		benchmarkDatagramServerThroughput(
			b,
			tc.name,
			func(tb testing.TB, handler IHandler, options ServerOptions) (string, func()) {
				udpSrv, addr := startUDPServer(tb, handler, options)
				return addr, func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					udpSrv.Shutdown(ctx)
				}
			},
			tc.payloadLen,
			tc.connections,
		)
	}
}

func BenchmarkWebSocketServerThroughput(b *testing.B) {
	cases := []struct {
		name        string
		payloadLen  int
		connections int
		heavy       bool
	}{
		{name: "conn-100_payload-128B", payloadLen: 128, connections: 100},
		{name: "conn-100_payload-512B", payloadLen: 512, connections: 100},
		{name: "conn-500_payload-128B", payloadLen: 128, connections: 500, heavy: true},
		{name: "conn-500_payload-512B", payloadLen: 512, connections: 500, heavy: true},
	}

	for _, tc := range cases {
		tc := tc
		if tc.heavy && os.Getenv("NETWORK_BENCH_HEAVY") != "1" {
			continue
		}
		benchmarkWebSocketServerThroughput(b, tc.name, tc.payloadLen, tc.connections)
	}
}
