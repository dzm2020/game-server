package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type frameEchoHandler struct {
	received atomic.Int64
}

func (h *frameEchoHandler) OnConnect(conn IConnection) error {
	return nil
}

func (h *frameEchoHandler) OnMessage(conn IConnection, data []byte) (int, error) {
	consumed := 0
	for {
		if len(data[consumed:]) < 4 {
			break
		}
		bodyLen := int(binary.BigEndian.Uint32(data[consumed : consumed+4]))
		if bodyLen < 0 {
			return consumed, fmt.Errorf("invalid body length: %d", bodyLen)
		}
		frameLen := 4 + bodyLen
		if len(data[consumed:]) < frameLen {
			break
		}
		payload := append([]byte(nil), data[consumed+4:consumed+frameLen]...)
		if err := conn.Send(packFrame(payload)); err != nil {
			return consumed, err
		}
		h.received.Add(1)
		consumed += frameLen
	}
	return consumed, nil
}

func (h *frameEchoHandler) OnClose(conn IConnection, err error) {}

func packFrame(payload []byte) []byte {
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return frame
}

func writeFrame(conn net.Conn, payload []byte) error {
	frame := packFrame(payload)
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}

func readFrame(conn net.Conn, timeout time.Duration) ([]byte, error) {
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, err
		}
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	bodyLen := int(binary.BigEndian.Uint32(header))
	if bodyLen < 0 {
		return nil, fmt.Errorf("invalid body length: %d", bodyLen)
	}
	payload := make([]byte, bodyLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func makePayload(clientID, seq, size int) []byte {
	if size < 8 {
		size = 8
	}
	payload := make([]byte, size)
	binary.BigEndian.PutUint32(payload[:4], uint32(clientID))
	binary.BigEndian.PutUint32(payload[4:8], uint32(seq))
	fill := byte((clientID + seq) % 251)
	for i := 8; i < len(payload); i++ {
		payload[i] = fill
	}
	return payload
}

func startTCPServer(tb testing.TB, handler IHandler, options ServerOptions) (*TCPServer, string) {
	tb.Helper()
	srv, err := NewServer(handler, "tcp://127.0.0.1:0", normalization(options))
	if err != nil {
		tb.Fatalf("create server failed: %v", err)
	}
	tcpSrv, ok := srv.(*TCPServer)
	if !ok {
		tb.Fatalf("server type mismatch: %T", srv)
	}
	if err := tcpSrv.Start(); err != nil {
		tb.Fatalf("start server failed: %v", err)
	}
	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tcpSrv.Shutdown(ctx)
	})
	return tcpSrv, tcpSrv.listener.Addr().String()
}

func TestTCP_NoPacketLoss_MultiConnections(t *testing.T) {
	if os.Getenv("NETWORK_STRESS") != "1" {
		t.Skip("set NETWORK_STRESS=1 to run multi-connection packet-loss stress test")
	}

	handler := &frameEchoHandler{}
	opts := ServerOptions{
		SendChanSize:        8192,
		HeartIntervalSecond: 30,
		TcpOptions: TcpServerOptions{
			ReadBufferSize:  64 * 1024,
			WriteBufferSize: 64 * 1024,
			WriteTimeout:    10 * time.Second,
		},
	}
	_, addr := startTCPServer(t, handler, opts)

	const (
		clientCount       = 500
		messagePerClient  = 300
		messagePayloadLen = 128
		readTimeout       = 15 * time.Second
	)

	var wg sync.WaitGroup
	errCh := make(chan error, clientCount)

	for i := 0; i < clientCount; i++ {
		clientID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("dial failed client=%d err=%w", clientID, err)
				return
			}
			defer func() { _ = conn.Close() }()

			for seq := 0; seq < messagePerClient; seq++ {
				payload := makePayload(clientID, seq, messagePayloadLen)
				if err := writeFrame(conn, payload); err != nil {
					errCh <- fmt.Errorf("write failed client=%d seq=%d err=%w", clientID, seq, err)
					return
				}
			}

			for seq := 0; seq < messagePerClient; seq++ {
				resp, err := readFrame(conn, readTimeout)
				if err != nil {
					errCh <- fmt.Errorf("read failed client=%d seq=%d err=%w", clientID, seq, err)
					return
				}
				expect := makePayload(clientID, seq, messagePayloadLen)
				if !bytes.Equal(resp, expect) {
					errCh <- fmt.Errorf("echo mismatch client=%d seq=%d", clientID, seq)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("multi-connection packet loss check failed: %v", err)
		}
	}

	expected := int64(clientCount * messagePerClient)
	if got := handler.received.Load(); got != expected {
		t.Fatalf("server receive count mismatch: got=%d want=%d", got, expected)
	}
}

func BenchmarkTCPServerThroughput(b *testing.B) {
	cases := []struct {
		name          string
		payloadLen    int
		connections   int
		heavy         bool
		batchSize     int
		batchInterval time.Duration
		startupWarmup time.Duration
	}{
		{name: "conn-100_payload-128B", payloadLen: 128, connections: 100, batchSize: 100, batchInterval: 20 * time.Millisecond},
		{name: "conn-100_payload-512B", payloadLen: 512, connections: 100, batchSize: 100, batchInterval: 20 * time.Millisecond},
		{name: "conn-500_payload-128B", payloadLen: 128, connections: 500, heavy: true, batchSize: 100, batchInterval: 20 * time.Millisecond},
		{name: "conn-500_payload-512B", payloadLen: 512, connections: 500, heavy: true, batchSize: 100, batchInterval: 20 * time.Millisecond},
		{name: "conn-1000_payload-128B", payloadLen: 128, connections: 1000, heavy: true, batchSize: 100, batchInterval: 20 * time.Millisecond},
		{name: "conn-1000_payload-512B", payloadLen: 512, connections: 1000, heavy: true, batchSize: 100, batchInterval: 20 * time.Millisecond},
		//{name: "conn-3000_payload-128B", payloadLen: 128, connections: 3000, heavy: true, batchSize: 30, batchInterval: 80 * time.Millisecond, startupWarmup: 1 * time.Second},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			if tc.heavy && os.Getenv("NETWORK_BENCH_HEAVY") != "1" {
				b.Skip("set NETWORK_BENCH_HEAVY=1 to run heavy benchmark tiers")
			}

			handler := &frameEchoHandler{}
			opts := ServerOptions{
				SendChanSize:        16384,
				HeartIntervalSecond: 30,
				TcpOptions: TcpServerOptions{
					ReadBufferSize:  128 * 1024,
					WriteBufferSize: 128 * 1024,
					WriteTimeout:    10 * time.Second,
				},
			}
			_, addr := startTCPServer(b, handler, opts)
			const readTimeout = 10 * time.Second
			if tc.startupWarmup > 0 {
				time.Sleep(tc.startupWarmup)
			}

			b.SetBytes(int64(tc.payloadLen))
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

			readyWG.Add(tc.connections)
			connectBatchSize := tc.batchSize
			if connectBatchSize <= 0 {
				connectBatchSize = 100
			}
			connectBatchInterval := tc.batchInterval
			if connectBatchInterval <= 0 {
				connectBatchInterval = 20 * time.Millisecond
			}

			for workerID := 0; workerID < tc.connections; workerID++ {
				workerID := workerID
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
					if err != nil {
						recordErr(fmt.Errorf("dial failed worker=%d err=%w", workerID, err))
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

						payload := makePayload(workerID, seq, tc.payloadLen)
						seq++
						if err := writeFrame(conn, payload); err != nil {
							recordErr(fmt.Errorf("write failed worker=%d err=%w", workerID, err))
							return
						}

						resp, err := readFrame(conn, readTimeout)
						if err != nil {
							if errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) {
								recordErr(fmt.Errorf("connection closed worker=%d err=%w", workerID, err))
							} else {
								recordErr(fmt.Errorf("read failed worker=%d err=%w", workerID, err))
							}
							return
						}
						if !bytes.Equal(resp, payload) {
							recordErr(fmt.Errorf("echo mismatch worker=%d seq=%d", workerID, seq-1))
							return
						}
					}
				}()

				if (workerID+1)%connectBatchSize == 0 && workerID+1 < tc.connections {
					time.Sleep(connectBatchInterval)
				}
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
}
