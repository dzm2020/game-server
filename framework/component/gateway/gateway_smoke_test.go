package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"game-server/framework/actor"
	"game-server/framework/gen"
)

type loopbackAgent struct {
	Agent
}

func TestGatewayRoundTripSmoke(t *testing.T) {
	system := actor.NewSystemWithNodeID("gateway-smoke")
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = system.Stop(stopCtx)
	})

	listenAddr := pickFreeTCPAddr(t)
	opts := normalization(Options{
		ProtoAddr: "tcp://" + listenAddr,
		AgentFactory: func() (IAgent, gen.SpawnOptions) {
			ag := &loopbackAgent{}
			route := actor.NewRoute()
			route.Register(1, 1, func(ctx gen.IContext, request interface{}) error {
				msg, ok := request.(*gen.Message)
				if !ok {
					return ErrInvalidMessageType
				}
				return ag.Push(gen.NewMessage(2, 1, msg.Data))
			}, nil)
			return ag, gen.SpawnOptions{
				Route: route,
			}
		},
	})
	if err := validate(opts); err != nil {
		t.Fatalf("invalid gateway options: %v", err)
	}

	gw := newGatWay(opts, system)
	if err := gw.Init(); err != nil {
		t.Fatalf("gateway init failed: %v", err)
	}
	if err := gw.Start(context.Background()); err != nil {
		t.Fatalf("gateway start failed: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gw.Stop(stopCtx)
	})

	conn, err := net.DialTimeout("tcp", listenAddr, time.Second)
	if err != nil {
		t.Fatalf("dial gateway failed: %v", err)
	}
	defer func() { _ = conn.Close() }()

	outbound := gen.NewMessage(1, 1, []byte("gateway-smoke"))
	raw, err := gen.Encode(outbound)
	if err != nil {
		t.Fatalf("encode outbound message failed: %v", err)
	}
	n, err := conn.Write(raw)
	if err != nil {
		t.Fatalf("write outbound message failed: %v", err)
	}
	if n != len(raw) {
		t.Fatalf("partial write, wrote=%d total=%d", n, len(raw))
	}

	inbound, err := readOneMessage(conn, 2*time.Second)
	if err != nil {
		t.Fatalf("read inbound message failed: %v", err)
	}
	if inbound.Cmd != 2 || inbound.Act != 1 {
		t.Fatalf("unexpected inbound route, got cmd=%d act=%d", inbound.Cmd, inbound.Act)
	}
	if !bytes.Equal(inbound.Data, outbound.Data) {
		t.Fatalf("inbound payload mismatch, got=%q want=%q", inbound.Data, outbound.Data)
	}
}

func readOneMessage(conn net.Conn, timeout time.Duration) (*gen.Message, error) {
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	defer func() { _ = conn.SetReadDeadline(time.Time{}) }()

	header := make([]byte, gen.HeadLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	bodyLen := binary.BigEndian.Uint32(header[:4])
	packet := make([]byte, gen.HeadLen+int(bodyLen))
	copy(packet, header)
	if bodyLen > 0 {
		if _, err := io.ReadFull(conn, packet[gen.HeadLen:]); err != nil {
			return nil, err
		}
	}

	msg, _, err := gen.Decode(packet)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, io.ErrUnexpectedEOF
	}
	return msg, nil
}

func pickFreeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate tcp address failed: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
