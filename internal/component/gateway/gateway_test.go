package gateway

import (
	"actor"
	"context"
	"fmt"
	compcluster "game-server/internal/component/cluster"
	compsystem "game-server/internal/component/system"
	"game-server/internal/iface"
	"game-server/internal/profile"
	"game-server/internal/protocol"
	"game-server/pkg/component"
	"net"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type testNode struct {
	*iface.Member
	component.IManager
}

type fakeCluster struct {
	requestCalled bool
	sendCalled    bool
	reply         []byte
}

func (f *fakeCluster) SendToPID(_ actor.PID, _ actor.PID, _ *protocol.Message) error {
	f.sendCalled = true
	return nil
}

func (f *fakeCluster) RequestToPID(_ actor.PID, _ actor.PID, _ *protocol.Message, _ time.Duration) ([]byte, error) {
	f.requestCalled = true
	return f.reply, nil
}

func TestGatewayComponentLocalRequestRouting(t *testing.T) {
	gw, systemComp := setupGatewayForTest(t, false)
	defer teardownGatewayForTest(t, gw, systemComp)

	_, err := systemComp.Spawn(func(ctx actor.Context) {
		msg, ok := ctx.Message().(*protocol.Message)
		if !ok || msg == nil {
			_ = ctx.Respond([]byte("bad-request"))
			return
		}
		_ = ctx.Respond([]byte("local:" + string(msg.Data)))
	}, actor.WithName(gw.cfg.RouteActorName))
	if err != nil {
		t.Fatalf("spawn local route actor failed: %v", err)
	}

	conn := dialGateway(t, gw)
	defer conn.Close()

	req := protocol.NewMessage(9, 9, []byte("hello"))
	req.Index = 1
	sendProtoMessage(t, conn, req)

	reply := readProtoMessage(t, conn)
	if string(reply.Data) != "local:hello" {
		t.Fatalf("unexpected local route reply: %q", string(reply.Data))
	}
}

func TestGatewayComponentRemoteRequestRouting(t *testing.T) {
	gw, systemComp := setupGatewayForTest(t, true)
	defer teardownGatewayForTest(t, gw, systemComp)

	fc := &fakeCluster{reply: []byte("remote:ok")}
	gw.cluster = fc

	conn := dialGateway(t, gw)
	defer conn.Close()

	req := protocol.NewMessage(7, 7, []byte("ping"))
	req.Index = 1
	sendProtoMessage(t, conn, req)

	reply := readProtoMessage(t, conn)
	if string(reply.Data) != "remote:ok" {
		t.Fatalf("unexpected remote route reply: %q", string(reply.Data))
	}
	if !fc.requestCalled {
		t.Fatal("expected remote RequestToPID to be called")
	}
}

func setupGatewayForTest(t *testing.T, remoteRoute bool) (*Component, *compsystem.Component) {
	t.Helper()

	base := profile.GetBase()
	prev := *base
	t.Cleanup(func() {
		*base = prev
		iface.SetCurrentNode(nil)
	})

	base.Self = &iface.Member{ID: "gateway-test-node"}
	base.Gateway = profile.DefaultGatewayConfig()
	base.Gateway.Enable = true
	base.Gateway.Address = "127.0.0.1"
	base.Gateway.Port = testFreePort(t)
	base.Gateway.ActorName = "gateway-test-router"
	base.Gateway.RouteActorName = "gateway-test-target"
	base.Gateway.RequestTimeoutMs = 1000
	if remoteRoute {
		base.Gateway.RouteNodeID = "remote-node"
	} else {
		base.Gateway.RouteNodeID = base.Self.GetID()
	}

	mgr := component.NewComponentsMgr()
	node := &testNode{
		Member:   base.Self,
		IManager: mgr,
	}
	iface.SetCurrentNode(node)

	systemComp := compsystem.New()
	if err := systemComp.Init(); err != nil {
		t.Fatalf("init system component failed: %v", err)
	}
	if err := mgr.AddComponent(systemComp); err != nil {
		t.Fatalf("register system component failed: %v", err)
	}
	if err := mgr.AddComponent(&compcluster.Component{}); err != nil {
		t.Fatalf("register cluster component failed: %v", err)
	}

	gw := New()
	if err := gw.Init(); err != nil {
		t.Fatalf("init gateway component failed: %v", err)
	}
	if err := gw.Start(context.Background()); err != nil {
		t.Fatalf("start gateway component failed: %v", err)
	}
	return gw, systemComp
}

func teardownGatewayForTest(t *testing.T, gw *Component, systemComp *compsystem.Component) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if gw != nil {
		if err := gw.Stop(ctx); err != nil {
			t.Fatalf("stop gateway failed: %v", err)
		}
	}
	if systemComp != nil {
		_ = systemComp.Stop(ctx)
	}
}

func dialGateway(t *testing.T, gw *Component) *websocket.Conn {
	t.Helper()
	url := fmt.Sprintf("ws://%s/", gw.server.Addr)
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err == nil {
			return conn
		}
		lastErr = err
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("dial gateway failed: %v", lastErr)
	return nil
}

func sendProtoMessage(t *testing.T, conn *websocket.Conn, msg *protocol.Message) {
	t.Helper()
	data := protocol.NewCodec().Encode(msg)
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write ws message failed: %v", err)
	}
}

func readProtoMessage(t *testing.T, conn *websocket.Conn) *protocol.Message {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read ws message failed: %v", err)
	}
	msgList, err := decodeProtocolMessages(data)
	if err != nil {
		t.Fatalf("decode protocol message failed: %v", err)
	}
	if len(msgList) == 0 {
		t.Fatal("decoded empty protocol message list")
	}
	return msgList[0]
}

func testFreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("alloc free port failed: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
