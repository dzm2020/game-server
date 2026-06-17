package cluster

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"game-server/framework/cluster/define"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var _ IServerInstance = (*mockServiceInstance)(nil)

type mockServiceInstance struct {
	ID      string
	Name    string
	Address string
	Port    int
}

func (m mockServiceInstance) GetID() string {
	return m.ID
}

func (m mockServiceInstance) GetName() string {
	return m.Name
}

func (m mockServiceInstance) GetAddress() string {
	return m.Address
}

func (m mockServiceInstance) GetPort() int {
	return m.Port
}

type mockDispatcher struct {
}

func (m *mockDispatcher) Handler(message *define.ClusterMessage) error {
	return nil
}

type mockServerList struct {
	discoverAll []IServerInstance
}

func (m *mockServerList) Get() []IServerInstance {
	return m.discoverAll
}

type testNodeService struct {
	UnimplementedNodeServiceServer
}

func (s *testNodeService) Stream(stream NodeService_StreamServer) error {
	<-stream.Context().Done()
	return nil
}

func newReadyClientConn(t *testing.T) (*grpc.ClientConn, func()) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	RegisterNodeServiceServer(srv, &testNodeService{})
	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	if err != nil {
		srv.Stop()
		_ = lis.Close()
		t.Fatalf("create client conn failed: %v", err)
	}

	conn.Connect()
	waitCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			_ = conn.Close()
			srv.Stop()
			_ = lis.Close()
			t.Fatalf("wait ready timeout, state=%s", state.String())
		}
	}

	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	}
	return conn, cleanup
}

func TestTryConnectPeersFiltersAndSkipsSelf(t *testing.T) {
	serverList := &mockServerList{
		discoverAll: []IServerInstance{
			mockServiceInstance{ID: "self", Name: "game", Address: "127.0.0.1", Port: 9001},
			mockServiceInstance{ID: "node-a", Name: "game", Address: "127.0.0.1", Port: 9002},
			mockServiceInstance{ID: "node-x", Name: "other", Address: "127.0.0.1", Port: 9003},
		},
	}
	c := NewCluster(&Config{
		NodeID:           "self",
		ListenAddr:       "127.0.0.1:0",
		PeerNames:        []string{"game"},
		PeerSendChanSize: 8,
	}, serverList, &mockDispatcher{})
	defer c.Close()

	c.tryConnectPeers()

	if _, ok := c.peers.Get("self"); ok {
		t.Fatal("self node should not be added as peer")
	}
	if _, ok := c.peers.Get("node-x"); ok {
		t.Fatal("service outside PeerNames should not be added")
	}
	peer, ok := c.peers.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to be added")
	}
	if peer.address != "127.0.0.1:9002" {
		t.Fatalf("unexpected node-a address: %s", peer.address)
	}
}

func TestSendToNodeSuccess(t *testing.T) {
	conn, cleanup := newReadyClientConn(t)
	defer cleanup()

	c := NewCluster(&Config{
		NodeID:           "self",
		ListenAddr:       "127.0.0.1:0",
		PeerSendChanSize: 1,
	}, nil, &mockDispatcher{})
	defer c.Close()

	peer := &PeerConn{
		nodeID: "node-a",
		conn:   conn,
		sendCh: make(chan *define.ClusterMessage, 1),
	}
	c.peers.Set("node-a", peer)

	if err := c.SendToNode("node-a", []byte("hello")); err != nil {
		t.Fatalf("send should succeed, got err: %v", err)
	}
	if len(peer.sendCh) != 1 {
		t.Fatalf("expected queued message len=1, got %d", len(peer.sendCh))
	}
}

func TestSendToNodeChannelFull(t *testing.T) {
	conn, cleanup := newReadyClientConn(t)
	defer cleanup()

	c := NewCluster(&Config{
		NodeID:           "self",
		ListenAddr:       "127.0.0.1:0",
		PeerSendChanSize: 1,
	}, nil, &mockDispatcher{})
	defer c.Close()

	peer := &PeerConn{
		nodeID: "node-a",
		conn:   conn,
		sendCh: make(chan *define.ClusterMessage, 1),
	}
	peer.sendCh <- define.NewClusterMessage("self", "node-a", []byte("filled"))
	c.peers.Set("node-a", peer)

	err := c.SendToNode("node-a", []byte("next"))
	if err == nil {
		t.Fatal("expected send_channel_full error")
	}
	if !strings.Contains(err.Error(), "send_channel_full") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestOnPeerClosedRequiresMatchingInstance(t *testing.T) {
	c := NewCluster(&Config{
		NodeID:           "self",
		ListenAddr:       "127.0.0.1:0",
		PeerSendChanSize: 8,
	}, nil, &mockDispatcher{})
	defer c.Close()

	current := &PeerConn{nodeID: "node-a", address: "127.0.0.1:10001"}
	c.peers.Set("node-a", current)

	other := &PeerConn{nodeID: "node-a", address: "127.0.0.1:10002"}
	c.onPeerClosed("node-a", other)
	if _, ok := c.peers.Get("node-a"); !ok {
		t.Fatal("peer should remain when closed instance mismatches current")
	}

	c.onPeerClosed("node-a", current)
	if _, ok := c.peers.Get("node-a"); ok {
		t.Fatal("peer should be removed when closed instance matches current")
	}
}
