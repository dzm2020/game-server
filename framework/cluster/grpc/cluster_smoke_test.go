package grpc_cluster

import (
	"bytes"
	"context"
	"net"
	"sort"
	"sync"
	"testing"
	"time"

	"game-server/framework/gen"
)

type localInvokerFunc func(from *gen.PID, target *gen.PID, msg *gen.Message) error

func (f localInvokerFunc) Handler(from *gen.PID, target *gen.PID, msg *gen.Message) error {
	return f(from, target, msg)
}

type mutableDiscovery struct {
	mu     sync.RWMutex
	byID   map[string]gen.ServiceInstance
	byName map[string][]gen.ServiceInstance
}

func newMutableDiscovery() *mutableDiscovery {
	return &mutableDiscovery{
		byID:   make(map[string]gen.ServiceInstance),
		byName: make(map[string][]gen.ServiceInstance),
	}
}

func (d *mutableDiscovery) Set(instances ...gen.ServiceInstance) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.byID = make(map[string]gen.ServiceInstance, len(instances))
	d.byName = make(map[string][]gen.ServiceInstance)
	for _, ins := range instances {
		d.byID[ins.ID] = ins
		d.byName[ins.Name] = append(d.byName[ins.Name], ins)
	}
}

func (d *mutableDiscovery) GetInstance(serverID string) (gen.ServiceInstance, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	ins, ok := d.byID[serverID]
	return ins, ok
}

func (d *mutableDiscovery) Discover(serviceName string) []gen.ServiceInstance {
	d.mu.RLock()
	defer d.mu.RUnlock()
	items := d.byName[serviceName]
	out := make([]gen.ServiceInstance, len(items))
	copy(out, items)
	return out
}

func (d *mutableDiscovery) DiscoverAll() map[string][]gen.ServiceInstance {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string][]gen.ServiceInstance, len(d.byName))
	for name, instances := range d.byName {
		cp := make([]gen.ServiceInstance, len(instances))
		copy(cp, instances)
		out[name] = cp
	}
	return out
}

func (d *mutableDiscovery) ListServices() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	services := make([]string, 0, len(d.byName))
	for name := range d.byName {
		services = append(services, name)
	}
	sort.Strings(services)
	return services
}

func (d *mutableDiscovery) Watch(serviceName string, onChange gen.ServiceChangeHandler) (string, error) {
	return "smoke", nil
}

func (d *mutableDiscovery) Unwatch(serviceName, watchID string) {}

type delivery struct {
	from *gen.PID
	to   *gen.PID
	msg  *gen.Message
}

func TestClusterNodeInterconnectSmoke(t *testing.T) {
	addrA := pickFreeTCPAddr(t)
	addrB := pickFreeTCPAddr(t)
	discovery := newMutableDiscovery()
	received := make(chan delivery, 1)

	clusterA := New(&Options{
		NodeID:           "node-a",
		ListenAddr:       addrA,
		PeerSendChanSize: 16,
		PeerNames:        []string{"service-b"},
	})
	clusterB := New(&Options{
		NodeID:           "node-b",
		ListenAddr:       addrB,
		PeerSendChanSize: 16,
		PeerNames:        []string{"service-a"},
	})

	clusterA.SetDiscovery(discovery)
	clusterB.SetDiscovery(discovery)

	clusterA.SetLocalInvoker(localInvokerFunc(func(from *gen.PID, target *gen.PID, msg *gen.Message) error {
		return nil
	}))
	clusterB.SetLocalInvoker(localInvokerFunc(func(from *gen.PID, target *gen.PID, msg *gen.Message) error {
		select {
		case received <- delivery{
			from: clonePID(from),
			to:   clonePID(target),
			msg:  cloneMessage(msg),
		}:
		default:
		}
		return nil
	}))

	if err := clusterA.Start(context.Background()); err != nil {
		t.Fatalf("start cluster A failed: %v", err)
	}
	if err := clusterB.Start(context.Background()); err != nil {
		t.Fatalf("start cluster B failed: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		done := make(chan struct{}, 2)
		go func() {
			_ = clusterA.Stop(stopCtx)
			done <- struct{}{}
		}()
		go func() {
			_ = clusterB.Stop(stopCtx)
			done <- struct{}{}
		}()
		<-done
		<-done
	})

	discovery.Set(
		gen.ServiceInstance{
			ID:         "node-a",
			Name:       "service-a",
			RpcAddress: addrA,
			ExtAddress: addrA,
		},
		gen.ServiceInstance{
			ID:         "node-b",
			Name:       "service-b",
			RpcAddress: addrB,
			ExtAddress: addrB,
		},
	)

	clusterA.tryConnectPeers()
	clusterB.tryConnectPeers()

	from := gen.NewPID(101, "sender", "node-a")
	to := gen.NewPID(202, "receiver", "node-b")
	outbound := gen.NewMessage(20, 1, []byte("cluster-smoke"))

	deadline := time.Now().Add(3 * time.Second)
	var sendErr error
	for {
		sendErr = clusterA.SendToNode(from, to, outbound)
		if sendErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("send to remote node failed before deadline: %v", sendErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	select {
	case got := <-received:
		if got.from == nil || got.from.NodeID != "node-a" {
			t.Fatalf("unexpected source pid: %+v", got.from)
		}
		if got.to == nil || got.to.NodeID != "node-b" {
			t.Fatalf("unexpected target pid: %+v", got.to)
		}
		if got.msg == nil {
			t.Fatal("received nil message")
		}
		if got.msg.Cmd != outbound.Cmd || got.msg.Act != outbound.Act {
			t.Fatalf("message route mismatch, got cmd=%d act=%d", got.msg.Cmd, got.msg.Act)
		}
		if !bytes.Equal(got.msg.Data, outbound.Data) {
			t.Fatalf("message data mismatch, got=%q want=%q", got.msg.Data, outbound.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cluster delivery timeout")
	}
}

func clonePID(p *gen.PID) *gen.PID {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}

func cloneMessage(msg *gen.Message) *gen.Message {
	if msg == nil {
		return nil
	}
	head := &gen.Head{
		Len:   msg.Len,
		Cmd:   msg.Cmd,
		Act:   msg.Act,
		Error: msg.Error,
		Index: msg.Index,
	}
	return &gen.Message{
		Head: head,
		Data: append([]byte(nil), msg.Data...),
	}
}

func pickFreeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate tcp address failed: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}
