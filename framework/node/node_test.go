package node

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"testing"
	"time"
)

type stubComponent struct {
	component.BaseComponent
	name string
}

func (c *stubComponent) Init(context.Context) error  { return nil }
func (c *stubComponent) Start(context.Context) error { return nil }
func (c *stubComponent) Stop(context.Context) error  { return nil }

type stubRegistry struct {
	stubComponent
	registerErr error
}

func (r *stubRegistry) Register(gen.ServiceInstance) error { return r.registerErr }
func (r *stubRegistry) Deregister() error                  { return nil }
func (r *stubRegistry) SetHealthState(string) error        { return nil }
func (r *stubRegistry) Discover(string) map[string]gen.ServiceInstance {
	return nil
}
func (r *stubRegistry) DiscoverByID(string) *gen.ServiceInstance { return nil }

type stubSystem struct {
	stubComponent
}

func (s *stubSystem) SetNodeID(string)                        {}
func (s *stubSystem) SetRemoteInvoker(gen.IRemoteInvoker)     {}
func (s *stubSystem) Tell(*gen.PID, any, *gen.Message) error  { return nil }
func (s *stubSystem) Spawn(gen.ActorHandler, gen.SpawnOptions) (*gen.PID, error) {
	return gen.NoSender, nil
}
func (s *stubSystem) SpawnActor(gen.IActor, gen.SpawnOptions) (*gen.PID, error) {
	return gen.NoSender, nil
}
func (s *stubSystem) Ask(*gen.PID, any, *gen.Message, time.Duration) ([]byte, error) {
	return nil, nil
}
func (s *stubSystem) DoTask(*gen.PID, any, gen.ActorTask) error { return nil }
func (s *stubSystem) SendEnvelope(any, gen.ActorEnvelope) error  { return nil }
func (s *stubSystem) StopProcess(any)                              {}
func (s *stubSystem) Stop(context.Context) error                 { return nil }

type stubCluster struct {
	stubComponent
}

func (c *stubCluster) ApplyNode(string, string)              {}
func (c *stubCluster) SetLocalInvoker(gen.ILocalInvoker)     {}
func (c *stubCluster) SetDiscovery(gen.IDiscovery)           {}
func (c *stubCluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	return nil
}
func (c *stubCluster) Broadcast(*gen.PID, *gen.Message) error { return nil }

type prepareBehavior struct {
	gen.BaseNodeBehavior
	onBeforeInit func(gen.INode)
}

func (b prepareBehavior) OnBeforeInit(node gen.INode) {
	if b.onBeforeInit != nil {
		b.onBeforeInit(node)
	}
}

func TestNode_StartRegistersComponentsAddedInOnBeforeInit(t *testing.T) {
	n := New(gen.NodeOptions{
		ID:   "node-test",
		Name: "test",
		Behavior: prepareBehavior{
			onBeforeInit: func(node gen.INode) {
				node.AddComponents(&stubComponent{name: "custom"})
			},
		},
		Factories: gen.NodeFactories{
			Registry: func() gen.IRegistry { return &stubRegistry{} },
			System:   func() gen.ISystem { return &stubSystem{} },
			Cluster:  func() gen.ICluster { return &stubCluster{} },
		},
	})

	if err := n.start(context.Background()); err != nil {
		t.Fatalf("start node failed: %v", err)
	}
	if n.ComponentCount() != 4 {
		t.Fatalf("component count mismatch: got=%d want=4", n.ComponentCount())
	}
	if err := n.stop(context.Background()); err != nil {
		t.Fatalf("stop node failed: %v", err)
	}
}

func TestNode_StartRollbackWhenRegisterFails(t *testing.T) {
	n := New(gen.NodeOptions{
		ID:   "node-test",
		Name: "test",
		Factories: gen.NodeFactories{
			Registry: func() gen.IRegistry {
				return &stubRegistry{registerErr: gen.ErrServiceNotRegister}
			},
			System:  func() gen.ISystem { return &stubSystem{} },
			Cluster: func() gen.ICluster { return &stubCluster{} },
		},
	})

	err := n.start(context.Background())
	if err == nil {
		t.Fatal("expected register failure")
	}
	if n.phase == phaseRunning {
		t.Fatal("node should not remain running after failed start")
	}
}
