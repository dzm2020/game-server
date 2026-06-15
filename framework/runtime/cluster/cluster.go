package cluster

import (
	"actor"
	consulregistry "consul_registry"
	"context"
	"errors"
	"fmt"
	"game-server/framework/runtime/protocol"
	queue "message_queue"
	"sync"
	"time"
)

var (
	ErrClusterNoActorSystem   = errors.New("cluster actor system is nil")
	ErrInvalidActorResponse   = errors.New("invalid actor response type")
	ErrInvalidClusterReply    = errors.New("invalid cluster reply message")
	ErrClusterRemoteReply     = errors.New("cluster remote reply error")
	ErrClusterAlreadyRegister = errors.New("cluster already registered")
	ErrClusterNotRegistered   = errors.New("cluster not registered")
)

// Cluster exposes cluster communication and service discovery features.
type Cluster struct {
	queue    queue.IMessageQue
	registry consulregistry.IRegistry
	system   actor.ISystem

	registered bool
	closeOnce  sync.Once
	nodeID     string
	sub        queue.ISubscription
}

func New(ctx context.Context, queue queue.IMessageQue, registry consulregistry.IRegistry, system actor.ISystem) (*Cluster, error) {
	_ = ctx
	if queue == nil {
		return nil, errors.New("cluster message queue is nil")
	}
	if registry == nil {
		return nil, errors.New("cluster registry is nil")
	}
	if system == nil {
		return nil, ErrClusterNoActorSystem
	}
	c := &Cluster{
		queue:    queue,
		registry: registry,
		system:   system,
	}
	return c, nil
}

// Start launches discovery sync and is idempotent.
func (c *Cluster) Start(ctx context.Context, opts consulregistry.WatchOptions) error {
	if err := c.registry.StartSync(ctx, opts); err != nil {
		return fmt.Errorf("start discover sync: %w", err)
	}
	return nil
}

func (c *Cluster) Register(reg consulregistry.ServiceRegistration) (err error) {
	if c.registered {
		return ErrClusterAlreadyRegister
	}

	nodeSubject, err := NodeSubject(reg.ID)
	if err != nil {
		return err
	}
	if err = c.registry.Register(reg); err != nil {
		return err
	}
	c.sub, err = c.queue.Subscribe(nodeSubject, &nodeMessageSubscriber{cluster: c})
	if err != nil {
		_ = c.registry.Deregister(reg.ID)
		return err
	}
	c.nodeID = reg.ID
	c.registered = true
	return nil
}

func (c *Cluster) Deregister(serviceID string) error {
	if !c.registered {
		return ErrClusterNotRegistered
	}
	if c.sub != nil {
		_ = c.sub.Unsubscribe()
		c.sub = nil
	}
	err := c.registry.Deregister(serviceID)
	if err == nil {
		c.registered = false
		c.nodeID = ""
	}
	return err
}

func (c *Cluster) Subscribe(subject string, subscriber queue.ISubscriber) (queue.ISubscription, error) {
	return c.queue.Subscribe(subject, subscriber)
}

func (c *Cluster) Discover(serviceName string) ([]consulregistry.ServiceInstance, error) {
	return c.registry.Discover(serviceName)
}

func (c *Cluster) DiscoverAll() (map[string][]consulregistry.ServiceInstance, error) {
	return c.registry.DiscoverAll()
}

func (c *Cluster) ListServices() ([]string, error) {
	return c.registry.ListServices()
}

func (c *Cluster) Watch(serviceName string, onChange consulregistry.ServiceChangeHandler) (string, error) {
	return c.registry.Watch(serviceName, onChange)
}

func (c *Cluster) Unwatch(serviceName, watchID string) {
	c.registry.Unwatch(serviceName, watchID)
}

func (c *Cluster) TTLPass(serviceID, note string) error {
	return c.registry.TTLPass(serviceID, note)
}

func (c *Cluster) TTLWarn(serviceID, note string) error {
	return c.registry.TTLWarn(serviceID, note)
}

func (c *Cluster) TTLFail(serviceID, note string) error {
	return c.registry.TTLFail(serviceID, note)
}

func (c *Cluster) PublishToPID(sourcePID actor.PID, targetPID actor.PID, msg *protocol.Message) error {
	return c.SendToPID(sourcePID, targetPID, msg)
}

func (c *Cluster) SendToPID(sourcePID actor.PID, targetPID actor.PID, msg *protocol.Message) error {
	clusterMsg, err := newClusterMessage(sourcePID, targetPID, msg)
	if err != nil {
		return err
	}
	subject, err := NodeSubject(targetPID.NodeID)
	if err != nil {
		return err
	}
	payload, err := clusterMsg.Marshal()
	if err != nil {
		return err
	}
	return c.queue.Publish(subject, payload)
}

func (c *Cluster) RequestToPID(sourcePID actor.PID, targetPID actor.PID, msg *protocol.Message, timeout time.Duration) ([]byte, error) {
	clusterMsg, err := newClusterMessage(sourcePID, targetPID, msg)
	if err != nil {
		return nil, err
	}

	subject, err := NodeSubject(targetPID.NodeID)
	if err != nil {
		return nil, err
	}
	payload, err := clusterMsg.Marshal()
	if err != nil {
		return nil, err
	}

	reply, err := c.queue.Request(subject, payload, timeout)
	if err != nil {
		return nil, err
	}

	replyMsg, err := UnmarshalClusterMessage(reply)
	if err != nil {
		return nil, err
	}

	if reply == nil || replyMsg.GetMessage() == nil {
		return nil, ErrInvalidClusterReply
	}
	if code := replyMsg.GetMessage().GetError(); code != 0 {
		return nil, fmt.Errorf("%w: code=%d msg=%s", ErrClusterRemoteReply, code, string(replyMsg.GetMessage().GetData()))
	}
	return replyMsg.GetMessage().GetData(), nil
}

func (c *Cluster) currentNodeID() string {
	nodeID := c.nodeID
	return nodeID
}

func (c *Cluster) Close() {
	c.closeOnce.Do(func() {
		_ = c.Deregister(c.currentNodeID())
	})
}
