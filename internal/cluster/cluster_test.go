package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"actor"
	consulregistry "consul_registry"
	"game-server/internal/protocol"
	queue "message_queue"
)

func TestClusterAssembleWithPkgImplementations(t *testing.T) {
	ctx := context.Background()
	nodeID := fmt.Sprintf("cluster-test-node-%d", time.Now().UnixNano())

	mq, registry, sys, cleanup := setupPkgImplementations(t, nodeID)
	defer cleanup()

	c, err := New(ctx, mq, registry, sys)
	if err != nil {
		t.Fatalf("assemble cluster failed: %v", err)
	}
	c.Close()
}

func TestClusterRequestReplyWithPIDInterfaces(t *testing.T) {
	ctx := context.Background()
	nodeID := fmt.Sprintf("cluster-test-node-%d", time.Now().UnixNano())

	mq, registry, sys, cleanup := setupPkgImplementations(t, nodeID)
	defer cleanup()

	c, err := New(ctx, mq, registry, sys)
	if err != nil {
		t.Fatalf("assemble cluster failed: %v", err)
	}
	defer c.Close()

	if _, err = sys.Spawn(func(ctx actor.Context) {
		msg, ok := ctx.Message().(*protocol.Message)
		if !ok || msg == nil {
			_ = ctx.Respond([]byte("bad-request"))
			return
		}
		_ = ctx.Respond([]byte("pong"))
	}, actor.WithName("echo")); err != nil {
		t.Fatalf("spawn actor failed: %v", err)
	}

	reg := consulregistry.ServiceRegistration{
		ID:      nodeID,
		Name:    "cluster-test-service",
		Address: "127.0.0.1",
		Port:    19000,
	}
	if err = c.Register(reg); err != nil {
		t.Skipf("register requires running consul/nats, skip: %v", err)
	}
	defer func() { _ = c.Deregister(nodeID) }()

	sourcePID := actor.NewPID(0, "caller", "caller-node")
	targetPID := actor.NewPID(0, "echo", nodeID)
	replyData, err := c.RequestToPID(sourcePID, targetPID, protocol.NewMessage(1, 2, []byte("ping")), 3*time.Second)
	if err != nil {
		t.Fatalf("cluster request failed: %v", err)
	}
	if got := string(replyData); got != "pong" {
		t.Fatalf("reply payload mismatch, got=%q want=%q", got, "pong")
	}
}

func TestRequestToPIDReturnsRemoteError(t *testing.T) {
	ctx := context.Background()
	nodeID := fmt.Sprintf("cluster-test-node-%d", time.Now().UnixNano())

	mq, registry, sys, cleanup := setupPkgImplementations(t, nodeID)
	defer cleanup()

	c, err := New(ctx, mq, registry, sys)
	if err != nil {
		t.Fatalf("assemble cluster failed: %v", err)
	}
	defer c.Close()

	reg := consulregistry.ServiceRegistration{
		ID:      nodeID,
		Name:    "cluster-test-service",
		Address: "127.0.0.1",
		Port:    19001,
	}
	if err = c.Register(reg); err != nil {
		t.Skipf("register requires running consul/nats, skip: %v", err)
	}
	defer func() { _ = c.Deregister(nodeID) }()

	sourcePID := actor.NewPID(0, "caller", "caller-node")
	targetPID := actor.NewPID(0, "not-exist-actor", nodeID)
	_, err = c.RequestToPID(sourcePID, targetPID, protocol.NewMessage(1, 2, []byte("ping")), 3*time.Second)
	if err == nil {
		t.Fatal("expected remote error, got nil")
	}
	if !errors.Is(err, ErrClusterRemoteReply) {
		t.Fatalf("expected ErrClusterRemoteReply, got %v", err)
	}
	if !strings.Contains(err.Error(), "code=1") {
		t.Fatalf("expected code in error, got %v", err)
	}
}

func setupPkgImplementations(t *testing.T, nodeID string) (queue.IMessageQue, consulregistry.IRegistry, actor.ISystem, func()) {
	t.Helper()

	mq, err := queue.NewNATSMessageQueue("nats://127.0.0.1:4222")
	if err != nil {
		t.Skipf("nats not available, skip integration test: %v", err)
	}

	registry, err := consulregistry.New(consulregistry.Config{
		Address: "127.0.0.1:8500",
		Scheme:  "http",
	})
	if err != nil {
		mq.Close()
		t.Skipf("consul not available, skip integration test: %v", err)
	}

	sys := actor.NewSystemWithNodeID(nodeID)
	cleanup := func() {
		sys.Shutdown()
		mq.Close()
	}
	return mq, registry, sys, cleanup
}
