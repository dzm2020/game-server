package actor

import (
	"context"
	"game-server/framework/gen"
	"testing"
	"time"
)

func newStartedSystem(t *testing.T) *System {
	t.Helper()
	return newStartedSystemWithRemote(t, "node-test", nil)
}

func newStartedSystemWithRemote(t *testing.T, nodeID string, invoker gen.IRemoteInvoker) *System {
	t.Helper()

	s := NewSystem()
	s.SetNodeID(nodeID)
	if invoker != nil {
		s.SetRemoteInvoker(invoker)
	}
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("init system failed: %v", err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start system failed: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Stop(context.Background())
	})
	return s
}

type captureCluster struct {
	from *gen.PID
	to   *gen.PID
	msg  *gen.Message
	err  error
}

func (c *captureCluster) Init(context.Context) error  { return nil }
func (c *captureCluster) Start(context.Context) error { return nil }
func (c *captureCluster) Stop(context.Context) error  { return nil }

func (c *captureCluster) SetLocalInvoker(gen.ILocalInvoker) {}
func (c *captureCluster) SetDiscovery(gen.IDiscovery)       {}

func (c *captureCluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	c.from = from
	c.to = to
	c.msg = msg
	return c.err
}

func (c *captureCluster) Broadcast(*gen.PID, *gen.Message) error { return nil }

type blockingActor struct {
	enterCh chan struct{}
	blockCh chan struct{}
}

func (a *blockingActor) OnInit(gen.IContext) error { return nil }
func (a *blockingActor) OnDestroy(gen.IContext) error {
	return nil
}
func (a *blockingActor) OnError(gen.IContext, any) {}

func (a *blockingActor) OnMessage(gen.IContext) error {
	select {
	case a.enterCh <- struct{}{}:
	default:
	}
	<-a.blockCh
	return nil
}

func TestSystem_TypedNilPIDReturnsPidNil(t *testing.T) {
	s := newStartedSystem(t)
	msg := &gen.Message{}
	var pid *gen.PID

	if err := s.Tell(gen.NoSender, pid, msg); err != gen.ErrActorPidNil {
		t.Fatalf("Tell typed nil pid error mismatch: got=%v want=%v", err, gen.ErrActorPidNil)
	}

	if _, err := s.Ask(gen.NoSender, pid, msg, time.Second); err != gen.ErrActorPidNil {
		t.Fatalf("Ask typed nil pid error mismatch: got=%v want=%v", err, gen.ErrActorPidNil)
	}

	if err := s.SendEnvelope(pid, gen.ActorEnvelope{Payload: msg, Sender: gen.NoSender}); err != gen.ErrActorPidNil {
		t.Fatalf("SendEnvelope typed nil pid error mismatch: got=%v want=%v", err, gen.ErrActorPidNil)
	}
}

func TestSystem_NilTargetReturnsPidNil(t *testing.T) {
	s := newStartedSystem(t)
	msg := &gen.Message{}

	if err := s.Tell(gen.NoSender, nil, msg); err != gen.ErrActorPidNil {
		t.Fatalf("Tell nil target error mismatch: got=%v want=%v", err, gen.ErrActorPidNil)
	}
	if _, err := s.Ask(gen.NoSender, nil, msg, time.Second); err != gen.ErrActorPidNil {
		t.Fatalf("Ask nil target error mismatch: got=%v want=%v", err, gen.ErrActorPidNil)
	}
	if err := s.SendEnvelope(nil, gen.ActorEnvelope{Payload: msg, Sender: gen.NoSender}); err != gen.ErrActorPidNil {
		t.Fatalf("SendEnvelope nil target error mismatch: got=%v want=%v", err, gen.ErrActorPidNil)
	}
}

func TestSystem_StopProcessDoesNotBlockWhenBusinessQueueFull(t *testing.T) {
	s := newStartedSystem(t)

	actor := &blockingActor{
		enterCh: make(chan struct{}, 1),
		blockCh: make(chan struct{}),
	}
	pid, err := s.SpawnActor(actor, gen.SpawnOptions{MailboxSize: 1})
	if err != nil {
		t.Fatalf("spawn actor failed: %v", err)
	}

	msg := &gen.Message{}
	if err := s.Tell(gen.NoSender, pid, msg); err != nil {
		t.Fatalf("first tell failed: %v", err)
	}

	select {
	case <-actor.enterCh:
	case <-time.After(time.Second):
		t.Fatal("actor did not enter message handler in time")
	}

	// actor 正在处理首条消息时，业务队列会占用 1 个槽位（另 1 个槽位为 stop 预留）
	if err := s.Tell(gen.NoSender, pid, msg); err != nil {
		t.Fatalf("second tell should enqueue business message, got err=%v", err)
	}

	stopDone := make(chan struct{})
	go func() {
		s.StopProcess(pid)
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("StopProcess should not block when business queue is full")
	}

	// 释放处理协程，避免测试清理阶段悬挂。
	close(actor.blockCh)
}

func TestSystem_SendEnvelopeRequiresStartedState(t *testing.T) {
	s := NewSystem()
	s.SetNodeID("node-test")
	err := s.SendEnvelope("target", gen.ActorEnvelope{Payload: gen.NewMessage(1, 1, nil), Sender: gen.NoSender})
	if err != gen.ErrComponentNotStart {
		t.Fatalf("SendEnvelope before start error mismatch: got=%v want=%v", err, gen.ErrComponentNotStart)
	}
}

func TestSystem_AskRejectsRemotePID(t *testing.T) {
	s := newStartedSystem(t)
	remote := gen.NewPID(1, "", "remote-node")
	_, err := s.Ask(gen.NoSender, remote, gen.NewMessage(1, 1, nil), time.Second)
	if err != gen.ErrActorNoAskClusterProvided {
		t.Fatalf("Ask remote pid error mismatch: got=%v want=%v", err, gen.ErrActorNoAskClusterProvided)
	}
}

func TestSystem_TellRemoteForwardsToCluster(t *testing.T) {
	cluster := &captureCluster{}
	s := newStartedSystemWithRemote(t, "node-test", cluster)

	from := gen.NewPID(100, "sender", "node-test")
	to := gen.NewPID(200, "remote", "remote-node")
	msg := gen.NewMessage(1, 2, []byte("payload"))

	if err := s.Tell(from, to, msg); err != nil {
		t.Fatalf("Tell remote failed: %v", err)
	}

	if cluster.from != from || cluster.to != to || cluster.msg != msg {
		t.Fatal("Tell remote should forward exact from/to/msg to cluster")
	}
}

func TestSystem_TellRemoteWithoutClusterReturnsErrClusterNil(t *testing.T) {
	s := newStartedSystem(t)

	err := s.Tell(gen.NoSender, gen.NewPID(2, "remote", "remote-node"), gen.NewMessage(1, 1, nil))
	if err != gen.ErrClusterNil {
		t.Fatalf("Tell remote without cluster error mismatch: got=%v want=%v", err, gen.ErrClusterNil)
	}
}

func TestSystem_TellRemoteAfterStopRejected(t *testing.T) {
	cluster := &captureCluster{}
	s := newStartedSystemWithRemote(t, "node-test", cluster)
	if err := s.Stop(context.Background()); err != nil {
		t.Fatalf("stop system failed: %v", err)
	}

	err := s.Tell(gen.NoSender, gen.NewPID(2, "remote", "remote-node"), gen.NewMessage(1, 1, nil))
	if err != gen.ErrComponentNotStart {
		t.Fatalf("Tell remote after stop error mismatch: got=%v want=%v", err, gen.ErrComponentNotStart)
	}
}

func TestSystem_StopProcessWithTypedNilPIDNoPanic(t *testing.T) {
	s := newStartedSystem(t)
	var pid *gen.PID

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("StopProcess should not panic for typed nil PID: %v", r)
		}
	}()
	s.StopProcess(pid)
}
