package actor

import (
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestSpawnAfterShutdownReturnsErrSystemClosed(t *testing.T) {
	sys := NewSystem()
	sys.Shutdown()

	_, err := sys.Spawn(func(Context) {})
	if !errors.Is(err, ErrSystemClosed) {
		t.Fatalf("expected ErrSystemClosed, got %v", err)
	}
}

func TestTellAfterShutdownReturnsErrSystemClosed(t *testing.T) {
	sys := NewSystem()
	pid, err := sys.Spawn(func(Context) {})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	sys.Shutdown()

	err = sys.Tell(NoSender, pid, "msg")
	if !errors.Is(err, ErrSystemClosed) {
		t.Fatalf("expected ErrSystemClosed, got %v", err)
	}
}

func TestProcessSendAfterStopReturnsErrStopped(t *testing.T) {
	sys := NewSystem()
	proc := &process{
		system:  sys,
		pid:     PIDFromActorID(1),
		mailbox: make(chan Envelope, 1),
	}

	proc.stop()

	err := proc.send(Envelope{Payload: "after-stop"})
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("expected ErrStopped, got %v", err)
	}
}

func TestSpawnNilHandlerReturnsErrNilHandler(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	_, err := sys.Spawn(nil)
	if !errors.Is(err, ErrNilHandler) {
		t.Fatalf("expected ErrNilHandler, got %v", err)
	}
}

func TestSpawnActorNilHandlerReturnsErrNilHandler(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	var h IActor
	_, err := sys.SpawnActor(h)
	if !errors.Is(err, ErrNilHandler) {
		t.Fatalf("expected ErrNilHandler, got %v", err)
	}
}

func TestSpawnWithDuplicateNameReturnsErrActorNameExists(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	_, err := sys.Spawn(func(Context) {}, WithName("worker"))
	if err != nil {
		t.Fatalf("first spawn failed: %v", err)
	}

	_, err = sys.Spawn(func(Context) {}, WithName("worker"))
	if !errors.Is(err, ErrActorNameExists) {
		t.Fatalf("expected ErrActorNameExists, got %v", err)
	}
}

func TestTellActorNotFoundReturnsErrActorNotFound(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	err := sys.Tell(NoSender, PIDFromActorID(99999), "x")
	if !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound, got %v", err)
	}
}

func TestAskActorNotFoundReturnsErrActorNotFound(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	_, err := sys.Ask(NoSender, PIDFromActorID(99999), "x", 20*time.Millisecond)
	if !errors.Is(err, ErrActorNotFound) {
		t.Fatalf("expected ErrActorNotFound, got %v", err)
	}
}

func TestAskTimeoutReturnsErrAskTimeout(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	pid, err := sys.Spawn(func(Context) {})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	_, err = sys.Ask(NoSender, pid, "ping", 30*time.Millisecond)
	if !errors.Is(err, ErrAskTimeout) {
		t.Fatalf("expected ErrAskTimeout, got %v", err)
	}
}

func TestAskSuccessReturnsResponse(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	pid, err := sys.Spawn(func(ctx Context) {
		if msg, ok := ctx.Message().(string); ok && msg == "ping" {
			_ = ctx.Respond("pong")
		}
	})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	got, err := sys.Ask(NoSender, pid, "ping", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("ask failed: %v", err)
	}
	if got != "pong" {
		t.Fatalf("expected pong, got %v", got)
	}
}

func TestContextTellPropagatesSenderPID(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	senderCh := make(chan PID, 1)
	receiverPID, err := sys.Spawn(func(ctx Context) {
		senderCh <- ctx.Sender()
	})
	if err != nil {
		t.Fatalf("spawn receiver failed: %v", err)
	}

	forwarderPID, err := sys.Spawn(func(ctx Context) {
		if ctx.Message() == "forward" {
			_ = ctx.Tell(receiverPID, "hello")
		}
	})
	if err != nil {
		t.Fatalf("spawn forwarder failed: %v", err)
	}

	if err := sys.Tell(NoSender, forwarderPID, "forward"); err != nil {
		t.Fatalf("tell forwarder failed: %v", err)
	}

	select {
	case sender := <-senderCh:
		if sender != forwarderPID {
			t.Fatalf("expected sender %v, got %v", forwarderPID, sender)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting receiver message")
	}
}

func TestContextInitArgsReturnsCopy(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	valueCh := make(chan string, 1)
	pid, err := sys.Spawn(func(ctx Context) {
		args1 := ctx.InitArgs()
		if len(args1) > 0 {
			args1[0] = "mutated"
		}
		args2 := ctx.InitArgs()
		if len(args2) > 0 {
			if v, ok := args2[0].(string); ok {
				valueCh <- v
				return
			}
		}
		valueCh <- ""
	}, WithInitArgs("origin"))
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	if err := sys.Tell(NoSender, pid, "trigger"); err != nil {
		t.Fatalf("tell failed: %v", err)
	}

	select {
	case v := <-valueCh:
		if v != "origin" {
			t.Fatalf("expected origin, got %q", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting init args check")
	}
}

type doneAwareActor struct {
	doneClosed atomic.Bool
	doneSignal chan struct{}
}

func (a *doneAwareActor) OnInit(ctx Context) error {
	go func() {
		<-ctx.Done()
		a.doneClosed.Store(true)
		close(a.doneSignal)
	}()
	return nil
}

func (a *doneAwareActor) OnDestroy(Context) error    { return nil }
func (a *doneAwareActor) OnMessage(Context) error    { return nil }
func (a *doneAwareActor) OnError(Context, any) error { return nil }

func TestContextDoneClosesOnShutdown(t *testing.T) {
	sys := NewSystem()
	actor := &doneAwareActor{doneSignal: make(chan struct{})}

	_, err := sys.SpawnActor(actor)
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	sys.Shutdown()

	select {
	case <-actor.doneSignal:
		if !actor.doneClosed.Load() {
			t.Fatal("done signal received but flag not set")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting done channel close")
	}
}

func TestPoisonPillDrainsFrontMessagesOnly(t *testing.T) {
	sys := NewSystem()
	defer sys.Shutdown()

	processed := make(chan int, 8)
	pid, err := sys.Spawn(func(ctx Context) {
		if v, ok := ctx.Message().(int); ok {
			processed <- v
		}
	}, WithMailboxSize(8))
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	if err := sys.Tell(NoSender, pid, 1); err != nil {
		t.Fatalf("tell 1 failed: %v", err)
	}
	if err := sys.Tell(NoSender, pid, 2); err != nil {
		t.Fatalf("tell 2 failed: %v", err)
	}
	if err := sys.Tell(NoSender, pid, PoisonPill); err != nil {
		t.Fatalf("tell poison failed: %v", err)
	}
	if err := sys.Tell(NoSender, pid, 3); err != nil {
		t.Fatalf("tell 3 failed: %v", err)
	}

	waitProcessStopped(t, sys, pid, time.Second)

	got := drainInts(processed)
	if !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("expected [1 2], got %v", got)
	}
}

func TestShutdownDrainsMailboxBeforeStop(t *testing.T) {
	sys := NewSystem()

	var processed atomic.Int64
	pid, err := sys.Spawn(func(ctx Context) {
		if _, ok := ctx.Message().(int); ok {
			processed.Add(1)
		}
	}, WithMailboxSize(256))
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	const total = 100
	for i := 0; i < total; i++ {
		if err := sys.Tell(NoSender, pid, i); err != nil {
			t.Fatalf("tell %d failed: %v", i, err)
		}
	}

	sys.Shutdown()

	if got := processed.Load(); got != total {
		t.Fatalf("expected processed=%d, got %d", total, got)
	}
}

func waitProcessStopped(t *testing.T, sys *System, pid PID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok := sys.GetProcess(pid); !ok {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("process %v still exists after timeout", pid)
}

func drainInts(ch <-chan int) []int {
	var out []int
	for {
		select {
		case v := <-ch:
			out = append(out, v)
		default:
			return out
		}
	}
}
