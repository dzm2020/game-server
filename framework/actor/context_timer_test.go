package actor

import (
	"game-server/framework/gen"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type tickerProbeActor struct {
	interval time.Duration

	readyCh chan struct{}
	tickCh  chan struct{}

	mu     sync.Mutex
	stopFn func()

	tickCount atomic.Int32
}

func newTickerProbeActor(interval time.Duration) *tickerProbeActor {
	return &tickerProbeActor{
		interval: interval,
		readyCh:  make(chan struct{}),
		tickCh:   make(chan struct{}, 8),
	}
}

func (a *tickerProbeActor) OnInit(ctx gen.IContext) error {
	a.mu.Lock()
	a.stopFn = ctx.Ticker(a.interval, func(gen.IContext) error {
		a.tickCount.Add(1)
		select {
		case a.tickCh <- struct{}{}:
		default:
		}
		return nil
	})
	a.mu.Unlock()

	close(a.readyCh)
	return nil
}

func (a *tickerProbeActor) OnDestroy(gen.IContext) error {
	a.stopTicker()
	return nil
}

func (a *tickerProbeActor) OnMessage(gen.IContext) error { return nil }
func (a *tickerProbeActor) OnError(gen.IContext, any)    {}

func (a *tickerProbeActor) stopTicker() {
	a.mu.Lock()
	stop := a.stopFn
	a.mu.Unlock()
	if stop != nil {
		stop()
	}
}

type afterProbeActor struct {
	delay time.Duration

	readyCh chan struct{}
	fireCh  chan struct{}

	mu     sync.Mutex
	stopFn func()

	fireCount atomic.Int32
}

func newAfterProbeActor(delay time.Duration) *afterProbeActor {
	return &afterProbeActor{
		delay:   delay,
		readyCh: make(chan struct{}),
		fireCh:  make(chan struct{}, 4),
	}
}

func (a *afterProbeActor) OnInit(ctx gen.IContext) error {
	a.mu.Lock()
	a.stopFn = ctx.AfterFunc(a.delay, func(gen.IContext) error {
		a.fireCount.Add(1)
		select {
		case a.fireCh <- struct{}{}:
		default:
		}
		return nil
	})
	a.mu.Unlock()

	close(a.readyCh)
	return nil
}

func (a *afterProbeActor) OnDestroy(gen.IContext) error {
	a.stopAfter()
	return nil
}

func (a *afterProbeActor) OnMessage(gen.IContext) error { return nil }
func (a *afterProbeActor) OnError(gen.IContext, any)    {}

func (a *afterProbeActor) stopAfter() {
	a.mu.Lock()
	stop := a.stopFn
	a.mu.Unlock()
	if stop != nil {
		stop()
	}
}

func TestActorContext_TickerDispatchAndStop(t *testing.T) {
	s := newStartedSystem(t)
	actor := newTickerProbeActor(50 * time.Millisecond)

	pid, err := s.SpawnActor(actor, gen.SpawnOptions{MailboxSize: 8})
	if err != nil {
		t.Fatalf("spawn ticker actor failed: %v", err)
	}
	t.Cleanup(func() { s.StopProcess(pid) })

	select {
	case <-actor.readyCh:
	case <-time.After(time.Second):
		t.Fatal("ticker actor init timeout")
	}

	select {
	case <-actor.tickCh:
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("ticker task was not dispatched")
	}

	actor.stopTicker()
	countAtStop := actor.tickCount.Load()

	time.Sleep(120 * time.Millisecond)
	got := actor.tickCount.Load()
	if got > countAtStop+1 {
		t.Fatalf("ticker should stop shortly after stop(): before=%d after=%d", countAtStop, got)
	}
}

func TestActorContext_AfterFuncFiresOnce(t *testing.T) {
	s := newStartedSystem(t)
	actor := newAfterProbeActor(40 * time.Millisecond)

	pid, err := s.SpawnActor(actor, gen.SpawnOptions{MailboxSize: 8})
	if err != nil {
		t.Fatalf("spawn after actor failed: %v", err)
	}
	t.Cleanup(func() { s.StopProcess(pid) })

	select {
	case <-actor.readyCh:
	case <-time.After(time.Second):
		t.Fatal("after actor init timeout")
	}

	select {
	case <-actor.fireCh:
	case <-time.After(1200 * time.Millisecond):
		t.Fatal("after func was not dispatched")
	}

	time.Sleep(150 * time.Millisecond)
	if got := actor.fireCount.Load(); got != 1 {
		t.Fatalf("after func should fire exactly once: got=%d want=1", got)
	}
}

func TestActorContext_AfterFuncStopCancels(t *testing.T) {
	s := newStartedSystem(t)
	actor := newAfterProbeActor(200 * time.Millisecond)

	pid, err := s.SpawnActor(actor, gen.SpawnOptions{MailboxSize: 8})
	if err != nil {
		t.Fatalf("spawn after actor failed: %v", err)
	}
	t.Cleanup(func() { s.StopProcess(pid) })

	select {
	case <-actor.readyCh:
	case <-time.After(time.Second):
		t.Fatal("after actor init timeout")
	}

	actor.stopAfter()
	time.Sleep(260 * time.Millisecond)

	if got := actor.fireCount.Load(); got != 0 {
		t.Fatalf("after func should not fire after stop: got=%d want=0", got)
	}
}
