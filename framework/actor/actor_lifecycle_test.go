package actor_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"game-server/framework/actor"
	"game-server/framework/gen"
)

type signalActor struct {
	onDestroy func()
}

func (a *signalActor) OnInit(gen.IContext) error { return nil }

func (a *signalActor) OnDestroy(gen.IContext) error {
	if a.onDestroy != nil {
		a.onDestroy()
	}
	return nil
}

func (a *signalActor) OnMessage(gen.IContext) error { return nil }

func (a *signalActor) OnError(gen.IContext, any) error { return nil }

func TestStopProcessLifecycle(t *testing.T) {
	system := actor.NewSystemWithNodeID("stop-process")
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = system.Stop(stopCtx)
	})

	destroyed := make(chan struct{})
	var once sync.Once
	pid, err := system.SpawnActor(&signalActor{
		onDestroy: func() {
			once.Do(func() { close(destroyed) })
		},
	}, gen.SpawnOptions{Name: "stop-target"})
	if err != nil {
		t.Fatalf("spawn actor failed: %v", err)
	}

	system.StopProcess(pid)

	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("stop process timeout: OnDestroy not called")
	}

	if err := system.Tell(gen.NoSender, pid, gen.NewMessage(1, 1, nil)); err != gen.ErrActorNotFound {
		t.Fatalf("actor should be unregistered after stop, got err=%v", err)
	}
}

func TestConcurrentSpawnAndStop(t *testing.T) {
	system := actor.NewSystemWithNodeID("spawn-stop-race")
	const workers = 6
	const perWorker = 100

	startCh := make(chan struct{})
	spawnErrCh := make(chan error, workers)
	stopErrCh := make(chan error, 1)

	for w := 0; w < workers; w++ {
		go func(worker int) {
			<-startCh
			for i := 0; i < perWorker; i++ {
				_, err := system.SpawnActor(&gen.BaseActor{}, gen.SpawnOptions{})
				if err != nil && err != gen.ErrActorSystemClosed {
					spawnErrCh <- fmt.Errorf("worker=%d iter=%d err=%w", worker, i, err)
					return
				}
			}
			spawnErrCh <- nil
		}(w)
	}

	go func() {
		<-startCh
		time.Sleep(10 * time.Millisecond)
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		stopErrCh <- system.Stop(stopCtx)
	}()

	close(startCh)

	for i := 0; i < workers; i++ {
		if err := <-spawnErrCh; err != nil {
			t.Fatalf("spawn race failed: %v", err)
		}
	}

	if err := <-stopErrCh; err != nil {
		t.Fatalf("stop should finish under concurrent spawn, got err=%v", err)
	}

	if _, err := system.SpawnActor(&gen.BaseActor{}, gen.SpawnOptions{}); err != gen.ErrActorSystemClosed {
		t.Fatalf("spawn after stop must fail with system closed, got=%v", err)
	}
}

func TestStopWithBlockingTask(t *testing.T) {
	system := actor.NewSystemWithNodeID("blocking-stop")

	started := make(chan struct{})
	release := make(chan struct{})
	destroyed := make(chan struct{})
	var startOnce sync.Once
	var destroyOnce sync.Once

	route := actor.NewRoute()
	route.Register(7, 1, func(ctx gen.IContext, request interface{}) error {
		startOnce.Do(func() { close(started) })
		<-release
		return nil
	}, nil)

	pid, err := system.SpawnActor(&signalActor{
		onDestroy: func() {
			destroyOnce.Do(func() { close(destroyed) })
		},
	}, gen.SpawnOptions{
		Name:  "blocking-actor",
		Route: route,
	})
	if err != nil {
		t.Fatalf("spawn blocking actor failed: %v", err)
	}

	if err := system.Tell(gen.NoSender, pid, gen.NewMessage(7, 1, nil)); err != nil {
		t.Fatalf("send blocking message failed: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("blocking handler did not start in time")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if err := system.Stop(stopCtx); err == nil {
		t.Fatal("expected stop timeout while handler is blocked")
	}

	close(release)

	select {
	case <-destroyed:
	case <-time.After(2 * time.Second):
		t.Fatal("actor not destroyed after releasing blocking task")
	}
}
