package grs

import (
	"context"
	"game-server/framework/pkg/glog"
	"runtime/debug"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SafeGo starts a goroutine with panic recovery and logging.
func SafeGo(fn func()) {
	go func() {
		Try(fn, nil)
	}()
}

func Try(fn func(), recoverFunc func(e error)) {
	defer func() {
		if rec := recover(); rec != nil {
			if err, ok := rec.(error); ok {
				if recoverFunc != nil {
					recoverFunc(err)
				}
			}
			glog.Panic("panic recovered", zap.Any("panic", rec), zap.ByteString("stack", debug.Stack()))
		}
	}()
	if fn != nil {
		fn()
	}
}

func NewGroup(ctx context.Context) *Group {
	g := &Group{}
	if ctx == nil {
		ctx = context.Background()
	}
	g.ctx, g.cancel = context.WithCancel(ctx)
	return g
}

type Group struct {
	runWG  sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (g *Group) Context() context.Context {
	return g.ctx
}
func (g *Group) Go(fn func(ctx context.Context)) {
	g.runWG.Add(1)
	go func() {
		defer g.runWG.Done()
		Try(func() {
			fn(g.ctx)
		}, nil)
	}()
}

func (g *Group) Cancel() {
	if g.cancel != nil {
		g.cancel()
	}
}
func (g *Group) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		g.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *Group) WaitTimeout(timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		g.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
}
