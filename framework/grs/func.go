package grs

import (
	"context"
	"game-server/framework/pkg/glog"
	"runtime/debug"
	"sync"

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
			glog.Error("panic recovered", zap.Any("panic", rec), zap.ByteString("stack", debug.Stack()))
		}
	}()
	if fn != nil {
		fn()
	}
}

func NewGroup() *Group {
	return &Group{}
}

type Group struct {
	runWG sync.WaitGroup
}

func (g *Group) Go(fn func()) {
	g.runWG.Add(1)
	go func() {
		defer g.runWG.Done()
		Try(fn, nil)
	}()
}

func (g *Group) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
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
