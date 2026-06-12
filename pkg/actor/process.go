package actor

import (
	"context"
	"sync"
	"sync/atomic"
)

type process struct {
	system   *System
	pid      PID
	mailbox  chan Envelope
	initArgs []any
	runCtx   context.Context
	cancel   context.CancelFunc
	stopped  atomic.Bool
	once     sync.Once
}

func (c *process) getPID() PID {
	return c.pid
}

func (c *process) getName() string {
	return c.pid.ActorName
}

func (c *process) run(handler ActorHandler) {
	if c.runCtx == nil {
		c.runCtx = context.Background()
	}

	defer c.stop()

	system := c.system
	ctx := &actorContext{
		self:     c.pid,
		system:   system,
		initArgs: c.initArgs,
		done:     c.runCtx.Done(),
	}

	c.invokeWithPanicCallback(ctx, handler, func() {
		handler.OnInit(ctx)
	})

	for {
		select {
		case <-c.runCtx.Done():
			c.invokeWithPanicCallback(ctx, handler, func() {
				handler.OnDestroy(ctx)
			})
			return
		case env := <-c.mailbox:
			ctx.current = env
			if isPoisonPill(env.Payload) {
				c.invokeWithPanicCallback(ctx, handler, func() {
					handler.OnDestroy(ctx)
				})
				return
			}
			c.invokeWithPanicCallback(ctx, handler, func() {
				if c.stopped.Load() {
					return
				}
				handler.OnMessage(ctx)
			})
		}
	}
}

func (c *process) invokeWithPanicCallback(ctx Context, handler ActorHandler, fn func()) {
	defer func() {
		if recovered := recover(); recovered != nil {
			defer func() { _ = recover() }()
			handler.OnPanic(ctx, recovered)
		}
	}()
	fn()
}

func (c *process) send(env Envelope) error {
	if c.stopped.Load() {
		return ErrStopped
	}
	select {
	case c.mailbox <- env:
		return nil
	default:
		return ErrMailboxFull
	}
}

func (c *process) stop() {
	c.once.Do(func() {
		c.stopped.Store(true)
		c.system.removeProcess(c)
		if c.cancel != nil {
			c.cancel()
		}
	})
}
