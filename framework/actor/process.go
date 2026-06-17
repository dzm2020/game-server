package actor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type process struct {
	system   *System
	pid      PID
	mailbox  chan Envelope
	initArgs []any
	route    *Route
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

func (c *process) run(actor IActor) {
	if c.runCtx == nil {
		c.runCtx = context.Background()
	}

	defer c.stop()

	system := c.system
	ctx := &actorContext{
		self:       c.pid,
		system:     system,
		initArgs:   c.initArgs,
		done:       c.runCtx.Done(),
		actor:      actor,
		askTimeout: time.Second * 3,
		route:      c.route,
	}

	c.invokeWithPanicCallback(ctx, actor, func() error {
		return actor.OnInit(ctx)
	})

	for {
		select {
		case <-c.runCtx.Done():
			c.invokeWithPanicCallback(ctx, actor, func() error {
				return actor.OnDestroy(ctx)
			})
			return
		case env := <-c.mailbox:
			ctx.current = env
			//if isPoisonPill(env.Payload) {
			c.invokeWithPanicCallback(ctx, actor, func() error {
				return actor.OnDestroy(ctx)
			})
			return
			//}
			c.invokeWithPanicCallback(ctx, actor, func() error {
				if c.stopped.Load() {
					return nil
				}
				return actor.OnMessage(ctx)
			})
		}
	}
}

func (c *process) invokeWithPanicCallback(ctx Context, handler IActor, fn func() error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			defer func() { _ = recover() }()
			_ = handler.OnError(ctx, recovered)
		}
	}()
	if err := fn(); err != nil {
		_ = handler.OnError(ctx, err)
	}
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
