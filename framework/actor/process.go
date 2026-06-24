package actor

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type stopEnvelopeMessage struct {
}

type process struct {
	system   *System
	pid      *gen.PID
	mailbox  chan gen.ActorEnvelope
	initArgs []any
	route    gen.IActorRoute
	runCtx   context.Context
	cancel   context.CancelFunc
	stopping atomic.Bool
	stopped  atomic.Bool
	once     sync.Once
}

func (c *process) getPID() *gen.PID {
	return c.pid
}

func (c *process) getName() string {
	return c.pid.ActorName
}

func (c *process) run(actor gen.IActor) {
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

	glog.Info("actor启动", zap.String("pid", c.pid.String()))

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
			c.invokeWithPanicCallback(ctx, actor, func() error {
				return c.onMessage(ctx, actor, env)
			})
		}
	}
}

func (c *process) invokeWithPanicCallback(ctx gen.IContext, handler gen.IActor, fn func() error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			defer func() { _ = recover() }()
			_ = handler.OnError(ctx, recovered)
		}
	}()
	if err := fn(); err != nil {
		_ = handler.OnError(ctx, err)
		glog.Error("actor处理消息错误", zap.String("pid", c.pid.String()), zap.Error(err))
	}
}

func (c *process) onMessage(ctx gen.IContext, actor gen.IActor, env gen.ActorEnvelope) error {
	switch msg := env.Payload.(type) {
	case *stopEnvelopeMessage:
		return actor.OnDestroy(ctx)
	case *gen.Message:
		if c.route.Exist(msg.Cmd, msg.Act) {
			return c.route.Handle(ctx, msg)
		}
		return actor.OnMessage(ctx)
	case gen.ActorTask:
		return msg(ctx)
	default:
		return actor.OnMessage(ctx)
	}
}

func (c *process) send(env gen.ActorEnvelope) error {
	if c.stopped.Load() || c.stopping.Load() {
		glog.Error("actor接收消息错误", zap.String("pid", c.pid.String()), zap.Error(ErrStopped))
		return ErrStopped
	}
	select {
	case c.mailbox <- env:
		return nil
	default:
		glog.Error("actor接收消息错误", zap.String("pid", c.pid.String()), zap.Error(ErrMailboxFull))
		return ErrMailboxFull
	}
}

func (c *process) requestStop() {
	if c.stopped.Load() {
		return
	}
	if !c.stopping.CompareAndSwap(false, true) {
		return
	}
	stopEnv := gen.ActorEnvelope{
		Payload: &stopEnvelopeMessage{},
		Sender:  gen.NoSender,
	}

	select {
	case c.mailbox <- stopEnv:
	default:
		// 邮箱满时异步重试，保证终止消息最终入队。
		grs.SafeGo(func() {
			select {
			case c.mailbox <- stopEnv:
			case <-c.runCtx.Done():
			}
		})
	}
}

func (c *process) stop() {
	c.once.Do(func() {
		c.stopped.Store(true)
		c.system.removeProcess(c)
		if c.cancel != nil {
			c.cancel()
		}
		glog.Info("actor关闭", zap.String("pid", c.pid.String()))
	})
}
