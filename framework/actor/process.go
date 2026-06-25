package actor

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/obs"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/stopper"
	"sync"

	"go.uber.org/zap"
)

type stopEnvelopeMessage struct {
}

type process struct {
	stopper.Stopper
	system   *System
	pid      *gen.PID
	mailbox  chan gen.ActorEnvelope
	initArgs []any
	context  *actorContext
	route    gen.IActorRoute
	runCtx   context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once
}

func (c *process) getPID() *gen.PID {
	return c.pid
}

func (c *process) getName() string {
	return c.pid.ActorName
}

func (c *process) run(ctx context.Context, actor gen.IActor) {
	c.runCtx, c.cancel = context.WithCancel(ctx)

	system := c.system
	c.context = &actorContext{
		self:       c.pid,
		system:     system,
		initArgs:   c.initArgs,
		done:       c.runCtx.Done(),
		actor:      actor,
		askTimeout: gen.DefaultActorAskTimeout,
		route:      c.route,
	}

	obs.Inc("actor.process_total")

	defer func() {
		c.exit()
	}()

	glog.Info("actor启动", glog.Component("actor.process"), glog.PID(c.pid))

	c.invokeWithPanicCallback(actor, func() error {
		return actor.OnInit(c.context)
	})

	for !c.IsStop() {
		select {
		case <-c.runCtx.Done():
			return
		case env := <-c.mailbox:
			c.context.current = env
			c.invokeWithPanicCallback(actor, func() error {
				return c.onMessage(c.context, actor, env)
			})
		}
	}
}

func (c *process) invokeWithPanicCallback(handler gen.IActor, fn func() error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			defer func() { _ = recover() }()
			c.onError(c.context, handler, recovered)
		}
	}()
	if err := fn(); err != nil {
		c.onError(c.context, handler, err)
	}
}

func (c *process) onError(ctx gen.IContext, handler gen.IActor, err interface{}) {
	glog.Error("actor发生错误", glog.PID(c.pid), zap.Any("err", err))
	_ = handler.OnError(ctx, err)
}

func (c *process) onMessage(ctx gen.IContext, actor gen.IActor, env gen.ActorEnvelope) error {
	switch msg := env.Payload.(type) {
	case *stopEnvelopeMessage:
		c.Stop()
		return nil
	case *gen.Message:
		if c.route != nil && c.route.Exist(msg.Cmd, msg.Act) {
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
	if c.IsStop() {
		glog.Error("actor接收消息错误", glog.Component("actor.process"), glog.PID(c.pid), glog.Err(gen.ErrActorProcessStopped))
		return gen.ErrActorProcessStopped
	}
	select {
	case c.mailbox <- env:
		return nil
	default:
		obs.Inc("actor.mailbox_full_total")
		glog.Error("actor接收消息错误", glog.Component("actor.process"), glog.PID(c.pid), glog.Err(gen.ErrActorMailboxFull))
		return gen.ErrActorMailboxFull
	}
}

func (c *process) exit() {
	c.cancel()
	c.system.removeProcess(c)
	c.invokeWithPanicCallback(c.context.Actor(), func() error {
		return c.context.Actor().OnDestroy(c.context)
	})
	obs.Dec("actor.process_total")
}

func (c *process) requestStop() {
	c.stopOnce.Do(func() {
		stopEnv := gen.ActorEnvelope{
			Payload: &stopEnvelopeMessage{},
			Sender:  gen.NoSender,
		}
		ctx := c.runCtx
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case c.mailbox <- stopEnv:
		default:
			// 邮箱满时异步重试，保证终止消息最终入队。
			grs.SafeGo(func() {
				select {
				case c.mailbox <- stopEnv:
				case <-ctx.Done():
				}
			})
		}
		glog.Info("actor请求关闭", glog.PID(c.pid))
	})
}
