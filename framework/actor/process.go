package actor

import (
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/stopper"
	"sync"
)

const actorProcessComponent = "actor.process"

type process struct {
	stopper.Stopper
	system   *System
	ctx      *actorContext
	mailbox  *mailbox
	stopOnce sync.Once
}

func (c *process) getPID() *gen.PID {
	return c.ctx.self
}

func (c *process) getName() string {
	return c.getPID().GetActorName()
}

func (c *process) push(env gen.ActorEnvelope) error {
	if c.IsStop() {
		return gen.ErrActorProcessStopped
	}
	return c.mailbox.push(env)
}

func (c *process) stop() {
	if !c.Stopper.Stop() {
		return
	}
	stopEnv := gen.ActorEnvelope{
		Payload: &stopEnvelopeMessage{},
		Sender:  gen.NoSender,
	}
	c.system.removeProcess(c)
	// 邮箱满时异步重试，保证终止消息最终入队。
	grs.SafeGo(func() {
		c.mailbox.safePush(stopEnv)
	})
}
