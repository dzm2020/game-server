package actor

import (
	"game-server/framework/gen"
	"game-server/framework/pkg/stopper"
)

type process struct {
	stopper.Stopper
	system  *System
	ctx     *actorContext
	mailbox *mailbox
}

func (c *process) getPID() *gen.PID {
	return c.ctx.self
}

func (c *process) getName() string {
	return c.getPID().GetActorName()
}

func (c *process) push(env gen.ActorEnvelope) error {
	return c.mailbox.push(env)
}

func (c *process) stop() {
	ok := c.system.removeProcess(c)
	if !ok {
		return
	}
	stopEnv := gen.ActorEnvelope{
		Payload: &stopEnvelopeMessage{},
		Sender:  gen.NoSender,
	}
	c.mailbox.pushStopMessage(stopEnv)
}
