package actor

import (
	"game-server/framework/gen"
	"time"
)

type actorContext struct {
	self       *gen.PID
	system     *System
	initArgs   []any
	done       <-chan struct{}
	current    gen.ActorEnvelope
	actor      gen.IActor
	askTimeout time.Duration
	route      gen.IActorRoute
}

func (c *actorContext) Self() *gen.PID {
	return c.self
}

func (c *actorContext) Sender() *gen.PID {
	return c.current.Sender
}

func (c *actorContext) InitArgs() []any {
	if len(c.initArgs) == 0 {
		return nil
	}
	return append([]any(nil), c.initArgs...)
}

func (c *actorContext) System() gen.ISystem {
	return c.system
}

func (c *actorContext) Done() <-chan struct{} {
	return c.done
}

func (c *actorContext) Tell(target any, msg *gen.Message) error {
	return c.system.Tell(c.self, target, msg)
}

func (c *actorContext) SetAskTimeout(timeout time.Duration) {
	c.askTimeout = timeout
}

func (c *actorContext) Ask(target any, msg *gen.Message) ([]byte, error) {
	return c.system.Ask(c.self, target, msg, c.askTimeout)
}

func (c *actorContext) Respond(v []byte) error {
	if c.current.Respond == nil {
		return ErrNoResponder
	}
	return c.current.Respond(v)
}
func (c *actorContext) Actor() gen.IActor {
	return c.actor
}

func (c *actorContext) GetRouter() gen.IActorRoute {
	return c.route
}

func (c *actorContext) Exit() {
	c.system.StopProcess(c.self)
}
