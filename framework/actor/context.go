package actor

import (
	"game-server/framework/protocol"
	"time"
)

type Responder func(v []byte) error

// Envelope 是投递到 actor mailbox 的消息载体。
type Envelope struct {
	Payload *protocol.Message
	Sender  PID
	Respond Responder
}

type Context interface {
	Self() PID
	Sender() PID
	Message() *protocol.Message
	InitArgs() []any
	System() *System
	Done() <-chan struct{}
	Tell(target any, msg *protocol.Message) error
	SetAskTimeout(timeout time.Duration)
	Ask(target any, msg *protocol.Message) ([]byte, error)
	Respond(v []byte) error
	Actor() IActor
}

type actorContext struct {
	self       PID
	system     *System
	initArgs   []any
	done       <-chan struct{}
	current    Envelope
	actor      IActor
	askTimeout time.Duration
	route      *Route
}

func (c *actorContext) Self() PID {
	return c.self
}

func (c *actorContext) Sender() PID {
	return c.current.Sender
}

func (c *actorContext) Message() *protocol.Message {
	return c.current.Payload
}

func (c *actorContext) InitArgs() []any {
	if len(c.initArgs) == 0 {
		return nil
	}
	return append([]any(nil), c.initArgs...)
}

func (c *actorContext) System() *System {
	return c.system
}

func (c *actorContext) Done() <-chan struct{} {
	return c.done
}

func (c *actorContext) Tell(target any, msg *protocol.Message) error {
	return c.system.Tell(c.self, target, msg)
}

func (c *actorContext) SetAskTimeout(timeout time.Duration) {
	c.askTimeout = timeout
}

func (c *actorContext) Ask(target any, msg *protocol.Message) ([]byte, error) {
	return c.system.Ask(c.self, target, msg, c.askTimeout)
}

func (c *actorContext) Respond(v []byte) error {
	if c.current.Respond == nil {
		return ErrNoResponder
	}
	return c.current.Respond(v)
}
func (c *actorContext) Actor() IActor {
	return c.actor
}
