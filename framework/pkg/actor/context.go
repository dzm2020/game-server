package actor

type Responder func(v any) error

// Envelope 是投递到 actor mailbox 的消息载体。
type Envelope struct {
	Payload any
	Sender  PID
	Respond Responder
}

type Context interface {
	Self() PID
	Sender() PID
	Message() any
	InitArgs() []any
	System() *System
	Done() <-chan struct{}
	Tell(target any, msg any) error
	Respond(v any) error
}

type actorContext struct {
	self     PID
	system   *System
	initArgs []any
	done     <-chan struct{}
	current  Envelope
}

func (c *actorContext) Self() PID {
	return c.self
}

func (c *actorContext) Sender() PID {
	return c.current.Sender
}

func (c *actorContext) Message() any {
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

func (c *actorContext) Tell(target any, msg any) error {
	return c.system.Tell(c.self, target, msg)
}

func (c *actorContext) Respond(v any) error {
	if c.current.Respond == nil {
		return ErrNoResponder
	}
	return c.current.Respond(v)
}
