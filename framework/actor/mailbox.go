package actor

import (
	"game-server/framework/gen"
)

type stopEnvelopeMessage struct{}

func newMailbox(chSize int, ctx *actorContext) *mailbox {
	return &mailbox{
		ch:      make(chan gen.ActorEnvelope, chSize),
		context: ctx,
		running: true,
	}
}

type mailbox struct {
	ch      chan gen.ActorEnvelope
	context *actorContext
	running bool
}

func (m *mailbox) push(env gen.ActorEnvelope) error {
	select {
	case m.ch <- env:
		return nil
	default:
		return gen.ErrActorMailboxFull
	}
}

func (m *mailbox) safePush(stopEnv gen.ActorEnvelope) {
	select {
	case m.ch <- stopEnv:
	}
}

func (m *mailbox) run() {
	actor := m.context.Actor()
	m.invokeWithPanicCallback(actor, func() error {
		return actor.OnInit(m.context)
	})
	defer m.invokeWithPanicCallback(actor, func() error {
		return actor.OnDestroy(m.context)
	})
	for m.running {
		select {
		case env := <-m.ch:
			m.context.current = env
			m.invokeWithPanicCallback(actor, func() error {
				return m.onMessage(m.context, env)
			})
		}
	}
}

func (m *mailbox) onMessage(ctx gen.IContext, env gen.ActorEnvelope) error {
	switch msg := env.Payload.(type) {
	case *stopEnvelopeMessage:
		m.running = false
		return nil
	case *gen.Message:
		route := m.context.GetRouter()
		if route != nil && route.Exist(msg.Cmd, msg.Act) {
			return route.Handle(ctx, msg)
		} else {
			return ctx.Actor().OnMessage(ctx)
		}
	case gen.ActorTask:
		return msg(ctx)
	default:
		return ctx.Actor().OnMessage(ctx)
	}
}

func (m *mailbox) invokeWithPanicCallback(handler gen.IActor, fn func() error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			defer func() { _ = recover() }()
			handler.OnError(m.context, recovered)
		}
	}()
	if err := fn(); err != nil {
		handler.OnError(m.context, err)
	}
}
