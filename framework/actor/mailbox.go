package actor

import (
	"game-server/framework/gen"
	"sync"
)

type stopEnvelopeMessage struct{}

func newMailbox(chSize int, ctx *actorContext) *mailbox {
	return &mailbox{
		// 预留 1 个槽位给 stop 消息，确保停止信号可异步入队。
		ch:      make(chan gen.ActorEnvelope, chSize+1),
		context: ctx,
		running: true,
	}
}

type mailbox struct {
	ch      chan gen.ActorEnvelope
	context *actorContext
	running bool

	mu       sync.Mutex
	closed   bool
	stopOnce sync.Once
}

func (m *mailbox) push(env gen.ActorEnvelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return gen.ErrActorProcessStopped
	}

	// 保留最后 1 个槽位给 stop 消息。
	if len(m.ch) >= cap(m.ch)-1 {
		return gen.ErrActorMailboxFull
	}
	m.ch <- env
	return nil
}

func (m *mailbox) pushStopMessage(stopEnv gen.ActorEnvelope) {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		defer m.mu.Unlock()

		m.closed = true
		m.ch <- stopEnv
	})
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
		env := <-m.ch
		m.context.current = env
		m.invokeWithPanicCallback(actor, func() error {
			return m.onMessage(m.context, env)
		})
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
