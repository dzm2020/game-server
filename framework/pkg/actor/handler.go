package actor

type Handler func(Context)

type Actor interface {
	OnInit(Context)
	OnDestroy(Context)
	OnMessage(Context)
	OnPanic(Context, any)
}

type messageActorAdapter struct {
	fn Handler
}

func (h messageActorAdapter) OnInit(Context) {}

func (h messageActorAdapter) OnDestroy(Context) {}

func (h messageActorAdapter) OnMessage(ctx Context) {
	if h.fn != nil {
		h.fn(ctx)
	}
}

func (h messageActorAdapter) OnPanic(Context, any) {}

type BaseActor struct {
}

func (h *BaseActor) OnInit(Context) {}

func (h *BaseActor) OnDestroy(Context) {}

func (h *BaseActor) OnMessage(ctx Context) {
}

func (h *BaseActor) OnPanic(Context, any) {}
