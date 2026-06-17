package actor

type Handler func(Context)

type IActor interface {
	OnInit(Context) error
	OnDestroy(Context) error
	OnMessage(Context) error
	OnError(Context, any) error
}

type messageActorAdapter struct {
	fn Handler
}

func (h messageActorAdapter) OnInit(Context) error { return nil }

func (h messageActorAdapter) OnDestroy(Context) error { return nil }

func (h messageActorAdapter) OnMessage(ctx Context) error {
	if h.fn != nil {
		h.fn(ctx)
	}
	return nil
}

func (h messageActorAdapter) OnError(Context, any) error { return nil }

type BaseActor struct {
}

func (h *BaseActor) OnInit(Context) error { return nil }

func (h *BaseActor) OnDestroy(Context) error { return nil }

func (h *BaseActor) OnMessage(ctx Context) error { return nil }

func (h *BaseActor) OnError(Context, any) error { return nil }
