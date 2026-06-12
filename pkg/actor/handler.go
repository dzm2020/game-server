package actor

type Handler func(Context)

type ActorHandler interface {
	OnInit(Context)
	OnDestroy(Context)
	OnMessage(Context)
	OnPanic(Context, any)
}

type messageHandlerAdapter struct {
	fn Handler
}

func (h messageHandlerAdapter) OnInit(Context) {}

func (h messageHandlerAdapter) OnDestroy(Context) {}

func (h messageHandlerAdapter) OnMessage(ctx Context) {
	if h.fn != nil {
		h.fn(ctx)
	}
}

func (h messageHandlerAdapter) OnPanic(Context, any) {}

type BaseHandler struct {
}

func (h *BaseHandler) OnInit(Context) {}

func (h *BaseHandler) OnDestroy(Context) {}

func (h *BaseHandler) OnMessage(ctx Context) {
}

func (h *BaseHandler) OnPanic(Context, any) {}
