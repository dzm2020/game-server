package actor

import "game-server/framework/gen"

type messageActorAdapter struct {
	fn gen.ActorHandler
}

func (h messageActorAdapter) OnInit(gen.IContext) error    { return nil }
func (h messageActorAdapter) OnDestroy(gen.IContext) error { return nil }
func (h messageActorAdapter) OnMessage(ctx gen.IContext) error {
	if h.fn != nil {
		h.fn(ctx)
	}
	return nil
}
func (h messageActorAdapter) OnError(gen.IContext, any) error {
	return nil
}
