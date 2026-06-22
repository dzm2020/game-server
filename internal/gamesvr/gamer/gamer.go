package gamer

import (
	"game-server/framework/gen"
	"time"
)

type GameActor struct {
	gen.BaseActor
}

func (h *GameActor) OnInit(ctx gen.IContext) error {
	time.Sleep(time.Second * 3)
	pid := gen.NewPID(0, "chat", "chat1")

	msg := gen.NewMessage(1, 1, []byte("1111111111111111111"))
	_ = ctx.Tell(pid, msg)
	time.Sleep(time.Second * 3)
	_ = ctx.Tell(pid, msg)

	return ctx.Tell(pid, msg)
}
