package service

import (
	"actor"
	"fmt"
)

type Actor struct {
	actor.BaseHandler
}

func (h *Actor) OnInit(ctx actor.Context) {
	fmt.Printf("[hook] on_init\n")
}

func (h *Actor) OnMessage(ctx actor.Context) {
}
