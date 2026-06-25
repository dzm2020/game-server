package actor_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"game-server/framework/actor"
	"game-server/framework/gen"
)

func TestActorLocalSendReceiveSmoke(t *testing.T) {
	system := actor.NewSystemWithNodeID("smoke-local")
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = system.Stop(stopCtx)
	})

	route := actor.NewRoute()
	tellReceived := make(chan []byte, 1)

	const (
		tellCmd uint8 = 10
		tellAct uint8 = 1
		askCmd  uint8 = 10
		askAct  uint8 = 2
	)

	route.Register(tellCmd, tellAct, func(ctx gen.IContext, request interface{}) error {
		msg, ok := request.(*gen.Message)
		if !ok {
			return gen.ErrActorInvalidTarget
		}
		tellReceived <- append([]byte(nil), msg.Data...)
		return nil
	}, nil)

	route.Register(askCmd, askAct, func(ctx gen.IContext, request interface{}) error {
		msg, ok := request.(*gen.Message)
		if !ok {
			return gen.ErrActorInvalidTarget
		}
		reply := append([]byte("ack:"), msg.Data...)
		return ctx.Respond(reply)
	}, nil)

	pid, err := system.SpawnActor(&gen.BaseActor{}, gen.SpawnOptions{
		Name:  "smoke-actor",
		Route: route,
	})
	if err != nil {
		t.Fatalf("spawn actor failed: %v", err)
	}

	tellPayload := []byte("local-tell")
	if err := system.Tell(gen.NoSender, pid, gen.NewMessage(tellCmd, tellAct, tellPayload)); err != nil {
		t.Fatalf("tell failed: %v", err)
	}

	select {
	case got := <-tellReceived:
		if !bytes.Equal(got, tellPayload) {
			t.Fatalf("tell payload mismatch, got=%q want=%q", got, tellPayload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tell timeout: actor did not receive local message")
	}

	askPayload := []byte("local-ask")
	reply, err := system.Ask(gen.NoSender, pid, gen.NewMessage(askCmd, askAct, askPayload), time.Second)
	if err != nil {
		t.Fatalf("ask failed: %v", err)
	}
	wantReply := []byte("ack:local-ask")
	if !bytes.Equal(reply, wantReply) {
		t.Fatalf("ask reply mismatch, got=%q want=%q", reply, wantReply)
	}
}
