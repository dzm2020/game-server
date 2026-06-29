package actor

import (
	"errors"
	"game-server/framework/gen"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestRoute_HandleNotFound(t *testing.T) {
	r := NewRoute()
	err := r.Handle(nil, gen.NewMessage(1, 1, nil))
	if !errors.Is(err, gen.ErrActorRouteNotFound) {
		t.Fatalf("route not found error mismatch: got=%v want wrapped %v", err, gen.ErrActorRouteNotFound)
	}
}

func TestRoute_DuplicateRegisterKeepsFirstHandler(t *testing.T) {
	r := NewRoute()

	var firstCalled, secondCalled bool
	r.Register(1, 2, func(gen.IContext, interface{}) error {
		firstCalled = true
		return nil
	}, nil)
	r.Register(1, 2, func(gen.IContext, interface{}) error {
		secondCalled = true
		return nil
	}, nil)

	if err := r.Handle(nil, gen.NewMessage(1, 2, nil)); err != nil {
		t.Fatalf("handle failed: %v", err)
	}
	if !firstCalled || secondCalled {
		t.Fatalf("duplicate register should keep first handler: first=%v second=%v", firstCalled, secondCalled)
	}
}

func TestRoute_HandleProtoDecode(t *testing.T) {
	r := NewRoute()

	var got *gen.PID
	r.Register(2, 3, func(_ gen.IContext, request interface{}) error {
		pid, ok := request.(*gen.PID)
		if !ok {
			t.Fatalf("request type mismatch: %T", request)
		}
		got = pid
		return nil
	}, &gen.PID{})

	want := &gen.PID{ActorID: 77, ActorName: "a", NodeID: "node-x"}
	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	if err = r.Handle(nil, gen.NewMessage(2, 3, data)); err != nil {
		t.Fatalf("handle failed: %v", err)
	}
	if got == nil || got.ActorID != want.ActorID || got.ActorName != want.ActorName || got.NodeID != want.NodeID {
		t.Fatalf("decoded request mismatch: got=%+v want=%+v", got, want)
	}
}

func TestRoute_HandleProtoDecodeError(t *testing.T) {
	r := NewRoute()
	r.Register(4, 5, func(gen.IContext, interface{}) error { return nil }, &gen.PID{})

	err := r.Handle(nil, gen.NewMessage(4, 5, []byte("not-protobuf")))
	if err == nil {
		t.Fatal("expected decode error, got nil")
	}
}
