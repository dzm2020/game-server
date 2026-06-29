package actor

import (
	"game-server/framework/gen"
	"testing"
	"time"
)

func TestMailbox_ReservesSlotForStopMessage(t *testing.T) {
	m := newMailbox(1, nil)

	if err := m.push(gen.ActorEnvelope{Payload: &gen.Message{}}); err != nil {
		t.Fatalf("first push failed: %v", err)
	}
	if got, want := len(m.ch), 1; got != want {
		t.Fatalf("business queue length mismatch: got=%d want=%d", got, want)
	}

	done := make(chan struct{})
	go func() {
		m.pushStopMessage(gen.ActorEnvelope{Payload: &stopEnvelopeMessage{}, Sender: gen.NoSender})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("pushStopMessage should not block when business queue is full")
	}

	if got, want := len(m.ch), 2; got != want {
		t.Fatalf("queue should contain business+stop message: got=%d want=%d", got, want)
	}
}

func TestMailbox_PushRejectsAfterStop(t *testing.T) {
	m := newMailbox(1, nil)
	m.pushStopMessage(gen.ActorEnvelope{Payload: &stopEnvelopeMessage{}, Sender: gen.NoSender})

	if err := m.push(gen.ActorEnvelope{Payload: &gen.Message{}}); err != gen.ErrActorProcessStopped {
		t.Fatalf("push after stop error mismatch: got=%v want=%v", err, gen.ErrActorProcessStopped)
	}
}

func TestMailbox_BusinessPushRespectsReservedStopSlot(t *testing.T) {
	m := newMailbox(2, nil)

	if err := m.push(gen.ActorEnvelope{Payload: &gen.Message{}}); err != nil {
		t.Fatalf("first push failed: %v", err)
	}
	if err := m.push(gen.ActorEnvelope{Payload: &gen.Message{}}); err != nil {
		t.Fatalf("second push failed: %v", err)
	}
	if err := m.push(gen.ActorEnvelope{Payload: &gen.Message{}}); err != gen.ErrActorMailboxFull {
		t.Fatalf("third push should fail by reserved stop slot, got=%v want=%v", err, gen.ErrActorMailboxFull)
	}
}

func TestMailbox_PushStopMessageIdempotent(t *testing.T) {
	m := newMailbox(1, nil)
	stopEnv := gen.ActorEnvelope{Payload: &stopEnvelopeMessage{}, Sender: gen.NoSender}

	m.pushStopMessage(stopEnv)

	done := make(chan struct{})
	go func() {
		m.pushStopMessage(stopEnv)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second pushStopMessage should be idempotent and non-blocking")
	}

	if got, want := len(m.ch), 1; got != want {
		t.Fatalf("idempotent stop should enqueue once only: got=%d want=%d", got, want)
	}
}
