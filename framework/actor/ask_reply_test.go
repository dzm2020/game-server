package actor

import (
	"bytes"
	"game-server/framework/gen"
	"testing"
	"time"
)

func TestAskReply_FirstResponseWins(t *testing.T) {
	reply := newAskReply()
	responder := reply.responder()

	if err := responder([]byte("first")); err != nil {
		t.Fatalf("first responder call failed: %v", err)
	}
	if err := responder([]byte("second")); err != nil {
		t.Fatalf("second responder call failed: %v", err)
	}

	got, err := reply.wait(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("wait failed: %v", err)
	}
	if !bytes.Equal(got, []byte("first")) {
		t.Fatalf("first response should win, got=%q", string(got))
	}
}

func TestAskReply_Timeout(t *testing.T) {
	reply := newAskReply()

	_, err := reply.wait(20 * time.Millisecond)
	if err != gen.ErrActorAskTimeout {
		t.Fatalf("timeout error mismatch: got=%v want=%v", err, gen.ErrActorAskTimeout)
	}
}
