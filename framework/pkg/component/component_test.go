package component

import (
	"context"
	"testing"
)

func TestBaseComponent_DefaultLifecycle(t *testing.T) {
	base := &BaseComponent{}
	ctx := context.Background()

	if err := base.Init(ctx); err != nil {
		t.Fatalf("Init should return nil, got=%v", err)
	}
	if got := base.Status(); got != LifecycleStateInited {
		t.Fatalf("status mismatch after Init, got=%s want=%s", got, LifecycleStateInited)
	}
	if err := base.Start(ctx); err != nil {
		t.Fatalf("Start should return nil, got=%v", err)
	}
	if got := base.Status(); got != LifecycleStateStarted {
		t.Fatalf("status mismatch after Start, got=%s want=%s", got, LifecycleStateStarted)
	}
	if err := base.Stop(ctx); err != nil {
		t.Fatalf("Stop should return nil, got=%v", err)
	}
	if got := base.Status(); got != LifecycleStateStopped {
		t.Fatalf("status mismatch after Stop, got=%s want=%s", got, LifecycleStateStopped)
	}
}

func TestBaseComponent_EnforceOrderAndOnlyOnce(t *testing.T) {
	base := &BaseComponent{}
	ctx := context.Background()

	if err := base.Start(ctx); err == nil {
		t.Fatal("Start before Init should fail")
	}
	if err := base.Init(ctx); err != nil {
		t.Fatalf("Init should succeed, got=%v", err)
	}
	if err := base.Init(ctx); err == nil {
		t.Fatal("Init should only execute once")
	}
	if err := base.Stop(ctx); err == nil {
		t.Fatal("Stop before Start should fail")
	}
	if err := base.Start(ctx); err != nil {
		t.Fatalf("Start should succeed after Init, got=%v", err)
	}
	if err := base.Start(ctx); err == nil {
		t.Fatal("Start should only execute once")
	}
	if err := base.Stop(ctx); err != nil {
		t.Fatalf("Stop should succeed after Start, got=%v", err)
	}
	if err := base.Stop(ctx); err == nil {
		t.Fatal("Stop should only execute once")
	}
}
