package component

import (
	"context"
	"errors"
	"testing"
)

func TestController_OrderAndState(t *testing.T) {
	ctrl := &Controller{}
	ctx := context.Background()

	if err := ctrl.Init(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if got := ctrl.State(); got != StateInited {
		t.Fatalf("state mismatch after Init, got=%s want=%s", got, StateInited)
	}

	if err := ctrl.Start(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if got := ctrl.State(); got != StateStarted {
		t.Fatalf("state mismatch after Start, got=%s want=%s", got, StateStarted)
	}

	if err := ctrl.Stop(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if got := ctrl.State(); got != StateStopped {
		t.Fatalf("state mismatch after Stop, got=%s want=%s", got, StateStopped)
	}
}

func TestController_InvalidOrder(t *testing.T) {
	ctrl := &Controller{}
	ctx := context.Background()

	if err := ctrl.Start(ctx, nil); !errors.Is(err, ErrInvalidOrder) {
		t.Fatalf("Start before Init should return ErrInvalidOrder, got=%v", err)
	}
	if err := ctrl.Stop(ctx, nil); !errors.Is(err, ErrInvalidOrder) {
		t.Fatalf("Stop before Start should return ErrInvalidOrder, got=%v", err)
	}
}

func TestController_OnlyOncePerPhase(t *testing.T) {
	ctrl := &Controller{}
	ctx := context.Background()

	if err := ctrl.Init(ctx, nil); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if err := ctrl.Init(ctx, nil); !errors.Is(err, ErrInitAlreadyCalled) {
		t.Fatalf("Init twice should return ErrInitAlreadyCalled, got=%v", err)
	}

	if err := ctrl.Start(ctx, nil); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := ctrl.Start(ctx, nil); !errors.Is(err, ErrStartAlreadyCalled) {
		t.Fatalf("Start twice should return ErrStartAlreadyCalled, got=%v", err)
	}

	if err := ctrl.Stop(ctx, nil); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if err := ctrl.Stop(ctx, nil); !errors.Is(err, ErrStopAlreadyCalled) {
		t.Fatalf("Stop twice should return ErrStopAlreadyCalled, got=%v", err)
	}
}

func TestController_FailedPhaseCannotRetry(t *testing.T) {
	ctrl := &Controller{}
	ctx := context.Background()
	expected := errors.New("init boom")

	err := ctrl.Init(ctx, func(context.Context) error { return expected })
	if err == nil {
		t.Fatal("Init should fail")
	}
	if !errors.Is(err, expected) {
		t.Fatalf("Init error should wrap original error, got=%v", err)
	}
	if got := ctrl.State(); got != StateNew {
		t.Fatalf("state should keep New when Init failed, got=%s", got)
	}
	if err = ctrl.Init(ctx, nil); !errors.Is(err, ErrInitAlreadyCalled) {
		t.Fatalf("Init retry should return ErrInitAlreadyCalled, got=%v", err)
	}
}
