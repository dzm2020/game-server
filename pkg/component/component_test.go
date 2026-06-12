package component

import (
	"context"
	"testing"
)

func TestBaseComponent_DefaultLifecycle(t *testing.T) {
	base := &BaseComponent{}
	ctx := context.Background()

	if err := base.Init(); err != nil {
		t.Fatalf("Init should return nil, got=%v", err)
	}
	if err := base.Start(ctx); err != nil {
		t.Fatalf("Start should return nil, got=%v", err)
	}
	if err := base.Stop(ctx); err != nil {
		t.Fatalf("Stop should return nil, got=%v", err)
	}
}
