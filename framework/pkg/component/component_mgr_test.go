package component

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type recordingComponent struct {
	BaseComponent
	id         string
	events     *[]string
	initErr    error
	startErr   error
	stopErr    error
	stopCalled bool
}

func (c *recordingComponent) Init(_ context.Context) error {
	*c.events = append(*c.events, c.id+":init")
	return c.initErr
}

func (c *recordingComponent) Start(_ context.Context) error {
	*c.events = append(*c.events, c.id+":start")
	return c.startErr
}

func (c *recordingComponent) Stop(_ context.Context) error {
	c.stopCalled = true
	*c.events = append(*c.events, c.id+":stop")
	return c.stopErr
}

type recordingComponentA struct{ *recordingComponent }
type recordingComponentB struct{ *recordingComponent }

func TestManager_RegisterAndGetByType(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}

	if err := mgr.AddComponent(c1); err != nil {
		t.Fatalf("AddComponent failed: %v", err)
	}
	if got := mgr.ComponentCount(); got != 1 {
		t.Fatalf("ComponentCount mismatch, got=%d want=1", got)
	}

	got := mgr.GetComponent((*recordingComponentA)(nil))
	if got == nil {
		t.Fatal("GetComponent should return registered component")
	}
	if got != c1 {
		t.Fatal("GetComponent returned unexpected instance")
	}
}

func TestManager_AddComponentErrors(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}

	if err := mgr.AddComponent(nil); !errors.Is(err, ErrComponentCannotBeNil) {
		t.Fatalf("AddComponent(nil) error mismatch, got=%v", err)
	}
	if err := mgr.AddComponent(c1); err != nil {
		t.Fatalf("AddComponent first component failed: %v", err)
	}
	if err := mgr.AddComponent(c1); !errors.Is(err, ErrComponentAlreadyRegistered) {
		t.Fatalf("duplicate AddComponent error mismatch, got=%v", err)
	}

	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := mgr.AddComponent(&recordingComponentB{recordingComponent: &recordingComponent{id: "c2", events: &events}}); !errors.Is(err, ErrCannotRegisterComponentAfterStarted) {
		t.Fatalf("AddComponent after started error mismatch, got=%v", err)
	}
}

func TestManager_RemoveComponentBeforeStart(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}
	c2 := &recordingComponentB{recordingComponent: &recordingComponent{id: "c2", events: &events}}

	if err := mgr.AddComponent(c1); err != nil {
		t.Fatalf("AddComponent c1 failed: %v", err)
	}
	if err := mgr.AddComponent(c2); err != nil {
		t.Fatalf("AddComponent c2 failed: %v", err)
	}
	if err := mgr.RemoveComponent((*recordingComponentA)(nil)); err != nil {
		t.Fatalf("RemoveComponent c1 failed: %v", err)
	}

	if got := mgr.ComponentCount(); got != 1 {
		t.Fatalf("ComponentCount mismatch after remove, got=%d want=1", got)
	}
	if got := mgr.GetComponent((*recordingComponentA)(nil)); got != nil {
		t.Fatalf("removed component should not be returned, got=%T", got)
	}
	if got := mgr.GetComponent((*recordingComponentB)(nil)); got != c2 {
		t.Fatalf("remaining component mismatch, got=%T", got)
	}

	// 移除后同类型可再次注册。
	if err := mgr.AddComponent(&recordingComponentA{recordingComponent: &recordingComponent{id: "c3", events: &events}}); err != nil {
		t.Fatalf("AddComponent after remove failed: %v", err)
	}
}

func TestManager_RemoveComponentErrors(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}

	if err := mgr.RemoveComponent(nil); !errors.Is(err, ErrComponentTypeCannotBeNil) {
		t.Fatalf("RemoveComponent(nil) error mismatch, got=%v", err)
	}
	if err := mgr.RemoveComponent((*recordingComponentA)(nil)); !errors.Is(err, ErrComponentNotRegistered) {
		t.Fatalf("RemoveComponent(not registered) error mismatch, got=%v", err)
	}
	if err := mgr.AddComponent(c1); err != nil {
		t.Fatalf("AddComponent failed: %v", err)
	}
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := mgr.RemoveComponent((*recordingComponentA)(nil)); !errors.Is(err, ErrCannotRemoveComponentAfterStarted) {
		t.Fatalf("RemoveComponent after started error mismatch, got=%v", err)
	}
}

func TestManager_StartInitOrderAndCannotRestart(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}
	c2 := &recordingComponentB{recordingComponent: &recordingComponent{id: "c2", events: &events}}

	if err := mgr.AddComponent(c1); err != nil {
		t.Fatalf("AddComponent c1 failed: %v", err)
	}
	if err := mgr.AddComponent(c2); err != nil {
		t.Fatalf("AddComponent c2 failed: %v", err)
	}
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	want := []string{"c1:init", "c2:init", "c1:start", "c2:start"}
	if strings.Join(events, ",") != strings.Join(want, ",") {
		t.Fatalf("event order mismatch, got=%v want=%v", events, want)
	}
	if err := mgr.Start(context.Background()); !errors.Is(err, ErrManagerAlreadyStarted) {
		t.Fatalf("Start twice should return ErrManagerAlreadyStarted, got=%v", err)
	}
}

func TestManager_StartRollbackOnFailure(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}
	c2 := &recordingComponentB{recordingComponent: &recordingComponent{id: "c2", events: &events, startErr: errors.New("boom")}}

	_ = mgr.AddComponent(c1)
	_ = mgr.AddComponent(c2)
	err := mgr.Start(context.Background())
	if err == nil {
		t.Fatal("Start should fail when one component start returns error")
	}
	if !strings.Contains(err.Error(), "组件启动失败") {
		t.Fatalf("Start error message mismatch, got=%v", err)
	}
	if !c1.stopCalled {
		t.Fatal("already-started components should be stopped on rollback")
	}
}

func TestManager_StopReverseOrderAndIdempotent(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}
	c2 := &recordingComponentB{recordingComponent: &recordingComponent{id: "c2", events: &events}}

	_ = mgr.AddComponent(c1)
	_ = mgr.AddComponent(c2)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	got := mgr.GetComponent(&recordingComponentA{})
	got1 := mgr.GetComponent(&recordingComponentB{})
	_, _ = got, got1
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop second time should keep first result, got=%v", err)
	}

	joined := strings.Join(events, ",")
	if !strings.Contains(joined, "c2:stop,c1:stop") {
		t.Fatalf("stop order should be reverse registration, events=%v", events)
	}
}

func TestManager_StopReturnsLastError(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	err1 := errors.New("e1")
	err2 := errors.New("e2")
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events, stopErr: err1}}
	c2 := &recordingComponentB{recordingComponent: &recordingComponent{id: "c2", events: &events, stopErr: err2}}

	_ = mgr.AddComponent(c1)
	_ = mgr.AddComponent(c2)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := mgr.Stop(context.Background()); !errors.Is(err, err1) {
		t.Fatalf("Stop should return last stop error in reverse order, got=%v", err)
	}
}

func TestManager_StartAfterStoppedRejected(t *testing.T) {
	mgr := NewComponentsMgr()
	events := make([]string, 0)
	c1 := &recordingComponentA{recordingComponent: &recordingComponent{id: "c1", events: &events}}

	_ = mgr.AddComponent(c1)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if err := mgr.Start(context.Background()); !errors.Is(err, ErrManagerStoppedCannotRestart) {
		t.Fatalf("Start after Stop should return ErrManagerStoppedCannotRestart, got=%v", err)
	}
}
