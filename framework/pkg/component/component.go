package component

import (
	"context"
	"fmt"
	"sync/atomic"
)

type IComponent interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Status() LifecycleState
}

var ErrInvalidLifecycleTransition = fmt.Errorf("组件生命周期转换非法")

type BaseComponent struct {
	status atomic.Int64
}

func (c *BaseComponent) Init(ctx context.Context) error {
	_ = ctx
	return c.transition(LifecycleStateNew, LifecycleStateInited, "Init")
}

func (c *BaseComponent) Start(ctx context.Context) error {
	_ = ctx
	return c.transition(LifecycleStateInited, LifecycleStateStarted, "Start")
}

func (c *BaseComponent) Stop(ctx context.Context) error {
	_ = ctx
	return c.transition(LifecycleStateStarted, LifecycleStateStopped, "Stop")
}

func (c *BaseComponent) Status() LifecycleState {
	if c == nil {
		return LifecycleStateNew
	}
	return LifecycleState(c.status.Load())
}

func (c *BaseComponent) transition(expect, next LifecycleState, action string) error {
	if c == nil {
		return fmt.Errorf("%w: action=%s component=nil", ErrInvalidLifecycleTransition, action)
	}
	if c.status.CompareAndSwap(int64(expect), int64(next)) {
		return nil
	}
	return fmt.Errorf(
		"%w: action=%s current=%s expected=%s",
		ErrInvalidLifecycleTransition,
		action,
		c.Status(),
		expect,
	)
}

type LifecycleState int64

const (
	LifecycleStateNew LifecycleState = iota
	LifecycleStateInited
	LifecycleStateStarted
	LifecycleStateStopped
)

func (s LifecycleState) String() string {
	switch s {
	case LifecycleStateNew:
		return "new"
	case LifecycleStateInited:
		return "inited"
	case LifecycleStateStarted:
		return "started"
	case LifecycleStateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}
