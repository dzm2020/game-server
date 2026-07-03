package component

import (
	"context"
)

// IComponent 组件提供的生命周期函数要保证幂等性和顺序执行
type IComponent interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Status() LifecycleState
}

type BaseComponent[T IComponent] struct {
	lifecycle Controller
}

func (c *BaseComponent) Init(ctx context.Context) error {
	return c.lifecycle.Init(ctx, nil)
}

func (c *BaseComponent) Start(ctx context.Context) error {
	return c.lifecycle.Start(ctx, nil)
}

func (c *BaseComponent) Stop(ctx context.Context) error {
	return c.lifecycle.Stop(ctx, nil)
}

func (c *BaseComponent) GuardInit(ctx context.Context, run func(context.Context) error) error {
	return c.lifecycle.Init(ctx, run)
}

func (c *BaseComponent) GuardStart(ctx context.Context, run func(context.Context) error) error {
	return c.lifecycle.Start(ctx, run)
}

func (c *BaseComponent) GuardStop(ctx context.Context, run func(context.Context) error) error {
	return c.lifecycle.Stop(ctx, run)
}

func (c *BaseComponent) Status() LifecycleState {
	if c == nil {
		return LifecycleStateNew
	}
	return c.lifecycle.State()
}
