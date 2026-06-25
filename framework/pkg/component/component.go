package component

import (
	"context"
)

type IComponent interface {
	Init(ctx context.Context) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type BaseComponent struct {
}

func (*BaseComponent) Init() error { return nil }
func (*BaseComponent) Start(ctx context.Context) error {
	return nil
}
func (*BaseComponent) Stop(ctx context.Context) error {
	return nil
}
