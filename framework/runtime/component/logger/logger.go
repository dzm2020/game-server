package logger

import (
	"context"
	"game-server/framework/runtime/profile"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
)

type Component struct {
	component.BaseComponent
}

func New() *Component {
	return &Component{}
}

func (c *Component) Init() error {
	cfg := profile.GetBase().Logger
	glog.Init(&cfg)
	return nil
}

func (c *Component) Stop(_ context.Context) error {
	return glog.Stop()
}
