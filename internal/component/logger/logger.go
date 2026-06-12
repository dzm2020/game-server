package logger

import (
	"context"
	"game-server/internal/profile"
	"game-server/pkg/component"
	"game-server/pkg/glog"
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
