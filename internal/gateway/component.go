package gateway

import (
	"context"
	config2 "game-server/framework/config"
)

func NewComponent() *Component {
	return &Component{}
}

type Component struct {
	cfg *config2.GatewayConfig
	*gatWay
}

func (c *Component) Init() error {
	cfg := config2.GetGatWay()
	if cfg == nil {
		return ErrConfigNil
	}

	if !c.cfg.Enable {
		return nil
	}
	//  todo
	c.gatWay = newGatWay(cfg, nil)
	return nil
}
func (c *Component) Start(ctx context.Context) error {
	if !c.cfg.Enable {
		return nil
	}

	return c.gatWay.Start(ctx)
}
func (c *Component) Stop(ctx context.Context) error {
	if !c.cfg.Enable {
		return nil
	}
	return c.gatWay.Stop(ctx)
}
