package messagequeue

import (
	"context"
	"game-server/internal/profile"

	"game-server/pkg/component"
	queue "message_queue"
)

type Component struct {
	component.BaseComponent
	queue.IMessageQue
}

func New() *Component {
	return &Component{}
}

func (c *Component) Init() error {
	cfg := profile.GetBase().Nats
	mq, err := queue.NewNATSMessageQueueFromConfig(cfg)
	if err != nil {
		return err
	}
	c.IMessageQue = mq
	return nil
}

func (c *Component) Stop(_ context.Context) error {
	if c.IMessageQue != nil {
		c.IMessageQue.Close()
	}
	return nil
}
