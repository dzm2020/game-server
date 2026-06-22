package registry

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry/consul"

	"go.uber.org/zap"
)

func NewComponent(node gen.INode) *Component {
	return &Component{
		node: node,
	}
}

type Component struct {
	node gen.INode
	gen.IRegistry
}

func (c *Component) Init() error {
	glog.Info("注册发现组件初始化")
	options := c.node.GetOptions()
	c.IRegistry = options.Registry
	if c.IRegistry != nil {
		return nil
	}

	reg, err := consul.New(options.Consul)
	if err != nil {
		glog.Error("consul初始化失败", zap.Error(err))
		return err
	}
	c.IRegistry = reg

	return nil
}

func (c *Component) Start(ctx context.Context) error {
	glog.Info("注册发现组件运行")
	reg := gen.ServiceInstance{
		ID:         c.node.GetId(),
		Name:       c.node.GetName(),
		ExtAddress: c.node.GetExtAddress(),
		RpcAddress: c.node.GetRpcAddress(),
	}
	if err := c.Register(reg); err != nil {
		glog.Error("注册服务失败", zap.Error(err))
		return err
	}

	glog.Info("注册服务", zap.Any("reg", reg))
	return c.IRegistry.Run(ctx)
}

func (c *Component) Stop(ctx context.Context) error {
	if c.IRegistry == nil {
		return nil
	}
	if err := c.IRegistry.Deregister(c.node.GetId()); err != nil {
		glog.Error("注销服务失败", zap.Error(err))
	}
	glog.Info("注销服务", zap.String("node", c.node.GetId()))
	c.IRegistry.Shutdown()
	return nil
}
