package cluster

import (
	"context"
	"fmt"
	"game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

type LocalInvoker struct {
	node gen.INode
}

func (l *LocalInvoker) Handler(from *gen.PID, target *gen.PID, msg *gen.Message) error {
	system := l.node.GetSystem()
	if system == nil {
		err := fmt.Errorf("system is nil")
		glog.Error("集群消息分发失败", zap.Error(err))
		return err
	}
	if err := system.Tell(from, target, msg); err != nil {
		glog.Error("集群消息分发失败", zap.Error(err))
		return err
	}
	glog.Debug("集群消息分发", zap.Any("msg", msg))
	return nil
}

func NewComponent(node gen.INode) *Component {
	return &Component{
		node: node,
	}
}

type Component struct {
	node gen.INode
	gen.ICluster
}

func (c *Component) Init() error {
	glog.Info("集群组件启动")
	options := c.node.GetOptions()
	c.ICluster = options.Cluster
	if c.ICluster != nil {
		return nil
	}

	grpcOpt := &grpc_cluster.Options{
		NodeID:           options.ID,
		ListenAddr:       options.RpcAddress,
		PeerSendChanSize: options.Grpc.PeerSendChanSize,
		PeerNames:        options.RemoteNames,
	}
	c.ICluster = grpc_cluster.New(grpcOpt)

	c.ICluster.SetLocalInvoker(&LocalInvoker{node: c.node})
	c.ICluster.SetDiscovery(c.node.GetRegistry())
	glog.Info("集群组件启动")
	return nil
}

func (c *Component) Start(ctx context.Context) error {
	glog.Info("集群组件运行")
	return c.ICluster.Run()
}

func (c *Component) Stop(ctx context.Context) error {
	glog.Info("集群组件停止")
	c.ICluster.Close()
	return nil
}
