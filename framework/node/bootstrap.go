package node

import (
	"game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry"
	"game-server/framework/registry/consul"

	"go.uber.org/zap"
)

func bootstrap(node *Node, options *gen.NodeOptions) {

	options = gen.EnsureNodeOptions(options)

	glog.Init(options.Logger)

	reg, err := consul.New(options.Consul)
	if err != nil {
		glog.Error("consul init fail", zap.Error(err))
		return
	}
	compRegistry := registry.NewComponent(node, reg)

	compSystem := actor.NewComponent(node)

	grpcOpt := &grpc_cluster.Options{
		NodeID:           options.ID,
		ListenAddr:       options.RpcAddress,
		PeerSendChanSize: options.Grpc.PeerSendChanSize,
		PeerNames:        options.RemoteNames,
	}
	compCluster := cluster.NewComponent(node, grpc_cluster.New(grpcOpt))

	node.AddComponents(compRegistry, compSystem, compCluster)
}
