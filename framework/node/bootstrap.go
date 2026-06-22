package node

import (
	"game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry"

	"go.uber.org/zap"
)

func bootstrap(node *Node, options *gen.NodeOptions) {

	options = gen.EnsureNodeOptions(options)

	glog.Init(options.Logger, zap.Fields(
		zap.String("nodeId", options.ID),
		zap.String("nodeName", options.Name),
	))

	compRegistry := registry.NewComponent(node)

	grpcOpt := &grpc_cluster.Options{
		NodeID:           options.ID,
		ListenAddr:       options.RpcAddress,
		PeerSendChanSize: options.Grpc.PeerSendChanSize,
		PeerNames:        options.RemoteNames,
	}
	compCluster := cluster.NewComponent(node, grpc_cluster.New(grpcOpt))

	compSystem := actor.NewComponent(node)

	node.AddComponents(compRegistry, compSystem, compCluster)
}
