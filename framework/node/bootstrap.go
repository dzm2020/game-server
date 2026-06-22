package node

import (
	"game-server/framework/actor"
	"game-server/framework/cluster"
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

	compCluster := cluster.NewComponent(node)

	compSystem := actor.NewComponent(node)

	node.AddComponents(compRegistry, compSystem, compCluster)
}
