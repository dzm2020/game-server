package main

import (
	grpc_cluster "game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/node"
	"game-server/framework/pkg/glog"
	"game-server/internal/gatewaysvr"
)

func main() {
	logger := glog.DefaultConfig()
	logger.Level = "info"

	n := node.New(gen.NodeOptions{
		ID:         "gateway1",
		Name:       "gateway",
		ExtAddress: "127.0.0.1:7000",
		RpcAddress: "127.0.0.1:9002",
		Logger:     logger,
		Behavior:   gatewaysvr.Behavior{},
	})
	if err := n.SetCluster(grpc_cluster.NewWithOptions(grpc_cluster.Options{
		Remotes: []string{
			"chat",
			"game",
		},
		Client: grpc_cluster.ClientOptions{
			SendChanSize: 1000,
		},
	})); err != nil {
		panic(err)
	}

	_ = n.Startup()
}
