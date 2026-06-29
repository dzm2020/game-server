package main

import (
	grpc_cluster "game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/node"
	"game-server/framework/pkg/glog"
	"game-server/internal/gamesvr"
)

func main() {
	logger := glog.DefaultConfig()
	logger.Level = "debug"
	n := node.New(gen.NodeOptions{
		ID:         "game1",
		Name:       "game",
		ExtAddress: "",
		RpcAddress: "127.0.0.1:9001",
		Logger:     logger,
		Behavior:   gamesvr.Behavior{},
	})
	if err := n.SetCluster(grpc_cluster.NewWithOptions(n, grpc_cluster.Options{
		Remotes: []string{"chat"},
		Client: grpc_cluster.ClientOptions{
			SendChanSize: 1000,
		},
	})); err != nil {
		panic(err)
	}
	_ = n.Startup()
}
