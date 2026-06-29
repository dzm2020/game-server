package main

import (
	grpc_cluster "game-server/framework/cluster/grpc"
	"game-server/framework/gen"
	"game-server/framework/node"
	"game-server/framework/pkg/glog"
	"game-server/internal/chatsvr"
)

func main() {
	logger := glog.DefaultConfig()
	logger.Level = "debug"
	n := node.New(gen.NodeOptions{
		ID:         "chat1",
		Name:       "chat",
		ExtAddress: "",
		RpcAddress: "127.0.0.1:9000",
		Logger:     logger,
		Behavior:   chatsvr.Behavior{},
	})
	if err := n.SetCluster(grpc_cluster.NewWithOptions(n, grpc_cluster.Options{
		Remotes: []string{"game", "gateway"},
		Client: grpc_cluster.ClientOptions{
			SendChanSize: 1000,
		},
	})); err != nil {
		panic(err)
	}
	_ = n.Startup()
}
