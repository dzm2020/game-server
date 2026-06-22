package main

import (
	"game-server/framework/gen"
	"game-server/framework/node"
	"game-server/framework/pkg/glog"
	"game-server/internal/gamesvr"
)

func main() {
	logger := glog.DefaultConfig()
	logger.Level = "debug"
	n := node.New(gen.NodeOptions{
		ID:          "game1",
		Name:        "game",
		ExtAddress:  "",
		RpcAddress:  "127.0.0.1:9001",
		RemoteNames: []string{"chat"},
		Grpc: &gen.GrpcOptions{
			PeerSendChanSize: 1000,
		},
		Logger:   logger,
		Behavior: gamesvr.Behavior{},
	})
	_ = n.Startup()
}
