package main

import (
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
		RemoteNames: []string{
			"chat",
			"game",
		},
		Grpc: gen.GrpcOptions{
			PeerSendChanSize: 1000,
		},
		Logger:   logger,
		Behavior: gatewaysvr.Behavior{},
	})

	_ = n.Startup()
}
