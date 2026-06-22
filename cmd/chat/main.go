package main

import (
	"game-server/framework/gen"
	"game-server/framework/node"
	"game-server/framework/pkg/glog"
	"game-server/internal/chatsvr"
)

func main() {
	logger := glog.DefaultConfig()
	logger.Level = "debug"
	n := node.New(gen.NodeOptions{
		ID:          "chat1",
		Name:        "chat",
		ExtAddress:  "",
		RpcAddress:  "127.0.0.1:9000",
		RemoteNames: []string{"game"},
		Grpc: gen.GrpcOptions{
			PeerSendChanSize: 1000,
		},
		Logger:   logger,
		Behavior: chatsvr.Behavior{},
	})
	_ = n.Startup()
}
