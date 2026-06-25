package gen

import (
	"context"
	"game-server/framework/pkg/component"
)

type IClusterComp interface {
	component.IComponent
	ICluster
}

type ICluster interface {
	// Run 启动集群通信组件。
	// 约定：应快速返回（非长期阻塞）；重复调用应尽量幂等。
	Run() error
	SetLocalInvoker(invoker ILocalInvoker)
	SetDiscovery(discovery IDiscovery)
	SendToNode(from, to *PID, msg *Message) error
	Broadcast(to *PID, msg *Message)
	Stop(ctx context.Context) error
}
