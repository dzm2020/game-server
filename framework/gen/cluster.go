package gen

import "game-server/framework/pkg/component"

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

	// Close 关闭集群通信并释放资源。
	// 约定：应可重复调用（幂等）。
	Close()
}
