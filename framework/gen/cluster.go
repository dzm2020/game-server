package gen

import "game-server/framework/pkg/component"

type IClusterComp interface {
	component.IComponent
	ICluster
}

type ICluster interface {
	Run() error
	SetLocalInvoker(invoker ILocalInvoker)
	SetDiscovery(discovery IDiscovery)
	SendToNode(from, to *PID, msg *Message) error
	Broadcast(to *PID, msg *Message)
	Close()
}
