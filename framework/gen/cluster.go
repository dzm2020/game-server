package gen

import (
	"game-server/framework/pkg/component"
)

type IRemoteInvoker interface {
	SendToNode(from, to *PID, msg *Message) error
	Broadcast(to *PID, msg *Message) error
}

type ICluster interface {
	component.IComponent
	IRemoteInvoker
	SetLocalInvoker(invoker ILocalInvoker)
	SetDiscovery(discover IDiscovery)
}
