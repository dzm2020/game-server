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
	Start(ctx context.Context) error
	SetLocalInvoker(invoker ILocalInvoker)
	SetDiscovery(discovery IDiscovery)
	SendToNode(from, to *PID, msg *Message) error
	Broadcast(to *PID, msg *Message)
	Stop(ctx context.Context) error
}
