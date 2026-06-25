package gen

import (
	"game-server/framework/pkg/component"
)

type ICluster interface {
	component.IComponent
	SendToNode(from, to *PID, msg *Message) error
	Broadcast(to *PID, msg *Message)
}
