package gen

import (
	"game-server/framework/pkg/component"
)

type ICluster interface {
	component.IComponent
	SendToNode(from, to *PID, msg *Message) error
	// Broadcast
	//
	//	@Description: 广播消息到所有节点
	//	@receiver c
	//	@param to
	//	@param msg
	//	@return error  部分失败仍返回 nil
	Broadcast(to *PID, msg *Message) error
}
