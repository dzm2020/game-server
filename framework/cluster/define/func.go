package define

import "time"

func NewClusterMessage(from, to string, payload []byte) *ClusterMessage {
	return &ClusterMessage{
		CreatedAt:  time.Now().Unix(),
		FromNodeID: from,
		ToNodeID:   to,
		Payload:    payload,
	}
}

//func ActorPIDToProto(pid actor.PID) *ActorPID {
//	return &ActorPID{
//		ActorId:   pid.ActorID,
//		ActorName: pid.ActorName,
//		NodeId:    pid.NodeID,
//	}
//}
//
//func ProtoPIDToActor(pid *ActorPID) actor.PID {
//	if pid == nil {
//		return actor.NoSender
//	}
//	return actor.PID{
//		ActorID:   pid.ActorId,
//		ActorName: pid.ActorName,
//		NodeID:    pid.NodeId,
//	}
//}
//
//func ClusterMessageToProtocol(m *ActorMessage) *protocol.Message {
//	if m == nil {
//		return nil
//	}
//	return &protocol.Message{
//		Head: &protocol.Head{
//			Cmd:   uint8(m.Cmd),
//			Act:   uint8(m.Act),
//			Error: uint16(m.Error),
//			Index: m.Index,
//		},
//		Data: m.Data,
//	}
//}
//
//func ProtocolToClusterMessage(from, to actor.PID, message *protocol.Message) *ClusterMessage {
//	return &ClusterMessage{
//		TraceId:   fmt.Sprintf("%d", time.Now().UnixNano()),
//		CreatedAt: time.Now().UnixMilli(),
//		Message: &ActorMessage{
//			Cmd:   uint32(message.Cmd),
//			Act:   uint32(message.Act),
//			Index: uint32(message.Index),
//			Error: uint32(message.Error),
//			Data:  message.Data,
//		},
//		SourcePid: ActorPIDToProto(from),
//		TargetPid: ActorPIDToProto(to),
//	}
//}
