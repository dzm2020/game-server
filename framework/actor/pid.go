package actor

import "fmt"

// PID 是 actor 的完整进程标识。
// ActorID 在节点内唯一，ActorName 为唯一名字（可为空），NodeID 为节点标识。
type PID struct {
	ActorID   uint64
	ActorName string
	NodeID    string
}

var NoSender = PID{}

func NewPID(actorID uint64, actorName, nodeID string) PID {
	return PID{
		ActorID:   actorID,
		ActorName: actorName,
		NodeID:    nodeID,
	}
}

func PIDFromActorID(actorID uint64) PID {
	return PID{ActorID: actorID}
}

func (p PID) IsZero() bool {
	return p.ActorID == 0 && p.ActorName == "" && p.NodeID == ""
}

func (p PID) String() string {
	return fmt.Sprintf("PID{actor_id=%d, actor_name=%q, node_id=%q}", p.ActorID, p.ActorName, p.NodeID)
}
