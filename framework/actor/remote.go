package actor

import (
	"game-server/framework/gen"
	"time"
)

type remoteInvoker struct {
	node gen.INode
}

func (r *remoteInvoker) Tell(from *gen.PID, target *gen.PID, msg *gen.Message) error {
	cluster := r.node.GetCluster()
	if cluster == nil {
		return gen.ErrActorClusterNil
	}
	return cluster.SendToNode(from, target, msg)
}
func (r *remoteInvoker) Ask(from *gen.PID, target any, msg *gen.Message, timeout time.Duration) ([]byte, error) {
	return nil, gen.ErrActorClusterAskNotImpl
}
