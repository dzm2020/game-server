package gateway

import (
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/network"
)

func (c *gatWay) bindConnection(conn network.IConnection) error {
	if c.system == nil || conn == nil {
		return nil
	}
	if _, ok := c.getConnActorPID(conn.ID()); ok {
		return nil
	}
	if c.agentFactory == nil {
		return ErrFactoryNotConfigured
	}

	agent, err := c.agentFactory()
	if err != nil {
		return fmt.Errorf("build client agent conn_id=%d: %w", conn.ID(), err)
	}
	if agent == nil {

		return fmt.Errorf("build client agent conn_id=%d: nil handler", conn.ID())
	}
	agent.SetConnection(conn)
	pid, err := c.system.SpawnActor(agent, gen.SpawnOptions{})
	if err != nil {
		return fmt.Errorf("spawn client agent conn_id=%d: %w", conn.ID(), err)
	}
	c.registry.Bind(conn.ID(), pid)
	return nil
}

func (c *gatWay) unbindConnection(connID uint64) {
	pid, ok := c.registry.Unbind(connID)
	if ok {
		_ = c.stopConnActor(pid)
	}
}

func (c *gatWay) getConnActorPID(connID uint64) (*gen.PID, bool) {
	return c.registry.Get(connID)
}

func (c *gatWay) stopConnActor(pid *gen.PID) error {
	if c.system == nil || pid == nil || pid.IsZero() {
		return nil
	}
	c.system.StopProcess(pid)
	return nil
}
