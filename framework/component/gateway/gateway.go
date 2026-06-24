package gateway

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/network"
	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

func newGatWay(options Options, system gen.ISystem) *gatWay {
	c := &gatWay{
		options: options,
		manager: newConnManager(),
		system:  system,
	}
	return c
}

type gatWay struct {
	options Options
	system  gen.ISystem
	server  network.IServer

	manager *manager
}

func (c *gatWay) Init() error {
	server, err := network.NewServer(newEventHandler(c), c.options.ProtoAddr, c.options.NetworkOptions)
	if err != nil {
		glog.Error("网关创建网络服务失败",
			zap.String("proto_addr", c.options.ProtoAddr),
			zap.Error(err))
		return ErrCreateNetworkServer
	}
	c.server = server
	return nil
}

func (c *gatWay) Start(_ context.Context) error {
	if c.server == nil || c.system == nil {
		return ErrComponentNotInited
	}

	if c.options.AgentFactory == nil {
		return ErrAgentSpawnerNil
	}

	if err := c.server.Start(); err != nil {
		return err
	}
	glog.Info("网关组件启动成功", zap.String("listen", c.server.Addr()))
	return nil
}

func (c *gatWay) routeInbound(conn network.IConnection, msg *gen.Message) error {
	if c.system == nil {
		return ErrComponentNotInited
	}

	pid, err := c.ensureClientAgent(conn)
	if err != nil {
		return err
	}
	if err := c.dispatchToClientAgent(pid, msg); err != nil {
		return err
	}
	return nil
}

func (c *gatWay) ensureClientAgent(conn network.IConnection) (*gen.PID, error) {
	pid, ok := c.getConnActorPID(conn.ID())
	if ok {
		return pid, nil
	}

	if err := c.bindConnection(conn); err != nil {
		glog.Error("网关绑定连接失败", zap.Error(err))
		return gen.NoSender, err
	}
	pid, ok = c.getConnActorPID(conn.ID())
	if !ok {
		glog.Error("网关获取连接Actor失败", zap.Int64("client_id", conn.ID()))
		return gen.NoSender, ErrClientAgentNotFound
	}
	return pid, nil
}

func (c *gatWay) dispatchToClientAgent(pid *gen.PID, msg *gen.Message) error {
	return c.system.SendEnvelope(pid, gen.ActorEnvelope{
		Sender:  gen.NoSender,
		Payload: msg,
	})
}

func (c *gatWay) bindConnection(conn network.IConnection) error {
	if c.system == nil || conn == nil {
		return nil
	}
	if _, ok := c.getConnActorPID(conn.ID()); ok {
		return nil
	}
	agent, spawnerOptions := c.options.AgentFactory()
	agent.SetConnection(conn)

	pid, err := c.system.SpawnActor(agent, spawnerOptions)
	if err != nil {
		glog.Error("网关启动客户端Actor失败",
			zap.Int64("conn_id", conn.ID()),
			zap.Error(err))
		return ErrSpawnClientAgent
	}
	c.manager.Bind(conn.ID(), pid)
	return nil
}

func (c *gatWay) unbindConnection(connID int64) {
	pid, ok := c.manager.Unbind(connID)
	if ok {
		_ = c.stopConnActor(pid)
	}
}

func (c *gatWay) getConnActorPID(connID int64) (*gen.PID, bool) {
	return c.manager.Get(connID)
}

func (c *gatWay) stopConnActor(pid *gen.PID) error {
	if c.system == nil || pid == nil || pid.IsZero() {
		return nil
	}
	c.system.StopProcess(pid)
	return nil
}

func (c *gatWay) Stop(ctx context.Context) error {
	if c.server != nil {
		c.server.Shutdown(ctx)
	}
	current := c.manager.Reset()
	for _, pid := range current {
		_ = c.stopConnActor(pid)
	}
	return nil
}
