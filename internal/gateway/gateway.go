package gateway

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/network"

	"game-server/framework/pkg/glog"

	"go.uber.org/zap"
)

type ClientAgentFactory func() (IAgent, error)

func newGatWay(cfg *Options, system gen.ISystem) *gatWay {
	c := &gatWay{
		cfg:      cfg,
		registry: newConnRegistry(),
		system:   system,
	}
	return c
}

type gatWay struct {
	system gen.ISystem
	cfg    *Options
	server network.IServer

	registry     *connRegistry
	agentFactory ClientAgentFactory
}

func (c *gatWay) Init() error {
	if c.cfg == nil {
		return ErrConfigNil
	}
	protoAddr := c.cfg.Network + "://" + c.cfg.Address
	if c.cfg.Network == "ws" || c.cfg.Network == "wss" {
		protoAddr = c.cfg.Network + "://" + c.cfg.Address + c.cfg.WsPath
	}
	server, err := network.NewServer(newEventHandler(c), protoAddr, network.ServerOptions{
		SendChanSize: c.cfg.SendBuffer,
	})
	if err != nil {
		return fmt.Errorf("create gateway network server failed: %w", err)
	}
	c.server = server
	return nil
}

func (c *gatWay) Start(_ context.Context) error {

	if c.server == nil || c.system == nil {
		return ErrComponentNotInited
	}
	if c.agentFactory == nil {
		return ErrFactoryNotConfigured
	}
	if err := c.server.Start(); err != nil {
		return err
	}
	glog.Info("gateway component started", zap.String("listen", c.server.Addr()))
	return nil
}

func (c *gatWay) SetClientAgentFactory(factory ClientAgentFactory) {
	c.agentFactory = factory
}

func (c *gatWay) Stop(ctx context.Context) error {
	if c.server != nil {
		c.server.Shutdown(ctx)
	}
	current := c.registry.Reset()
	for _, pid := range current {
		_ = c.stopConnActor(pid)
	}
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
		return gen.NoSender, err
	}
	pid, ok = c.getConnActorPID(conn.ID())
	if !ok {
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
