package gateway

import (
	"context"
	"fmt"
	"game-server/framework/actor"
	"game-server/framework/config"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/network"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"net"
	"strconv"
	"time"
)

type ClientAgentFactory func() (IAgent, error)

func newGatWay(cfg *config.GatewayConfig, system actor.ISystem) *gatWay {
	c := &gatWay{
		cfg:      cfg,
		registry: newConnRegistry(),
		system:   system,
	}
	return c
}

type gatWay struct {
	component.BaseComponent

	cfg    *config.GatewayConfig
	server *network.WebsocketServer
	system actor.ISystem

	runErrCh chan error

	registry     *connRegistry
	agentFactory ClientAgentFactory
}

func (c *gatWay) Init() error {
	addr := net.JoinHostPort(c.cfg.Address, strconv.Itoa(c.cfg.Port))
	c.server = network.NewWebsocketServer(
		addr,
		network.WithCodec(newWSProtocolCodec()),
		network.WithEventHandler(newEventHandler(c)),
		network.WithReadLimit(c.cfg.ReadLimit),
		network.WithPongWait(time.Duration(c.cfg.PongWaitSec)*time.Second),
		network.WithPingPeriod(time.Duration(c.cfg.PingPeriodSec)*time.Second),
		network.WithWriteWait(time.Duration(c.cfg.WriteWaitSec)*time.Second),
		network.WithSendBuffer(c.cfg.SendBuffer),
	)
	return nil
}

func (c *gatWay) Start(_ context.Context) error {

	if c.server == nil || c.system == nil {
		return ErrComponentNotInited
	}
	if c.agentFactory == nil {
		return ErrFactoryNotConfigured
	}

	c.runErrCh = make(chan error, 1)
	grs.SafeGo(func() {
		c.runErrCh <- c.server.Start()
		close(c.runErrCh)
	})

	if err := c.waitServerReady(2 * time.Second); err != nil {
		return err
	}

	glog.Infof("gateway component started, listen=%s", c.server.Addr)
	return nil
}

func (c *gatWay) SetClientAgentFactory(factory ClientAgentFactory) {
	c.agentFactory = factory
}

func (c *gatWay) Stop(ctx context.Context) error {
	if err := c.server.ShutdownContext(ctx); err != nil {
		return err
	}
	if c.runErrCh != nil {
		select {
		case <-c.runErrCh:
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
	current := c.registry.Reset()
	for _, pid := range current {
		_ = c.stopConnActor(pid)
	}
	return nil
}

func (c *gatWay) waitServerReady(timeout time.Duration) error {
	if c.server == nil {
		return ErrServerNil
	}

	deadline := time.Now().Add(timeout)
	for {
		select {
		case runErr := <-c.runErrCh:
			if runErr != nil {
				return runErr
			}
			return ErrServerExitedEarly
		case <-c.server.Ready():
			return nil
		default:
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("gateway startup timeout, listen=%s", c.server.Addr)
		}
		time.Sleep(20 * time.Millisecond)
	}
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

func (c *gatWay) ensureClientAgent(conn network.IConnection) (actor.PID, error) {
	pid, ok := c.getConnActorPID(conn.ID())
	if ok {
		return pid, nil
	}

	if err := c.bindConnection(conn); err != nil {
		return actor.NoSender, err
	}
	pid, ok = c.getConnActorPID(conn.ID())
	if !ok {
		return actor.NoSender, ErrClientAgentNotFound
	}
	return pid, nil
}

func (c *gatWay) dispatchToClientAgent(pid actor.PID, msg *gen.Message) error {
	return c.system.SendEnvelope(pid, actor.Envelope{
		Sender:  actor.NoSender,
		Payload: msg,
	})
}
