package gateway

import (
	"actor"
	"context"
	"errors"
	"fmt"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/network"
	compcluster "game-server/framework/runtime/component/cluster"
	compsystem "game-server/framework/runtime/component/system"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/profile"
	"game-server/framework/runtime/protocol"
	internalroute "game-server/framework/runtime/route"
	"net"
	"strconv"
	"sync"
	"time"
)

type clusterRouter interface {
	SendToPID(sourcePID actor.PID, targetPID actor.PID, msg *protocol.Message) error
	RequestToPID(sourcePID actor.PID, targetPID actor.PID, msg *protocol.Message, timeout time.Duration) ([]byte, error)
}

type ClientAgentFactory func(
	conn network.IConnection,
) (actor.Actor, error)

type Component struct {
	component.BaseComponent

	cfg      Config
	server   *network.WebsocketServer
	system   actor.ISystem
	cluster  clusterRouter
	runErrCh chan error

	connMu     sync.RWMutex
	connActors map[uint64]actor.PID

	handlerRegistry *internalroute.Registry
	agentFactory    ClientAgentFactory
}

func New() *Component {
	return &Component{
		cfg:             defaultConfig(),
		handlerRegistry: internalroute.NewRegistry(),
	}
}

func (c *Component) Init() error {
	base := profile.GetBase()
	if base == nil {
		return errors.New("profile base config is nil")
	}

	c.cfg = normalizeConfig(base.Gateway, base.Self)
	if !c.cfg.Enable {
		return nil
	}
	if c.cfg.Path != "/" {
		glog.Warnf("gateway path %q is not supported by current network server, fallback to \"/\"", c.cfg.Path)
		c.cfg.Path = "/"
	}

	systemComp := iface.GetComponent[*compsystem.Component]()
	if systemComp == nil || systemComp.ISystem == nil {
		return errors.New("gateway depends on system component")
	}
	c.system = systemComp.ISystem

	if clusterComp := iface.GetComponent[*compcluster.Component](); clusterComp != nil && clusterComp.Cluster != nil {
		c.cluster = clusterComp.Cluster
	}

	addr := net.JoinHostPort(c.cfg.Address, strconv.Itoa(c.cfg.Port))
	c.server = network.NewWebsocketServer(
		addr,
		network.WithCodec(newWSProtocolCodec()),
		network.WithEventHandler(newEventHandler(c)),
		network.WithReadLimit(c.cfg.ReadLimit),
		network.WithPongWait(c.cfg.PongWait),
		network.WithPingPeriod(c.cfg.PingPeriod),
		network.WithWriteWait(c.cfg.WriteWait),
		network.WithSendBuffer(c.cfg.SendBuffer),
		network.WithLogger(glog.Logger{}),
	)
	c.connActors = make(map[uint64]actor.PID)
	return nil
}

func (c *Component) Start(_ context.Context) error {
	if !c.cfg.Enable {
		return nil
	}
	if c.server == nil || c.system == nil {
		return errors.New("gateway component is not initialized")
	}
	if c.agentFactory == nil {
		return errors.New("gateway client agent factory is not configured")
	}

	c.runErrCh = make(chan error, 1)

	go func() {
		c.runErrCh <- c.server.Start()
		close(c.runErrCh)
	}()

	if err := c.waitServerReady(2 * time.Second); err != nil {
		return err
	}

	glog.Infof("gateway component started, listen=%s", c.server.Addr)
	return nil
}

func (c *Component) Stop(ctx context.Context) error {
	if !c.cfg.Enable || c.server == nil {
		return nil
	}
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
	c.connMu.Lock()
	current := c.connActors
	c.connActors = make(map[uint64]actor.PID)
	c.connMu.Unlock()
	for _, pid := range current {
		_ = c.stopConnActor(pid)
	}
	return nil
}

func (c *Component) RegisterAsyncHandler(cmd, act uint8, handler internalroute.AsyncHandler) error {
	if handler == nil {
		return errors.New("async handler is nil")
	}
	c.handlerRegistry.RegisterAsync(cmd, act, handler)
	return nil
}

func (c *Component) RegisterSyncHandler(cmd, act uint8, handler internalroute.SyncHandler) error {
	if handler == nil {
		return errors.New("sync handler is nil")
	}
	c.handlerRegistry.RegisterSync(cmd, act, handler)
	return nil
}

func (c *Component) SetClientAgentFactory(factory ClientAgentFactory) {
	c.agentFactory = factory
}

func (c *Component) routeInbound(conn network.IConnection, msg *protocol.Message) error {
	if c.system == nil || conn == nil || msg == nil || msg.Head == nil {
		return nil
	}

	pid, ok := c.getConnActorPID(conn.ID())
	if !ok {
		if err := c.bindConnection(conn); err != nil {
			return err
		}
		pid, ok = c.getConnActorPID(conn.ID())
		if !ok {
			return errors.New("gateway client agent not found")
		}
	}

	return c.system.SendEnvelope(pid, actor.Envelope{
		Sender:  actor.NoSender,
		Payload: msg,
	})
}

func (c *Component) bindConnection(conn network.IConnection) error {
	if c.system == nil || conn == nil {
		return nil
	}
	if _, ok := c.getConnActorPID(conn.ID()); ok {
		return nil
	}
	if c.agentFactory == nil {
		return errors.New("gateway client agent factory is not configured")
	}

	handler, err := c.agentFactory(conn)
	if err != nil {
		return fmt.Errorf("build client agent conn_id=%d: %w", conn.ID(), err)
	}
	if handler == nil {
		return fmt.Errorf("build client agent conn_id=%d: nil handler", conn.ID())
	}
	pid, err := c.system.SpawnActor(handler)
	if err != nil {
		return fmt.Errorf("spawn client agent conn_id=%d: %w", conn.ID(), err)
	}
	c.connMu.Lock()
	c.connActors[conn.ID()] = pid
	c.connMu.Unlock()
	return nil
}

func (c *Component) unbindConnection(connID uint64) {
	var pid actor.PID
	var ok bool

	c.connMu.Lock()
	pid, ok = c.connActors[connID]
	delete(c.connActors, connID)
	c.connMu.Unlock()
	if ok {
		_ = c.stopConnActor(pid)
	}
}

func (c *Component) getConnActorPID(connID uint64) (actor.PID, bool) {
	c.connMu.RLock()
	pid, ok := c.connActors[connID]
	c.connMu.RUnlock()
	return pid, ok
}

func (c *Component) stopConnActor(pid actor.PID) error {
	if c.system == nil || pid.IsZero() {
		return nil
	}
	return c.system.Tell(actor.NoSender, pid, actor.PoisonPill)
}

func (c *Component) waitServerReady(timeout time.Duration) error {
	if c.server == nil {
		return errors.New("gateway websocket server is nil")
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		select {
		case runErr := <-c.runErrCh:
			if runErr != nil {
				return runErr
			}
			return errors.New("gateway server exited before ready")
		default:
		}

		conn, err := net.DialTimeout("tcp", c.server.Addr, 150*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err

		if time.Now().After(deadline) {
			if lastErr == nil {
				lastErr = errors.New("unknown startup failure")
			}
			return fmt.Errorf("gateway startup timeout, listen=%s: %w", c.server.Addr, lastErr)
		}

		<-ticker.C
	}
}
