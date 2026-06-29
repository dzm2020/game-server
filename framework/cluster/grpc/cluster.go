package grpc

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"

	"time"

	"github.com/duke-git/lancet/v2/convertor"
	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

const clusterComponent = "cluster.grpc"

var _ gen.ICluster = (*Cluster)(nil)

func New(node gen.INode) *Cluster {
	return NewWithOptions(node, Options{})
}

func NewWithOptions(node gen.INode, options Options) *Cluster {
	c := &Cluster{
		node:      node,
		options:   options,
		clients:   maputil.NewConcurrentMap[string, *client](10),
		remoteSet: make(map[string]struct{}),
	}
	return c
}

// Cluster 集群管理器
type Cluster struct {
	component.BaseComponent
	node      gen.INode
	options   Options
	remoteSet map[string]struct{}
	clients   *maputil.ConcurrentMap[string, *client]
	server    *server
	bgGroup   *grs.Group
	logger    *zap.Logger
}

func (c *Cluster) validateDependencies() error {
	if c.node == nil {
		return gen.ErrNodeIsNil
	}
	if c.node.GetSystem() == nil {
		return gen.ErrActorSystemNil
	}
	if c.node.GetRegistry() == nil {
		return gen.ErrRegistryNil
	}
	return nil
}

func (c *Cluster) Init(ctx context.Context) error {
	return c.BaseComponent.GuardInit(ctx, func(ctx context.Context) error {
		if err := c.validateDependencies(); err != nil {
			return err
		}
		c.logger = glog.GetLogger().With(gen.FieldComponent(clusterComponent), gen.FieldNodeID(c.selfNodeID()))

		c.options = NormalizeOptions(c.options)
		if err := ValidateOptions(c.options); err != nil {
			return fmt.Errorf("invalid grpc options: %w", err)
		}

		c.remoteSet = make(map[string]struct{}, len(c.options.Remotes))
		for _, name := range c.options.Remotes {
			if name == "" {
				continue
			}
			c.remoteSet[name] = struct{}{}
		}
		c.server = newServer(c.node.GetRpcAddress(), c)
		c.logger.Info("初始化成功",
			zap.Strings("remotes", c.options.Remotes),
			zap.Int("sendChanSize", c.options.Client.SendChanSize),
		)
		return nil
	})
}

func (c *Cluster) Start(ctx context.Context) error {
	return c.BaseComponent.GuardStart(ctx, func(ctx context.Context) error {
		c.bgGroup = grs.NewGroup(ctx)
		if err := c.server.run(); err != nil {
			c.bgGroup = nil
			return err
		}

		c.connectAll()

		c.bgGroup.Go(func(ctx context.Context) {
			c.connectLoop(ctx)
		})
		c.logger.Info("启动完成")
		return nil
	})
}

func (c *Cluster) addClient(client *client) {
	if client == nil {
		return
	}
	c.clients.Set(client.nodeID, client)
}
func (c *Cluster) removeClient(client *client) {
	if client == nil {
		return
	}
	c.clients.Delete(client.nodeID)
}
func (c *Cluster) forEachClient(fn func(client *client) bool) {
	c.clients.Range(func(key string, value *client) bool {
		return fn(value)
	})
}
func (c *Cluster) getClientByNodeID(nodeID string) *client {
	client, _ := c.clients.Get(nodeID)
	return client
}

func (c *Cluster) shouldConnectService(serviceName string) bool {
	_, ok := c.remoteSet[serviceName]
	return ok
}

func (c *Cluster) selfNodeID() string {
	if c.node == nil {
		return ""
	}
	return c.node.GetId()
}

func (c *Cluster) connectLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.connectAll()
		case <-ctx.Done():
			return
		}
	}
}

func (c *Cluster) connectAll() {
	if c.node == nil {
		return
	}
	discovery := c.node.GetRegistry()
	if discovery == nil {
		return
	}
	serviceInstances := discovery.DiscoverAll()
	for serviceName, instances := range serviceInstances {
		if !c.shouldConnectService(serviceName) {
			continue
		}
		for _, instance := range instances {
			c.reconcilePeer(instance)
		}
	}
}

func (c *Cluster) reconcilePeer(instance gen.ServiceInstance) {
	nodeID := instance.GetID()
	if nodeID == "" || nodeID == c.selfNodeID() {
		return
	}
	if c.getClientByNodeID(nodeID) != nil {
		return
	}
	cli := newClient(clientConfig{
		nodeID:   nodeID,
		address:  instance.GetRpcAddress(),
		sendSize: c.options.Client.SendChanSize,
		cluster:  c,
	})
	c.addClient(cli)
	cli.start()
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	if from == nil || to == nil {
		return gen.ErrActorPidNil
	}
	if msg == nil {
		return gen.ErrMessageNil
	}
	cli, err := c.resolveClientForTarget(to)
	if err != nil {
		return err
	}
	clusterMsg, err := newClusterMessage(from, to, msg)
	if err != nil {
		return err
	}
	if err = cli.send(clusterMsg); err != nil {
		return err
	}
	c.logger.Debug("客户端发送消息",
		zap.String("from", from.String()),
		zap.String("to", to.String()),
		zap.Uint16("msgID", msg.ID()),
	)
	return nil
}

func (c *Cluster) resolveClientForTarget(to *gen.PID) (*client, error) {
	if to == nil {
		return nil, gen.ErrActorPidNil
	}
	if c.node == nil {
		return nil, gen.ErrNodeIsNil
	}
	discovery := c.node.GetRegistry()
	if discovery == nil {
		return nil, gen.ErrRegistryNil
	}
	nodeID := to.GetNodeID()
	if nodeID == "" {
		return nil, gen.ErrClusterNodeNotFound
	}
	cli := c.getClientByNodeID(nodeID)
	if cli == nil {
		return nil, gen.ErrClusterNodeNotFound
	}
	if _, ok := discovery.GetInstance(nodeID); !ok {
		return nil, gen.ErrClusterNodeNotInServiceList
	}
	if !cli.IsConnected() {
		return nil, gen.ErrClusterNodeNotConnected
	}
	return cli, nil
}

// Broadcast
//
//	@Description: 广播消息到所有节点
//	@receiver c
//	@param to
//	@param msg
//	@return error  部分失败仍返回 nil
func (c *Cluster) Broadcast(to *gen.PID, msg *gen.Message) error {
	if to == nil {
		c.logger.Error("集群广播", gen.FieldErr(gen.ErrActorPidNil))
		return gen.ErrActorPidNil
	}
	if msg == nil {
		c.logger.Error("集群广播", gen.FieldErr(gen.ErrMessageNil))
		return gen.ErrMessageNil
	}

	var success int
	c.forEachClient(func(client *client) bool {
		target := convertor.DeepClone(to)
		target.NodeID = client.nodeID
		clusterMsg, err := newClusterMessage(gen.NoSender, target, msg)
		if err != nil {
			c.logger.Warn("集群广播", zap.String("to", to.String()), gen.FieldErr(err))
			return true
		}
		if err := client.send(clusterMsg); err != nil {
			c.logger.Warn("集群广播", zap.String("to", to.String()), gen.FieldErr(err))
			return true
		}
		success++
		return true
	})

	c.logger.Debug("集群广播",
		zap.String("to", to.String()),
		zap.Any("msg", msg),
		zap.Int("success", success),
	)
	return nil
}

func (c *Cluster) Dispatch(msg *gen.ClusterMessage) error {
	if msg == nil {
		return gen.ErrMessageNil
	}
	if msg.TargetPid == nil || msg.SourcePid == nil {
		return gen.ErrActorPidNil
	}
	if c.node == nil {
		return gen.ErrNodeIsNil
	}
	system := c.node.GetSystem()
	if system == nil {
		return gen.ErrActorSystemNil
	}
	decodedMsg, _, err := gen.Decode(msg.Data)
	if err != nil {
		return err
	}
	if decodedMsg == nil {
		err = gen.ErrClusterDecodeFailed
		return err
	}
	to := msg.TargetPid
	if err = system.Tell(msg.SourcePid, to, decodedMsg); err != nil {
		return err
	}
	c.logger.Debug("分发消息",
		zap.String("from", msg.SourcePid.String()),
		zap.String("to", msg.TargetPid.String()),
	)
	return nil
}

func (c *Cluster) closeAllClients() {
	var clients []*client
	c.forEachClient(func(cli *client) bool {
		if cli == nil {
			return true
		}
		clients = append(clients, cli)
		return true
	})
	for _, cli := range clients {
		cli.Close()
	}
}

// Stop 关闭集群连接
func (c *Cluster) Stop(ctx context.Context) error {
	return c.BaseComponent.GuardStop(ctx, func(ctx context.Context) error {
		if ctx == nil {
			ctx = context.Background()
		}
		if c.server != nil {
			c.server.shutdown()
		}
		c.closeAllClients()
		c.bgGroup.Cancel()
		if err := c.bgGroup.Wait(ctx); err != nil {
			c.logger.Warn("等待集群后台协程退出超时", gen.FieldErr(err))
			return err
		}
		c.logger.Info("集群已关闭")
		return nil
	})

}
