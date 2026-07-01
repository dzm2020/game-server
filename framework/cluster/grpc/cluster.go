package grpc

import (
	"context"
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

func New() *Cluster {
	return NewWithOptions(Options{})
}

func NewWithOptions(options Options) *Cluster {
	c := &Cluster{
		options: options,
		clients: maputil.NewConcurrentMap[string, *client](10),
	}
	return c
}

var _ gen.ICluster = (*Cluster)(nil)

// Cluster 集群管理器
type Cluster struct {
	component.BaseComponent
	options      Options
	localInvoker gen.ILocalInvoker
	discover     gen.IDiscovery
	clients      *maputil.ConcurrentMap[string, *client]
	server       *server
	logger       *zap.Logger
	ctx          context.Context
	cancel       context.CancelFunc
}

func (c *Cluster) SetLocalInvoker(invoker gen.ILocalInvoker) {
	c.localInvoker = invoker
}

func (c *Cluster) SetDiscovery(discover gen.IDiscovery) {
	c.discover = discover
}

func (c *Cluster) getID() string {
	return c.options.ID
}

func (c *Cluster) Init(ctx context.Context) error {
	return c.BaseComponent.GuardInit(ctx, func(ctx context.Context) error {

		c.logger = glog.GetLogger().With(gen.FieldComponent(clusterComponent))

		c.options = NormalizeOptions(c.options)
		if err := ValidateOptions(c.options); err != nil {
			return err
		}

		c.server = newServer(c.options.ListenAddr, c.localInvoker)

		c.logger.Info("初始化成功",
			zap.Strings("remotes", c.options.Remotes),
			zap.Int("sendChanSize", c.options.Client.SendChanSize),
		)
		return nil
	})
}

func (c *Cluster) Start(ctx context.Context) error {
	return c.BaseComponent.GuardStart(ctx, func(ctx context.Context) error {
		c.ctx, c.cancel = context.WithCancel(ctx)
		if err := c.server.run(); err != nil {
			return err
		}
		grs.SafeGo(func() {
			c.connectLoop()
		})
		c.logger.Info("启动完成")
		return nil
	})
}

func (c *Cluster) connectLoop() {
	c.connectAll()
	ticker := time.NewTicker(c.options.RefreshNodeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.connectAll()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cluster) connectAll() {
	discovery := c.discover
	if c.discover == nil {
		return
	}
	activeIDs := make(map[string]struct{})
	allDiscovered := len(c.options.Remotes) > 0

	for _, service := range c.options.Remotes {
		instances := discovery.Discover(service)
		if instances == nil {
			allDiscovered = false
			continue
		}

		for _, ins := range instances {
			//  跳过本节点
			if ins.GetID() == c.getID() {
				continue
			}
			activeIDs[ins.GetID()] = struct{}{}
			if _, ok := c.clients.Get(ins.GetID()); ok {
				continue
			}
			c.createClient(ins)
		}
	}

	if allDiscovered {
		c.pruneStaleClients(activeIDs)
	}
}

func (c *Cluster) pruneStaleClients(activeIDs map[string]struct{}) {
	var stale []*client
	c.clients.Range(func(key string, cli *client) bool {
		if _, ok := activeIDs[key]; !ok {
			stale = append(stale, cli)
		}
		return true
	})
	for _, cli := range stale {
		c.logger.Info("移除下线节点客户端", gen.FieldNodeID(cli.getID()))
		cli.Close(gen.ErrClusterNodeNotFound)
	}
}

func (c *Cluster) createClient(ins gen.ServiceInstance) {
	cli := newClient(ins, c.options.Client, c.clients)
	c.clients.Set(ins.GetID(), cli)
	grs.SafeGo(func() {
		cli.run(c.ctx)
	})
}

func (c *Cluster) sendToNode(from, to *gen.PID, msg *gen.ClusterMessage) error {
	if from == nil || to == nil {
		return gen.ErrActorPidNil
	}
	if c.discover == nil {
		return gen.ErrDiscoverIsNil
	}
	//  节点不可用
	if c.discover.DiscoverByID(to.GetNodeID()) == nil {
		return gen.ErrClusterPeerNotConnected
	}
	//  连接不存在
	cli, ok := c.clients.Get(to.GetNodeID())
	if !ok {
		return gen.ErrClusterPeerNotConnected
	}
	//  发送消息
	if err := cli.send(msg); err != nil {
		return err
	}
	c.logger.Debug("客户端发送消息",
		zap.String("from", from.String()),
		zap.String("to", to.String()),
	)
	return nil
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	message, err := newClusterMessage(from, to, msg)
	if err != nil {
		return err
	}
	return c.sendToNode(from, to, message)
}

func (c *Cluster) Broadcast(to *gen.PID, msg *gen.Message) error {
	if to == nil {
		c.logger.Error("集群广播", gen.FieldErr(gen.ErrActorPidNil))
		return gen.ErrActorPidNil
	}
	//  序列化消息
	encodedData, err := gen.Encode(msg)
	if err != nil {
		return err
	}
	//  遍历所有客户端
	c.clients.Range(func(key string, cli *client) bool {
		target := convertor.DeepClone(to)
		target.NodeID = cli.getID()
		message, err := newClusterMessageWithBin(gen.NoSender, target, encodedData)
		if err != nil {
			c.logger.Warn("广播消息失败", zap.String("target", target.String()), zap.Error(err))
			return true
		}
		if err := c.sendToNode(to, target, message); err != nil {
			c.logger.Warn("广播消息失败", zap.String("target", target.String()), zap.Error(err))
			return true
		}
		return true
	})

	return nil
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
		if c.cancel != nil {
			c.cancel()
		}
		c.logger.Info("集群已关闭")
		return nil
	})

}
