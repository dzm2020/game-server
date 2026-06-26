package grpc_cluster

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"slices"

	"time"

	"github.com/duke-git/lancet/v2/convertor"
	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

const clusterComponent = "cluster.grpc"

var _ gen.ICluster = (*Cluster)(nil)

func New(node gen.INode) *Cluster {
	c := &Cluster{
		node:    node,
		clients: maputil.NewConcurrentMap[string, *Client](10),
	}
	return c
}

// Cluster 集群管理器
type Cluster struct {
	component.BaseComponent
	node gen.INode

	clusterNames []string

	clientSendChanSize int
	clients            *maputil.ConcurrentMap[string, *Client]

	server *NodeServer

	bgGroup *grs.Group

	logger *zap.Logger
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
	if err := c.BaseComponent.Init(ctx); err != nil {
		return err
	}
	if err := c.validateDependencies(); err != nil {
		return err
	}
	c.logger = glog.GetLogger().With(glog.Component(clusterComponent), glog.NodeID(c.selfNodeID()))

	opts := c.node.GetOptions()
	c.clientSendChanSize = opts.Grpc.ClientSendChanSize
	c.clusterNames = opts.Clusters
	c.server = NewNodeServer(opts.RpcAddress, c)
	c.logger.Info("初始化成功",
		zap.Strings("clusterNames", c.clusterNames),
		zap.Int("clientSendChanSize", c.clientSendChanSize),
	)
	return nil
}

func (c *Cluster) Start(ctx context.Context) error {
	if err := c.BaseComponent.Start(ctx); err != nil {
		return err
	}
	c.logger.Info("开始启动")
	c.bgGroup = grs.NewGroup(ctx)
	if err := c.server.run(); err != nil {
		return err
	}

	c.connectAll()

	c.bgGroup.Go(func(ctx context.Context) {
		c.connectPeers(ctx)
	})

	c.logger.Info("启动完成")
	return nil
}

func (c *Cluster) addClient(client *Client) {
	if client == nil {
		return
	}
	c.clients.Set(client.getNodeId(), client)
}
func (c *Cluster) removeClient(client *Client) {
	if client == nil {
		return
	}
	c.clients.Delete(client.getNodeId())
}
func (c *Cluster) rangeClient(fn func(client *Client) bool) {
	c.clients.Range(func(key string, value *Client) bool {
		return fn(value)
	})
}
func (c *Cluster) getClient(nodeId string) *Client {
	cli, _ := c.clients.Get(nodeId)
	return cli
}

func (c *Cluster) shouldConnectService(serviceName string) bool {
	return slices.Contains(c.clusterNames, serviceName)
}

func (c *Cluster) selfNodeID() string {
	if c.node == nil {
		return ""
	}
	return c.node.GetId()
}

func (c *Cluster) connectPeers(ctx context.Context) {
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
	list := discovery.DiscoverAll()
	for name, instances := range list {
		if !c.shouldConnectService(name) {
			continue
		}
		for _, instance := range instances {
			c.reconcilePeer(instance)
		}
	}
}

func (c *Cluster) reconcilePeer(ins gen.ServiceInstance) {
	nodeID := ins.GetID()
	if nodeID == "" || nodeID == c.selfNodeID() {
		return
	}
	if c.getClient(nodeID) != nil {
		return
	}
	client := NewClient(ClientConfig{
		nodeID:   ins.GetID(),
		address:  ins.GetRpcAddress(),
		sendSize: c.clientSendChanSize,
		cluster:  c,
	})
	c.addClient(client)
	client.start()
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	if from == nil || to == nil {
		return gen.ErrActorPidNil
	}
	if msg == nil {
		return gen.ErrMessageNil
	}
	client, err := c.resolveSendClient(to)
	if err != nil {
		return err
	}
	m, err := NewClusterMessage(from, to, msg)
	if err != nil {
		return err
	}
	if err := client.send(m); err != nil {
		return err
	}
	c.logger.Debug("客户端发送消息",
		zap.String("from", from.String()),
		zap.String("to", to.String()),
		zap.Uint16("msgID", msg.ID()),
	)
	return nil
}

func (c *Cluster) resolveSendClient(to *gen.PID) (*Client, error) {
	discovery := c.node.GetRegistry()
	nodeID := to.GetNodeID()
	client := c.getClient(nodeID)
	if client == nil {
		return nil, gen.ErrClusterNodeNotFound
	}
	if _, ok := discovery.GetInstance(nodeID); !ok {
		return nil, gen.ErrClusterNodeNotInServiceList
	}
	if !client.IsConnected() {
		return nil, gen.ErrClusterNodeNotConnected
	}
	return client, nil
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
		c.logger.Error("集群广播", glog.Err(gen.ErrActorPidNil))
		return gen.ErrActorPidNil
	}
	if msg == nil {
		c.logger.Error("集群广播", glog.Err(gen.ErrMessageNil))
		return gen.ErrMessageNil
	}

	var success int
	c.rangeClient(func(client *Client) bool {
		target := convertor.DeepClone(to)
		target.NodeID = client.getNodeId()
		m, err := NewClusterMessage(gen.NoSender, target, msg)
		if err != nil {
			c.logger.Warn("集群广播", zap.String("to", to.String()), glog.Err(err))
			return true
		}
		if err := client.send(m); err != nil {
			c.logger.Warn("集群广播", zap.String("to", to.String()), glog.Err(err))
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
	system := c.node.GetSystem()
	m, _, err := gen.Decode(msg.Data)
	if err != nil {
		return err
	}
	if m == nil {
		err = gen.ErrClusterDecodeFailed
		return err
	}
	to := msg.TargetPid
	if err = system.Tell(msg.SourcePid, to, m); err != nil {
		return err
	}
	c.logger.Debug("分发消息",
		zap.String("from", msg.SourcePid.String()),
		zap.String("to", msg.TargetPid.String()),
	)
	return nil
}

// Stop 关闭集群连接
func (c *Cluster) Stop(ctx context.Context) error {
	if err := c.BaseComponent.Stop(ctx); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if c.server != nil {
		c.server.shutdown()
	}

	if c.bgGroup != nil {
		c.bgGroup.Cancel()
		if err := c.bgGroup.Wait(ctx); err != nil {
			c.logger.Warn("等待集群后台协程退出超时", glog.Err(err))
			return err
		}
	}

	c.logger.Info("集群已关闭")
	return nil
}

func NewClusterMessage(from, to *gen.PID, msg *gen.Message) (*gen.ClusterMessage, error) {
	data, err := gen.Encode(msg)
	if err != nil {
		return nil, err
	}
	return &gen.ClusterMessage{
		CreatedAt: time.Now().Unix(),
		SourcePid: from,
		TargetPid: to,
		Data:      data,
	}, nil
}
