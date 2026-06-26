package grpc_cluster

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
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
		node:             node,
		peers:            maputil.NewConcurrentMap[string, *PeerConn](10),
		remoteNames:      node.GetOptions().RemoteNames,
		peerSendChanSize: node.GetOptions().Grpc.PeerSendChanSize,
	}
	c.server = NewNodeServer(c.bgGroup, c.node.GetOptions().RpcAddress, c)
	return c
}

// Cluster 集群管理器
type Cluster struct {
	node gen.INode

	remoteNames      []string
	peerSendChanSize int
	peers            *maputil.ConcurrentMap[string, *PeerConn]

	server *NodeServer

	bgGroup *grs.Group

	logger *zap.Logger
}

func (c *Cluster) Init(ctx context.Context) error {
	//  检测依赖
	if c.node == nil {
		return gen.ErrNodeIsNil
	}
	if c.node.GetSystem() == nil {
		return gen.ErrActorSystemNil
	}
	if c.node.GetRegistry() == nil {
		return gen.ErrRegistryNil
	}
	c.bgGroup = grs.NewGroup(ctx)

	c.logger = glog.GetLogger().With(glog.Component(clusterComponent))
	c.logger.Info("grpc集群初始化成功", zap.Strings("remoteNames", c.remoteNames))

	return nil
}

// GetSelfID 获取自身节点ID
func (c *Cluster) GetSelfID() string {
	return c.node.GetId()
}

func (c *Cluster) Start(ctx context.Context) error {
	c.logger.Info("grpc集群启动中")

	if err := c.server.run(); err != nil {
		return err
	}

	c.connectAll()

	c.bgGroup.Go(func(ctx context.Context) {
		c.connectPeers(ctx)
	})

	c.logger.Info("grpc集群启动完成")
	return nil
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
	discovery := c.node.GetRegistry()
	list := discovery.DiscoverAll()
	for name, instances := range list {
		if !slices.Contains(c.remoteNames, name) {
			continue
		}
		for _, instance := range instances {
			c.reconcilePeer(instance)
		}
	}
}

func (c *Cluster) reconcilePeer(ins gen.ServiceInstance) {
	nodeID := ins.GetID()
	if nodeID == "" || nodeID == c.node.GetId() {
		return
	}
	_, ok := c.peers.Get(nodeID)
	if ok {
		return
	}
	peer := NewPeer(c.bgGroup, &PeerConfig{
		nodeID:  nodeID,
		address: ins.GetRpcAddress(),
		sendCh:  make(chan *gen.ClusterMessage, c.peerSendChanSize),
	})
	peer.run(time.Second)
	c.peers.Set(nodeID, peer)
	c.logger.Info("grpc集群连接节点", glog.NodeID(nodeID))
}

func (c *Cluster) onPeerClosed(nodeID string, peer *PeerConn) {
	current, ok := c.peers.Get(nodeID)
	if !ok {
		return
	}
	if current.nodeID != peer.nodeID || current.address != peer.address {
		return
	}
	c.peers.Delete(nodeID)
	c.logger.Info("grpc集群删除节点", glog.NodeID(nodeID))
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(from, to *gen.PID, msg *gen.Message) (err error) {
	discovery := c.node.GetRegistry()
	nodeId := to.GetNodeID()
	peer, exists := c.peers.Get(nodeId)
	if !exists {
		err = gen.ErrClusterNodeNotFound
		return
	}
	_, ok := discovery.GetInstance(nodeId)
	if !ok {
		err = gen.ErrClusterNodeNotInServiceList
		return
	}
	if !peer.IsConnected() {
		err = gen.ErrClusterNodeNotConnected
		return
	}
	m, wrong := NewClusterMessage(from, to, msg)
	if wrong != nil {
		err = wrong
		return
	}
	if err = peer.send(m); err != nil {
		return
	}

	c.logger.Debug("grpc集群发送消息到指定节点", zap.String("from", from.String()), zap.String("to", to.String()), zap.Uint16("msgID", msg.ID()))

	return
}

// Broadcast 广播消息到所有节点
func (c *Cluster) Broadcast(to *gen.PID, msg *gen.Message) {
	var success int
	c.peers.Range(func(nodeID string, peer *PeerConn) bool {
		to = convertor.DeepClone(to)
		to.NodeID = nodeID
		m, err := NewClusterMessage(gen.NoSender, to, msg)
		if err != nil {
			c.logger.Warn("广播到节点失败", zap.String("target_node_id", nodeID), glog.Err(err))
			return true
		}
		if err := peer.send(m); err != nil {
			c.logger.Warn("广播到节点失败", zap.String("target_node_id", nodeID), glog.Err(err))
			return true
		}
		success++
		return true
	})
	c.logger.Debug("grpc集群广播消息到所有节点", zap.Any("to", to), zap.Any("msg", msg), zap.Int("success", success))
}

func (c *Cluster) Dispatch(msg *gen.ClusterMessage) error {
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
	c.logger.Debug("grpc集群接受处理消息", zap.Any("msg", msg))
	return nil
}

// Stop 关闭集群连接
func (c *Cluster) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	c.bgGroup.Cancel()
	if err = c.server.shutdown(); err != nil {
		c.logger.Warn("集群server停止失败", zap.Error(err))
		return err
	}
	if err = c.bgGroup.Wait(ctx); err != nil {
		c.logger.Warn("等待集群后台协程退出超时", glog.NodeID(c.GetSelfID()), glog.Err(err))
		return err
	}
	c.logger.Info("集群已关闭", glog.NodeID(c.GetSelfID()))
	return err
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
