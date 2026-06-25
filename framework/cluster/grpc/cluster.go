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
		bgGroup:          grs.NewGroup(context.Background()),
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
	return nil
}

func (c *Cluster) Start(ctx context.Context) error {
	var err error
	glog.Info("grpc集群启动中", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()))
	c.bgGroup = grs.NewGroup(ctx)
	if err = c.server.Serve(); err != nil {
		glog.Error("grpc集群启动失败", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()), glog.Err(err))
		return err
	}
	c.tryConnectPeers()
	c.bgGroup.Go(func(ctx context.Context) {
		glog.Info("grpc监控集群变化协程启动", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()), zap.Strings("peer_names", c.remoteNames))
		c.connectPeers(ctx)
		glog.Info("grpc监控集群变化协程关闭", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()), zap.Strings("peer_names", c.remoteNames))
	})

	if err = c.waitPeersReady(10 * time.Second); err != nil {
		glog.Warn("grpc集群启动完成(部分peer未就绪)", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()), glog.Err(err))
	}
	_, total := c.peerReadyState()
	glog.Info("grpc集群启动完成", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()), zap.Int("peer_count", total))

	return err
}

func (c *Cluster) waitPeersReady(timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		connected, total := c.peerReadyState()
		if total == 0 || connected == total {
			return nil
		}

		select {
		case <-c.bgGroup.Context().Done():
			return gen.ErrClusterClosed
		case <-deadline.C:
			return gen.ErrClusterWaitPeerReadyTimeout
		case <-ticker.C:
		}
	}
}

func (c *Cluster) peerReadyState() (connected int, total int) {
	c.peers.Range(func(_ string, peer *PeerConn) bool {
		total++
		if peer != nil && peer.IsConnected() {
			connected++
		}
		return true
	})
	return
}

func (c *Cluster) tryConnectPeers() {
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

func (c *Cluster) connectPeers(ctx context.Context) {
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.tryConnectPeers()
		case <-ctx.Done():
			return
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
		nodeID:     nodeID,
		address:    ins.GetRpcAddress(),
		sendCh:     make(chan *gen.ClusterMessage, c.peerSendChanSize),
		dispatcher: c,
		onClosed:   c.onPeerClosed,
	})
	c.peers.Set(nodeID, peer)
	glog.Info("grpc集群新增连接",
		glog.Component(clusterComponent),
		glog.NodeID(nodeID),
		zap.String("name", ins.GetName()),
		zap.String("ext_address", ins.GetExtAddress()),
		zap.String("rpc_address", ins.GetRpcAddress()),
	)
}

func (c *Cluster) onPeerClosed(nodeID string, peer *PeerConn) {
	current, ok := c.peers.Get(nodeID)
	if !ok {
		return
	}
	if current.nodeID != peer.nodeID || current.address != peer.address {
		glog.Warn("grpc集群删除节点",
			glog.Component(clusterComponent),
			glog.NodeID(nodeID),
			zap.String("current_address", current.address),
			zap.String("closing_address", peer.address),
		)
		return
	}
	c.peers.Delete(nodeID)
	glog.Info("grpc集群删除节点", glog.Component(clusterComponent), glog.NodeID(nodeID))
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	var err error
	for {
		discovery := c.node.GetRegistry()
		nodeId := to.GetNodeID()
		peer, exists := c.peers.Get(nodeId)
		if !exists {
			err = gen.ErrClusterNodeNotFound
			break
		}
		_, ok := discovery.GetInstance(nodeId)
		if !ok {
			err = gen.ErrClusterNodeNotInServiceList
			break
		}
		if !peer.IsConnected() {
			err = gen.ErrClusterNodeNotConnected
			break
		}
		m, wrong := NewClusterMessage(from, to, msg)
		if wrong != nil {
			err = wrong
			break
		}
		if err = peer.send(m); err != nil {
			break
		}
		break
	}
	if err != nil {
		glog.Error("grpc集群发送消息到指定节点", zap.String("from", from.String()), zap.String("to", to.String()), zap.Uint16("msgID", msg.ID()), zap.Error(err))
	} else {
		glog.Debug("grpc集群发送消息到指定节点", zap.String("from", from.String()), zap.String("to", to.String()), zap.Uint16("msgID", msg.ID()))
	}
	return nil
}

// Broadcast 广播消息到所有节点
func (c *Cluster) Broadcast(to *gen.PID, msg *gen.Message) {
	var success int
	c.peers.Range(func(nodeID string, peer *PeerConn) bool {
		to = convertor.DeepClone(to)
		to.NodeID = nodeID
		m, err := NewClusterMessage(gen.NoSender, to, msg)
		if err != nil {
			glog.Warn("广播到节点失败", glog.Component(clusterComponent), zap.String("target_node_id", nodeID), glog.Err(err))
			return true
		}
		if err := peer.send(m); err != nil {
			glog.Warn("广播到节点失败", glog.Component(clusterComponent), zap.String("target_node_id", nodeID), glog.Err(err))
			return true
		}
		success++
		return true
	})
	glog.Debug("grpc集群广播消息到所有节点", zap.Any("to", to), zap.Any("msg", msg), zap.Int("success", success))
}

// GetSelfID 获取自身节点ID
func (c *Cluster) GetSelfID() string {
	return c.node.GetId()
}

func (c *Cluster) Dispatch(msg *gen.ClusterMessage) error {
	system := c.node.GetSystem()
	m, _, err := gen.Decode(msg.Data)
	if err != nil {
		glog.Error("grpc集群接受处理消息", glog.Component(clusterComponent), glog.Err(err))
		return err
	}
	if m == nil {
		err = gen.ErrClusterDecodeFailed
		glog.Error("grpc集群接受处理消息", glog.Component(clusterComponent), zap.ByteString("data", msg.Data), glog.Err(err))
		return err
	}
	to := msg.TargetPid
	if err = system.Tell(msg.SourcePid, to, m); err != nil {
		glog.Error("grpc集群接受处理消息", glog.Component(clusterComponent), glog.PID(to), glog.Err(err))
	}
	glog.Debug("grpc集群接受处理消息", zap.Any("msg", msg))
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
		glog.Warn("集群server停止失败", zap.Error(err))
		return err
	}
	if err = c.bgGroup.Wait(ctx); err != nil {
		glog.Warn("等待集群后台协程退出超时", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()), glog.Err(err))
		return err
	}
	glog.Info("集群已关闭", glog.Component(clusterComponent), glog.NodeID(c.GetSelfID()))

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
