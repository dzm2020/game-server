package grpc_cluster

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"

	"slices"

	"sync"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

type Options struct {
	NodeID           string
	ListenAddr       string
	PeerSendChanSize int
	PeerNames        []string
}

type Dispatcher interface {
	Dispatch(msg *gen.ClusterMessage) error
}

// Cluster 集群管理器
type Cluster struct {
	options *Options

	peers *maputil.ConcurrentMap[string, *PeerConn]

	invoker   gen.ILocalInvoker
	discovery gen.IDiscovery

	server *NodeServer

	ctx       context.Context
	ctxCancel context.CancelFunc

	closeOnce sync.Once
}

func New(options *Options) *Cluster {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		peers:     maputil.NewConcurrentMap[string, *PeerConn](10),
		ctx:       ctx,
		ctxCancel: cancel,
		options:   options,
	}
	c.server = NewNodeServer(options.ListenAddr, c)

	return c
}
func (c *Cluster) SetLocalInvoker(invoker gen.ILocalInvoker) {
	c.invoker = invoker
}

func (c *Cluster) SetDiscovery(discovery gen.IDiscovery) {
	c.discovery = discovery
}

// Run 启动服务端和对端连接协程。
// 该方法会等待服务监听就绪以及已有 peer 连通检查，然后快速返回。
func (c *Cluster) Run() error {
	glog.Info("grpc集群启动中", zap.String("node_id", c.GetSelfID()))

	if err := c.server.Serve(); err != nil {
		glog.Info("grpc集群启动中", zap.String("node_id", c.GetSelfID()))
		return err
	}

	c.tryConnectPeers()
	grs.SafeGo(func() {
		glog.Info("grpc监控集群变化协程启动", zap.String("node_id", c.GetSelfID()), zap.Strings("peer_names", c.options.PeerNames))
		c.connectPeers()
		glog.Info("grpc监控集群变化协程关闭", zap.String("node_id", c.GetSelfID()), zap.Strings("peer_names", c.options.PeerNames))
	})

	if err := c.waitPeersReady(10 * time.Second); err != nil {
		glog.Error("grpc集群启动完成", zap.String("node_id", c.GetSelfID()), zap.Error(err))
	}
	_, total := c.peerReadyState()
	glog.Info("grpc集群启动完成", zap.String("node_id", c.GetSelfID()), zap.Int("peer_count", total))
	return nil
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
		case <-c.ctx.Done():
			return gen.ErrClusterClosed
		case <-deadline.C:
			return fmt.Errorf("%w: connected=%d total=%d", gen.ErrClusterWaitPeerReadyTimeout, connected, total)
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
	list := c.discovery.DiscoverAll()
	for name, instances := range list {
		if !slices.Contains(c.options.PeerNames, name) {
			continue
		}
		for _, instance := range instances {
			c.reconcilePeer(instance)
		}
	}
}

func (c *Cluster) connectPeers() {
	if c.discovery == nil {
		glog.Warn("grpc集群连接对端失败,未配置注册中心", zap.String("node_id", c.GetSelfID()))
		return
	}
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.tryConnectPeers()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Cluster) reconcilePeer(ins gen.ServiceInstance) {
	nodeID := ins.GetID()
	if nodeID == "" || nodeID == c.options.NodeID {
		return
	}
	_, ok := c.peers.Get(nodeID)
	if ok {
		return
	}
	peer := NewPeer(&PeerConfig{
		nodeID:     nodeID,
		address:    ins.GetRpcAddress(),
		sendCh:     make(chan *gen.ClusterMessage, c.options.PeerSendChanSize),
		dispatcher: c,
		onClosed:   c.onPeerClosed,
	})
	c.peers.Set(nodeID, peer)
	glog.Info("grpc集群新增连接",
		zap.String("node_id", nodeID),
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
			zap.String("node_id", nodeID),
			zap.String("current_address", current.address),
			zap.String("closing_address", peer.address),
		)
		return
	}
	c.peers.Delete(nodeID)
	glog.Info("grpc集群删除节点", zap.String("node_id", nodeID))
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(from, to *gen.PID, msg *gen.Message) error {
	nodeId := to.GetNodeID()
	peer, exists := c.peers.Get(nodeId)
	if !exists {
		err := fmt.Errorf("%w: %s", gen.ErrClusterNodeNotFound, nodeId)
		glog.Error("grpc集群发送消息到指定节点", zap.String("target_node_id", nodeId), zap.Error(err))
		return err
	}
	_, ok := c.discovery.GetInstance(nodeId)
	if !ok {
		err := fmt.Errorf("%w: %s", gen.ErrClusterNodeNotInServiceList, nodeId)
		glog.Error("grpc集群发送消息到指定节点", zap.String("target_node_id", nodeId), zap.Error(err))
		return err
	}
	if !peer.IsConnected() {
		err := fmt.Errorf("%w: %s", gen.ErrClusterNodeNotConnected, nodeId)
		glog.Error("grpc集群发送消息到指定节点", zap.String("target_node_id", nodeId))
		return err
	}
	m, err := NewClusterMessage(from, to, msg)
	if err != nil {
		glog.Error("grpc集群发送消息到指定节点", zap.String("target_node_id", nodeId), zap.Error(err))
		return err
	}
	if err := peer.send(m); err != nil {
		glog.Error("grpc集群发送消息到指定节点", zap.String("target_node_id", nodeId), zap.Error(err))
		return err
	}
	glog.Debug("grpc集群发送消息到指定节点", zap.Any("from", from), zap.Any("to", to), zap.Any("msg", msg))
	return nil
}

// Broadcast 广播消息到所有节点
func (c *Cluster) Broadcast(to *gen.PID, msg *gen.Message) {
	var success int
	c.peers.Range(func(nodeID string, peer *PeerConn) bool {
		to.NodeID = nodeID
		m, err := NewClusterMessage(gen.NoSender, to, msg)
		if err != nil {
			glog.Warn("广播到节点失败", zap.String("target_node_id", nodeID), zap.Error(err))
			return true
		}
		if err := peer.send(m); err != nil {
			glog.Warn("广播到节点失败", zap.String("target_node_id", nodeID), zap.Error(err))
			return true
		}
		success++
		return true
	})
	glog.Debug("grpc集群广播消息到所有节点", zap.Any("to", to), zap.Any("msg", msg), zap.Int("success", success))
}

// GetSelfID 获取自身节点ID
func (c *Cluster) GetSelfID() string {
	return c.options.NodeID
}

func (c *Cluster) Dispatch(msg *gen.ClusterMessage) error {
	m, _, err := gen.Decode(msg.Data)
	if err != nil {
		glog.Error("grpc集群接受处理消息", zap.Error(err))
		return err
	}
	if m == nil {
		err := gen.ErrClusterDecodeFailed
		glog.Error("grpc集群接受处理消息", zap.ByteString("data", msg.Data), zap.Error(err))
		return err
	}
	to := msg.TargetPid
	if err := c.invoker.Handler(msg.SourcePid, msg.TargetPid, m); err != nil {
		glog.Error("grpc集群接受处理消息",
			zap.String("target_node_id", to.NodeID),
			zap.String("target_actor_name", to.ActorName),
			zap.Uint64("target_actor_id", to.ActorID),
			zap.Error(err),
		)
	}
	glog.Debug("grpc集群接受处理消息", zap.Any("msg", msg))
	return nil
}

// Close 关闭集群连接
func (c *Cluster) Close() {
	c.closeOnce.Do(func() {
		if c.ctxCancel != nil {
			c.ctxCancel()
		}

		var peers []*PeerConn
		c.peers.Range(func(nodeID string, peer *PeerConn) bool {
			peers = append(peers, peer)
			return true
		})

		for _, peer := range peers {
			peer.Close()
		}
		c.server.shutdown()
		glog.Info("集群已关闭", zap.String("node_id", c.GetSelfID()))
	})
}

// Shutdown 是 Close 的语义别名，方便与其他组件的生命周期命名保持一致。
func (c *Cluster) Shutdown() {
	c.Close()
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
