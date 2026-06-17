package cluster

import (
	"context"
	"fmt"
	"game-server/framework/cluster/define"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
)

type IServerInstance interface {
	GetID() string
	GetName() string
	GetAddress() string
	GetPort() int
}

type IDispatcher interface {
	Handler(message *define.ClusterMessage) error
}

type IServerList interface {
	Get() []IServerInstance
}

// Cluster 集群管理器
type Cluster struct {
	cfg *Config

	peers *maputil.ConcurrentMap[string, *PeerConn]

	dispatcher IDispatcher
	serverList IServerList

	server *NodeServer

	ctx       context.Context
	ctxCancel context.CancelFunc

	closeOnce sync.Once
}

// Config 集群配置
type Config struct {
	NodeID           string
	ListenAddr       string
	PeerSendChanSize int // 通道缓冲区大小
	PeerNames        []string
}

// NewCluster 创建集群管理器
func NewCluster(cfg *Config, serverList IServerList, dispatcher IDispatcher) *Cluster {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Cluster{
		peers:      maputil.NewConcurrentMap[string, *PeerConn](10),
		ctx:        ctx,
		ctxCancel:  cancel,
		serverList: serverList,
		cfg:        cfg,
		dispatcher: dispatcher,
	}
	c.server = NewNodeServer(cfg.ListenAddr, c)

	glog.Info("集群初始化完成",
		zap.String("node_id", c.GetSelfID()),
		zap.String("listen_addr", c.cfg.ListenAddr),
	)
	return c
}

// Run
//
//	@Description: 阻塞调用
//	@receiver c
//	@return error
func (c *Cluster) Run() error {
	glog.Info("集群启动中", zap.String("node_id", c.GetSelfID()))
	grs.SafeGo(func() {
		c.connectPeers()
	})
	return c.server.Serve()

}

func (c *Cluster) connectPeers() {
	if c.serverList == nil {
		glog.Warn("未配置注册中心",
			zap.String("msg", "未配置注册中心，跳过节点发现"),
			zap.String("node_id", c.GetSelfID()),
		)
		return
	}
	glog.Info("开始节点发现",
		zap.String("msg", "开始节点发现"),
		zap.String("node_id", c.GetSelfID()),
	)
	c.tryConnectPeers()
	ticker := time.NewTicker(time.Second * 3)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.tryConnectPeers()
		case <-c.ctx.Done():
			glog.Info("节点发现已停止",
				zap.String("msg", "节点发现已停止"),
				zap.String("node_id", c.GetSelfID()),
			)
			return
		}
	}
}
func (c *Cluster) tryConnectPeers() {
	list := c.serverList.Get()
	for _, instance := range list {
		if !slices.Contains(c.cfg.PeerNames, instance.GetName()) {
			continue
		}
		c.reconcilePeer(instance)
	}
}

func (c *Cluster) reconcilePeer(ins IServerInstance) {
	nodeID := ins.GetID()
	if nodeID == "" || nodeID == c.cfg.NodeID {
		if nodeID == "" {
			glog.Warn("跳过无效节点实例",
				zap.String("msg", "服务实例缺少节点ID，跳过"),
			)
		}
		return
	}
	address := net.JoinHostPort(ins.GetAddress(), strconv.Itoa(ins.GetPort()))
	_, ok := c.peers.Get(nodeID)
	if ok {
		glog.Debug("节点已存在",
			zap.String("msg", "节点已存在，跳过重复连接"),
			zap.String("node_id", nodeID),
		)
		return
	}
	peer := NewPeer(&PeerConfig{
		nodeID:     nodeID,
		address:    address,
		sendCh:     make(chan *define.ClusterMessage, c.cfg.PeerSendChanSize),
		dispatcher: c,
		onClosed:   c.onPeerClosed,
	})
	c.peers.Set(nodeID, peer)
	glog.Info("新增节点连接",
		zap.String("msg", "已添加节点连接"),
		zap.String("node_id", nodeID),
		zap.String("address", address),
	)
}

func (c *Cluster) onPeerClosed(nodeID string, peer *PeerConn) {
	current, ok := c.peers.Get(nodeID)
	if !ok {
		return
	}
	if current.nodeID != peer.nodeID || current.address != peer.address {
		glog.Debug("跳过删除过期节点",
			zap.String("msg", "关闭回调实例不匹配，跳过删除"),
			zap.String("node_id", nodeID),
			zap.String("current_address", current.address),
			zap.String("closing_address", peer.address),
		)
		return
	}
	c.peers.Delete(nodeID)
	glog.Info("节点已从集群移除", zap.String("node_id", nodeID))
}

// SendToNode 发送消息到指定节点
func (c *Cluster) SendToNode(nodeId string, payload []byte) error {
	peer, exists := c.peers.Get(nodeId)
	if !exists {
		glog.Warn("发送失败，目标节点不存在",
			zap.String("target_node_id", nodeId),
		)
		return fmt.Errorf("节点不存在: %s", nodeId)
	}

	if !peer.IsConnected() {
		glog.Warn("发送失败，目标节点未连接",
			zap.String("target_node_id", nodeId),
		)
		return fmt.Errorf("节点未连接: %s", nodeId)
	}

	msg := define.NewClusterMessage(c.cfg.NodeID, nodeId, payload)
	if err := peer.send(msg); err != nil {
		glog.Warn("发送失败，写入节点发送队列异常",
			zap.String("target_node_id", nodeId),
			zap.Error(err),
		)
		return err
	}
	return nil
}

// Broadcast 广播消息到所有节点
func (c *Cluster) Broadcast(payload []byte) {

	c.peers.Range(func(nodeID string, peer *PeerConn) bool {

		msg := define.NewClusterMessage(c.cfg.NodeID, nodeID, payload)

		if err := peer.send(msg); err != nil {
			glog.Warn("广播到节点失败",
				zap.String("target_node_id", nodeID),
				zap.Error(err),
			)
		}
		return true
	})
}

// GetSelfID 获取自身节点ID
func (c *Cluster) GetSelfID() string {
	return c.cfg.NodeID
}

func (c *Cluster) Handler(clusterMsg *define.ClusterMessage) error {
	//from := define.ProtoPIDToActor(clusterMsg.GetSourcePid())
	//to := define.ProtoPIDToActor(clusterMsg.GetTargetPid())
	//msg := define.ClusterMessageToProtocol(clusterMsg.GetMessage())
	if err := c.dispatcher.Handler(clusterMsg); err != nil {
		glog.Error("grpc 分发消息失败",
			//zap.String("target_node_id", to.NodeID),
			//zap.String("target_actor_name", to.ActorName),
			//zap.Uint64("target_actor_id", to.ActorID),
			zap.Error(err),
		)
	}
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
		glog.Info("集群已关闭",
			zap.String("msg", "集群已关闭"),
			zap.String("node_id", c.GetSelfID()),
		)
	})
}
