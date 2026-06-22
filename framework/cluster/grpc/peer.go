package grpc_cluster

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// PeerConn 节点连接
type PeerConn struct {
	mu sync.RWMutex

	nodeID  string
	address string

	conn   *grpc.ClientConn
	client NodeServiceClient
	stream NodeService_StreamClient

	sendCh     chan *gen.ClusterMessage
	dispatcher Dispatcher
	onClosed   func(nodeID string, peer *PeerConn)

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// PeerConfig 节点配置
type PeerConfig struct {
	nodeID     string
	address    string
	sendCh     chan *gen.ClusterMessage
	dispatcher Dispatcher
	onClosed   func(nodeID string, peer *PeerConn)
}

// NewPeer 创建节点连接
func NewPeer(cfg *PeerConfig) *PeerConn {
	ctx, cancel := context.WithCancel(context.Background())

	p := &PeerConn{
		nodeID:     cfg.nodeID,
		address:    cfg.address,
		sendCh:     cfg.sendCh,
		dispatcher: cfg.dispatcher,
		onClosed:   cfg.onClosed,
		ctx:        ctx,
		cancel:     cancel,
	}

	grs.SafeGo(func() {
		//  阻塞等待连接成功
		if err := p.connect(); err != nil {
			glog.Error("节点连接失败",
				zap.String("node_id", p.nodeID),
				zap.String("address", p.address),
				zap.Error(err),
			)
			p.Close()
			return
		}

		grs.SafeGo(func() {
			p.sendLoop()
		})

		grs.SafeGo(func() {
			p.recvLoop()
		})
	})
	return p
}

// connect 建立连接
func (p *PeerConn) connect() error {
	conn, err := grpc.NewClient(p.address,
		// 生产环境建议替换为 TLS 凭证（credentials.NewTLS），
		// 当前使用明文凭证适用于内网可信环境。
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				// 首次重连等待时间：避免瞬时抖动导致的高频拨号。
				BaseDelay: 1 * time.Second,
				// 指数退避倍数：每次失败后等待时间按该倍率增长。
				Multiplier: 1.6,
				// 退避抖动比例：打散多节点同时重连，避免惊群。
				Jitter: 0.2,
				// 最大退避上限：故障持续时，最长每 30s 尝试一次重连。
				MaxDelay: 30 * time.Second,
			},
			// 单次建连最小超时窗口：避免网络抖动下过快失败。
			MinConnectTimeout: 5 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			// 空闲 10s 发送一次 ping，及时发现静默断链。
			Time: 10 * time.Second,
			// ping 超过 3s 无响应则判定链路异常。
			Timeout: 3 * time.Second,
			// 即使没有活跃 RPC 也发送 keepalive，适合长连接场景。
			PermitWithoutStream: true,
		}),
		// 单地址场景使用 pick_first，优先保持连接稳定性。
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"pick_first"}`),
	)
	if err != nil {
		return err
	}

	client := NewNodeServiceClient(conn)

	stream, err := client.Stream(p.ctx, grpc.WaitForReady(true))
	if err != nil {
		conn.Close()
		return err
	}

	p.mu.Lock()
	p.conn = conn
	p.client = client
	p.stream = stream
	p.mu.Unlock()

	glog.Info("节点连接成功",
		zap.String("node_id", p.nodeID),
		zap.String("address", p.address),
	)
	return nil
}

// sendLoop 发送循环
func (p *PeerConn) sendLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-p.sendCh:
			if !ok {
				return
			}
			p.mu.RLock()
			stream := p.stream
			p.mu.RUnlock()

			if stream == nil {
				continue
			}

			if err := stream.Send(msg); err != nil {
				glog.Error("节点消息发送失败",
					zap.String("node_id", p.nodeID),
					zap.Error(err),
				)
				p.Close()
				return
			}
		}
	}
}

// recvLoop 接收循环
func (p *PeerConn) recvLoop() {
	for {
		p.mu.RLock()
		stream := p.stream
		p.mu.RUnlock()

		if stream == nil {
			return
		}

		msg, err := stream.Recv()
		if err != nil {
			// 对端 handler 正常 return nil，流结束 || 连接关闭
			if err == io.EOF {
				glog.Info("节点流已结束",
					zap.String("node_id", p.nodeID),
				)
				p.Close()
				return
			}
			glog.Error("节点消息接收失败",
				zap.String("node_id", p.nodeID),
				zap.Error(err),
			)
			p.Close()
			return

		}
		if err = p.dispatcher.Dispatch(msg); err != nil {
			glog.Error("节点消息分发失败",
				zap.String("node_id", p.nodeID),
				zap.Error(err),
			)
		}
	}
}

func (p *PeerConn) send(msg *gen.ClusterMessage) error {
	if !p.IsConnected() {
		return fmt.Errorf("peer not connected")
	}
	select {
	case p.sendCh <- msg:
	default:
		glog.Warn("节点发送队列写入失败",
			zap.String("target_node_id", p.nodeID),
			zap.String("reason", "发送通道已满"),
		)
		return fmt.Errorf("send_channel_full")
	}
	return nil
}

// IsConnected 检查连接状态
func (p *PeerConn) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conn == nil {
		return false
	}
	state := p.conn.GetState()
	return state == connectivity.Ready
}

// Close 关闭节点连接
func (p *PeerConn) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		cancel := p.cancel
		stream := p.stream
		conn := p.conn
		p.stream = nil
		p.client = nil
		p.conn = nil
		p.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if stream != nil {
			_ = stream.CloseSend()
		}
		if conn != nil {
			_ = conn.Close()
		}
		if p.onClosed != nil {
			p.onClosed(p.nodeID, p)
		}

		glog.Info("节点连接已关闭", zap.String("node_id", p.nodeID))
	})
}
