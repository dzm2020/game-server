package grpc_cluster

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const clientComponent = "cluster.grpc.client"

// ClientConfig 节点配置
type ClientConfig struct {
	nodeID   string
	address  string
	sendSize int
	cluster  *Cluster
}

// NewClient 创建节点连接
func NewClient(cfg ClientConfig) *Client {
	p := &Client{
		nodeID:   cfg.nodeID,
		address:  cfg.address,
		sendCh:   make(chan *gen.ClusterMessage, cfg.sendSize),
		cluster:  cfg.cluster,
		runGroup: cfg.cluster.bgGroup,
	}

	p.logger = glog.GetLogger().With(
		glog.Component(clientComponent),
		glog.NodeID(cfg.nodeID),
		zap.String("address", cfg.address),
	)

	p.ctx, p.cancelFunc = context.WithCancel(p.runGroup.Context())
	p.logger.Info("初始换客户端")
	return p
}

type Client struct {
	mu sync.RWMutex

	nodeID  string
	address string

	conn   *grpc.ClientConn
	stream NodeService_StreamClient

	sendCh  chan *gen.ClusterMessage
	cluster *Cluster

	runGroup  *grs.Group
	closeOnce sync.Once

	logger *zap.Logger

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (p *Client) getNodeId() string {
	return p.nodeID
}
func (p *Client) start() {
	p.runGroup.Go(func(context.Context) {
		if err := p.connect(); err != nil {
			p.Close()
			return
		}
		p.startIOLoops()
		p.logger.Info("客户端连接成功")
		return
	})
}

// connect 建立连接
func (p *Client) connect() error {
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
		p.logger.Error("客户端创建失败")
		return err
	}
	//  懒连接，创建 ClientConn 时通常不会马上连。真正连接一般发生在第一次 RPC，或者你主动Connect
	conn.Connect()
	client := NewNodeServiceClient(conn)
	stream, err := client.Stream(p.ctx, grpc.WaitForReady(true))
	if err != nil {
		p.logger.Error("客户端开启流失败")
		_ = conn.Close()
		return err
	}

	p.mu.Lock()
	p.conn = conn
	p.stream = stream
	p.mu.Unlock()
	p.logger.Info("客户端连接成功")
	return nil
}

func (p *Client) startIOLoops() {
	p.runGroup.Go(func(_ context.Context) {
		p.logger.Info("客户端写协程启动")
		p.sendLoop()
		p.logger.Info("客户端写协程关闭")
	})

	p.runGroup.Go(func(_ context.Context) {
		p.logger.Info("客户端读协程启动")
		p.recvLoop()
		p.logger.Info("客户端读协程关闭")
	})
}

// sendLoop 发送循环
func (p *Client) sendLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, ok := <-p.sendCh:
			if !ok {
				return
			}
			stream := p.currentStream()

			if stream == nil {
				p.logger.Error("发送流为空")
				continue
			}

			if err := stream.Send(msg); err != nil {
				p.logger.Error("流发送失败", glog.Err(err))
				p.Close()
				return
			}
		}
	}
}

// recvLoop 接收循环
func (p *Client) recvLoop() {
	for {
		stream := p.currentStream()

		if stream == nil {
			p.logger.Error("接收流为nil")
			return
		}

		msg, err := stream.Recv()
		if err != nil {
			// 对端 handler 正常 return nil，流结束 || 连接关闭
			if isStreamClosedErr(err) {
				p.logger.Info("接收流关闭", glog.Err(err))
				p.Close()
				return
			}

			p.logger.Error("接收流关闭", glog.Err(err))
			p.Close()
			return

		}
		if err = p.cluster.Dispatch(msg); err != nil {
			p.logger.Error("分发消息失败", glog.Err(err))
		}
	}
}

func (p *Client) send(msg *gen.ClusterMessage) error {
	if !p.IsConnected() {
		err := gen.ErrClusterPeerNotConnected
		p.logger.Error("客户端发送消息", glog.Err(err))
		return err
	}
	select {
	case p.sendCh <- msg:
	default:
		err := gen.ErrClusterSendChannelFull
		p.logger.Error("客户端发送消息", glog.Err(err))
		return err
	}
	return nil
}

func (p *Client) currentStream() NodeService_StreamClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stream
}

func isStreamClosedErr(err error) bool {
	return err == io.EOF || errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

// IsConnected 检查连接状态
func (p *Client) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conn == nil {
		return false
	}
	state := p.conn.GetState()
	return state == connectivity.Ready
}

// Close 关闭节点连接
func (p *Client) Close() {
	p.closeOnce.Do(func() {
		if p.cancelFunc != nil {
			p.cancelFunc()
		}

		p.mu.Lock()
		stream := p.stream
		conn := p.conn

		p.conn = nil
		p.stream = nil
		p.mu.Unlock()

		if stream != nil {
			_ = stream.CloseSend()
		}
		if conn != nil {
			_ = conn.Close()
		}
		p.cluster.removeClient(p)
		p.logger.Info("客户端关闭")
	})
}
