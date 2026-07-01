package grpc

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/glog"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

const clientComponent = "cluster.grpc.client"

// clientConfig 节点配置
type clientConfig struct {
	nodeID   string
	address  string
	sendSize int
	cluster  *Cluster
}

// newClient 创建节点连接
func newClient(cfg clientConfig) *client {
	p := &client{
		nodeID:  cfg.nodeID,
		address: cfg.address,
		sendCh:  make(chan *gen.ClusterMessage, cfg.sendSize),
		cluster: cfg.cluster,
	}

	p.logger = glog.GetLogger().With(
		gen.FieldComponent(clientComponent),
		gen.FieldNodeID(cfg.nodeID),
		zap.String("address", cfg.address),
	)

	p.ctx, p.cancelFunc = context.WithCancel(p.cluster.bgGroup.Context())
	p.logger.Debug("初始化客户端")
	return p
}

type client struct {
	mu sync.RWMutex

	nodeID  string
	address string

	conn   *grpc.ClientConn
	stream NodeService_StreamClient

	sendCh  chan *gen.ClusterMessage
	cluster *Cluster

	closeOnce sync.Once

	logger *zap.Logger

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (p *client) start() {
	p.cluster.bgGroup.Go(func(context.Context) {
		if err := p.connect(); err != nil {
			p.Close()
			return
		}
		p.startIOLoops()
		p.logger.Debug("客户端连接成功")
	})
}

// connect 建立连接
func (p *client) connect() error {
	backoffOptions := p.cluster.options.Client.Backoff
	keepaliveOptions := p.cluster.options.Client.Keepalive
	permitWithoutStream := defaultClientPermitWithoutStream
	if keepaliveOptions.PermitWithoutStream != nil {
		permitWithoutStream = *keepaliveOptions.PermitWithoutStream
	}

	conn, err := grpc.NewClient(p.address,
		// 生产环境建议替换为 TLS 凭证（credentials.NewTLS），
		// 当前使用明文凭证适用于内网可信环境。
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  backoffOptions.BaseDelay,
				Multiplier: backoffOptions.Multiplier,
				Jitter:     backoffOptions.Jitter,
				MaxDelay:   backoffOptions.MaxDelay,
			},
			MinConnectTimeout: backoffOptions.MinConnectTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveOptions.Time,
			Timeout:             keepaliveOptions.Timeout,
			PermitWithoutStream: permitWithoutStream,
		}),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"`+p.cluster.options.Client.LoadBalancingPolicy+`"}`),
	)
	if err != nil {
		p.logger.Error("客户端创建失败", gen.FieldErr(err))
		return err
	}

	conn.Connect()
	if err = p.waitReady(conn); err != nil {
		p.logger.Error("等待客户端连接就绪失败", gen.FieldErr(err))
		_ = conn.Close()
		return err
	}

	waitForReady := defaultClientStreamWaitForReady
	if p.cluster.options.Client.WaitForReady != nil {
		waitForReady = *p.cluster.options.Client.WaitForReady
	}

	client := NewNodeServiceClient(conn)
	stream, err := client.Stream(p.ctx, grpc.WaitForReady(waitForReady))
	if err != nil {
		p.logger.Error("客户端开启流失败", gen.FieldErr(err))
		_ = conn.Close()
		return err
	}

	p.mu.Lock()
	p.conn = conn
	p.stream = stream
	p.mu.Unlock()
	return nil
}

func (p *client) waitReady(conn *grpc.ClientConn) error {
	waitCtx, waitCancel := context.WithTimeout(p.ctx, p.cluster.options.Client.ConnectTimeout)
	defer waitCancel()

	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return gen.ErrClusterPeerNotConnected
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			return waitCtx.Err()
		}
	}
}

func (p *client) startIOLoops() {
	p.cluster.bgGroup.Go(func(_ context.Context) {
		p.sendLoop()
	})

	p.cluster.bgGroup.Go(func(_ context.Context) {
		p.recvLoop()
	})
}

// sendLoop 发送循环
func (p *client) sendLoop() {
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
				p.logger.Error("流发送失败", gen.FieldErr(err))
				p.Close()
				return
			}
		}
	}
}

// recvLoop 接收循环
func (p *client) recvLoop() {
	for {
		stream := p.currentStream()

		if stream == nil {
			p.logger.Error("接收流为nil")
			return
		}

		msg, err := stream.Recv()
		if err != nil {
			// 对端 handler 正常 return nil，流结束 || 连接关闭
			if isServerStreamClosedErr(err) {
				p.logger.Debug("接收流关闭", gen.FieldErr(err))
				p.Close()
				return
			}

			p.logger.Error("接收流关闭", gen.FieldErr(err))
			p.Close()
			return

		}
		if err = p.cluster.Dispatch(msg); err != nil {
			p.logger.Error("分发消息失败", gen.FieldErr(err))
		}
	}
}

func (p *client) send(msg *gen.ClusterMessage) error {
	if !p.IsConnected() {
		err := gen.ErrClusterPeerNotConnected
		p.logger.Error("客户端发送消息", gen.FieldErr(err))
		return err
	}
	select {
	case p.sendCh <- msg:
	default:
		err := gen.ErrClusterSendChannelFull
		p.logger.Error("客户端发送消息", gen.FieldErr(err))
		return err
	}
	return nil
}

func (p *client) currentStream() NodeService_StreamClient {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.stream
}

// IsConnected 检查连接状态
func (p *client) IsConnected() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conn == nil {
		return false
	}
	state := p.conn.GetState()
	return state == connectivity.Ready
}

// Close 关闭节点连接
func (p *client) Close() {
	p.closeOnce.Do(func() {
		p.cluster.removeClient(p)
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
		p.logger.Debug("客户端关闭")
	})
}
