package grpc

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"sync"

	"github.com/duke-git/lancet/v2/maputil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const clientComponent = "cluster.grpc.client"

// newClient 创建节点连接
func newClient(instance gen.ServiceInstance, options ClientOptions, manager *maputil.ConcurrentMap[string, *client]) *client {
	p := &client{
		manager:  manager,
		instance: instance,
		options:  options,
		sendCh:   make(chan *gen.ClusterMessage, options.SendChanSize),
	}

	p.logger = glog.GetLogger().With(
		gen.FieldComponent(clientComponent),
		gen.FieldNodeID(p.getID()),
		zap.String("address", p.getAddress()),
	)

	p.logger.Debug("初始化客户端")
	return p
}

type client struct {
	mu sync.RWMutex

	instance gen.ServiceInstance
	options  ClientOptions
	manager  *maputil.ConcurrentMap[string, *client]

	conn   *grpc.ClientConn
	stream NodeService_StreamClient
	sendCh chan *gen.ClusterMessage

	closeOnce  sync.Once
	logger     *zap.Logger
	ctx        context.Context
	cancelFunc context.CancelFunc
}

func (p *client) getID() string {
	return p.instance.GetID()
}
func (p *client) getAddress() string {
	return p.instance.GetRpcAddress()
}
func (p *client) run(ctx context.Context) {
	p.ctx, p.cancelFunc = context.WithCancel(ctx)
	if err := p.connect(); err != nil {
		p.Close(err)
		return
	}
	grs.SafeGo(func() {
		p.sendLoop()
	})

	p.logger.Debug("客户端连接成功")
}

// connect 建立连接
func (p *client) connect() error {
	conn, err := grpc.NewClient(p.instance.GetRpcAddress(), toDialOptions(p.options)...)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()

	//  触发建立连接
	ctx, cancel := context.WithTimeout(p.ctx, p.options.ConnectTimeout)
	defer cancel()
	cli := NewNodeServiceClient(conn)
	stream, err := cli.Stream(ctx)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.stream = stream
	p.mu.Unlock()

	return nil
}

// sendLoop 发送循环
func (p *client) sendLoop() {
	var wrong error
	defer p.Close(wrong)
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg, _ := <-p.sendCh:
			stream := p.currentStream()
			if stream == nil {
				wrong = gen.ErrClusterPeerNotConnected
				return
			}
			if err := stream.Send(msg); err != nil {
				wrong = err
				return
			}
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

	if p.conn == nil || p.stream == nil {
		return false
	}
	state := p.conn.GetState()
	return state == connectivity.Ready
}

// Close 关闭节点连接
func (p *client) Close(err error) {
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

		if err != nil && !isServerStreamClosedErr(err) {
			p.logger.Error("客户端关闭", zap.Error(err))
		} else {
			p.logger.Info("客户端关闭", zap.Any("reason", err))
		}

		p.manager.Delete(p.getID())
	})
}
