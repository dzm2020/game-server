package node

import (
	"context"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

func (n *Node) wire() {
	n.system.SetNodeID(n.GetId())
	n.cluster.ApplyNode(n.GetId(), n.GetRpcAddress())
	n.cluster.SetLocalInvoker(n.system)
	n.cluster.SetDiscovery(n.registry)
	n.system.SetRemoteInvoker(n.cluster)
}

func (n *Node) registerCore() error {
	return n.AddComponent(n.registry, n.system, n.cluster)
}

func (n *Node) serviceInstance() gen.ServiceInstance {
	return gen.ServiceInstance{
		ID:         n.GetId(),
		Name:       n.GetName(),
		ExtAddress: n.GetExtAddress(),
		RpcAddress: n.GetRpcAddress(),
		Meta:       n.options.Meta,
		Tags:       n.options.Tags,
	}
}

func (n *Node) initComponents(ctx context.Context) error {
	n.options.Behavior.OnBeforeInit(n)
	if err := n.IManager.Init(ctx); err != nil {
		glog.Error("节点组件初始化失败", gen.FieldErr(err))
		return err
	}
	n.options.Behavior.OnAfterInit(n)
	glog.Info("节点组件初始化完成")
	return nil
}

func (n *Node) startComponents(ctx context.Context) error {
	n.options.Behavior.OnBeforeStart(n)
	if err := n.IManager.Start(ctx); err != nil {
		glog.Error("节点启动组件失败", gen.FieldErr(err))
		return err
	}
	n.options.Behavior.OnAfterStart(n)
	glog.Info("节点组件启动完成")
	return nil
}

func (n *Node) stopComponents(ctx context.Context) error {
	n.options.Behavior.OnBeforeStop(n)
	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Info("节点组件停止失败", gen.FieldErr(stopErr))
	}
	n.options.Behavior.OnAfterStop(n)
	glog.Info("节点组件完成停止")
	return stopErr
}

func (n *Node) start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n.phase == phaseRunning {
		return component.ErrManagerAlreadyStarted
	}
	if n.phase == phaseStopped {
		return component.ErrManagerStoppedCannotRestart
	}

	n.wire()
	if err := n.registerCore(); err != nil {
		return err
	}
	if err := n.initComponents(ctx); err != nil {
		return err
	}
	if err := n.startComponents(ctx); err != nil {
		return err
	}
	if err := n.registry.Register(n.serviceInstance()); err != nil {
		_ = n.stopComponents(ctx)
		return err
	}

	n.phase = phaseRunning
	return nil
}

func (n *Node) stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if n.phase != phaseRunning {
		return nil
	}

	var stopErr error
	if err := n.registry.Deregister(); err != nil {
		stopErr = err
	}
	if err := n.stopComponents(ctx); err != nil && stopErr == nil {
		stopErr = err
	}
	n.phase = phaseStopped
	return stopErr
}

// Startup starts the node and blocks until an OS shutdown signal is received.
func (n *Node) Startup() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	defer stop()

	if err := n.start(ctx); err != nil {
		_ = glog.Stop()
		return err
	}

	glog.Info("节点进入运行",
		zap.String("ext_address", n.GetExtAddress()),
		zap.String("rpc_address", n.GetRpcAddress()),
		zap.Int("process_id", os.Getpid()),
	)

	<-ctx.Done()

	stopErr := n.stop(context.Background())
	_ = glog.Stop()
	if stopErr != nil {
		return stopErr
	}
	glog.Info("节点收到退出信号")
	return nil
}
