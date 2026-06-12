package node

import (
	consulregistry "consul_registry"
	"context"
	compcluster "game-server/internal/component/cluster"
	compdiscovery "game-server/internal/component/discovery"
	compgateway "game-server/internal/component/gateway"
	complogger "game-server/internal/component/logger"
	compmq "game-server/internal/component/messagequeue"
	compsystem "game-server/internal/component/system"
	"game-server/internal/iface"
	"game-server/internal/profile"
	"game-server/pkg/component"
	"game-server/pkg/glog"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// New 创建节点实例
func New(path string) *Node {
	node := &Node{
		Member:   new(iface.Member),
		IManager: component.NewComponentsMgr(),
		path:     path,
	}
	iface.SetCurrentNode(node)
	node.init()
	return node
}

var _ iface.INode = (*Node)(nil)

type Node struct {
	*iface.Member
	component.IManager
	path string

	lifecycleHook iface.INodeLifecycleHook
	components    []component.IComponent
}

func (n *Node) init() {
	profile.Init(n.path)
	glog.Info("加载全局配置完成", zap.String("path", n.path))

	logger := complogger.New()

	n.components = []component.IComponent{
		logger,
		compsystem.New(),
		compdiscovery.New(),
		compmq.New(),
		compcluster.New(),
		compgateway.New(),
	}

	if n.lifecycleHook != nil {
		glog.Info("触发生命周期回调", zap.String("hook", "OnInit"))
		n.lifecycleHook.OnInit(n)
	}

	glog.Info("节点初始化完成",
		zap.String("path", n.path),
		zap.Int("component_count", len(n.components)),
	)
}

func (n *Node) Info() *iface.Member {
	return n.Member
}

func (n *Node) AddComponents(comps ...component.IComponent) error {
	n.components = append(n.components, comps...)
	glog.Info("追加组件完成",
		zap.Int("added", len(comps)),
		zap.Int("total", len(n.components)),
	)
	return nil
}

func (n *Node) SetLifecycleHook(hook iface.INodeLifecycleHook) {
	n.lifecycleHook = hook
}

func (n *Node) Startup() (err error) {
	if err = n.Start(context.Background()); err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	glog.Info("等待退出信号", zap.Strings("signals", []string{"SIGINT", "SIGQUIT", "SIGKILL", "SIGTERM"}))
	sig := <-sigChan
	glog.Info("收到退出信号，开始优雅停机", zap.String("signal", sig.String()))

	return n.Stop(context.Background())
}

func (n *Node) Start(ctx context.Context) (err error) {
	glog.Info("节点启动流程开始",
		zap.String("node_id", n.GetID()),
		zap.Int("component_count", len(n.components)),
	)

	if n.lifecycleHook != nil {
		glog.Info("触发生命周期回调", zap.String("hook", "BeforeStart"))
		n.lifecycleHook.BeforeStart(n)
	}
	iface.SetCurrentNode(n)

	for idx, comp := range n.components {
		if err := n.IManager.AddComponent(comp); err != nil {
			glog.Error("注册组件失败",
				zap.Int("index", idx),
				zap.Error(err),
			)
			return err
		}
	}
	glog.Info("组件注册完成", zap.Int("registered", n.IManager.ComponentCount()))

	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("组件启动失败", zap.Error(err))
		return err
	}
	glog.Info("组件启动完成")

	if err = n.registerService(); err != nil {
		glog.Error("节点服务注册失败", zap.Error(err))
		return err
	}
	glog.Info("节点服务注册完成")

	if n.lifecycleHook != nil {
		glog.Info("触发生命周期回调", zap.String("hook", "AfterStart"))
		n.lifecycleHook.AfterStart(n)
	}
	return nil
}

// Shutdown 优雅关闭节点，关闭所有组件
func (n *Node) Stop(ctx context.Context) error {
	glog.Info("节点停止流程开始", zap.String("node_id", n.GetID()))

	if n.lifecycleHook != nil {
		glog.Info("触发生命周期回调", zap.String("hook", "BeforeStop"))
		n.lifecycleHook.BeforeStop(n)
	}

	if err := n.deregisterService(); err != nil {
		glog.Error("节点服务注销失败", zap.Error(err))
	}

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("组件停止失败", zap.Error(stopErr))
	} else {
		glog.Info("组件停止完成")
	}

	if n.lifecycleHook != nil {
		glog.Info("触发生命周期回调", zap.String("hook", "AfterStop"))
		n.lifecycleHook.AfterStop(n, stopErr)
	}
	glog.Info("节点停止流程结束", zap.Error(stopErr))
	return stopErr
}

func (n *Node) registerService() error {
	clusterComp := iface.GetComponent[*compcluster.Component]()
	if clusterComp == nil || clusterComp.Cluster == nil {
		return nil
	}

	base := profile.GetBase()
	if base == nil || base.Self == nil || base.Self.GetID() == "" {
		return nil
	}

	reg := consulregistry.ServiceRegistration{
		ID:      base.Self.GetID(),
		Name:    base.Self.Name,
		Address: base.Self.Address,
		Port:    base.Self.Port,
		Meta:    base.Self.Meta,
	}
	return clusterComp.Register(reg)
}

func (n *Node) deregisterService() error {
	clusterComp := iface.GetComponent[*compcluster.Component]()
	if clusterComp == nil || clusterComp.Cluster == nil {
		return nil
	}

	base := profile.GetBase()
	if base == nil || base.Self == nil || base.Self.GetID() == "" {
		return nil
	}

	return clusterComp.Deregister(base.Self.GetID())
}
