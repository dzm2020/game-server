package node

import (
	consulregistry "consul_registry"
	"context"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	compcluster "game-server/framework/runtime/component/cluster"
	compdiscovery "game-server/framework/runtime/component/discovery"
	complogger "game-server/framework/runtime/component/logger"
	compmq "game-server/framework/runtime/component/messagequeue"
	compsystem "game-server/framework/runtime/component/system"
	"game-server/framework/runtime/iface"
	"game-server/framework/runtime/profile"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// New creates a node instance.
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

	lifecycleHook iface.INodeBehavior
	components    []component.IComponent
}

func (n *Node) init() {
	profile.Init(n.path)
	glog.Info("base config loaded", zap.String("path", n.path))

	logger := complogger.New()

	n.components = []component.IComponent{
		logger,
		compsystem.New(),
		compdiscovery.New(),
		compmq.New(),
		compcluster.New(),
	}

	if n.lifecycleHook != nil {
		glog.Info("trigger lifecycle hook", zap.String("hook", "OnInit"))
		n.lifecycleHook.OnInit(n)
	}

	glog.Info("node initialized",
		zap.String("path", n.path),
		zap.Int("component_count", len(n.components)),
	)
}

func (n *Node) Info() *iface.Member {
	return n.Member
}

func (n *Node) AddComponents(comps ...component.IComponent) error {
	n.components = append(n.components, comps...)
	glog.Info("components appended",
		zap.Int("added", len(comps)),
		zap.Int("total", len(n.components)),
	)
	return nil
}

func (n *Node) SetLifecycleHook(hook iface.INodeBehavior) {
	n.lifecycleHook = hook
}

func (n *Node) Startup() (err error) {
	if err = n.Start(context.Background()); err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	glog.Info("waiting for shutdown signal", zap.Strings("signals", []string{"SIGINT", "SIGQUIT", "SIGKILL", "SIGTERM"}))
	sig := <-sigChan
	glog.Info("signal received, stopping node", zap.String("signal", sig.String()))

	return n.Stop(context.Background())
}

func (n *Node) Start(ctx context.Context) (err error) {
	glog.Info("node startup begin",
		zap.String("node_id", n.GetID()),
		zap.Int("component_count", len(n.components)),
	)

	if n.lifecycleHook != nil {
		glog.Info("trigger lifecycle hook", zap.String("hook", "BeforeStart"))
		n.lifecycleHook.BeforeStart(n)
	}
	
	for idx, comp := range n.components {
		if err := n.IManager.AddComponent(comp); err != nil {
			glog.Error("register component failed",
				zap.Int("index", idx),
				zap.Error(err),
			)
			return err
		}
	}
	glog.Info("components registered", zap.Int("registered", n.IManager.ComponentCount()))

	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("start components failed", zap.Error(err))
		return err
	}
	glog.Info("components started")

	if err = n.registerService(); err != nil {
		glog.Error("register service failed", zap.Error(err))
		return err
	}
	glog.Info("service registered")

	if n.lifecycleHook != nil {
		glog.Info("trigger lifecycle hook", zap.String("hook", "AfterStart"))
		n.lifecycleHook.AfterStart(n)
	}
	return nil
}

// Stop gracefully stops the node and all components.
func (n *Node) Stop(ctx context.Context) error {
	glog.Info("node stop begin", zap.String("node_id", n.GetID()))

	if n.lifecycleHook != nil {
		glog.Info("trigger lifecycle hook", zap.String("hook", "BeforeStop"))
		n.lifecycleHook.BeforeStop(n)
	}

	if err := n.deregisterService(); err != nil {
		glog.Error("deregister service failed", zap.Error(err))
	}

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("stop components failed", zap.Error(stopErr))
	} else {
		glog.Info("components stopped")
	}

	if n.lifecycleHook != nil {
		glog.Info("trigger lifecycle hook", zap.String("hook", "AfterStop"))
		n.lifecycleHook.AfterStop(n, stopErr)
	}
	glog.Info("node stop end", zap.Error(stopErr))
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
