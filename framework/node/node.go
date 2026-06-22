package node

import (
	"context"
	compactor "game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry"
	"reflect"

	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
)

// New creates a node instance.
func New(options gen.NodeOptions) *Node {
	node := &Node{
		options:  options,
		IManager: component.NewComponentsMgr(),
	}
	bootstrap(node, &options)
	return node
}

type Node struct {
	options gen.NodeOptions
	component.IManager
	components []component.IComponent
}

func (n *Node) GetId() string {
	return n.options.ID
}

func (n *Node) GetExtAddress() string {
	return n.options.ExtAddress
}

func (n *Node) GetRpcAddress() string {
	return n.options.RpcAddress
}

func (n *Node) GetName() string {
	return n.options.Name
}

func (n *Node) GetOptions() *gen.NodeOptions {
	return &n.options
}

func (n *Node) AddComponents(comps ...component.IComponent) {
	n.components = append(n.components, comps...)
}

func (n *Node) Startup() (err error) {
	if err = n.Start(context.Background()); err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGKILL, syscall.SIGTERM)
	glog.Info("节点等待终止信号", zap.Strings("signals", []string{"SIGINT", "SIGQUIT", "SIGKILL", "SIGTERM"}))
	sig := <-sigChan
	glog.Info("节点停止运行", zap.String("signal", sig.String()))
	return n.Stop(context.Background())
}

func (n *Node) Start(ctx context.Context) (err error) {
	glog.Info("节点启动", zap.Int("component_count", len(n.components)))

	n.options.Behavior.OnBeforeStart(n)

	for _, comp := range n.components {
		if err := n.IManager.AddComponent(comp); err != nil {
			glog.Error("节点添加组件失败", zap.Error(err), zap.String("typ", reflect.TypeOf(comp).String()))
			return err
		}
	}
	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("节点启动组件失败", zap.Error(err), zap.Error(err))
		return err
	}

	n.options.Behavior.OnAfterStart(n)

	glog.Info("节点启动完成")
	return nil
}

// Stop gracefully stops the node and all components.
func (n *Node) Stop(ctx context.Context) error {
	glog.Info("节点开始停止")

	n.options.Behavior.OnBeforeStop(n)

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("节点组件停止错误", zap.Error(stopErr))
	}

	n.options.Behavior.OnAfterStop(n, stopErr)

	glog.Info("节点完成停止", zap.Error(stopErr))
	_ = glog.Stop()
	return stopErr
}

func (n *Node) GetRegistry() gen.IRegistry {
	comp, err := gen.RequireComponent(n, (*registry.Component)(nil))
	if err != nil {
		return nil
	}
	if comp.IRegistry == nil {
		return nil
	}
	return comp.IRegistry
}

func (n *Node) GetSystem() gen.ISystem {
	comp, err := gen.RequireComponent(n, (*compactor.Component)(nil))
	if err != nil {
		return nil
	}
	if comp.ISystem == nil {
		return nil
	}
	return comp.ISystem
}

func (n *Node) GetCluster() gen.ICluster {
	comp, err := gen.RequireComponent(n, (*cluster.Component)(nil))
	if err != nil {
		return nil
	}
	if comp.ICluster == nil {
		return nil
	}
	return comp.ICluster
}
