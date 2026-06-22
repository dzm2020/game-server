package node

import (
	"context"
	compactor "game-server/framework/actor"
	"game-server/framework/cluster"
	"game-server/framework/gen"
	"game-server/framework/pkg/component"
	"game-server/framework/pkg/glog"
	"game-server/framework/registry"

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

func (n *Node) AddComponents(comps ...component.IComponent) {
	n.components = append(n.components, comps...)
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
		zap.String("node_id", n.GetId()),
		zap.Int("component_count", len(n.components)),
	)

	n.options.Behavior.OnBeforeStart(n)

	for idx, comp := range n.components {
		if err := n.IManager.AddComponent(comp); err != nil {
			glog.Error("register component failed",
				zap.Int("index", idx),
				zap.Error(err),
			)
			return err
		}
	}
	if err = n.IManager.Start(ctx); err != nil {
		glog.Error("start components failed", zap.Error(err))
		return err
	}

	n.options.Behavior.OnAfterStart(n)

	glog.Info("node startup complete", zap.String("node_id", n.GetId()))
	return nil
}

// Stop gracefully stops the node and all components.
func (n *Node) Stop(ctx context.Context) error {
	glog.Info("node stop begin", zap.String("node_id", n.GetId()))

	n.options.Behavior.OnBeforeStop(n)

	stopErr := n.IManager.Stop(ctx)
	if stopErr != nil {
		glog.Error("stop components failed", zap.Error(stopErr))
	}

	n.options.Behavior.OnAfterStop(n, stopErr)

	glog.Info("node stop end", zap.Error(stopErr))
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
