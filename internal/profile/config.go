package profile

import (
	consulregistry "consul_registry"
	"game-server/internal/iface"
	"game-server/pkg/glog"
	queue "message_queue"
)

type NodeConfig struct {
	Base *NodeBaseConfig `mapstructure:"base" yaml:"base"`
}

type NodeBaseConfig struct {
	Logger  glog.Config           `mapstructure:"logger" yaml:"logger"`
	Consul  consulregistry.Config `mapstructure:"consul" yaml:"consul"`
	Nats    queue.Config          `mapstructure:"nats" yaml:"nats"`
	Gateway GatewayConfig         `mapstructure:"gateway" yaml:"gateway"`
	Self    *iface.Member         `mapstructure:"self" yaml:"self"`
}

type GatewayConfig struct {
	Enable           bool   `mapstructure:"enable" yaml:"enable"`
	Address          string `mapstructure:"address" yaml:"address"`
	Port             int    `mapstructure:"port" yaml:"port"`
	Path             string `mapstructure:"path" yaml:"path"`
	ActorName        string `mapstructure:"actorName" yaml:"actorName"`
	RouteNodeID      string `mapstructure:"routeNodeID" yaml:"routeNodeID"`
	RouteActorName   string `mapstructure:"routeActorName" yaml:"routeActorName"`
	RequestTimeoutMs int    `mapstructure:"requestTimeoutMs" yaml:"requestTimeoutMs"`
	ReadLimit        int64  `mapstructure:"readLimit" yaml:"readLimit"`
	PongWaitSec      int    `mapstructure:"pongWaitSec" yaml:"pongWaitSec"`
	PingPeriodSec    int    `mapstructure:"pingPeriodSec" yaml:"pingPeriodSec"`
	WriteWaitSec     int    `mapstructure:"writeWaitSec" yaml:"writeWaitSec"`
	SendBuffer       int    `mapstructure:"sendBuffer" yaml:"sendBuffer"`
}

func DefaultGatewayConfig() GatewayConfig {
	return GatewayConfig{
		Enable:           false,
		Address:          "0.0.0.0",
		Port:             19090,
		Path:             "/",
		ActorName:        "gateway-router",
		RouteActorName:   "echo",
		RequestTimeoutMs: 2000,
		ReadLimit:        1024 * 1024,
		PongWaitSec:      60,
		PingPeriodSec:    54,
		WriteWaitSec:     10,
		SendBuffer:       256,
	}
}

func DefaultNodeBaseConfig() *NodeBaseConfig {
	return &NodeBaseConfig{
		Logger: *glog.DefaultConfig(),
		Consul: consulregistry.Config{
			Address: "127.0.0.1:8500",
			Scheme:  "http",
		},
		Nats: queue.Config{
			URL: "nats://127.0.0.1:4222",
		},
		Gateway: DefaultGatewayConfig(),
		Self: &iface.Member{
			ID: "node-1",
		},
	}
}
