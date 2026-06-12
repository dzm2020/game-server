package gateway

import (
	"game-server/internal/iface"
	"game-server/internal/profile"
	"time"
)

type Config struct {
	Enable         bool
	Address        string
	Port           int
	Path           string
	ActorName      string
	RouteNodeID    string
	RouteActorName string
	RequestTimeout time.Duration
	ReadLimit      int64
	PongWait       time.Duration
	PingPeriod     time.Duration
	WriteWait      time.Duration
	SendBuffer     int
}

func defaultConfig() Config {
	cfg := profile.DefaultGatewayConfig()
	return normalizeConfig(cfg, nil)
}

func normalizeConfig(cfg profile.GatewayConfig, self *iface.Member) Config {
	def := profile.DefaultGatewayConfig()
	if cfg.Address == "" {
		cfg.Address = def.Address
	}
	if cfg.Port <= 0 {
		cfg.Port = def.Port
	}
	if cfg.Path == "" {
		cfg.Path = def.Path
	}
	if cfg.ActorName == "" {
		cfg.ActorName = def.ActorName
	}
	if cfg.RouteActorName == "" {
		cfg.RouteActorName = def.RouteActorName
	}
	if cfg.RequestTimeoutMs <= 0 {
		cfg.RequestTimeoutMs = def.RequestTimeoutMs
	}
	if cfg.ReadLimit <= 0 {
		cfg.ReadLimit = def.ReadLimit
	}
	if cfg.PongWaitSec <= 0 {
		cfg.PongWaitSec = def.PongWaitSec
	}
	if cfg.PingPeriodSec <= 0 {
		cfg.PingPeriodSec = def.PingPeriodSec
	}
	if cfg.WriteWaitSec <= 0 {
		cfg.WriteWaitSec = def.WriteWaitSec
	}
	if cfg.SendBuffer <= 0 {
		cfg.SendBuffer = def.SendBuffer
	}
	if cfg.RouteNodeID == "" && self != nil {
		cfg.RouteNodeID = self.GetID()
	}

	return Config{
		Enable:         cfg.Enable,
		Address:        cfg.Address,
		Port:           cfg.Port,
		Path:           cfg.Path,
		ActorName:      cfg.ActorName,
		RouteNodeID:    cfg.RouteNodeID,
		RouteActorName: cfg.RouteActorName,
		RequestTimeout: time.Duration(cfg.RequestTimeoutMs) * time.Millisecond,
		ReadLimit:      cfg.ReadLimit,
		PongWait:       time.Duration(cfg.PongWaitSec) * time.Second,
		PingPeriod:     time.Duration(cfg.PingPeriodSec) * time.Second,
		WriteWait:      time.Duration(cfg.WriteWaitSec) * time.Second,
		SendBuffer:     cfg.SendBuffer,
	}
}
