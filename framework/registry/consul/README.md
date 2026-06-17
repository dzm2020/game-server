# consul_registry

`consul_registry` 是一个基于 Consul API 的注册发现实现，当前对外接口以 `Registry` 聚合对象为主，包含：

- 注册能力（Register / Deregister / 健康状态控制）
- 发现能力（Run + Discover / DiscoverAll / Watch）
- 基于本地缓存的读路径（避免每次查询都直连 Consul）

## 当前实现特性

- `Registry.Register` 直接接收 `define.ServiceInstance`
- TTL 心跳由注册端自动维护（当 `Config.TTL > 0`）
- 发现端通过 `Run(ctx)` 启动后台阻塞同步
- `Watch` 提供 `all / added / updated / removed` 四类差异通知
- 健康状态通过 `SetHealthState(serviceID, state)` 统一设置（状态类型来自 `define.HealthState`）
- 日志统一使用 `game-server/framework/pkg/glog`（结构化字段）

## 配置

`Config` 字段：

- `Address`：Consul 地址，如 `127.0.0.1:8500`
- `Scheme`：`http` / `https`
- `Token`：ACL Token（可选）
- `Datacenter`：数据中心（可选）
- `TTL`：启用注册 TTL 检查（`<=0` 表示不开启）
- `DeregisterAfter`：TTL 进入 critical 后自动清理时间（默认 `1m`）
- `HeartbeatInterval`：TTL 心跳间隔（默认 `TTL/2`，兜底 `1s`）
- `HeartbeatNote`：TTL 心跳备注

## 快速开始

```go
package main

import (
	"context"
	"time"

	consulregistry "consul_registry"
	registrydefine "game-server/framework/registry/define"
)

func main() {
	reg, err := consulregistry.New(consulregistry.Config{
		Address:           "127.0.0.1:8500",
		Scheme:            "http",
		TTL:               15 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		HeartbeatNote:     "user-api healthy",
		DeregisterAfter:   time.Minute,
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = reg.Run(ctx); err != nil {
		panic(err)
	}

	err = reg.Register(consulregistry.ServiceInstance{
		ID:      "user-api-1",
		Name:    "user-api",
		Address: "127.0.0.1",
		Port:    8080,
	})
	if err != nil {
		panic(err)
	}
	defer reg.Deregister("user-api-1")

// 可选：主动更新健康状态（例如收到业务探活结果后）
// _ = reg.SetHealthState("user-api-1", registrydefine.HealthStatePassing)

	instances, err := reg.Discover("user-api")
	if err != nil {
		panic(err)
	}
	_ = instances
}
```

## API 摘要

- `New(cfg Config) (*Registry, error)`
- `(*Registry).Run(ctx context.Context) error`
- `(*Registry).Register(reg ServiceInstance) error`
- `(*Registry).Deregister(serviceID string) error`
- `(*Registry).SetHealthState(serviceID string, state define.HealthState) error`
- `(*Registry).Discover(serviceName string) ([]ServiceInstance, error)`
- `(*Registry).DiscoverAll() (map[string][]ServiceInstance, error)`
- `(*Registry).ListServices() ([]string, error)`
- `(*Registry).Watch(serviceName string, onChange define.ServiceChangeHandler) (string, error)`
- `(*Registry).Unwatch(serviceName, watchID string)`
- `(*Registry).Shutdown()`（当前为空实现，建议通过取消 `Run` 的 `ctx` 完成优雅停止）

## 测试说明

- 单元测试：纯内存逻辑（缓存、diff、watch）
- 集成测试：依赖可访问 Consul；不可访问时自动 `Skip`
- benchmark：需要可访问 Consul，主要覆盖注册与发现缓存读取路径

运行方式：

```bash
go test ./...
go test -bench . -run ^$
```
