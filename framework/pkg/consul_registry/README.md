# consul_registry

`consul_registry` 是一个面向 Go 的轻量 Consul 服务注册与发现库，核心目标是：

- 注册端简单可控（支持 TTL 保活）
- 发现端低压高可用（阻塞同步 + 本地缓存）
- 可观测与可扩展（可注入日志、可配置同步错误处理）

## 功能特性

- 服务注册 / 注销
- TTL 健康检查与自动心跳
- 手动 TTL 状态更新（`pass/warn/fail`）
- 基于 Consul blocking query 的后台同步
- 发现接口默认走本地缓存（仅健康实例）
- 服务变更订阅（`all/added/updated/removed`）
- 可配置同步错误处理（`OnSyncError`）
- 内部协程统一 `recover`，panic 自动记录日志

## 安装

```bash
go mod tidy
```

## 快速开始

```go
package main

import (
	"context"
	"log"
	"time"

	consulregistry "consul_registry"
)

func main() {
	registry, err := consulregistry.New(consulregistry.Config{
		Address: "127.0.0.1:8500",
		Scheme:  "http",
		Logger:  consulregistry.StdLoggerAdapter{L: log.Default()},
	})
	if err != nil {
		panic(err)
	}

	// 1) 注册服务（启用 TTL 后会自动启动心跳协程）
	err = registry.Registrar.Register(consulregistry.ServiceRegistration{
		ID:                "user-api-1",
		Name:              "user-api",
		Address:           "127.0.0.1",
		Port:              8080,
		TTL:               15 * time.Second,
		DeregisterAfter:   1 * time.Minute,
		HeartbeatInterval: 5 * time.Second,
		HeartbeatNote:     "user-api healthy",
	})
	if err != nil {
		panic(err)
	}
	defer registry.Registrar.Deregister("user-api-1")

	// 2) 启动发现端后台同步（推荐）
	err = registry.Discoverer.StartSync(context.Background(), consulregistry.WatchOptions{
		Interval: 30 * time.Second,
		OnSyncError: func(err error) {
			// 生产环境可接入告警系统
			log.Printf("discover sync stopped: %v", err)
		},
	})
	if err != nil {
		panic(err)
	}

	// 3) 从本地缓存发现实例
	instances, err := registry.Discoverer.DiscoverDefault("user-api")
	if err != nil {
		panic(err)
	}
	log.Printf("instances=%+v", instances)
}
```

## 核心 API

### 初始化

- `New(cfg Config) (*Registry, error)`：创建聚合对象（`Registry.Registrar` + `Registry.Discoverer`）
- `NewRegistrar(cfg Config) (*Registrar, error)`：仅创建注册端
- `NewDiscoverer(cfg Config) (*Discoverer, error)`：仅创建发现端

### 注册端（Registrar）

- `Register(reg ServiceRegistration) error`
  - 注册服务实例
  - 当 `TTL > 0` 时自动创建 TTL Check 并启动心跳
  - 当 `DeregisterAfter == 0` 且启用 TTL 时，默认设置为 `1m`
- `Deregister(serviceID string) error`
  - 先调用 Consul 注销，成功后停止本地心跳协程
- `TTLPass(serviceID, note string) error`
- `TTLWarn(serviceID, note string) error`
- `TTLFail(serviceID, note string) error`
  - 更新内存中的 TTL 状态，心跳协程按最新状态上报

### 发现端（Discoverer）

- `StartSync(ctx context.Context, opts WatchOptions) error`
  - 异步启动全量同步（内部阻塞查询）
  - 同步出错时：
    - 若配置 `OnSyncError`，回调该错误
    - 否则记录错误日志
- `SyncBlocking(ctx context.Context, opts WatchOptions) error`
  - 阻塞模式同步（通常由 `StartSync` 调用）
- `Discover(serviceName string) ([]ServiceInstance, error)`
  - 从本地缓存读取单个服务实例（仅健康实例）
- `DiscoverDefault(serviceName string) ([]ServiceInstance, error)`
  - 等价于 `Discover`
- `ListServices() ([]string, error)`
  - 返回当前缓存中的服务名列表
- `DiscoverAll() (map[string][]ServiceInstance, error)`
  - 返回缓存中的全量服务实例
  - 采用高可用策略：单个服务读取失败时跳过，返回当前可用数据

### 订阅

- `type ServiceChangeHandler func(all, added, updated, removed []ServiceInstance)`
- `Watch(serviceName string, onChange ServiceChangeHandler) (watchID string, err error)`
- `Unwatch(serviceName, watchID string)`

## 配置说明

### Config

- `Address`：Consul 地址，例如 `127.0.0.1:8500`
- `Scheme`：`http` / `https`
- `Token`：ACL Token（可选）
- `Datacenter`：数据中心（可选）
- `Logger`：自定义日志实现（可选，默认静默）

### WatchOptions

- `Interval`：blocking query 的等待时间
- `OnSyncError`：后台同步错误回调（可选）

## 日志

库定义了统一日志接口：

```go
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}
```

默认提供 `StdLoggerAdapter` 适配标准库 `log.Logger`。

## 生产建议

- 启动后尽早调用 `StartSync`，避免发现缓存未命中
- 为 `OnSyncError` 接入告警（如 Sentry/Prometheus Alert）
- `Watch` 回调中避免阻塞操作；重任务建议投递到业务队列
- 为 `DeregisterAfter` 设定合理值，避免异常实例长期滞留

## 本地验证

先启动 Consul（开发模式）：

```bash
consul agent -dev
```

然后执行测试：

```bash
go test ./...
```
