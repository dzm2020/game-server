package consul

import (
	"context"
	"errors"
	"fmt"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// Discoverer wraps discovery-side capabilities.
type Discoverer struct {
	client   *api.Client
	cacheMu  sync.RWMutex
	cache    map[string][]ServiceInstance
	names    []string
	watchMu  sync.Mutex
	watching bool
	waitTime time.Duration
	workers  map[string]context.CancelFunc
	subMu    sync.RWMutex
	watchers map[string]map[string]gen.ServiceChangeHandler
	watchSeq uint64
}

// newDiscoverer
//
//	@Description: 使用 Consul 客户端和日志器创建 Discoverer
//	@param client
//	@param logger
//	@return *Discoverer
func newDiscoverer(client *api.Client) *Discoverer {
	return &Discoverer{
		client:   client,
		cache:    make(map[string][]ServiceInstance),
		waitTime: 30 * time.Second,
		workers:  make(map[string]context.CancelFunc),
		watchers: make(map[string]map[string]gen.ServiceChangeHandler),
	}
}

// NewDiscoverer creates a standalone discover-side client.
func NewDiscoverer(options *gen.ConsulOptions) (*Discoverer, error) {
	client, err := newConsulClient(options)
	if err != nil {
		return nil, err
	}
	glog.Info("consul discoverer initialized")
	return newDiscoverer(client), nil
}

// Discover returns service instances from Consul health API.
func (d *Discoverer) Discover(serviceName string) ([]ServiceInstance, error) {
	if serviceName == "" {
		return nil, errors.New("service name is required")
	}
	if instances, ok := d.getCache(serviceName); ok {
		return instances, nil
	}
	err := fmt.Errorf("discover cache miss for %q, start sync first", serviceName)
	glog.Error("discover service failed", zap.String("service_name", serviceName), zap.Error(err))
	return nil, err
}

// queryServiceEntries
//
//	@Description: 从 Consul 拉取指定服务的健康实例条目
//	@receiver d
//	@param serviceName
//	@param q
//	@return []*api.ServiceEntry
//	@return *api.QueryMeta
//	@return error
func (d *Discoverer) queryServiceEntries(serviceName string, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
	if serviceName == "" {
		return nil, nil, errors.New("service name is required")
	}

	entries, meta, err := d.client.Health().Service(serviceName, "", true, q)
	if err != nil {
		return nil, nil, fmt.Errorf("discover service %q: %w", serviceName, err)
	}
	return entries, meta, nil
}

// instancesFromEntries
//
//	@Description: 将 Consul ServiceEntry 列表转换为统一的 ServiceInstance 列表
//	@param entries
//	@return []ServiceInstance
func instancesFromEntries(entries []*api.ServiceEntry) []ServiceInstance {
	instances := make([]ServiceInstance, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.Service == nil {
			continue
		}

		extAddress := ""
		if entry.Service.Meta != nil {
			if v := entry.Service.Meta["ext_address"]; v != "" {
				extAddress = v
			}

		}
		instances = append(instances, ServiceInstance{
			ID:         entry.Service.ID,
			Name:       entry.Service.Service,
			ExtAddress: extAddress,
			RpcAddress: net.JoinHostPort(entry.Service.Address, strconv.Itoa(entry.Service.Port)),
			Tags:       entry.Service.Tags,
			Meta:       entry.Service.Meta,
		})
	}
	return instances
}

// ListServices returns all registered service names.
func (d *Discoverer) ListServices() ([]string, error) {
	d.cacheMu.RLock()
	defer d.cacheMu.RUnlock()
	names := make([]string, len(d.names))
	copy(names, d.names)
	return names, nil
}

// DiscoverAll returns discovered instances grouped by service name.
func (d *Discoverer) DiscoverAll() (map[string][]ServiceInstance, error) {
	names, err := d.ListServices()
	if err != nil {
		return nil, err
	}

	all := make(map[string][]ServiceInstance, len(names))
	for _, name := range names {
		instances, discoverErr := d.Discover(name)
		if discoverErr != nil {
			// 高可用策略: 单个服务缓存未就绪或读取失败时跳过，返回当前可用数据。
			glog.Warn("discover all partial failure", zap.String("service_name", name), zap.Error(discoverErr))
			continue
		}
		all[name] = instances
	}
	return all, nil
}

// Watch registers a callback for service instance changes.
func (d *Discoverer) Watch(serviceName string, onChange gen.ServiceChangeHandler) (string, error) {
	if serviceName == "" {
		return "", errors.New("service name is required")
	}
	if onChange == nil {
		return "", errors.New("onChange callback is required")
	}

	watchID := strconv.FormatUint(atomic.AddUint64(&d.watchSeq, 1), 10)
	d.subMu.Lock()
	if d.watchers[serviceName] == nil {
		d.watchers[serviceName] = make(map[string]gen.ServiceChangeHandler)
	}
	d.watchers[serviceName][watchID] = onChange
	d.subMu.Unlock()
	glog.Info("service watch registered", zap.String("service_name", serviceName), zap.String("watch_id", watchID))
	return watchID, nil
}

// Unwatch removes one registered callback by serviceName and watchID.
func (d *Discoverer) Unwatch(serviceName, watchID string) {
	if serviceName == "" || watchID == "" {
		return
	}
	d.subMu.Lock()
	watches, ok := d.watchers[serviceName]
	if ok {
		delete(watches, watchID)
		if len(watches) == 0 {
			delete(d.watchers, serviceName)
		}
	}
	d.subMu.Unlock()
	glog.Info("service watch removed", zap.String("service_name", serviceName), zap.String("watch_id", watchID))
}

// Run starts one background blocking sync loop.
func (d *Discoverer) Run(ctx context.Context) error {
	d.watchMu.Lock()
	if d.watching {
		d.watchMu.Unlock()
		return nil
	}
	d.watching = true
	d.watchMu.Unlock()

	grs.SafeGo(func() {
		defer func() {
			d.watchMu.Lock()
			d.watching = false
			d.watchMu.Unlock()
		}()
		if err := d.SyncBlocking(ctx); err != nil {
			glog.Error("discover sync stopped", zap.Error(err))
		}
	})
	glog.Info("discover sync started", zap.Duration("wait", 30*time.Second))
	return nil
}

// SyncBlocking blocks and long-polls all services, then refreshes local cache.
func (d *Discoverer) SyncBlocking(ctx context.Context) error {
	waitTime := 30 * time.Second
	var waitIndex uint64

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		query := &api.QueryOptions{
			WaitIndex: waitIndex,
			WaitTime:  waitTime,
		}

		services, meta, err := d.client.Catalog().Services(query.WithContext(ctx))
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			glog.Warn("watch services catalog failed", zap.Error(err))
			return fmt.Errorf("watch all services: %w", err)
		}
		if meta != nil && meta.LastIndex > 0 {
			waitIndex = meta.LastIndex
		}

		names := make([]string, 0, len(services))
		for name := range services {
			names = append(names, name)
		}
		d.setServiceNames(names)
		d.reconcileServiceWorkers(ctx, names, waitTime)
	}
}

// getCache
//
//	@Description: 从本地缓存读取指定服务实例，并返回副本
//	@receiver d
//	@param serviceName
//	@return []ServiceInstance
//	@return bool
func (d *Discoverer) getCache(serviceName string) ([]ServiceInstance, bool) {
	d.cacheMu.RLock()
	key := serviceName
	instances, ok := d.cache[key]
	d.cacheMu.RUnlock()
	if !ok {
		return nil, false
	}
	return cloneInstances(instances), true
}

// setServiceCache
//
//	@Description: 更新服务缓存，并在发生变更时触发监听回调
//	@receiver d
//	@param serviceName
//	@param passing
func (d *Discoverer) setServiceCache(serviceName string, passing []ServiceInstance) {
	var previous []ServiceInstance
	var hadPrevious bool
	d.cacheMu.Lock()
	previous, hadPrevious = d.cache[serviceName]
	d.cache[serviceName] = cloneInstances(passing)
	d.cacheMu.Unlock()

	added, updated, removed, changed := diffInstances(previous, passing)
	topology := &gen.Topology{
		All:     passing,
		Added:   added,
		Updated: updated,
		Removed: removed,
	}
	if !hadPrevious || changed {
		d.notifyWatchers(serviceName, topology)
	}
}

// removeServiceCache
//
//	@Description: 删除指定服务缓存，并在有历史数据时触发移除通知
//	@receiver d
//	@param serviceName
func (d *Discoverer) removeServiceCache(serviceName string) {
	var previous []ServiceInstance
	var hadPrevious bool
	d.cacheMu.Lock()
	previous, hadPrevious = d.cache[serviceName]
	delete(d.cache, serviceName)
	d.cacheMu.Unlock()
	if hadPrevious && len(previous) > 0 {
		topology := &gen.Topology{
			All:     nil,
			Added:   nil,
			Updated: nil,
			Removed: previous,
		}
		d.notifyWatchers(serviceName, topology)
	}
}

// setServiceNames
//
//	@Description: 更新当前服务名列表快照
//	@receiver d
//	@param names
func (d *Discoverer) setServiceNames(names []string) {
	d.cacheMu.Lock()
	d.names = make([]string, len(names))
	copy(d.names, names)
	d.cacheMu.Unlock()
}

// reconcileServiceWorkers
//
//	@Description: 按最新服务名列表增减对应的同步 worker
//	@receiver d
//	@param ctx
//	@param names
//	@param waitTime
func (d *Discoverer) reconcileServiceWorkers(ctx context.Context, names []string, waitTime time.Duration) {
	next := make(map[string]struct{}, len(names))
	for _, name := range names {
		next[name] = struct{}{}
	}

	toStop := make(map[string]context.CancelFunc)

	d.watchMu.Lock()
	for name, cancel := range d.workers {
		if _, ok := next[name]; !ok {
			toStop[name] = cancel
			delete(d.workers, name)
		}
	}
	for _, name := range names {
		if _, ok := d.workers[name]; !ok {
			svcName := name
			serviceCtx, cancel := context.WithCancel(ctx)
			d.workers[svcName] = cancel
			grs.SafeGo(func() {
				d.serviceSyncLoop(serviceCtx, svcName, waitTime)
			})
		}
	}
	d.watchMu.Unlock()

	for name, cancel := range toStop {
		cancel()
		d.removeServiceCache(name)
	}
}

// serviceSyncLoop
//
//	@Description: 单服务阻塞同步循环，持续拉取健康实例并更新缓存
//	@receiver d
//	@param ctx
//	@param serviceName
//	@param waitTime
func (d *Discoverer) serviceSyncLoop(ctx context.Context, serviceName string, waitTime time.Duration) {
	var waitIndex uint64
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		query := &api.QueryOptions{
			WaitIndex: waitIndex,
			WaitTime:  waitTime,
		}

		entries, meta, err := d.queryServiceEntries(serviceName, query.WithContext(ctx))
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			glog.Warn("service sync failed", zap.String("service_name", serviceName), zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		if meta != nil && meta.LastIndex > 0 {
			waitIndex = meta.LastIndex
		}

		passing := instancesFromEntries(entries)
		d.setServiceCache(serviceName, passing)
	}
}

// cloneInstances
//
//	@Description: 复制实例切片，避免外部修改内部缓存数据
//	@param instances
//	@return []ServiceInstance
func cloneInstances(instances []ServiceInstance) []ServiceInstance {
	if len(instances) == 0 {
		return []ServiceInstance{}
	}
	cloned := make([]ServiceInstance, len(instances))
	copy(cloned, instances)
	return cloned
}

// notifyWatchers
//
//	@Description: 通知指定服务的所有变更监听回调
//	@receiver d
//	@param serviceName
//	@param all
//	@param added
//	@param updated
//	@param removed
func (d *Discoverer) notifyWatchers(serviceName string, topology *gen.Topology) {
	d.subMu.RLock()
	watches := d.watchers[serviceName]
	if len(watches) == 0 {
		d.subMu.RUnlock()
		return
	}
	callbacks := make([]gen.ServiceChangeHandler, 0, len(watches))
	for _, callback := range watches {
		callbacks = append(callbacks, callback)
	}
	d.subMu.RUnlock()

	grs.SafeGo(func() {
		for _, callback := range callbacks {
			callback(topology)
		}
	})
}

// diffInstances
//
//	@Description: 对比服务实例快照，计算新增/更新/删除结果
//	@param prev
//	@param curr
//	@return added
//	@return updated
//	@return removed
//	@return changed
func diffInstances(prev []ServiceInstance, curr []ServiceInstance) (added []ServiceInstance, updated []ServiceInstance, removed []ServiceInstance, changed bool) {
	prevMap := make(map[string]ServiceInstance, len(prev))
	currMap := make(map[string]ServiceInstance, len(curr))
	for _, ins := range prev {
		prevMap[ins.ID] = ins
	}
	for _, ins := range curr {
		currMap[ins.ID] = ins
	}

	for id, now := range currMap {
		before, ok := prevMap[id]
		if !ok {
			added = append(added, now)
			continue
		}
		if !instanceEqual(before, now) {
			updated = append(updated, now)
		}
	}
	for id, before := range prevMap {
		if _, ok := currMap[id]; !ok {
			removed = append(removed, before)
		}
	}
	changed = len(added) > 0 || len(updated) > 0 || len(removed) > 0
	return
}

// instanceEqual
//
//	@Description: 判断两个服务实例是否语义一致
//	@param a
//	@param b
//	@return bool
func instanceEqual(a ServiceInstance, b ServiceInstance) bool {
	return a.ID == b.ID &&
		a.Name == b.Name &&
		a.ExtAddress == b.ExtAddress &&
		a.RpcAddress == b.RpcAddress &&
		reflect.DeepEqual(a.Tags, b.Tags) &&
		reflect.DeepEqual(a.Meta, b.Meta)
}
