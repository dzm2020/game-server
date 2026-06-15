package consulregistry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
)

// ServiceChangeHandler handles service instance diff events.
type ServiceChangeHandler func(all []ServiceInstance, added []ServiceInstance, updated []ServiceInstance, removed []ServiceInstance)

// Discoverer wraps discovery-side capabilities.
type Discoverer struct {
	client   *api.Client
	logger   Logger
	cacheMu  sync.RWMutex
	cache    map[string][]ServiceInstance
	names    []string
	watchMu  sync.Mutex
	watching bool
	waitTime time.Duration
	workers  map[string]context.CancelFunc
	subMu    sync.RWMutex
	watchers map[string]map[string]ServiceChangeHandler
	watchSeq uint64
}

// newDiscoverer
//
//	@Description: 使用 Consul 客户端和日志器创建 Discoverer
//	@param client
//	@param logger
//	@return *Discoverer
func newDiscoverer(client *api.Client, logger Logger) *Discoverer {
	return &Discoverer{
		client:   client,
		logger:   ensureLogger(logger),
		cache:    make(map[string][]ServiceInstance),
		waitTime: 30 * time.Second,
		workers:  make(map[string]context.CancelFunc),
		watchers: make(map[string]map[string]ServiceChangeHandler),
	}
}

// NewDiscoverer creates a standalone discover-side client.
func NewDiscoverer(cfg Config) (*Discoverer, error) {
	logger := ensureLogger(cfg.Logger)
	client, err := newConsulClient(cfg, logger)
	if err != nil {
		return nil, err
	}
	logger.Infof("独立 Discoverer 初始化完成")
	return newDiscoverer(client, logger), nil
}

// DiscoverDefault returns healthy instances.
func (d *Discoverer) DiscoverDefault(serviceName string) ([]ServiceInstance, error) {
	return d.Discover(serviceName)
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
	d.logger.Errorf("服务发现失败: %v", err)
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
		address := entry.Service.Address
		if address == "" && entry.Node != nil {
			address = entry.Node.Address
		}
		instances = append(instances, ServiceInstance{
			ID:      entry.Service.ID,
			Name:    entry.Service.Service,
			Address: address,
			Port:    entry.Service.Port,
			Tags:    entry.Service.Tags,
			Meta:    entry.Service.Meta,
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
			d.logger.Warnf("批量发现局部失败, service=%s err=%v", name, discoverErr)
			continue
		}
		all[name] = instances
	}
	return all, nil
}

// Watch registers a callback for service instance changes.
func (d *Discoverer) Watch(serviceName string, onChange ServiceChangeHandler) (string, error) {
	if serviceName == "" {
		return "", errors.New("service name is required")
	}
	if onChange == nil {
		return "", errors.New("onChange callback is required")
	}

	watchID := strconv.FormatUint(atomic.AddUint64(&d.watchSeq, 1), 10)
	d.subMu.Lock()
	if d.watchers[serviceName] == nil {
		d.watchers[serviceName] = make(map[string]ServiceChangeHandler)
	}
	d.watchers[serviceName][watchID] = onChange
	d.subMu.Unlock()
	d.logger.Infof("已注册服务监听, service=%s watchID=%s", serviceName, watchID)
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
	d.logger.Infof("已移除服务监听, service=%s watchID=%s", serviceName, watchID)
}

// StartSync starts one background blocking sync loop.
func (d *Discoverer) StartSync(ctx context.Context, opts WatchOptions) error {
	d.watchMu.Lock()
	if d.watching {
		d.watchMu.Unlock()
		return nil
	}
	d.watching = true
	d.watchMu.Unlock()

	safeGo(d.logger, "discoverer.startSync", func() {
		defer func() {
			d.watchMu.Lock()
			d.watching = false
			d.watchMu.Unlock()
		}()
		if err := d.SyncBlocking(ctx, WatchOptions{Interval: 30 * time.Second}); err != nil {
			if opts.OnSyncError != nil {
				opts.OnSyncError(err)
				return
			}
			d.logger.Errorf("发现同步已停止: %v", err)
		}
	})
	d.logger.Infof("发现同步已启动, wait=%s", 30*time.Second)
	return nil
}

// SyncBlocking blocks and long-polls all services, then refreshes local cache.
func (d *Discoverer) SyncBlocking(ctx context.Context, opts WatchOptions) error {
	waitTime := opts.Interval
	if waitTime <= 0 {
		waitTime = 30 * time.Second
	}
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
			d.logger.Warnf("监听服务目录失败: %v", err)
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
	if !hadPrevious || changed {
		d.notifyWatchers(serviceName, passing, added, updated, removed)
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
		d.notifyWatchers(serviceName, []ServiceInstance{}, []ServiceInstance{}, []ServiceInstance{}, previous)
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
			safeGo(d.logger, goName("discoverer.serviceSyncLoop", svcName), func() {
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
			d.logger.Warnf("服务实例同步失败, service=%s err=%v", serviceName, err)
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
func (d *Discoverer) notifyWatchers(serviceName string, all []ServiceInstance, added []ServiceInstance, updated []ServiceInstance, removed []ServiceInstance) {
	d.subMu.RLock()
	watches := d.watchers[serviceName]
	if len(watches) == 0 {
		d.subMu.RUnlock()
		return
	}
	callbacks := make([]ServiceChangeHandler, 0, len(watches))
	for _, callback := range watches {
		callbacks = append(callbacks, callback)
	}
	d.subMu.RUnlock()

	safeGo(d.logger, goName("discoverer.notifyWatchers", serviceName), func() {
		for _, callback := range callbacks {
			callback(all, added, updated, removed)
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
		a.Address == b.Address &&
		a.Port == b.Port &&
		reflect.DeepEqual(a.Tags, b.Tags) &&
		reflect.DeepEqual(a.Meta, b.Meta)
}
