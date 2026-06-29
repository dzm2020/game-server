package consul

import (
	"context"
	"errors"
	"game-server/framework/gen"
	"game-server/framework/grs"
	"game-server/framework/pkg/glog"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duke-git/lancet/v2/maputil"
	"github.com/hashicorp/consul/api"
	"go.uber.org/zap"
)

// Discoverer wraps discovery-side capabilities.
type Discoverer struct {
	client *api.Client

	nameMu sync.RWMutex
	names  []string

	waitTime  time.Duration
	mainGroup *grs.Group
	watchSeq  atomic.Uint64
	logger    *zap.Logger

	cache       *maputil.ConcurrentMap[string, []ServiceInstance]
	instanceMap *maputil.ConcurrentMap[string, ServiceInstance] // instanceMap 以服务实例ID为索引，便于按ID快速查询实例信息。
	workers     *maputil.ConcurrentMap[string, context.CancelFunc]

	subMu    sync.RWMutex
	watchers map[string]map[string]gen.ServiceChangeHandler
}

// newDiscoverer
//
//	@Description: 使用 Consul 客户端和日志器创建 Discoverer
//	@param client
//	@param logger
//	@return *Discoverer
func newDiscoverer(client *api.Client, logger *zap.Logger) *Discoverer {
	return &Discoverer{
		client:      client,
		waitTime:    30 * time.Second,
		logger:      logger,
		cache:       maputil.NewConcurrentMap[string, []ServiceInstance](10),
		instanceMap: maputil.NewConcurrentMap[string, ServiceInstance](10),
		workers:     maputil.NewConcurrentMap[string, context.CancelFunc](10),
		watchers:    make(map[string]map[string]gen.ServiceChangeHandler),
	}
}

// Start
//
//	@Description: 运行服务发现协程
//	@receiver d
//	@param ctx
//	@return error
func (d *Discoverer) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d.mainGroup = grs.NewGroup(ctx)
	d.mainGroup.Go(func(ctx context.Context) {
		d.logger.Info("运行服务发现协程", zap.Duration("wait", 30*time.Second))
		if err := d.watchServices(ctx); err != nil {
			glog.Error("运行服务发现协程", glog.Err(err))
		}
		d.logger.Info("运行服务发现协程终止", zap.Duration("wait", 30*time.Second))
	})
	return nil
}

// SyncBlocking blocks and long-polls all services, then refreshes local cache.
func (d *Discoverer) watchServices(ctx context.Context) error {
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
			return err
		}
		if meta != nil && meta.LastIndex > 0 {
			waitIndex = meta.LastIndex
		}

		names := make([]string, 0, len(services))
		for name := range services {
			if name == "consul" {
				continue
			}
			names = append(names, name)
		}

		d.logger.Debug("服务列表", zap.Strings("services", names))
		d.nameMu.Lock()
		d.names = names
		d.nameMu.Unlock()
		//
		d.runServiceWorkers(ctx, names, waitTime)
	}
}

// runServiceWorkers
//
//	@Description: 按最新服务名列表增减对应的同步 worker
//	@receiver d
//	@param ctx
//	@param names
//	@param waitTime
func (d *Discoverer) runServiceWorkers(ctx context.Context, names []string, waitTime time.Duration) {
	next := make(map[string]struct{}, len(names))
	for _, name := range names {
		next[name] = struct{}{}
	}
	//  查找需要停止的worker
	toStop := make(map[string]context.CancelFunc)
	d.workers.Range(func(name string, cancel context.CancelFunc) bool {
		if cancel == nil {
			return true
		}
		if _, ok := next[name]; !ok {
			toStop[name] = cancel
		}
		return true
	})
	//  停止worker
	for name, cancel := range toStop {
		d.workers.Delete(name)
		cancel()
		d.removeServiceCache(name)
	}
	//  启动新worker
	for _, name := range names {
		if _, ok := d.workers.Get(name); ok {
			continue
		}
		d.workers.Set(name, nil)
		d.runWorker(name, waitTime)
	}

}
func (d *Discoverer) runWorker(svcName string, waitTime time.Duration) {
	d.mainGroup.Go(func(ctx context.Context) {
		d.logger.Info("启动服务实例发现协程", zap.String("service_name", svcName))

		serviceCtx, cancel := context.WithCancel(ctx)
		d.workers.Set(svcName, cancel)
		d.serviceSyncLoop(serviceCtx, svcName, waitTime)
		d.logger.Info("结束服务实例发现协程", zap.String("service_name", svcName))
	})
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
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				d.logger.Info("服务发现同步已停止", zap.String("service_name", serviceName))
				return
			}
			d.logger.Error("服务实例发现", glog.Component("registry.consul.discoverer"), zap.String("service_name", serviceName), glog.Err(err))
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

// Discover returns service instances from Consul health API.
func (d *Discoverer) Discover(serviceName string) []ServiceInstance {
	if serviceName == "" {
		return nil
	}
	instances, _ := d.getCache(serviceName)
	return instances
}

// queryServiceEntries
//
//	@Description: 从Consul拉取指定服务的健康实例条目
//	@receiver d
//	@param serviceName
//	@param q
//	@return []*api.ServiceEntry
//	@return *api.QueryMeta
//	@return error
func (d *Discoverer) queryServiceEntries(serviceName string, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
	if serviceName == "" {
		return nil, nil, nil
	}
	entries, meta, err := d.client.Health().Service(serviceName, "", true, q)
	if err != nil {
		// 关闭或超时取消，视为正常结束
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, err
		}
		d.logger.Error("Consul拉取指定服务的健康实例条目", glog.Component("registry.consul.discoverer"), zap.String("service_name", serviceName), glog.Err(err))
		return nil, nil, err
	}
	d.logger.Debug("Consul拉取指定服务的健康实例条目", zap.String("service_name", serviceName), zap.Int("count", len(entries)))
	return entries, meta, nil
}

// ListServices returns all registered service names.
func (d *Discoverer) ListServices() []string {
	d.nameMu.RLock()
	names := make([]string, len(d.names))
	copy(names, d.names)
	d.nameMu.RUnlock()
	return names
}

// DiscoverAll returns discovered instances grouped by service name.
func (d *Discoverer) DiscoverAll() map[string][]ServiceInstance {
	names := d.ListServices()
	all := make(map[string][]ServiceInstance, len(names))
	for _, name := range names {
		instances := d.Discover(name)
		all[name] = instances
	}
	return all
}

func (d *Discoverer) GetInstance(serverID string) (ServiceInstance, bool) {
	instances, ok := d.instanceMap.Get(serverID)
	return instances, ok
}

// Watch
//
//	@Description: 监控服务
//	@receiver d
//	@param serviceName
//	@param onChange
//	@return string
//	@return error
func (d *Discoverer) Watch(serviceName string, onChange gen.ServiceChangeHandler) (string, error) {
	if serviceName == "" {
		return "", nil
	}
	if onChange == nil {
		return "", nil
	}

	watchID := strconv.FormatUint(d.watchSeq.Add(1), 10)
	d.subMu.Lock()
	if d.watchers[serviceName] == nil {
		d.watchers[serviceName] = make(map[string]gen.ServiceChangeHandler)
	}
	d.watchers[serviceName][watchID] = onChange
	d.subMu.Unlock()
	d.logger.Info("监控服务", zap.String("service_name", serviceName), zap.String("watch_id", watchID))
	return watchID, nil
}

// Unwatch
//
//	@Description: 移除服务监控
//	@receiver d
//	@param serviceName
//	@param watchID
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
	d.logger.Info("移除服务监控", zap.String("service_name", serviceName), zap.String("watch_id", watchID))
}

func (d *Discoverer) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if d.mainGroup != nil {
		d.mainGroup.Cancel()
		if err := d.mainGroup.Wait(ctx); err != nil {
			return err
		}
	}

	return nil
}

// getCache
//
//	@Description: 从本地缓存读取指定服务实例，并返回副本
//	@receiver d
//	@param serviceName
//	@return []ServiceInstance
//	@return bool
func (d *Discoverer) getCache(serviceName string) ([]ServiceInstance, bool) {
	instances, ok := d.cache.Get(serviceName)
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

	previous, hadPrevious = d.cache.Get(serviceName)
	d.cache.Set(serviceName, cloneInstances(passing))
	d.rebuildInstanceMapLocked()

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

	previous, hadPrevious = d.cache.Get(serviceName)
	d.cache.Delete(serviceName)

	d.rebuildInstanceMapLocked()

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
		d.logger.Debug("触发服务的变更监听回调", zap.String("service_name", serviceName))
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

func (d *Discoverer) rebuildInstanceMapLocked() {
	instanceMap := maputil.NewConcurrentMap[string, ServiceInstance](10)
	d.cache.Range(func(_ string, instances []ServiceInstance) bool {
		for _, ins := range instances {
			if ins.ID == "" {
				continue
			}
			instanceMap.Set(ins.ID, ins)
		}
		return true
	})
	d.instanceMap = instanceMap
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
