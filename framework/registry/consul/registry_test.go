package consul

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"game-server/framework/gen"
	"game-server/framework/pkg/component"

	"github.com/hashicorp/consul/api"
)

func mustConsulOptionsFromEnv(t *testing.T) Options {
	t.Helper()
	options := DefaultOptions()
	if addr := os.Getenv("CONSUL_HTTP_ADDR"); addr != "" {
		options.Address = addr
	}
	if scheme := os.Getenv("CONSUL_HTTP_SCHEME"); scheme != "" {
		options.Scheme = scheme
	}
	options.TTL = 200 * time.Millisecond
	options.DeregisterAfter = time.Minute

	client, err := api.NewClient(toConsulConfig(options))
	if err != nil {
		t.Fatalf("create consul client failed: %v", err)
	}
	if _, err = client.Status().Leader(); err != nil {
		t.Fatalf("consul not reachable at %s://%s: %v", options.Scheme, options.Address, err)
	}
	return options
}

func mustConsulOptionsFromEnvB(b *testing.B) Options {
	b.Helper()
	options := DefaultOptions()
	if addr := os.Getenv("CONSUL_HTTP_ADDR"); addr != "" {
		options.Address = addr
	}
	if scheme := os.Getenv("CONSUL_HTTP_SCHEME"); scheme != "" {
		options.Scheme = scheme
	}
	options.TTL = 200 * time.Millisecond
	options.DeregisterAfter = time.Minute

	client, err := api.NewClient(toConsulConfig(options))
	if err != nil {
		b.Fatalf("create consul client failed: %v", err)
	}
	if _, err = client.Status().Leader(); err != nil {
		b.Fatalf("consul not reachable at %s://%s: %v", options.Scheme, options.Address, err)
	}
	return options
}

func startRegistryForTest(t *testing.T, opts Options) *Registry {
	t.Helper()
	r := New(opts)
	if r == nil {
		t.Fatal("registry should not be nil")
	}
	if err := r.Init(context.Background()); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	return r
}

func waitUntil(t *testing.T, timeout time.Duration, check func() bool, hint string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("wait timeout: %s", hint)
}

func TestOptionsExportedFunctions(t *testing.T) {
	defaults := DefaultOptions()
	if defaults.Address == "" || defaults.TTL <= 0 || defaults.DeregisterAfter <= 0 {
		t.Fatalf("invalid defaults: %+v", defaults)
	}
	normalized := NormalizeOptions(Options{})
	if normalized.Address != defaults.Address || normalized.TTL != defaults.TTL || normalized.DeregisterAfter != defaults.DeregisterAfter {
		t.Fatalf("normalize mismatch, got=%+v want=%+v", normalized, defaults)
	}
	if err := ValidateOptions(defaults); err != nil {
		t.Fatalf("validate defaults failed: %v", err)
	}
	if err := ValidateOptions(Options{}); err == nil {
		t.Fatal("validate should fail for empty options")
	}
}

func TestRegistryExportedMethodsHappyPath(t *testing.T) {
	r := startRegistryForTest(t, mustConsulOptionsFromEnv(t))
	if r.Status() != component.StateStarted {
		t.Fatalf("unexpected status: %s", r.Status())
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	service := ServiceInstance{
		ID:         "registry-test-" + suffix,
		Name:       "registry-test-service-" + suffix,
		RpcAddress: "127.0.0.1:9000",
		ExtAddress: "127.0.0.1:8000",
	}
	if err := r.Register(service); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Deregister()
	})

	if err := r.SetHealthState(gen.ServiceHealthStateWarning); err != nil {
		t.Fatalf("set health state failed: %v", err)
	}
	if got := r.keeper.getState(); got != gen.ServiceHealthStateWarning {
		t.Fatalf("unexpected keeper state: got=%s want=%s", got, gen.ServiceHealthStateWarning)
	}
	if err := r.SetHealthState(gen.ServiceHealthStatePassing); err != nil {
		t.Fatalf("reset health state failed: %v", err)
	}

	waitUntil(t, 8*time.Second, func() bool {
		discovered := r.Discover(service.Name)
		for _, item := range discovered {
			if item.ID == service.ID {
				return true
			}
		}
		return false
	}, "discover registered service")
	r.cacheMu.RLock()
	_, cached := r.discoverCache[service.Name]
	r.cacheMu.RUnlock()
	if !cached {
		t.Fatalf("discover cache missing for service %s", service.Name)
	}

	if err := r.Deregister(); err != nil {
		t.Fatalf("deregister failed: %v", err)
	}
	r.cacheMu.RLock()
	cacheSize := len(r.discoverCache)
	r.cacheMu.RUnlock()
	if cacheSize != 0 {
		t.Fatalf("discover cache should be cleared after deregister, got size=%d", cacheSize)
	}
	if err := r.Deregister(); err != gen.ErrServiceNotRegister {
		t.Fatalf("expected service-not-register after second deregister, got %v", err)
	}

	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestRegistryExportedMethodsErrorPaths(t *testing.T) {
	opts := mustConsulOptionsFromEnv(t)
	r := New(opts)

	if err := r.Register(ServiceInstance{}); err != gen.ErrComponentNotStart {
		t.Fatalf("expected component-not-start, got %v", err)
	}
	if r.Discover("game") != nil {
		t.Fatal("discover before start should return nil")
	}

	if err := r.Init(context.Background()); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop without register should be nil, got %v", err)
	}

	r = startRegistryForTest(t, opts)
	if err := r.SetHealthState(gen.ServiceHealthStatePassing); err != gen.ErrServiceNotRegister {
		t.Fatalf("expected service-not-register, got %v", err)
	}
	if err := r.Deregister(); err != gen.ErrServiceNotRegister {
		t.Fatalf("expected service-not-register, got %v", err)
	}
	if err := r.Register(ServiceInstance{ID: "", Name: "game"}); err != gen.ErrConsulInvalidServiceReg {
		t.Fatalf("expected invalid service reg, got %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	service := ServiceInstance{
		ID:         "registry-test-err-" + suffix,
		Name:       "registry-test-err-service-" + suffix,
		RpcAddress: "127.0.0.1:9001",
	}
	if err := r.Register(service); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Deregister()
	})
	if err := r.Register(service); err != gen.ErrServiceRegistered {
		t.Fatalf("expected service-registered, got %v", err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatalf("stop after register failed: %v", err)
	}
}

func TestRegistryDiscoverConcurrentStress(t *testing.T) {
	r := startRegistryForTest(t, mustConsulOptionsFromEnv(t))
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	service := ServiceInstance{
		ID:         "registry-stress-" + suffix,
		Name:       "registry-stress-service-" + suffix,
		RpcAddress: "127.0.0.1:9010",
		ExtAddress: "127.0.0.1:8010",
	}
	if err := r.Register(service); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Deregister()
		_ = r.Stop(context.Background())
	})

	waitUntil(t, 8*time.Second, func() bool {
		items := r.Discover(service.Name)
		for _, item := range items {
			if item.ID == service.ID {
				return true
			}
		}
		return false
	}, "discover service before concurrent stress")

	const workers = 32
	const loopsPerWorker = 80
	var (
		wg         sync.WaitGroup
		missCount  atomic.Int64
		errCount   atomic.Int64
	)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < loopsPerWorker; j++ {
				if err := r.SetHealthState(gen.ServiceHealthStatePassing); err != nil {
					errCount.Add(1)
				}
				items := r.Discover(service.Name)
				found := false
				for _, item := range items {
					if item.ID == service.ID {
						found = true
						break
					}
				}
				if !found {
					missCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if errCount.Load() != 0 {
		t.Fatalf("unexpected SetHealthState errors: %d", errCount.Load())
	}
	if missCount.Load() != 0 {
		t.Fatalf("discover missed registered service %d times", missCount.Load())
	}
}

func TestRegistryDiscoverConcurrentCacheRebuild(t *testing.T) {
	opts := mustConsulOptionsFromEnv(t)
	opts.DiscoverCacheTTL = 200 * time.Millisecond
	r := startRegistryForTest(t, opts)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	service := ServiceInstance{
		ID:         "registry-rebuild-" + suffix,
		Name:       "registry-rebuild-service-" + suffix,
		RpcAddress: "127.0.0.1:9011",
		ExtAddress: "127.0.0.1:8011",
	}
	if err := r.Register(service); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = r.Deregister()
		_ = r.Stop(context.Background())
	})

	waitUntil(t, 8*time.Second, func() bool {
		items := r.Discover(service.Name)
		for _, item := range items {
			if item.ID == service.ID {
				return true
			}
		}
		return false
	}, "discover service before cache rebuild test")

	// 主动失效缓存并发触发重建，检查不会死锁/返回空。
	r.cacheMu.Lock()
	r.discoverCache[service.Name] = discoverCacheEntry{
		expireAt:  time.Now().Add(-time.Second),
		instances: nil,
	}
	r.cacheMu.Unlock()

	const workers = 16
	var wg sync.WaitGroup
	var misses atomic.Int64
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			items := r.Discover(service.Name)
			found := false
			for _, item := range items {
				if item.ID == service.ID {
					found = true
					break
				}
			}
			if !found {
				misses.Add(1)
			}
		}()
	}
	wg.Wait()

	if misses.Load() != 0 {
		t.Fatalf("cache rebuild discover misses: %d", misses.Load())
	}
}

func BenchmarkRegistryDiscoverCacheHitParallel(b *testing.B) {
	opts := mustConsulOptionsFromEnvB(b)
	r := New(opts)
	if err := r.Init(context.Background()); err != nil {
		b.Fatalf("init failed: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		b.Fatalf("start failed: %v", err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	service := ServiceInstance{
		ID:         "registry-bench-hit-" + suffix,
		Name:       "registry-bench-hit-service-" + suffix,
		RpcAddress: "127.0.0.1:9012",
		ExtAddress: "127.0.0.1:8012",
	}
	if err := r.Register(service); err != nil {
		b.Fatalf("register failed: %v", err)
	}
	b.Cleanup(func() {
		_ = r.Deregister()
		_ = r.Stop(context.Background())
	})

	waitUntil(&testing.T{}, 8*time.Second, func() bool {
		items := r.Discover(service.Name)
		for _, item := range items {
			if item.ID == service.ID {
				return true
			}
		}
		return false
	}, "discover service before benchmark")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.Discover(service.Name)
		}
	})
}

func BenchmarkRegistryDiscoverCacheMiss(b *testing.B) {
	opts := mustConsulOptionsFromEnvB(b)
	opts.DiscoverCacheTTL = time.Nanosecond
	r := New(opts)
	if err := r.Init(context.Background()); err != nil {
		b.Fatalf("init failed: %v", err)
	}
	if err := r.Start(context.Background()); err != nil {
		b.Fatalf("start failed: %v", err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	service := ServiceInstance{
		ID:         "registry-bench-miss-" + suffix,
		Name:       "registry-bench-miss-service-" + suffix,
		RpcAddress: "127.0.0.1:9013",
		ExtAddress: "127.0.0.1:8013",
	}
	if err := r.Register(service); err != nil {
		b.Fatalf("register failed: %v", err)
	}
	b.Cleanup(func() {
		_ = r.Deregister()
		_ = r.Stop(context.Background())
	})

	waitUntil(&testing.T{}, 8*time.Second, func() bool {
		items := r.Discover(service.Name)
		for _, item := range items {
			if item.ID == service.ID {
				return true
			}
		}
		return false
	}, "discover service before miss benchmark")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.clearDiscoverCache()
		_ = r.Discover(service.Name)
	}
}
