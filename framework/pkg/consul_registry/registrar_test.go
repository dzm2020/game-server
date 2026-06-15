package consulregistry

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func consulTestConfig() Config {
	addr := os.Getenv("CONSUL_ADDR")
	if addr == "" {
		addr = os.Getenv("CONSUL_HTTP_ADDR")
	}
	if addr == "" {
		addr = "127.0.0.1:8500"
	}
	scheme := "http"
	if v := os.Getenv("CONSUL_SCHEME"); v != "" {
		scheme = v
	}
	return Config{
		Address: addr,
		Scheme:  scheme,
		Token:   os.Getenv("CONSUL_HTTP_TOKEN"),
	}
}

func newIntegrationRegistrar(t *testing.T) (*Registrar, *api.Client) {
	t.Helper()

	cfg := consulTestConfig()
	client, err := newConsulClient(cfg, ensureLogger(nil))
	if err != nil {
		t.Fatalf("create consul client failed: %v", err)
	}
	if _, err := client.Status().Leader(); err != nil {
		t.Skipf("consul 不可达，跳过集成测试: %v", err)
	}
	return newRegistrar(client, nil), client
}

func waitUntil(t *testing.T, timeout time.Duration, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func serviceHasStatus(client *api.Client, serviceName, serviceID, want string) bool {
	checks, _, err := client.Health().Checks(serviceName, nil)
	if err != nil {
		return false
	}
	for _, chk := range checks {
		if chk == nil {
			continue
		}
		if chk.ServiceID == serviceID && chk.Status == want {
			return true
		}
	}
	return false
}

func cleanupService(rr *Registrar, client *api.Client, serviceID string) {
	services, err := client.Agent().Services()
	if err != nil {
		return
	}
	if _, ok := services[serviceID]; !ok {
		return
	}
	if err := rr.Deregister(serviceID); err != nil && !strings.Contains(err.Error(), "Unknown service ID") {
		return
	}
}

func TestRegistrarRegisterHeartbeatAndDeregister(t *testing.T) {
	rr, client := newIntegrationRegistrar(t)
	serviceID := "svc-it-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	serviceName := "svc-it"
	t.Cleanup(func() {
		cleanupService(rr, client, serviceID)
	})

	err := rr.Register(ServiceRegistration{
		ID:                serviceID,
		Name:              serviceName,
		Address:           "127.0.0.1",
		Port:              8080,
		TTL:               5 * time.Second,
		HeartbeatInterval: 300 * time.Millisecond,
		HeartbeatNote:     "hb",
		DeregisterAfter:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	waitUntil(t, 3*time.Second, func() bool {
		return serviceHasStatus(client, serviceName, serviceID, api.HealthPassing)
	})

	if err := rr.TTLWarn(serviceID, "warn-note"); err != nil {
		t.Fatalf("ttl warn failed: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return serviceHasStatus(client, serviceName, serviceID, api.HealthWarning)
	})

	if err := rr.TTLPass(serviceID, "ok"); err != nil {
		t.Fatalf("ttl pass failed: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return serviceHasStatus(client, serviceName, serviceID, api.HealthPassing)
	})

	if err := rr.Deregister(serviceID); err != nil {
		t.Fatalf("deregister failed: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		services, err := client.Agent().Services()
		if err != nil {
			return false
		}
		_, ok := services[serviceID]
		return !ok
	})
}

func TestRegistrarTTLStatusTransitions(t *testing.T) {
	rr := newRegistrar(nil, nil)

	rr.ttlMu.Lock()
	rr.ttlCancels["svc"] = func() {}
	rr.ttlStates["svc"] = ttlState{status: api.HealthPassing, note: "init"}
	rr.ttlMu.Unlock()

	if err := rr.TTLWarn("svc", "warn-note"); err != nil {
		t.Fatalf("ttl warn failed: %v", err)
	}
	state, ok := rr.getTTLState("svc")
	if !ok || state.status != api.HealthWarning || state.note != "warn-note" {
		t.Fatalf("unexpected warn state: %#v, ok=%v", state, ok)
	}

	if err := rr.TTLFail("svc", ""); err != nil {
		t.Fatalf("ttl fail failed: %v", err)
	}
	state, _ = rr.getTTLState("svc")
	if state.status != api.HealthCritical || state.note != "warn-note" {
		t.Fatalf("unexpected fail state: %#v", state)
	}

	if err := rr.TTLPass("svc", "ok"); err != nil {
		t.Fatalf("ttl pass failed: %v", err)
	}
	state, _ = rr.getTTLState("svc")
	if state.status != api.HealthPassing || state.note != "ok" {
		t.Fatalf("unexpected pass state: %#v", state)
	}
}

func TestRegistrarUpdateTTLRealConsul(t *testing.T) {
	rr, client := newIntegrationRegistrar(t)
	serviceID := "svc-it-ttl-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	serviceName := "svc-it-ttl"
	t.Cleanup(func() {
		cleanupService(rr, client, serviceID)
	})

	if err := rr.Register(ServiceRegistration{
		ID:                serviceID,
		Name:              serviceName,
		Address:           "127.0.0.1",
		Port:              8088,
		TTL:               5 * time.Second,
		HeartbeatInterval: 400 * time.Millisecond,
		HeartbeatNote:     "init",
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	waitUntil(t, 3*time.Second, func() bool {
		return serviceHasStatus(client, serviceName, serviceID, api.HealthPassing)
	})
	if err := rr.updateTTL(serviceID, "manual-fail", api.HealthCritical); err != nil {
		t.Fatalf("update ttl failed: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		return serviceHasStatus(client, serviceName, serviceID, api.HealthCritical)
	})
}

func TestDiscovererStartSyncAndWatchWithRealConsul(t *testing.T) {
	cfg := consulTestConfig()
	reg, err := New(cfg)
	if err != nil {
		t.Fatalf("new registry failed: %v", err)
	}
	if _, err := reg.Discoverer.client.Status().Leader(); err != nil {
		t.Skipf("consul 不可达，跳过集成测试: %v", err)
	}

	serviceID := "svc-sync-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	serviceName := "svc-sync"
	t.Cleanup(func() {
		cleanupService(reg.Registrar, reg.Discoverer.client, serviceID)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := reg.Discoverer.StartSync(ctx, WatchOptions{Interval: 200 * time.Millisecond}); err != nil {
		t.Fatalf("start sync failed: %v", err)
	}

	var (
		changedMu sync.Mutex
		changed   bool
	)
	watchID, err := reg.Discoverer.Watch(serviceName, func(all, added, updated, removed []ServiceInstance) {
		changedMu.Lock()
		changed = true
		changedMu.Unlock()
	})
	if err != nil {
		t.Fatalf("watch failed: %v", err)
	}
	defer reg.Discoverer.Unwatch(serviceName, watchID)

	if err := reg.Registrar.Register(ServiceRegistration{
		ID:                serviceID,
		Name:              serviceName,
		Address:           "127.0.0.1",
		Port:              8090,
		TTL:               5 * time.Second,
		HeartbeatInterval: 300 * time.Millisecond,
		HeartbeatNote:     "sync",
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	waitUntil(t, 5*time.Second, func() bool {
		instances, err := reg.Discoverer.Discover(serviceName)
		if err != nil {
			return false
		}
		for _, ins := range instances {
			if ins.ID == serviceID {
				return true
			}
		}
		return false
	})

	waitUntil(t, 3*time.Second, func() bool {
		changedMu.Lock()
		defer changedMu.Unlock()
		return changed
	})
}
