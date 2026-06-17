package consul

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func TestDiscovererWatchAndCacheDiff(t *testing.T) {
	d := newDiscoverer(nil)

	type event struct {
		all     []ServiceInstance
		added   []ServiceInstance
		updated []ServiceInstance
		removed []ServiceInstance
	}
	var (
		mu     sync.Mutex
		events []event
	)

	watchID, err := d.Watch("svc", func(all, added, updated, removed []ServiceInstance) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event{
			all:     cloneInstances(all),
			added:   cloneInstances(added),
			updated: cloneInstances(updated),
			removed: cloneInstances(removed),
		})
	})
	if err != nil {
		t.Fatalf("watch failed: %v", err)
	}
	if watchID == "" {
		t.Fatalf("expect non-empty watch id")
	}

	first := []ServiceInstance{
		{ID: "a", Name: "svc", Address: "10.0.0.1", Port: 8080},
	}
	d.setServiceCache("svc", first)

	second := []ServiceInstance{
		{ID: "a", Name: "svc", Address: "10.0.0.1", Port: 8081},
		{ID: "b", Name: "svc", Address: "10.0.0.2", Port: 8080},
	}
	d.setServiceCache("svc", second)
	d.setServiceCache("svc", second) // same snapshot, should not notify
	d.removeServiceCache("svc")

	waitUntil(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) == 3
	})

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 {
		t.Fatalf("expect 3 events, got %d", len(events))
	}
	var hasInit, hasUpdate, hasRemove bool
	for _, ev := range events {
		if len(ev.all) == 1 && len(ev.added) == 1 && len(ev.updated) == 0 && len(ev.removed) == 0 {
			hasInit = true
		}
		if len(ev.all) == 2 && len(ev.added) == 1 && len(ev.updated) == 1 && len(ev.removed) == 0 {
			hasUpdate = true
		}
		if len(ev.all) == 0 && len(ev.added) == 0 && len(ev.updated) == 0 && len(ev.removed) == 2 {
			hasRemove = true
		}
	}
	if !hasInit || !hasUpdate || !hasRemove {
		t.Fatalf("unexpected events: %#v", events)
	}

	d.Unwatch("svc", watchID)
}

func TestDiscovererDiscoverAndDiscoverAll(t *testing.T) {
	d := newDiscoverer(nil)
	d.setServiceNames([]string{"svcA", "svcB"})
	d.setServiceCache("svcA", []ServiceInstance{{ID: "1", Name: "svcA", Address: "127.0.0.1", Port: 80}})
	d.setServiceCache("svcB", []ServiceInstance{{ID: "2", Name: "svcB", Address: "127.0.0.2", Port: 81}})

	instances, err := d.Discover("svcA")
	if err != nil {
		t.Fatalf("discover failed: %v", err)
	}
	if len(instances) != 1 || instances[0].ID != "1" {
		t.Fatalf("unexpected discover instances: %#v", instances)
	}

	// Ensure Discover returns a cloned slice.
	instances[0].Port = 9999
	instances2, err := d.Discover("svcA")
	if err != nil {
		t.Fatalf("discover second time failed: %v", err)
	}
	if instances2[0].Port == 9999 {
		t.Fatalf("discover should return cloned data")
	}

	all, err := d.DiscoverAll()
	if err != nil {
		t.Fatalf("discover all failed: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expect 2 services, got %d", len(all))
	}

	if _, err = d.Discover("missing"); err == nil {
		t.Fatalf("discover missing service should fail")
	}
}

func TestDiscovererDiscoverAllHighAvailability(t *testing.T) {
	d := newDiscoverer(nil)
	d.setServiceNames([]string{"svcA", "svcB"})
	d.setServiceCache("svcA", []ServiceInstance{{ID: "1", Name: "svcA", Address: "127.0.0.1", Port: 80}})
	// svcB 故意不写缓存，模拟同步窗口或局部异常场景

	all, err := d.DiscoverAll()
	if err != nil {
		t.Fatalf("discover all should not fail on partial miss: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expect only available services, got %d", len(all))
	}
	if _, ok := all["svcA"]; !ok {
		t.Fatalf("svcA should be present")
	}
	if _, ok := all["svcB"]; ok {
		t.Fatalf("svcB should be skipped on cache miss")
	}
}

func TestDiffInstancesAndEqual(t *testing.T) {
	prev := []ServiceInstance{
		{ID: "a", Name: "svc", Address: "10.0.0.1", Port: 8080},
		{ID: "b", Name: "svc", Address: "10.0.0.2", Port: 8080},
	}
	curr := []ServiceInstance{
		{ID: "a", Name: "svc", Address: "10.0.0.1", Port: 8081}, // updated
		{ID: "c", Name: "svc", Address: "10.0.0.3", Port: 8080}, // added
	}
	added, updated, removed, changed := diffInstances(prev, curr)
	if !changed || len(added) != 1 || len(updated) != 1 || len(removed) != 1 {
		t.Fatalf("unexpected diff result: added=%d updated=%d removed=%d changed=%v", len(added), len(updated), len(removed), changed)
	}

	a := ServiceInstance{ID: "x", Name: "svc", Address: "1", Port: 1, Tags: []string{"a"}, Meta: map[string]string{"k": "v"}}
	b := ServiceInstance{ID: "x", Name: "svc", Address: "1", Port: 1, Tags: []string{"a"}, Meta: map[string]string{"k": "v"}}
	c := ServiceInstance{ID: "x", Name: "svc", Address: "1", Port: 2}
	if !instanceEqual(a, b) {
		t.Fatalf("equal instances should be true")
	}
	if instanceEqual(a, c) {
		t.Fatalf("different instances should be false")
	}
}

func TestInstancesFromEntries(t *testing.T) {
	entries := []*api.ServiceEntry{
		{
			Node: &api.Node{Address: "192.168.1.1"},
			Service: &api.AgentService{
				ID:      "id-1",
				Service: "svc",
				Port:    9000,
				Address: "",
			},
		},
		{
			Node: nil,
			Service: &api.AgentService{
				ID:      "id-2",
				Service: "svc",
				Port:    9001,
				Address: "192.168.1.2",
			},
		},
		nil,
	}

	got := instancesFromEntries(entries)
	if len(got) != 2 {
		t.Fatalf("expect 2 instances, got %d", len(got))
	}
	if got[0].Address != "192.168.1.1" {
		t.Fatalf("expect fallback to node address, got %s", got[0].Address)
	}
	if got[1].Address != "192.168.1.2" {
		t.Fatalf("expect service address, got %s", got[1].Address)
	}
}

func TestDiscovererWatchValidation(t *testing.T) {
	d := newDiscoverer(nil)
	if _, err := d.Watch("", func(_, _, _, _ []ServiceInstance) {}); err == nil {
		t.Fatalf("watch with empty service name should fail")
	}
	if _, err := d.Watch("svc", nil); err == nil {
		t.Fatalf("watch with nil callback should fail")
	}
}

func TestDiscovererRunAndWatchWithRealConsul(t *testing.T) {
	cfg := consulTestConfig()
	cfg.TTL = 5 * time.Second
	cfg.HeartbeatInterval = 300 * time.Millisecond
	cfg.HeartbeatNote = "sync"

	reg, err := New(cfg)
	if err != nil {
		t.Fatalf("new registry failed: %v", err)
	}
	if _, err = reg.Discoverer.client.Status().Leader(); err != nil {
		t.Skipf("consul 不可达，跳过集成测试: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err = reg.Run(ctx); err != nil {
		t.Fatalf("run discoverer failed: %v", err)
	}

	serviceID := "svc-sync-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	serviceName := "svc-sync"
	t.Cleanup(func() {
		cleanupService(reg.Registrar, reg.Discoverer.client, serviceID)
	})

	var (
		changedMu sync.Mutex
		changed   bool
	)
	watchID, err := reg.Watch(serviceName, func(all, added, updated, removed []ServiceInstance) {
		changedMu.Lock()
		changed = true
		changedMu.Unlock()
	})
	if err != nil {
		t.Fatalf("watch failed: %v", err)
	}
	defer reg.Unwatch(serviceName, watchID)

	if err = reg.Register(ServiceInstance{
		ID:      serviceID,
		Name:    serviceName,
		Address: "127.0.0.1",
		Port:    8090,
	}); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	waitUntil(t, 5*time.Second, func() bool {
		instances, discoverErr := reg.Discover(serviceName)
		if discoverErr != nil {
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

func waitUntilBench(b *testing.B, timeout time.Duration, f func() bool) {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	b.Fatalf("condition not met within %s", timeout)
}

func setupDiscoverBenchmarkData(b *testing.B, totalInstances, totalServices int) (*Registry, string, string, []string) {
	b.Helper()

	cfg := consulTestConfig()
	reg, err := New(cfg)
	if err != nil {
		b.Fatalf("new registry failed: %v", err)
	}
	if _, err = reg.Discoverer.client.Status().Leader(); err != nil {
		b.Skipf("consul 不可达，跳过 benchmark: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	if err = reg.Run(ctx); err != nil {
		b.Fatalf("run discoverer failed: %v", err)
	}

	prefix := "bench-discover-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	targetService := fmt.Sprintf("%s-%03d", prefix, 0)
	ids := make([]string, 0, totalInstances)
	targetCount := 0

	for i := 0; i < totalInstances; i++ {
		serviceName := fmt.Sprintf("%s-%03d", prefix, i%totalServices)
		if serviceName == targetService {
			targetCount++
		}
		id := fmt.Sprintf("%s-%04d", serviceName, i)
		ids = append(ids, id)

		err = reg.Register(ServiceInstance{
			ID:      id,
			Name:    serviceName,
			Address: "127.0.0.1",
			Port:    10000 + (i % 1000),
		})
		if err != nil {
			b.Fatalf("register failed at %d/%d, id=%s, err=%v", i+1, totalInstances, id, err)
		}
	}

	b.Cleanup(func() {
		for _, id := range ids {
			cleanupService(reg.Registrar, reg.Discoverer.client, id)
		}
	})

	waitUntilBench(b, 20*time.Second, func() bool {
		instances, discoverErr := reg.Discoverer.Discover(targetService)
		if discoverErr != nil {
			return false
		}
		return len(instances) >= targetCount
	})

	waitUntilBench(b, 20*time.Second, func() bool {
		all, discoverErr := reg.Discoverer.DiscoverAll()
		if discoverErr != nil {
			return false
		}
		serviceSeen := 0
		instanceSeen := 0
		for name, list := range all {
			if strings.HasPrefix(name, prefix+"-") {
				serviceSeen++
				instanceSeen += len(list)
			}
		}
		return serviceSeen >= totalServices && instanceSeen >= totalInstances
	})

	return reg, prefix, targetService, ids
}

func BenchmarkDiscovererDiscoverAll1000Instances(b *testing.B) {
	const (
		totalInstances = 1000
		totalServices  = 10
	)

	reg, prefix, _, _ := setupDiscoverBenchmarkData(b, totalInstances, totalServices)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		all, err := reg.Discoverer.DiscoverAll()
		if err != nil {
			b.Fatalf("discover all failed: %v", err)
		}
		if len(all) == 0 {
			b.Fatalf("discover all returned empty result")
		}
		prefixed := 0
		for name := range all {
			if strings.HasPrefix(name, prefix+"-") {
				prefixed++
			}
		}
		if prefixed < totalServices {
			b.Fatalf("discover all missing bench services: got=%d want>=%d", prefixed, totalServices)
		}
	}
}

func BenchmarkDiscovererDiscoverDefault1000InstancesCluster(b *testing.B) {
	const (
		totalInstances = 1000
		totalServices  = 100
	)

	reg, _, targetService, _ := setupDiscoverBenchmarkData(b, totalInstances, totalServices)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		instances, err := reg.Discoverer.Discover(targetService)
		if err != nil {
			b.Fatalf("discover failed: %v", err)
		}
		if len(instances) == 0 {
			b.Fatalf("discover returned empty result")
		}
	}
}
