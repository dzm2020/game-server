package consulregistry

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func TestDiscovererWatchAndCacheDiff(t *testing.T) {
	d := newDiscoverer(nil, nil)

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
	d := newDiscoverer(nil, nil)
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
	d := newDiscoverer(nil, nil)
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

func TestStartSyncOnSyncErrorCallback(t *testing.T) {
	cfg := api.DefaultConfig()
	cfg.Address = "127.0.0.1:1"
	cfg.Scheme = "http"
	client, err := api.NewClient(cfg)
	if err != nil {
		t.Fatalf("create test client failed: %v", err)
	}

	d := newDiscoverer(client, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	if err := d.StartSync(ctx, WatchOptions{
		Interval: 50 * time.Millisecond,
		OnSyncError: func(err error) {
			select {
			case errCh <- err:
			default:
			}
		},
	}); err != nil {
		t.Fatalf("start sync failed: %v", err)
	}

	waitUntil(t, 2*time.Second, func() bool {
		select {
		case err := <-errCh:
			return err != nil
		default:
			return false
		}
	})
}
