package consulregistry

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

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
	if _, err := reg.Discoverer.client.Status().Leader(); err != nil {
		b.Skipf("consul 不可达，跳过 benchmark: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	if err := reg.Discoverer.StartSync(ctx, WatchOptions{Interval: 200 * time.Millisecond}); err != nil {
		b.Fatalf("start sync failed: %v", err)
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

		err := reg.Registrar.Register(ServiceRegistration{
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
		instances, err := reg.Discoverer.DiscoverDefault(targetService)
		if err != nil {
			return false
		}
		return len(instances) >= targetCount
	})

	waitUntilBench(b, 20*time.Second, func() bool {
		all, err := reg.Discoverer.DiscoverAll()
		if err != nil {
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
		instances, err := reg.Discoverer.DiscoverDefault(targetService)
		if err != nil {
			b.Fatalf("discover default failed: %v", err)
		}
		if len(instances) == 0 {
			b.Fatalf("discover default returned empty result")
		}
	}
}
