package consulregistry

import (
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

func newIntegrationRegistrarForBenchmark(b *testing.B) (*Registrar, *api.Client) {
	b.Helper()
	cfg := consulTestConfig()
	client, err := newConsulClient(cfg, ensureLogger(nil))
	if err != nil {
		b.Fatalf("create consul client failed: %v", err)
	}
	if _, err := client.Status().Leader(); err != nil {
		b.Skipf("consul 不可达，跳过 benchmark: %v", err)
	}
	return newRegistrar(client, nil), client
}

func BenchmarkRegistrarRegister1000Services(b *testing.B) {
	const total = 1000

	rr, client := newIntegrationRegistrarForBenchmark(b)
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		prefix := "bench-svc-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.Itoa(n)
		ids := make([]string, 0, total)

		start := time.Now()
		for i := 0; i < total; i++ {
			id := prefix + "-" + strconv.Itoa(i)
			ids = append(ids, id)

			err := rr.Register(ServiceRegistration{
				ID:      id,
				Name:    "perf-register",
				Address: "127.0.0.1",
				Port:    9000 + (i % 1000),
			})
			if err != nil {
				b.Fatalf("register failed at %d/%d, id=%s, err=%v", i+1, total, id, err)
			}
		}
		elapsed := time.Since(start)
		b.ReportMetric(float64(total)/elapsed.Seconds(), "register_qps/op")
		b.ReportMetric(float64(elapsed.Microseconds())/float64(total), "us_per_register")

		b.StopTimer()
		for _, id := range ids {
			cleanupService(rr, client, id)
		}
		b.StartTimer()
	}
}
