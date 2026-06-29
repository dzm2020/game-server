package actor

import (
	"context"
	"fmt"
	"game-server/framework/gen"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkSingleActorThroughput(b *testing.B) {
	s := NewSystem(newTestNode("node-bench"))
	if err := s.Init(context.Background()); err != nil {
		b.Fatalf("init system failed: %v", err)
	}
	if err := s.Start(context.Background()); err != nil {
		b.Fatalf("start system failed: %v", err)
	}
	b.Cleanup(func() {
		_ = s.Stop(context.Background())
	})

	var processed atomic.Int64
	pid, err := s.Spawn(func(gen.IContext) {
		processed.Add(1)
	}, gen.SpawnOptions{MailboxSize: 1024})
	if err != nil {
		b.Fatalf("spawn actor failed: %v", err)
	}

	msg := gen.NewMessage(1, 1, nil)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for {
			err = s.Tell(gen.NoSender, pid, msg)
			if err == nil {
				break
			}
			if err == gen.ErrActorMailboxFull {
				runtime.Gosched()
				continue
			}
			b.Fatalf("tell failed: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for processed.Load() < int64(b.N) {
		if time.Now().After(deadline) {
			b.Fatalf("wait processed timeout: got=%d want=%d", processed.Load(), b.N)
		}
		runtime.Gosched()
	}
}

func BenchmarkSingleActorThroughputParallel(b *testing.B) {
	parallelisms := []int{1, 10, 100, 200, 500, 1000}
	seen := make(map[int]struct{}, len(parallelisms))

	for _, p := range parallelisms {
		if p <= 0 {
			b.Fatalf("invalid parallelism: %d (must be > 0)", p)
		}
		if _, exists := seen[p]; exists {
			continue
		}
		seen[p] = struct{}{}

		p := p
		b.Run(fmt.Sprintf("P%d", p), func(b *testing.B) {
			s := NewSystem(newTestNode("node-bench"))
			if err := s.Init(context.Background()); err != nil {
				b.Fatalf("init system failed: %v", err)
			}
			if err := s.Start(context.Background()); err != nil {
				b.Fatalf("start system failed: %v", err)
			}
			b.Cleanup(func() {
				_ = s.Stop(context.Background())
			})

			var processed atomic.Int64
			pid, err := s.Spawn(func(gen.IContext) {
				processed.Add(1)
			}, gen.SpawnOptions{MailboxSize: 10240})
			if err != nil {
				b.Fatalf("spawn actor failed: %v", err)
			}

			msg := gen.NewMessage(1, 1, nil)
			var failed atomic.Bool
			var firstErr atomic.Value

			b.ReportAllocs()
			b.SetParallelism(p)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for {
						if failed.Load() {
							return
						}

						err = s.Tell(gen.NoSender, pid, msg)
						if err == nil {
							break
						}
						if err == gen.ErrActorMailboxFull {
							runtime.Gosched()
							continue
						}

						if failed.CompareAndSwap(false, true) {
							firstErr.Store(err)
						}
						return
					}
				}
			})

			if failed.Load() {
				if v := firstErr.Load(); v != nil {
					b.Fatalf("parallel tell failed: %v", v.(error))
				}
				b.Fatal("parallel tell failed")
			}

			// 高并发入口时 actor 排空速度会明显慢于投递阶段，给足回收窗口避免误判超时。
			deadline := time.Now().Add(60 * time.Second)
			for processed.Load() < int64(b.N) {
				if time.Now().After(deadline) {
					b.Fatalf("wait processed timeout: got=%d want=%d", processed.Load(), b.N)
				}
				runtime.Gosched()
			}
		})
	}
}
