package obs

import (
	"sync"
	"sync/atomic"
)

type counter struct {
	value atomic.Int64
}

var counters sync.Map // map[string]*counter

func Inc(name string) {
	Add(name, 1)
}

func Dec(name string) {
	Add(name, -1)
}

func Add(name string, delta int64) {
	if name == "" || delta == 0 {
		return
	}
	raw, _ := counters.LoadOrStore(name, &counter{})
	c := raw.(*counter)
	c.value.Add(delta)
}

func Get(name string) int64 {
	if name == "" {
		return 0
	}
	raw, ok := counters.Load(name)
	if !ok {
		return 0
	}
	c := raw.(*counter)
	return c.value.Load()
}

func Snapshot() map[string]int64 {
	out := make(map[string]int64)
	counters.Range(func(key, value any) bool {
		k, ok := key.(string)
		if !ok {
			return true
		}
		c, ok := value.(*counter)
		if !ok {
			return true
		}
		out[k] = c.value.Load()
		return true
	})
	return out
}
