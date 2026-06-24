package gateway

import (
	"game-server/framework/gen"
	"sync"
)

type connRegistry struct {
	mu   sync.RWMutex
	data map[int64]*gen.PID
}

func newConnRegistry() *connRegistry {
	return &connRegistry{data: make(map[int64]*gen.PID)}
}

func (r *connRegistry) Bind(connID int64, pid *gen.PID) {
	r.mu.Lock()
	r.data[connID] = pid
	r.mu.Unlock()
}

func (r *connRegistry) Get(connID int64) (*gen.PID, bool) {
	r.mu.RLock()
	pid, ok := r.data[connID]
	r.mu.RUnlock()
	return pid, ok
}

func (r *connRegistry) Unbind(connID int64) (*gen.PID, bool) {
	r.mu.Lock()
	pid, ok := r.data[connID]
	delete(r.data, connID)
	r.mu.Unlock()
	return pid, ok
}

func (r *connRegistry) Reset() map[int64]*gen.PID {
	r.mu.Lock()
	old := r.data
	r.data = make(map[int64]*gen.PID)
	r.mu.Unlock()
	return old
}
