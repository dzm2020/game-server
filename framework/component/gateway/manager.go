package gateway

import (
	"game-server/framework/gen"
	"sync"
)

type manager struct {
	mu   sync.RWMutex
	data map[int64]*gen.PID
}

func newConnManager() *manager {
	return &manager{data: make(map[int64]*gen.PID)}
}

func (r *manager) Bind(connID int64, pid *gen.PID) {
	r.mu.Lock()
	r.data[connID] = pid
	r.mu.Unlock()
}

func (r *manager) Get(connID int64) (*gen.PID, bool) {
	r.mu.RLock()
	pid, ok := r.data[connID]
	r.mu.RUnlock()
	return pid, ok
}

func (r *manager) Unbind(connID int64) (*gen.PID, bool) {
	r.mu.Lock()
	pid, ok := r.data[connID]
	delete(r.data, connID)
	r.mu.Unlock()
	return pid, ok
}

func (r *manager) Reset() map[int64]*gen.PID {
	r.mu.Lock()
	old := r.data
	r.data = make(map[int64]*gen.PID)
	r.mu.Unlock()
	return old
}
