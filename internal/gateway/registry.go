package gateway

import (
	"game-server/framework/actor"
	"sync"
)

type connRegistry struct {
	mu   sync.RWMutex
	data map[uint64]actor.PID
}

func newConnRegistry() *connRegistry {
	return &connRegistry{data: make(map[uint64]actor.PID)}
}

func (r *connRegistry) Bind(connID uint64, pid actor.PID) {
	r.mu.Lock()
	r.data[connID] = pid
	r.mu.Unlock()
}

func (r *connRegistry) Get(connID uint64) (actor.PID, bool) {
	r.mu.RLock()
	pid, ok := r.data[connID]
	r.mu.RUnlock()
	return pid, ok
}

func (r *connRegistry) Unbind(connID uint64) (actor.PID, bool) {
	r.mu.Lock()
	pid, ok := r.data[connID]
	delete(r.data, connID)
	r.mu.Unlock()
	return pid, ok
}

func (r *connRegistry) Reset() map[uint64]actor.PID {
	r.mu.Lock()
	old := r.data
	r.data = make(map[uint64]actor.PID)
	r.mu.Unlock()
	return old
}
