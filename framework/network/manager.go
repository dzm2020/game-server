package network

import (
	"sync"
	"sync/atomic"
)

type ConnManager struct {
	mu          sync.RWMutex
	connections map[int64]IConnection
	count       atomic.Int64

	udpMu          sync.RWMutex
	udpConnections map[string]*UDPConnection
}

func NewConnManager() *ConnManager {
	return &ConnManager{
		connections:    make(map[int64]IConnection),
		udpConnections: make(map[string]*UDPConnection),
	}
}

func (m *ConnManager) Add(conn IConnection) {
	if conn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.connections[conn.ID()]; !exists {
		m.connections[conn.ID()] = conn
		m.count.Add(1)
	}
}

func (m *ConnManager) Remove(conn IConnection) {
	if conn == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.connections[conn.ID()]; exists {
		delete(m.connections, conn.ID())
		m.count.Add(-1)
	}
}

func (m *ConnManager) Get(id int64) IConnection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connections[id]
}

func (m *ConnManager) ConnectionCount() int64 {
	return m.count.Load()
}

func (m *ConnManager) GetAll() []IConnection {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conns := make([]IConnection, 0, len(m.connections))
	for _, conn := range m.connections {
		if conn != nil {
			conns = append(conns, conn)
		}
	}
	return conns
}

func (m *ConnManager) Range(fn func(conn IConnection) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, conn := range m.connections {
		if !fn(conn) {
			break
		}
	}
}

func (m *ConnManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connections = make(map[int64]IConnection)
	m.count.Store(0)
}

func (m *ConnManager) AddUDP(connKey string, conn *UDPConnection) (*UDPConnection, bool) {
	if conn == nil {
		return nil, false
	}
	m.udpMu.Lock()
	defer m.udpMu.Unlock()
	if existing, exists := m.udpConnections[connKey]; exists {
		return existing, false
	}
	m.udpConnections[connKey] = conn
	return conn, true
}

func (m *ConnManager) RemoveUDP(connKey string) {
	m.udpMu.Lock()
	defer m.udpMu.Unlock()
	delete(m.udpConnections, connKey)
}

func (m *ConnManager) GetUDP(connKey string) (*UDPConnection, bool) {
	m.udpMu.RLock()
	defer m.udpMu.RUnlock()
	conn, ok := m.udpConnections[connKey]
	return conn, ok
}
