package network

import "testing"

type stubConn struct {
	id      int64
	ctx     interface{}
	stopped bool
}

func (c *stubConn) ID() int64                { return c.id }
func (c *stubConn) Send([]byte) error        { return nil }
func (c *stubConn) Close(error) error        { c.stopped = true; return nil }
func (c *stubConn) LocalAddr() string        { return "local" }
func (c *stubConn) RemoteAddr() string       { return "remote" }
func (c *stubConn) IsStop() bool             { return c.stopped }
func (c *stubConn) Context() interface{}     { return c.ctx }
func (c *stubConn) SetContext(v interface{}) { c.ctx = v }
func (c *stubConn) SetReadBuffer(int) error  { return nil }
func (c *stubConn) SetWriteBuffer(int) error { return nil }

func TestConnManager_RangeSupportsMutation(t *testing.T) {
	mgr := NewConnManager()
	conns := []*stubConn{
		{id: 1},
		{id: 2},
		{id: 3},
	}
	for _, conn := range conns {
		mgr.Add(conn)
	}

	visited := 0
	mgr.Range(func(conn IConnection) bool {
		visited++
		mgr.Remove(conn)
		return true
	})

	if visited != len(conns) {
		t.Fatalf("visited mismatch, got=%d want=%d", visited, len(conns))
	}
	if got := mgr.ConnectionCount(); got != 0 {
		t.Fatalf("ConnectionCount mismatch after range remove, got=%d want=0", got)
	}
}

func TestConnManager_RangeCanBreakEarly(t *testing.T) {
	mgr := NewConnManager()
	mgr.Add(&stubConn{id: 1})
	mgr.Add(&stubConn{id: 2})

	called := 0
	mgr.Range(func(conn IConnection) bool {
		called++
		return false
	})

	if called != 1 {
		t.Fatalf("Range should stop on first false callback, got=%d want=1", called)
	}
}
