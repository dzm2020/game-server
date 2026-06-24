package network

import (
	"errors"
	"game-server/framework/pkg/buffer"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"io"
	"net"
	"time"

	"go.uber.org/zap"
)

const (
	maxBatchWriteBytes = 4 * 1024 * 1024
)

type TCPConnection struct {
	*baseConn
	conn        *net.TCPConn
	tmpBuf      []byte
	readBuffer  buffer.IBuffer
	writeBuffer buffer.IBuffer
}

func newTCPConnection(common connCommon, conn *net.TCPConn) *TCPConnection {
	base := newBaseConn(common, "tcp", conn, conn.RemoteAddr())

	readBufSize := common.serverOpts.TcpOptions.ReadBufferSize
	writeBufferSize := common.serverOpts.TcpOptions.WriteBufferSize

	tcpConn := &TCPConnection{
		baseConn:    base,
		tmpBuf:      make([]byte, readBufSize),
		readBuffer:  buffer.New(readBufSize),
		writeBuffer: buffer.New(writeBufferSize),
	}
	return tcpConn
}

func (c *TCPConnection) readLoop() {
	var err error
	var n int
	defer func() {
		_ = c.Close(err)
	}()

	if err = c.onConnect(c); err != nil {
		return
	}
	for !c.IsStop() {
		n, err = c.conn.Read(c.tmpBuf)
		if err != nil {
			if err == io.EOF {
				return
			}
			if !errors.Is(err, net.ErrClosed) {
				glog.Error("TCP连接读取错误", zap.Int64("connectionId", c.ID()), zap.Error(err))
			}
			return
		}
		if n == 0 {
			err = io.EOF
			return
		}
		_, err = c.process(c.tmpBuf[:n])
		if err != nil {
			return
		}
	}
}

func (c *TCPConnection) writeLoop() {
	var err error
	defer func() {
		_ = c.batchWriteMsg(nil)
		_ = c.conn.Close()
		_ = c.Close(err)
	}()

	for !c.IsStop() {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.sendChan:
			if !ok {
				return
			}
			if err = c.batchWriteMsg(msg); err != nil {
				return
			}
		}
	}
}

func (c *TCPConnection) batchWriteMsg(msg []byte) error {
	totalBytes := 0
	loop := true
	for totalBytes < maxBatchWriteBytes && loop {
		totalBytes += len(msg)
		if _, err := c.writeBuffer.Write(msg); err != nil {
			return err
		}

		if len(c.sendChan) <= 0 {
			break
		}
		var ok bool
		select {
		case msg, ok = <-c.sendChan:
			if !ok {
				loop = false
				break
			}
			if msg == nil {
				continue
			}
		default:
			loop = false
			break
		}
	}

	for c.writeBuffer.Len() > 0 {
		n, err := c.conn.Write(c.writeBuffer.Bytes())
		if err != nil {
			return err
		}
		_ = c.writeBuffer.Skip(n)
	}
	return nil
}

func (c *TCPConnection) process(data []byte) (int, error) {
	_, _ = c.readBuffer.Write(data)
	n, err := c.OnMessage(c, c.readBuffer.Bytes())
	if err != nil {
		return n, err
	}
	_ = c.readBuffer.Skip(n)
	return n, nil
}

func (c *TCPConnection) Close(err error) (w error) {
	if !c.Stop() {
		return ErrConnectionClosed
	}

	c.baseConn.Close(c, err)

	glog.Info("TCP连接断开", zap.Int64("connectionId", c.ID()), zap.Error(err))
	return
}

func (c *TCPConnection) SetLinger(enable bool, sec int) error {
	return netutil.SetTCPLinger(c.conn, enable, sec)
}

func (c *TCPConnection) SetNoDelay(noDelay bool) error {
	return netutil.SetTCPNoDelay(c.conn, noDelay)
}

func (c *TCPConnection) SetTCPKeepAlive(enable bool, period time.Duration) error {
	return netutil.SetTCPKeepAlive(c.conn, enable, period)
}
