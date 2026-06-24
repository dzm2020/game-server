package network

import (
	"game-server/framework/pkg/glog"
	"net"

	"go.uber.org/zap"
)

type UDPConnection struct {
	*baseConn
	remoteAddr *net.UDPAddr
	conn       *net.UDPConn
	connKey    string
	rcvChan    chan []byte
	sendChan   chan<- *udpPacket
	udpConnMgr *ConnManager
}

func newUDPConnection(common connCommon, conn *net.UDPConn, remoteAddr *net.UDPAddr, sendChan chan<- *udpPacket) *UDPConnection {
	base := newBaseConn(common, "udp", conn, remoteAddr)
	connKey := remoteAddr.String()
	udpConn := &UDPConnection{
		baseConn:   base,
		remoteAddr: remoteAddr,
		conn:       conn,
		connKey:    connKey,
		rcvChan:    make(chan []byte, common.serverOpts.UdpOptions.ReadChanSize),
		sendChan:   sendChan,
		udpConnMgr: common.connMgr,
	}
	return udpConn
}

func (c *UDPConnection) Send(data []byte) error {
	if c.IsStop() {
		return ErrConnectionClosed
	}
	copyData := append([]byte(nil), data...)
	select {
	case c.sendChan <- &udpPacket{data: copyData, remoteAddr: c.remoteAddr}:
	default:
		return ErrChannelFull
	}
	return nil
}

func (c *UDPConnection) readLoop() {
	var err error
	defer func() {
		_ = c.Close(err)
	}()

	if err = c.onConnect(c); err != nil {
		return
	}

	for !c.IsStop() {
		select {
		case <-c.ctx.Done():
			return
		case data, ok := <-c.rcvChan:
			if !ok {
				return
			}
			_, err = c.OnMessage(c, data)
			if err != nil {
				return
			}
		}
	}
}

func (c *UDPConnection) writeRcvChan(data []byte) {
	select {
	case c.rcvChan <- data:
	default:
		glog.Error("UDP读取chan已满", zap.Int64("connectionId", c.ID()))
	}
}

func (c *UDPConnection) Close(err error) (w error) {
	if !c.Stop() {
		return ErrConnectionClosed
	}
	if c.udpConnMgr != nil {
		c.udpConnMgr.RemoveUDP(c.connKey)
	}
	c.baseConn.Close(c, err)

	glog.Info("UDP连接断开", zap.Int64("connectionId", c.ID()), zap.Error(err))
	return
}
