package network

import (
	"game-server/framework/gen"
	"game-server/framework/obs"
	"game-server/framework/pkg/glog"
	"net"
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
		return gen.ErrConnectionClosed
	}
	copyData := append([]byte(nil), data...)
	select {
	case c.sendChan <- &udpPacket{data: copyData, remoteAddr: c.remoteAddr}:
	default:
		return gen.ErrNetworkChannelFull
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
		obs.Inc("network.udp_read_chan_full_total")
		glog.Error("UDP读取chan已满", glog.Component("network.udp"), glog.ConnID(c.ID()))
	}
}

func (c *UDPConnection) Close(err error) (w error) {
	if !c.Stop() {
		return gen.ErrConnectionClosed
	}
	if c.udpConnMgr != nil {
		c.udpConnMgr.RemoveUDP(c.connKey)
	}
	c.baseConn.Close(c, err)

	obs.Inc("network.udp_close_total")
	glog.Info("UDP连接断开", glog.Component("network.udp"), glog.ConnID(c.ID()), glog.Err(err))
	return
}
