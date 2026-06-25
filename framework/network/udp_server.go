package network

import (
	"context"
	"errors"
	"fmt"
	"game-server/framework/pkg/glog"
	"game-server/framework/pkg/netutil"
	"net"

	"github.com/duke-git/lancet/v2/convertor"

	"go.uber.org/zap"
)

type udpPacket struct {
	data       []byte
	remoteAddr *net.UDPAddr
}

func NewUDPServer(base *baseServer) *UDPServer {
	server := &UDPServer{
		baseServer: base,
		sendChan:   make(chan *udpPacket, base.serverOpts.UdpOptions.SendChanSize),
	}
	return server
}

type UDPServer struct {
	*baseServer
	conn     *net.UDPConn
	sendChan chan *udpPacket
}

func (s *UDPServer) Start() error {
	if err := s.listen(); err != nil {
		return err
	}

	s.runGroup.Go(func() {
		s.readLoop()
	})
	s.runGroup.Go(func() {
		s.writeLoop()
	})

	glog.Info("UDP服务器监听", zap.String("address", s.Addr()))
	return nil

}

func (s *UDPServer) listen() (err error) {
	config := netutil.ListenConfig{
		ReuseAddr: s.serverOpts.ReuseAddr,
		ReusePort: s.serverOpts.ReusePort,
	}
	var ln net.PacketConn
	ln, err = config.ListenPacket(s.ctx, s.network, s.address)
	if err != nil {
		return err
	}

	udpConn, ok := ln.(*net.UDPConn)
	if !ok {
		if closeErr := ln.Close(); closeErr != nil {
			glog.Error("关闭 PacketConn 时出错", zap.Error(closeErr))
		}
		return fmt.Errorf("failed to convert PacketConn to UDPConn")
	}
	s.conn = udpConn
	return nil
}

func (s *UDPServer) readLoop() {
	buf := make([]byte, 1492)
	for !s.IsStop() {
		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				glog.Error("UDP服务器读取数据包异常", zap.String("address", s.Addr()), zap.Any("err", err))
			}
			continue
		}
		if n == 0 {
			continue
		}
		packet := make([]byte, n)
		copy(packet, buf[:n])

		remoteAddrCopy := convertor.DeepClone(remoteAddr)
		if remoteAddrCopy == nil {
			glog.Error("UDP DeepClone失败", zap.String("address", s.Addr()), zap.String("remoteAddr", remoteAddr.String()))
			continue
		}

		connKey := remoteAddrCopy.String()
		udpConn, exists := s.connMgr.GetUDP(connKey)
		if !exists {
			udpConn = s.addConnection(connKey, remoteAddrCopy)
		}
		udpConn.writeRcvChan(packet)
	}
}

func (s *UDPServer) writeLoop() {
	for !s.IsStop() {
		select {
		case <-s.ctx.Done():
			return
		case packet, ok := <-s.sendChan:
			if !ok {
				return
			}
			_, err := s.conn.WriteToUDP(packet.data, packet.remoteAddr)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					glog.Error("UDP服务器写入失败", zap.String("address", s.Addr()), zap.Error(err))
				}
				continue
			}
		}
	}
}

func (s *UDPServer) addConnection(connKey string, remoteAddr *net.UDPAddr) *UDPConnection {
	udpConn, exists := s.connMgr.GetUDP(connKey)
	if exists {
		return udpConn
	}

	udpConn = newUDPConnection(s.connCommon(), s.conn, remoteAddr, s.sendChan)
	existingConn, added := s.connMgr.AddUDP(connKey, udpConn)
	if !added {
		return existingConn
	}
	s.connMgr.Add(udpConn)

	s.runGroup.Go(func() {
		udpConn.readLoop()
	})

	s.runGroup.Go(func() {
		udpConn.heartbeat(udpConn)
	})

	return udpConn
}

func (s *UDPServer) Shutdown(ctx context.Context) {
	if !s.Stop() {
		return
	}
	if err := s.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		glog.Error("关闭 UDP 连接时出错", zap.String("address", s.Addr()), zap.Error(err))
	}
	s.baseServer.Shutdown(ctx)

	glog.Debug("UDP服务器已关闭", zap.String("address", s.Addr()))
	return
}
