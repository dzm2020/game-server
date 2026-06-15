package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"game-server/framework/runtime/protocol"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := flag.String("url", "ws://127.0.0.1:19091/", "gateway websocket url")
	cmd := flag.Int("cmd", 10, "protocol cmd (0-255)")
	act := flag.Int("act", 1, "protocol act (0-255)")
	index := flag.Uint("index", 1, "protocol index (0 means async)")
	data := flag.String("data", "hello", "request payload text")
	timeout := flag.Duration("timeout", 3*time.Second, "dial/read timeout")
	flag.Parse()

	if *cmd < 0 || *cmd > 255 || *act < 0 || *act > 255 {
		log.Fatalf("invalid cmd/act: cmd=%d act=%d (range: 0-255)", *cmd, *act)
	}

	dialer := websocket.Dialer{HandshakeTimeout: *timeout}
	conn, _, err := dialer.Dial(*url, nil)
	if err != nil {
		log.Fatalf("dial gateway failed: %v", err)
	}
	defer conn.Close()

	req := protocol.NewMessage(uint8(*cmd), uint8(*act), []byte(*data))
	req.Index = uint32(*index)
	payload := protocol.NewCodec().Encode(req)
	if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		log.Fatalf("write message failed: %v", err)
	}
	fmt.Printf("sent: cmd=%d act=%d index=%d data=%q\n", req.Cmd, req.Act, req.Index, string(req.Data))

	if req.Index == 0 {
		fmt.Println("async request sent, no response expected")
		return
	}

	_ = conn.SetReadDeadline(time.Now().Add(*timeout))
	_, respRaw, err := conn.ReadMessage()
	if err != nil {
		log.Fatalf("read response failed: %v", err)
	}

	resp, err := decodeSingleMessage(respRaw)
	if err != nil {
		log.Fatalf("decode response failed: %v", err)
	}

	fmt.Printf("recv: cmd=%d act=%d index=%d err=%d data=%q\n",
		resp.Cmd, resp.Act, resp.Index, resp.Error, string(resp.Data))
}

func decodeSingleMessage(buf []byte) (*protocol.Message, error) {
	if len(buf) < protocol.HeadLen {
		return nil, fmt.Errorf("payload too short: %d", len(buf))
	}
	bodyLen := binary.BigEndian.Uint32(buf[0:4])
	total := protocol.HeadLen + int(bodyLen)
	if len(buf) < total {
		return nil, fmt.Errorf("incomplete frame: have=%d need=%d", len(buf), total)
	}

	msg := &protocol.Message{
		Head: &protocol.Head{
			Len:   bodyLen,
			Cmd:   buf[4],
			Act:   buf[5],
			Error: binary.BigEndian.Uint16(buf[6:8]),
			Index: binary.BigEndian.Uint32(buf[8:12]),
		},
	}
	msg.Data = append([]byte(nil), buf[protocol.HeadLen:total]...)
	return msg, nil
}
