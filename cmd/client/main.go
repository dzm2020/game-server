package main

import (
	"flag"
	"fmt"
	"game-server/framework/gen"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	addr := flag.String("addr", "ws://127.0.0.1:7000/ws", "gateway websocket address")
	cmd := flag.Int("cmd", 1, "message cmd")
	act := flag.Int("act", 1, "message act")
	payload := flag.String("payload", "hello from cmd/client", "message payload")
	count := flag.Int("count", 1, "send message count")
	interval := flag.Duration("interval", 500*time.Millisecond, "interval between requests")
	timeout := flag.Duration("timeout", 5*time.Second, "read/write timeout")
	flag.Parse()

	conn, _, err := websocket.DefaultDialer.Dial(*addr, nil)
	if err != nil {
		log.Fatalf("连接网关失败: %v", err)
	}
	defer func() { _ = conn.Close() }()

	log.Printf("连接成功: %s", *addr)

	for i := 0; i < *count; i++ {
		req := gen.NewMessage(uint8(*cmd), uint8(*act), []byte(*payload))
		req.Index = uint32(i + 1)
		reqBytes, err := gen.Encode(req)
		if err != nil {
			log.Fatalf("编码请求失败: %v", err)
		}

		_ = conn.SetWriteDeadline(time.Now().Add(*timeout))
		if err := conn.WriteMessage(websocket.BinaryMessage, reqBytes); err != nil {
			log.Fatalf("发送请求失败: %v", err)
		}
		log.Printf("已发送: cmd=%d act=%d index=%d payload=%q", req.Cmd, req.Act, req.Index, req.Data)

		_ = conn.SetReadDeadline(time.Now().Add(*timeout))
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("读取回复失败: %v", err)
		}
		if msgType != websocket.BinaryMessage {
			log.Fatalf("收到非二进制消息: type=%d", msgType)
		}

		resp, n, err := gen.Decode(data)
		if err != nil {
			log.Fatalf("解码回复失败: %v", err)
		}
		if n == 0 || resp == nil {
			log.Fatalf("回复数据不完整: len=%d", len(data))
		}

		fmt.Printf("收到回复: cmd=%d act=%d index=%d err=%d payload=%q\n",
			resp.Cmd, resp.Act, resp.Index, resp.Error, string(resp.Data))

		if i < *count-1 {
			time.Sleep(*interval)
		}
	}
}
