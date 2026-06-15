package network

import (
	"encoding/json"
	"errors"
)

// -------------------------------------------
// 可自定义的类型：消息类型、编解码器、回调
// -------------------------------------------

// Message 业务消息体（由外部定义，这里仅作示例）
type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

var (
	ErrCodecNotConfigured  = errors.New("codec is not configured")
	ErrConnectionClosed    = errors.New("connection is closed")
	ErrTLSCertFileRequired = errors.New("tls cert file is required")
	ErrTLSKeyFileRequired  = errors.New("tls key file is required")
)

type IConnection interface {
	ID() uint64
	LocalAddr() string
	RemoteAddr() string
	Send(v interface{}) error
	Close()
	Available() bool
}

// Codec 定义 WebSocket 业务编解码接口。
type Codec interface {
	// Decode 解包：将原始 WebSocket 数据解析成业务消息
	Decode(data []byte) (interface{}, error)
	// Encode 打包：将业务消息编码为可发送的字节流
	Encode(v interface{}) ([]byte, error)
}

type jsonCodec struct{}

func (jsonCodec) Decode(data []byte) (interface{}, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (jsonCodec) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// EventHandler 定义连接与消息事件回调。
type EventHandler interface {
	// OnConnect 在连接建立成功后触发
	OnConnect(conn IConnection)
	// OnDisconnect 在连接断开后触发，err 为断开原因（正常断开可能为 nil）
	OnDisconnect(conn IConnection, err error)
	// OnMessage 在消息解包成功后触发
	OnMessage(conn IConnection, msg interface{})
}

// BaseEventHandler 提供空实现，业务可按需覆写。
type BaseEventHandler struct{}

func (BaseEventHandler) OnConnect(_ IConnection)                {}
func (BaseEventHandler) OnDisconnect(_ IConnection, _ error)    {}
func (BaseEventHandler) OnMessage(_ IConnection, _ interface{}) {}
