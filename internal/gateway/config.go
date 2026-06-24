package gateway

type Options struct {
	Enable          bool
	Address         string
	Network         string
	SendBuffer      int
	ReadLimit       int64
	WsPongWaitSec   int
	WsPingPeriodSec int
	WriteWaitSec    int
	WsPath          string
}

func DefaultConfig() *Options {
	return &Options{
		Enable:  true,
		Address: "127.0.0.1:9000",
		Network: "ws",
		WsPath:  "/ws",
	}
}
