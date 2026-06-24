package gateway

type Options struct {
	Address         string
	SendBuffer      int
	ReadLimit       int64
	WsPongWaitSec   int
	WsPingPeriodSec int
	WriteWaitSec    int
	WsPath          string
}
