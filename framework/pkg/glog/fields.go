package glog

import (
	"go.uber.org/zap"
)

const (
	FieldNodeID    = "node_id"
	FieldComponent = "component"
	FieldConnID    = "conn_id"
	FieldPID       = "pid"
	FieldErr       = "err"
)

func NodeID(nodeID string) zap.Field {
	return zap.String(FieldNodeID, nodeID)
}

func Component(component string) zap.Field {
	return zap.String(FieldComponent, component)
}

func ConnID(connID int64) zap.Field {
	return zap.Int64(FieldConnID, connID)
}

func PID(pid any) zap.Field {
	return zap.Any(FieldPID, pid)
}

func Err(err error) zap.Field {
	return zap.NamedError(FieldErr, err)
}
