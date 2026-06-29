package gen

import (
	"go.uber.org/zap"
)

const (
	FieldNodeIDKey    = "node_id"
	FieldComponentKey = "component"
	FieldConnIDKey    = "conn_id"
	FieldPIDKey       = "pid"
	FieldErrKey       = "err"
)

func FieldNodeID(nodeID string) zap.Field {
	return zap.String(FieldNodeIDKey, nodeID)
}

func FieldComponent(component string) zap.Field {
	return zap.String(FieldComponentKey, component)
}

func FieldConnID(connID int64) zap.Field {
	return zap.Int64(FieldConnIDKey, connID)
}

func FieldPID(pid any) zap.Field {
	return zap.Any(FieldPIDKey, pid)
}

func FieldErr(err error) zap.Field {
	return zap.NamedError(FieldErrKey, err)
}
