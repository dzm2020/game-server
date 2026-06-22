package grs

import (
	"game-server/framework/pkg/glog"
	"runtime/debug"

	"go.uber.org/zap"
)

// SafeGo starts a goroutine with panic recovery and logging.
func SafeGo(fn func()) {
	go func() {
		Try(fn, nil)
	}()
}

func Try(fn func(), recoverFunc func(e error)) {
	defer func() {
		if rec := recover(); rec != nil {
			if err, ok := rec.(error); ok {
				if recoverFunc != nil {
					recoverFunc(err)
				}
			}
			glog.Error("panic recovered", zap.Any("panic", rec), zap.ByteString("stack", debug.Stack()))
		}
	}()
	if fn != nil {
		fn()
	}
}
