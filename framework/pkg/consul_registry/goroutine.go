package consulregistry

import (
	"fmt"
	"runtime/debug"
)

// safeGo starts a goroutine with panic recovery and logging.
func safeGo(logger Logger, name string, fn func()) {
	l := ensureLogger(logger)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				l.Errorf("协程 panic 已恢复, name=%s err=%v stack=%s", name, rec, string(debug.Stack()))
			}
		}()
		fn()
	}()
}

func goName(base string, suffix any) string {
	return fmt.Sprintf("%s[%v]", base, suffix)
}
