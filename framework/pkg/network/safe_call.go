package network

import "runtime/debug"

// safeCall executes fn and recovers panic for loop continuity.
func safeCall(logger Logger, fn func(), onRecover func(panicValue any, stack []byte)) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			ensureLogger(logger).Errorf("panic recovered: err=%v stack=%s", r, string(stack))
			if onRecover != nil {
				onRecover(r, stack)
			}
		}
	}()
	fn()
}
