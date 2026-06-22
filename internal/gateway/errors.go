package gateway

import "errors"

var (
	ErrConfigNil             = errors.New(" config is nil")
	ErrSystemComponentAbsent = errors.New("gateway depends on system component")
	ErrComponentNotInited    = errors.New("gateway component is not initialized")
	ErrFactoryNotConfigured  = errors.New("gateway client agent factory is not configured")
	ErrSessionActorNotReady  = errors.New("gateway session actor is not ready")
	ErrServerNil             = errors.New("gateway websocket server is nil")
	ErrServerExitedEarly     = errors.New("gateway server exited before ready")
	ErrClientAgentNotFound   = errors.New("gateway client agent not found")
)
