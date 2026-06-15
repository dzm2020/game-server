package network

import (
	"log"
)

// Logger is the pluggable logging interface for this library.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type noopLogger struct{}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}

// StdLoggerAdapter adapts the standard library logger.
type StdLoggerAdapter struct {
	L *log.Logger
}

func (s StdLoggerAdapter) Debugf(format string, args ...any) { s.output("DEBUG", format, args...) }
func (s StdLoggerAdapter) Infof(format string, args ...any)  { s.output("INFO", format, args...) }
func (s StdLoggerAdapter) Warnf(format string, args ...any)  { s.output("WARN", format, args...) }
func (s StdLoggerAdapter) Errorf(format string, args ...any) { s.output("ERROR", format, args...) }

func (s StdLoggerAdapter) output(level, format string, args ...any) {
	if s.L == nil {
		return
	}
	s.L.Printf("["+level+"] "+format, args...)
}

func ensureLogger(logger Logger) Logger {
	if logger == nil {
		return noopLogger{}
	}
	return logger
}
