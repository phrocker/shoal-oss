package accumulo

import (
	"log/slog"
	"sync/atomic"
)

// LogLevel controls process-wide diagnostics emitted by the Accumulo client.
type LogLevel int32

const (
	LogOff LogLevel = iota
	LogDebug
	LogTrace
)

var clientLogLevel atomic.Int32

// SetLogLevel sets the process-wide Accumulo client diagnostic level.
func SetLogLevel(level LogLevel) {
	if level < LogOff || level > LogTrace {
		level = LogOff
	}
	clientLogLevel.Store(int32(level))
}

// CurrentLogLevel returns the process-wide Accumulo client diagnostic level.
func CurrentLogLevel() LogLevel {
	return LogLevel(clientLogLevel.Load())
}

func logClient(level LogLevel, message string, args ...any) {
	if CurrentLogLevel() < level {
		return
	}
	slog.Debug(message, args...)
}
