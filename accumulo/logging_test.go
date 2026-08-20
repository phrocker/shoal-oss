package accumulo

import "testing"

func TestClientLogLevelIsProcessWideAndValidated(t *testing.T) {
	t.Cleanup(func() { SetLogLevel(LogOff) })
	SetLogLevel(LogDebug)
	if got := CurrentLogLevel(); got != LogDebug {
		t.Fatalf("log level = %v, want debug", got)
	}
	SetLogLevel(LogTrace)
	if got := CurrentLogLevel(); got != LogTrace {
		t.Fatalf("log level = %v, want trace", got)
	}
	SetLogLevel(LogLevel(99))
	if got := CurrentLogLevel(); got != LogOff {
		t.Fatalf("invalid log level = %v, want off", got)
	}
}
