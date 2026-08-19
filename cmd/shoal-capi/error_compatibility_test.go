package main

import (
	"sync"
	"testing"
)

func TestCompatibilityErrorClasses(t *testing.T) {
	tests := []struct {
		name   string
		status int32
		source int32
		compat int32
	}{
		{"client", 9, errorSourceClientException, errorCompatibilityClientException},
		{"closed", 6, errorSourceIllegalStateException, errorCompatibilityRuntimeError},
		{"cancelled", 7, errorSourceIterationInterruptedException, errorCompatibilityRuntimeError},
		{"runtime", 1, errorSourceRuntime, errorCompatibilityRuntimeError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, compat := compatibilityClassesForStatus(test.status)
			if source != test.source {
				t.Fatalf("source = %d, want %d", source, test.source)
			}
			if compat != test.compat {
				t.Fatalf("compatibility = %d, want %d", compat, test.compat)
			}
		})
	}
}

func TestCompatibilityErrorClassificationIsConcurrent(t *testing.T) {
	const goroutines = 32
	const iterations = 1024
	var wait sync.WaitGroup
	wait.Add(goroutines)
	for range goroutines {
		go func() {
			defer wait.Done()
			for range iterations {
				source, compat := compatibilityClassesForStatus(7)
				if source != errorSourceIterationInterruptedException ||
					compat != errorCompatibilityRuntimeError {
					t.Error("classification changed")
					return
				}
			}
		}()
	}
	wait.Wait()
}
