// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

// Package executorreftest holds the executor-reference probe table that the
// rule's own test and the cross-package parity test both run, so every place
// that validates an executor reference is checked against the same rows.
package executorreftest

import (
	"strings"

	"github.com/phrocker/shoal-oss/pkg/executorref"
)

// Probe is one executor reference and whether it is valid.
type Probe struct {
	Name  string
	Ref   string
	Valid bool
}

// Probes returns the table: look-alike pairs (a refused non-normal or
// invisible form beside the accepted plain form it imitates), the bounds, and
// an NFKC-stable non-ASCII reference that must be accepted everywhere.
func Probes() []Probe {
	return []Probe{
		{"plain ASCII", "worker-a", true},
		{"ASCII with an inner space", "worker a", true},
		{"precomposed é (NFKC-stable)", "café", true},
		{"decomposed e + U+0301", "café", false},
		{"plain fi", "file-worker", true},
		{"ﬁ ligature U+FB01", "ﬁle-worker", false},
		{"full-width letter U+FF57", "ｗorker-a", false},
		{"tab", "worker\tA", false},
		{"zero-width space U+200B", "worker​A", false},
		{"NBSP U+00A0", "worker A", false},
		{"em space U+2003", "worker A", false},
		{"bidi override U+202E", "worker‮A", false},
		{"combining mark alone", "́", false},
		{"leading combining mark", "́worker", false},
		{"empty", "", false},
		{"surrounding space", " worker", false},
		{"invalid UTF-8", "worker\xff", false},
		{"at the bound", strings.Repeat("w", executorref.MaxExecutorRefBytes), true},
		{"over the bound", strings.Repeat("w", executorref.MaxExecutorRefBytes+1), false},
	}
}
