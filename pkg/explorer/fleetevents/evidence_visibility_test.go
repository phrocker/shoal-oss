// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleetevents

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/interaction"
)

// TestEvidenceVisibilityMustAlignAndBeCanonical pins the publish-time shape
// the delivery check relies on (#562): an expression per reference, or none
// at all, each canonical. A misaligned group could hand a reference another
// reference's label, so it is refused rather than guessed at.
func TestEvidenceVisibilityMustAlignAndBeCanonical(t *testing.T) {
	two := []interaction.EvidenceReference{{AnchorID: "a"}, {AnchorID: "b"}}
	for _, probe := range []struct {
		name       string
		visibility [][]string
		ok         bool
	}{
		{name: "no labels at all", ok: true},
		{name: "one labelled, one not",
			visibility: [][]string{nil, {"project-x", "secret"}}, ok: true},
		{name: "fewer expressions than references",
			visibility: [][]string{{"secret"}}},
		{name: "a non-canonical expression",
			visibility: [][]string{nil, {"secret", "project-x"}}},
	} {
		t.Run(probe.name, func(t *testing.T) {
			err := validateEvidenceVisibility(two, probe.visibility)
			if (err == nil) != probe.ok {
				t.Fatalf("validateEvidenceVisibility = %v, want ok=%v",
					err, probe.ok)
			}
		})
	}
}
