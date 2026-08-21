package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/deployment"
)

func TestPlanCommandDeterministic(t *testing.T) {
	var first, second, stderr bytes.Buffer
	args := []string{"plan", "--mode", "distributed"}
	if err := run(args, &first, &stderr); err != nil {
		t.Fatalf("run plan: %v; stderr: %s", err, stderr.String())
	}
	stderr.Reset()
	if err := run(args, &second, &stderr); err != nil {
		t.Fatalf("run plan again: %v; stderr: %s", err, stderr.String())
	}
	if first.String() != second.String() {
		t.Fatalf("plan output changed:\n%s\n%s", first.String(), second.String())
	}
	for _, want := range []string{
		"mode: distributed",
		"authority: shoal-coordinated",
		"Kubernetes Lease coordination is not implemented",
	} {
		if !strings.Contains(first.String(), want) {
			t.Fatalf("plan output %q missing %q", first.String(), want)
		}
	}
}

func TestStatusAndTransitionCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := deployment.NewState(deployment.ModeSingle)
	if err != nil {
		t.Fatal(err)
	}
	state.LastValidation = "healthy"
	if err := deployment.Save(path, state); err != nil {
		t.Fatal(err)
	}

	var status, stderr bytes.Buffer
	if err := run([]string{"status", "--state", path}, &status, &stderr); err != nil {
		t.Fatalf("status: %v; stderr: %s", err, stderr.String())
	}
	for _, want := range []string{
		"current mode: single",
		"desired mode: single",
		"phase: ready",
		"authority: local",
		"generation: 0",
		"last validation: healthy",
	} {
		if !strings.Contains(status.String(), want) {
			t.Fatalf("status output %q missing %q", status.String(), want)
		}
	}

	var transition bytes.Buffer
	stderr.Reset()
	if err := run([]string{"transition", "--state", path, "--to", "accumulo"}, &transition, &stderr); err != nil {
		t.Fatalf("transition: %v; stderr: %s", err, stderr.String())
	}
	for _, want := range []string{
		"accepted: true",
		"desired mode: accumulo",
		"phase: quiescing",
		"authority: local",
		"generation: 1",
		"quiesce all writes",
		"does not migrate data or switch live authority",
	} {
		if !strings.Contains(transition.String(), want) {
			t.Fatalf("transition output %q missing %q", transition.String(), want)
		}
	}

	var replay bytes.Buffer
	stderr.Reset()
	if err := run([]string{"transition", "--state", path, "--to", "accumulo"}, &replay, &stderr); err != nil {
		t.Fatalf("replay: %v; stderr: %s", err, stderr.String())
	}
	if !strings.Contains(replay.String(), "idempotent: true") ||
		!strings.Contains(replay.String(), "generation: 1") {
		t.Fatalf("replay output = %q", replay.String())
	}
}

func TestCommandErrors(t *testing.T) {
	tests := [][]string{
		nil,
		{"unknown"},
		{"plan", "--mode", "bad"},
		{"status"},
		{"transition", "--state", "missing.json", "--to", "single"},
	}
	for _, args := range tests {
		if err := run(args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("run(%q) succeeded", args)
		}
	}
}
