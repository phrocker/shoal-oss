package engine

import (
	"path/filepath"
	"testing"
)

func TestJoinBackendPathPreservesCustomSchemeRoots(t *testing.T) {
	got := joinBackendPath("custom+backend://bucket/prefix", "nested/F0001.rf")
	if got != "custom+backend://bucket/prefix/nested/F0001.rf" {
		t.Fatalf("joinBackendPath(custom scheme) = %q, want %q", got, "custom+backend://bucket/prefix/nested/F0001.rf")
	}
}

func TestJoinBackendPathTreatsWindowsDriveRootsAsLocal(t *testing.T) {
	got := joinBackendPath(`C://bulk`, "F0001.rf")
	want := filepath.Join(`C://bulk`, "F0001.rf")
	if got != want {
		t.Fatalf("joinBackendPath(%q, %q) = %q, want %q", `C://bulk`, "F0001.rf", got, want)
	}
	if got == `C://bulk/F0001.rf` {
		t.Fatalf("joinBackendPath treated %q as a backend URL root", `C://bulk`)
	}
}
