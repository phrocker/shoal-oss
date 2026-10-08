// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package importboundary

import (
	"io/fs"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func check(t *testing.T, fsys fs.FS) []Violation {
	t.Helper()
	got, err := Check(fsys)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(got, func(i, j int) bool { return got[i].String() < got[j].String() })
	return got
}

// TestRepositoryRespectsBoundary runs the rules over the real tree. It reads
// source files directly, so it behaves the same with GOWORK=off.
func TestRepositoryRespectsBoundary(t *testing.T) {
	root := os.DirFS("../..")
	for _, v := range check(t, root) {
		t.Error(v)
	}
	// The allowlist must name real packages, or rule C passes vacuously.
	for _, pkg := range Allowlist {
		dir := pkg[len(Module)+1:]
		matches, err := fs.Glob(root, path.Join(dir, "*.go"))
		if err != nil || len(matches) == 0 {
			t.Errorf("allowlisted package %s has no Go files", pkg)
		}
	}
	// Rule A must reach root-module packages outside pkg/, internal/, cmd/.
	outside := 0
	if err := walkGo(root, ".", func(dir string) bool { return dir == "extensions" }, func(file string, _ []string) error {
		if top, _, _ := strings.Cut(file, "/"); top != "pkg" && top != "internal" && top != "cmd" {
			outside++
		}
		return nil
	}); err != nil || outside == 0 {
		t.Errorf("rule A scanned no files outside pkg/internal/cmd (%v)", err)
	}
	// And the scaffolding the rules exist for must be present.
	if mods, _ := fs.Glob(root, "extensions/*/go.mod"); len(mods) == 0 {
		t.Error("no extension module found; rule B checked nothing")
	}
}

func TestFixturesDetectEachDirection(t *testing.T) {
	const m = Module
	cases := map[string][]Violation{
		"clean":                  nil,
		"core-imports-extension": {{"A", "cmd/tool/main.go", m + "/extensions/good/lines"}},
		"extension-imports-internal": {
			{"B", "extensions/bad/main.go", m + "/internal/engine"},
			{"B", "extensions/bad/main.go", m + "/pkg/explorer/webapi"},
		},
		"sdk-leaks-internal": {{"C", "pkg/helper/helper.go", m + "/internal/secret"}},
		// A module naming itself into the repository cannot exempt its imports.
		"extension-module-spoof": {{"B", "extensions/spoof/go.mod", "module " + m + "/internal (want " + m + "/extensions/spoof)"}},
		"extension-bad-replace": {
			{"B", "extensions/r/go.mod", "replace " + m + " => ../../../elsewhere"},
			{"B", "extensions/r/go.mod", "replace golang.org/x/crypto => ./fork"},
		},
		"nested-extension-module":        {{"B", "extensions/grp/evil/main.go", m + "/internal/engine"}},
		"root-package-imports-extension": {{"A", "showcases/demo/main.go", m + "/extensions/good/lines"}},
		"loose-extension-file":           {{"B", "extensions/loose/loose.go", "(file outside an extension module)"}},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			sub, err := fs.Sub(os.DirFS("testdata"), name)
			if err != nil {
				t.Fatal(err)
			}
			if got := check(t, sub); !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}
