// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package routershadow

import (
	"bufio"
	"bytes"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// forbidden are the operations through which work runs or is created. The
// router and its shadow service must never reach any of them: a proposal is
// only a proposal.
var forbidden = map[string]bool{"Enqueue": true, "Invoke": true, "Evaluate": true, "Register": true}

// guardedPackages are checked. Paths are relative to the module root.
var guardedPackages = []string{"pkg/router", "pkg/router/eval", "internal/routershadow"}

func moduleRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(strings.TrimSpace(string(out)))
}

// exportImporter type-checks against compiled export data from the build
// cache, located with go list, so resolution is by the compiler's own types.
func exportImporter(t *testing.T, root string, fset *token.FileSet, patterns ...string) types.Importer {
	t.Helper()
	args := append([]string{"list", "-export", "-deps", "-f", "{{.ImportPath}}\t{{.Export}}"}, patterns...)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	exports := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		path, file, ok := strings.Cut(scanner.Text(), "\t")
		if ok && file != "" {
			exports[path] = file
		}
	}
	return importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return os.Open(file)
	})
}

type finding struct {
	pos, what string
}

// forbiddenUses type-checks the non-test Go files of dir and reports every
// resolved use of a forbidden function or method, however it is reached: a
// call, a method value, a method expression, or an interface method; and
// every string literal naming one, which is how reflection would reach it.
func forbiddenUses(t *testing.T, fset *token.FileSet, imp types.Importer, dir, path string) []finding {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatalf("no Go files in %s", dir)
	}
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	if _, err := (&types.Config{Importer: imp}).Check(path, fset, files, info); err != nil {
		t.Fatalf("type-check %s: %v", path, err)
	}
	seen := map[string]bool{}
	var out []finding
	report := func(pos token.Pos, what string) {
		p := fset.Position(pos).String()
		if !seen[p+what] {
			seen[p+what] = true
			out = append(out, finding{p, what})
		}
	}
	for ident, obj := range info.Uses {
		if fn, ok := obj.(*types.Func); ok && forbidden[fn.Name()] {
			report(ident.Pos(), fn.FullName())
		}
	}
	for sel, selection := range info.Selections {
		if fn, ok := selection.Obj().(*types.Func); ok && forbidden[fn.Name()] {
			report(sel.Sel.Pos(), fn.FullName())
		}
	}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if s, err := strconv.Unquote(lit.Value); err == nil && forbidden[s] {
					report(lit.Pos(), "string literal "+lit.Value)
				}
			}
			return true
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

// TestRouterNeverRunsAnything fails if the router, its evaluation harness or
// its shadow service reaches Enqueue, Invoke, Evaluate or Register.
func TestRouterNeverRunsAnything(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	var patterns []string
	for _, p := range guardedPackages {
		patterns = append(patterns, "./"+p)
	}
	imp := exportImporter(t, root, fset, patterns...)
	for _, p := range guardedPackages {
		for _, f := range forbiddenUses(t, fset, imp, filepath.Join(root, p), "github.com/phrocker/shoal-oss/"+p) {
			t.Errorf("%s reaches %s", f.pos, f.what)
		}
	}
}

// TestAuthorityCheckFlagsViolations proves the check can fail: a fixture that
// reaches the forbidden operations directly, as a method value, as a method
// expression, through an interface, on the registry and by reflection is
// flagged at every one.
func TestAuthorityCheckFlagsViolations(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	imp := exportImporter(t, root, fset, "./pkg/explorer/fleet", "reflect", "context")
	findings := forbiddenUses(t, fset, imp, filepath.Join("testdata", "violating"), "example.test/violating")
	want := []string{
		"(*github.com/phrocker/shoal-oss/pkg/explorer/fleet.DispatchService).Enqueue", // direct
		"(*github.com/phrocker/shoal-oss/pkg/explorer/fleet.DispatchService).Invoke",  // method value
		"(example.test/violating.enqueuer).Enqueue",                                   // interface
		"(*github.com/phrocker/shoal-oss/pkg/explorer/fleet.Service).Register",        // registry
		`string literal "Enqueue"`,                                                    // reflection
	}
	got := map[string]int{}
	for _, f := range findings {
		got[f.what]++
	}
	for _, w := range want {
		if got[w] == 0 {
			t.Errorf("violation %s was not flagged; findings: %v", w, findings)
		}
	}
	// Direct call and method expression both name DispatchService.Enqueue.
	if got[want[0]] < 2 {
		t.Errorf("method expression not flagged separately from the direct call: %v", findings)
	}
}
