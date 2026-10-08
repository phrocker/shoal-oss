// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package routershadow

import (
	"bufio"
	"bytes"
	"fmt"
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

// The router and its shadow service may only read. Rather than deny the
// operations known to run work (a denylist misses every one it does not
// name), the check allows by name: every use of a function, method, func-typed
// field or func variable belonging to a controlled package must be on
// authorityAllowlist, whatever form the use takes (call, method value, method
// expression, interface method, func value). The guarded packages are what
// `go list ./pkg/router/... ./internal/routershadow/...` reports, so a new
// subpackage is guarded the moment it exists.

const module = "github.com/phrocker/shoal-oss/"

// controlledPackages hold the operations that create, run, decide, register or
// mutate. Any use of their functions or methods needs an allowlist entry.
var controlledPackages = map[string]bool{
	module + "pkg/explorer":                  true,
	module + "pkg/explorer/auth":             true,
	module + "pkg/explorer/authorized":       true,
	module + "pkg/explorer/fleet":            true,
	module + "pkg/decision":                  true,
	module + "internal/decisionservice":      true,
	module + "internal/decisionregistration": true,
	module + "internal/decisionlinear":       true,
}

// authorityAllowlist is every controlled function or method the guarded
// packages may use, keyed by types.Func.FullName (or package.Name for a
// func-typed variable), with why it cannot run, create or decide anything.
var authorityAllowlist = map[string]string{
	// Reads under the caller's authorization.
	"(*" + module + "pkg/explorer/fleet.Service).List":                           "lists descriptors the caller may resolve; read-only",
	"(*" + module + "pkg/explorer/authorized.Client).ResolveMentions":            "links mentions to visible nodes; read-only",
	"(*" + module + "pkg/explorer/authorized.Client).Neighborhood":               "reads visible nodes' concept property; read-only",
	"(*" + module + "pkg/explorer/authorized.Client).AuthorizePublishedOntology": "checks visibility of a published ontology; read-only",
	"(" + module + "pkg/explorer/auth.Resolver).Resolve":                         "resolves the caller's own decision from its context",
	"(" + module + "pkg/explorer/auth.Decision).AuthorizeObject":                 "checks whether a decision profile is visible; pure",
	module + "pkg/explorer/auth.AuthorizationFingerprint":                        "hashes the caller's decision; pure",
	"(" + module + "pkg/explorer/auth.Decision).Subject":                         "accessor",
	"(" + module + "pkg/explorer/auth.Decision).RequestID":                       "accessor",
	"(" + module + "pkg/explorer/auth.Decision).CorrelationID":                   "accessor",
	"(" + module + "pkg/explorer/auth.Decision).AuthenticationExpires":           "accessor",
	// Validation that changes nothing.
	module + "pkg/explorer/fleet.ValidateActionInput": "Enqueue's own schema check and canonicalization; pure",
	// The target-choice decision, served in process: construction and
	// validation of immutable records, and the provider's pure prediction.
	module + "pkg/decision.NewTaskSpec":                          "builds an immutable task record; pure",
	module + "pkg/decision.NewPictureManifest":                   "builds an immutable picture record; pure",
	module + "pkg/decision.NewDecisionRequest":                   "builds an immutable request record; pure",
	module + "pkg/decision.NewPredictionRecord":                  "validates a response into a record; pure",
	"(" + module + "pkg/decision.TaskSpec).ID":                   "accessor",
	"(" + module + "pkg/decision.PictureManifest).ID":            "accessor",
	"(" + module + "pkg/decision.DecisionRequest).ID":            "accessor",
	"(" + module + "pkg/decision.DecisionRequest).PredictorID":   "accessor",
	"(" + module + "pkg/decision.PredictionRecord).ID":           "accessor",
	"(" + module + "pkg/decision.PredictionRecord).Config":       "accessor",
	module + "internal/decisionlinear.New":                       "loads a model from bytes; no I/O",
	"(*" + module + "internal/decisionlinear.Provider).Identity": "accessor",
	"(*" + module + "internal/decisionlinear.Provider).Resolve":  "returns the provider itself for its own release; no I/O",
	"(" + module + "internal/decisionservice.Predictor).Predict": "the linear model's margin over the feature artifact; pure",
}

// forbiddenImports may not be imported by a guarded package: they bypass
// type-based resolution (unsafe, reflect, plugin) or reach outside the process.
var forbiddenImports = map[string]bool{
	"unsafe": true, "reflect": true, "plugin": true, "os/exec": true, "syscall": true,
	"net": true, "net/http": true, "net/rpc": true,
}

// allowedModuleImports are the module packages a guarded package may import.
// A wrapper elsewhere in the module could call a controlled operation on the
// router's behalf; the guard does not follow calls into other packages, so it
// limits which ones may be imported at all. Those listed hold data and pure
// functions, or are controlled and checked use by use.
var allowedModuleImports = map[string]bool{
	module + "pkg/document": true, module + "pkg/extraction": true, module + "pkg/graph": true,
	module + "pkg/inference": true, module + "pkg/lexicon": true, module + "pkg/ontology": true,
	module + "pkg/shoal": true,
}

type finding struct {
	pos, what string
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(strings.TrimSpace(string(out)))
}

type listedPackage struct {
	path, dir string
}

// guardedPackages is the guarded set, from go list, so it cannot go stale.
func guardedPackages(t *testing.T, root string) []listedPackage {
	t.Helper()
	cmd := exec.Command("go", "list", "-f", "{{.ImportPath}}\t{{.Dir}}", "./pkg/router/...", "./internal/routershadow/...")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	var pkgs []listedPackage
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		path, dir, ok := strings.Cut(line, "\t")
		if ok {
			pkgs = append(pkgs, listedPackage{path, dir})
		}
	}
	return pkgs
}

// exportImporter resolves imports from compiled export data located with go
// list, so uses are resolved by the compiler's own types. sources maps import
// paths to directories type-checked from source instead (fixtures).
func exportImporter(t *testing.T, root string, fset *token.FileSet, sources map[string]string, patterns ...string) types.Importer {
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
	gc := importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("no export data for %s", path)
		}
		return os.Open(file)
	})
	imp := &sourceImporter{fset: fset, sources: sources, fallback: gc, done: map[string]*types.Package{}}
	return imp
}

type sourceImporter struct {
	fset     *token.FileSet
	sources  map[string]string
	fallback types.Importer
	done     map[string]*types.Package
}

func (s *sourceImporter) Import(path string) (*types.Package, error) {
	if path == "unsafe" {
		return types.Unsafe, nil
	}
	if p, ok := s.done[path]; ok {
		return p, nil
	}
	dir, ok := s.sources[path]
	if !ok {
		return s.fallback.Import(path)
	}
	files, err := parseDir(s.fset, dir)
	if err != nil {
		return nil, err
	}
	p, err := (&types.Config{Importer: s}).Check(path, s.fset, files, nil)
	if err != nil {
		return nil, err
	}
	s.done[path] = p
	return p, nil
}

func parseDir(fset *token.FileSet, dir string) ([]*ast.File, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no Go files in %s", dir)
	}
	return files, nil
}

func controlled(p *types.Package) bool { return p != nil && controlledPackages[p.Path()] }

// mentionsControlled reports whether a type is, or is a signature whose
// parameters or results mention, a named type from a controlled package.
func mentionsControlled(t types.Type, depth int) bool {
	if depth > 8 || t == nil {
		return false
	}
	switch t := t.(type) {
	case *types.Named:
		if controlled(t.Obj().Pkg()) {
			return true
		}
		if sig, ok := t.Underlying().(*types.Signature); ok {
			return mentionsControlled(sig, depth+1)
		}
	case *types.Pointer:
		return mentionsControlled(t.Elem(), depth+1)
	case *types.Slice:
		return mentionsControlled(t.Elem(), depth+1)
	case *types.Signature:
		for _, tuple := range []*types.Tuple{t.Params(), t.Results()} {
			for i := 0; i < tuple.Len(); i++ {
				if mentionsControlled(tuple.At(i).Type(), depth+1) {
					return true
				}
			}
		}
		if t.Recv() != nil {
			return mentionsControlled(t.Recv().Type(), depth+1)
		}
	}
	return false
}

func isFunc(t types.Type) (*types.Signature, bool) {
	sig, ok := t.Underlying().(*types.Signature)
	return sig, ok
}

// authorityFindings type-checks one guarded package from source and reports
// every use the policy does not allow.
func authorityFindings(t *testing.T, fset *token.FileSet, imp types.Importer, guarded map[string]bool, dir, path string, used map[string]bool) []finding {
	t.Helper()
	files, err := parseDir(fset, dir)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{
		Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{},
		Selections: map[*ast.SelectorExpr]*types.Selection{},
	}
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
	checkObj := func(pos token.Pos, obj types.Object) {
		switch obj := obj.(type) {
		case *types.Func:
			if _, ok := authorityAllowlist[obj.FullName()]; ok {
				used[obj.FullName()] = true
				return
			}
			if controlled(obj.Pkg()) {
				report(pos, "use of "+obj.FullName())
				return
			}
			// An interface declared anywhere else can be satisfied by a
			// controlled type, so calling one of its methods over controlled
			// types may reach a controlled operation.
			sig := obj.Type().(*types.Signature)
			if sig.Recv() != nil && types.IsInterface(sig.Recv().Type()) && mentionsControlled(sig, 0) {
				report(pos, "use of interface method "+obj.FullName()+" over controlled types")
			}
		case *types.Var:
			if _, ok := isFunc(obj.Type()); !ok {
				return
			}
			name := obj.Name()
			if obj.Pkg() != nil {
				name = obj.Pkg().Path() + "." + obj.Name()
			}
			if _, ok := authorityAllowlist[name]; ok {
				used[name] = true
				return
			}
			declaredOutside := obj.Pkg() != nil && !guarded[obj.Pkg().Path()]
			packageLevel := obj.Pkg() != nil && obj.Parent() == obj.Pkg().Scope()
			switch {
			case controlled(obj.Pkg()):
				report(pos, "use of func value "+name)
			case declaredOutside && (packageLevel || obj.IsField()):
				report(pos, "use of func value "+name+" declared outside the guarded packages")
			case mentionsControlled(obj.Type(), 0):
				report(pos, "func value "+name+" of a controlled type")
			}
		}
	}
	for ident, obj := range info.Uses {
		checkObj(ident.Pos(), obj)
	}
	for ident, obj := range info.Defs {
		if v, ok := obj.(*types.Var); ok && mentionsControlled(v.Type(), 0) {
			if _, isSig := isFunc(v.Type()); isSig {
				report(ident.Pos(), "stored func value "+v.Name()+" of a controlled type")
			}
		}
	}
	for sel, selection := range info.Selections {
		checkObj(sel.Sel.Pos(), selection.Obj())
	}
	for _, f := range files {
		for _, imp := range f.Imports {
			p, _ := strconv.Unquote(imp.Path.Value)
			switch {
			case forbiddenImports[p]:
				report(imp.Pos(), "import of "+p)
			case strings.HasPrefix(p, module) && !guarded[p] && !controlledPackages[p] && !allowedModuleImports[p]:
				report(imp.Pos(), "import of module package "+p+" outside the allowed set")
			case !strings.Contains(strings.SplitN(p, "/", 2)[0], ".") || strings.HasPrefix(p, module):
			default:
				report(imp.Pos(), "import of external package "+p)
			}
		}
		for _, group := range f.Comments {
			for _, c := range group.List {
				if strings.Contains(c.Text, "go:linkname") {
					report(c.Pos(), "go:linkname directive")
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pos < out[j].pos })
	return out
}

func checkGuarded(t *testing.T, root string, pkgs []listedPackage, used map[string]bool) []finding {
	t.Helper()
	fset := token.NewFileSet()
	guarded := map[string]bool{}
	var patterns []string
	for _, p := range pkgs {
		guarded[p.path] = true
		patterns = append(patterns, p.path)
	}
	imp := exportImporter(t, root, fset, nil, patterns...)
	var all []finding
	for _, p := range pkgs {
		all = append(all, authorityFindings(t, fset, imp, guarded, p.dir, p.path, used)...)
	}
	return all
}

// TestRouterNeverRunsAnything fails on any use the policy does not allow in
// any package under pkg/router or internal/routershadow.
func TestRouterNeverRunsAnything(t *testing.T) {
	root := moduleRoot(t)
	pkgs := guardedPackages(t, root)
	if len(pkgs) < 3 {
		t.Fatalf("guarded packages = %v", pkgs)
	}
	used := map[string]bool{}
	for _, f := range checkGuarded(t, root, pkgs, used) {
		t.Errorf("%s: %s", f.pos, f.what)
	}
	// An entry nothing uses is a permission nobody needs: remove it.
	for name := range authorityAllowlist {
		if !used[name] {
			t.Errorf("allowlist entry %s is unused", name)
		}
	}
}

// TestAuthorityCheckFlagsViolations proves the check can fail. The fixture
// under testdata/violating reaches controlled operations every way the
// review named, through a subpackage and through a func variable declared in
// another package, and each is flagged.
func TestAuthorityCheckFlagsViolations(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	base := filepath.Join(root, "internal", "routershadow", "testdata")
	sources := map[string]string{
		"example.test/violating":     filepath.Join(base, "violating"),
		"example.test/violating/sub": filepath.Join(base, "violating", "sub"),
		"example.test/elsewhere":     filepath.Join(base, "elsewhere"),
	}
	imp := exportImporter(t, root, fset, sources, "./pkg/explorer/fleet", "./pkg/decision", "context", "reflect", "os/exec")
	guarded := map[string]bool{"example.test/violating": true, "example.test/violating/sub": true}
	var findings []finding
	// The fixture's subpackages are found by walking it, as go list finds a
	// real one.
	_ = filepath.WalkDir(sources["example.test/violating"], func(dir string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return err
		}
		rel, _ := filepath.Rel(sources["example.test/violating"], dir)
		path := "example.test/violating"
		if rel != "." {
			path += "/" + filepath.ToSlash(rel)
		}
		findings = append(findings, authorityFindings(t, fset, imp, guarded, dir, path, map[string]bool{})...)
		return nil
	})
	fleetPkg := module + "pkg/explorer/fleet"
	want := []string{
		"use of (*" + fleetPkg + ".DispatchService).Enqueue",      // direct, method value, method expression
		"use of (*" + fleetPkg + ".DispatchService).Invoke",       // method value
		"use of (*" + fleetPkg + ".DispatchService).ExecuteClaim", // claim and run
		"use of (*" + fleetPkg + ".DispatchService).Claim",
		"use of (*" + fleetPkg + ".DispatchService).Cancel",
		"use of (*" + fleetPkg + ".Service).Register",
		"use of (*" + fleetPkg + ".Service).Revoke",
		"use of (*" + fleetPkg + ".Service).Heartbeat",
		"use of interface method (example.test/violating.enqueuer).Enqueue over controlled types", // a local interface
		"stored func value enqueueVar of a controlled type",                                       // package-level func var
		"use of func value example.test/elsewhere.Enqueue declared outside the guarded packages",  // bound elsewhere
		"import of reflect",
		"import of unsafe",
		"import of os/exec",
		"go:linkname directive",
		"use of (*" + fleetPkg + ".DispatchService).CompleteClaim", // in the subpackage
	}
	got := map[string]int{}
	for _, f := range findings {
		got[f.what]++
	}
	for _, w := range want {
		if got[w] == 0 {
			t.Errorf("violation %q was not flagged", w)
		}
	}
	if got["use of (*"+fleetPkg+".DispatchService).Enqueue"] < 3 {
		t.Errorf("direct call, method value and method expression of Enqueue not all flagged")
	}
	if t.Failed() {
		for _, f := range findings {
			t.Logf("finding %s: %s", f.pos, f.what)
		}
	}
}

// TestNewSubpackageIsGuarded: a package added under pkg/router is in the
// guarded set without editing this test, and its violation is flagged.
func TestNewSubpackageIsGuarded(t *testing.T) {
	root := moduleRoot(t)
	name := fmt.Sprintf("guardprobe%d", os.Getpid())
	dir := filepath.Join(root, "pkg", "router", name)
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	src := `package ` + name + `

import (
	"context"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

func Probe(ctx context.Context, s *fleet.DispatchService, r fleet.ActionRecord) {
	_, _ = s.ExecuteClaim(ctx, r)
}
`
	if err := os.WriteFile(filepath.Join(dir, "probe.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	pkgs := guardedPackages(t, root)
	found := false
	for _, p := range pkgs {
		found = found || p.path == module+"pkg/router/"+name
	}
	if !found {
		t.Fatalf("new subpackage not guarded: %v", pkgs)
	}
	flagged := false
	for _, f := range checkGuarded(t, root, pkgs, map[string]bool{}) {
		flagged = flagged || (strings.Contains(f.pos, name) && strings.Contains(f.what, "ExecuteClaim"))
	}
	if !flagged {
		t.Fatal("the new subpackage's ExecuteClaim was not flagged")
	}
}
