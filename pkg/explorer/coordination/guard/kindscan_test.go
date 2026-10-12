// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package guard

// This file finds every guard entity kind the code can write, by type
// checking the sources rather than by matching their spelling.
//
// The analysis starts at the field Entity.Kind and works backwards. Every
// value that can reach that field is followed to its origin:
//   - A composite literal, an assignment or a struct conversion.
//   - A variable or parameter. For a parameter, this means every call site
//     of its function, including calls made through an interface the
//     function's receiver satisfies.
//   - A field of another struct, such as a request type carrying a kind.
//   - The returns of a function.
//
// A value that type checks to a constant is recorded as the byte it
// evaluates to, in the file where the value enters this chain. That holds
// however the constant is spelled: 'P', 0x50, byte(0x41), an untyped or
// typed constant, or 'A'+1.
//
// A value the analysis cannot follow is reported as a problem rather than
// skipped. That covers:
//   - A map or slice element, or arithmetic on non-constants.
//   - A function value, a closure parameter, or a call it cannot resolve.
//   - An address-taken variable, a compound assignment, or a range variable.
//   - A struct type identical to Entity but distinct from it.
//
// The analysis does not see reflection or unsafe; nothing outside the
// guard's own decoders writes kinds that way.
//
// The packages analysed are those in this repository's modules (the root
// module and every module under extensions/) that depend on this package,
// and this package itself. Only they can name Entity, or call something that
// does. They are type checked from source. Their dependencies are read from
// the export data `go list -export` builds, so the analysis needs no module
// outside the standard library.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/constant"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

const guardImportPath = "github.com/phrocker/shoal-oss/pkg/explorer/coordination/guard"

// decodedKinds are the functions allowed to write Entity.Kind from a
// non-constant. Each one decodes an entity that was already written, so it
// introduces no kind of its own. The key is the file, relative to the
// repository root, and the function name.
var decodedKinds = map[string]string{
	"pkg/explorer/coordination/guard/codec.go:decodeIntent": "decodes a stored intent",
}

type listedPackage struct {
	ImportPath     string
	Dir            string
	GoFiles        []string
	CgoFiles       []string
	IgnoredGoFiles []string
	Imports        []string
	ImportMap      map[string]string
	Export         string
	Error          *struct{ Err string }
	DepsErrors     []*struct{ Err string }
}

// goList runs `go list -json` in dir with the workspace off, as CI builds,
// so that each module resolves against its own go.mod.
func goList(t *testing.T, dir string, args ...string) []listedPackage {
	t.Helper()
	command := exec.Command("go", append([]string{"list", "-e"}, args...)...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list %v in %s: %v\n%s", args, dir, err, stderr.String())
	}
	var packages []listedPackage
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var listed listedPackage
		if err := decoder.Decode(&listed); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		packages = append(packages, listed)
	}
	return packages
}

// repositoryModules is the root module and every module under extensions/,
// which is the set CI vets and tests.
func repositoryModules(t *testing.T, root string) []string {
	t.Helper()
	modules := []string{root}
	err := filepath.WalkDir(filepath.Join(root, "extensions"),
		func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() && (entry.Name() == "testdata" ||
				strings.HasPrefix(entry.Name(), ".")) {
				return filepath.SkipDir
			}
			if !entry.IsDir() && entry.Name() == "go.mod" {
				modules = append(modules, filepath.Dir(path))
			}
			return nil
		})
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return modules
}

type scannedPackage struct {
	path  string
	files []*ast.File
	info  *types.Info
	pkg   *types.Package
}

// universe is one module's packages, type checked together so that an
// object means the same thing in every package that refers to it.
type universe struct {
	fset     *token.FileSet
	checked  map[string]*types.Package
	exports  map[string]string
	gc       types.Importer
	packages []*scannedPackage
	// excluded are the non-test files of analysed packages that this
	// build's constraints leave out, and so are not type checked.
	excluded []string
}

func newUniverse() *universe {
	u := &universe{
		fset: token.NewFileSet(), checked: map[string]*types.Package{},
		exports: map[string]string{},
	}
	u.gc = importer.ForCompiler(u.fset, "gc", func(path string) (io.ReadCloser, error) {
		file, ok := u.exports[path]
		if !ok || file == "" {
			return nil, fmt.Errorf("no export data for %q", path)
		}
		return os.Open(file)
	})
	return u
}

func (u *universe) importerFor(importMap map[string]string) types.Importer {
	return importerFunc(func(path string) (*types.Package, error) {
		if mapped, ok := importMap[path]; ok {
			path = mapped
		}
		if pkg, ok := u.checked[path]; ok {
			return pkg, nil
		}
		return u.gc.Import(path)
	})
}

type importerFunc func(string) (*types.Package, error)

func (f importerFunc) Import(path string) (*types.Package, error) { return f(path) }

// check type checks one package from the named files.
func (u *universe) check(
	t *testing.T, path string, filenames []string, importMap map[string]string,
) *scannedPackage {
	t.Helper()
	files := make([]*ast.File, 0, len(filenames))
	for _, filename := range filenames {
		file, err := parser.ParseFile(u.fset, filename, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	info := &types.Info{
		Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{},
		Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{},
		Implicits: map[ast.Node]types.Object{},
	}
	var typeErrors []string
	config := types.Config{
		Importer: u.importerFor(importMap),
		Sizes:    types.SizesFor("gc", runtime.GOARCH),
		Error:    func(err error) { typeErrors = append(typeErrors, err.Error()) },
	}
	pkg, _ := config.Check(path, u.fset, files, info)
	if len(typeErrors) != 0 {
		t.Fatalf("type check %s: %s", path, strings.Join(typeErrors, "\n"))
	}
	u.checked[path] = pkg
	scanned := &scannedPackage{path: path, files: files, info: info, pkg: pkg}
	u.packages = append(u.packages, scanned)
	return scanned
}

// moduleListing is every package the module at dir builds, with its
// dependencies.
type moduleListing struct {
	root, dir string
	listed    []listedPackage
	byPath    map[string]listedPackage
}

func listModule(t *testing.T, root, dir string) *moduleListing {
	t.Helper()
	m := &moduleListing{root: root, dir: dir, byPath: map[string]listedPackage{}}
	m.listed = goList(t, dir, "-deps",
		"-json=ImportPath,Dir,GoFiles,CgoFiles,IgnoredGoFiles,Imports,ImportMap,Error,DepsErrors",
		"./...")
	for _, pkg := range m.listed {
		m.byPath[pkg.ImportPath] = pkg
	}
	return m
}

// inRepository reports whether path is a package of this repository.
func (m *moduleListing) inRepository(path string) bool {
	pkg, ok := m.byPath[path]
	return ok && (pkg.Dir == m.root ||
		strings.HasPrefix(pkg.Dir, m.root+string(filepath.Separator)))
}

// load type checks from source every repository package that is one of
// seeds or depends on one. That set is closed under importers, so no package
// read from export data depends on one checked from source, and every
// object has one identity in the universe. Everything else is read from
// export data.
func (m *moduleListing) load(t *testing.T, seeds map[string]bool) *universe {
	t.Helper()
	// reaches is computed over the parsed imports, which go list reports
	// even for a package whose dependencies failed to load.
	memo := map[string]bool{}
	var reaches func(path string, visiting map[string]bool) bool
	reaches = func(path string, visiting map[string]bool) bool {
		if seeds[path] {
			return true
		}
		if value, ok := memo[path]; ok {
			return value
		}
		if visiting[path] {
			return false
		}
		visiting[path] = true
		pkg := m.byPath[path]
		result := false
		for _, imported := range pkg.Imports {
			if mapped, ok := pkg.ImportMap[imported]; ok {
				imported = mapped
			}
			if reaches(imported, visiting) {
				result = true
				break
			}
		}
		memo[path] = result
		return result
	}
	var analysed []listedPackage
	for _, pkg := range m.listed {
		if !m.inRepository(pkg.ImportPath) || !reaches(pkg.ImportPath, map[string]bool{}) {
			continue
		}
		if pkg.Error != nil || len(pkg.DepsErrors) != 0 {
			t.Fatalf("package %s carries guard kinds but does not load: %+v %+v",
				pkg.ImportPath, pkg.Error, pkg.DepsErrors)
		}
		if len(pkg.CgoFiles) != 0 {
			t.Fatalf("package %s carries guard kinds and uses cgo, which this "+
				"analysis does not type check", pkg.ImportPath)
		}
		analysed = append(analysed, pkg)
	}
	u := newUniverse()
	if len(analysed) == 0 {
		return u
	}
	paths := make([]string, 0, len(analysed))
	for _, pkg := range analysed {
		paths = append(paths, pkg.ImportPath)
		for _, ignored := range pkg.IgnoredGoFiles {
			if !strings.HasSuffix(ignored, "_test.go") {
				u.excluded = append(u.excluded, filepath.Join(pkg.Dir, ignored))
			}
		}
	}
	for _, pkg := range goList(t, m.dir, append([]string{"-export", "-deps",
		"-json=ImportPath,Export"}, paths...)...) {
		u.exports[pkg.ImportPath] = pkg.Export
	}
	// go list -deps lists every package after its dependencies.
	for _, pkg := range analysed {
		filenames := make([]string, 0, len(pkg.GoFiles))
		for _, name := range pkg.GoFiles {
			filenames = append(filenames, filepath.Join(pkg.Dir, name))
		}
		u.check(t, pkg.ImportPath, filenames, pkg.ImportMap)
	}
	return u
}

// scanModule analyses one module. It starts from the packages that depend on
// the guard. A kind can also pass through a package that does not, such as
// a request type's field that a guard caller copies into Entity.Kind. Each
// such package found is added, with every package that depends on it, and
// the analysis runs again until no new package turns up. The result is the
// last run's universe and scan, or a nil universe if the module does not
// reach the guard.
func scanModule(t *testing.T, m *moduleListing) (*universe, *kindScan) {
	t.Helper()
	seeds := map[string]bool{guardImportPath: true}
	for {
		u := m.load(t, seeds)
		if u.guardPackage() == nil {
			return nil, nil
		}
		scan := scanKinds(t, m.root, u, append([]*scannedPackage(nil), u.packages...))
		added := false
		for _, path := range sortedKeys(scan.foreign) {
			switch {
			case seeds[path]:
				t.Fatalf("package %s was type checked from source and is still foreign", path)
			case m.inRepository(path):
				seeds[path], added = true, true
			default:
				scan.problems = append(scan.problems, fmt.Sprintf(
					"a kind passes through %s, outside this repository, "+
						"where its writes cannot be followed", path))
			}
		}
		if !added {
			sort.Strings(scan.problems)
			return u, scan
		}
	}
}

// guardPackage is the universe's source-checked guard package.
func (u *universe) guardPackage() *scannedPackage {
	for _, pkg := range u.packages {
		if pkg.path == guardImportPath {
			return pkg
		}
	}
	return nil
}

type kindScan struct {
	root     string
	fset     *token.FileSet
	packages []*scannedPackage

	found    map[byte]map[string]bool
	problems []string
	// foreign are the packages, read from export data, that a kind was
	// found to pass through. scanModule type checks them from source and
	// runs again.
	foreign map[string]bool
	local   map[*types.Package]bool
	// exempted are the decodedKinds and decodedEntities entries used.
	exempted map[string]bool

	seenVars    map[*types.Var]bool
	seenResults map[resultKey]bool
	queue       []func()

	// Indexes over every analysed file.
	decls      map[*types.Func]*ast.FuncDecl
	declPkg    map[*types.Func]*scannedPackage
	params     map[*types.Var]paramSite
	literals   []located[*ast.CompositeLit]
	assigns    []located[*ast.AssignStmt]
	specs      []located[*ast.ValueSpec]
	incdecs    []located[*ast.IncDecStmt]
	addresses  []located[*ast.UnaryExpr]
	ranges     []located[*ast.RangeStmt]
	calls      []located[*ast.CallExpr]
	funcUses   map[*types.Func][]located[*ast.Ident]
	callFuns   map[*ast.Ident]bool
	allowEmpty map[*types.Var]bool
}

type resultKey struct {
	fn    *types.Func
	index int
}

type paramSite struct {
	fn    *types.Func // nil for a closure parameter
	index int
	recv  bool
}

type located[T ast.Node] struct {
	pkg  *scannedPackage
	node T
	fn   string // enclosing top-level function name, if any
}

func newKindScan(root string, fset *token.FileSet, packages []*scannedPackage) *kindScan {
	s := &kindScan{
		root: root, fset: fset, packages: packages,
		found:   map[byte]map[string]bool{},
		foreign: map[string]bool{}, local: map[*types.Package]bool{},
		exempted: map[string]bool{},
		seenVars: map[*types.Var]bool{}, seenResults: map[resultKey]bool{},
		decls: map[*types.Func]*ast.FuncDecl{}, declPkg: map[*types.Func]*scannedPackage{},
		params: map[*types.Var]paramSite{}, funcUses: map[*types.Func][]located[*ast.Ident]{},
		callFuns: map[*ast.Ident]bool{}, allowEmpty: map[*types.Var]bool{},
	}
	for _, pkg := range packages {
		s.local[pkg.pkg] = true
		for _, file := range pkg.files {
			s.index(pkg, file)
		}
	}
	return s
}

func (s *kindScan) index(pkg *scannedPackage, file *ast.File) {
	for _, decl := range file.Decls {
		name := ""
		if fd, ok := decl.(*ast.FuncDecl); ok {
			name = fd.Name.Name
			if fn, ok := pkg.info.Defs[fd.Name].(*types.Func); ok {
				s.decls[fn] = fd
				s.declPkg[fn] = pkg
				sig := fn.Type().(*types.Signature)
				if sig.Recv() != nil {
					s.params[sig.Recv()] = paramSite{fn: fn, recv: true}
				}
				for i := 0; i < sig.Params().Len(); i++ {
					s.params[sig.Params().At(i)] = paramSite{fn: fn, index: i}
				}
				for i := 0; i < sig.Results().Len(); i++ {
					s.allowEmpty[sig.Results().At(i)] = true
				}
			}
		}
		ast.Inspect(decl, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.FuncLit:
				if sig, ok := pkg.info.Types[n].Type.(*types.Signature); ok {
					for i := 0; i < sig.Params().Len(); i++ {
						s.params[sig.Params().At(i)] = paramSite{index: i}
					}
					for i := 0; i < sig.Results().Len(); i++ {
						s.allowEmpty[sig.Results().At(i)] = true
					}
				}
			case *ast.CompositeLit:
				s.literals = append(s.literals, located[*ast.CompositeLit]{pkg, n, name})
			case *ast.AssignStmt:
				s.assigns = append(s.assigns, located[*ast.AssignStmt]{pkg, n, name})
			case *ast.ValueSpec:
				s.specs = append(s.specs, located[*ast.ValueSpec]{pkg, n, name})
			case *ast.IncDecStmt:
				s.incdecs = append(s.incdecs, located[*ast.IncDecStmt]{pkg, n, name})
			case *ast.UnaryExpr:
				if n.Op == token.AND {
					s.addresses = append(s.addresses, located[*ast.UnaryExpr]{pkg, n, name})
				}
			case *ast.RangeStmt:
				s.ranges = append(s.ranges, located[*ast.RangeStmt]{pkg, n, name})
			case *ast.CallExpr:
				s.calls = append(s.calls, located[*ast.CallExpr]{pkg, n, name})
				if id := calleeIdent(n.Fun); id != nil {
					s.callFuns[id] = true
				}
			case *ast.Ident:
				if fn, ok := pkg.info.Uses[n].(*types.Func); ok {
					fn = fn.Origin()
					s.funcUses[fn] = append(s.funcUses[fn], located[*ast.Ident]{pkg, n, name})
				}
			}
			return true
		})
	}
}

// calleeIdent is the identifier naming a call's function: f, pkg.f, x.m or
// T.m, through any parentheses or instantiation.
func calleeIdent(fun ast.Expr) *ast.Ident {
	switch f := ast.Unparen(fun).(type) {
	case *ast.Ident:
		return f
	case *ast.SelectorExpr:
		return f.Sel
	case *ast.IndexExpr:
		return calleeIdent(f.X)
	case *ast.IndexListExpr:
		return calleeIdent(f.X)
	}
	return nil
}

func (s *kindScan) relative(pos token.Pos) string {
	filename := s.fset.Position(pos).Filename
	if relative, err := filepath.Rel(s.root, filename); err == nil {
		return filepath.ToSlash(relative)
	}
	return filepath.ToSlash(filename)
}

func (s *kindScan) problem(node ast.Node, format string, args ...any) {
	position := s.fset.Position(node.Pos())
	if relative, err := filepath.Rel(s.root, position.Filename); err == nil {
		position.Filename = filepath.ToSlash(relative)
	}
	s.problems = append(s.problems, fmt.Sprintf("%s: %s", position, fmt.Sprintf(format, args...)))
}

// run finds every kind that reaches field.
func (s *kindScan) run(field *types.Var) {
	s.enqueueVar(field)
	for len(s.queue) != 0 {
		next := s.queue[0]
		s.queue = s.queue[1:]
		next()
	}
	sort.Strings(s.problems)
}

// noteForeign records that a kind passes through pkg, if this scan did not
// type check it from source.
func (s *kindScan) noteForeign(pkg *types.Package) {
	if pkg != nil && !s.local[pkg] {
		s.foreign[pkg.Path()] = true
	}
}

func (s *kindScan) enqueueVar(v *types.Var) {
	v = v.Origin()
	s.noteForeign(v.Pkg())
	if s.seenVars[v] {
		return
	}
	s.seenVars[v] = true
	s.queue = append(s.queue, func() { s.writesTo(v) })
}

func (s *kindScan) enqueueResult(fn *types.Func, index int) {
	key := resultKey{fn.Origin(), index}
	if s.seenResults[key] {
		return
	}
	s.seenResults[key] = true
	s.queue = append(s.queue, func() { s.returnsOf(key.fn, index) })
}

// names reports whether expr denotes v: an identifier for a variable, or a
// field selection for a field.
func names(info *types.Info, expr ast.Expr, v *types.Var) bool {
	switch e := ast.Unparen(expr).(type) {
	case *ast.Ident:
		obj := info.Defs[e]
		if obj == nil {
			obj = info.Uses[e]
		}
		if variable, ok := obj.(*types.Var); ok {
			return variable.Origin() == v
		}
	case *ast.SelectorExpr:
		if selection, ok := info.Selections[e]; ok && selection.Kind() == types.FieldVal {
			return selection.Obj().(*types.Var).Origin() == v
		}
		if variable, ok := info.Uses[e.Sel].(*types.Var); ok {
			return variable.Origin() == v
		}
	}
	return false
}

// writesTo resolves every value written to v.
func (s *kindScan) writesTo(v *types.Var) {
	writes := 0
	if v.IsField() {
		writes += s.literalWrites(v)
	}
	for _, a := range s.assigns {
		for i, lhs := range a.node.Lhs {
			if !names(a.pkg.info, lhs, v) {
				continue
			}
			writes++
			if s.allowed(a.pkg, a.node, a.fn) {
				continue
			}
			switch {
			case a.node.Tok != token.ASSIGN && a.node.Tok != token.DEFINE:
				s.problem(a.node, "compound assignment %s to %s", a.node.Tok, v.Name())
			case len(a.node.Lhs) == len(a.node.Rhs):
				s.resolve(a.pkg, a.node.Rhs[i])
			default:
				s.resolveTuple(a.pkg, a.node.Rhs[0], i)
			}
		}
	}
	if !v.IsField() {
		for _, spec := range s.specs {
			for i, name := range spec.node.Names {
				if spec.pkg.info.Defs[name] != v {
					continue
				}
				switch {
				case len(spec.node.Values) == 0:
					// The zero value, kind 0, which validate refuses.
				case len(spec.node.Values) == len(spec.node.Names):
					writes++
					s.resolve(spec.pkg, spec.node.Values[i])
				default:
					writes++
					s.resolveTuple(spec.pkg, spec.node.Values[0], i)
				}
			}
		}
	}
	for _, n := range s.incdecs {
		if names(n.pkg.info, n.node.X, v) {
			s.problem(n.node, "%s of %s", n.node.Tok, v.Name())
		}
	}
	for _, n := range s.addresses {
		if names(n.pkg.info, n.node.X, v) {
			s.problem(n.node, "the address of %s is taken, so its writes cannot be followed", v.Name())
		}
	}
	for _, n := range s.ranges {
		if (n.node.Key != nil && names(n.pkg.info, n.node.Key, v)) ||
			(n.node.Value != nil && names(n.pkg.info, n.node.Value, v)) {
			s.problem(n.node, "%s is a range variable", v.Name())
		}
	}
	if site, ok := s.params[v]; ok {
		switch {
		case site.recv:
			s.problem(s.decls[site.fn], "a kind is the receiver of %s", site.fn.Name())
		case site.fn == nil:
			pos := v.Pos()
			s.problems = append(s.problems, fmt.Sprintf("%s: a kind is a closure parameter (%s)",
				s.fset.Position(pos), v.Name()))
		default:
			writes += s.callArguments(site.fn, site.index)
		}
		return
	}
	if v.IsField() || s.allowEmpty[v] {
		return
	}
	if writes == 0 {
		s.problems = append(s.problems, fmt.Sprintf(
			"%s: no write to %s that this analysis can follow", s.fset.Position(v.Pos()), v.Name()))
	}
}

// allowed reports whether a non-constant write at node is one of
// decodedKinds.
func (s *kindScan) allowed(pkg *scannedPackage, node ast.Node, fn string) bool {
	key := s.relative(node.Pos()) + ":" + fn
	_, ok := decodedKinds[key]
	if ok {
		s.exempted[key] = true
	}
	return ok
}

// literalWrites resolves the value given to field in every composite literal,
// and refuses literals and conversions of a struct identical to the field's
// own but not it.
func (s *kindScan) literalWrites(field *types.Var) int {
	writes := 0
	owner := s.ownerOf(field)
	for _, lit := range s.literals {
		tv, ok := lit.pkg.info.Types[lit.node]
		if !ok {
			continue
		}
		typ := tv.Type
		if pointer, ok := typ.Underlying().(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		structure, ok := typ.Underlying().(*types.Struct)
		if !ok {
			continue
		}
		index := -1
		for i := 0; i < structure.NumFields(); i++ {
			if structure.Field(i).Origin() == field {
				index = i
			}
		}
		if index < 0 {
			if owner != nil && types.Identical(structure, owner.Underlying()) {
				s.problem(lit.node, "a literal of %s, identical to %s, can carry a kind unseen",
					typ, owner)
			}
			continue
		}
		for i, element := range lit.node.Elts {
			value := element
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				key, _ := kv.Key.(*ast.Ident)
				if key == nil || lit.pkg.info.Uses[key] == nil ||
					lit.pkg.info.Uses[key].(*types.Var).Origin() != field {
					continue
				}
				value = kv.Value
			} else if i != index {
				continue
			}
			writes++
			if s.allowed(lit.pkg, lit.node, lit.fn) {
				continue
			}
			s.resolve(lit.pkg, value)
		}
	}
	if owner != nil {
		for _, call := range s.calls {
			tv, ok := call.pkg.info.Types[call.node.Fun]
			if !ok || !tv.IsType() || len(call.node.Args) != 1 {
				continue
			}
			target := tv.Type
			if !types.Identical(target.Underlying(), owner.Underlying()) {
				continue
			}
			from := call.pkg.info.Types[call.node.Args[0]].Type
			if from != nil && !types.Identical(from, target) {
				s.problem(call.node, "a conversion from %s to %s can carry a kind unseen",
					from, target)
			}
		}
	}
	return writes
}

// ownerOf is the named struct type that declares field, if this analysis
// type checked it.
func (s *kindScan) ownerOf(field *types.Var) types.Type {
	for _, pkg := range s.packages {
		if pkg.pkg != field.Pkg() {
			continue
		}
		scope := pkg.pkg.Scope()
		for _, name := range scope.Names() {
			named, ok := scope.Lookup(name).Type().(*types.Named)
			if !ok {
				continue
			}
			if structure, ok := named.Underlying().(*types.Struct); ok {
				for i := 0; i < structure.NumFields(); i++ {
					if structure.Field(i) == field {
						return named
					}
				}
			}
		}
	}
	return nil
}

// callArguments resolves the argument at index of every call to fn,
// including dynamic calls through an interface fn's receiver satisfies.
// Any use of fn other than calling it lets the kind escape.
func (s *kindScan) callArguments(fn *types.Func, index int) int {
	writes := 0
	sig := fn.Type().(*types.Signature)
	for _, use := range s.funcUses[fn] {
		if !s.callFuns[use.node] {
			s.problem(use.node, "%s, which takes a kind, is used as a value", fn.Name())
		}
	}
	for _, call := range s.calls {
		callee, shift := s.callee(call.pkg.info, call.node)
		if callee == nil {
			continue
		}
		if callee != fn && !s.dispatchesTo(callee, fn) {
			continue
		}
		writes++
		position := index + shift
		switch {
		case sig.Variadic() && index == sig.Params().Len()-1:
			if call.node.Ellipsis.IsValid() {
				s.problem(call.node, "a kind is passed to %s as a spread slice", fn.Name())
				continue
			}
			for _, argument := range call.node.Args[min(position, len(call.node.Args)):] {
				s.resolve(call.pkg, argument)
			}
		case len(call.node.Args) == 1 && sig.Params().Len()+shift > 1:
			s.resolveTuple(call.pkg, call.node.Args[0], position)
		case position < len(call.node.Args):
			s.resolve(call.pkg, call.node.Args[position])
		default:
			s.problem(call.node, "a call to %s that this analysis cannot match to its parameters",
				fn.Name())
		}
	}
	return writes
}

// callee is the function a call statically names, and how many arguments
// precede its parameters (one for a method expression's receiver). An
// interface method is returned as itself.
func (s *kindScan) callee(info *types.Info, call *ast.CallExpr) (*types.Func, int) {
	fun := ast.Unparen(call.Fun)
	switch f := fun.(type) {
	case *ast.IndexExpr:
		fun = f.X
	case *ast.IndexListExpr:
		fun = f.X
	}
	switch f := ast.Unparen(fun).(type) {
	case *ast.Ident:
		if fn, ok := info.Uses[f].(*types.Func); ok {
			return fn.Origin(), 0
		}
	case *ast.SelectorExpr:
		if selection, ok := info.Selections[f]; ok {
			fn, ok := selection.Obj().(*types.Func)
			if !ok {
				return nil, 0
			}
			if selection.Kind() == types.MethodExpr {
				return fn.Origin(), 1
			}
			return fn.Origin(), 0
		}
		if fn, ok := info.Uses[f.Sel].(*types.Func); ok {
			return fn.Origin(), 0
		}
	}
	return nil, 0
}

// dispatchesTo reports whether a call of the interface method callee can
// reach the concrete method fn.
func (s *kindScan) dispatchesTo(callee, fn *types.Func) bool {
	calleeSig := callee.Type().(*types.Signature)
	fnSig := fn.Type().(*types.Signature)
	if calleeSig.Recv() == nil || fnSig.Recv() == nil || callee.Name() != fn.Name() {
		return false
	}
	iface, ok := calleeSig.Recv().Type().Underlying().(*types.Interface)
	if !ok {
		return false
	}
	receiver := fnSig.Recv().Type()
	if pointer, ok := receiver.(*types.Pointer); ok {
		receiver = pointer.Elem()
	}
	return types.Implements(receiver, iface) ||
		types.Implements(types.NewPointer(receiver), iface)
}

// returnsOf resolves result index of fn at every return in its body.
func (s *kindScan) returnsOf(fn *types.Func, index int) {
	decl, pkg := s.decls[fn], s.declPkg[fn]
	if decl == nil || decl.Body == nil {
		s.problems = append(s.problems, fmt.Sprintf(
			"%s: a kind is the result of %s, whose body this analysis cannot read",
			s.fset.Position(fn.Pos()), fn.FullName()))
		return
	}
	sig := fn.Type().(*types.Signature)
	result := sig.Results().At(index)
	if result.Name() != "" && result.Name() != "_" {
		s.enqueueVar(result)
	}
	ast.Inspect(decl.Body, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			switch {
			case len(n.Results) == 0:
				// A bare return of the named result, followed above.
			case len(n.Results) == sig.Results().Len():
				s.resolve(pkg, n.Results[index])
			default:
				s.resolveTuple(pkg, n.Results[0], index)
			}
		}
		return true
	})
}

// resolveTuple resolves element index of a multi-valued expression.
func (s *kindScan) resolveTuple(pkg *scannedPackage, expr ast.Expr, index int) {
	if call, ok := ast.Unparen(expr).(*ast.CallExpr); ok {
		if fn, shift := s.callee(pkg.info, call); fn != nil && shift == 0 {
			if s.decls[fn] != nil {
				s.enqueueResult(fn, index)
				return
			}
			if fn.Pkg() != nil && !s.local[fn.Pkg()] {
				s.noteForeign(fn.Pkg())
				return
			}
		}
	}
	s.problem(expr, "a kind is element %d of a multi-valued expression this analysis cannot follow", index)
}

// resolve follows one expression that reaches a kind.
func (s *kindScan) resolve(pkg *scannedPackage, expr ast.Expr) {
	expr = ast.Unparen(expr)
	if tv, ok := pkg.info.Types[expr]; ok && tv.Value != nil {
		value, exact := constant.Int64Val(constant.ToInt(tv.Value))
		if !exact || value < 0 || value > 0xff {
			s.problem(expr, "a constant kind %s that is not a byte", tv.Value)
			return
		}
		file := s.relative(expr.Pos())
		if s.found[byte(value)] == nil {
			s.found[byte(value)] = map[string]bool{}
		}
		s.found[byte(value)][file] = true
		return
	}
	switch e := expr.(type) {
	case *ast.Ident:
		if v, ok := pkg.info.Uses[e].(*types.Var); ok {
			s.enqueueVar(v)
			return
		}
	case *ast.SelectorExpr:
		if selection, ok := pkg.info.Selections[e]; ok {
			if selection.Kind() == types.FieldVal {
				s.enqueueVar(selection.Obj().(*types.Var))
				return
			}
			break
		}
		if v, ok := pkg.info.Uses[e.Sel].(*types.Var); ok {
			s.enqueueVar(v)
			return
		}
	case *ast.CallExpr:
		if tv := pkg.info.Types[e.Fun]; tv.IsType() && len(e.Args) == 1 {
			s.resolve(pkg, e.Args[0])
			return
		}
		if fn, shift := s.callee(pkg.info, e); fn != nil && shift == 0 {
			if s.decls[fn] != nil {
				s.enqueueResult(fn, 0)
				return
			}
			if fn.Pkg() != nil && !s.local[fn.Pkg()] {
				s.noteForeign(fn.Pkg())
				return
			}
		}
		s.problem(e, "a kind is the result of a call this analysis cannot follow")
		return
	}
	s.problem(expr, "a kind is a non-constant %T this analysis cannot follow", expr)
}

// scanKinds is the complete analysis over a set of type-checked packages,
// one of which must be the guard.
func scanKinds(t *testing.T, root string, u *universe, packages []*scannedPackage) *kindScan {
	t.Helper()
	guardPkg := u.guardPackage()
	if guardPkg == nil {
		t.Fatal("the guard package was not type checked")
	}
	entity, ok := guardPkg.pkg.Scope().Lookup("Entity").(*types.TypeName)
	if !ok {
		t.Fatal("guard.Entity is gone")
	}
	structure := entity.Type().Underlying().(*types.Struct)
	var kind *types.Var
	for i := 0; i < structure.NumFields(); i++ {
		if structure.Field(i).Name() == "Kind" {
			kind = structure.Field(i)
		}
	}
	if kind == nil {
		t.Fatal("guard.Entity.Kind is gone")
	}
	scan := newKindScan(root, u.fset, packages)
	scan.run(kind)
	scan.checkExcluded(u.excluded)
	scan.checkDecoders(entity.Type())
	return scan
}

// reflectiveDecoders are the standard decoders that can set a field by
// reflection, keyed by package path, with the index of the decoded-into
// argument for each function or method.
var reflectiveDecoders = map[string]map[string]int{
	"encoding/json": {"Unmarshal": 1, "Decode": 0},
	"encoding/gob":  {"Decode": 0},
	"encoding/xml":  {"Unmarshal": 1, "Decode": 0, "DecodeElement": 0},
	"encoding/asn1": {"Unmarshal": 1, "UnmarshalWithParams": 1},
}

// decodedEntities are the calls allowed to decode into a value holding a
// guard.Entity, keyed by file and enclosing function. Each reads back a
// record this process wrote, so it brings no kind of its own.
var decodedEntities = map[string]string{
	"internal/explorercoord/intents.go:decodeStoredIntent": "a stored publication intent",
}

// checkDecoders reports a reflective decode into a value that holds a
// guard.Entity. Reflection writes Kind out of the analysis's sight, from
// whatever the input says.
func (s *kindScan) checkDecoders(entity types.Type) {
	for _, call := range s.calls {
		fn, _ := s.callee(call.pkg.info, call.node)
		if fn == nil || fn.Pkg() == nil {
			continue
		}
		index, ok := reflectiveDecoders[fn.Pkg().Path()][fn.Name()]
		if !ok || index >= len(call.node.Args) {
			continue
		}
		target := call.pkg.info.Types[call.node.Args[index]].Type
		if target == nil || !holds(target, entity, map[types.Type]bool{}) {
			continue
		}
		key := s.relative(call.node.Pos()) + ":" + call.fn
		if _, ok := decodedEntities[key]; ok {
			s.exempted[key] = true
			continue
		}
		s.problem(call.node, "%s.%s decodes into %s, which holds a guard.Entity, "+
			"so its kind comes from the input", fn.Pkg().Name(), fn.Name(), target)
	}
}

// holds reports whether a value of typ can contain a value of entity.
func holds(typ, entity types.Type, seen map[types.Type]bool) bool {
	if types.Identical(typ, entity) {
		return true
	}
	if seen[typ] {
		return false
	}
	seen[typ] = true
	switch t := typ.Underlying().(type) {
	case *types.Pointer:
		return holds(t.Elem(), entity, seen)
	case *types.Slice:
		return holds(t.Elem(), entity, seen)
	case *types.Array:
		return holds(t.Elem(), entity, seen)
	case *types.Map:
		return holds(t.Key(), entity, seen) || holds(t.Elem(), entity, seen)
	case *types.Struct:
		for i := 0; i < t.NumFields(); i++ {
			if holds(t.Field(i).Type(), entity, seen) {
				return true
			}
		}
	case *types.Interface:
		// any can hold anything; a decode into one yields maps and slices,
		// never a guard.Entity.
	}
	return false
}

// checkExcluded reports a file this build leaves out that could write a kind
// the analysis did not see. Such a file cannot be type checked here, so it
// is held to a conservative syntactic rule. It must not import the guard,
// and it must not name anything the analysis followed: the Entity type, a
// field, variable or parameter a kind reaches, or a function whose
// parameters or results carry one. Names are compared without packages, so
// the rule can refuse a harmless file, never pass a harmful one.
func (s *kindScan) checkExcluded(files []string) {
	followed := map[string]bool{"Entity": true}
	for v := range s.seenVars {
		followed[v.Name()] = true
		if site, ok := s.params[v]; ok && site.fn != nil {
			followed[site.fn.Name()] = true
		}
	}
	for key := range s.seenResults {
		followed[key.fn.Name()] = true
	}
	for _, filename := range files {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
		if err != nil {
			s.problems = append(s.problems, fmt.Sprintf("%s: %v", filename, err))
			continue
		}
		relative := filename
		if r, err := filepath.Rel(s.root, filename); err == nil {
			relative = filepath.ToSlash(r)
		}
		for _, spec := range file.Imports {
			if strings.Trim(spec.Path.Value, "`\"") == guardImportPath {
				s.problems = append(s.problems, fmt.Sprintf(
					"%s: excluded from this build and imports the guard, so its kinds go unseen",
					relative))
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && followed[id.Name] && id.Name != "_" {
				s.problems = append(s.problems, fmt.Sprintf(
					"%s: excluded from this build and names %s, which carries a kind, "+
						"so what it writes goes unseen", relative, id.Name))
				return false
			}
			return true
		})
	}
}
