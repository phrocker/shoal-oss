// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

// Package importboundary checks the core/extension import boundary described
// in docs/gateways.md ("Core and extensions") over source files, without the
// go command, so it holds with GOWORK=off and for modules CI does not build.
//
//   - Rule A: no package of the root module imports extensions/.
//   - Rule B: every extension module (any go.mod under extensions/, at any
//     depth) imports from this repository only the public allowlist: pkg/sdk,
//     pkg/collector, pkg/collector/api, pkg/decision/api and pkg/shoal. Its
//     module path must be the repository module plus its directory, its only
//     permitted replace is the repository onto this tree, and no Go file
//     under extensions/ may sit outside an extension module. Standard library
//     and third-party imports are the extension's own business.
//   - Rule C: the in-repository import closure of the allowlist contains no
//     internal/ package (and no extension), so the allowlist cannot leak
//     internals transitively.
//
// Symlinks in the root module or under extensions/ are violations under A
// and B respectively, and an extension go.mod this package cannot parse
// exactly is a B violation (see walkGo and parseGoMod).
package importboundary

import (
	"bufio"
	"bytes"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"slices"
	"strconv"
	"strings"
)

// Module is the root module path.
const Module = "github.com/phrocker/shoal-oss"

// Allowlist is every repository package an extension may import.
var Allowlist = []string{
	Module + "/pkg/sdk",
	Module + "/pkg/collector",
	Module + "/pkg/collector/api",
	Module + "/pkg/decision/api",
	Module + "/pkg/shoal",
}

// Violation is one forbidden import.
type Violation struct {
	Rule   string
	File   string
	Import string
}

func (v Violation) String() string {
	return fmt.Sprintf("rule %s: %s imports %s", v.Rule, v.File, v.Import)
}

func within(p, prefix string) bool { return p == prefix || strings.HasPrefix(p, prefix+"/") }
func internalPath(p string) bool {
	return strings.Contains("/"+p+"/", "/internal/")
}

// goMod is the subset of a go.mod file the rules need.
type goMod struct {
	module   string
	replaces [][2]string // old path (version stripped), new path
}

// goModVerbs are the directives the go command accepts. Anything else fails
// closed rather than being skipped.
var goModVerbs = []string{"module", "go", "toolchain", "godebug", "require", "replace", "exclude", "retract", "tool", "ignore"}

// goModFields splits one line, separating parentheses glued to tokens
// ("replace(") and unquoting quoted tokens. A quote that does not wrap a
// whole token is malformed.
func goModFields(line string) ([]string, error) {
	line = strings.ReplaceAll(strings.ReplaceAll(line, "(", " ( "), ")", " ) ")
	fields := strings.Fields(line)
	for i, f := range fields {
		if !strings.ContainsAny(f, "\"`") {
			continue
		}
		u, err := strconv.Unquote(f)
		if err != nil {
			return nil, fmt.Errorf("malformed quoted token %q", f)
		}
		fields[i] = u
	}
	return fields, nil
}

// parseGoMod reads module and replace directives, including blocks. It
// rejects unknown directives, nested or unbalanced blocks, and parentheses
// anywhere but a block opener or closer, so an unusual spelling cannot hide a
// replace from the rules.
func parseGoMod(fsys fs.FS, name string) (goMod, error) {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return goMod{}, err
	}
	var out goMod
	block := ""
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for n := 1; scanner.Scan(); n++ {
		line := scanner.Text()
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		fields, err := goModFields(line)
		if err != nil {
			return goMod{}, fmt.Errorf("%s:%d: %w", name, n, err)
		}
		if len(fields) == 0 {
			continue
		}
		bad := func(what string) (goMod, error) {
			return goMod{}, fmt.Errorf("%s:%d: %s", name, n, what)
		}
		switch {
		case block != "" && len(fields) == 1 && fields[0] == ")":
			block = ""
			continue
		case block == "" && len(fields) == 2 && fields[1] == "(":
			if !slices.Contains(goModVerbs, fields[0]) {
				return bad("unknown directive " + fields[0])
			}
			block = fields[0]
			continue
		case block != "":
			fields = append([]string{block}, fields...)
		}
		if slices.Contains(fields, "(") || slices.Contains(fields, ")") {
			return bad("unexpected parenthesis")
		}
		switch fields[0] {
		case "module":
			if len(fields) != 2 || out.module != "" {
				return bad("malformed module directive")
			}
			out.module = fields[1]
		case "replace":
			arrow := slices.Index(fields, "=>")
			// replace old [version] => new [version]
			if arrow < 2 || arrow > 3 || len(fields)-arrow-1 < 1 || len(fields)-arrow-1 > 2 {
				return bad("malformed replace directive")
			}
			out.replaces = append(out.replaces, [2]string{fields[1], fields[arrow+1]})
		default:
			if !slices.Contains(goModVerbs, fields[0]) {
				return bad("unknown directive " + fields[0])
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return goMod{}, err
	}
	if block != "" {
		return goMod{}, fmt.Errorf("%s: unterminated %s block", name, block)
	}
	if out.module == "" {
		return goMod{}, fmt.Errorf("%s: no module directive", name)
	}
	return out, nil
}

func modulePath(fsys fs.FS, name string) (string, error) {
	m, err := parseGoMod(fsys, name)
	return m.module, err
}

func imports(fsys fs.FS, name string) ([]string, error) {
	src, err := fs.ReadFile(fsys, name)
	if err != nil {
		return nil, err
	}
	file, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, spec := range file.Imports {
		p, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// walkGo visits every .go file under root and reports every symlink.
//
// It descends into every directory, including testdata, vendor and
// directories beginning with "_" or ".": go build ignores those for ./...
// patterns but still compiles them when a package imports them by explicit
// path. The only directories skipped are nested modules (a directory with its
// own go.mod, which is a different module) and those skip names. The
// checker's own fixtures need no special case: each fixture tree under
// internal/importboundary/testdata is a nested module.
//
// fs.WalkDir does not follow symlinks, but the go command and go.work do, so
// a symlink could graft code into a module unseen. link receives every
// symlink; Check reports each as a violation.
func walkGo(fsys fs.FS, root string, skip func(dir string) bool, visit func(file string, imports []string) error, link func(name string)) error {
	return fs.WalkDir(fsys, root, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			if link != nil {
				link(name)
			}
			return nil
		}
		if d.IsDir() {
			if name != root && skip != nil && skip(name) {
				return fs.SkipDir
			}
			if name != root {
				if info, err := fs.Lstat(fsys, path.Join(name, "go.mod")); err == nil && info.Mode().IsRegular() {
					return fs.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") {
			return nil
		}
		list, err := imports(fsys, name)
		if err != nil {
			return err
		}
		return visit(name, list)
	})
}

// Extensions returns every go.mod under extensions/, at any depth.
func Extensions(fsys fs.FS) ([]string, error) {
	var mods []string
	if _, err := fs.Stat(fsys, "extensions"); err != nil {
		return nil, nil
	}
	err := fs.WalkDir(fsys, "extensions", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() && path.Base(name) == "go.mod" {
			mods = append(mods, name)
		}
		return nil
	})
	return mods, err
}

// Check applies all three rules to the repository rooted at fsys.
func Check(fsys fs.FS) ([]Violation, error) {
	root, err := modulePath(fsys, "go.mod")
	if err != nil {
		return nil, err
	}
	if root != Module {
		return nil, fmt.Errorf("root module is %q, want %q", root, Module)
	}
	var out []Violation
	extensions := Module + "/extensions"

	// Rule A covers every package of the root module, not just pkg/,
	// internal/ and cmd/. Nested modules (wal-quorum-sidecar, extensions) are
	// skipped by walkGo; extensions/ is checked under rule B.
	// The repository's .git directory is never part of a module: Go rejects
	// import path elements that begin with a dot.
	if err := walkGo(fsys, ".", func(dir string) bool { return dir == "extensions" || dir == ".git" }, func(file string, list []string) error {
		for _, p := range list {
			if within(p, extensions) {
				out = append(out, Violation{"A", file, p})
			}
		}
		return nil
	}, func(name string) { out = append(out, Violation{"A", name, "(symlink)"}) }); err != nil {
		return nil, err
	}

	mods, err := Extensions(fsys)
	if err != nil {
		return nil, err
	}
	moduleDirs := map[string]bool{}
	for _, mod := range mods {
		moduleDirs[path.Dir(mod)] = true
	}
	// Go files under extensions/ outside every extension module would belong
	// to the root module and escape rule B.
	if _, statErr := fs.Stat(fsys, "extensions"); statErr == nil {
		if err := walkGo(fsys, "extensions", func(dir string) bool { return moduleDirs[dir] }, func(file string, _ []string) error {
			out = append(out, Violation{"B", file, "(file outside an extension module)"})
			return nil
		}, func(name string) { out = append(out, Violation{"B", name, "(symlink)"}) }); err != nil {
			return nil, err
		}
	}
	for _, mod := range mods {
		dir := path.Dir(mod)
		parsed, err := parseGoMod(fsys, mod)
		if err != nil {
			// Fail closed: a go.mod this checker cannot read exactly is a
			// violation, not a skipped module.
			out = append(out, Violation{"B", mod, "(malformed go.mod: " + err.Error() + ")"})
			continue
		}
		// The module path must match the directory, or a module could name
		// itself into the repository (for example .../internal) and exempt
		// its own imports. Go's internal rule does not protect this tree.
		if want := Module + "/" + dir; parsed.module != want {
			out = append(out, Violation{"B", mod, "module " + parsed.module + " (want " + want + ")"})
			continue
		}
		// Only the repository itself may be replaced, and only by this tree.
		for _, r := range parsed.replaces {
			target := r[1]
			relative := strings.HasPrefix(target, "./") || strings.HasPrefix(target, "../")
			if r[0] != Module || !relative || path.Clean(path.Join(dir, target)) != "." {
				out = append(out, Violation{"B", mod, "replace " + r[0] + " => " + target})
			}
		}
		own := parsed.module
		if err := walkGo(fsys, dir, nil, func(file string, list []string) error {
			for _, p := range list {
				if within(p, own) || !within(p, Module) {
					continue
				}
				if !slices.Contains(Allowlist, p) {
					out = append(out, Violation{"B", file, p})
				}
			}
			return nil
		}, func(name string) { out = append(out, Violation{"B", name, "(symlink)"}) }); err != nil {
			return nil, err
		}
	}

	seen := map[string]bool{}
	queue := slices.Clone(Allowlist)
	for len(queue) > 0 {
		pkg := queue[0]
		queue = queue[1:]
		if seen[pkg] {
			continue
		}
		seen[pkg] = true
		dir := strings.TrimPrefix(strings.TrimPrefix(pkg, Module), "/")
		entries, err := fs.ReadDir(fsys, dir)
		if err != nil {
			continue // Absent packages are a build error, not a boundary one.
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			file := path.Join(dir, name)
			list, err := imports(fsys, file)
			if err != nil {
				return nil, err
			}
			for _, p := range list {
				if !within(p, Module) {
					continue
				}
				if internalPath(p) || within(p, extensions) {
					out = append(out, Violation{"C", file, p})
					continue
				}
				queue = append(queue, p)
			}
		}
	}
	return out, nil
}
