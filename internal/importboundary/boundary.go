// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

// Package importboundary checks the core/extension import boundary described
// in docs/gateways.md ("Core and extensions") over source files, without the
// go command, so it holds with GOWORK=off and for modules CI does not build.
//
//   - Rule A: nothing under pkg/, internal/ or cmd/ imports extensions/.
//   - Rule B: an extension module (extensions/*/go.mod) imports from this
//     repository only the public allowlist: pkg/sdk, pkg/collector,
//     pkg/collector/api, pkg/decision/api and pkg/shoal. Standard library and
//     third-party imports are the extension's own business.
//   - Rule C: the in-repository import closure of the allowlist contains no
//     internal/ package (and no extension), so the allowlist cannot leak
//     internals transitively.
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

// modulePath reads the module directive of a go.mod file.
func modulePath(fsys fs.FS, name string) (string, error) {
	raw, err := fs.ReadFile(fsys, name)
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "module" {
			return strings.Trim(fields[1], `"`), nil
		}
	}
	return "", fmt.Errorf("%s: no module directive", name)
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

// walkGo visits every .go file under root, skipping testdata, hidden and
// underscore directories and nested modules.
func walkGo(fsys fs.FS, root string, visit func(file string, imports []string) error) error {
	return fs.WalkDir(fsys, root, func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			base := path.Base(name)
			if name != root && (base == "testdata" || strings.HasPrefix(base, ".") || strings.HasPrefix(base, "_")) {
				return fs.SkipDir
			}
			if name != root {
				if _, err := fs.Stat(fsys, path.Join(name, "go.mod")); err == nil {
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

	for _, top := range []string{"pkg", "internal", "cmd"} {
		if _, err := fs.Stat(fsys, top); err != nil {
			continue
		}
		if err := walkGo(fsys, top, func(file string, list []string) error {
			for _, p := range list {
				if within(p, extensions) {
					out = append(out, Violation{"A", file, p})
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}

	mods, err := fs.Glob(fsys, "extensions/*/go.mod")
	if err != nil {
		return nil, err
	}
	for _, mod := range mods {
		own, err := modulePath(fsys, mod)
		if err != nil {
			return nil, err
		}
		if err := walkGo(fsys, path.Dir(mod), func(file string, list []string) error {
			for _, p := range list {
				if within(p, own) || !within(p, Module) {
					continue
				}
				if !slices.Contains(Allowlist, p) {
					out = append(out, Violation{"B", file, p})
				}
			}
			return nil
		}); err != nil {
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
