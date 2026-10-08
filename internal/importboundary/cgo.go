// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package importboundary

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// cgo, assembly, .syso and SWIG files let a package compile or link code
// that no Go import names, and the C preprocessor has too many spellings to
// police by deny-list. The rules are therefore an allowlist.
//
// Extensions (rule B) may not use cgo or SWIG at all, and may not contain any
// source the go command builds besides .go files (extensionFileAllowed).
// Allowing cgo in extensions needs its own design.
//
// Core (rule A) may use cgo only in CgoPackages. Every #cgo path in those
// packages must resolve into the package directory or CgoIncludeDirs. Every
// include in their preambles and C-family files must be a plain quoted
// literal that resolves to an existing file in those directories, or a
// <system.h> name with no path. Included repository files are scanned the
// same way, transitively. All C-family files in the allowed directories are
// scanned, so a -include flag cannot reach an unscanned file. Anything the
// simple parser cannot read is a violation. Any other core package with
// import "C" or a buildable non-Go source is a violation.
//
// Residual: flags and search paths supplied by the build environment
// (CGO_CFLAGS, CGO_LDFLAGS, pkg-config) are outside a source check.

// CgoPackages are the only core packages that may use cgo. Today that is the
// C ABI library.
var CgoPackages = []string{"cmd/shoal-capi"}

// CgoIncludeDirs are the only directories, besides a cgo package's own,
// that its #cgo paths and includes may name. These are exactly what
// cmd/shoal-capi uses: the public C headers and the C test seam.
var CgoIncludeDirs = []string{"capi/include", "capi/tests"}

// InertCSourceDirs hold C fixtures read as data by a Go test. The go command
// refuses to build C files in a package without import "C", which the
// cgo rule forbids here, so these files cannot be compiled into core.
// Assembly, .syso and SWIG files stay forbidden even here.
var InertCSourceDirs = []string{"docs/testdata/validate_sharkbite_matrix"}

// buildableExts are the non-Go files the go command compiles or links into
// a package: C, C++, Objective-C, Fortran, headers, assembly, .syso and SWIG.
var buildableExts = []string{".c", ".h", ".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx", ".m", ".mm", ".f", ".F", ".for", ".f90", ".s", ".S", ".sx", ".syso", ".swig", ".swigcxx"}

// buildWithoutCgo are buildable files the go command uses even when a
// package does not import "C".
var buildWithoutCgo = []string{".s", ".syso", ".swig", ".swigcxx"}

func hasExt(name string, exts []string) bool {
	return slices.Contains(exts, path.Ext(name))
}

// extensionFileAllowed reports whether an extension module may contain the
// file: Go sources, module files and plain documentation or data. Under a
// testdata directory any non-buildable file is data.
func extensionFileAllowed(name string) bool {
	base := path.Base(name)
	switch {
	case strings.HasSuffix(base, ".go"), base == "go.mod", base == "go.sum":
		return true
	case hasExt(name, buildableExts):
		return false
	case strings.Contains("/"+name+"/", "/testdata/"):
		return true
	}
	switch path.Ext(base) {
	case ".md", ".txt", ".json", ".yaml", ".yml", ".csv", ".golden":
		return true
	}
	return base == "LICENSE" || base == "NOTICE" || base == ".gitignore"
}

// preprocess applies C translation phases 1 to 3: it splices
// backslash-newline continuations and replaces comments with a space, so
// "#include \", "#/**/include" and the like read as the directive the
// compiler sees. ok is false for trigraphs, which can splice lines or form
// '#' when a compiler flag enables them.
func preprocess(src []byte) (string, bool) {
	s := strings.ReplaceAll(string(src), "\r\n", "\n")
	if regexp.MustCompile(`\?\?[=/'()!<>-]`).MatchString(s) {
		return "", false
	}
	s = strings.ReplaceAll(s, "\\\n", "")
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\'':
			// Copy a literal through its closing quote.
			j := i + 1
			for j < len(s) && s[j] != c && s[j] != '\n' {
				if s[j] == '\\' {
					j++
				}
				j++
			}
			if j >= len(s) {
				j = len(s) - 1
			}
			b.WriteString(s[i : j+1])
			i = j
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return "", false
			}
			// Keep newlines so line structure survives.
			b.WriteByte(' ')
			b.WriteString(strings.Repeat("\n", strings.Count(s[i+2:i+2+end], "\n")))
			i += end + 3
		case c == '/' && i+1 < len(s) && s[i+1] == '/':
			for i < len(s) && s[i] != '\n' {
				i++
			}
			if i < len(s) {
				b.WriteByte('\n')
			}
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), true
}

var directiveName = regexp.MustCompile(`^[A-Za-z_]+`)

// include is one inclusion directive: kind is "quote" or "angle".
type include struct {
	kind, path string
}

// includes returns the inclusion directives of C source. A directive the
// parser cannot read exactly yields a problem description.
func includes(src []byte) ([]include, []string) {
	text, ok := preprocess(src)
	if !ok {
		return nil, []string{"(unreadable source: trigraph or unterminated comment)"}
	}
	var out []include
	var problems []string
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(t, "%:") {
			problems = append(problems, "(digraph directive "+t+")")
			continue
		}
		if !strings.HasPrefix(t, "#") {
			continue
		}
		rest := strings.TrimSpace(t[1:])
		name := directiveName.FindString(rest)
		if name == "" {
			if rest != "" { // "#" alone is the null directive.
				problems = append(problems, "(unreadable directive "+t+")")
			}
			continue
		}
		switch name {
		case "include", "include_next", "import", "embed":
		default:
			continue
		}
		arg := strings.TrimSpace(rest[len(name):])
		switch {
		case len(arg) >= 2 && arg[0] == '"' && strings.Count(arg, `"`) == 2 && arg[len(arg)-1] == '"':
			out = append(out, include{"quote", arg[1 : len(arg)-1]})
		case len(arg) >= 2 && arg[0] == '<' && strings.Count(arg, ">") == 1 && arg[len(arg)-1] == '>':
			out = append(out, include{"angle", arg[1 : len(arg)-1]})
		default:
			problems = append(problems, "include "+arg+" (not a literal path)")
		}
	}
	return out, problems
}

// preamble returns the cgo preamble of a Go file, the comment on import "C".
func preamble(name string, src []byte) (string, bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly|parser.ParseComments)
	if err != nil {
		return "", false, err
	}
	var b strings.Builder
	usesC := false
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.IMPORT {
			continue
		}
		for _, spec := range gen.Specs {
			imp := spec.(*ast.ImportSpec)
			if p, _ := strconv.Unquote(imp.Path.Value); p != "C" {
				continue
			}
			usesC = true
			for _, cg := range []*ast.CommentGroup{imp.Doc, gen.Doc} {
				if cg == nil {
					continue
				}
				for _, c := range cg.List {
					switch t := c.Text; {
					case strings.HasPrefix(t, "//"):
						b.WriteString(t[2:] + "\n")
					case strings.HasPrefix(t, "/*"):
						b.WriteString(strings.TrimSuffix(t[2:], "*/") + "\n")
					}
				}
			}
		}
	}
	return b.String(), usesC, nil
}

var flagPrefix = regexp.MustCompile(`^-+[A-Za-z_]*`)

// cgoArgRefs returns the path-like parts of one #cgo argument.
func cgoArgRefs(arg string) []string {
	var out []string
	for _, piece := range strings.FieldsFunc(arg, func(r rune) bool { return r == ',' || r == '=' }) {
		if !strings.HasPrefix(piece, "${") && !strings.HasPrefix(piece, "/") && !strings.HasPrefix(piece, ".") {
			piece = flagPrefix.ReplaceAllString(piece, "")
		}
		if piece != "" && (strings.ContainsAny(piece, `/\$`) || strings.HasPrefix(piece, ".")) {
			out = append(out, piece)
		}
	}
	return out
}

// resolveCgo resolves a #cgo path against the package directory dir. ok is
// false for absolute paths, variables other than ${SRCDIR}, backslashes and
// repository escapes.
func resolveCgo(dir, ref string) (string, bool) {
	if strings.Contains(ref, `\`) {
		return "", false
	}
	if rest, ok := strings.CutPrefix(ref, "${SRCDIR}"); ok {
		if strings.Contains(rest, "$") || (rest != "" && !strings.HasPrefix(rest, "/")) {
			return "", false
		}
		ref = "." + rest
	} else if strings.Contains(ref, "$") || strings.HasPrefix(ref, "/") {
		return "", false
	}
	p := path.Join(dir, ref)
	if p == ".." || strings.HasPrefix(p, "../") {
		return "", false
	}
	return p, true
}

// checkCgoPackage applies the allowlist rules to one cgo package.
func checkCgoPackage(fsys fs.FS, pkg string) ([]Violation, error) {
	var out []Violation
	allowed := append([]string{pkg}, CgoIncludeDirs...)
	inAllowed := func(p string) bool {
		for _, dir := range allowed {
			if within(p, dir) {
				return true
			}
		}
		return false
	}
	// Seed: every Go preamble and every C-family file in the package and
	// the allowed directories.
	type unit struct {
		file string
		src  []byte
	}
	var queue []unit
	seen := map[string]bool{}
	// enqueue schedules a repository file for include scanning once.
	enqueue := func(file string) error {
		if seen[file] {
			return nil
		}
		seen[file] = true
		src, err := fs.ReadFile(fsys, file)
		if err != nil {
			return err
		}
		queue = append(queue, unit{file, src})
		return nil
	}
	regular := func(p string) bool {
		info, err := fs.Stat(fsys, p)
		return err == nil && info.Mode().IsRegular()
	}
	for _, dir := range allowed {
		entries, err := fs.ReadDir(fsys, dir)
		if err != nil {
			continue // An allowed directory a fixture lacks is not an error.
		}
		for _, e := range entries {
			name := path.Join(dir, e.Name())
			if e.IsDir() {
				continue
			}
			src, err := fs.ReadFile(fsys, name)
			if err != nil {
				return nil, err
			}
			switch {
			case strings.HasSuffix(name, ".go") && dir == pkg:
				text, usesC, err := preamble(name, src)
				if err != nil {
					return nil, err
				}
				if !usesC {
					continue
				}
				for _, line := range strings.Split(text, "\n") {
					t := strings.TrimSpace(line)
					if !strings.HasPrefix(t, "#cgo") {
						continue
					}
					if strings.HasSuffix(t, `\`) {
						out = append(out, Violation{"A", name, "(#cgo line continuation)"})
						continue
					}
					_, args, ok := strings.Cut(t, ":")
					if !ok {
						out = append(out, Violation{"A", name, "(malformed #cgo directive)"})
						continue
					}
					for _, arg := range strings.Fields(args) {
						for _, ref := range cgoArgRefs(arg) {
							p, ok := resolveCgo(pkg, ref)
							if !ok || !inAllowed(p) {
								out = append(out, Violation{"A", name, "cgo flag " + ref})
								continue
							}
							// A flag naming a file (-include, an archive) puts
							// it in the build: scan it like an included file.
							if regular(p) {
								if err := enqueue(p); err != nil {
									return nil, err
								}
							}
						}
					}
				}
				queue = append(queue, unit{name, []byte(text)})
			case hasExt(name, buildWithoutCgo):
				out = append(out, Violation{"A", name, "(assembly, .syso or SWIG source)"})
			case hasExt(name, buildableExts):
				if err := enqueue(name); err != nil {
					return nil, err
				}
			}
		}
	}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		incs, problems := includes(u.src)
		for _, p := range problems {
			out = append(out, Violation{"A", u.file, p})
		}
		for _, inc := range incs {
			if inc.kind == "angle" {
				if inc.path == "" || strings.ContainsAny(inc.path, `/\`) || strings.Contains(inc.path, "..") {
					out = append(out, Violation{"A", u.file, "include <" + inc.path + "> (system header with a path)"})
					continue
				}
				// -I paths are searched for angle includes too, so a name
				// that exists in an allowed directory is scanned.
				for _, dir := range allowed {
					if p := path.Join(dir, inc.path); regular(p) {
						if err := enqueue(p); err != nil {
							return nil, err
						}
					}
				}
				continue
			}
			// A quoted include is searched beside the including file, then
			// on -I paths, which are confined to the allowed directories.
			target := ""
			if !strings.ContainsAny(inc.path, `\$`) && !strings.HasPrefix(inc.path, "/") {
				for _, dir := range append([]string{path.Dir(u.file)}, allowed...) {
					p := path.Join(dir, inc.path)
					if info, err := fs.Stat(fsys, p); err == nil && info.Mode().IsRegular() && inAllowed(p) && !strings.HasPrefix(p, "../") {
						target = p
						break
					}
				}
			}
			if target == "" {
				out = append(out, Violation{"A", u.file, "include \"" + inc.path + "\" (not found in allowed directories)"})
				continue
			}
			if err := enqueue(target); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

// cgoAudit applies the cgo allowlist to every package outside CgoPackages.
// visit is called by the rule A walk for every file it sees.
type cgoAudit struct {
	fsys     fs.FS
	packages map[string]bool // directories holding .go files
	pending  []string        // buildable non-Go files, judged in finish
	out      []Violation
}

func (a *cgoAudit) visit(file string, imports []string) {
	dir := path.Dir(file)
	if slices.Contains(CgoPackages, dir) {
		return // Checked in full by checkCgoPackage.
	}
	if strings.HasSuffix(file, ".go") {
		a.packages[dir] = true
		if slices.Contains(imports, "C") {
			a.out = append(a.out, Violation{"A", file, "(cgo outside the allowlist)"})
		}
		return
	}
	if hasExt(file, buildableExts) {
		a.pending = append(a.pending, file)
	}
}

// finish reports buildable non-Go files in Go package directories. They
// are recorded until the walk has seen every .go file.
func (a *cgoAudit) finish() []Violation {
	sort.Strings(a.pending)
	for _, file := range a.pending {
		dir := path.Dir(file)
		if !a.packages[dir] && !a.hasGo(dir) {
			continue // Not a Go package; the go command never builds it.
		}
		if slices.Contains(InertCSourceDirs, dir) && !hasExt(file, buildWithoutCgo) {
			continue
		}
		a.out = append(a.out, Violation{"A", file, "(non-Go source outside the cgo allowlist)"})
	}
	return a.out
}

func (a *cgoAudit) hasGo(dir string) bool {
	entries, _ := fs.ReadDir(a.fsys, dir)
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") {
			return true
		}
	}
	return false
}
