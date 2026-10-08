// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package importboundary

import (
	"bufio"
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"path"
	"regexp"
	"strconv"
	"strings"
)

// cgo can compile or link code from outside a package without any Go import:
// a #cgo CFLAGS -I or LDFLAGS path, or a quoted #include, in the preamble of
// import "C" or in a C-family source file beside it. The checker collects
// every path those name and resolves it against the package directory, so
// Check can require that core paths stay out of extensions/ and extension
// paths stay inside their own module.
//
// Accepted pattern (used by cmd/shoal-capi): ${SRCDIR}-relative paths that
// leave the package but stay in the same module, such as
// -I${SRCDIR}/../../capi/include. Rejected, fail closed: absolute paths,
// other variables, backslashes, and anything that escapes the repository.

// cSourceExts are files whose quoted #include lines are followed. Go
// assembly (.s) uses the same preprocessor.
var cSourceExts = []string{".c", ".h", ".cc", ".cpp", ".cxx", ".hh", ".hpp", ".hxx", ".m", ".s", ".S", ".sx", ".f", ".F", ".f90"}

func isCSource(name string) bool {
	for _, ext := range cSourceExts {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

var flagPrefix = regexp.MustCompile(`^-+[A-Za-z_]*`)

// quotedInclude returns the path of a quoted #include or #import line.
func quotedInclude(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "#") {
		return "", false
	}
	rest := strings.TrimSpace(line[1:])
	for _, d := range []string{"include_next", "include", "import"} {
		if strings.HasPrefix(rest, d) {
			rest = strings.TrimSpace(rest[len(d):])
			if strings.HasPrefix(rest, `"`) {
				if end := strings.Index(rest[1:], `"`); end >= 0 {
					return rest[1 : end+1], true
				}
				return rest, true // Unterminated: report it, fail closed.
			}
			return "", false
		}
	}
	return "", false
}

// cgoArgRefs returns the path-like parts of one #cgo argument.
func cgoArgRefs(arg string) []string {
	var out []string
	for _, piece := range strings.FieldsFunc(arg, func(r rune) bool { return r == ',' || r == '=' }) {
		if !strings.HasPrefix(piece, "${") && !strings.HasPrefix(piece, "/") && !strings.HasPrefix(piece, ".") {
			piece = flagPrefix.ReplaceAllString(piece, "")
		}
		if piece == "" {
			continue
		}
		if strings.ContainsAny(piece, `/\$`) || strings.HasPrefix(piece, ".") {
			out = append(out, piece)
		}
	}
	return out
}

// preambleLines returns the raw lines of the cgo preamble comment.
func preambleLines(cg *ast.CommentGroup) []string {
	var lines []string
	for _, c := range cg.List {
		text := c.Text
		switch {
		case strings.HasPrefix(text, "//"):
			lines = append(lines, text[2:])
		case strings.HasPrefix(text, "/*"):
			lines = append(lines, strings.Split(strings.TrimSuffix(text[2:], "*/"), "\n")...)
		}
	}
	return lines
}

// cgoRefs returns the paths named by a Go file's cgo preamble, or by a C
// source file's quoted includes.
func cgoRefs(name string, src []byte) ([]string, error) {
	var lines []string
	if strings.HasSuffix(name, ".go") {
		file, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ImportsOnly|parser.ParseComments)
		if err != nil {
			return nil, err
		}
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
				for _, cg := range []*ast.CommentGroup{imp.Doc, gen.Doc} {
					if cg != nil {
						lines = append(lines, preambleLines(cg)...)
					}
				}
			}
		}
	} else {
		scanner := bufio.NewScanner(bytes.NewReader(src))
		scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	var out []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#cgo") {
			_, args, ok := strings.Cut(trimmed, ":")
			if !ok {
				out = append(out, "(malformed #cgo directive)")
				continue
			}
			for _, arg := range strings.Fields(args) {
				out = append(out, cgoArgRefs(arg)...)
			}
			continue
		}
		if inc, ok := quotedInclude(trimmed); ok {
			out = append(out, inc)
		}
	}
	return out, nil
}

// resolveCgo resolves ref against the package directory dir. ok is false
// for references that cannot be confined: absolute paths, variables other
// than ${SRCDIR}, backslashes, and paths that leave the repository.
func resolveCgo(dir, ref string) (string, bool) {
	if strings.Contains(ref, `\`) || strings.HasPrefix(ref, "(") {
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
