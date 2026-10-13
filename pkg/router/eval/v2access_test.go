// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// sealedNames are the identifiers through which code can reach the sealed
// split's cases or bytes. sealedFile is the archive's name; a string literal
// holding it is a way to it too.
var sealedNames = map[string]bool{
	"sealedV2": true, "openArchive": true, "v2Archive": true, "v2Auditor": true,
	"PreRegisteredV2": true, "CheckV2Seal": true,
}

const sealedFile = "test.jsonl.gz"

// sealedAccess is every (file, function) allowed to name the sealed split:
// the seal tests, the leak guard and the run. "" is a file's top-level
// declarations. Any other use under the audit roots fails, so a new test
// cannot route, print or tune on the split without editing this list in
// review.
var sealedAccess = map[string]map[string]bool{
	"pkg/router/eval/v2gate.go": {"CheckV2Seal": true, "PreRegisteredV2": true},
	"pkg/router/eval/v2seal_test.go": {
		"": true, "openArchive": true, "sealedV2": true,
		"TestV2TestSplitSealed": true, "TestV2SealRefusesAnyChange": true, "TestV2TestLabelsWellFormed": true,
	},
	"pkg/router/eval/leak_test.go": {
		"v2Auditor": true, "v2CueSources": true, "TestNoTestPhrasingInCueSources": true,
		"TestV2BaselineIsClean": true, "TestV2TestIsFresh": true, "TestLeakGuardCatchesLeaks": true,
	},
	"pkg/router/eval/v2run_test.go":    {"TestEvaluationV2Test": true},
	"pkg/router/eval/v2access_test.go": {"": true},
}

// TestSealedSplitAccess walks every Go file under the audit roots and
// requires that only the functions in sealedAccess name the sealed split.
func TestSealedSplitAccess(t *testing.T) {
	var violations []string
	for _, root := range auditRoots {
		err := filepath.WalkDir(filepath.Join(moduleRootRel, root), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") {
				return err
			}
			rel, err := filepath.Rel(moduleRootRel, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			check := func(fn string, node ast.Node) {
				ast.Inspect(node, func(n ast.Node) bool {
					hit := ""
					switch x := n.(type) {
					case *ast.Ident:
						if sealedNames[x.Name] {
							hit = x.Name
						}
					case *ast.BasicLit:
						if x.Kind == token.STRING {
							if s, err := strconv.Unquote(x.Value); err == nil && strings.Contains(s, sealedFile) {
								hit = strconv.Quote(sealedFile)
							}
						}
					}
					if hit != "" && !sealedAccess[rel][fn] {
						where := fn
						if where == "" {
							where = "top level"
						}
						violations = append(violations, fset.Position(n.Pos()).String()+": "+where+" names "+hit)
					}
					return true
				})
			}
			for _, decl := range f.Decls {
				if fd, ok := decl.(*ast.FuncDecl); ok {
					check(fd.Name.Name, fd)
				} else {
					check("", decl)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(violations)
	for _, v := range violations {
		t.Errorf("sealed split reached outside the seal, leak and run tests: %s", v)
	}
}
