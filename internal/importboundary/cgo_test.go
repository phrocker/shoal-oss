// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package importboundary

import (
	"os"
	"reflect"
	"slices"
	"testing"
)

// The preprocessor spellings that defeated the old deny-list either read as
// the literal the compiler sees or fail closed.
func TestIncludeParsing(t *testing.T) {
	for name, tc := range map[string]struct {
		src      string
		includes []include
		problems int
	}{
		"plain":            {"#include \"a.h\"\n#include <stdio.h>\n", []include{{"quote", "a.h"}, {"angle", "stdio.h"}}, 0},
		"spaced":           {"  #  include   \"a.h\"  \n", []include{{"quote", "a.h"}}, 0},
		"continuation":     {"#include \\\n\"../x.h\"\n", []include{{"quote", "../x.h"}}, 0},
		"split directive":  {"#incl\\\nude \"../x.h\"\n", []include{{"quote", "../x.h"}}, 0},
		"comment in #":     {"#/**/include \"../x.h\"\n", []include{{"quote", "../x.h"}}, 0},
		"leading comment":  {"/* x */ #include \"../x.h\"\n", []include{{"quote", "../x.h"}}, 0},
		"commented out":    {"// #include \"../x.h\"\n/* #include \"../y.h\" */\n", nil, 0},
		"string with //":   {"char *s = \"//\";\n#include \"a.h\"\n", []include{{"quote", "a.h"}}, 0},
		"macro":            {"#define H \"../x.h\"\n#include H\n", nil, 1},
		"computed angle":   {"#include <a.h> extra\n", nil, 1},
		"digraph":          {"%:include \"../x.h\"\n", nil, 1},
		"trigraph":         {"??=include \"../x.h\"\n", nil, 1},
		"unterminated":     {"/* never closed\n#include \"a.h\"\n", nil, 1},
		"line marker":      {"# 1 \"x.h\"\n", nil, 1},
		"embed":            {"#embed \"blob.bin\"\n", []include{{"quote", "blob.bin"}}, 0},
		"null directive":   {"#\n#define X 1\n", nil, 0},
		"include_next":     {"#include_next <a.h>\n", []include{{"angle", "a.h"}}, 0},
		"objc import":      {"#import \"a.h\"\n", []include{{"quote", "a.h"}}, 0},
		"trailing comment": {"#include \"a.h\" // why\n", []include{{"quote", "a.h"}}, 0},
	} {
		incs, problems := includes([]byte(tc.src))
		if !reflect.DeepEqual(incs, tc.includes) || len(problems) != tc.problems {
			t.Errorf("%s: includes %v problems %v", name, incs, problems)
		}
	}
	for ref, want := range map[string]string{
		"${SRCDIR}/inc":       "a/b/inc",
		"${SRCDIR}":           "a/b",
		"${SRCDIR}/../../x":   "x",
		"../../../escape":     "",
		"${SRCDIR}/../../../": "",
		"${HOME}/lib":         "",
		"${SRCDIR}x":          "",
		"/usr/include":        "",
		`..\x`:                "",
	} {
		got, ok := resolveCgo("a/b", ref)
		if (want == "") == ok || got != want {
			t.Errorf("%q resolved to %q ok=%v, want %q", ref, got, ok, want)
		}
	}
	if got := cgoArgRefs("-Wl,-rpath,${SRCDIR}/lib"); !slices.Equal(got, []string{"${SRCDIR}/lib"}) {
		t.Errorf("cgoArgRefs: %q", got)
	}
}

// The real cgo user complies and is actually examined.
func TestRepositoryCgoIsExamined(t *testing.T) {
	root := os.DirFS("../..")
	for _, pkg := range CgoPackages {
		found, err := checkCgoPackage(root, pkg)
		if err != nil || len(found) != 0 {
			t.Fatalf("%s: %v %v", pkg, found, err)
		}
	}
	src, err := os.ReadFile("../../cmd/shoal-capi/export.go")
	if err != nil {
		t.Fatal(err)
	}
	text, usesC, err := preamble("export.go", src)
	if err != nil || !usesC {
		t.Fatal("shoal-capi preamble not found", err)
	}
	if incs, _ := includes([]byte(text)); !slices.Contains(incs, include{"quote", "bridge.h"}) {
		t.Fatalf("shoal-capi includes %v", incs)
	}
}
