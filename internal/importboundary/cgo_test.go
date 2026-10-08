// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package importboundary

import (
	"os"
	"slices"
	"testing"
)

func TestCgoRefsAndResolution(t *testing.T) {
	src := []byte("package p\n\n// #cgo linux CFLAGS: -I${SRCDIR}/inc -DX=1 -isystem ../sys\n// #cgo LDFLAGS: -L${HOME}/lib -Wl,--whole-archive,${SRCDIR}/../a.a\n// #include \"local.h\"\n// #include <stdlib.h>\nimport \"C\"\n")
	refs, err := cgoRefs("p/p.go", src)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"${SRCDIR}/inc", "../sys", "${HOME}/lib", "${SRCDIR}/../a.a", "local.h"}
	if !slices.Equal(refs, want) {
		t.Fatalf("refs %q, want %q", refs, want)
	}
	for ref, want := range map[string]string{
		"${SRCDIR}/inc":       "a/b/inc",
		"${SRCDIR}":           "a/b",
		"${SRCDIR}/../../x":   "x",
		"local.h":             "a/b/local.h",
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
	// A preamble on a grouped import, and a C file's quoted includes.
	grouped := []byte("package p\n\nimport (\n\t\"fmt\"\n\n\t/*\n\t#cgo CFLAGS: -I${SRCDIR}/../evil\n\t*/\n\t\"C\"\n)\n")
	if refs, err := cgoRefs("p/p.go", grouped); err != nil || !slices.Equal(refs, []string{"${SRCDIR}/../evil"}) {
		t.Fatalf("grouped preamble: %q %v", refs, err)
	}
	if refs, _ := cgoRefs("p/x.c", []byte("#include \"../y.h\"\n#  include \"z.h\"\n#include <a.h>\n")); !slices.Equal(refs, []string{"../y.h", "z.h"}) {
		t.Fatalf("C includes: %q", refs)
	}
}

// The real cgo user is examined, not skipped: shoal-capi's accepted
// ${SRCDIR}/../../capi/include stays in the root module.
func TestRepositoryCgoIsExamined(t *testing.T) {
	src, err := os.ReadFile("../../cmd/shoal-capi/export.go")
	if err != nil {
		t.Skip("cmd/shoal-capi not present")
	}
	refs, err := cgoRefs("cmd/shoal-capi/export.go", src)
	if err != nil || !slices.Contains(refs, "${SRCDIR}/../../capi/include") {
		t.Fatalf("shoal-capi refs %q %v", refs, err)
	}
	if p, ok := resolveCgo("cmd/shoal-capi", "${SRCDIR}/../../capi/include"); !ok || p != "capi/include" {
		t.Fatalf("resolved %q %v", p, ok)
	}
}
