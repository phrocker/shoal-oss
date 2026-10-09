// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

// Package executorref is the single definition of a valid executor
// reference: the name that ties a fleet descriptor to the executor that
// performs its actions, that an ATPL policy declares, that an attestation
// presentation names, and that an action-execution authorization decision is
// bound to (#391).
//
// Every one of those places calls ValidExecutorRef, so a reference that one
// accepts is accepted by all of them, and a worker can always be bound to the
// reference its descriptor registered. A reference is compared byte for byte
// everywhere it is used, so the rule also refuses every form that could look
// like another reference without being byte-identical to it: invisible and
// format characters, non-ASCII spaces, compatibility forms and decomposed
// sequences. A non-normalized reference is refused, never rewritten, because
// rewriting would change what fingerprints and digests cover.
//
// The package imports only the standard library and golang.org/x/text, so the
// fleet, the policy compiler, the public attestation contract and the
// authorization package may all depend on it.
package executorref

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// MaxExecutorRefBytes bounds an executor reference in bytes.
const MaxExecutorRefBytes = 1024

// ValidExecutorRef reports why ref is not a valid executor reference, or nil.
// A valid reference is non-empty, at most MaxExecutorRefBytes, valid UTF-8,
// made only of printable runes (unicode.IsPrint: no controls, format,
// zero-width or bidi characters) with ASCII space as the only space and none
// at either end, does not begin with a combining mark, and is already in NFKC
// form.
func ValidExecutorRef(ref string) error {
	if ref == "" {
		return errors.New("executor reference is required")
	}
	if len(ref) > MaxExecutorRefBytes {
		return errors.New("executor reference is outside its bound")
	}
	if !utf8.ValidString(ref) {
		return errors.New("executor reference is not valid UTF-8")
	}
	if strings.TrimSpace(ref) != ref {
		return errors.New("executor reference has surrounding whitespace")
	}
	// unicode.IsPrint admits letters, marks, numbers, punctuation, symbols and
	// U+0020 only. Every other space (NBSP, U+2003 and the rest of Zs) is
	// refused here, as are controls and format characters (zero-width and
	// bidi), so no separate space check is needed.
	for _, character := range ref {
		if !unicode.IsPrint(character) {
			return errors.New("executor reference contains a non-printable character or a non-ASCII space")
		}
	}
	// A leading combining mark has no base of its own: it renders on whatever
	// precedes the reference wherever it is displayed.
	if first, _ := utf8.DecodeRuneInString(ref); unicode.In(first, unicode.M) {
		return errors.New("executor reference begins with a combining mark")
	}
	if norm.NFKC.String(ref) != ref {
		return errors.New("executor reference is not in NFKC form")
	}
	return nil
}
