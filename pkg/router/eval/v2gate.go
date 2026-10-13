// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"errors"
	"fmt"
)

// The v2 test split is sealed (docs/router-evaluation-v2.md). It is
// committed as testdata/eval/v2/test.jsonl.gz, compressed so that it does
// not show up in a diff, a grep or a code search of the tree. The digests
// below were recorded in the pre-registration commit, before any v2 grammar,
// lexicon, feature or model work.
const (
	// V2TestDigest is the Digest("test") of the sealed split: the SHA-256 of
	// its decompressed bytes, which are exactly its case lines, each followed
	// by a newline.
	V2TestDigest = "5510513c6c0935dff657355503167fd193ad5b0374e7c56f949bcdb83266df53"
	// V2TestArchiveSHA256 is the SHA-256 of the committed test.jsonl.gz.
	V2TestArchiveSHA256 = "a81dbc1e6b8a0cffc1da159b88eef4e52d1369b3d8b254d9659e921f3b76a0b8"
	// V2TestCases is the number of cases in the sealed split.
	V2TestCases = 138
	// V2WorldSHA256 is the SHA-256 of testdata/eval/v2/world.json, which is
	// byte-equal to v1's world. The sealed labels refer to its node keys and
	// targets, so it is frozen with them.
	V2WorldSHA256 = "7684d173e8fe64f2513bc52e7880fccfad356eff5bbf999dfdc34b4957140748"
)

// V2TestRunOpen opens the v2 test split for its one pre-registered run. It
// is false until the run's own pull request, which changes nothing else.
const V2TestRunOpen = false

// ErrV2Sealed is returned while the v2 test split is sealed.
var ErrV2Sealed = errors.New("the v2 test split is sealed until its pre-registered run")

// CheckV2Seal reports whether cs is exactly the sealed v2 test split: only
// test cases, as many as were registered, with the registered digest.
func (cs *Cases) CheckV2Seal() error {
	if len(cs.all) != V2TestCases {
		return fmt.Errorf("v2 test split holds %d cases, not the registered %d", len(cs.all), V2TestCases)
	}
	for _, c := range cs.all {
		if c.Split != "test" {
			return fmt.Errorf("v2 test split holds case %s of split %q", c.ID, c.Split)
		}
	}
	if digest := cs.Digest("test"); digest != V2TestDigest {
		return fmt.Errorf("v2 test split %s is not the registered %s", digest, V2TestDigest)
	}
	return nil
}

// PreRegisteredV2 is the only way to the v2 test split's cases. It refuses
// while the split is sealed, and refuses any split but the registered one.
func (cs *Cases) PreRegisteredV2() ([]Case, error) {
	if err := cs.CheckV2Seal(); err != nil {
		return nil, err
	}
	if !V2TestRunOpen {
		return nil, ErrV2Sealed
	}
	return cs.selectSplit("test"), nil
}
