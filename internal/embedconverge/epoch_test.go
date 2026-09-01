// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embedconverge

import (
	"errors"
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
)

func ref(entry string) FileRef {
	return FileRef{Table: "t", Extent: "t;;", Entry: entry}
}

func observations() []Observation {
	return []Observation{
		{Ref: ref("f-old.rf"), State: embeddingspace.Has("model-b")},
		{Ref: ref("f-target.rf"), State: embeddingspace.Has("model-a")},
		{Ref: ref("f-none.rf"), State: embeddingspace.NoEmbeddings()},
		{Ref: ref("f-unknown.rf"), State: embeddingspace.Unknown()},
	}
}

func testEpoch(t *testing.T) Epoch {
	t.Helper()
	epoch, err := Snapshot("e1", "t", "model-a", ModeForced, 42, observations())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	return epoch
}

func TestFileRefKeyDistinguishesFencedRanges(t *testing.T) {
	t.Parallel()

	a := FileRef{Table: "t", Extent: "t;;", Entry: "f.rf"}
	b := FileRef{Table: "t", Extent: "t;;", Entry: "f.rf R:(a,b,c,d)"}
	if a.Key() == b.Key() {
		t.Fatal("two references over one path with different ranges are different files")
	}
	// Length prefixing means no concatenation of fields can collide.
	c := FileRef{Table: "t", Extent: "", Entry: "t;;f.rf"}
	if a.Key() == c.Key() {
		t.Fatal("field boundaries must be unambiguous")
	}
}

func TestSnapshotMarksOnlyTargetFilesSkipped(t *testing.T) {
	t.Parallel()

	epoch := testEpoch(t)
	if len(epoch.Files) != 4 {
		t.Fatalf("epoch has %d files, want 4: skipped files stay in the denominator", len(epoch.Files))
	}
	byEntry := map[string]EpochFile{}
	for _, file := range epoch.Files {
		byEntry[file.Ref.Entry] = file
	}
	if got := byEntry["f-target.rf"].Status; got != StatusSkipped {
		t.Fatalf("f-target.rf status = %q, want %q", got, StatusSkipped)
	}
	for _, entry := range []string{"f-old.rf", "f-none.rf", "f-unknown.rf"} {
		if got := byEntry[entry].Status; got != StatusPending {
			t.Fatalf("%s status = %q, want %q", entry, got, StatusPending)
		}
	}
	// Files are ordered so two coordinators snapshotting the same
	// observations produce the same epoch.
	for i := 1; i < len(epoch.Files); i++ {
		if epoch.Files[i-1].Ref.Key() >= epoch.Files[i].Ref.Key() {
			t.Fatal("epoch files must be in a deterministic order")
		}
	}
}

func TestSnapshotRefusesUnusableInput(t *testing.T) {
	t.Parallel()

	valid := observations()
	cases := []struct {
		name string
		call func() (Epoch, error)
	}{
		{"no id", func() (Epoch, error) { return Snapshot(" ", "t", "model-a", ModeLazy, 0, valid) }},
		{"no table", func() (Epoch, error) { return Snapshot("e", " ", "model-a", ModeLazy, 0, valid) }},
		{"no target", func() (Epoch, error) { return Snapshot("e", "t", "  ", ModeLazy, 0, valid) }},
		{"unknown mode", func() (Epoch, error) { return Snapshot("e", "t", "model-a", Mode("wat"), 0, valid) }},
		{"foreign table", func() (Epoch, error) {
			return Snapshot("e", "t", "model-a", ModeLazy, 0, []Observation{
				{Ref: FileRef{Table: "other", Entry: "f.rf"}, State: embeddingspace.NoEmbeddings()},
			})
		}},
		{"duplicate ref", func() (Epoch, error) {
			return Snapshot("e", "t", "model-a", ModeLazy, 0, []Observation{
				{Ref: ref("f.rf"), State: embeddingspace.NoEmbeddings()},
				{Ref: ref("f.rf"), State: embeddingspace.NoEmbeddings()},
			})
		}},
		{"invalid state", func() (Epoch, error) {
			return Snapshot("e", "t", "model-a", ModeLazy, 0, []Observation{
				{Ref: ref("f.rf"), State: embeddingspace.FileState{State: "bogus"}},
			})
		}},
		{"no entry", func() (Epoch, error) {
			return Snapshot("e", "t", "model-a", ModeLazy, 0, []Observation{
				{Ref: FileRef{Table: "t"}, State: embeddingspace.NoEmbeddings()},
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.call(); !errors.Is(err, ErrInvalidEpoch) {
				t.Fatalf("err = %v, want ErrInvalidEpoch", err)
			}
		})
	}
}

func TestEpochRoundTrips(t *testing.T) {
	t.Parallel()

	epoch := testEpoch(t)
	raw, err := Encode(epoch)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.ID != epoch.ID || decoded.Target != epoch.Target || decoded.Mode != epoch.Mode {
		t.Fatalf("decoded header = %+v, want %+v", decoded, epoch)
	}
	if len(decoded.Files) != len(epoch.Files) {
		t.Fatalf("decoded %d files, want %d", len(decoded.Files), len(epoch.Files))
	}
	for i := range decoded.Files {
		if decoded.Files[i] != epoch.Files[i] {
			t.Fatalf("file %d = %+v, want %+v", i, decoded.Files[i], epoch.Files[i])
		}
	}
	if _, err := Decode(nil); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("Decode(nil) err = %v, want ErrInvalidEpoch", err)
	}
	if _, err := Decode([]byte("{")); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("Decode(garbage) err = %v, want ErrInvalidEpoch", err)
	}
}

func TestEpochValidateReDerivesConvergenceClaims(t *testing.T) {
	t.Parallel()

	// A hand-edited or corrupted epoch must not be able to mark an
	// unconverged file done and have the migration skip it forever.
	epoch := testEpoch(t)
	for i := range epoch.Files {
		if epoch.Files[i].Ref.Entry == "f-old.rf" {
			epoch.Files[i].Status = StatusConverged
		}
	}
	err := epoch.Validate()
	if !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("err = %v, want ErrInvalidEpoch", err)
	}

	epoch = testEpoch(t)
	for i := range epoch.Files {
		if epoch.Files[i].Ref.Entry == "f-none.rf" {
			epoch.Files[i].Status = StatusSkipped
		}
	}
	if err := epoch.Validate(); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("err = %v, want ErrInvalidEpoch for a bogus skip", err)
	}

	epoch = testEpoch(t)
	for i := range epoch.Files {
		if epoch.Files[i].Ref.Entry == "f-old.rf" {
			epoch.Files[i].Current = embeddingspace.Has("model-c")
		}
	}
	if err := epoch.Validate(); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("err = %v, want ErrInvalidEpoch for a third-space drift", err)
	}

	epoch = testEpoch(t)
	epoch.Files[0].Status = FileStatus("wat")
	if err := epoch.Validate(); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("err = %v, want ErrInvalidEpoch for an unknown status", err)
	}
}

func TestEncodeRefusesAnInvalidEpoch(t *testing.T) {
	t.Parallel()

	epoch := testEpoch(t)
	epoch.Target = ""
	if _, err := Encode(epoch); !errors.Is(err, ErrInvalidEpoch) {
		t.Fatalf("err = %v, want ErrInvalidEpoch", err)
	}
}
