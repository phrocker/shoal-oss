// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package itercfg

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"log/slog"
	"testing"

	"github.com/phrocker/shoal/internal/iterrt"
)

// silentLogger is a slog logger that discards everything; tests use it
// to avoid noisy stderr output when exercising the warn paths.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

// graphVidxStack mirrors a representative graph_vidx iterator config:
// latentEdgeDiscovery at priority 10, vers at priority 20.
func TestParseStack_GraphVidxMajc(t *testing.T) {
	props := map[string]string{
		"table.iterator.majc.latentEdgeDiscovery":                     "10,org.apache.accumulo.core.graph.LatentEdgeDiscoveryIterator",
		"table.iterator.majc.latentEdgeDiscovery.opt.maxCellBuffer":   "200",
		"table.iterator.majc.latentEdgeDiscovery.opt.maxPairsPerCell": "500",
		"table.iterator.majc.latentEdgeDiscovery.opt.similarityThreshold": "0.85",
		"table.iterator.majc.vers":               "20,org.apache.accumulo.core.iterators.user.VersioningIterator",
		"table.iterator.majc.vers.opt.maxVersions": "10",
	}
	got := parseStack("tbl-vidx", iterrt.ScopeMajc, "table.iterator.majc.", props, silentLogger())
	if !got.HasShoalCoverage() {
		t.Fatalf("expected full coverage, got skipped=%+v", got.Skipped)
	}
	if len(got.Stack) != 2 {
		t.Fatalf("stack len = %d, want 2; stack=%+v", len(got.Stack), got.Stack)
	}
	// Priority ascending → latentEdge (10) before versioning (20).
	if got.Stack[0].Name != iterrt.IterLatentEdgeDiscovery {
		t.Errorf("stack[0].Name = %q, want %q", got.Stack[0].Name, iterrt.IterLatentEdgeDiscovery)
	}
	if got.Stack[1].Name != iterrt.IterVersioning {
		t.Errorf("stack[1].Name = %q, want %q", got.Stack[1].Name, iterrt.IterVersioning)
	}
	if got.Stack[1].Options["maxVersions"] != "10" {
		t.Errorf("versioning maxVersions = %q, want 10", got.Stack[1].Options["maxVersions"])
	}
	if got.Stack[0].Options["similarityThreshold"] != "0.85" {
		t.Errorf("latentEdge similarityThreshold = %q, want 0.85", got.Stack[0].Options["similarityThreshold"])
	}
}

// Unknown classes get skipped and reported; the rest of the stack
// resolves normally so partial-coverage tables can still shadow.
func TestParseStack_SkipsUnknownClass(t *testing.T) {
	props := map[string]string{
		"table.iterator.majc.weird":               "5,com.example.NotPortedYet",
		"table.iterator.majc.weird.opt.something": "1",
		"table.iterator.majc.vers":                "20,org.apache.accumulo.core.iterators.user.VersioningIterator",
	}
	got := parseStack("tbl", iterrt.ScopeMajc, "table.iterator.majc.", props, silentLogger())
	if got.HasShoalCoverage() {
		t.Fatalf("expected NO shoal coverage; got %+v", got)
	}
	if len(got.Stack) != 1 || got.Stack[0].Name != iterrt.IterVersioning {
		t.Errorf("stack = %+v, want [versioning]", got.Stack)
	}
	if len(got.Skipped) != 1 || got.Skipped[0].Class != "com.example.NotPortedYet" {
		t.Errorf("skipped = %+v, want one NotPortedYet entry", got.Skipped)
	}
}

// Malformed header (no comma) is reported as a skipped iterator with
// empty class, not a hard error.
func TestParseStack_MalformedHeader(t *testing.T) {
	props := map[string]string{
		"table.iterator.majc.bad": "garbage-no-comma",
	}
	got := parseStack("tbl", iterrt.ScopeMajc, "table.iterator.majc.", props, silentLogger())
	if len(got.Stack) != 0 {
		t.Errorf("stack should be empty, got %+v", got.Stack)
	}
	// Two warns: parse failure (skipped), AND header missing (skipped).
	// The empty-class path puts it into Skipped with Class=="" — we
	// don't constrain to which warn fires, just that Skipped reflects it.
	found := false
	for _, s := range got.Skipped {
		if s.Name == "bad" {
			found = true
		}
	}
	if !found {
		t.Errorf("skipped should contain 'bad', got %+v", got.Skipped)
	}
}

// Empty scope returns an empty stack and HasShoalCoverage()=true (no
// iterators is a valid table state).
func TestParseStack_NoIteratorsConfigured(t *testing.T) {
	got := parseStack("tbl", iterrt.ScopeMajc, "table.iterator.majc.", nil, silentLogger())
	if !got.HasShoalCoverage() {
		t.Errorf("empty stack should be 'covered': skipped=%+v", got.Skipped)
	}
	if len(got.Stack) != 0 {
		t.Errorf("stack = %+v, want empty", got.Stack)
	}
}

// TestDecodePropBlob_GzipRoundtrip builds a versioned-props blob with
// the wire layout described in decodePropBlob's doc-comment and asserts
// the decoder reproduces the original map. Catches breakage in the
// version/compressed-flag/timestamp/gzip framing without needing a
// live cluster.
func TestDecodePropBlob_GzipRoundtrip(t *testing.T) {
	want := map[string]string{
		"table.iterator.majc.vers":                                            "20,org.apache.accumulo.core.iterators.user.VersioningIterator",
		"table.iterator.majc.vers.opt.maxVersions":                            "10",
		"table.iterator.majc.latentEdgeDiscovery":                             "10,org.apache.accumulo.core.graph.LatentEdgeDiscoveryIterator",
		"table.iterator.majc.latentEdgeDiscovery.opt.maxCellBuffer":           "200",
		"table.iterator.majc.latentEdgeDiscovery.opt.maxPairsPerCell":         "500",
		"table.iterator.majc.latentEdgeDiscovery.opt.similarityThreshold":     "0.85",
		"table.unrelated.someprop":                                            "ignored",
	}
	blob := buildSyntheticPropBlob(t, want, true /*gzip*/)

	got, err := decodePropBlob(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(want) {
		t.Errorf("entry count: got=%d want=%d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got=%q want=%q", k, got[k], v)
		}
	}
}

// TestDecodePropBlob_UncompressedRoundtrip exercises the compressed=false
// path. Some Accumulo configs disable gzip on small property sets.
func TestDecodePropBlob_UncompressedRoundtrip(t *testing.T) {
	want := map[string]string{
		"table.iterator.majc.vers":                 "20,VersioningIterator",
		"table.iterator.majc.vers.opt.maxVersions": "5",
	}
	blob := buildSyntheticPropBlob(t, want, false /*no gzip*/)
	got, err := decodePropBlob(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("key %q: got=%q want=%q", k, got[k], v)
		}
	}
}

// buildSyntheticPropBlob encodes a property map in Java's
// VersionedPropGzipCodec wire format. Identical layout to the Java code
// path so the decoder is exercised against the actual on-wire bytes.
func buildSyntheticPropBlob(t *testing.T, props map[string]string, compressed bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	// int32 version=1
	binary.Write(&buf, binary.BigEndian, uint32(1))
	// bool compressed
	if compressed {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	// UTF timestamp
	ts := "2026-05-15T13:09:09.000000000Z"
	binary.Write(&buf, binary.BigEndian, uint16(len(ts)))
	buf.WriteString(ts)

	// Payload: int32 count + UTF pairs.
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint32(len(props)))
	for k, v := range props {
		binary.Write(&payload, binary.BigEndian, uint16(len(k)))
		payload.WriteString(k)
		binary.Write(&payload, binary.BigEndian, uint16(len(v)))
		payload.WriteString(v)
	}

	if compressed {
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(payload.Bytes()); err != nil {
			t.Fatalf("gzip write: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
	} else {
		buf.Write(payload.Bytes())
	}
	return buf.Bytes()
}

func TestSplitPriorityClass(t *testing.T) {
	cases := []struct {
		in       string
		wantPri  int
		wantCls  string
		wantErr  bool
	}{
		{"10,foo.bar.Baz", 10, "foo.bar.Baz", false},
		{" 20 , foo.Bar ", 20, "foo.Bar", false},
		{"noComma", 0, "", true},
		{"abc,foo.Bar", 0, "", true},
		{"10,", 10, "", true},
	}
	for _, c := range cases {
		pri, cls, err := splitPriorityClass(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("%q: err = %v, wantErr = %v", c.in, err, c.wantErr)
			continue
		}
		if err == nil {
			if pri != c.wantPri || cls != c.wantCls {
				t.Errorf("%q: got (%d, %q), want (%d, %q)", c.in, pri, cls, c.wantPri, c.wantCls)
			}
		}
	}
}
