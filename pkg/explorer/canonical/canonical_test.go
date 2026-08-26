/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package canonical

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

var updateGolden = flag.Bool(
	"update", false, "update canonical codec golden fixtures")

func TestV1GoldenDeterminismAndRoundTrip(t *testing.T) {
	record := fixtureRecord(false)
	encoded, err := MarshalV1(record)
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		repeated, err := MarshalV1(record)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(repeated, encoded) {
			t.Fatal("repeated marshal changed canonical bytes")
		}
	}

	reordered, err := MarshalV1(fixtureRecord(true))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reordered, encoded) {
		t.Fatal("map insertion or time-zone representation changed canonical bytes")
	}

	goldenPath := filepath.Join("testdata", "record_v1.bin")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	golden, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, golden) {
		t.Fatalf(
			"canonical bytes differ from golden: got %x, want %x",
			encoded,
			golden,
		)
	}

	decoded, err := UnmarshalV1(golden)
	if err != nil {
		t.Fatal(err)
	}
	want := fixtureRecord(false)
	want.Revision.CreatedAt = want.Revision.CreatedAt.UTC()
	want.Publication.PublishedAt = want.Publication.PublishedAt.UTC()
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("decoded record differs:\ngot  %#v\nwant %#v", decoded, want)
	}
	if math.Float64bits(float64(decoded.Edges[0].Weight)) !=
		math.Float64bits(math.Copysign(0, -1)) {
		t.Fatal("edge weight bits were not preserved")
	}
	remarshaled, err := MarshalV1(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(remarshaled, golden) {
		t.Fatal("decoded record did not remarshal to the golden bytes")
	}
}

func TestV1DigestAndSourceSHA256(t *testing.T) {
	record := fixtureRecord(false)
	encoded, err := MarshalV1(record)
	if err != nil {
		t.Fatal(err)
	}

	digest, err := Digest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	recordDigest, err := DigestV1(record)
	if err != nil {
		t.Fatal(err)
	}
	if recordDigest != digest {
		t.Fatalf("record digest = %s, canonical digest = %s", recordDigest, digest)
	}
	if got, want := digest.String(),
		"b5d729e244cabdf61d26f1004256179dd66f8dbaa2f588ec1b5c43b397b32cc3"; got != want {
		t.Fatalf("canonical digest = %s, want %s", got, want)
	}
	if got, want := SourceSHA256(record.Source).String(),
		"ba94e9182f80ffe2cf3b6f7dfe0eac43813f55fcca6bd6596b6093f52570ece6"; got != want {
		t.Fatalf("source digest = %s, want %s", got, want)
	}
	if err := VerifyChecksum(encoded); err != nil {
		t.Fatal(err)
	}
}

func TestV1PreservesOrderedSlicesAndEmptyPublication(t *testing.T) {
	record := fixtureRecord(false)
	record.Publication = nil
	record.Sections[0], record.Sections[1] = record.Sections[1], record.Sections[0]
	record.Spans[0], record.Spans[1] = record.Spans[1], record.Spans[0]
	record.Nodes[0], record.Nodes[1] = record.Nodes[1], record.Nodes[0]

	encoded, err := MarshalV1(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalV1(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Publication != nil {
		t.Fatal("absent publication metadata became present")
	}
	if decoded.Sections[0].ID != record.Sections[0].ID ||
		decoded.Spans[0].ID != record.Spans[0].ID ||
		decoded.Nodes[0].ID != record.Nodes[0].ID {
		t.Fatal("ordered slices were not preserved")
	}
}

func TestV1MarshalRejectsInvalidRecords(t *testing.T) {
	tests := map[string]func(*RecordV1){
		"duplicate graph node ID": func(record *RecordV1) {
			record.Nodes[1].ID = record.Nodes[0].ID
		},
		"duplicate graph edge ID": func(record *RecordV1) {
			record.Edges = append(record.Edges, record.Edges[0])
		},
		"invalid source": func(record *RecordV1) {
			record.Source = []byte{0xff}
		},
		"invalid edge": func(record *RecordV1) {
			record.Edges[0].Type = " "
		},
		"invalid publication time": func(record *RecordV1) {
			record.Publication.PublishedAt = time.Date(
				10_000, time.January, 1, 0, 0, 0, 0, time.UTC)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			record := fixtureRecord(false)
			mutate(&record)
			_, err := MarshalV1(record)
			assertInvalidArgument(t, err)
		})
	}
}

func TestV1UnmarshalRejectsCorruption(t *testing.T) {
	valid, err := MarshalV1(fixtureRecord(false))
	if err != nil {
		t.Fatal(err)
	}
	payloadLengthOffset := envelopeHeaderBytes - 8

	tests := map[string]func(*testing.T) []byte{
		"truncated header": func(*testing.T) []byte {
			return append([]byte(nil), valid[:envelopeHeaderBytes-1]...)
		},
		"truncated payload": func(*testing.T) []byte {
			return append([]byte(nil), valid[:len(valid)-1]...)
		},
		"invalid magic": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			corrupt[0] ^= 0xff
			return corrupt
		},
		"unknown version": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			binary.BigEndian.PutUint16(
				corrupt[len(canonicalMagic):], VersionV1+1)
			return corrupt
		},
		"unknown kind": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			binary.BigEndian.PutUint16(
				corrupt[len(canonicalMagic)+2:], uint16(KindRecord)+1)
			return corrupt
		},
		"excessive envelope length": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			binary.BigEndian.PutUint64(
				corrupt[payloadLengthOffset:], uint64(maxPayloadBytes)+1)
			return corrupt
		},
		"declared length too long": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			length := binary.BigEndian.Uint64(corrupt[payloadLengthOffset:])
			binary.BigEndian.PutUint64(corrupt[payloadLengthOffset:], length+1)
			return corrupt
		},
		"declared length too short": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			length := binary.BigEndian.Uint64(corrupt[payloadLengthOffset:])
			binary.BigEndian.PutUint64(corrupt[payloadLengthOffset:], length-1)
			return corrupt
		},
		"checksum mismatch": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			corrupt[len(corrupt)-1] ^= 0xff
			return corrupt
		},
		"trailing bytes": func(*testing.T) []byte {
			return append(cloneBytes(valid), 0)
		},
		"invalid decoded source": func(t *testing.T) []byte {
			return replacePayloadBytes(t, valid, []byte("café"), []byte{
				'C', 'a', 'f', 0xff, 0xa9,
			})
		},
		"duplicate decoded node ID": func(t *testing.T) []byte {
			return replacePayloadBytes(
				t, valid, []byte("node\x00B"), []byte("node\x00A"))
		},
		"invalid decoded edge": func(t *testing.T) []byte {
			return replacePayloadBytes(
				t, valid, []byte("relates"), []byte("       "))
		},
		"invalid publication presence": func(*testing.T) []byte {
			corrupt := cloneBytes(valid)
			corrupt[envelopeHeaderBytes] = 2
			rewriteChecksum(corrupt)
			return corrupt
		},
		"oversized source before allocation": func(*testing.T) []byte {
			payload := []byte{0, 0x20, 0x00, 0x00, 0x01}
			return wrapPayload(payload)
		},
	}

	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			encoded := corrupt(t)
			_, err := UnmarshalV1(encoded)
			assertInvalidArgument(t, err)
			if _, err := Digest(encoded); err == nil {
				t.Fatal("Digest accepted corrupt canonical bytes")
			} else {
				assertInvalidArgument(t, err)
			}
		})
	}
}

func fixtureRecord(alternateInsertion bool) RecordV1 {
	source := []byte("Root café\nChild 🐚\n")
	childStart := int64(bytes.Index(source, []byte("Child")))
	documentID := shoal.ID("doc\x00\xff")
	revisionID := shoal.ID("rev\x00\xfe")
	rootID := shoal.ID("section\x00root")
	childID := shoal.ID("section\x00child")

	createdAt := time.Date(
		2024, time.March, 4, 5, 6, 7, 8,
		time.FixedZone("source-zone", -7*60*60),
	)
	publishedAt := time.Date(
		2025, time.April, 5, 6, 7, 8, 9,
		time.FixedZone("publication-zone", 5*60*60+30*60),
	)
	if alternateInsertion {
		createdAt = createdAt.UTC()
		publishedAt = publishedAt.UTC()
	}

	return RecordV1{
		Source: source,
		Document: document.Document{
			ID:            documentID,
			RevisionID:    revisionID,
			Title:         "Café document",
			RootSectionID: rootID,
			Metadata: metadata(alternateInsertion,
				[2]string{"\xfe", "\x00"},
				[2]string{"\x00key", "value\x00\xff"},
			),
		},
		Revision: document.Revision{
			ID:            revisionID,
			DocumentID:    documentID,
			CreatedAt:     createdAt,
			SourceVersion: "",
			Metadata:      nil,
		},
		Sections: []document.Section{
			{
				ID:         rootID,
				DocumentID: documentID,
				RevisionID: revisionID,
				ParentID:   "",
				Order:      0,
				Heading:    "",
				Range: document.SourceRange{
					Start: document.SourcePosition{Offset: 0, Page: 1},
					End: document.SourcePosition{
						Offset: int64(len(source)), Page: 2,
					},
				},
				Metadata: nil,
			},
			{
				ID:         childID,
				DocumentID: documentID,
				RevisionID: revisionID,
				ParentID:   rootID,
				Order:      1,
				Heading:    "Child 🐚",
				Range: document.SourceRange{
					Start: document.SourcePosition{
						Offset: childStart, Page: 2,
					},
					End: document.SourcePosition{
						Offset: int64(len(source)), Page: 2,
					},
				},
				Metadata: metadata(alternateInsertion,
					[2]string{"section\x00", "\xff"},
				),
			},
		},
		Spans: []document.Span{
			{
				ID:         shoal.ID("span\x00root"),
				DocumentID: documentID,
				RevisionID: revisionID,
				SectionID:  rootID,
				Order:      0,
				Range: document.SourceRange{
					Start: document.SourcePosition{Offset: 0, Page: 1},
					End: document.SourcePosition{
						Offset: childStart, Page: 1,
					},
				},
				Text: string(source[:childStart]),
				Metadata: metadata(alternateInsertion,
					[2]string{"\x80", "span\x00\xff"},
				),
			},
			{
				ID:         shoal.ID("span\x00child"),
				DocumentID: documentID,
				RevisionID: revisionID,
				SectionID:  childID,
				Order:      0,
				Range: document.SourceRange{
					Start: document.SourcePosition{
						Offset: childStart, Page: 2,
					},
					End: document.SourcePosition{
						Offset: int64(len(source)), Page: 2,
					},
				},
				Text:     string(source[childStart:]),
				Metadata: nil,
			},
		},
		Nodes: []graph.Node{
			{
				ID:     shoal.ID("node\x00A"),
				Kind:   "",
				Labels: []string{"root", "café"},
				Properties: metadata(alternateInsertion,
					[2]string{"\xffnode", "\x00value"},
					[2]string{"a", "b"},
				),
			},
			{
				ID:         shoal.ID("node\x00B"),
				Kind:       "topic",
				Labels:     nil,
				Properties: nil,
			},
		},
		Edges: []graph.Edge{
			{
				ID:     shoal.ID("edge\x00\xfb"),
				From:   shoal.ID("node\x00A"),
				To:     shoal.ID("node\x00B"),
				Type:   "relates",
				Weight: shoal.Score(math.Copysign(0, -1)),
				Properties: metadata(alternateInsertion,
					[2]string{"edge\x00", "\xfe\x00"},
				),
			},
		},
		Publication: &PublicationV1{
			Sequence:    42,
			PublishedAt: publishedAt,
		},
	}
}

func metadata(alternateInsertion bool, entries ...[2]string) shoal.Metadata {
	result := make(shoal.Metadata, len(entries))
	if alternateInsertion {
		for i := len(entries) - 1; i >= 0; i-- {
			result[entries[i][0]] = entries[i][1]
		}
		return result
	}
	for _, entry := range entries {
		result[entry[0]] = entry[1]
	}
	return result
}

func replacePayloadBytes(
	t *testing.T,
	encoded []byte,
	old []byte,
	replacement []byte,
) []byte {
	t.Helper()
	if len(old) != len(replacement) {
		t.Fatal("replacement must preserve payload length")
	}
	corrupt := cloneBytes(encoded)
	payload := corrupt[envelopeHeaderBytes : len(corrupt)-envelopeChecksumSize]
	offset := bytes.Index(payload, old)
	if offset < 0 {
		t.Fatalf("payload does not contain %x", old)
	}
	copy(payload[offset:], replacement)
	rewriteChecksum(corrupt)
	return corrupt
}

func rewriteChecksum(encoded []byte) {
	sum := sha256.Sum256(encoded[:len(encoded)-envelopeChecksumSize])
	copy(encoded[len(encoded)-envelopeChecksumSize:], sum[:])
}

func wrapPayload(payload []byte) []byte {
	encoded := make(
		[]byte, envelopeHeaderBytes+len(payload)+envelopeChecksumSize)
	copy(encoded, canonicalMagic)
	binary.BigEndian.PutUint16(encoded[len(canonicalMagic):], VersionV1)
	binary.BigEndian.PutUint16(
		encoded[len(canonicalMagic)+2:], uint16(KindRecord))
	binary.BigEndian.PutUint64(
		encoded[envelopeHeaderBytes-8:], uint64(len(payload)))
	copy(encoded[envelopeHeaderBytes:], payload)
	rewriteChecksum(encoded)
	return encoded
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func assertInvalidArgument(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("error = %v, want invalid_argument", err)
	}
}
