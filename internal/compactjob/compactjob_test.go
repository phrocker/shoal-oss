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

package compactjob

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/compaction"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/rfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/rfile/wire"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/manager"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
)

const (
	testECID    = ECIDPrefix + "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
	versClass   = "org.apache.accumulo.core.iterators.user.VersioningIterator"
	latentClass = "org.apache.accumulo.core.graph.LatentEdgeDiscoveryIterator"
)

// storedFile renders a whole-file StoredTabletFile metadata entry, the
// form Accumulo 3+ sends in InputFile.metadataFileEntry.
func storedFile(path string) string {
	return fmt.Sprintf(`{"path":"%s","startRow":"","endRow":""}`, path)
}

// fencedFile renders a row-fenced StoredTabletFile entry — a file whose
// referenced range is only part of the underlying RFile.
func fencedFile(path, start, end string) string {
	return fmt.Sprintf(`{"path":"%s","startRow":"%s","endRow":"%s"}`, path, start, end)
}

// validJob is a well-formed SYSTEM compaction of two whole files, the
// baseline every refusal test mutates one field of.
func validJob() *tabletserver.TExternalCompactionJob {
	job := tabletserver.NewTExternalCompactionJob()
	job.ExternalCompactionId = testECID
	job.Extent = &data.TKeyExtent{
		Table:      []byte("2"),
		PrevEndRow: []byte("c"),
		EndRow:     []byte("m"),
	}
	job.Files = []*tabletserver.InputFile{
		{MetadataFileEntry: storedFile("hdfs://nn/accumulo/tables/2/t-0001/F0001.rf"), Size: 1024, Entries: 10, Timestamp: 1700000000000},
		{MetadataFileEntry: storedFile("hdfs://nn/accumulo/tables/2/t-0001/F0002.rf"), Size: 2048, Entries: 20, Timestamp: 1700000000001},
	}
	job.OutputFile = "hdfs://nn/accumulo/tables/2/t-0001/C0003.rf"
	job.PropagateDeletes = true
	job.Kind = tabletserver.TCompactionKind_SYSTEM
	return job
}

func iterConfig(settings ...*tabletserver.TIteratorSetting) *tabletserver.IteratorConfig {
	return &tabletserver.IteratorConfig{Iterators: settings}
}

// mustTranslate fails the test if the job is refused.
func mustTranslate(t *testing.T, job *tabletserver.TExternalCompactionJob, opts Options) *Plan {
	t.Helper()
	plan, err := Translate(job, opts)
	if err != nil {
		t.Fatalf("Translate: %v", err)
	}
	return plan
}

// assertRefused checks that the job is refused with the expected
// coordinator-facing class, and that the detail names the offending
// field (an operator reading the manager log gets both).
func assertRefused(t *testing.T, job *tabletserver.TExternalCompactionJob, opts Options, wantClass, wantField string) *Refusal {
	t.Helper()
	plan, err := Translate(job, opts)
	if err == nil {
		t.Fatalf("Translate accepted the job (plan %+v), want refusal %s", plan, wantClass)
	}
	if plan != nil {
		t.Fatalf("Translate returned a plan alongside an error: %+v", plan)
	}
	refusal := RefusalOf(err)
	if refusal == nil {
		t.Fatalf("error %v is not a *Refusal; the compactor could not report a class", err)
	}
	if refusal.Class != wantClass {
		t.Fatalf("class = %q, want %q (detail: %s)", refusal.Class, wantClass, refusal.Error())
	}
	if wantField != "" && refusal.Field != wantField {
		t.Fatalf("field = %q, want %q (detail: %s)", refusal.Field, wantField, refusal.Error())
	}
	if !strings.Contains(refusal.Error(), refusal.Class) {
		t.Fatalf("Error() = %q, want it to carry the class", refusal.Error())
	}
	return refusal
}

// TestTranslateMapsEveryJobField is the fidelity test: a fully populated
// USER job must land in the plan field-for-field, including the
// propagateDeletes → FullMajorCompaction inversion that decides whether
// tombstones may be dropped.
func TestTranslateMapsEveryJobField(t *testing.T) {
	job := validJob()
	job.Kind = tabletserver.TCompactionKind_USER
	job.FateId = &manager.TFateId{
		Type:      manager.TFateInstanceType_USER,
		TxUUIDStr: "b7f1c0de-0000-4000-8000-00000000abcd",
	}
	job.PropagateDeletes = false
	job.Overrides = map[string]string{
		propCompressType:      "gz",
		propCompressBlockSize: "128K",
	}
	job.IteratorSettings = iterConfig(
		&tabletserver.TIteratorSetting{
			Priority:      20,
			Name:          "vers",
			IteratorClass: versClass,
			Properties:    map[string]string{"maxVersions": "3"},
		},
	)

	plan := mustTranslate(t, job, Options{})

	if plan.ECID != testECID {
		t.Errorf("ECID = %q, want %q", plan.ECID, testECID)
	}
	if plan.TableID != "2" {
		t.Errorf("TableID = %q, want 2", plan.TableID)
	}
	if plan.Extent != job.Extent {
		t.Errorf("Extent = %+v, want the job's extent verbatim", plan.Extent)
	}
	if plan.Kind != tabletserver.TCompactionKind_USER {
		t.Errorf("Kind = %v, want USER", plan.Kind)
	}
	if want := "USER:b7f1c0de-0000-4000-8000-00000000abcd"; plan.FateID != want {
		t.Errorf("FateID = %q, want %q", plan.FateID, want)
	}
	if plan.OutputFile != job.OutputFile {
		t.Errorf("OutputFile = %q, want %q", plan.OutputFile, job.OutputFile)
	}
	if plan.PropagateDeletes {
		t.Error("PropagateDeletes = true, want the job's false")
	}
	if !plan.FullMajorCompaction {
		t.Error("FullMajorCompaction = false; propagateDeletes=false means this output is the tablet's only file")
	}
	if plan.Scope != iterrt.ScopeMajc {
		t.Errorf("Scope = %v, want majc", plan.Scope)
	}
	if plan.Codec != block.CodecGzip {
		t.Errorf("Codec = %q, want %q", plan.Codec, block.CodecGzip)
	}
	if plan.BlockSize != 128*1024 {
		t.Errorf("BlockSize = %d, want %d", plan.BlockSize, 128*1024)
	}
	if plan.TotalInputBytes != 3072 || plan.TotalInputEntries != 30 {
		t.Errorf("totals = %d bytes / %d entries, want 3072 / 30",
			plan.TotalInputBytes, plan.TotalInputEntries)
	}

	if len(plan.Inputs) != 2 {
		t.Fatalf("Inputs = %d, want 2", len(plan.Inputs))
	}
	first := plan.Inputs[0]
	if first.Entry != job.Files[0].MetadataFileEntry {
		t.Errorf("Inputs[0].Entry = %q, want the metadata entry verbatim (the commit dereferences this exact string)", first.Entry)
	}
	if first.Path != "hdfs://nn/accumulo/tables/2/t-0001/F0001.rf" {
		t.Errorf("Inputs[0].Path = %q, want the decoded StoredTabletFile path", first.Path)
	}
	if first.Size != 1024 || first.Entries != 10 || first.Timestamp != 1700000000000 {
		t.Errorf("Inputs[0] DataFileValue = %+v, want size=1024 entries=10 ts=1700000000000", first)
	}

	if len(plan.Iterators) != 1 {
		t.Fatalf("Iterators = %d, want 1", len(plan.Iterators))
	}
	it := plan.Iterators[0]
	if it.Name != "vers" || it.Class != versClass || it.Priority != 20 {
		t.Errorf("Iterators[0] = %+v, want name=vers class=%s priority=20", it, versClass)
	}
	if it.Spec.Name != iterrt.IterVersioning {
		t.Errorf("Iterators[0].Spec.Name = %q, want %q", it.Spec.Name, iterrt.IterVersioning)
	}
	if it.Spec.Options["maxVersions"] != "3" {
		t.Errorf("Iterators[0] options = %v, want maxVersions=3", it.Spec.Options)
	}
	if len(plan.Stack) != 1 || plan.Stack[0].Name != iterrt.IterVersioning {
		t.Errorf("Stack = %+v, want one versioning entry", plan.Stack)
	}
}

// TestTranslateLegacyBarePathEntry covers the pre-3.0 metadata form: the
// entry is the path itself, which is whole-file by definition.
func TestTranslateLegacyBarePathEntry(t *testing.T) {
	job := validJob()
	job.Files = []*tabletserver.InputFile{
		{MetadataFileEntry: "hdfs://nn/accumulo/tables/2/t-0001/F0001.rf", Size: 1, Entries: 1},
	}

	plan := mustTranslate(t, job, Options{})
	if plan.Inputs[0].Path != "hdfs://nn/accumulo/tables/2/t-0001/F0001.rf" {
		t.Fatalf("Path = %q, want the bare entry", plan.Inputs[0].Path)
	}
}

// TestTranslateRefusesFencedInput is the ranged-input gate: a fenced
// StoredTabletFile covers only part of its RFile, and shoal's composer
// reads whole files, so merging one would pull in another tablet's cells.
func TestTranslateRefusesFencedInput(t *testing.T) {
	for _, tt := range []struct {
		name       string
		start, end string
	}{
		{"start only", "d", ""},
		{"end only", "", "k"},
		{"both bounds", "d", "k"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			job.Files[1].MetadataFileEntry = fencedFile("hdfs://nn/t/2/F0002.rf", tt.start, tt.end)

			r := assertRefused(t, job, Options{}, ClassRangedInputFile, "files[1]")
			if !strings.Contains(r.Detail, "fenced") {
				t.Fatalf("detail = %q, want it to name the fence", r.Detail)
			}
		})
	}
}

// TestTranslateRefusesMalformedJobs walks the structural checks. Every
// one of these would produce an uncommittable or cell-losing compaction,
// so each must come back as a malformed-job refusal naming its field.
func TestTranslateRefusesMalformedJobs(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*tabletserver.TExternalCompactionJob)
		wantField string
	}{
		{
			name:      "no ecid",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.ExternalCompactionId = "" },
			wantField: "externalCompactionId",
		},
		{
			name:      "ecid not in accumulo form",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.ExternalCompactionId = "compaction-7" },
			wantField: "externalCompactionId",
		},
		{
			name:      "ecid prefix with no uuid",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.ExternalCompactionId = ECIDPrefix },
			wantField: "externalCompactionId",
		},
		{
			name:      "no extent",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Extent = nil },
			wantField: "extent",
		},
		{
			name:      "extent without table id",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Extent.Table = nil },
			wantField: "extent.table",
		},
		{
			name: "inverted extent bounds",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.Extent.PrevEndRow = []byte("z")
				j.Extent.EndRow = []byte("m")
			},
			wantField: "extent",
		},
		{
			name:      "no input files",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Files = nil },
			wantField: "files",
		},
		{
			name:      "nil input file",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Files[1] = nil },
			wantField: "files[1]",
		},
		{
			name:      "empty input entry",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Files[1].MetadataFileEntry = "" },
			wantField: "files[1]",
		},
		{
			name:      "negative input size",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Files[1].Size = -1 },
			wantField: "files[1]",
		},
		{
			name:      "negative input entries",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Files[1].Entries = -5 },
			wantField: "files[1]",
		},
		{
			name: "duplicate input",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.Files[1].MetadataFileEntry = j.Files[0].MetadataFileEntry
			},
			wantField: "files[1]",
		},
		{
			name: "undecodable stored tablet file",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.Files[0].MetadataFileEntry = `{"path":`
			},
			wantField: "files[0]",
		},
		{
			name: "stored tablet file without a path",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.Files[0].MetadataFileEntry = `{"startRow":"","endRow":""}`
			},
			wantField: "files[0]",
		},
		{
			name:      "no output file",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.OutputFile = "" },
			wantField: "outputFile",
		},
		{
			name: "output file is a metadata entry",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.OutputFile = storedFile("hdfs://nn/t/2/C0003.rf")
			},
			wantField: "outputFile",
		},
		{
			name:      "output file is not an rfile",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.OutputFile = "hdfs://nn/t/2/C0003.tmp" },
			wantField: "outputFile",
		},
		{
			name: "output file aliases an input",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.OutputFile = "hdfs://nn/accumulo/tables/2/t-0001/F0001.rf"
			},
			wantField: "outputFile",
		},
		{
			name:      "unknown compaction kind",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Kind = tabletserver.TCompactionKind(9) },
			wantField: "kind",
		},
		{
			name:      "user compaction without a fate id",
			mutate:    func(j *tabletserver.TExternalCompactionJob) { j.Kind = tabletserver.TCompactionKind_USER },
			wantField: "fateId",
		},
		{
			name: "fate id without a transaction uuid",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.Kind = tabletserver.TCompactionKind_USER
				j.FateId = &manager.TFateId{Type: manager.TFateInstanceType_USER}
			},
			wantField: "fateId",
		},
		{
			name: "nil iterator setting",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.IteratorSettings = iterConfig(nil)
			},
			wantField: "iteratorSettings[0]",
		},
		{
			name: "iterator without a name",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{IteratorClass: versClass})
			},
			wantField: "iteratorSettings[0]",
		},
		{
			name: "iterator without a class",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{Name: "vers"})
			},
			wantField: "iteratorSettings.vers",
		},
		{
			name: "duplicate iterator name",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.IteratorSettings = iterConfig(
					&tabletserver.TIteratorSetting{Priority: 10, Name: "vers", IteratorClass: versClass},
					&tabletserver.TIteratorSetting{Priority: 20, Name: "vers", IteratorClass: versClass},
				)
			},
			wantField: "iteratorSettings.vers",
		},
		{
			name: "priorities out of order",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.IteratorSettings = iterConfig(
					&tabletserver.TIteratorSetting{Priority: 30, Name: "late", IteratorClass: versClass},
					&tabletserver.TIteratorSetting{Priority: 10, Name: "early", IteratorClass: latentClass},
				)
			},
			wantField: "iteratorSettings.early",
		},
		{
			name: "non-numeric block size override",
			mutate: func(j *tabletserver.TExternalCompactionJob) {
				j.Overrides = map[string]string{propCompressBlockSize: "big"}
			},
			wantField: "overrides[" + propCompressBlockSize + "]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			tt.mutate(job)
			assertRefused(t, job, Options{}, ClassMalformedJob, tt.wantField)
		})
	}
}

func TestTranslateRefusesNilJob(t *testing.T) {
	assertRefused(t, nil, Options{}, ClassMalformedJob, "")
}

// TestTranslateRefusesUnportedIterator: an unknown class must fail the
// whole job. Dropping it (which is the right call for the shadow
// resolver, where the output is discarded) would here write an output
// missing that iterator's effect and then replace the inputs with it.
func TestTranslateRefusesUnportedIterator(t *testing.T) {
	job := validJob()
	job.IteratorSettings = iterConfig(
		&tabletserver.TIteratorSetting{
			Priority:      15,
			Name:          "ageoff",
			IteratorClass: "org.apache.accumulo.core.iterators.user.AgeOffFilter",
			Properties:    map[string]string{"ttl": "3600000"},
		},
	)

	r := assertRefused(t, job, Options{}, ClassUnsupportedIterator, "iteratorSettings.ageoff")
	if !strings.Contains(r.Detail, "AgeOffFilter") {
		t.Fatalf("detail = %q, want the unported class named", r.Detail)
	}
}

// TestTranslateRefusesSystemIterator: shoal's composer installs the
// deleting iterator itself, so a job that also lists one would suppress
// tombstones twice.
func TestTranslateRefusesSystemIterator(t *testing.T) {
	for _, tt := range []struct{ name, class string }{
		{"deleting", "org.apache.accumulo.core.iterators.system.DeletingIterator"},
		{"visibility", "org.apache.accumulo.core.iterators.system.VisibilityFilter"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			job.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{
				Priority: 5, Name: "sys", IteratorClass: tt.class,
			})
			assertRefused(t, job, Options{}, ClassUnsupportedIterator, "iteratorSettings.sys")
		})
	}
}

// TestTranslateRefusesUnusableIteratorOptions proves the dry-run stack
// build is wired: a bad option is caught at translation time, not after
// the compaction has read gigabytes of input.
func TestTranslateRefusesUnusableIteratorOptions(t *testing.T) {
	for _, tt := range []struct{ name, value string }{
		{"not an integer", "many"},
		{"below the legal minimum", "0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			job.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{
				Priority:      20,
				Name:          "vers",
				IteratorClass: versClass,
				Properties:    map[string]string{"maxVersions": tt.value},
			})
			assertRefused(t, job, Options{}, ClassUnsupportedIterator, "iteratorSettings")
		})
	}
}

// TestTranslateCopiesIteratorOptions guards against aliasing the thrift
// struct: the plan must not change when the job it came from is reused
// or recycled by the transport.
func TestTranslateCopiesIteratorOptions(t *testing.T) {
	job := validJob()
	props := map[string]string{"maxVersions": "3"}
	job.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{
		Priority: 20, Name: "vers", IteratorClass: versClass, Properties: props,
	})

	plan := mustTranslate(t, job, Options{})
	props["maxVersions"] = "99"

	if got := plan.Stack[0].Options["maxVersions"]; got != "3" {
		t.Fatalf("plan option = %q after mutating the job, want the translated 3", got)
	}
}

// TestTranslateAcceptsEmptyIteratorStack: no majc iterators configured is
// an identity compaction, not an error.
func TestTranslateAcceptsEmptyIteratorStack(t *testing.T) {
	for _, tt := range []struct {
		name     string
		settings *tabletserver.IteratorConfig
	}{
		{"unset", nil},
		{"empty list", iterConfig()},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			job.IteratorSettings = tt.settings
			plan := mustTranslate(t, job, Options{})
			if len(plan.Stack) != 0 {
				t.Fatalf("Stack = %+v, want empty", plan.Stack)
			}
		})
	}
}

// TestTranslateOverridesResolveOutputEncoding pins the property mapping
// shoal honors, including Accumulo's memory-suffix syntax.
func TestTranslateOverridesResolveOutputEncoding(t *testing.T) {
	tests := []struct {
		name          string
		overrides     map[string]string
		opts          Options
		wantCodec     string
		wantBlockSize int
	}{
		{
			name:      "no overrides falls back to the caller's table defaults",
			opts:      Options{DefaultCodec: block.CodecSnappy, DefaultBlockSize: 64 * 1024},
			wantCodec: block.CodecSnappy, wantBlockSize: 64 * 1024,
		},
		{
			name:      "no overrides and no defaults defers to the composer",
			wantCodec: "", wantBlockSize: 0,
		},
		{
			name:      "codec none",
			overrides: map[string]string{propCompressType: "none"},
			wantCodec: block.CodecNone,
		},
		{
			name:      "codec gz",
			overrides: map[string]string{propCompressType: "gz"},
			wantCodec: block.CodecGzip,
		},
		{
			name:      "codec gzip alias",
			overrides: map[string]string{propCompressType: "GZIP"},
			wantCodec: block.CodecGzip,
		},
		{
			name:      "codec snappy overrides the table default",
			overrides: map[string]string{propCompressType: "snappy"},
			opts:      Options{DefaultCodec: block.CodecNone},
			wantCodec: block.CodecSnappy,
		},
		{
			name:          "block size in bytes",
			overrides:     map[string]string{propCompressBlockSize: "100000"},
			wantBlockSize: 100000,
		},
		{
			name:          "block size with a byte suffix",
			overrides:     map[string]string{propCompressBlockSize: "4096B"},
			wantBlockSize: 4096,
		},
		{
			name:          "block size in kibibytes",
			overrides:     map[string]string{propCompressBlockSize: "100K"},
			wantBlockSize: 100 * 1024,
		},
		{
			name:          "block size in mebibytes",
			overrides:     map[string]string{propCompressBlockSize: "2m"},
			wantBlockSize: 2 * 1024 * 1024,
		},
		{
			name: "filesystem placement overrides are ignored",
			overrides: map[string]string{
				"table.file.blocksize":   "256M",
				"table.file.replication": "3",
			},
			opts:      Options{DefaultCodec: block.CodecSnappy},
			wantCodec: block.CodecSnappy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			job.Overrides = tt.overrides
			plan := mustTranslate(t, job, tt.opts)
			if plan.Codec != tt.wantCodec {
				t.Errorf("Codec = %q, want %q", plan.Codec, tt.wantCodec)
			}
			if plan.BlockSize != tt.wantBlockSize {
				t.Errorf("BlockSize = %d, want %d", plan.BlockSize, tt.wantBlockSize)
			}
		})
	}
}

// TestTranslateRefusesUnsupportedOverrides covers the capability gate on
// table properties: anything shoal cannot reproduce in the output file
// sends the job back to a Java compactor.
func TestTranslateRefusesUnsupportedOverrides(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		wantClass string
		wantField string
	}{
		{
			name:      "codec shoal cannot write",
			overrides: map[string]string{propCompressType: "zstd"},
			wantClass: ClassUnsupportedProperty,
			wantField: "overrides[" + propCompressType + "]",
		},
		{
			name:      "block size of zero",
			overrides: map[string]string{propCompressBlockSize: "0"},
			wantClass: ClassUnsupportedProperty,
			wantField: "overrides[" + propCompressBlockSize + "]",
		},
		{
			name:      "absurd block size",
			overrides: map[string]string{propCompressBlockSize: "8G"},
			wantClass: ClassUnsupportedProperty,
			wantField: "overrides[" + propCompressBlockSize + "]",
		},
		{
			name:      "table crypto service",
			overrides: map[string]string{"table.crypto.opts.key": "kms://k1"},
			wantClass: ClassUnsupportedCrypto,
			wantField: "overrides[table.crypto.opts.key]",
		},
		{
			name:      "instance crypto service",
			overrides: map[string]string{"instance.crypto.service": "o.a.a.core.crypto.AESCryptoService"},
			wantClass: ClassUnsupportedCrypto,
			wantField: "overrides[instance.crypto.service]",
		},
		{
			name:      "unknown property",
			overrides: map[string]string{"table.sampler": "o.a.a.core.sample.RowSampler"},
			wantClass: ClassUnsupportedProperty,
			wantField: "overrides[table.sampler]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := validJob()
			job.Overrides = tt.overrides
			assertRefused(t, job, Options{}, tt.wantClass, tt.wantField)
		})
	}
}

// TestTranslateReportsTheSameOverrideRefusalEveryTime: map iteration
// order must not leak into the class an operator sees for a job with
// several unsupported overrides.
func TestTranslateReportsTheSameOverrideRefusalEveryTime(t *testing.T) {
	job := validJob()
	job.Overrides = map[string]string{
		"table.crypto.opts.key": "kms://k1",
		"table.sampler":         "o.a.a.core.sample.RowSampler",
		propCompressType:        "zstd",
	}

	first := assertRefused(t, job, Options{}, ClassUnsupportedCrypto, "overrides[table.crypto.opts.key]")
	for i := 0; i < 50; i++ {
		_, err := Translate(job, Options{})
		got := RefusalOf(err)
		if got == nil || got.Field != first.Field || got.Class != first.Class {
			t.Fatalf("iteration %d refused with %v, want a stable %s/%s", i, err, first.Class, first.Field)
		}
	}
}

// TestTranslateEnforcesInputLimits: the composer holds whole RFile images
// in memory, so an oversized job must be refused before it can OOM the
// process and take every other compaction on it down.
func TestTranslateEnforcesInputLimits(t *testing.T) {
	t.Run("too many files", func(t *testing.T) {
		job := validJob()
		r := assertRefused(t, job, Options{Limits: Limits{MaxInputFiles: 1}}, ClassResourceLimitExceeded, "files")
		if !strings.Contains(r.Detail, "limit of 1") {
			t.Fatalf("detail = %q, want the configured limit", r.Detail)
		}
	})

	t.Run("too many bytes", func(t *testing.T) {
		job := validJob()
		r := assertRefused(t, job, Options{Limits: Limits{MaxTotalInputBytes: 3071}}, ClassResourceLimitExceeded, "files")
		if !strings.Contains(r.Detail, "3072") {
			t.Fatalf("detail = %q, want the measured total", r.Detail)
		}
	})

	t.Run("exactly at the limit is accepted", func(t *testing.T) {
		job := validJob()
		mustTranslate(t, job, Options{Limits: Limits{MaxInputFiles: 2, MaxTotalInputBytes: 3072}})
	})

	t.Run("zero means unlimited", func(t *testing.T) {
		job := validJob()
		job.Files[0].Size = 1 << 62
		mustTranslate(t, job, Options{Limits: Limits{}})
	})

	t.Run("defaults admit an ordinary job", func(t *testing.T) {
		job := validJob()
		mustTranslate(t, job, Options{Limits: DefaultLimits()})
	})
}

// TestTranslateChecksStructureBeforeCapability: a job that is both
// malformed and unsupported must report the malformed field, because
// that is the one an operator can act on.
func TestTranslateChecksStructureBeforeCapability(t *testing.T) {
	job := validJob()
	job.Files = nil
	job.Overrides = map[string]string{propCompressType: "zstd"}
	job.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{
		Priority: 1, Name: "ageoff", IteratorClass: "org.apache.accumulo.core.iterators.user.AgeOffFilter",
	})

	assertRefused(t, job, Options{}, ClassMalformedJob, "files")
}

// TestPlanIsExecutable closes the loop: the translated plan, fed real
// input images, drives internal/compaction to the output the job asked
// for — versioning applied, tombstone dropped because propagateDeletes
// was false, and the requested codec on the output blocks.
func TestPlanIsExecutable(t *testing.T) {
	fileA := buildRFile(t, []testCell{
		{key: mkKey("row1", "cf", "cq", 10), value: "old"},
		{key: mkKey("row2", "cf", "cq", 10), value: "keep"},
	})
	fileB := buildRFile(t, []testCell{
		{key: mkKey("row1", "cf", "cq", 20), value: "new"},
		{key: deleteKey("row3", "cf", "cq", 30), value: ""},
	})

	job := validJob()
	job.PropagateDeletes = false
	job.Overrides = map[string]string{propCompressType: "gz"}
	job.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{
		Priority:      20,
		Name:          "vers",
		IteratorClass: versClass,
		Properties:    map[string]string{"maxVersions": "1"},
	})

	plan := mustTranslate(t, job, Options{})
	spec := plan.Spec([]compaction.Input{
		{Name: plan.Inputs[0].Path, Bytes: fileA},
		{Name: plan.Inputs[1].Path, Bytes: fileB},
	})
	if spec.Codec != block.CodecGzip || !spec.FullMajorCompaction || spec.Scope != iterrt.ScopeMajc {
		t.Fatalf("spec = %+v, want gz / full-major / majc", spec)
	}

	result, err := compaction.Compact(spec)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	got := drainRFile(t, result.Output)
	want := []string{"row1/cf:cq@20=new", "row2/cf:cq@10=keep"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("output cells = %v, want %v", got, want)
	}
	if result.EntriesWritten != 2 {
		t.Fatalf("EntriesWritten = %d, want 2", result.EntriesWritten)
	}
}

// TestPlanLogValueCarriesTheJobIdentity: the refusal/acceptance log line
// is the only operator-visible record of what shoal decided, so the
// fields an operator correlates on must be present.
func TestPlanLogValueCarriesTheJobIdentity(t *testing.T) {
	job := validJob()
	job.Kind = tabletserver.TCompactionKind_USER
	job.FateId = &manager.TFateId{
		Type:      manager.TFateInstanceType_USER,
		TxUUIDStr: "b7f1c0de-0000-4000-8000-00000000abcd",
	}
	job.IteratorSettings = iterConfig(&tabletserver.TIteratorSetting{
		Priority: 20, Name: "vers", IteratorClass: versClass,
	})

	plan := mustTranslate(t, job, Options{})
	rendered := plan.LogValue().String()
	want := []string{
		testECID,
		"table=2",
		"hdfs://nn/accumulo/tables/2/t-0001/C0003.rf",
		"USER",
		"b7f1c0de-0000-4000-8000-00000000abcd",
		"vers=" + iterrt.IterVersioning,
	}
	for _, want := range want {
		if !strings.Contains(rendered, want) {
			t.Fatalf("log value %q missing %q", rendered, want)
		}
	}
}

func TestExtentStringHandlesPartialExtents(t *testing.T) {
	if got := ExtentString(nil); got != "<no-extent>" {
		t.Fatalf("nil extent = %q", got)
	}
	got := ExtentString(&data.TKeyExtent{Table: []byte("2")})
	if !strings.Contains(got, "prev=-inf") || !strings.Contains(got, "end=+inf") {
		t.Fatalf("open extent = %q, want infinite bounds", got)
	}
}

func TestRefusalOfIgnoresOtherErrors(t *testing.T) {
	if got := RefusalOf(errors.New("boom")); got != nil {
		t.Fatalf("RefusalOf(non-refusal) = %+v, want nil", got)
	}
	if got := RefusalOf(nil); got != nil {
		t.Fatalf("RefusalOf(nil) = %+v, want nil", got)
	}
	wrapped := fmt.Errorf("context: %w", refuse(ClassMalformedJob, "files", "boom"))
	if got := RefusalOf(wrapped); got == nil || got.Class != ClassMalformedJob {
		t.Fatalf("RefusalOf(wrapped) = %+v, want the wrapped refusal", got)
	}
}

func TestParseMemoryBytes(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{in: "1024", want: 1024},
		{in: " 2048 ", want: 2048},
		{in: "512b", want: 512},
		{in: "1K", want: 1024},
		{in: "1k", want: 1024},
		{in: "1M", want: 1024 * 1024},
		{in: "1G", want: 1024 * 1024 * 1024},
		{in: "-1", want: -1},
		{in: "", wantErr: true},
		{in: "K", wantErr: true},
		{in: "1.5M", wantErr: true},
		{in: "9223372036854775807G", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseMemoryBytes(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseMemoryBytes(%q) = %d, want error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMemoryBytes(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parseMemoryBytes(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// --- RFile helpers -------------------------------------------------

type testCell struct {
	key   *wire.Key
	value string
}

func mkKey(row, cf, cq string, ts int64) *wire.Key {
	return &wire.Key{
		Row:             []byte(row),
		ColumnFamily:    []byte(cf),
		ColumnQualifier: []byte(cq),
		Timestamp:       ts,
	}
}

func deleteKey(row, cf, cq string, ts int64) *wire.Key {
	k := mkKey(row, cf, cq, ts)
	k.Deleted = true
	return k
}

func buildRFile(t *testing.T, cells []testCell) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := rfile.NewWriter(&buf, rfile.WriterOptions{})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for i, c := range cells {
		if err := w.Append(c.key, []byte(c.value)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// drainRFile renders every cell of an RFile image as
// "row/cf:cq@ts=value" so assertions read like the shell's scan output.
func drainRFile(t *testing.T, image []byte) []string {
	t.Helper()
	bc, err := bcfile.NewReader(bytes.NewReader(image), int64(len(image)))
	if err != nil {
		t.Fatalf("bcfile.NewReader: %v", err)
	}
	r, err := rfile.Open(bc, block.Default())
	if err != nil {
		t.Fatalf("rfile.Open: %v", err)
	}
	defer r.Close()

	var out []string
	for {
		k, v, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, fmt.Sprintf("%s/%s:%s@%d=%s",
			k.Row, k.ColumnFamily, k.ColumnQualifier, k.Timestamp, v))
	}
}
