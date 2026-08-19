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

// Package compactjob translates a CompactionCoordinator assignment
// (tabletserver.TExternalCompactionJob) into the compaction shoal would
// run, or refuses it with a structured, operator-legible reason.
//
// It is the layer between cmd/shoal-compactor's poll loop (which speaks
// Thrift to the manager) and internal/compaction (the pure "read N
// RFiles → apply the iterator stack → write one RFile" composer). It
// performs no I/O: Translate is a pure function of the job plus the
// caller's limits, so every field mapping and every refusal is unit
// testable without a coordinator.
//
// # Why refusal is a first-class result
//
// A compaction that runs with a *slightly* different stack than Java
// would have used silently corrupts a tablet: the output replaces its
// inputs, so a dropped filter or an unhonored fence is not recoverable
// from the surviving files. The safe failure mode is therefore always
// "refuse the job, let the coordinator reschedule it onto a Java
// compactor" — Accumulo treats a failed external compaction as a normal,
// retryable event. Every construct shoal cannot reproduce cell-for-cell
// is refused here, before a single input byte is read, and the reason
// travels back to the manager as the compactionFailed exception-class
// name (see the Class* constants).
//
// # What Translate deliberately cannot see
//
// The job carries only the table properties the compaction *overrides*
// (job.overrides, populated by a CompactionConfigurer). The table's own
// configuration — locality groups, samplers, summarizers, bloom filters,
// crypto — is not in the job; a compactor reads it from ZooKeeper.
// shoal-compactor does not read it yet, so Translate gates only what the
// job itself declares. Wiring the table config in (and refusing tables
// whose output shoal cannot reproduce, e.g. multi-locality-group or
// sampled tables) is a precondition for actually executing a job, and is
// tracked on the umbrella issue rather than assumed here.
package compactjob

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/phrocker/shoal/internal/compaction"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/rfile/bcfile/block"
	"github.com/phrocker/shoal/internal/shadow/itercfg"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/manager"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
)

// ECIDPrefix is the prefix Accumulo's ExternalCompactionId carries
// ("ECID:" + UUID, see core/.../metadata/schema/ExternalCompactionId.java).
// Jobs whose id is not in this form did not come from a coordinator this
// compactor can talk to.
const ECIDPrefix = "ECID:"

// Sentinel exception-class names reported to the coordinator through
// compactionFailed(..., exceptionClassName, ...). Java only logs and
// stores this string, so the vocabulary is ours; keeping it stable and
// specific is what lets an operator tell "shoal cannot do this yet" from
// "this job is broken" in the manager log without correlating compactor
// logs by ecid.
const (
	// ClassMalformedJob: the assignment itself is not internally
	// consistent (missing extent, no input files, duplicate inputs,
	// output colliding with an input, ...). Nothing about shoal's
	// capabilities is implied — a Java compactor would reject it too.
	ClassMalformedJob = "org.apache.accumulo.shoal.MalformedCompactionJob"

	// ClassRangedInputFile: an input is a *fenced* StoredTabletFile
	// (non-empty startRow/endRow in the metadata entry). Reading it whole
	// would pull in cells that belong to another tablet, so the job is
	// refused until ranged reads land.
	ClassRangedInputFile = "org.apache.accumulo.shoal.RangedInputFileUnsupported"

	// ClassUnsupportedIterator: the job's iterator stack references a
	// class shoal has not ported, a system iterator shoal composes
	// itself, or options the ported iterator rejects.
	ClassUnsupportedIterator = "org.apache.accumulo.shoal.UnsupportedIterator"

	// ClassUnsupportedProperty: job.overrides carries a table property
	// shoal cannot honor when writing the output RFile.
	ClassUnsupportedProperty = "org.apache.accumulo.shoal.UnsupportedTableProperty"

	// ClassUnsupportedCrypto: job.overrides configures on-disk
	// encryption. shoal's RFile writer emits cleartext blocks, so an
	// encrypted table's output must come from Java.
	ClassUnsupportedCrypto = "org.apache.accumulo.shoal.UnsupportedCrypto"

	// ClassResourceLimitExceeded: the job is executable in principle but
	// exceeds this compactor's configured input budget. The composer
	// holds whole RFile images in memory, so an unbounded job would OOM
	// the process (and take every other compaction on it down).
	ClassResourceLimitExceeded = "org.apache.accumulo.shoal.ResourceLimitExceeded"

	// ClassCommitUnavailable: the job translated cleanly and shoal could
	// execute it, but the manager-authoritative commit RPC this pool
	// requires does not exist yet, so the compaction is handed back
	// rather than executed and thrown away. Reported by the caller after
	// a successful Translate — see cmd/shoal-compactor.
	ClassCommitUnavailable = "org.apache.accumulo.shoal.CommitNotImplemented"

	// ClassShuttingDown: the compactor was asked to stop while it held
	// this assignment. Reported so the coordinator can reschedule
	// immediately instead of waiting for the assignment to age out.
	ClassShuttingDown = "org.apache.accumulo.shoal.CompactorShuttingDown"
)

// Refusal is the structured reason a job cannot be executed by shoal.
// It is the error type every Translate failure carries; callers use
// RefusalOf to recover the coordinator-facing Class.
type Refusal struct {
	// Class is the sentinel exception-class name reported to the
	// coordinator (one of the Class* constants).
	Class string
	// Field names the job field that caused the refusal ("files[2]",
	// "iteratorSettings.vers", "overrides[table.file.compress.type]"),
	// or "" when the whole job is at fault.
	Field string
	// Detail is the human-readable explanation.
	Detail string
}

func (r *Refusal) Error() string {
	if r.Field == "" {
		return fmt.Sprintf("compactjob: %s: %s", r.Class, r.Detail)
	}
	return fmt.Sprintf("compactjob: %s: %s: %s", r.Class, r.Field, r.Detail)
}

// RefusalOf returns the structured refusal wrapped in err, or nil when
// err is not a refusal. Every error Translate returns is a refusal.
func RefusalOf(err error) *Refusal {
	var r *Refusal
	if errors.As(err, &r) {
		return r
	}
	return nil
}

func refuse(class, field, format string, args ...any) *Refusal {
	return &Refusal{Class: class, Field: field, Detail: fmt.Sprintf(format, args...)}
}

// Default input budget for a single compaction. The composer reads every
// input RFile fully into memory and buffers the output image, so peak RSS
// is roughly (sum of input sizes + output size). These defaults keep one
// job inside a few GiB; operators raise them for compactor pools with
// more headroom, or lower them to push big merges at Java.
const (
	DefaultMaxInputFiles      = 64
	DefaultMaxTotalInputBytes = int64(2) << 30 // 2 GiB
)

// Limits bounds what a single job may consume. A zero field means
// "unlimited" so a caller can opt out explicitly; use DefaultLimits for
// the safe defaults.
type Limits struct {
	// MaxInputFiles caps len(job.files).
	MaxInputFiles int
	// MaxTotalInputBytes caps the summed size of the input files.
	MaxTotalInputBytes int64
}

// DefaultLimits returns the standard per-job input budget.
func DefaultLimits() Limits {
	return Limits{
		MaxInputFiles:      DefaultMaxInputFiles,
		MaxTotalInputBytes: DefaultMaxTotalInputBytes,
	}
}

// Options tunes translation.
type Options struct {
	// DefaultCodec is the output block codec to use when the job carries
	// no table.file.compress.type override. Empty defers to the
	// compaction composer default. A caller that has read the table's
	// configuration should pass the table's codec here so the output
	// matches what Java would have written.
	DefaultCodec string

	// DefaultBlockSize is the output data-block threshold used when the
	// job carries no table.file.compress.blocksize override. Zero defers
	// to the RFile writer default.
	DefaultBlockSize int

	// Limits bounds the job's inputs. The zero value is unlimited; pass
	// DefaultLimits() for the standard budget.
	Limits Limits
}

// InputFile is one translated entry of job.files.
type InputFile struct {
	// Entry is the raw metadata file entry exactly as the coordinator
	// sent it (StoredTabletFile JSON in Accumulo 3+, a bare path in the
	// legacy form). Preserved byte-for-byte because the eventual commit
	// must dereference this exact string.
	Entry string
	// Path is the file's URI, decoded out of Entry.
	Path string
	// Size is the file's byte size from its DataFileValue.
	Size int64
	// Entries is the file's cell count from its DataFileValue.
	Entries int64
	// Timestamp is the DataFileValue time field (-1 when absent).
	Timestamp int64
}

// Iterator is one translated entry of job.iteratorSettings.
type Iterator struct {
	// Name is the operator's nickname for the stack entry.
	Name string
	// Class is the fully-qualified Java class the coordinator sent.
	Class string
	// Priority is the entry's stack priority (low runs closer to the
	// source).
	Priority int32
	// Spec is the shoal iterator the class maps to, with the job's
	// options attached.
	Spec iterrt.IterSpec
}

// Plan is a fully translated, executable description of one coordinator
// job: everything internal/compaction needs except the input bytes.
type Plan struct {
	// ECID is the external compaction id the coordinator assigned.
	ECID string
	// TableID is the extent's table id.
	TableID string
	// Extent is the tablet being compacted, as sent.
	Extent *data.TKeyExtent
	// Kind is SYSTEM (the coordinator's own planning) or USER (a
	// client-requested compaction driven by a FATE transaction).
	Kind tabletserver.TCompactionKind
	// FateID renders job.fateId ("<INSTANCE_TYPE>:<uuid>") for USER
	// compactions; empty for SYSTEM jobs that carry none.
	FateID string
	// Inputs are the files to merge, in the coordinator's order.
	Inputs []InputFile
	// OutputFile is the path the compactor must write.
	OutputFile string
	// PropagateDeletes is the job flag as sent: false means this output
	// becomes the tablet's only file, so tombstones may be dropped.
	PropagateDeletes bool
	// TotalInputBytes / TotalInputEntries are the summed DataFileValue
	// figures across Inputs.
	TotalInputBytes   int64
	TotalInputEntries int64
	// Iterators is the translated stack with its Java provenance intact
	// (for logging and for the eventual shadow comparison).
	Iterators []Iterator
	// Stack is the iterator chain handed to iterrt.BuildStack.
	Stack []iterrt.IterSpec
	// Scope is always iterrt.ScopeMajc — an external compaction is a
	// major compaction by construction.
	Scope iterrt.IteratorScope
	// FullMajorCompaction is !PropagateDeletes, mirroring Java's
	// FileCompactor.CompactionIteratorEnvironment.isFullMajorCompaction.
	FullMajorCompaction bool
	// Codec is the resolved output block codec.
	Codec string
	// BlockSize is the resolved output data-block threshold (0 = writer
	// default).
	BlockSize int
}

// Spec assembles the compaction.Spec for this plan. inputs must be the
// RFile images for Plan.Inputs; the caller fetches them (translation is
// I/O-free by design).
func (p *Plan) Spec(inputs []compaction.Input) compaction.Spec {
	return compaction.Spec{
		Inputs:              inputs,
		Stack:               p.Stack,
		Scope:               p.Scope,
		FullMajorCompaction: p.FullMajorCompaction,
		Codec:               p.Codec,
		BlockSize:           p.BlockSize,
	}
}

// LogValue renders the plan for slog without dumping every file path at
// info level.
func (p *Plan) LogValue() slog.Value {
	names := make([]string, 0, len(p.Iterators))
	for _, it := range p.Iterators {
		names = append(names, it.Name+"="+it.Spec.Name)
	}
	return slog.GroupValue(
		slog.String("ecid", p.ECID),
		slog.String("table", p.TableID),
		slog.String("extent", ExtentString(p.Extent)),
		slog.String("kind", p.Kind.String()),
		slog.String("fate_id", p.FateID),
		slog.Int("inputs", len(p.Inputs)),
		slog.Int64("input_bytes", p.TotalInputBytes),
		slog.Int64("input_entries", p.TotalInputEntries),
		slog.String("output", p.OutputFile),
		slog.Bool("propagate_deletes", p.PropagateDeletes),
		slog.Bool("full_major", p.FullMajorCompaction),
		slog.String("codec", p.Codec),
		slog.Int("block_size", p.BlockSize),
		slog.Any("stack", names),
	)
}

// ExtentString renders a tablet extent for logs. Defensive against a
// nil/partial extent so logging can never panic on a malformed job.
func ExtentString(ex *data.TKeyExtent) string {
	if ex == nil {
		return "<no-extent>"
	}
	end := "+inf"
	if r := ex.GetEndRow(); r != nil {
		end = fmt.Sprintf("%q", r)
	}
	prev := "-inf"
	if r := ex.GetPrevEndRow(); r != nil {
		prev = fmt.Sprintf("%q", r)
	}
	return fmt.Sprintf("table=%s prev=%s end=%s", ex.GetTable(), prev, end)
}

// Translate converts a coordinator assignment into an executable Plan,
// or returns a *Refusal describing why shoal must hand the job back.
//
// Field mapping (Java reference: server/compactor/.../Compactor.java and
// core/.../compaction/thrift/TExternalCompactionJob):
//
//	externalCompactionId → Plan.ECID              (format-checked)
//	extent               → Plan.Extent, TableID   (must be populated)
//	files                → Plan.Inputs            (fences refused)
//	iteratorSettings     → Plan.Stack             (allowlisted classes)
//	outputFile           → Plan.OutputFile        (must not alias an input)
//	propagateDeletes     → FullMajorCompaction    (inverted, ScopeMajc)
//	kind                 → Plan.Kind              (USER requires fateId)
//	fateId               → Plan.FateID
//	overrides            → Codec, BlockSize       (allowlisted properties)
//
// Checking happens in two complete passes, and the split is load-bearing
// for the class an operator sees:
//
//	pass 1  every structural check, over every field: is this assignment
//	        internally consistent? Failures are ClassMalformedJob and say
//	        nothing about shoal.
//	pass 2  every capability check: could *shoal* reproduce this exact
//	        compaction? Failures name the missing capability.
//
// A job that is both malformed and unsupported is therefore always
// reported as malformed, no matter which field each problem is in —
// a job nobody can run is more actionable than a job only Java can run.
// Within pass 2 the order is inputs, then output encoding, then
// iterators, so a job with several gaps always reports the same one.
func Translate(job *tabletserver.TExternalCompactionJob, opts Options) (*Plan, error) {
	if job == nil {
		return nil, refuse(ClassMalformedJob, "", "nil job")
	}

	// --- pass 1: structure ------------------------------------------

	ecid := job.GetExternalCompactionId()
	if ecid == "" {
		return nil, refuse(ClassMalformedJob, "externalCompactionId", "missing")
	}
	if !strings.HasPrefix(ecid, ECIDPrefix) || len(ecid) == len(ECIDPrefix) {
		return nil, refuse(ClassMalformedJob, "externalCompactionId",
			"%q is not in Accumulo's %s<uuid> form", ecid, ECIDPrefix)
	}

	extent, tableID, err := translateExtent(job)
	if err != nil {
		return nil, err
	}

	kind, fateID, err := translateKind(job)
	if err != nil {
		return nil, err
	}

	inputs, totalBytes, totalEntries, err := parseInputs(job)
	if err != nil {
		return nil, err
	}

	outputFile, err := translateOutput(job, inputs)
	if err != nil {
		return nil, err
	}

	overrideKeys, blockSizeOverride, err := parseOverrides(job.GetOverrides())
	if err != nil {
		return nil, err
	}

	rawIters, err := parseIterators(job)
	if err != nil {
		return nil, err
	}

	// --- pass 2: capability -----------------------------------------

	if err := checkInputCapability(inputs, totalBytes, opts.Limits); err != nil {
		return nil, err
	}

	codec, blockSize, err := resolveOutputEncoding(job.GetOverrides(), overrideKeys, blockSizeOverride, opts)
	if err != nil {
		return nil, err
	}

	fullMajor := !job.GetPropagateDeletes()
	iters, err := mapIterators(rawIters, fullMajor)
	if err != nil {
		return nil, err
	}

	stack := make([]iterrt.IterSpec, 0, len(iters))
	for _, it := range iters {
		stack = append(stack, it.Spec)
	}

	return &Plan{
		ECID:                ecid,
		TableID:             tableID,
		Extent:              extent,
		Kind:                kind,
		FateID:              fateID,
		Inputs:              inputFiles(inputs),
		OutputFile:          outputFile,
		PropagateDeletes:    job.GetPropagateDeletes(),
		TotalInputBytes:     totalBytes,
		TotalInputEntries:   totalEntries,
		Iterators:           iters,
		Stack:               stack,
		Scope:               iterrt.ScopeMajc,
		FullMajorCompaction: fullMajor,
		Codec:               codec,
		BlockSize:           blockSize,
	}, nil
}

// translateExtent validates the tablet the job names. A compaction whose
// extent is unusable cannot be committed later, so it is refused before
// any work happens.
func translateExtent(job *tabletserver.TExternalCompactionJob) (*data.TKeyExtent, string, error) {
	if !job.IsSetExtent() || job.GetExtent() == nil {
		return nil, "", refuse(ClassMalformedJob, "extent", "missing")
	}
	ex := job.GetExtent()
	if len(ex.GetTable()) == 0 {
		return nil, "", refuse(ClassMalformedJob, "extent.table", "missing table id")
	}
	// prevEndRow is exclusive and endRow inclusive, so prev must sort
	// strictly before end whenever both bounds are finite.
	if prev, end := ex.GetPrevEndRow(), ex.GetEndRow(); prev != nil && end != nil &&
		bytes.Compare(prev, end) >= 0 {
		return nil, "", refuse(ClassMalformedJob, "extent",
			"prevEndRow %q is not before endRow %q", prev, end)
	}
	return ex, string(ex.GetTable()), nil
}

// translateKind checks the compaction kind and its FATE binding. A USER
// compaction is driven by a FATE transaction that waits for the commit,
// so a job missing its fateId could never be completed.
func translateKind(job *tabletserver.TExternalCompactionJob) (tabletserver.TCompactionKind, string, error) {
	kind := job.GetKind()
	switch kind {
	case tabletserver.TCompactionKind_SYSTEM, tabletserver.TCompactionKind_USER:
	default:
		return kind, "", refuse(ClassMalformedJob, "kind", "unknown compaction kind %d", int64(kind))
	}

	fateID := ""
	if job.IsSetFateId() && job.GetFateId() != nil {
		fate := job.GetFateId()
		if fate.GetTxUUIDStr() == "" {
			return kind, "", refuse(ClassMalformedJob, "fateId", "empty transaction uuid")
		}
		fateID = fateIDString(fate)
	}
	if kind == tabletserver.TCompactionKind_USER && fateID == "" {
		return kind, "", refuse(ClassMalformedJob, "fateId",
			"USER compaction has no FATE transaction to commit against")
	}
	return kind, fateID, nil
}

func fateIDString(fate *manager.TFateId) string {
	return fate.GetType().String() + ":" + fate.GetTxUUIDStr()
}

// parsedInput is one structurally valid input entry, carrying the fence
// that pass 2 refuses on.
type parsedInput struct {
	file             InputFile
	field            string
	startRow, endRow string
}

func (p parsedInput) fenced() bool { return p.startRow != "" || p.endRow != "" }

func inputFiles(inputs []parsedInput) []InputFile {
	out := make([]InputFile, 0, len(inputs))
	for _, in := range inputs {
		out = append(out, in.file)
	}
	return out
}

// parseInputs decodes every input entry and checks the job's inputs for
// internal consistency. Structure only: whether shoal can read a given
// file, and whether the job fits this compactor's budget, are pass-2
// questions (checkInputCapability).
func parseInputs(job *tabletserver.TExternalCompactionJob) ([]parsedInput, int64, int64, error) {
	files := job.GetFiles()
	if len(files) == 0 {
		return nil, 0, 0, refuse(ClassMalformedJob, "files", "job has no input files")
	}

	out := make([]parsedInput, 0, len(files))
	// Duplicates are detected by decoded path, not by the raw entry: the
	// same RFile can arrive as JSON with different field order or as a
	// legacy bare path, and merging it twice would double its cells.
	seen := make(map[string]int, len(files))
	var totalBytes, totalEntries int64
	for i, f := range files {
		field := fmt.Sprintf("files[%d]", i)
		if f == nil {
			return nil, 0, 0, refuse(ClassMalformedJob, field, "nil entry")
		}
		entry := f.GetMetadataFileEntry()
		if entry == "" {
			return nil, 0, 0, refuse(ClassMalformedJob, field, "empty metadata file entry")
		}
		if f.GetSize() < 0 || f.GetEntries() < 0 {
			return nil, 0, 0, refuse(ClassMalformedJob, field,
				"negative DataFileValue (size=%d entries=%d)", f.GetSize(), f.GetEntries())
		}

		in, err := decodeFileEntry(entry, field)
		if err != nil {
			return nil, 0, 0, err
		}
		in.file.Size = f.GetSize()
		in.file.Entries = f.GetEntries()
		in.file.Timestamp = f.GetTimestamp()

		if prev, dup := seen[in.file.Path]; dup {
			return nil, 0, 0, refuse(ClassMalformedJob, field,
				"duplicates files[%d] (%s); compacting a file twice would double its cells",
				prev, in.file.Path)
		}
		seen[in.file.Path] = i

		// The declared sizes are attacker- or bug-supplied. Summing them
		// blindly can wrap negative, which would slip past the budget
		// check below and report nonsense totals in the plan.
		if totalBytes > math.MaxInt64-f.GetSize() || totalEntries > math.MaxInt64-f.GetEntries() {
			return nil, 0, 0, refuse(ClassMalformedJob, field,
				"DataFileValue totals overflow int64 (size=%d entries=%d after %d files); "+
					"the declared file sizes cannot describe a real tablet",
				f.GetSize(), f.GetEntries(), i)
		}
		totalBytes += f.GetSize()
		totalEntries += f.GetEntries()
		out = append(out, in)
	}
	return out, totalBytes, totalEntries, nil
}

// checkInputCapability refuses inputs shoal cannot read and jobs beyond
// this compactor's configured budget. Fences are reported first: raising
// a limit could never make a fenced job runnable, so it is the more
// useful answer when a job has both problems.
func checkInputCapability(inputs []parsedInput, totalBytes int64, limits Limits) error {
	for _, in := range inputs {
		if in.fenced() {
			return refuse(ClassRangedInputFile, in.field,
				"input %s is fenced to (%q,%q]; shoal reads whole RFiles, so the fence cannot be honored",
				in.file.Path, in.startRow, in.endRow)
		}
	}
	if limits.MaxInputFiles > 0 && len(inputs) > limits.MaxInputFiles {
		return refuse(ClassResourceLimitExceeded, "files",
			"%d input files exceeds the configured limit of %d", len(inputs), limits.MaxInputFiles)
	}
	if limits.MaxTotalInputBytes > 0 && totalBytes > limits.MaxTotalInputBytes {
		return refuse(ClassResourceLimitExceeded, "files",
			"total input size %d bytes exceeds the configured limit of %d bytes",
			totalBytes, limits.MaxTotalInputBytes)
	}
	return nil
}

// decodeFileEntry extracts the file's URI and row fence from a metadata
// file entry. Accumulo 3+ sends StoredTabletFile JSON ({"path":...,
// "startRow":...,"endRow":...}); the legacy form is a bare path.
func decodeFileEntry(entry, field string) (parsedInput, error) {
	in := parsedInput{field: field, file: InputFile{Entry: entry}}
	if !strings.HasPrefix(strings.TrimSpace(entry), "{") {
		// Legacy bare-path form (Accumulo ≤ 2.0). Whole-file by
		// definition, so there is no fence to carry.
		in.file.Path = entry
		return in, nil
	}
	decoded, err := metadata.DecodeStoredTabletFile([]byte(entry))
	if err != nil {
		return parsedInput{}, refuse(ClassMalformedJob, field,
			"undecodable StoredTabletFile entry: %v", err)
	}
	in.file.Path = decoded.Path
	in.startRow, in.endRow = decoded.StartRow, decoded.EndRow
	return in, nil
}

// translateOutput validates the destination file. An output that aliases
// an input would destroy the input mid-compaction, so it is refused even
// though the coordinator should never produce one.
func translateOutput(job *tabletserver.TExternalCompactionJob, inputs []parsedInput) (string, error) {
	out := job.GetOutputFile()
	if out == "" {
		return "", refuse(ClassMalformedJob, "outputFile", "missing")
	}
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		return "", refuse(ClassMalformedJob, "outputFile",
			"expected a plain path, got a StoredTabletFile entry: %s", out)
	}
	if !strings.HasSuffix(out, ".rf") {
		return "", refuse(ClassMalformedJob, "outputFile",
			"%q does not name an RFile (.rf)", out)
	}
	for i, in := range inputs {
		if in.file.Path == out {
			return "", refuse(ClassMalformedJob, "outputFile",
				"output %s is also files[%d]", out, i)
		}
	}
	return out, nil
}

// Table properties the compaction may override that shoal can honor when
// writing the output RFile.
const (
	propCompressType      = "table.file.compress.type"
	propCompressBlockSize = "table.file.compress.blocksize"
)

// ignorableOverrides are properties that change where/how the output file
// is *stored* by the filesystem, not what is inside it. Honoring them is
// the storage layer's job, and ignoring them cannot change a single cell,
// so they do not force the job onto a Java compactor.
var ignorableOverrides = map[string]bool{
	"table.file.blocksize":   true, // HDFS block size
	"table.file.replication": true, // HDFS replication factor
}

// cryptoOverridePrefixes mark on-disk-encryption configuration. shoal's
// RFile writer emits cleartext blocks, so any of these means the output
// must come from Java.
var cryptoOverridePrefixes = []string{
	"table.crypto.",
	"instance.crypto.",
	"general.custom.crypto.",
}

// accumuloCodecs maps Accumulo's table.file.compress.type values to
// shoal's BCFile codec names. Accumulo also ships zstd/lz4/bzip2/lzo;
// shoal's writer cannot produce those, and writing a differently
// compressed file than the table asks for would surprise both the
// operator and any size-based compaction planner, so they are refused.
var accumuloCodecs = map[string]string{
	"none":   block.CodecNone,
	"gz":     block.CodecGzip,
	"gzip":   block.CodecGzip,
	"snappy": block.CodecSnappy,
}

// maxBlockSize caps table.file.compress.blocksize. The writer buffers a
// whole data block, and the value is operator-controlled, so an absurd
// setting is refused rather than trusted.
const maxBlockSize = 1 << 30 // 1 GiB

// parseOverrides checks the syntax of the job's property overrides and
// returns the keys in sorted order. Sorting is what makes a job with
// several unsupported overrides always report the same one: map
// iteration order would otherwise change the class an operator sees
// between identical jobs.
//
// Whether shoal can *honor* a property is a pass-2 question
// (resolveOutputEncoding); only a value that is not a legal property
// value at all is malformed here.
func parseOverrides(overrides map[string]string) ([]string, int64, error) {
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	blockSize := int64(0)
	if raw, ok := overrides[propCompressBlockSize]; ok {
		n, err := parseMemoryBytes(raw)
		if err != nil {
			return nil, 0, refuse(ClassMalformedJob, "overrides["+propCompressBlockSize+"]", "%v", err)
		}
		blockSize = n
	}
	return keys, blockSize, nil
}

// resolveOutputEncoding turns the job's overrides into the output RFile's
// codec and block size. Unknown properties are refused rather than
// ignored: an override exists precisely because someone wanted this
// compaction to behave differently, and silently dropping it would
// produce an output that does not match the request.
func resolveOutputEncoding(
	overrides map[string]string,
	keys []string,
	blockSizeOverride int64,
	opts Options,
) (string, int, error) {
	codec := opts.DefaultCodec
	blockSize := opts.DefaultBlockSize

	for _, key := range keys {
		value := overrides[key]
		field := "overrides[" + key + "]"
		if isCryptoOverride(key) {
			return "", 0, refuse(ClassUnsupportedCrypto, field,
				"shoal writes cleartext RFiles; encrypted output must come from a Java compactor")
		}
		switch {
		case key == propCompressType:
			mapped, ok := accumuloCodecs[strings.ToLower(strings.TrimSpace(value))]
			if !ok {
				return "", 0, refuse(ClassUnsupportedProperty, field,
					"unsupported compression codec %q (shoal writes none, gz or snappy)", value)
			}
			codec = mapped
		case key == propCompressBlockSize:
			if blockSizeOverride <= 0 || blockSizeOverride > maxBlockSize {
				return "", 0, refuse(ClassUnsupportedProperty, field,
					"block size %d is outside the supported range (1..%d bytes)",
					blockSizeOverride, maxBlockSize)
			}
			blockSize = int(blockSizeOverride)
		case ignorableOverrides[key]:
			// Filesystem placement only — cannot change output cells.
		default:
			return "", 0, refuse(ClassUnsupportedProperty, field,
				"shoal cannot honor this override when writing the output RFile")
		}
	}
	return codec, blockSize, nil
}

func isCryptoOverride(key string) bool {
	for _, prefix := range cryptoOverridePrefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// parseMemoryBytes mirrors Java's ConfigurationTypeHelper.getMemoryAsBytes:
// an optional B/K/M/G suffix (case-insensitive) scales a decimal value by
// 2^0/2^10/2^20/2^30; no suffix means bytes.
func parseMemoryBytes(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("empty memory value")
	}
	shift := uint(0)
	switch s[len(s)-1] {
	case 'B', 'b':
		shift = 0
		s = s[:len(s)-1]
	case 'K', 'k':
		shift = 10
		s = s[:len(s)-1]
	case 'M', 'm':
		shift = 20
		s = s[:len(s)-1]
	case 'G', 'g':
		shift = 30
		s = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a memory size: %w", raw, err)
	}
	if shift > 0 && (n > math.MaxInt64>>shift || n < math.MinInt64>>shift) {
		return 0, fmt.Errorf("%q overflows int64", raw)
	}
	return n << shift, nil
}

// systemIterators are stack entries shoal's composer installs itself
// (internal/compaction wraps the merge in a DeletingIterator, and
// visibility filtering is a scan-scope concern). A job that also lists
// one would double-apply it, so it is refused instead of silently
// deduplicated.
var systemIterators = map[string]bool{
	iterrt.IterDeleting:   true,
	iterrt.IterVisibility: true,
}

// rawIterator is one structurally valid stack entry, before it is mapped
// onto a shoal iterator.
type rawIterator struct {
	name     string
	class    string
	field    string
	priority int32
	options  map[string]string
}

// parseIterators checks job.iteratorSettings for internal consistency,
// over the whole list, before any entry is mapped: an unported class in
// the first entry must not hide a malformed second one.
//
// Order is taken from the coordinator's list, not re-sorted: Java applies
// the settings in list order (IteratorConfigUtil.loadIterators), and
// re-sorting here could build a different stack than Java would from the
// same job. Well-formed jobs are already priority-ordered, so a list that
// is not is treated as malformed rather than silently reinterpreted.
func parseIterators(job *tabletserver.TExternalCompactionJob) ([]rawIterator, error) {
	if !job.IsSetIteratorSettings() || job.GetIteratorSettings() == nil {
		return nil, nil
	}
	settings := job.GetIteratorSettings().GetIterators()
	if len(settings) == 0 {
		return nil, nil
	}

	out := make([]rawIterator, 0, len(settings))
	seen := make(map[string]int, len(settings))
	prevPriority := int32(math.MinInt32)
	for i, s := range settings {
		field := fmt.Sprintf("iteratorSettings[%d]", i)
		if s == nil {
			return nil, refuse(ClassMalformedJob, field, "nil iterator setting")
		}
		name, class := s.GetName(), s.GetIteratorClass()
		if name == "" {
			return nil, refuse(ClassMalformedJob, field, "iterator has no name")
		}
		field = "iteratorSettings." + name
		if class == "" {
			return nil, refuse(ClassMalformedJob, field, "iterator has no class")
		}
		if prev, dup := seen[name]; dup {
			return nil, refuse(ClassMalformedJob, field,
				"duplicates iteratorSettings[%d]", prev)
		}
		seen[name] = i
		if s.GetPriority() < prevPriority {
			return nil, refuse(ClassMalformedJob, field,
				"priority %d follows %d; the stack order is ambiguous",
				s.GetPriority(), prevPriority)
		}
		prevPriority = s.GetPriority()

		// Copied, not aliased: the plan must not change if the transport
		// reuses or recycles the job struct.
		options := make(map[string]string, len(s.GetProperties()))
		for k, v := range s.GetProperties() {
			options[k] = v
		}
		out = append(out, rawIterator{
			name:     name,
			class:    class,
			field:    field,
			priority: s.GetPriority(),
			options:  options,
		})
	}
	return out, nil
}

// mapIterators resolves each entry onto shoal's iterator registry, then
// builds the stack once against an empty source: that runs every
// iterator's Init, which is where iterrt validates options, so a bad
// option is refused before any input is read instead of failing
// mid-compaction.
func mapIterators(raw []rawIterator, fullMajor bool) ([]Iterator, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]Iterator, 0, len(raw))
	for _, r := range raw {
		alias, ok := itercfg.ClassAllowlist[r.class]
		if !ok {
			return nil, refuse(ClassUnsupportedIterator, r.field,
				"%s has no shoal port; the compaction would silently drop its behaviour", r.class)
		}
		if systemIterators[alias] {
			return nil, refuse(ClassUnsupportedIterator, r.field,
				"%s is a system iterator shoal's compaction composer installs itself; "+
					"running it twice would change the output", r.class)
		}
		out = append(out, Iterator{
			Name:     r.name,
			Class:    r.class,
			Priority: r.priority,
			Spec:     iterrt.IterSpec{Name: alias, Options: r.options},
		})
	}

	if err := validateStack(out, fullMajor); err != nil {
		return nil, err
	}
	return out, nil
}

// validateStack builds the translated stack over an empty source so every
// iterator's Init — and therefore its option parsing — runs now.
func validateStack(iters []Iterator, fullMajor bool) error {
	specs := make([]iterrt.IterSpec, 0, len(iters))
	for _, it := range iters {
		specs = append(specs, it.Spec)
	}
	env := iterrt.IteratorEnvironment{
		Scope:               iterrt.ScopeMajc,
		FullMajorCompaction: fullMajor,
	}
	if _, err := iterrt.BuildStack(iterrt.NewSliceSource(nil), specs, env); err != nil {
		return refuse(ClassUnsupportedIterator, "iteratorSettings",
			"shoal cannot build this stack: %v", err)
	}
	return nil
}
