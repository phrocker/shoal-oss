package promotion

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
	"golang.org/x/text/unicode/norm"
)

// StageBulkDir copies every RFile referenced by manifest from src (the
// export destination backend, where Engine.ExportRFiles /
// ExportRFilesIncremental already placed them, nested under per-tablet
// directories) into bulkDir on dst as a FLAT directory, and writes the
// corresponding loadmap.json describing which destination KeyExtent each
// file belongs to. Accumulo's Bulk Import V2 lists the bulk directory
// non-recursively and expects every referenced file to sit directly inside
// it (see BulkImport.java's directory listing), unlike ExportRFiles' own
// t-NNNN/ nesting.
//
// Copies (rather than moves) so a failed or interrupted stage never loses
// the source export, and re-running StageBulkDir with the same manifest and
// bulkDir reproduces byte-identical files and an identical loadmap.json —
// safe to retry.
//
// Basenames must be unique across the whole manifest once flattened; a
// collision is reported as an error before any copy happens, rather than
// silently overwriting one file with another (see package docs for the
// deferred auto-rename gap, mirroring Accumulo's own renames.json step).
//
// bulkDir itself is preflight-validated before any read or write: empty,
// whitespace-padded, or backend-root destinations fail before staging can
// mutate dst. BuildLoadMapping likewise rejects any split-bearing or
// multi-tablet manifest before staging starts: this slice stages only
// unambiguous single-tablet exports.
//
// The manifest is verified (engine.VerifyRFileExport: every RFile exists at
// src and matches its recorded size/SHA256) before anything is copied, so a
// truncated, corrupted, or stale export manifest fails fast here rather
// than silently staging incomplete or mismatched data for Accumulo to bulk
// import.
//
// Every computed destination path is also preflighted against every
// unique source path in the manifest — not just the source it's paired
// with — before any copy starts: if bulkDir happens to alias any
// manifest source location (e.g. bulkDir is set to the export's own
// tablet directory, or a destination is reached through a symlink/hard
// link to a *different* RFile's source), copying would truncate that
// source before it's read (storage.Copy opens the destination for
// writing, which truncates in place on backends like local, before
// streaming the source's bytes) and silently "succeed" with zero bytes
// copied. A same-manifest cross-file alias is particularly dangerous:
// the truncated file might not be staged until a later iteration of the
// copy loop, by which point its already-completed VerifyRFileExport
// check won't be repeated, so the corrupted bytes would be staged and
// reported as a successful copy. StageBulkDir rejects every such case up
// front rather than destroying any part of the source export.
func StageBulkDir(
	ctx context.Context,
	src storage.Backend,
	manifest *engine.RFileExportManifest,
	dst storage.Backend,
	bulkDir string,
) (LoadMapping, error) {
	if manifest == nil {
		return nil, fmt.Errorf("promotion: nil export manifest")
	}
	if err := validateBulkDir(bulkDir); err != nil {
		return nil, err
	}
	mapping, err := BuildLoadMapping(manifest)
	if err != nil {
		return nil, err
	}
	flatNames, err := flattenNames(manifest.RFiles)
	if err != nil {
		return nil, err
	}
	if err := engine.VerifyRFileExport(ctx, src, manifest); err != nil {
		return nil, fmt.Errorf("promotion: stage: %w", err)
	}
	if err := checkNoStagingAliases(flatNames, bulkDir); err != nil {
		return nil, err
	}
	staged := make(map[string]bool, len(manifest.RFiles))
	for _, rf := range manifest.RFiles {
		if staged[rf.DestinationPath] {
			// Same physical file referenced by more than one identical
			// manifest entry (see flattenNames and BuildLoadMapping's
			// same-tablet dedup): already copied once, nothing more to do.
			continue
		}
		staged[rf.DestinationPath] = true
		dstPath := joinBulkPath(bulkDir, flatNames[rf.DestinationPath])
		if _, err := storage.Copy(ctx, src, rf.DestinationPath, dst, dstPath); err != nil {
			return nil, fmt.Errorf("promotion: stage %s: %w", rf.DestinationPath, err)
		}
	}
	if err := WriteLoadMapping(ctx, dst, bulkDir, mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}

// checkNoStagingAliases rejects the whole stage before any copy or the
// load-mapping write starts if any write target StageBulkDir will open on
// dst — every flattened RFile destination path, *and* bulkDir's own
// loadmap.json (WriteLoadMapping truncates it exactly like storage.Copy
// truncates an RFile destination, but it runs after every RFile copy, so
// an aliased loadmap.json would destroy an already-staged source) —
// resolves to the same physical location as *any* unique manifest source
// path, or as any *other* write target. Checking only "does this RFile's
// destination alias its own source" would miss a destination aliasing a
// *different* manifest entry's source (for example via a symlink/hard
// link), which would silently truncate that other file before it's ever
// copied; checking only target-vs-source would separately miss two
// distinct write targets (e.g. two RFiles' flattened basenames) that are
// hard-linked to *each other* — individually harmless against every
// source, but the second copy would silently overwrite the first while
// the load mapping still lists both names as independent files.
//
// Every unique manifest source path is also checked against every
// *other* unique source path: two different DestinationPath strings
// that physically alias each other (e.g. one is a symlink or hard link
// to the other, or they differ only by case or Unicode normalization
// form) each individually verify against their own recorded size/SHA256
// and flatten to distinct basenames, so neither the target-vs-source
// nor the target-vs-target check above would reject them — StageBulkDir
// would stage two independent, fully-successful copies of what is
// really the same underlying file. That isn't data-destroying like the
// aliases above, but it silently duplicates the source's rows once
// Accumulo bulk-imports both flattened copies under loadmap.json's two
// independent names.
//
// The all-pairs comparison is O(N^2) in the number of write targets, but
// the underlying os.Stat calls are cached in one pathIdentityCache shared
// across the whole comparison (O(N) stats: each unique source path and
// each write target is stat'd at most once), rather than re-stat'ing the
// same handful of paths on every iteration. Manifests are expected to
// hold at most a few thousand RFiles per single-tablet export, so an
// O(N^2) in-memory comparison over already-cached FileInfo values is not
// a practical concern.
func checkNoStagingAliases(flatNames map[string]string, bulkDir string) error {
	srcPaths := make([]string, 0, len(flatNames))
	for srcPath := range flatNames {
		srcPaths = append(srcPaths, srcPath)
	}

	// Every path StageBulkDir will open for writing: one per unique
	// flattened RFile destination, plus the load mapping itself.
	targets := make([]string, 0, len(srcPaths)+1)
	for _, srcPath := range srcPaths {
		targets = append(targets, joinBulkPath(bulkDir, flatNames[srcPath]))
	}
	targets = append(targets, joinBulkPath(bulkDir, bulkLoadMappingFile))

	cache := make(pathIdentityCache, len(srcPaths)+len(targets))
	for i, srcPath := range srcPaths {
		// Compare against later sources only: an unordered pair
		// (srcPath, other) only needs checking once.
		for _, other := range srcPaths[i+1:] {
			if pathsAlias(srcPath, other, cache) {
				return fmt.Errorf(
					"promotion: stage: manifest sources %s and %s resolve to the same physical file; refusing to stage duplicate copies",
					srcPath, other,
				)
			}
		}
	}
	for i, target := range targets {
		for _, srcPath := range srcPaths {
			if pathsAlias(srcPath, target, cache) {
				return fmt.Errorf(
					"promotion: stage: bulk directory %s write target %s resolves to the same location as source file %s; refusing to copy in place",
					bulkDir, target, srcPath,
				)
			}
		}
		// Compare against later targets only: an unordered pair (target,
		// other) only needs checking once.
		for _, other := range targets[i+1:] {
			if pathsAlias(target, other, cache) {
				return fmt.Errorf(
					"promotion: stage: bulk directory %s has two write targets (%s and %s) that resolve to the same location; refusing to stage, since the second write would silently overwrite the first",
					bulkDir, target, other,
				)
			}
		}
	}
	return nil
}

// stagePathsAlias reports whether srcPath (opened for reading on src) and
// dstPath (opened for writing on dst, which truncates in place on
// backends such as local) resolve to the same physical location. src and
// dst are not guaranteed comparable (e.g. local.Backend is a stateless
// zero-field struct, so two independently constructed instances are
// indistinguishable by identity even when they really are "the same
// backend"), so path identity is decided by string/filesystem inspection
// rather than backend identity: a false positive — rejecting a harmless
// coincidental collision between two genuinely different backends — is
// far cheaper than a false negative that silently destroys the source
// export.
//
// URL-style paths (containing "://") have no local filesystem entry to
// inspect, so those are compared as normalized strings only. Filesystem-
// style paths are first compared the same way as a cheap common-case
// check, then a case- and Unicode-normalization-insensitive lexical
// comparison (Windows and macOS default to case-insensitive filesystems,
// and macOS additionally normalizes Unicode filenames, so two paths
// differing only in case and/or normalization form can alias even
// before either exists, which os.Stat cannot detect), then — because
// none of the lexical comparisons above catch an absolute source path
// aliasing an equivalent relative destination path (or vice versa), nor
// a destination reached through a symlink or hard
// link to the same source file — checked for physical identity via
// os.Stat + os.SameFile, which both backends ultimately resolve through
// (local.Backend.Open/Create pass paths straight to the os package). If
// either path can't be stat'd (most commonly because dstPath doesn't
// exist yet, the overwhelmingly common non-aliased case), that's decided
// by the lexical result alone.
func stagePathsAlias(srcPath, dstPath string) bool {
	return pathsAlias(srcPath, dstPath, make(pathIdentityCache))
}

// pathIdentityCache memoizes os.Stat results by path so a caller
// comparing many paths against each other (checkNoStagingAliases) stats
// each unique path once, not once per comparison. A cached nil FileInfo
// means the path could not be stat'd (most commonly because it doesn't
// exist yet); that's distinct from "not yet looked up," so a failed stat
// is cached too rather than retried.
type pathIdentityCache map[string]os.FileInfo

func (c pathIdentityCache) stat(path string) os.FileInfo {
	if info, cached := c[path]; cached {
		return info
	}
	info, err := os.Stat(path)
	if err != nil {
		info = nil
	}
	c[path] = info
	return info
}

// pathsAlias is stagePathsAlias's implementation, factored out so
// checkNoStagingAliases's O(N^2) all-pairs comparison can share one
// pathIdentityCache across every call instead of stat'ing the same
// handful of paths repeatedly (each call to stagePathsAlias's physical-
// identity fallback below performs up to two os.Stat calls).
func pathsAlias(srcPath, dstPath string, cache pathIdentityCache) bool {
	if strings.Contains(srcPath, "://") || strings.Contains(dstPath, "://") {
		return strings.TrimRight(srcPath, `/\`) == strings.TrimRight(dstPath, `/\`)
	}
	srcClean := filepath.Clean(srcPath)
	dstClean := filepath.Clean(dstPath)
	if srcClean == dstClean {
		return true
	}
	// Windows and macOS (by default) resolve filesystem paths
	// case-insensitively, and macOS additionally normalizes Unicode
	// filenames to NFD on disk, so composed and decomposed spellings of
	// the same character (e.g. NFC vs NFD "é") also name the same file
	// there — both cases can alias even before either path exists, which
	// the physical-identity stat below cannot catch, since it needs an
	// existing file to stat, and two not-yet-created destinations both
	// correctly stat as "doesn't exist" right up until the second Create
	// silently resolves onto the first. Normalizing both cleaned paths to
	// NFC before folding case is conservative — it may also reject a
	// legitimate same-spelling collision on a genuinely case-sensitive,
	// normalization-preserving filesystem — matching this package's
	// preference for a safe false positive over a data-destroying false
	// negative.
	if strings.EqualFold(norm.NFC.String(srcClean), norm.NFC.String(dstClean)) {
		return true
	}
	srcInfo := cache.stat(srcPath)
	if srcInfo == nil {
		return false
	}
	dstInfo := cache.stat(dstPath)
	if dstInfo == nil {
		return false
	}
	return os.SameFile(srcInfo, dstInfo)
}

// flattenNames validates that every RFile's basename is unique once
// flattened into a single bulk directory and returns the
// DestinationPath -> flat basename mapping. Two manifest entries sharing
// the same DestinationPath (the same file listed twice) are not a
// collision; distinct source paths that happen to share a basename are.
func flattenNames(rfiles []engine.RFileExportFile) (map[string]string, error) {
	names := make(map[string]string, len(rfiles))
	byName := make(map[string]string, len(rfiles))
	for _, rf := range rfiles {
		name := filepath.Base(rf.DestinationPath)
		if prior, ok := byName[name]; ok && prior != rf.DestinationPath {
			return nil, fmt.Errorf(
				"promotion: bulk dir flatten collision: %q and %q both flatten to %q",
				prior, rf.DestinationPath, name,
			)
		}
		byName[name] = rf.DestinationPath
		names[rf.DestinationPath] = name
	}
	return names, nil
}
