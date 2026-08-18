package promotion

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/azure"
	"github.com/phrocker/shoal/internal/storage/gcs"
	"github.com/phrocker/shoal/internal/storage/hdfs"
	"github.com/phrocker/shoal/internal/storage/local"
	"github.com/phrocker/shoal/internal/storage/s3"
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
// Every write target is also preflighted before any copy starts: every
// flattened RFile destination plus loadmap.json is compared against
// every unique source path in the manifest, and every write target is
// compared against every other write target. That catches in-place
// staging (bulkDir is the export's own tablet directory), a destination
// reached through a symlink/hard link to a *different* RFile's source,
// and aliased write targets such as two staged filenames (or a staged
// filename and loadmap.json) that resolve to the same location.
//
// Those aliases are dangerous because storage.Copy opens the destination
// for writing before it reads the source bytes: on backends like local,
// the first write would truncate the aliased file in place, silently
// corrupting a source export or previously written staging output before
// StageBulkDir ever notices. StageBulkDir rejects every such case up
// front rather than destroying any source or staging path.
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
	if err := checkNoStagingAliases(src, dst, flatNames, bulkDir); err != nil {
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

// checkNoStagingAliases rejects the whole stage before any copy starts
// if any write target (every flattened RFile destination plus
// loadmap.json) resolves to the same location as any unique source path,
// or if any two write targets resolve to the same location. Checking
// only the 1:1 source/destination pairing would miss a destination that
// aliases a *different* manifest entry's source (for example via a
// symlink/hard link), while checking only target-vs-source would
// separately miss two aliased write targets (for example two
// hard-linked/symlinked flattened destinations, or loadmap.json
// aliasing one of them) that would overwrite each other mid-stage.
//
// The all-pairs comparison is O(N^2) in the number of unique sources and
// write targets, but the underlying local-filesystem os.Stat/symlink
// probes are cached in one pathIdentityCache shared across the whole
// comparison (O(N) local path probes overall), and remote backends are
// compared via backend-aware canonicalized path strings only. Manifests
// are expected to hold at most a few thousand RFiles per single-tablet
// export, so an O(N^2) in-memory comparison over already-cached path
// identities is not a practical concern.
func checkNoStagingAliases(src, dst storage.Backend, flatNames map[string]string, bulkDir string) error {
	srcPaths := make([]stagePathRef, 0, len(flatNames))
	targets := make([]stageWriteTarget, 0, len(flatNames)+1)
	for srcPath, flatName := range flatNames {
		srcPaths = append(srcPaths, stagePathRef{backend: src, path: srcPath})
		targets = append(targets, stageWriteTarget{
			name: flatName,
			path: joinBulkPath(bulkDir, flatName),
		})
	}
	targets = append(targets, stageWriteTarget{
		name: bulkLoadMappingFile,
		path: joinBulkPath(bulkDir, bulkLoadMappingFile),
	})

	cache := newPathIdentityCache(len(srcPaths) + len(targets))
	for _, target := range targets {
		targetRef := stagePathRef{backend: dst, path: target.path}
		for _, srcPath := range srcPaths {
			if pathsAlias(srcPath, targetRef, cache) {
				return fmt.Errorf(
					"promotion: stage: write target %s (%s) resolves to the same location as source file %s; refusing to write aliased staging output",
					target.name, target.path, srcPath.path,
				)
			}
		}
	}
	for i := range targets {
		left := stagePathRef{backend: dst, path: targets[i].path}
		for j := i + 1; j < len(targets); j++ {
			right := stagePathRef{backend: dst, path: targets[j].path}
			if pathsAlias(left, right, cache) {
				return fmt.Errorf(
					"promotion: stage: write targets %s (%s) and %s (%s) resolve to the same location; refusing to write aliased staging outputs",
					targets[i].name, targets[i].path, targets[j].name, targets[j].path,
				)
			}
		}
	}
	return nil
}

type stagePathRef struct {
	backend storage.Backend
	path    string
}

type stageWriteTarget struct {
	name string
	path string
}

// stagePathsAlias reports whether srcPath and dstPath would resolve to
// the same location under the default local/URL heuristics. It exists as
// a lightweight wrapper for tests; StageBulkDir itself uses
// stagePathsAliasOnBackends so backend-aware canonicalization can treat
// equivalent spellings such as s3://bucket/key versus bucket/key as the
// same destination.
func stagePathsAlias(srcPath, dstPath string) bool {
	return pathsAlias(
		stagePathRef{path: srcPath},
		stagePathRef{path: dstPath},
		newPathIdentityCache(0),
	)
}

func stagePathsAliasOnBackends(src storage.Backend, srcPath string, dst storage.Backend, dstPath string) bool {
	return pathsAlias(
		stagePathRef{backend: src, path: srcPath},
		stagePathRef{backend: dst, path: dstPath},
		newPathIdentityCache(0),
	)
}

// pathIdentityCache memoizes local-filesystem identity probes by path so
// callers comparing many paths against each other (checkNoStagingAliases)
// stat each unique path once and resolve each direct symlink target once,
// not once per comparison. A cached nil FileInfo means the path could not
// be stat'd (most commonly because it doesn't exist yet); that's distinct
// from "not yet looked up," so failed stats are cached too rather than
// retried.
type pathIdentityCache struct {
	stats          map[string]os.FileInfo
	symlinkTargets map[string]symlinkTargetResult
}

type symlinkTargetResult struct {
	target   string
	resolved bool
}

func newPathIdentityCache(capacity int) pathIdentityCache {
	return pathIdentityCache{
		stats:          make(map[string]os.FileInfo, capacity),
		symlinkTargets: make(map[string]symlinkTargetResult, capacity),
	}
}

func (c pathIdentityCache) stat(path string) os.FileInfo {
	if info, cached := c.stats[path]; cached {
		return info
	}
	info, err := os.Stat(path)
	if err != nil {
		info = nil
	}
	c.stats[path] = info
	return info
}

func (c pathIdentityCache) symlinkTarget(path string) (string, bool) {
	if result, cached := c.symlinkTargets[path]; cached {
		return result.target, result.resolved
	}
	target, resolved := resolveLocalSymlinkTarget(path)
	c.symlinkTargets[path] = symlinkTargetResult{target: target, resolved: resolved}
	return target, resolved
}

// pathsAlias is StageBulkDir's alias detector, factored out so
// checkNoStagingAliases's O(N^2) comparison can share one
// pathIdentityCache across every call instead of stat'ing the same
// handful of local paths repeatedly.
//
// Remote/object-store paths are canonicalized through the backend-aware
// parsers already used by the storage packages (s3.ParsePath,
// gcs.ParsePath, azure.ParsePath, and HDFS URI parsing) so equivalent
// spellings compare equal even when one path is qualified and the other
// uses the backend's scheme-less form. Local filesystem paths still get
// a case-insensitive lexical check (to conservatively catch
// not-yet-created aliases on Windows/macOS), plus os.Stat + os.SameFile
// so equivalent absolute/relative spellings and symlink/hardlink aliases
// are caught too.
func pathsAlias(srcPath, dstPath stagePathRef, cache pathIdentityCache) bool {
	srcCanonical, srcCanonicalOK := canonicalBackendPath(srcPath)
	dstCanonical, dstCanonicalOK := canonicalBackendPath(dstPath)
	if srcCanonicalOK || dstCanonicalOK {
		return srcCanonicalOK && dstCanonicalOK && srcCanonical == dstCanonical
	}

	if usesLocalFilesystemSemantics(srcPath) && usesLocalFilesystemSemantics(dstPath) {
		srcClean := filepath.Clean(srcPath.path)
		dstClean := filepath.Clean(dstPath.path)
		if localPathsLexicallyAlias(srcClean, dstClean) {
			return true
		}
		if localSymlinkTargetsAlias(srcPath.path, dstPath.path, cache) {
			return true
		}
		srcInfo := cache.stat(srcPath.path)
		if srcInfo == nil {
			return false
		}
		dstInfo := cache.stat(dstPath.path)
		if dstInfo == nil {
			return false
		}
		return os.SameFile(srcInfo, dstInfo)
	}

	if strings.Contains(srcPath.path, "://") || strings.Contains(dstPath.path, "://") {
		return strings.TrimRight(srcPath.path, `/\`) == strings.TrimRight(dstPath.path, `/\`)
	}

	return srcPath.path == dstPath.path
}

func localSymlinkTargetsAlias(srcPath, dstPath string, cache pathIdentityCache) bool {
	srcClean := filepath.Clean(srcPath)
	dstClean := filepath.Clean(dstPath)

	if target, ok := cache.symlinkTarget(srcPath); ok && localPathsLexicallyAlias(target, dstClean) {
		return true
	}
	if target, ok := cache.symlinkTarget(dstPath); ok && localPathsLexicallyAlias(target, srcClean) {
		return true
	}

	srcTarget, srcIsSymlink := cache.symlinkTarget(srcPath)
	dstTarget, dstIsSymlink := cache.symlinkTarget(dstPath)
	return srcIsSymlink && dstIsSymlink && localPathsLexicallyAlias(srcTarget, dstTarget)
}

func localPathsLexicallyAlias(srcPath, dstPath string) bool {
	srcClean := filepath.Clean(srcPath)
	dstClean := filepath.Clean(dstPath)
	return srcClean == dstClean || strings.EqualFold(srcClean, dstClean)
}

func resolveLocalSymlinkTarget(path string) (string, bool) {
	const maxSymlinkHops = 40

	current := filepath.Clean(path)
	info, err := os.Lstat(current)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		return "", false
	}
	for hops := 0; hops < maxSymlinkHops; hops++ {
		target, err := os.Readlink(current)
		if err != nil {
			return "", false
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(current), target)
		}
		current = filepath.Clean(target)

		info, err = os.Lstat(current)
		if err != nil {
			return current, true
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return current, true
		}
	}
	return current, true
}

func usesLocalFilesystemSemantics(ref stagePathRef) bool {
	if explicitBackendScheme(ref.path) != "" {
		return false
	}
	if ref.backend == nil {
		return true
	}
	_, ok := ref.backend.(*local.Backend)
	return ok
}

func canonicalBackendPath(ref stagePathRef) (string, bool) {
	scheme := explicitBackendScheme(ref.path)
	switch b := ref.backend.(type) {
	case *s3.Backend:
		if scheme != "" && scheme != "s3" {
			return "", false
		}
		return canonicalS3Path(ref.path)
	case *gcs.Backend:
		if scheme != "" && scheme != "gs" {
			return "", false
		}
		return canonicalGCSPath(ref.path)
	case *azure.Backend:
		if scheme != "" && scheme != "az" {
			return "", false
		}
		return canonicalAzurePath(ref.path)
	case *hdfs.Backend:
		if scheme != "" && scheme != "hdfs" {
			return "", false
		}
		return canonicalHDFSPath(ref.path, b.Authority())
	}

	switch scheme {
	case "s3":
		return canonicalS3Path(ref.path)
	case "gs":
		return canonicalGCSPath(ref.path)
	case "az":
		return canonicalAzurePath(ref.path)
	case "hdfs":
		return canonicalHDFSPath(ref.path, "")
	default:
		return "", false
	}
}

func canonicalS3Path(path string) (string, bool) {
	bucket, key, err := s3.ParsePath(path)
	if err != nil {
		return "", false
	}
	return "s3://" + bucket + "/" + key, true
}

func canonicalGCSPath(path string) (string, bool) {
	bucket, object, err := gcs.ParsePath(path)
	if err != nil {
		return "", false
	}
	return "gs://" + bucket + "/" + object, true
}

func canonicalAzurePath(path string) (string, bool) {
	container, blob, err := azure.ParsePath(path)
	if err != nil {
		return "", false
	}
	return "az://" + container + "/" + blob, true
}

func canonicalHDFSPath(objectPath, authorityHint string) (string, bool) {
	if explicitBackendScheme(objectPath) == "" {
		if !strings.HasPrefix(objectPath, "/") {
			return "", false
		}
		return canonicalHDFSString(authorityHint, path.Clean(objectPath)), true
	}

	u, err := url.Parse(objectPath)
	if err != nil {
		return "", false
	}
	if u.Scheme != "hdfs" || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", false
	}

	authority := u.Host
	if authority == "" {
		authority = authorityHint
	}
	resolved := u.Path
	if resolved == "" {
		resolved = "/"
	}
	return canonicalHDFSString(authority, path.Clean(resolved)), true
}

func canonicalHDFSString(authority, resolved string) string {
	if resolved == "." || resolved == "" {
		resolved = "/"
	}
	if !strings.HasPrefix(resolved, "/") {
		resolved = "/" + resolved
	}
	authority = strings.ToLower(authority)
	if authority == "" {
		return "hdfs:" + resolved
	}
	return "hdfs://" + authority + resolved
}

func explicitBackendScheme(path string) string {
	switch {
	case strings.HasPrefix(path, "s3://"):
		return "s3"
	case strings.HasPrefix(path, "gs://"):
		return "gs"
	case strings.HasPrefix(path, "az://"):
		return "az"
	case strings.HasPrefix(path, "hdfs:"):
		return "hdfs"
	default:
		return ""
	}
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
