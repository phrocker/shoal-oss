package promotion

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/azure"
	"github.com/phrocker/shoal/internal/storage/gcs"
	"github.com/phrocker/shoal/internal/storage/hdfs"
	"github.com/phrocker/shoal/internal/storage/s3"
)

var (
	parseS3Path            = s3.ParsePath
	parseGCSPath           = gcs.ParsePath
	parseAzurePath         = azure.ParsePath
	parseCanonicalHDFSPath = url.Parse
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
// compared against every other write target, and every unique source
// path is also compared against every other unique source path. That
// catches in-place staging (bulkDir is the export's own tablet
// directory), a destination reached through a symlink/hard link to a
// *different* RFile's source, aliased write targets such as two staged
// filenames (or a staged filename and loadmap.json) that resolve to the
// same location, and two manifest sources that are themselves the same
// physical file reached through two different DestinationPath spellings
// (for example a symlink/hard link, or a case/Unicode-normalization
// difference). If those aliases flatten to the same basename,
// StageBulkDir dedupes them to one staged file; if they flatten to
// different basenames, StageBulkDir rejects the manifest as ambiguous
// rather than staging the same physical file twice under different
// names for Accumulo to bulk import.
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
	if err := validateBulkDirOnBackend(dst, bulkDir); err != nil {
		return nil, err
	}
	if _, _, err := resolveManifestTablet(manifest); err != nil {
		return nil, err
	}
	if err := engine.VerifyRFileExport(ctx, src, manifest); err != nil {
		return nil, fmt.Errorf("promotion: stage: %w", err)
	}
	stageRFiles, err := dedupeStageSources(src, manifest.RFiles)
	if err != nil {
		return nil, err
	}
	stageManifest := *manifest
	stageManifest.RFiles = stageRFiles

	mapping, err := BuildLoadMapping(&stageManifest)
	if err != nil {
		return nil, err
	}
	flatNames, err := flattenNames(stageManifest.RFiles)
	if err != nil {
		return nil, err
	}
	if err := checkNoStagingAliases(src, dst, flatNames, bulkDir); err != nil {
		return nil, err
	}
	for _, rf := range stageManifest.RFiles {
		dstPath := joinBulkPath(dst, bulkDir, flatNames[rf.DestinationPath])
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
// if any two write targets resolve to the same location, or if any two
// unique source paths resolve to the same location. Checking only the
// 1:1 source/destination pairing would miss a destination that aliases
// a *different* manifest entry's source (for example via a
// symlink/hard link), while checking only target-vs-source would
// separately miss two aliased write targets (for example two
// hard-linked/symlinked flattened destinations, or loadmap.json
// aliasing one of them) that would overwrite each other mid-stage.
// Checking only the two write-side comparisons would still miss two
// manifest sources that alias each other: each verifies and flattens to
// a distinct basename independently of the other, so neither the
// target-vs-source nor the target-vs-target check has any reason to
// reject them, yet staging both would silently duplicate one physical
// file's rows under two different bulk-import filenames.
//
// The all-pairs comparison is O(N^2) in the number of unique sources and
// write targets across all three comparisons combined, but the
// underlying local-filesystem os.Stat/symlink probes are cached in one
// pathIdentityCache shared across the whole comparison (O(N) local path
// probes overall), and remote backends are compared via backend-aware
// canonicalized path strings only. Manifests are expected to hold at
// most a few thousand RFiles per single-tablet export, so an O(N^2)
// in-memory comparison over already-cached path identities is not a
// practical concern.
func checkNoStagingAliases(src, dst storage.Backend, flatNames map[string]string, bulkDir string) error {
	srcPaths := make([]stagePathRef, 0, len(flatNames))
	targets := make([]stageWriteTarget, 0, len(flatNames)+1)
	for srcPath, flatName := range flatNames {
		srcPaths = append(srcPaths, stagePathRef{backend: src, path: srcPath})
		targets = append(targets, stageWriteTarget{
			name: flatName,
			path: joinBulkPath(dst, bulkDir, flatName),
		})
	}
	targets = append(targets, stageWriteTarget{
		name: bulkLoadMappingFile,
		path: joinBulkPath(dst, bulkDir, bulkLoadMappingFile),
	})

	cache := newPathIdentityCache(len(srcPaths) + len(targets))
	for i := range srcPaths {
		for j := i + 1; j < len(srcPaths); j++ {
			if pathsAlias(srcPaths[i], srcPaths[j], cache) {
				return fmt.Errorf(
					"promotion: stage: manifest sources %s and %s resolve to the same physical file; refusing to stage duplicate copies",
					srcPaths[i].path, srcPaths[j].path,
				)
			}
		}
	}
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

type backendUnwrapper interface {
	InnerBackend() storage.Backend
}

type stageSourceAliasGroup struct {
	reference stagePathRef
	members   []engine.RFileExportFile
}

// dedupeStageSources collapses manifest entries that resolve to the same
// already-exported source object before any staging begins. This avoids
// false flatten collisions and duplicate staged output when one source is
// reachable through multiple symlink/hardlink or backend-equivalent path
// spellings. Alias groups are only auto-deduped when every member would
// flatten to the same basename; if the same physical source is advertised
// under multiple flattened filenames, StageBulkDir fails closed rather
// than arbitrarily picking one bulk-import name.
func dedupeStageSources(src storage.Backend, rfiles []engine.RFileExportFile) ([]engine.RFileExportFile, error) {
	cache := newPathIdentityCache(len(rfiles))
	groups := make([]stageSourceAliasGroup, 0, len(rfiles))
	for _, rf := range rfiles {
		ref := stagePathRef{backend: src, path: rf.DestinationPath}
		grouped := false
		for i := range groups {
			if sourceRefsAlias(ref, groups[i].reference, cache) {
				groups[i].members = append(groups[i].members, rf)
				grouped = true
				break
			}
		}
		if grouped {
			continue
		}
		groups = append(groups, stageSourceAliasGroup{
			reference: ref,
			members:   []engine.RFileExportFile{rf},
		})
	}

	deduped := make([]engine.RFileExportFile, 0, len(groups))
	for _, group := range groups {
		rf, err := canonicalStageSource(group.members)
		if err != nil {
			return nil, err
		}
		deduped = append(deduped, rf)
	}
	return deduped, nil
}

func canonicalStageSource(members []engine.RFileExportFile) (engine.RFileExportFile, error) {
	if len(members) == 0 {
		return engine.RFileExportFile{}, fmt.Errorf("promotion: empty source alias group")
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].DestinationPath == members[j].DestinationPath {
			return members[i].TabletIndex < members[j].TabletIndex
		}
		return members[i].DestinationPath < members[j].DestinationPath
	})

	reference := members[0]
	referenceBase := filepath.Base(reference.DestinationPath)
	for _, member := range members[1:] {
		if member.TabletIndex != reference.TabletIndex {
			return engine.RFileExportFile{}, fmt.Errorf(
				"promotion: source alias %q is declared under multiple tablet indexes (%d and %d)",
				reference.DestinationPath, reference.TabletIndex, member.TabletIndex,
			)
		}
		if filepath.Base(member.DestinationPath) != referenceBase {
			return engine.RFileExportFile{}, fmt.Errorf(
				"promotion: source alias %q is also declared as %q with a different flattened filename; refusing ambiguous dedupe",
				reference.DestinationPath, member.DestinationPath,
			)
		}
	}
	return reference, nil
}

func sourceRefsAlias(left, right stagePathRef, cache pathIdentityCache) bool {
	leftCanonical, leftCanonicalOK := cache.canonicalPath(left)
	rightCanonical, rightCanonicalOK := cache.canonicalPath(right)
	if leftCanonicalOK || rightCanonicalOK {
		return leftCanonicalOK && rightCanonicalOK && leftCanonical == rightCanonical
	}

	if usesLocalFilesystemSemantics(left) && usesLocalFilesystemSemantics(right) {
		leftInfo := cache.stat(left.path)
		rightInfo := cache.stat(right.path)
		if leftInfo == nil || rightInfo == nil {
			return false
		}
		return os.SameFile(leftInfo, rightInfo)
	}

	if pathLooksURLLikeOnBackend(left.backend, left.path) || pathLooksURLLikeOnBackend(right.backend, right.path) {
		return strings.TrimRight(left.path, `/\`) == strings.TrimRight(right.path, `/\`)
	}
	return left.path == right.path
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

// pathIdentityCache memoizes local-filesystem identity probes and remote
// canonical path identities by path so callers comparing many paths against
// each other (checkNoStagingAliases, dedupeStageSources) stat each unique
// local path once, resolve each local path's existing-parent symlink
// prefixes once, compute each publication key once, and parse each remote
// path into its canonical backend-aware identity once rather than
// re-walking or re-parsing the same inputs on every comparison. A cached
// nil FileInfo means the path could not be stat'd (most commonly because it
// doesn't exist yet); that's distinct from "not yet looked up," so failed
// stats are cached too rather than retried.
type pathIdentityCache struct {
	stats              map[string]os.FileInfo
	resolvedLocalPaths map[string]string
	publicationKeys    map[string]string
	canonicalPaths     map[canonicalPathCacheKey]canonicalPathIdentity
}

func newPathIdentityCache(capacity int) pathIdentityCache {
	return pathIdentityCache{
		stats:              make(map[string]os.FileInfo, capacity),
		resolvedLocalPaths: make(map[string]string, capacity),
		publicationKeys:    make(map[string]string, capacity),
		canonicalPaths:     make(map[canonicalPathCacheKey]canonicalPathIdentity, capacity),
	}
}

type canonicalPathCacheKey struct {
	backend string
	path    string
}

type canonicalPathIdentity struct {
	value string
	ok    bool
}

func (c pathIdentityCache) stat(path string) os.FileInfo {
	if info, cached := c.stats[path]; cached {
		return info
	}
	var info os.FileInfo
	for _, statPath := range localStatCandidates(path) {
		candidateInfo, err := os.Stat(statPath)
		if err == nil {
			info = candidateInfo
			break
		}
	}
	c.stats[path] = info
	return info
}

func localStatCandidates(path string) []string {
	if normalized, ok := normalizeWindowsDrivePath(path); ok && normalized != path {
		if runtime.GOOS == "windows" {
			return []string{normalized, path}
		}
		return []string{path, normalized}
	}
	return []string{path}
}

func (c pathIdentityCache) publicationKey(path string) string {
	if key, cached := c.publicationKeys[path]; cached {
		return key
	}
	key := normalizeLocalPublicationPath(c.resolvedLocalPath(path))
	c.publicationKeys[path] = key
	return key
}

func (c pathIdentityCache) resolvedLocalPath(path string) string {
	if resolved, cached := c.resolvedLocalPaths[path]; cached {
		return resolved
	}
	resolved := resolveExistingLocalPathPrefixes(path)
	c.resolvedLocalPaths[path] = resolved
	return resolved
}

func (c pathIdentityCache) canonicalPath(ref stagePathRef) (string, bool) {
	key := canonicalPathCacheKey{
		backend: canonicalPathBackendKey(ref.backend),
		path:    ref.path,
	}
	if identity, cached := c.canonicalPaths[key]; cached {
		return identity.value, identity.ok
	}
	value, ok := canonicalBackendPath(ref)
	c.canonicalPaths[key] = canonicalPathIdentity{value: value, ok: ok}
	return value, ok
}

func canonicalPathBackendKey(backend storage.Backend) string {
	backend = unwrapBackend(backend)
	if backend == nil {
		return "<nil>"
	}
	value := reflect.ValueOf(backend)
	if value.Kind() == reflect.Pointer && !value.IsNil() {
		return value.Type().String() + ":" + strconv.FormatUint(uint64(value.Pointer()), 16)
	}
	return fmt.Sprintf("%T:%#v", backend, backend)
}

// pathsAlias is StageBulkDir's alias detector, factored out so
// checkNoStagingAliases's O(N^2) comparison can share one
// pathIdentityCache across every call instead of stat'ing the same
// handful of local paths repeatedly.
//
// Remote/object-store paths are canonicalized through the backend-aware
// parsers already used by the built-in storage packages (s3.ParsePath,
// gcs.ParsePath, azure.ParsePath, and HDFS URI parsing), even when the
// backend is wrapped (for example by diskcache.Backend), so equivalent
// qualified vs scheme-less spellings compare equal. Local filesystem
// paths compare a collision-safe publication key first (resolving
// existing parent-prefix symlinks, dangling final symlink chains, and
// conservative case/Unicode/trailing-dot-space equivalence), then fall
// back to os.Stat + os.SameFile so existing symlink/hardlink aliases are
// caught too.
func pathsAlias(srcPath, dstPath stagePathRef, cache pathIdentityCache) bool {
	srcCanonical, srcCanonicalOK := cache.canonicalPath(srcPath)
	dstCanonical, dstCanonicalOK := cache.canonicalPath(dstPath)
	if srcCanonicalOK || dstCanonicalOK {
		return srcCanonicalOK && dstCanonicalOK && srcCanonical == dstCanonical
	}

	if usesLocalFilesystemSemantics(srcPath) && usesLocalFilesystemSemantics(dstPath) {
		if localPublicationKeysAlias(srcPath.path, dstPath.path, cache) {
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

	if pathLooksURLLikeOnBackend(srcPath.backend, srcPath.path) || pathLooksURLLikeOnBackend(dstPath.backend, dstPath.path) {
		return strings.TrimRight(srcPath.path, `/\`) == strings.TrimRight(dstPath.path, `/\`)
	}

	return srcPath.path == dstPath.path
}

func localPathsLexicallyAlias(srcPath, dstPath string) bool {
	return normalizeLocalPublicationPath(normalizeLocalPathForAlias(srcPath)) ==
		normalizeLocalPublicationPath(normalizeLocalPathForAlias(dstPath))
}

func localPublicationKeysAlias(srcPath, dstPath string, cache pathIdentityCache) bool {
	return cache.publicationKey(srcPath) == cache.publicationKey(dstPath)
}

func resolveExistingLocalPathPrefixes(path string) string {
	const maxSymlinkHops = 40
	return resolveExistingLocalPathPrefixesWithState(path, &localPathResolutionState{
		hopsRemaining: maxSymlinkHops,
		seen:          make(map[string]struct{}),
	})
}

type localPathResolutionState struct {
	hopsRemaining int
	seen          map[string]struct{}
}

func resolveExistingLocalPathPrefixesWithState(path string, state *localPathResolutionState) string {
	path = normalizeLocalPathForResolution(path)
	prefix, parts := splitLocalPath(path)
	current := prefix
	for i := 0; i < len(parts); {
		candidate := joinLocalPath(current, parts[i])
		info, err := os.Lstat(candidate)
		if err != nil {
			return appendLocalPathParts(current, parts[i:])
		}
		if info.Mode()&os.ModeSymlink != 0 {
			current = resolveLocalSymlinkPathLexically(candidate, state)
			i++
			continue
		}
		if !info.IsDir() && i < len(parts)-1 {
			return appendLocalPathParts(current, parts[i:])
		}
		current = candidate
		i++
	}
	if current == "" {
		return path
	}
	return current
}

// resolveLocalSymlinkPathLexically resolves current -- which the caller
// has already confirmed via os.Lstat is a symlink -- to its final
// target, one hop at a time via os.Readlink. After each hop the new
// target is re-run through resolveExistingLocalPathPrefixesWithState,
// which recursively resolves any symlinked ancestor directory the
// target's own path passes through, not only the target's final
// component; this lets a relative symlink whose target traverses a
// symlinked parent directory (for example a target under a directory
// that is itself "bulk/alias -> .") compare correctly against a
// literal destination path, even when the ultimate file does not exist
// yet. A shared *localPathResolutionState carries a hop budget and a
// "seen" set across every hop and every recursive call, so a symlink
// cycle is detected exactly (returning the repeating path as-is)
// rather than merely bounded by a hop count that could otherwise still
// spin all the way through the cycle before giving up.
//
// This deliberately does not use filepath.EvalSymlinks: that resolves
// the entire input path through the platform's native path-resolution
// APIs, which on Windows can silently rewrite a path's *spelling* even
// for components that are not themselves symlinks -- for example
// expanding a short 8.3-style component such as MARCPA~1 to its long
// form -- which would make an unrelated, never-symlinked path compare
// unequal to another path purely because one of them happened to pass
// through EvalSymlinks. Instead this substitutes a resolved value only
// where a real symlink is actually found, leaving every other
// component's original spelling untouched.
func resolveLocalSymlinkPathLexically(path string, state *localPathResolutionState) string {
	current := normalizeLocalPathForResolution(path)
	for state.hopsRemaining > 0 {
		if _, ok := state.seen[current]; ok {
			return current
		}
		state.seen[current] = struct{}{}
		target, err := os.Readlink(current)
		if err != nil {
			return current
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(current), target)
		}
		state.hopsRemaining--
		target = resolveExistingLocalPathPrefixesWithState(target, state)
		info, err := os.Lstat(target)
		if err != nil || info.Mode()&os.ModeSymlink == 0 {
			return target
		}
		current = normalizeLocalPathForResolution(target)
	}
	return current
}

func normalizeLocalPathForResolution(path string) string {
	path = normalizeLocalPathForAlias(path)
	if !looksLikeWindowsDrivePath(path) && !filepath.IsAbs(path) {
		if absPath, err := filepath.Abs(path); err == nil {
			path = absPath
		}
	}
	return path
}

func normalizeLocalPublicationPath(path string) string {
	prefix, parts := splitLocalPath(path)
	normalizedParts := make([]string, len(parts))
	for i, part := range parts {
		normalizedParts[i] = normalizeLocalPublicationComponent(part)
	}

	prefix = strings.ReplaceAll(prefix, `\`, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix + strings.Join(normalizedParts, "/")
}

func splitLocalPath(path string) (string, []string) {
	path = normalizeLocalPathForAlias(path)
	volume := filepath.VolumeName(path)
	rest := strings.TrimPrefix(path, volume)
	prefix := volume
	if strings.HasPrefix(rest, `/`) || strings.HasPrefix(rest, `\`) {
		prefix += string(filepath.Separator)
		rest = strings.TrimLeft(rest, `/\`)
	}
	if rest == "" {
		return prefix, nil
	}
	return prefix, strings.FieldsFunc(rest, func(r rune) bool { return r == '/' || r == '\\' })
}

func joinLocalPath(prefix, part string) string {
	if prefix == "" {
		return part
	}
	return filepath.Join(prefix, part)
}

func appendLocalPathParts(prefix string, parts []string) string {
	current := prefix
	for _, part := range parts {
		current = joinLocalPath(current, part)
	}
	if current == "" {
		return "."
	}
	return current
}

func usesLocalFilesystemSemantics(ref stagePathRef) bool {
	backend := unwrapBackend(ref.backend)
	if storage.UsesLocalPathSemantics(backend) {
		return true
	}
	if explicitBackendSchemeOnBackend(backend, ref.path) != "" {
		return false
	}
	if backend == nil {
		return true
	}
	return false
}

func canonicalBackendPath(ref stagePathRef) (string, bool) {
	backend := unwrapBackend(ref.backend)
	if storage.UsesLocalPathSemantics(backend) {
		return "", false
	}
	scheme := explicitBackendSchemeOnBackend(backend, ref.path)
	switch b := backend.(type) {
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
	bucket, key, err := parseS3Path(path)
	if err != nil {
		return "", false
	}
	return "s3://" + bucket + "/" + key, true
}

func canonicalGCSPath(path string) (string, bool) {
	bucket, object, err := parseGCSPath(path)
	if err != nil {
		return "", false
	}
	return "gs://" + bucket + "/" + object, true
}

func canonicalAzurePath(path string) (string, bool) {
	container, blob, err := parseAzurePath(path)
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

	u, err := parseCanonicalHDFSPath(objectPath)
	if err != nil {
		return "", false
	}
	if !strings.EqualFold(u.Scheme, "hdfs") || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
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
	return explicitBackendSchemeOnBackend(nil, path)
}

func explicitBackendSchemeOnBackend(backend storage.Backend, path string) string {
	return storage.ExplicitPathScheme(backend, path)
}

func unwrapBackend(backend storage.Backend) storage.Backend {
	for backend != nil {
		unwrapper, ok := backend.(backendUnwrapper)
		if !ok {
			break
		}
		inner := unwrapper.InnerBackend()
		if inner == nil || inner == backend {
			break
		}
		backend = inner
	}
	return backend
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
