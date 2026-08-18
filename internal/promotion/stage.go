package promotion

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage"
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
// The manifest is verified (engine.VerifyRFileExport: every RFile exists at
// src and matches its recorded size/SHA256) before anything is copied, so a
// truncated, corrupted, or stale export manifest fails fast here rather
// than silently staging incomplete or mismatched data for Accumulo to bulk
// import.
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
	staged := make(map[string]bool, len(manifest.RFiles))
	for _, rf := range manifest.RFiles {
		if staged[rf.DestinationPath] {
			// Same physical file referenced by more than one manifest
			// entry (see flattenNames and BuildLoadMapping's dedup):
			// already copied once, nothing more to do.
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
