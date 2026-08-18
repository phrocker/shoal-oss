//go:build windows

package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/local"
)

func TestStagePathsAliasWindowsDrivePathReachesSameFile(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.rf")
	aliasPath := filepath.Join(dir, "alias.rf")

	if err := os.WriteFile(realPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(realPath, aliasPath); err != nil {
		t.Fatalf("Link(%q, %q): %v", realPath, aliasPath, err)
	}

	urlLikeAlias := toWindowsDoubleSlashDrivePath(t, aliasPath)
	if localPathsLexicallyAlias(urlLikeAlias, realPath) {
		t.Fatalf("test setup invalid: %q and %q alias lexically, want to exercise SameFile", urlLikeAlias, realPath)
	}
	if !stagePathsAlias(urlLikeAlias, realPath) {
		t.Fatalf("stagePathsAlias(%q, %q) = false, want true via local SameFile identity", urlLikeAlias, realPath)
	}
}

func toWindowsDoubleSlashDrivePath(t *testing.T, path string) string {
	t.Helper()

	volume := filepath.VolumeName(path)
	if volume == "" {
		t.Fatalf("path %q has no Windows volume", path)
	}
	rest := strings.TrimPrefix(path, volume)
	return volume + "/" + filepath.ToSlash(rest)
}

// TestLocalPathsLexicallyAliasTrailingDotsAndSpaces exercises the
// Windows-only fallback in localPathsLexicallyAlias: Win32 path
// resolution silently strips trailing dots and spaces from every path
// component, so "A.rf", "A.rf.", and "A.rf " all name the same NTFS
// file even though they are three distinct byte strings and none of
// the earlier (exact, case-fold, NFC-fold) comparisons treats them as
// equal.
func TestLocalPathsLexicallyAliasTrailingDotsAndSpaces(t *testing.T) {
	tests := []struct {
		name string
		src  string
		dst  string
		want bool
	}{
		{name: "trailing dot on final component", src: `C:\bulk\events-1\A.rf`, dst: `C:\bulk\events-1\A.rf.`, want: true},
		{name: "trailing space on final component", src: `C:\bulk\events-1\A.rf`, dst: `C:\bulk\events-1\A.rf `, want: true},
		{name: "trailing dot on an intermediate directory component", src: `C:\bulk\events-1\A.rf`, dst: `C:\bulk\events-1.\A.rf`, want: true},
		{name: "trailing dot changes which file is named", src: `C:\bulk\events-1\A.rf`, dst: `C:\bulk\events-1\A.rf.rf`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := localPathsLexicallyAlias(tt.src, tt.dst); got != tt.want {
				t.Fatalf("localPathsLexicallyAlias(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
			}
		})
	}
}

// TestStageBulkDirRejectsWindowsTrailingDotAliasedWriteTargetsBeforeCopying
// proves the integration-level effect of the same fix: two manifest
// entries whose DestinationPath values are only lexically distinct by
// a trailing dot flatten (via the plain string operation
// filepath.Base) to two different-looking bulk names, but those two
// names would resolve to the identical NTFS file once actually
// written, silently overwriting one staged rfile's bytes with the
// other's. StageBulkDir must reject this before either write target is
// created, matching TestStageBulkDirRejectsCaseInsensitiveAliasBeforeCopying's
// case-folding scenario.
func TestStageBulkDirRejectsWindowsTrailingDotAliasedWriteTargetsBeforeCopying(t *testing.T) {
	root := t.TempDir()
	exportDir0 := filepath.Join(root, "export", "t-0000")
	exportDir1 := filepath.Join(root, "export", "t-0001")
	if err := os.MkdirAll(exportDir0, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(exportDir1, 0o755); err != nil {
		t.Fatal(err)
	}
	plainPath := filepath.Join(exportDir0, "A.rf")
	// dottedPath is a literal manifest DestinationPath value with a
	// trailing dot; Windows transparently resolves it to the same
	// on-disk file as its dot-less sibling below, so this is a real
	// source file readable under either spelling, not just a string
	// that happens to differ.
	realPath := filepath.Join(exportDir1, "A.rf")
	dottedPath := realPath + "."
	plainContent := []byte("plain file bytes that must survive a rejected stage")
	dottedContent := []byte("dotted-alias file bytes")
	if err := os.WriteFile(plainPath, plainContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(realPath, dottedContent, 0o644); err != nil {
		t.Fatal(err)
	}

	bulkDir := filepath.Join(root, "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Neither bulkDir/A.rf nor bulkDir/A.rf. exists yet -- the scenario
	// an os.Stat-based physical-identity check on the *targets* cannot
	// catch, since there is nothing to stat until after the (unsafe)
	// first copy.

	plainSum := sha256.Sum256(plainContent)
	dottedSum := sha256.Sum256(dottedContent)
	manifest := &engine.RFileExportManifest{
		Version:     engine.RFileExportManifestVersion,
		SourceTable: "events",
		Tablets:     []engine.RFileExportTablet{{Index: 0}},
		RFiles: []engine.RFileExportFile{
			{TabletIndex: 0, DestinationPath: plainPath, Size: int64(len(plainContent)), SHA256: hex.EncodeToString(plainSum[:])},
			{TabletIndex: 0, DestinationPath: dottedPath, Size: int64(len(dottedContent)), SHA256: hex.EncodeToString(dottedSum[:])},
		},
	}

	be := local.New()
	ctx := context.Background()
	if _, err := StageBulkDir(ctx, be, manifest, be, bulkDir); err == nil {
		t.Fatal("StageBulkDir with two trailing-dot-aliased write targets = nil error, want error")
	}

	gotPlain, err := os.ReadFile(plainPath)
	if err != nil {
		t.Fatalf("source file A.rf (plain) missing after rejected stage: %v", err)
	}
	if string(gotPlain) != string(plainContent) {
		t.Fatalf("source file A.rf (plain) corrupted by rejected stage: got %q, want %q", gotPlain, plainContent)
	}
	gotReal, err := os.ReadFile(realPath)
	if err != nil {
		t.Fatalf("source file A.rf (dotted alias's real file) missing after rejected stage: %v", err)
	}
	if string(gotReal) != string(dottedContent) {
		t.Fatalf("source file A.rf (dotted alias's real file) corrupted by rejected stage: got %q, want %q", gotReal, dottedContent)
	}
}
