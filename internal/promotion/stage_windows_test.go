//go:build windows

package promotion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/storage/local"
	"golang.org/x/sys/windows"
)

func TestStagePathsAliasWindowsDrivePathReachesSameFile(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.rf")
	aliasPath := filepath.Join(dir, "alias.rf")

	if err := os.WriteFile(realPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(realPath, aliasPath); err != nil {
		t.Skipf("hard links not supported in this environment: %v", err)
	}

	urlLikeAlias := toWindowsDoubleSlashDrivePath(t, aliasPath)
	if localPathsLexicallyAlias(urlLikeAlias, realPath) {
		t.Fatalf("test setup invalid: %q and %q alias lexically, want to exercise SameFile", urlLikeAlias, realPath)
	}
	if !stagePathsAlias(urlLikeAlias, realPath) {
		t.Fatalf("stagePathsAlias(%q, %q) = false, want true via local SameFile identity", urlLikeAlias, realPath)
	}
}

func TestStagePathsAliasWindowsTrailingDotOrSpaceAliases(t *testing.T) {
	dir := t.TempDir()
	realPath := filepath.Join(dir, "real.rf")
	if err := os.WriteFile(realPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, alias := range []string{
		filepath.Join(dir, "real.rf."),
		filepath.Join(dir, "real.rf "),
	} {
		if !stagePathsAlias(alias, realPath) {
			t.Fatalf("stagePathsAlias(%q, %q) = false, want true for a Windows trailing dot/space alias", alias, realPath)
		}
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

func TestLocalPathsLexicallyAliasWindowsDOS83ShortNameAmbiguity(t *testing.T) {
	tests := []struct {
		name string
		src  string
		dst  string
		want bool
	}{
		{name: "long name aliases literal dos short name", src: `C:\bulk\LongFilename.rf`, dst: `C:\bulk\LONGFI~1.RF`, want: true},
		{name: "long name with legal windows punctuation aliases literal dos short name", src: `C:\bulk\Long$Filename.rf`, dst: `C:\bulk\LONG$F~1.RF`, want: true},
		{name: "different ordinal still rejected conservatively", src: `C:\bulk\LongFilename.rf`, dst: `C:\bulk\LONGFI~9.RF`, want: true},
		{name: "8dot3-compatible long name does not gain a dos alias family", src: `C:\bulk\PLAIN.RF`, dst: `C:\bulk\PLAIN~1.RF`, want: false},
		{name: "different short-name prefix is distinct", src: `C:\bulk\LongFilename.rf`, dst: `C:\bulk\LONGFJ~1.RF`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := localPathsLexicallyAlias(tt.src, tt.dst); got != tt.want {
				t.Fatalf("localPathsLexicallyAlias(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
			}
		})
	}
}

func TestLocalPathsLexicallyAliasWindowsUNCSharePrefixNormalization(t *testing.T) {
	tests := []struct {
		name string
		src  string
		dst  string
		want bool
	}{
		{name: "standard UNC path aliases differing only by server case", src: `\\SERVER\share\B.rf`, dst: `\\server\share\B.rf`, want: true},
		{name: "standard UNC path aliases differing only by unicode normalization", src: "\\\\SERV\u00c9R\\shar\u00e9\\B.rf", dst: "\\\\serve\u0301r\\share\u0301\\B.rf", want: true},
		{name: "extended UNC path aliases differing only by marker and server case", src: `\\?\UNC\SERVER\share\B.rf`, dst: `\\?\unc\server\share\B.rf`, want: true},
		{name: "extended UNC path aliases ordinary UNC path with no case difference", src: `\\?\UNC\SERVER\share\B.rf`, dst: `\\SERVER\share\B.rf`, want: true},
		{name: "extended UNC path aliases ordinary UNC path with server case difference", src: `\\?\UNC\SERVER\share\B.rf`, dst: `\\server\share\B.rf`, want: true},
		{name: "extended drive path aliases ordinary drive path", src: `\\?\C:\bulk\B.rf`, dst: `C:\bulk\B.rf`, want: true},
		{name: "extended drive path aliases ordinary drive path with case difference", src: `\\?\c:\bulk\B.rf`, dst: `C:\bulk\B.rf`, want: true},
		{name: "distinct shares stay distinct", src: `\\SERVER\share-a\B.rf`, dst: `\\server\share-b\B.rf`, want: false},
		{name: "distinct servers stay distinct", src: `\\SERVER-a\share\B.rf`, dst: `\\server-b\share\B.rf`, want: false},
		{name: "extended UNC path to distinct share stays distinct from ordinary UNC path", src: `\\?\UNC\SERVER\share-a\B.rf`, dst: `\\server\share-b\B.rf`, want: false},
		{name: "extended drive path to distinct directory stays distinct from ordinary drive path", src: `\\?\C:\bulk-a\B.rf`, dst: `C:\bulk-b\B.rf`, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := localPathsLexicallyAlias(tt.src, tt.dst); got != tt.want {
				t.Fatalf("localPathsLexicallyAlias(%q, %q) = %v, want %v", tt.src, tt.dst, got, tt.want)
			}
		})
	}
}

func TestStagePathsAliasWindowsDOS83ShortNameReachesSameFile(t *testing.T) {
	dir := t.TempDir()
	longPath := filepath.Join(dir, "LongFilename.rf")
	if err := os.WriteFile(longPath, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	shortPath := windowsShortPath(t, longPath)
	if strings.EqualFold(shortPath, longPath) {
		t.Skip("filesystem did not surface a distinct DOS 8.3 short name")
	}
	if !stagePathsAlias(shortPath, longPath) {
		t.Fatalf("stagePathsAlias(%q, %q) = false, want true via local SameFile identity", shortPath, longPath)
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

// TestStageBulkDirRejectsTrailingDotOrSpaceWriteTargetsBeforeCopying
// covers the same Win32 trailing-dot/space quirk as
// TestStageBulkDirRejectsWindowsTrailingDotAliasedWriteTargetsBeforeCopying
// above, but from an in-memory source manifest with two distinct blobs
// (rather than one real source file reachable under two spellings),
// matching TestStageBulkDirRejectsCaseInsensitiveAliasBeforeCopying's
// and TestStageBulkDirRejectsUnicodeNormalizedWriteTargetsBeforeCopying's
// pattern for the other publication-key equivalences. This scenario is
// Windows-only: POSIX filesystems store trailing dots and spaces as
// literal, significant filename bytes, so "F0001.rf" and "F0001.rf."
// are genuinely distinct destination files there and StageBulkDir must
// not reject them.
func TestStageBulkDirRejectsTrailingDotOrSpaceWriteTargetsBeforeCopying(t *testing.T) {
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "trailing dot", first: "export/events/t-0000/F0001.rf", second: "export/events/t-0000/F0001.rf."},
		{name: "trailing space", first: "export/events/t-0000/F0002.rf", second: "export/events/t-0000/F0002.rf "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, manifest := memoryManifestFromBlobs(map[string][]byte{
				tt.first:  []byte("first"),
				tt.second: []byte("second"),
			})
			bulkDir := filepath.Join(t.TempDir(), "bulk")
			if err := os.MkdirAll(bulkDir, 0o755); err != nil {
				t.Fatal(err)
			}

			if _, err := StageBulkDir(context.Background(), src, manifest, local.New(), bulkDir); err == nil {
				t.Fatalf("StageBulkDir with %s write-target alias = nil error, want error", tt.name)
			}
		})
	}
}

func TestCheckNoStagingAliasesRejectsDanglingWindowsUNCAliasBeforeAnyWrites(t *testing.T) {
	tests := []struct {
		name    string
		srcPath string
		bulkDir string
	}{
		{name: "standard UNC case variant", srcPath: `\\SERVER\share\B.rf`, bulkDir: `\\server\share`},
		{name: "extended UNC case variant", srcPath: `\\?\UNC\SERVER\share\B.rf`, bulkDir: `\\?\unc\server\share`},
		{name: "extended UNC form vs ordinary UNC form", srcPath: `\\?\UNC\SERVER\share\B.rf`, bulkDir: `\\server\share`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flatNames := map[string]string{tt.srcPath: "B.rf"}
			if err := checkNoStagingAliases(local.New(), local.New(), flatNames, tt.bulkDir); err == nil {
				t.Fatalf("checkNoStagingAliases with dangling UNC source %q and bulkDir %q = nil error, want error", tt.srcPath, tt.bulkDir)
			}
		})
	}
}

func TestStageBulkDirRejectsWindowsADSAbsentWriteTargetsBeforeCopying(t *testing.T) {
	tests := []struct {
		name       string
		targetName string
		baseName   string
	}{
		{name: "default stream short form", targetName: "A.rf:$DATA", baseName: "A.rf"},
		{name: "default stream long form", targetName: "A.rf::$DATA", baseName: "A.rf"},
		{name: "case varied trailing-dot default stream", targetName: "A.rf.::$dAtA", baseName: "A.rf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, manifest := memoryManifestFromBlobs(map[string][]byte{
				filepath.Join("export", "events", "t-0000", tt.targetName): []byte("stream bytes"),
			})
			bulkDir := filepath.Join(t.TempDir(), "bulk")
			if err := os.MkdirAll(bulkDir, 0o755); err != nil {
				t.Fatal(err)
			}

			if _, err := StageBulkDir(context.Background(), src, manifest, local.New(), bulkDir); err == nil {
				t.Fatalf("StageBulkDir with ADS target %q = nil error, want error", tt.targetName)
			}
			if _, err := os.Stat(filepath.Join(bulkDir, tt.baseName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("base target %q exists after rejected ADS stage: err=%v", tt.baseName, err)
			}
			if _, err := os.Stat(filepath.Join(bulkDir, "loadmap.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("loadmap.json exists after rejected ADS stage: err=%v", err)
			}
		})
	}
}

func TestStageBulkDirRejectsWindowsADSWriteTargetsWithoutMutatingExistingFiles(t *testing.T) {
	tests := []struct {
		name         string
		targetName   string
		checkNoNamed bool
	}{
		{name: "unnamed stream alias keeps existing base file intact", targetName: "A.rf::$dAtA"},
		{name: "named stream keeps existing base file intact", targetName: "A.rf:Meta:$DATA", checkNoNamed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bulkDir := filepath.Join(t.TempDir(), "bulk")
			if err := os.MkdirAll(bulkDir, 0o755); err != nil {
				t.Fatal(err)
			}
			baseTarget := filepath.Join(bulkDir, "A.rf")
			original := []byte("existing target bytes that must survive a rejected ADS stage")
			if err := os.WriteFile(baseTarget, original, 0o644); err != nil {
				t.Fatal(err)
			}

			src, manifest := memoryManifestFromBlobs(map[string][]byte{
				filepath.Join("export", "events", "t-0000", tt.targetName): []byte("replacement bytes"),
			})
			if _, err := StageBulkDir(context.Background(), src, manifest, local.New(), bulkDir); err == nil {
				t.Fatalf("StageBulkDir with ADS target %q = nil error, want error", tt.targetName)
			}

			got, err := os.ReadFile(baseTarget)
			if err != nil {
				t.Fatalf("existing base target missing after rejected ADS stage: %v", err)
			}
			if string(got) != string(original) {
				t.Fatalf("existing base target corrupted by rejected ADS stage: got %q, want %q", got, original)
			}
			if tt.checkNoNamed {
				if _, err := os.ReadFile(filepath.Join(bulkDir, tt.targetName)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("named stream target %q became readable after rejected ADS stage: err=%v", tt.targetName, err)
				}
			}
			if _, err := os.Stat(filepath.Join(bulkDir, "loadmap.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("loadmap.json exists after rejected ADS stage: err=%v", err)
			}
		})
	}
}

func TestStageBulkDirRejectsWindowsDOS83AliasedAbsentWriteTargetsBeforeCopying(t *testing.T) {
	src, manifest := memoryManifestFromBlobs(map[string][]byte{
		"export/events/t-0000/LongFilename.rf": []byte("first"),
		"export/events/t-0000/LONGFI~1.RF":     []byte("second"),
	})
	bulkDir := filepath.Join(t.TempDir(), "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if _, err := StageBulkDir(context.Background(), src, manifest, local.New(), bulkDir); err == nil {
		t.Fatal("StageBulkDir with absent Windows DOS 8.3-aliased write targets = nil error, want error")
	}
	for _, target := range []string{"LongFilename.rf", "LONGFI~1.RF", "loadmap.json"} {
		if _, err := os.Stat(filepath.Join(bulkDir, target)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("target %q exists after rejected DOS 8.3 absent-target stage: err=%v", target, err)
		}
	}
}

func TestStageBulkDirRejectsWindowsDOS83ExistingWriteTargetAliasBeforeCopying(t *testing.T) {
	bulkDir := filepath.Join(t.TempDir(), "bulk")
	if err := os.MkdirAll(bulkDir, 0o755); err != nil {
		t.Fatal(err)
	}
	longTarget := filepath.Join(bulkDir, "LongFilename.rf")
	original := []byte("existing target bytes that must survive a rejected DOS 8.3 alias stage")
	if err := os.WriteFile(longTarget, original, 0o644); err != nil {
		t.Fatal(err)
	}
	shortTarget := windowsShortPath(t, longTarget)
	if strings.EqualFold(shortTarget, longTarget) {
		t.Skip("filesystem did not surface a distinct DOS 8.3 short name")
	}
	shortBase := filepath.Base(shortTarget)

	src, manifest := memoryManifestFromBlobs(map[string][]byte{
		filepath.Join("export", "events", "t-0000", "LongFilename.rf"): []byte("first"),
		filepath.Join("export", "events", "t-0000", shortBase):         []byte("second"),
	})
	if _, err := StageBulkDir(context.Background(), src, manifest, local.New(), bulkDir); err == nil {
		t.Fatal("StageBulkDir with an existing DOS 8.3 write-target alias = nil error, want error")
	}

	got, err := os.ReadFile(longTarget)
	if err != nil {
		t.Fatalf("existing target missing after rejected DOS 8.3 alias stage: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("existing target corrupted by rejected DOS 8.3 alias stage: got %q, want %q", got, original)
	}
	gotShort, err := os.ReadFile(shortTarget)
	if err != nil {
		t.Fatalf("existing short-path alias missing after rejected stage: %v", err)
	}
	if string(gotShort) != string(original) {
		t.Fatalf("existing short-path alias corrupted by rejected stage: got %q, want %q", gotShort, original)
	}
}

func windowsShortPath(t *testing.T, path string) string {
	t.Helper()

	longPath, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatalf("UTF16PtrFromString(%q): %v", path, err)
	}
	buf := make([]uint16, len(path)+32)
	for {
		n, err := windows.GetShortPathName(longPath, &buf[0], uint32(len(buf)))
		if err != nil {
			t.Skipf("GetShortPathName(%q): %v", path, err)
		}
		if n == 0 {
			t.Skipf("GetShortPathName(%q) returned no short path", path)
		}
		if int(n) > len(buf) {
			buf = make([]uint16, n)
			continue
		}
		return windows.UTF16ToString(buf[:n])
	}
}
