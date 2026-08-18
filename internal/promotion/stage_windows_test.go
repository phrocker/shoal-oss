//go:build windows

package promotion

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
