package local

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestLocal_ListHidesGeneratedReplacementArtifacts(t *testing.T) {
	dir := t.TempDir()
	files := []string{
		"1.rf",
		".shl-user-visible",
		replacementTempPrefix + "visible",
		replacementBackupPrefix + "visible",
		replacementTempPrefix + strings.Repeat("a", replacementNameTokenBytes*2),
		replacementBackupPrefix + strings.Repeat("b", replacementNameTokenBytes*2),
	}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%q): %v", name, err)
		}
	}

	got, err := New().List(context.Background(), dir)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	want := []string{
		filepath.Join(dir, "1.rf"),
		filepath.Join(dir, ".shl-user-visible"),
		filepath.Join(dir, replacementTempPrefix+"visible"),
		filepath.Join(dir, replacementBackupPrefix+"visible"),
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
}
