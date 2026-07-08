package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/storage/memory"
)

func TestRFileExportImportMemoryRoundTrip(t *testing.T) {
	ctx := context.Background()
	srcDir := filepath.Join(t.TempDir(), "src")
	dstDir := filepath.Join(t.TempDir(), "dst")

	src, err := Open(srcDir, Options{})
	if err != nil {
		t.Fatalf("Open source: %v", err)
	}
	if err := src.CreateTable("graph", TableOptions{Splits: PrefixSplit("entity:", "event:")}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	for i, row := range []string{"entity:a", "entity:b", "event:1", "z:last"} {
		m, err := cclient.NewMutation([]byte(row))
		if err != nil {
			t.Fatalf("NewMutation: %v", err)
		}
		m.Put([]byte("cf"), []byte(fmt.Sprintf("cq-%d", i)), []byte("tenantA"), int64(100+i), []byte("value-"+row))
		if err := src.Write("graph", []*cclient.Mutation{m}); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	dstBackend := memory.New()
	manifest, err := src.ExportRFiles(ctx, "graph", dstBackend, RFileExportOptions{
		DestinationRoot:     dstDir,
		CFSchema:            "graphschema/v1",
		VisibilityStamp:     "tenantA",
		AuthorizationsStamp: "tenantA",
		EngineVersion:       "test",
	})
	if err != nil {
		t.Fatalf("ExportRFiles: %v", err)
	}
	defer src.Close()
	if got, want := manifest.Version, RFileExportManifestVersion; got != want {
		t.Fatalf("manifest version = %d, want %d", got, want)
	}
	if got, want := manifest.CFSchema, "graphschema/v1"; got != want {
		t.Fatalf("cf schema = %q, want %q", got, want)
	}
	if len(manifest.RFiles) == 0 {
		t.Fatalf("manifest has no RFiles")
	}
	if len(manifest.Tablets) != 3 {
		t.Fatalf("manifest tablets = %d, want 3", len(manifest.Tablets))
	}
	for _, rf := range manifest.RFiles {
		if rf.Size <= 0 {
			t.Fatalf("RFile %s has non-positive size %d", rf.DestinationPath, rf.Size)
		}
		if len(rf.SHA256) != 64 {
			t.Fatalf("RFile %s sha256 = %q", rf.DestinationPath, rf.SHA256)
		}
		if rf.BCFileVersion == "" {
			t.Fatalf("RFile %s missing BCFile version", rf.DestinationPath)
		}
		if !strings.HasPrefix(rf.DestinationPath, dstDir) {
			t.Fatalf("RFile destination %q not under %q", rf.DestinationPath, dstDir)
		}
	}
	if err := VerifyRFileExport(ctx, dstBackend, manifest); err != nil {
		t.Fatalf("VerifyRFileExport: %v", err)
	}

	wantCells := scanAll(t, src, "graph")
	dst, err := Open(dstDir, Options{Backend: dstBackend})
	if err != nil {
		t.Fatalf("Open destination: %v", err)
	}
	defer dst.Close()
	if err := dst.ImportRFileManifest(ctx, manifest); err != nil {
		t.Fatalf("ImportRFileManifest: %v", err)
	}
	gotCells := scanAll(t, dst, "graph")
	if fmt.Sprint(gotCells) != fmt.Sprint(wantCells) {
		t.Fatalf("imported scan mismatch\ngot  %v\nwant %v", gotCells, wantCells)
	}
}

func scanAll(t *testing.T, eng *Engine, table string) []string {
	t.Helper()
	sc, err := eng.Scan(table, iterrt.InfiniteRange(), ScanOptions{})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer sc.Close()
	var out []string
	for sc.Next() {
		k := sc.Key()
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%d|%s",
			k.Row, k.ColumnFamily, k.ColumnQualifier, k.ColumnVisibility, k.Timestamp, sc.Value()))
		if err := sc.Advance(); err != nil {
			t.Fatalf("Advance: %v", err)
		}
	}
	sort.Strings(out)
	return out
}
