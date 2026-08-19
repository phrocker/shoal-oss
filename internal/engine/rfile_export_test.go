package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/iterrt"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/memory"
)

type exportSourceBackend struct{}

func (exportSourceBackend) Open(context.Context, string) (storage.File, error) {
	return exportFailingFile{}, nil
}

type exportFailingFile struct{}

func (exportFailingFile) ReadAt([]byte, int64) (int, error) { return 0, io.ErrUnexpectedEOF }
func (exportFailingFile) Close() error                      { return nil }
func (exportFailingFile) Size() int64                       { return 1 }

type exportDestinationBackend struct {
	writer *exportAbortWriter
}

func (*exportDestinationBackend) Open(context.Context, string) (storage.File, error) {
	return nil, storage.ErrNotFound
}

func (b *exportDestinationBackend) Create(context.Context, string) (storage.Writer, error) {
	return b.writer, nil
}

type exportAbortWriter struct {
	aborted bool
	closed  bool
}

func (*exportAbortWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *exportAbortWriter) Close() error {
	w.closed = true
	return nil
}
func (w *exportAbortWriter) Abort() error {
	w.aborted = true
	return nil
}

func TestCopyWithSHA256AbortsDestinationOnReadFailure(t *testing.T) {
	writer := &exportAbortWriter{}
	_, _, _, err := copyWithSHA256(
		context.Background(),
		exportSourceBackend{},
		"source.rf",
		&exportDestinationBackend{writer: writer},
		"destination.rf",
	)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("copyWithSHA256 error = %v, want read failure", err)
	}
	if !writer.aborted {
		t.Fatal("failed export did not abort destination")
	}
	if writer.closed {
		t.Fatal("failed export closed and could have committed destination")
	}
}

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

func TestCopyWithSHA256PreservesDestinationOnReadFailure(t *testing.T) {
	readErr := errors.New("read failed")
	src := exportFileBackend{file: &exportReadFuncFile{
		size: 2,
		read: func(p []byte, off int64) (int, error) {
			switch off {
			case 0:
				p[0] = 'x'
				return 1, nil
			default:
				return 0, readErr
			}
		},
	}}
	dst := newExportTrackingBackend()
	dst.files["/dst.rf"] = []byte("old")

	written, sum, bcVersion, err := copyWithSHA256(context.Background(), src, "/src.rf", dst, "/dst.rf")
	if !errors.Is(err, readErr) {
		t.Fatalf("copyWithSHA256 error = %v, want %v", err, readErr)
	}
	if written != 1 {
		t.Fatalf("written = %d, want 1", written)
	}
	if sum != "" {
		t.Fatalf("sum = %q, want empty on failure", sum)
	}
	if bcVersion != "" {
		t.Fatalf("bcVersion = %q, want empty on failure", bcVersion)
	}
	if got := string(dst.files["/dst.rf"]); got != "old" {
		t.Fatalf("destination contents = %q, want old", got)
	}
	if dst.lastWriter == nil || dst.lastWriter.abortCalls != 1 {
		t.Fatalf("Abort calls = %d, want 1", dst.lastWriter.abortCalls)
	}
	if dst.lastWriter.stagePresent {
		t.Fatal("failed export copy left staged bytes behind")
	}
}

func TestCopyWithSHA256AbortsDestinationOnShortWrite(t *testing.T) {
	src := exportFileBackend{file: &exportReadFuncFile{
		size: 3,
		read: func(p []byte, off int64) (int, error) {
			switch off {
			case 0:
				copy(p, []byte("xyz"))
				return 3, io.EOF
			default:
				return 0, io.EOF
			}
		},
	}}
	dst := newExportTrackingBackend()
	dst.files["/dst.rf"] = []byte("old")
	dst.shortWrite = 1

	written, sum, bcVersion, err := copyWithSHA256(context.Background(), src, "/src.rf", dst, "/dst.rf")
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("copyWithSHA256 error = %v, want %v", err, io.ErrShortWrite)
	}
	if written != 1 {
		t.Fatalf("written = %d, want 1", written)
	}
	if sum != "" {
		t.Fatalf("sum = %q, want empty on failure", sum)
	}
	if bcVersion != "" {
		t.Fatalf("bcVersion = %q, want empty on failure", bcVersion)
	}
	if got := string(dst.files["/dst.rf"]); got != "old" {
		t.Fatalf("destination contents = %q, want old", got)
	}
	if dst.lastWriter == nil || dst.lastWriter.abortCalls != 1 {
		t.Fatalf("Abort calls = %d, want 1", dst.lastWriter.abortCalls)
	}
	if dst.lastWriter.closed {
		t.Fatal("short export write closed and could have committed destination")
	}
	if dst.lastWriter.stagePresent {
		t.Fatal("short export write left staged bytes behind")
	}
}

func TestCopyWithSHA256CountsPartialWriteProgressBeforeError(t *testing.T) {
	src := exportFileBackend{file: &exportReadFuncFile{
		size: 3,
		read: func(p []byte, off int64) (int, error) {
			switch off {
			case 0:
				copy(p, []byte("xyz"))
				return 3, io.EOF
			default:
				return 0, io.EOF
			}
		},
	}}
	dst := newExportTrackingBackend()
	dst.files["/dst.rf"] = []byte("old")
	dst.partialErrBytes = 2
	dst.writeErr = errors.New("write failed")

	written, sum, bcVersion, err := copyWithSHA256(context.Background(), src, "/src.rf", dst, "/dst.rf")
	if !errors.Is(err, dst.writeErr) {
		t.Fatalf("copyWithSHA256 error = %v, want %v", err, dst.writeErr)
	}
	if written != 2 {
		t.Fatalf("written = %d, want 2", written)
	}
	if sum != "" {
		t.Fatalf("sum = %q, want empty on failure", sum)
	}
	if bcVersion != "" {
		t.Fatalf("bcVersion = %q, want empty on failure", bcVersion)
	}
	if got := string(dst.files["/dst.rf"]); got != "old" {
		t.Fatalf("destination contents = %q, want old", got)
	}
	if dst.lastWriter == nil || dst.lastWriter.abortCalls != 1 {
		t.Fatalf("Abort calls = %d, want 1", dst.lastWriter.abortCalls)
	}
	if dst.lastWriter.closed {
		t.Fatal("partial error write closed and could have committed destination")
	}
	if dst.lastWriter.stagePresent {
		t.Fatal("partial error write left staged bytes behind")
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

type exportFileBackend struct {
	file storage.File
}

func (b exportFileBackend) Open(context.Context, string) (storage.File, error) {
	return b.file, nil
}

type exportReadFuncFile struct {
	size int64
	read func([]byte, int64) (int, error)
}

func (f *exportReadFuncFile) ReadAt(p []byte, off int64) (int, error) {
	return f.read(p, off)
}

func (*exportReadFuncFile) Close() error { return nil }
func (f *exportReadFuncFile) Size() int64 {
	return f.size
}

type exportTrackingBackend struct {
	files           map[string][]byte
	lastWriter      *exportTrackingWriter
	shortWrite      int
	partialErrBytes int
	writeErr        error
}

func newExportTrackingBackend() *exportTrackingBackend {
	return &exportTrackingBackend{files: make(map[string][]byte)}
}

func (b *exportTrackingBackend) Open(context.Context, string) (storage.File, error) {
	return nil, storage.ErrNotFound
}

func (b *exportTrackingBackend) Create(_ context.Context, path string) (storage.Writer, error) {
	writer := &exportTrackingWriter{
		backend:         b,
		path:            path,
		shortWrite:      b.shortWrite,
		partialErrBytes: b.partialErrBytes,
		writeErr:        b.writeErr,
	}
	b.lastWriter = writer
	return writer, nil
}

type exportTrackingWriter struct {
	backend         *exportTrackingBackend
	path            string
	stage           strings.Builder
	stagePresent    bool
	abortCalls      int
	closed          bool
	shortWrite      int
	partialErrBytes int
	writeErr        error
}

func (w *exportTrackingWriter) Write(p []byte) (int, error) {
	w.stagePresent = true
	if w.writeErr != nil {
		if w.partialErrBytes > 0 && len(p) > w.partialErrBytes {
			n, _ := w.stage.WriteString(string(p[:w.partialErrBytes]))
			return n, w.writeErr
		}
		return 0, w.writeErr
	}
	if w.shortWrite > 0 && len(p) > w.shortWrite {
		n, _ := w.stage.WriteString(string(p[:w.shortWrite]))
		return n, nil
	}
	return w.stage.WriteString(string(p))
}

func (w *exportTrackingWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.stagePresent = false
	w.backend.files[w.path] = []byte(w.stage.String())
	return nil
}

func (w *exportTrackingWriter) Abort() error {
	w.abortCalls++
	w.stagePresent = false
	w.stage.Reset()
	return nil
}
