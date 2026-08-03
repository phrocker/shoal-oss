package hdfs

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal/internal/storage"
)

func TestBackendOpenQualifiedPath(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("rfile")
	backend, err := New("hdfs://nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	f, err := backend.Open(context.Background(), "hdfs://nn:8020/tables/1.rf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got := make([]byte, f.Size())
	if _, err := f.ReadAt(got, 0); err != nil {
		t.Fatal(err)
	}
	if string(got) != "rfile" {
		t.Fatalf("got %q, want rfile", got)
	}
}

func TestBackendCreateReplacesAndCreatesParents(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("old")
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	w, err := backend.Create(context.Background(), "/tables/1.rf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if client.mkdir != "/tables" {
		t.Fatalf("MkdirAll path = %q, want /tables", client.mkdir)
	}
	if got := string(client.files["/tables/1.rf"]); got != "new" {
		t.Fatalf("created contents = %q, want new", got)
	}
}

func TestBackendCreateFailurePreservesExistingFile(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("old")
	client.failWriterClose = true
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	w, err := backend.Create(context.Background(), "/tables/1.rf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded, want injected failure")
	}
	if got := string(client.files["/tables/1.rf"]); got != "old" {
		t.Fatalf("existing contents = %q, want old", got)
	}
}

func TestBackendListPreservesPathForm(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("one")
	client.files["/tables/2.rf"] = []byte("two")
	client.dirs["/tables/nested"] = true
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	got, err := backend.List(context.Background(), "hdfs://nn:8020/tables")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"hdfs://nn:8020/tables/1.rf",
		"hdfs://nn:8020/tables/2.rf",
	}
	slices.Sort(got)
	slices.Sort(want)
	if len(got) != len(want) {
		t.Fatalf("List returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("List returned %v, want %v", got, want)
		}
	}
}

func TestBackendNotFoundSemantics(t *testing.T) {
	client := newFakeClient()
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := backend.Open(context.Background(), "/missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("Open error = %v, want storage.ErrNotFound", err)
	}
	if got, err := backend.List(context.Background(), "/missing"); err != nil || len(got) != 0 {
		t.Fatalf("List = %v, %v; want empty, nil", got, err)
	}
	if err := backend.Remove(context.Background(), "/missing"); err != nil {
		t.Fatalf("Remove missing path: %v", err)
	}
}

func TestBackendRejectsDifferentAuthority(t *testing.T) {
	backend, err := New("nn1:8020", WithClient(newFakeClient()))
	if err != nil {
		t.Fatal(err)
	}
	_, err = backend.Open(context.Background(), "hdfs://nn2:8020/tables/1.rf")
	if err == nil {
		t.Fatal("Open accepted a path for a different namenode")
	}
}

func TestBackendAcceptsAuthoritylessHDFSPaths(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("rfile")
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	for _, objectPath := range []string{"hdfs:/tables/1.rf", "hdfs:///tables/1.rf"} {
		f, err := backend.Open(context.Background(), objectPath)
		if err != nil {
			t.Fatalf("Open(%q): %v", objectPath, err)
		}
		_ = f.Close()
	}
}

func TestBackendRejectsOpaqueHDFSPath(t *testing.T) {
	backend, err := New("nn:8020", WithClient(newFakeClient()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Open(context.Background(), "hdfs:tables/1.rf"); err == nil {
		t.Fatal("Open accepted an opaque HDFS URI")
	}
}

func TestBackendListUsesRequestedAuthority(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("rfile")
	backend, err := New("", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	got, err := backend.List(context.Background(), "hdfs://configured-nn:8020/tables")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "hdfs://configured-nn:8020/tables/1.rf" {
		t.Fatalf("List returned %v", got)
	}
}

func TestFileSerializesConcurrentReadAt(t *testing.T) {
	reader := &overlapReader{}
	f := &file{reader: reader, size: 1}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 1)
			if _, err := f.ReadAt(buf, 0); err != nil {
				t.Errorf("ReadAt: %v", err)
			}
		}()
	}
	wg.Wait()

	if reader.overlapped.Load() {
		t.Fatal("underlying HDFS reader received concurrent ReadAt calls")
	}
}

type fakeClient struct {
	files           map[string][]byte
	dirs            map[string]bool
	mkdir           string
	failWriterClose bool
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (c *fakeClient) Open(name string) (Reader, error) {
	data, ok := c.files[name]
	if !ok {
		return nil, &os.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &fakeReader{
		Reader: bytes.NewReader(data),
		info:   fakeInfo{name: path.Base(name), size: int64(len(data))},
	}, nil
}

func (c *fakeClient) Create(name string) (storage.Writer, error) {
	return &fakeWriter{close: func(data []byte) {
		c.files[name] = append([]byte(nil), data...)
	}, failClose: c.failWriterClose}, nil
}

func (c *fakeClient) MkdirAll(dirname string, _ os.FileMode) error {
	c.mkdir = dirname
	c.dirs[dirname] = true
	return nil
}

func (c *fakeClient) ReadDir(dirname string) ([]os.FileInfo, error) {
	var out []os.FileInfo
	prefix := dirname + "/"
	for name, data := range c.files {
		if path.Dir(name) == dirname {
			out = append(out, fakeInfo{name: path.Base(name), size: int64(len(data))})
		}
	}
	for name := range c.dirs {
		if path.Dir(name) == dirname && name != dirname && len(name) > len(prefix) {
			out = append(out, fakeInfo{name: path.Base(name), dir: true})
		}
	}
	sortFileInfo(out)
	if len(out) == 0 {
		return nil, &os.PathError{Op: "readdir", Path: dirname, Err: fs.ErrNotExist}
	}
	return out, nil
}

func (c *fakeClient) Remove(name string) error {
	if _, ok := c.files[name]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(c.files, name)
	return nil
}

func (c *fakeClient) Rename(oldpath, newpath string) error {
	data, ok := c.files[oldpath]
	if !ok {
		return &os.PathError{Op: "rename", Path: oldpath, Err: fs.ErrNotExist}
	}
	if _, exists := c.files[newpath]; exists {
		return &os.PathError{Op: "rename", Path: newpath, Err: fs.ErrExist}
	}
	c.files[newpath] = data
	delete(c.files, oldpath)
	return nil
}

func (c *fakeClient) Close() error { return nil }

type fakeReader struct {
	*bytes.Reader
	info os.FileInfo
}

func (r *fakeReader) Close() error      { return nil }
func (r *fakeReader) Stat() os.FileInfo { return r.info }

type fakeWriter struct {
	bytes.Buffer
	close     func([]byte)
	failClose bool
}

func (w *fakeWriter) Close() error {
	if w.failClose {
		return errors.New("injected close failure")
	}
	w.close(w.Bytes())
	return nil
}

type fakeInfo struct {
	name string
	size int64
	dir  bool
}

func (i fakeInfo) Name() string { return i.name }
func (i fakeInfo) Size() int64  { return i.size }
func (i fakeInfo) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o755
	}
	return 0o644
}
func (i fakeInfo) ModTime() time.Time { return time.Time{} }
func (i fakeInfo) IsDir() bool        { return i.dir }
func (i fakeInfo) Sys() any           { return nil }

func sortFileInfo(entries []os.FileInfo) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && entries[j].Name() < entries[j-1].Name(); j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
}

type overlapReader struct {
	active     atomic.Int32
	overlapped atomic.Bool
}

func (r *overlapReader) ReadAt(p []byte, _ int64) (int, error) {
	if r.active.Add(1) != 1 {
		r.overlapped.Store(true)
	}
	time.Sleep(time.Millisecond)
	r.active.Add(-1)
	p[0] = 1
	return 1, nil
}

func (r *overlapReader) Close() error      { return nil }
func (r *overlapReader) Stat() os.FileInfo { return fakeInfo{name: "file", size: 1} }
