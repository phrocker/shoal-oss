package hdfs

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hdfsclient "github.com/colinmarc/hdfs/v2"
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

func TestBackendCreateReportsPublishAndRestoreFailures(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("old")
	client.failPublish = true
	client.failRestore = true
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
	err = w.Close()
	if err == nil {
		t.Fatal("Close succeeded, want publish and restore failures")
	}
	if !strings.Contains(err.Error(), "publish /tables/1.rf") {
		t.Fatalf("Close error %q does not include publish failure", err)
	}
	if !strings.Contains(err.Error(), "restore existing file /tables/1.rf") {
		t.Fatalf("Close error %q does not include restore failure", err)
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
	if client.writerCloseCalls != 1 {
		t.Fatalf("writer Close calls = %d, want 1", client.writerCloseCalls)
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

func TestBackendCreateRetriesReplicationInProgress(t *testing.T) {
	client := newFakeClient()
	client.replicatingCloseFailures = 2
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

	if client.writerCloseCalls != 3 {
		t.Fatalf("writer Close calls = %d, want 3", client.writerCloseCalls)
	}
	if got := string(client.files["/tables/1.rf"]); got != "new" {
		t.Fatalf("created contents = %q, want new", got)
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

func TestBackendRejectsQualifiedPathWithoutConfiguredNamenode(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("rfile")
	backend, err := New("", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := backend.List(context.Background(), "hdfs://configured-nn:8020/tables"); err == nil {
		t.Fatal("List accepted a qualified path without a configured namenode")
	}
}

func TestAddressFromPath(t *testing.T) {
	valid := []struct {
		path string
		want string
	}{
		{path: "hdfs://nn:8020/tables/1.rf", want: "nn:8020"},
		{path: "hdfs:/tables/1.rf", want: ""},
		{path: "hdfs:///tables/1.rf", want: ""},
	}
	for _, test := range valid {
		got, err := AddressFromPath(test.path)
		if err != nil {
			t.Fatalf("AddressFromPath(%q): %v", test.path, err)
		}
		if got != test.want {
			t.Fatalf("AddressFromPath(%q) = %q, want %q", test.path, got, test.want)
		}
	}

	invalid := []string{
		"https://nn:8020/tables/1.rf",
		"hdfs:tables/1.rf",
		"hdfs://nn:8020/tables/1.rf?version=1",
		"hdfs://nn:8020/tables/1.rf#fragment",
	}
	for _, objectPath := range invalid {
		if _, err := AddressFromPath(objectPath); err == nil {
			t.Fatalf("AddressFromPath(%q) succeeded, want error", objectPath)
		}
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

func TestNewContextPassesContextToNamenodeDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sawCanceled := false
	_, err := NewContext(ctx, "nn:8020", WithClientOptions(hdfsclient.ClientOptions{
		User: "shoal-test",
		NamenodeDialFunc: func(dialCtx context.Context, network, addr string) (net.Conn, error) {
			sawCanceled = errors.Is(dialCtx.Err(), context.Canceled)
			return nil, dialCtx.Err()
		},
	}))
	if err == nil {
		t.Fatal("NewContext succeeded, want dial failure")
	}
	if !sawCanceled {
		t.Fatal("NewContext did not pass the canceled context into the namenode dialer")
	}
}

func TestBackendOpenAppliesContextDeadline(t *testing.T) {
	client := newFakeClient()
	client.files["/tables/1.rf"] = []byte("rfile")
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	f, err := backend.Open(ctx, "/tables/1.rf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if client.lastReader == nil {
		t.Fatal("Open did not record a reader")
	}
	if !client.lastReader.deadline.Equal(deadline) {
		t.Fatalf("reader deadline = %v, want %v", client.lastReader.deadline, deadline)
	}
}

func TestBackendCreateAppliesContextDeadline(t *testing.T) {
	client := newFakeClient()
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	w, err := backend.Create(ctx, "/tables/1.rf")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if client.lastWriter == nil {
		t.Fatal("Create did not record a writer")
	}
	if !client.lastWriter.deadline.Equal(deadline) {
		t.Fatalf("writer deadline = %v, want %v", client.lastWriter.deadline, deadline)
	}
}

func TestBackendCreateStopsReplicationRetryOnContextDeadline(t *testing.T) {
	client := newFakeClient()
	client.replicatingCloseFailures = 1000
	backend, err := New("nn:8020", WithClient(client))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	w, err := backend.Create(ctx, "/tables/1.rf")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want context deadline exceeded", err)
	}
	if client.writerCloseCalls == 0 {
		t.Fatal("Close never retried the temporary file writer")
	}
}

func TestBackendAbortPreservesExistingTargetAndRemovesTemp(t *testing.T) {
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
	aborter, ok := w.(storage.Aborter)
	if !ok {
		t.Fatal("Create writer does not implement storage.Aborter")
	}
	if err := aborter.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := aborter.Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}

	if got := string(client.files["/tables/1.rf"]); got != "old" {
		t.Fatalf("existing contents = %q, want old", got)
	}
	if client.lastCreatePath == "" {
		t.Fatal("Create did not record the temporary path")
	}
	if _, ok := client.files[client.lastCreatePath]; ok {
		t.Fatalf("temporary file %s still exists after abort", client.lastCreatePath)
	}
	if client.writerCloseCalls != 1 {
		t.Fatalf("writer Close calls = %d, want 1", client.writerCloseCalls)
	}
	if !slices.Contains(client.removeCalls, client.lastCreatePath) {
		t.Fatalf("Remove calls = %v, want %s", client.removeCalls, client.lastCreatePath)
	}
	if len(client.renameCalls) != 0 {
		t.Fatalf("Abort must not rename files, got %v", client.renameCalls)
	}
}

func TestBackendAbortReportsCleanupFailure(t *testing.T) {
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
	client.failRemovePath = client.lastCreatePath
	client.failRemoveErr = errors.New("injected remove failure")

	aborter := w.(storage.Aborter)
	err = aborter.Abort()
	if err == nil {
		t.Fatal("Abort succeeded, want cleanup failure")
	}
	if !strings.Contains(err.Error(), "remove temporary file") {
		t.Fatalf("Abort error = %v, want remove failure", err)
	}
	if got := string(client.files["/tables/1.rf"]); got != "old" {
		t.Fatalf("existing contents = %q, want old", got)
	}
	if len(client.renameCalls) != 0 {
		t.Fatalf("Abort must not rename files, got %v", client.renameCalls)
	}
}

type fakeClient struct {
	files                    map[string][]byte
	dirs                     map[string]bool
	mkdir                    string
	failWriterClose          bool
	failPublish              bool
	failRestore              bool
	replicatingCloseFailures int
	writerCloseCalls         int
	lastReader               *fakeReader
	lastWriter               *fakeWriter
	lastCreatePath           string
	removeCalls              []string
	renameCalls              []renameCall
	failRemovePath           string
	failRemoveErr            error
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
	reader := &fakeReader{
		Reader: bytes.NewReader(data),
		info:   fakeInfo{name: path.Base(name), size: int64(len(data))},
	}
	c.lastReader = reader
	return reader, nil
}

func (c *fakeClient) Create(name string) (storage.Writer, error) {
	writer := &fakeWriter{close: func(data []byte) {
		c.files[name] = append([]byte(nil), data...)
	}, client: c, failClose: c.failWriterClose}
	c.lastWriter = writer
	c.lastCreatePath = name
	return writer, nil
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
	c.removeCalls = append(c.removeCalls, name)
	if c.failRemovePath != "" && name == c.failRemovePath && c.failRemoveErr != nil {
		return &os.PathError{Op: "remove", Path: name, Err: c.failRemoveErr}
	}
	if _, ok := c.files[name]; !ok {
		return &os.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(c.files, name)
	return nil
}

func (c *fakeClient) Rename(oldpath, newpath string) error {
	c.renameCalls = append(c.renameCalls, renameCall{oldpath: oldpath, newpath: newpath})
	if c.failPublish && strings.Contains(oldpath, ".shoal-tmp-") {
		return &os.PathError{Op: "rename", Path: oldpath, Err: errors.New("injected publish failure")}
	}
	if c.failRestore && strings.Contains(oldpath, ".shoal-backup-") {
		return &os.PathError{Op: "rename", Path: oldpath, Err: errors.New("injected restore failure")}
	}
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
	info     os.FileInfo
	deadline time.Time
}

func (r *fakeReader) Close() error      { return nil }
func (r *fakeReader) Stat() os.FileInfo { return r.info }
func (r *fakeReader) SetDeadline(t time.Time) error {
	r.deadline = t
	return nil
}

type fakeWriter struct {
	bytes.Buffer
	close     func([]byte)
	client    *fakeClient
	failClose bool
	deadline  time.Time
}

func (w *fakeWriter) Close() error {
	w.client.writerCloseCalls++
	if w.client.replicatingCloseFailures > 0 {
		w.client.replicatingCloseFailures--
		return &os.PathError{Op: "create", Path: "temporary", Err: hdfsclient.ErrReplicating}
	}
	if w.failClose {
		return errors.New("injected close failure")
	}
	w.close(w.Bytes())
	return nil
}

func (w *fakeWriter) SetDeadline(t time.Time) error {
	w.deadline = t
	return nil
}

type renameCall struct {
	oldpath string
	newpath string
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
