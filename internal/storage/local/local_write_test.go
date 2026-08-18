package local

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal/internal/storage"
)

func TestLocal_CreateAndReadBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	be := New()

	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("hello, write side")
	if _, err := w.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// File now exists with the bytes we wrote.
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Errorf("disk content = %q, want %q", got, body)
	}
}

func TestLocal_CreateMakesParentDirs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep", "nested", "tree", "out.bin")
	be := New()

	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("nested")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("Create did not produce file: %v", err)
	}
}

func TestLocal_CreateReplacesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.bin")
	if err := os.WriteFile(path, []byte("old contents that are longer"), 0o644); err != nil {
		t.Fatal(err)
	}

	be := New()
	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("got %q, want \"new\" (Create should truncate)", got)
	}
}

func TestLocal_CreateUsesOpenFileModeSubjectToUmask(t *testing.T) {
	dir := t.TempDir()
	referencePath := filepath.Join(dir, "reference")
	reference, err := os.OpenFile(referencePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := reference.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "created")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	referenceInfo, err := os.Stat(referencePath)
	if err != nil {
		t.Fatal(err)
	}
	createdInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := createdInfo.Mode().Perm(), referenceInfo.Mode().Perm(); got != want {
		t.Fatalf("created mode = %04o, want OpenFile(0644) mode %04o", got, want)
	}
}

func TestLocal_CreateReplacementPreservesExistingMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "replaced")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	replacedInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := replacedInfo.Mode().Perm(), originalInfo.Mode().Perm(); got != want {
		t.Fatalf("replacement mode = %04o, want existing mode %04o", got, want)
	}
}

func TestLocal_ReplacementKeepsTargetVisibleUntilAtomicPublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	localWriter := w.(*writer)
	blockingOps := &blockingAtomicReplaceOps{
		replacementOps: localWriter.ops,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	localWriter.ops = blockingOps
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- w.Close() }()
	<-blockingOps.entered
	for range 100 {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("observer saw target missing before publish: %v", err)
		}
		if string(got) != "old" {
			t.Fatalf("observer saw %q before atomic publish, want old", got)
		}
	}
	close(blockingOps.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("target contents = %q, want new", got)
	}
}

func TestLocal_AtomicReplaceFailureLeavesOldTargetIntact(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	localWriter := w.(*writer)
	replaceErr := errors.New("injected atomic replace failure")
	localWriter.ops = atomicReplaceErrorOps{replacementOps: localWriter.ops, err: replaceErr}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); !errors.Is(err, replaceErr) {
		t.Fatalf("Close error = %v, want %v", err, replaceErr)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("target contents = %q, want old", got)
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestLocal_CreateRejectsDirectoryTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err == nil || !strings.Contains(err.Error(), "not a regular file") {
		if w != nil {
			_ = w.(storage.Aborter).Abort()
		}
		t.Fatalf("Create error = %v, want non-regular target rejection", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("target mode = %v, want directory preserved", info.Mode())
	}
}

func TestLocal_BackupRemovalFailureRollsBackReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	localWriter := w.(*writer)
	removeErr := errors.New("injected backup removal failure")
	rollbackOps := &blockingRollbackOps{
		replacementOps: localWriter.ops,
		err:            removeErr,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	localWriter.ops = rollbackOps
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- w.Close() }()
	<-rollbackOps.entered
	for range 100 {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("observer saw target missing before rollback: %v", err)
		}
		if string(got) != "new" {
			t.Fatalf("observer saw %q before atomic rollback, want new", got)
		}
	}
	close(rollbackOps.release)
	err = <-done
	if !errors.Is(err, removeErr) {
		t.Fatalf("Close error = %v, want %v", err, removeErr)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("target contents = %q, want original data", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if gotMode, wantMode := info.Mode().Perm(), originalInfo.Mode().Perm(); gotMode != wantMode {
		t.Fatalf("target mode = %04o, want original mode %04o", gotMode, wantMode)
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatalf("Abort after rollback: %v", err)
	}
	if matches, err := filepath.Glob(path + ".shoal-*"); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("replacement artifacts remain: %v", matches)
	}
}

func TestPlatformAtomicReplaceAndRestore(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	temp := filepath.Join(dir, "temp")
	backup := filepath.Join(dir, "backup")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(temp, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}

	ops := osReplacementOps{}
	if err := ops.AtomicReplace(temp, target, backup, true); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "new" {
		t.Fatalf("published target = %q, %v; want new", got, err)
	}
	if got, err := os.ReadFile(backup); err != nil || string(got) != "old" {
		t.Fatalf("backup = %q, %v; want old", got, err)
	}
	if err := ops.AtomicRestore(target, backup); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "old" {
		t.Fatalf("restored target = %q, %v; want old", got, err)
	}
	if _, err := os.Stat(backup); !os.IsNotExist(err) {
		t.Fatalf("backup remains after restore: %v", err)
	}
}

func TestLocal_AbortPreservesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.bin")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	be := New()
	w, err := be.Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	aborter, ok := w.(storage.Aborter)
	if !ok {
		t.Fatal("writer does not implement storage.Aborter")
	}
	if err := aborter.Abort(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Fatalf("got %q, want original file preserved", got)
	}
}

func TestLocal_AbortAfterFailedCloseRemovesTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	localWriter := w.(*writer)
	temp := localWriter.temp
	if err := localWriter.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after the temporary file was closed")
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatalf("Abort after failed Close: %v", err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists after Abort: %v", err)
	}
}

func TestLocal_CloseErrorCleansUpTempViaReplacementOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	localWriter := w.(*writer)
	ops := &recordingRemoveOps{replacementOps: localWriter.ops}
	localWriter.ops = ops
	temp := localWriter.temp
	if err := localWriter.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after the temporary file was already closed")
	}
	if len(ops.removed) != 1 || ops.removed[0] != temp {
		t.Fatalf("Remove calls = %v, want [%s]", ops.removed, temp)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists after Close cleanup: %v", err)
	}
}

func TestLocal_AbortRetriesTempRemovalAfterFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}

	localWriter := w.(*writer)
	ops := &flakyRemoveOps{
		replacementOps: localWriter.ops,
		failPath:       localWriter.temp,
		failures:       1,
		err:            errors.New("transient remove failure"),
	}
	localWriter.ops = ops

	aborter := w.(storage.Aborter)
	err = aborter.Abort()
	if err == nil || !strings.Contains(err.Error(), "remove temporary file") {
		t.Fatalf("Abort error = %v, want remove temporary file failure", err)
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "writer already aborted") {
		t.Fatalf("Close after failed Abort error = %v, want writer already aborted", err)
	}

	if err := aborter.Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if ops.removeAttempts != 2 {
		t.Fatalf("Remove attempts = %d, want 2", ops.removeAttempts)
	}
	if _, err := os.Stat(localWriter.temp); !os.IsNotExist(err) {
		t.Fatalf("temporary file still exists after retry: %v", err)
	}
}

func TestPreserveExistingMetadataPreservesSpecialModeBitsAfterPlatformMetadata(t *testing.T) {
	originalPreservePlatformMetadata := preservePlatformMetadataFn
	t.Cleanup(func() {
		preservePlatformMetadataFn = originalPreservePlatformMetadata
	})

	var calls []string
	preservePlatformMetadataFn = func(_, _ string) error {
		calls = append(calls, "platform")
		return nil
	}

	ops := &metadataRecordingOps{
		infos: map[string]os.FileInfo{
			"target": stubFileInfo{mode: os.ModeSetuid | os.ModeSetgid | os.ModeSticky | 0o640},
		},
		onChmod: func() {
			calls = append(calls, "chmod")
		},
	}

	hadOld, err := preserveExistingMetadata(ops, "temp", "target")
	if err != nil {
		t.Fatal(err)
	}
	if !hadOld {
		t.Fatal("preserveExistingMetadata reported no existing target")
	}
	if len(ops.chmodModes) != 1 {
		t.Fatalf("Chmod calls = %d, want 1", len(ops.chmodModes))
	}
	wantMode := os.ModeSetuid | os.ModeSetgid | os.ModeSticky | 0o640
	if ops.chmodModes[0] != wantMode {
		t.Fatalf("Chmod mode = %v, want %v", ops.chmodModes[0], wantMode)
	}
	if len(calls) != 2 || calls[0] != "platform" || calls[1] != "chmod" {
		t.Fatalf("metadata call order = %v, want [platform chmod]", calls)
	}
}

type blockingAtomicReplaceOps struct {
	replacementOps
	entered chan struct{}
	release chan struct{}
}

func (o *blockingAtomicReplaceOps) AtomicReplace(temp, target, backup string, hadOld bool) error {
	close(o.entered)
	<-o.release
	return o.replacementOps.AtomicReplace(temp, target, backup, hadOld)
}

type atomicReplaceErrorOps struct {
	replacementOps
	err error
}

func (o atomicReplaceErrorOps) AtomicReplace(string, string, string, bool) error {
	return o.err
}

type blockingRollbackOps struct {
	replacementOps
	err     error
	entered chan struct{}
	release chan struct{}
}

func (o *blockingRollbackOps) Remove(name string) error {
	if strings.Contains(name, ".shoal-backup-") {
		return o.err
	}
	return o.replacementOps.Remove(name)
}

func (o *blockingRollbackOps) AtomicRestore(target, backup string) error {
	close(o.entered)
	<-o.release
	return o.replacementOps.AtomicRestore(target, backup)
}

type recordingRemoveOps struct {
	replacementOps
	removed []string
}

func (o *recordingRemoveOps) Remove(name string) error {
	o.removed = append(o.removed, name)
	return o.replacementOps.Remove(name)
}

type flakyRemoveOps struct {
	replacementOps
	failPath       string
	failures       int
	err            error
	removeAttempts int
}

func (o *flakyRemoveOps) Remove(name string) error {
	o.removeAttempts++
	if name == o.failPath && o.failures > 0 {
		o.failures--
		return o.err
	}
	return o.replacementOps.Remove(name)
}

type metadataRecordingOps struct {
	infos      map[string]os.FileInfo
	chmodModes []os.FileMode
	onChmod    func()
}

func (o *metadataRecordingOps) Lstat(name string) (os.FileInfo, error) {
	info, ok := o.infos[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return info, nil
}

func (o *metadataRecordingOps) Chmod(_ string, mode os.FileMode) error {
	if o.onChmod != nil {
		o.onChmod()
	}
	o.chmodModes = append(o.chmodModes, mode)
	return nil
}

func (*metadataRecordingOps) Remove(string) error                              { return nil }
func (*metadataRecordingOps) AtomicReplace(string, string, string, bool) error { return nil }
func (*metadataRecordingOps) AtomicRestore(string, string) error               { return nil }

type stubFileInfo struct {
	mode os.FileMode
}

func (i stubFileInfo) Name() string       { return "target" }
func (i stubFileInfo) Size() int64        { return 0 }
func (i stubFileInfo) Mode() os.FileMode  { return i.mode }
func (i stubFileInfo) ModTime() time.Time { return time.Time{} }
func (i stubFileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i stubFileInfo) Sys() any           { return nil }
