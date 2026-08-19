package local

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
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

func TestLocal_BackupRemovalFailureReturnsCommittedWriteError(t *testing.T) {
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
	removeErr := errors.New("injected backup removal failure")
	ops := &committedCleanupFailureOps{
		replacementOps: localWriter.ops,
		err:            removeErr,
	}
	localWriter.ops = ops
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if !errors.Is(err, removeErr) {
		t.Fatalf("Close error = %v, want %v", err, removeErr)
	}
	if !storage.IsCommittedWriteError(err) {
		t.Fatalf("Close error = %v, want committed-write marker", err)
	}
	if got := filepath.Dir(ops.backupPath); got != dir {
		t.Fatalf("backup directory = %s, want %s", got, dir)
	}
	if base := filepath.Base(ops.backupPath); !strings.HasPrefix(base, replacementBackupPrefix) {
		t.Fatalf("backup name = %q, want prefix %q", base, replacementBackupPrefix)
	} else if got, want := len(base), len(replacementBackupPrefix)+replacementNameTokenBytes*2; got != want {
		t.Fatalf("backup name length = %d, want %d", got, want)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Fatalf("target contents = %q, want committed replacement", got)
	}
	if got, err := os.ReadFile(ops.backupPath); err != nil || string(got) != "old" {
		t.Fatalf("backup contents = %q, %v; want old", got, err)
	}
	if err := w.(storage.Aborter).Abort(); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("Abort after committed cleanup failure = %v, want already closed", err)
	}
}

func TestLocal_CloseDurablySyncsReplacementBeforeAndAfterRename(t *testing.T) {
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
	ops := &durabilityRecordingOps{replacementOps: localWriter.ops}
	localWriter.ops = ops

	originalPreservePlatformMetadata := preservePlatformMetadataFn
	t.Cleanup(func() { preservePlatformMetadataFn = originalPreservePlatformMetadata })
	preservePlatformMetadataFn = func(_, _ string) error {
		ops.events = append(ops.events, "platform")
		return nil
	}

	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, want := ops.events, []string{"platform", "chmod", "sync-temp", "replace", "sync-parent", "remove-backup", "sync-parent"}; !equalStringSlices(got, want) {
		t.Fatalf("durability events = %v, want %v", got, want)
	}
}

func TestLocal_CloseParentSyncFailureAfterRenameReturnsCommittedWriteError(t *testing.T) {
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
	ops := &durabilityRecordingOps{
		replacementOps:      localWriter.ops,
		syncParentFailAfter: 1,
		syncParentErr:       errors.New("directory sync failed"),
	}
	localWriter.ops = ops

	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	err = w.Close()
	if !errors.Is(err, ops.syncParentErr) {
		t.Fatalf("Close error = %v, want %v", err, ops.syncParentErr)
	}
	if !storage.IsCommittedWriteError(err) {
		t.Fatalf("Close error = %v, want committed-write marker", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "new" {
		t.Fatalf("target contents = %q, %v; want committed replacement", got, err)
	}
	if err := w.(storage.Aborter).Abort(); err == nil || !strings.Contains(err.Error(), "already closed") {
		t.Fatalf("Abort after committed parent-sync failure = %v, want already closed", err)
	}
}

func TestLocal_LongNameTargetsUseFixedLengthSiblingArtifacts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("long NAME_MAX-style filename regression is not portable on Windows")
	}

	dir := t.TempDir()
	checkTemp := func(t *testing.T, w storage.Writer) {
		t.Helper()
		localWriter := w.(*writer)
		if got := filepath.Dir(localWriter.temp); got != dir {
			t.Fatalf("temporary directory = %s, want %s", got, dir)
		}
		if base := filepath.Base(localWriter.temp); !strings.HasPrefix(base, replacementTempPrefix) {
			t.Fatalf("temporary name = %q, want prefix %q", base, replacementTempPrefix)
		} else if got, want := len(base), len(replacementTempPrefix)+replacementNameTokenBytes*2; got != want {
			t.Fatalf("temporary name length = %d, want %d", got, want)
		}
	}

	createPath := filepath.Join(dir, strings.Repeat("c", 255))
	w, err := New().Create(context.Background(), createPath)
	if err != nil {
		t.Fatal(err)
	}
	checkTemp(t, w)
	if _, err := w.Write([]byte("create")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(createPath); err != nil || string(got) != "create" {
		t.Fatalf("created file = %q, %v; want create", got, err)
	}

	replacePath := filepath.Join(dir, strings.Repeat("r", 255))
	if err := os.WriteFile(replacePath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err = New().Create(context.Background(), replacePath)
	if err != nil {
		t.Fatal(err)
	}
	checkTemp(t, w)
	if _, err := w.Write([]byte("replace")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(replacePath); err != nil || string(got) != "replace" {
		t.Fatalf("replaced file = %q, %v; want replace", got, err)
	}

	abortPath := filepath.Join(dir, strings.Repeat("a", 255))
	w, err = New().Create(context.Background(), abortPath)
	if err != nil {
		t.Fatal(err)
	}
	checkTemp(t, w)
	if _, err := w.Write([]byte("abort")); err != nil {
		t.Fatal(err)
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(abortPath); !os.IsNotExist(err) {
		t.Fatalf("aborted target exists: %v", err)
	}
	if matches, err := filepath.Glob(filepath.Join(dir, ".shoal-*")); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("replacement artifacts remain: %v", matches)
	}
}

func TestLocal_TemporaryNameCollisionRetriesAndAbortCleansAllocatedSibling(t *testing.T) {
	dir := t.TempDir()
	collision := filepath.Join(dir, replacementTempPrefix+"collision")
	if err := os.WriteFile(collision, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	setReplacementNameTokens(t, "collision", "allocated")

	w, err := New().Create(context.Background(), filepath.Join(dir, "target"))
	if err != nil {
		t.Fatal(err)
	}
	temp := w.(*writer).temp
	if got := filepath.Base(temp); got != replacementTempPrefix+"allocated" {
		t.Fatalf("temporary sibling = %q, want allocated retry", got)
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(collision); err != nil || string(got) != "sentinel" {
		t.Fatalf("colliding file = %q, %v; want preserved sentinel", got, err)
	}
	if _, err := os.Stat(temp); !os.IsNotExist(err) {
		t.Fatalf("allocated temporary sibling remains after abort: %v", err)
	}
}

func TestLocal_BackupNameCollisionRetriesAndCleansAllocatedBackup(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	collision := filepath.Join(dir, replacementBackupPrefix+"collision")
	if err := os.WriteFile(collision, []byte("sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	setReplacementNameTokens(t, "temporary", "collision", "allocated")

	w, err := New().Create(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	localWriter := w.(*writer)
	ops := &collisionOnceAtomicReplaceOps{replacementOps: localWriter.ops}
	localWriter.ops = ops
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(target); err != nil || string(got) != "new" {
		t.Fatalf("target = %q, %v; want new", got, err)
	}
	if got, err := os.ReadFile(collision); err != nil || string(got) != "sentinel" {
		t.Fatalf("colliding backup = %q, %v; want preserved sentinel", got, err)
	}
	if len(ops.backups) != 2 {
		t.Fatalf("backup attempts = %v, want collision and allocated", ops.backups)
	}
	if got := filepath.Base(ops.backups[1]); got != replacementBackupPrefix+"allocated" {
		t.Fatalf("second backup = %q, want allocated retry", got)
	}
	if _, err := os.Stat(ops.backups[1]); !os.IsNotExist(err) {
		t.Fatalf("allocated backup remains after commit: %v", err)
	}
}

func setReplacementNameTokens(t *testing.T, tokens ...string) {
	t.Helper()
	original := randomReplacementNameToken
	index := 0
	randomReplacementNameToken = func() (string, error) {
		if index >= len(tokens) {
			return tokens[len(tokens)-1], nil
		}
		token := tokens[index]
		index++
		return token, nil
	}
	t.Cleanup(func() {
		randomReplacementNameToken = original
	})
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

func TestCleanupTemporaryCreateFileJoinsCloseAndRemoveFailures(t *testing.T) {
	file := fakeNamedCloser{name: "temp", closeErr: errors.New("close failed")}
	ops := cleanupFailureOps{removeErr: errors.New("remove failed")}

	err := cleanupTemporaryCreateFile(file, ops)
	if err == nil || !strings.Contains(err.Error(), "close temporary file temp") || !strings.Contains(err.Error(), "remove temporary file temp") {
		t.Fatalf("cleanupTemporaryCreateFile error = %v, want joined close/remove failures", err)
	}
}

func TestCleanupTemporaryCreateFileIgnoresNotFoundRemoval(t *testing.T) {
	file := fakeNamedCloser{name: "temp"}
	ops := cleanupFailureOps{removeErr: fs.ErrNotExist}

	if err := cleanupTemporaryCreateFile(file, ops); err != nil {
		t.Fatalf("cleanupTemporaryCreateFile: %v", err)
	}
}

func TestLocal_WriterSyncForwardsToTemporaryFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.bin")
	w, err := New().Create(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	sy, ok := w.(interface{ Sync() error })
	if !ok {
		t.Fatal("Create writer does not implement Sync")
	}
	if _, err := w.Write([]byte("new")); err != nil {
		t.Fatal(err)
	}
	if err := sy.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	localWriter := w.(*writer)
	if err := localWriter.file.Close(); err != nil {
		t.Fatal(err)
	}
	localWriter.fileClosed = true
	if err := sy.Sync(); err == nil || !strings.Contains(err.Error(), "sync temporary file") {
		t.Fatalf("Sync after underlying file close error = %v, want sync temporary file failure", err)
	}
	if err := w.(storage.Aborter).Abort(); err != nil {
		t.Fatalf("Abort after Sync regression: %v", err)
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

func TestPreserveExistingMetadataStripsPrivilegeBitsButPreservesStickyAfterPlatformMetadata(t *testing.T) {
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
	wantMode := os.ModeSticky | 0o640
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

type fakeNamedCloser struct {
	name     string
	closeErr error
}

func (f fakeNamedCloser) Name() string { return f.name }
func (f fakeNamedCloser) Close() error { return f.closeErr }

type cleanupFailureOps struct {
	removeErr error
}

func (cleanupFailureOps) Lstat(string) (os.FileInfo, error)                { return nil, fs.ErrNotExist }
func (cleanupFailureOps) Chmod(string, os.FileMode) error                  { return nil }
func (o cleanupFailureOps) Remove(string) error                            { return o.removeErr }
func (cleanupFailureOps) AtomicReplace(string, string, string, bool) error { return nil }
func (cleanupFailureOps) AtomicRestore(string, string) error               { return nil }

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

type collisionOnceAtomicReplaceOps struct {
	replacementOps
	backups []string
}

func (o *collisionOnceAtomicReplaceOps) AtomicReplace(temp, target, backup string, hadOld bool) error {
	o.backups = append(o.backups, backup)
	if len(o.backups) == 1 {
		return fs.ErrExist
	}
	return o.replacementOps.AtomicReplace(temp, target, backup, hadOld)
}

type blockingRollbackOps struct {
	replacementOps
	err        error
	entered    chan struct{}
	release    chan struct{}
	backupPath string
}

func (o *blockingRollbackOps) Remove(name string) error {
	if strings.Contains(name, replacementBackupPrefix) {
		o.backupPath = name
		return o.err
	}
	return o.replacementOps.Remove(name)
}

func (o *blockingRollbackOps) AtomicReplace(temp, target, backup string, hadOld bool) error {
	o.backupPath = backup
	return o.replacementOps.AtomicReplace(temp, target, backup, hadOld)
}

func (o *blockingRollbackOps) AtomicRestore(target, backup string) error {
	close(o.entered)
	<-o.release
	return o.replacementOps.AtomicRestore(target, backup)
}

type committedCleanupFailureOps struct {
	replacementOps
	err        error
	backupPath string
}

func (o *committedCleanupFailureOps) Remove(name string) error {
	if strings.Contains(name, replacementBackupPrefix) {
		o.backupPath = name
		return o.err
	}
	return o.replacementOps.Remove(name)
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

type durabilityRecordingOps struct {
	replacementOps
	events              []string
	syncParentCalls     int
	syncParentFailAfter int
	syncParentErr       error
}

func (o *durabilityRecordingOps) Chmod(name string, mode os.FileMode) error {
	o.events = append(o.events, "chmod")
	return o.replacementOps.Chmod(name, mode)
}

func (o *durabilityRecordingOps) AtomicReplace(temp, target, backup string, hadOld bool) error {
	o.events = append(o.events, "replace")
	return o.replacementOps.AtomicReplace(temp, target, backup, hadOld)
}

func (o *durabilityRecordingOps) Remove(name string) error {
	if strings.Contains(name, replacementBackupPrefix) {
		o.events = append(o.events, "remove-backup")
	}
	return o.replacementOps.Remove(name)
}

func (o *durabilityRecordingOps) SyncPath(string) error {
	o.events = append(o.events, "sync-temp")
	return nil
}

func (o *durabilityRecordingOps) SyncParent(string) error {
	o.events = append(o.events, "sync-parent")
	o.syncParentCalls++
	if o.syncParentFailAfter > 0 && o.syncParentCalls >= o.syncParentFailAfter {
		return o.syncParentErr
	}
	return nil
}

type stubFileInfo struct {
	mode os.FileMode
}

func (i stubFileInfo) Name() string       { return "target" }
func (i stubFileInfo) Size() int64        { return 0 }
func (i stubFileInfo) Mode() os.FileMode  { return i.mode }
func (i stubFileInfo) ModTime() time.Time { return time.Time{} }
func (i stubFileInfo) IsDir() bool        { return i.mode.IsDir() }
func (i stubFileInfo) Sys() any           { return nil }

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
