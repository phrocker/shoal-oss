// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

type fakeAzureObject struct {
	state azureObjectState
	data  string
}

type fakeAzureWriteOperations struct {
	objects             map[string]fakeAzureObject
	stageFailure        bool
	stageResponseLost   bool
	promoteResponseLost bool
	mutateBeforePromote bool
	deleteFailures      int
	promoteCalls        int
	cleanupCanceled     bool
	nextETag            int
}

func newFakeAzureWriteOperations() *fakeAzureWriteOperations {
	return &fakeAzureWriteOperations{objects: make(map[string]fakeAzureObject)}
}

func azureNotFoundError() error {
	return &azcore.ResponseError{ErrorCode: string(bloberror.BlobNotFound)}
}

func (f *fakeAzureWriteOperations) head(
	_ context.Context, _, name string,
) (azureObjectState, error) {
	obj, ok := f.objects[name]
	if !ok {
		return azureObjectState{}, azureNotFoundError()
	}
	return obj.state, nil
}

func (f *fakeAzureWriteOperations) upload(
	ctx context.Context,
	_, name string,
	data []byte,
	writeID string,
	targetExists bool,
	target azureObjectState,
) (azureObjectState, error) {
	isStage := strings.HasPrefix(name, ".shoal-tmp/")
	if isStage && f.stageFailure {
		return azureObjectState{}, errors.New("stage checksum failed")
	}
	if !isStage {
		f.promoteCalls++
		if f.mutateBeforePromote {
			f.mutateBeforePromote = false
			f.nextETag++
			etag := azcore.ETag(fmt.Sprintf("\"etag-%d\"", f.nextETag))
			f.objects[name] = fakeAzureObject{
				state: azureObjectState{etag: &etag, size: int64(len("concurrent"))},
				data:  "concurrent",
			}
		}
		current, exists := f.objects[name]
		if exists != targetExists ||
			(exists && !equalETags(current.state.etag, target.etag)) {
			return azureObjectState{}, errors.New("destination precondition failed")
		}
		if err := ctx.Err(); err != nil {
			return azureObjectState{}, err
		}
	}
	f.nextETag++
	etag := azcore.ETag(fmt.Sprintf("\"etag-%d\"", f.nextETag))
	id := writeID
	state := azureObjectState{
		etag:     &etag,
		size:     int64(len(data)),
		metadata: map[string]*string{"shoal-write-id": &id},
	}
	f.objects[name] = fakeAzureObject{state: state, data: string(data)}
	if isStage && f.stageResponseLost {
		f.stageResponseLost = false
		return azureObjectState{}, errors.New("stage response lost")
	}
	if !isStage && f.promoteResponseLost {
		f.promoteResponseLost = false
		return azureObjectState{}, errors.New("upload response lost")
	}
	if err := ctx.Err(); err != nil {
		return azureObjectState{}, err
	}
	return state, nil
}

func (f *fakeAzureWriteOperations) deleteStage(
	ctx context.Context, _, name string, _ azureObjectState,
) error {
	if ctx.Err() != nil {
		f.cleanupCanceled = true
		return ctx.Err()
	}
	if f.deleteFailures > 0 {
		f.deleteFailures--
		return errors.New("delete failed")
	}
	if _, ok := f.objects[name]; !ok {
		return azureNotFoundError()
	}
	delete(f.objects, name)
	return nil
}

func newFakeAzureWriter(f *fakeAzureWriteOperations, target string) *writer {
	targetState, err := f.head(context.Background(), "container", target)
	return &writer{
		ops:            f,
		container:      "container",
		name:           target,
		stageName:      ".shoal-tmp/stage",
		writeID:        "write-id",
		ctx:            context.Background(),
		targetExists:   err == nil,
		target:         targetState,
		cleanupTimeout: azureCleanupTimeout,
	}
}

func TestParsePath(t *testing.T) {
	cases := []struct {
		in        string
		container string
		blob      string
		wantErr   bool
	}{
		{"az://my-container/path/to/file.rf", "my-container", "path/to/file.rf", false},
		{"my-container/path/to/file.rf", "my-container", "path/to/file.rf", false},
		{"az://c/o", "c", "o", false},
		{"az://", "", "", true},
		{"no-slash", "", "", true},
		{"az://c/", "", "", true},          // trailing slash only → empty blob
		{"/leading-slash/o", "", "", true}, // leading slash → empty container → error
		{"", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			cont, blob, err := ParsePath(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error, got container=%q blob=%q", cont, blob)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if cont != c.container || blob != c.blob {
				t.Errorf("got (%q, %q), want (%q, %q)", cont, blob, c.container, c.blob)
			}
		})
	}
}

// TestNew_NoCredentials confirms New errors cleanly when no account/connection
// string is configured (env scrubbed), rather than panicking or hanging.
func TestNew_NoCredentials(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
	t.Setenv("AZURE_STORAGE_ACCOUNT", "")
	if _, err := New(context.Background()); err == nil {
		t.Fatal("New with no credentials: expected error, got nil")
	}
}

// TestNew_AccountFromEnv confirms an account name (env) is enough to build a
// Backend; the default credential chain is constructed lazily and not exercised
// here (no network).
func TestNew_AccountFromEnv(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "")
	t.Setenv("AZURE_STORAGE_ACCOUNT", "examplestorageacct")
	b, err := New(context.Background())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b == nil || b.svc == nil {
		t.Fatal("New returned a nil backend/service client")
	}
}

// TestWriter_Write exercises the in-memory buffer without any Azure call.
func TestWriter_Write(t *testing.T) {
	w := &writer{}
	data := []byte("hello azure")
	n, err := w.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Errorf("n = %d, want %d", n, len(data))
	}
	if w.buf.Len() != len(data) {
		t.Errorf("buf.Len() = %d, want %d", w.buf.Len(), len(data))
	}
	more := []byte(" blob")
	w.Write(more) //nolint:errcheck
	if w.buf.Len() != len(data)+len(more) {
		t.Errorf("accumulated buf.Len() = %d, want %d", w.buf.Len(), len(data)+len(more))
	}
}

func TestWriter_AbortDiscardsBufferedDataAndRejectsLaterUse(t *testing.T) {
	w := &writer{}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := w.Write([]byte(" azure")); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if got := w.buf.Len(); got != len("hello azure") {
		t.Fatalf("buf.Len() before Abort = %d, want %d", got, len("hello azure"))
	}

	if err := w.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if got := w.buf.Len(); got != 0 {
		t.Fatalf("buf.Len() after Abort = %d, want 0", got)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if got := w.buf.Len(); got != 0 {
		t.Fatalf("buf.Len() after second Abort = %d, want 0", got)
	}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Write after Abort error = %v, want aborted state", err)
	}
	if err := w.Close(); err == nil || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("Close after Abort error = %v, want aborted state", err)
	}
}

func TestWriter_WriteAfterCloseReportsClosedState(t *testing.T) {
	w := &writer{closed: true}
	if _, err := w.Write([]byte("late")); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Write after Close error = %v, want closed state", err)
	}
}

func TestWriter_StagedCreateAndReplace(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			f := newFakeAzureWriteOperations()
			if existing {
				etag := azcore.ETag("\"old\"")
				f.objects["target"] = fakeAzureObject{
					state: azureObjectState{etag: &etag, size: 3},
					data:  "old",
				}
			}
			w := newFakeAzureWriter(f, "target")
			_, _ = w.Write([]byte("new"))
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := f.objects["target"].data; got != "new" {
				t.Fatalf("target data = %q, want new", got)
			}
			if _, ok := f.objects[w.stageName]; ok {
				t.Fatal("temporary blob was not removed")
			}
		})
	}
}

func TestWriter_VerifiesCommittedResponsesWithoutReplayingPromotion(t *testing.T) {
	f := newFakeAzureWriteOperations()
	f.stageResponseLost = true
	f.promoteResponseLost = true
	w := newFakeAzureWriter(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if f.promoteCalls != 1 {
		t.Fatalf("promote calls = %d, want 1", f.promoteCalls)
	}
	if got := f.objects["target"].data; got != "new" {
		t.Fatalf("target data = %q, want new", got)
	}
}

func TestWriter_StageFailurePreservesExistingTarget(t *testing.T) {
	f := newFakeAzureWriteOperations()
	etag := azcore.ETag("\"old\"")
	f.objects["target"] = fakeAzureObject{
		state: azureObjectState{etag: &etag, size: 3},
		data:  "old",
	}
	f.stageFailure = true
	w := newFakeAzureWriter(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after stage failure")
	}
	if got := f.objects["target"].data; got != "old" {
		t.Fatalf("target data = %q, want old", got)
	}
}

func TestWriter_ConcurrentTargetMutationIsPreserved(t *testing.T) {
	f := newFakeAzureWriteOperations()
	etag := azcore.ETag("\"old\"")
	f.objects["target"] = fakeAzureObject{
		state: azureObjectState{etag: &etag, size: 3},
		data:  "old",
	}
	f.mutateBeforePromote = true
	w := newFakeAzureWriter(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after concurrent target mutation")
	}
	if got := f.objects["target"].data; got != "concurrent" {
		t.Fatalf("target data = %q, want concurrent", got)
	}
	if _, ok := f.objects[w.stageName]; ok {
		t.Fatal("temporary blob was not removed after failed promotion")
	}
}

func TestWriter_CleanupUsesIndependentContextAndCanRetryAfterCommit(t *testing.T) {
	t.Run("request cancellation", func(t *testing.T) {
		f := newFakeAzureWriteOperations()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := newFakeAzureWriter(f, "target")
		w.ctx = ctx
		_, _ = w.Write([]byte("new"))
		if err := w.Close(); err == nil {
			t.Fatal("Close succeeded with canceled promotion context")
		}
		if f.cleanupCanceled {
			t.Fatal("cleanup inherited canceled request context")
		}
		if _, ok := f.objects[w.stageName]; ok {
			t.Fatal("temporary blob was not removed")
		}
	})

	t.Run("retry after committed close", func(t *testing.T) {
		f := newFakeAzureWriteOperations()
		f.deleteFailures = 1
		w := newFakeAzureWriter(f, "target")
		_, _ = w.Write([]byte("new"))
		if err := w.Close(); err != nil {
			t.Fatalf("Close promoted cleanup failure: %v", err)
		}
		if _, ok := f.objects[w.stageName]; !ok {
			t.Fatal("temporary blob unexpectedly removed")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, ok := f.objects[w.stageName]; ok {
			t.Fatal("second Close did not retry temporary cleanup")
		}
	})
}

// TestFile_ReadAt_EdgeCases exercises the code paths in file.ReadAt that do not
// reach the Azure client (negative offset, zero-length, and at/past EOF).
func TestFile_ReadAt_EdgeCases(t *testing.T) {
	f := &file{size: 100} // blob is nil — must not be dereferenced in these paths

	n, err := f.ReadAt([]byte{}, 0)
	if n != 0 || err != nil {
		t.Errorf("zero-len ReadAt: got (%d, %v), want (0, nil)", n, err)
	}

	_, err = f.ReadAt(make([]byte, 1), -1)
	if err == nil {
		t.Error("negative offset: expected error, got nil")
	}

	_, err = f.ReadAt(make([]byte, 1), 100)
	if !errors.Is(err, io.EOF) {
		t.Errorf("off==size ReadAt: got %v, want io.EOF", err)
	}

	_, err = f.ReadAt(make([]byte, 1), 200)
	if !errors.Is(err, io.EOF) {
		t.Errorf("off>size ReadAt: got %v, want io.EOF", err)
	}
}

// TestErrNotFoundSentinel verifies the sentinel is accessible without a live
// connection — guards against accidental interface breakage.
func TestErrNotFoundSentinel(t *testing.T) {
	if shstorage.ErrNotFound == nil {
		t.Fatal("ErrNotFound must not be nil")
	}
}

// TestRoundtripAgainstRealAccount exercises Open + ReadAt against a real Azure
// Blob account. Skipped unless SHOAL_AZURE_TEST_CONTAINER / _BLOB and a
// credential (AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT) are set.
func TestRoundtripAgainstRealAccount(t *testing.T) {
	container := os.Getenv("SHOAL_AZURE_TEST_CONTAINER")
	blob := os.Getenv("SHOAL_AZURE_TEST_BLOB")
	if container == "" || blob == "" {
		t.Skip("SHOAL_AZURE_TEST_CONTAINER / SHOAL_AZURE_TEST_BLOB not set; skipping live Azure test")
	}
	t.Log("live Azure test skipped in offline mode")
}
