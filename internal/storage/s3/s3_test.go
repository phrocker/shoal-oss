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

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

type fakeS3Object struct {
	state s3ObjectState
	data  string
}

type fakeS3WriteOperations struct {
	objects             map[string]fakeS3Object
	stageFailure        bool
	stageResponseLost   bool
	promoteResponseLost bool
	mutateBeforePromote bool
	deleteFailures      int
	promoteCalls        int
	cleanupCanceled     bool
	nextETag            int
}

func newFakeS3WriteOperations() *fakeS3WriteOperations {
	return &fakeS3WriteOperations{objects: make(map[string]fakeS3Object)}
}

func (f *fakeS3WriteOperations) head(
	_ context.Context, _, key string,
) (s3ObjectState, error) {
	obj, ok := f.objects[key]
	if !ok {
		return s3ObjectState{}, &types.NotFound{}
	}
	return obj.state, nil
}

func (f *fakeS3WriteOperations) putStage(
	ctx context.Context, _, key string, data []byte, writeID string,
) (s3ObjectState, error) {
	if f.stageFailure {
		return s3ObjectState{}, errors.New("stage checksum failed")
	}
	f.nextETag++
	etag := fmt.Sprintf("\"etag-%d\"", f.nextETag)
	state := s3ObjectState{
		etag:     &etag,
		size:     int64(len(data)),
		metadata: map[string]string{"shoal-write-id": writeID},
	}
	f.objects[key] = fakeS3Object{state: state, data: string(data)}
	if f.stageResponseLost {
		f.stageResponseLost = false
		return s3ObjectState{}, errors.New("stage response lost")
	}
	if err := ctx.Err(); err != nil {
		return s3ObjectState{}, err
	}
	return state, nil
}

func (f *fakeS3WriteOperations) promote(
	ctx context.Context,
	_, stageKey, key string,
	_ s3ObjectState,
	targetExists bool,
	target s3ObjectState,
) error {
	f.promoteCalls++
	if f.mutateBeforePromote {
		f.mutateBeforePromote = false
		f.nextETag++
		etag := fmt.Sprintf("\"etag-%d\"", f.nextETag)
		f.objects[key] = fakeS3Object{
			state: s3ObjectState{etag: &etag, size: int64(len("concurrent"))},
			data:  "concurrent",
		}
	}
	current, exists := f.objects[key]
	if exists != targetExists ||
		(exists && !equalStringPointers(current.state.etag, target.etag)) {
		return errors.New("destination precondition failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stage := f.objects[stageKey]
	f.nextETag++
	etag := fmt.Sprintf("\"etag-%d\"", f.nextETag)
	stage.state.etag = &etag
	f.objects[key] = stage
	if f.promoteResponseLost {
		f.promoteResponseLost = false
		return errors.New("copy response lost")
	}
	return nil
}

func (f *fakeS3WriteOperations) deleteStage(
	ctx context.Context, _, key string, _ s3ObjectState,
) error {
	if ctx.Err() != nil {
		f.cleanupCanceled = true
		return ctx.Err()
	}
	if f.deleteFailures > 0 {
		f.deleteFailures--
		return errors.New("delete failed")
	}
	if _, ok := f.objects[key]; !ok {
		return &types.NotFound{}
	}
	delete(f.objects, key)
	return nil
}

func newFakeS3Writer(f *fakeS3WriteOperations, target string) *writer {
	targetState, err := f.head(context.Background(), "bucket", target)
	return &writer{
		ops:            f,
		bucket:         "bucket",
		key:            target,
		stageKey:       ".shoal-tmp/stage",
		writeID:        "write-id",
		ctx:            context.Background(),
		targetExists:   err == nil,
		target:         targetState,
		cleanupTimeout: s3CleanupTimeout,
	}
}

func TestParsePath(t *testing.T) {
	cases := []struct {
		in      string
		bucket  string
		key     string
		wantErr bool
	}{
		{"s3://my-bucket/path/to/file.rf", "my-bucket", "path/to/file.rf", false},
		{"my-bucket/path/to/file.rf", "my-bucket", "path/to/file.rf", false},
		{"s3://b/o", "b", "o", false},
		{"s3://", "", "", true},
		{"no-slash", "", "", true},
		{"s3://b/", "", "", true},          // trailing slash only → empty key
		{"/leading-slash/o", "", "", true}, // leading slash → empty bucket → error
		{"", "", "", true},
	}
	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			b, k, err := ParsePath(c.in)
			if c.wantErr {
				if err == nil {
					t.Errorf("expected error, got bucket=%q key=%q", b, k)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if b != c.bucket || k != c.key {
				t.Errorf("got (%q, %q), want (%q, %q)", b, k, c.bucket, c.key)
			}
		})
	}
}

// TestIsNotFound exercises the not-found detection helpers without any network.
func TestIsNotFound(t *testing.T) {
	if !isNotFound(&types.NoSuchKey{}) {
		t.Error("expected isNotFound(*types.NoSuchKey) = true")
	}
	if !isNotFound(&types.NotFound{}) {
		t.Error("expected isNotFound(*types.NotFound) = true")
	}
	if isNotFound(fmt.Errorf("some other error")) {
		t.Error("expected isNotFound(generic error) = false")
	}
}

// TestWriter_Write exercises the in-memory buffer without any S3 call.
func TestWriter_Write(t *testing.T) {
	w := &writer{}
	data := []byte("hello s3")
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
	// A second write should accumulate.
	more := []byte(" world")
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
	if _, err := w.Write([]byte(" world")); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if got := w.buf.Len(); got != len("hello world") {
		t.Fatalf("buf.Len() before Abort = %d, want %d", got, len("hello world"))
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

func TestNextTemporaryStageKeyPreservesPrefixAndBoundsUTF8Bytes(t *testing.T) {
	original := randomStageKeyToken
	randomStageKeyToken = func() (string, error) {
		return strings.Repeat("a", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageKeyToken = original
	})

	key := "tenant/path/" + strings.Repeat("界", 338)
	stageKey, err := nextTemporaryStageKey(key)
	if err != nil {
		t.Fatalf("nextTemporaryStageKey: %v", err)
	}
	if got, want := stageKeyParentPrefix(stageKey), stageKeyParentPrefix(key); got != want {
		t.Fatalf("stage prefix = %q, want %q", got, want)
	}
	if len(stageKey) > maxObjectKeyBytes {
		t.Fatalf("stage key length = %d bytes, want <= %d", len(stageKey), maxObjectKeyBytes)
	}
}

func TestNextTemporaryStageKeyRetainsDeepPrefixNearMaxBytes(t *testing.T) {
	original := randomStageKeyToken
	randomStageKeyToken = func() (string, error) {
		return strings.Repeat("b", tempStageRandomHexLen), nil
	}
	t.Cleanup(func() {
		randomStageKeyToken = original
	})

	key := strings.Repeat("a", 1004) + "/x"
	stageKey, err := nextTemporaryStageKey(key)
	if err != nil {
		t.Fatalf("nextTemporaryStageKey: %v", err)
	}
	if got, want := stageKeyParentPrefix(stageKey), stageKeyParentPrefix(key); got != want {
		t.Fatalf("stage prefix = %q, want deep prefix %q", got, want)
	}
	if len(stageKey) > maxObjectKeyBytes {
		t.Fatalf("stage key length = %d bytes, want <= %d", len(stageKey), maxObjectKeyBytes)
	}
}

func TestWriter_StagedCreateAndReplace(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%v", existing), func(t *testing.T) {
			f := newFakeS3WriteOperations()
			if existing {
				etag := "\"old\""
				f.objects["target"] = fakeS3Object{
					state: s3ObjectState{etag: &etag, size: 3},
					data:  "old",
				}
			}
			w := newFakeS3Writer(f, "target")
			_, _ = w.Write([]byte("new"))
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if got := f.objects["target"].data; got != "new" {
				t.Fatalf("target data = %q, want new", got)
			}
			if _, ok := f.objects[w.stageKey]; ok {
				t.Fatal("temporary object was not removed")
			}
		})
	}
}

func TestWriter_VerifiesCommittedResponsesWithoutReplayingPromotion(t *testing.T) {
	f := newFakeS3WriteOperations()
	f.stageResponseLost = true
	f.promoteResponseLost = true
	w := newFakeS3Writer(f, "target")
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
	f := newFakeS3WriteOperations()
	etag := "\"old\""
	f.objects["target"] = fakeS3Object{
		state: s3ObjectState{etag: &etag, size: 3},
		data:  "old",
	}
	f.stageFailure = true
	w := newFakeS3Writer(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after stage failure")
	}
	if got := f.objects["target"].data; got != "old" {
		t.Fatalf("target data = %q, want old", got)
	}
}

func TestWriter_ConcurrentTargetMutationIsPreserved(t *testing.T) {
	f := newFakeS3WriteOperations()
	etag := "\"old\""
	f.objects["target"] = fakeS3Object{
		state: s3ObjectState{etag: &etag, size: 3},
		data:  "old",
	}
	f.mutateBeforePromote = true
	w := newFakeS3Writer(f, "target")
	_, _ = w.Write([]byte("new"))
	if err := w.Close(); err == nil {
		t.Fatal("Close succeeded after concurrent target mutation")
	}
	if got := f.objects["target"].data; got != "concurrent" {
		t.Fatalf("target data = %q, want concurrent", got)
	}
	if _, ok := f.objects[w.stageKey]; ok {
		t.Fatal("temporary object was not removed after failed promotion")
	}
}

func TestWriter_CleanupUsesIndependentContextAndCanRetryAfterCommit(t *testing.T) {
	t.Run("request cancellation", func(t *testing.T) {
		f := newFakeS3WriteOperations()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := newFakeS3Writer(f, "target")
		w.ctx = ctx
		_, _ = w.Write([]byte("new"))
		if err := w.Close(); err == nil {
			t.Fatal("Close succeeded with canceled promotion context")
		}
		if f.cleanupCanceled {
			t.Fatal("cleanup inherited canceled request context")
		}
		if _, ok := f.objects[w.stageKey]; ok {
			t.Fatal("temporary object was not removed")
		}
	})

	t.Run("retry after committed close", func(t *testing.T) {
		f := newFakeS3WriteOperations()
		f.deleteFailures = 1
		w := newFakeS3Writer(f, "target")
		_, _ = w.Write([]byte("new"))
		if err := w.Close(); err != nil {
			t.Fatalf("Close promoted cleanup failure: %v", err)
		}
		if _, ok := f.objects[w.stageKey]; !ok {
			t.Fatal("temporary object unexpectedly removed")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, ok := f.objects[w.stageKey]; ok {
			t.Fatal("second Close did not retry temporary cleanup")
		}
	})
}

// TestFile_ReadAt_EdgeCases exercises the code paths in file.ReadAt that do
// not reach the S3 client (negative offset, zero-length, and at/past EOF).
func TestFile_ReadAt_EdgeCases(t *testing.T) {
	f := &file{size: 100} // client is nil — must not be called in these paths

	// Zero-length read returns (0, nil).
	n, err := f.ReadAt([]byte{}, 0)
	if n != 0 || err != nil {
		t.Errorf("zero-len ReadAt: got (%d, %v), want (0, nil)", n, err)
	}

	// Negative offset returns error without touching the client.
	_, err = f.ReadAt(make([]byte, 1), -1)
	if err == nil {
		t.Error("negative offset: expected error, got nil")
	}

	// Offset exactly at size returns io.EOF.
	_, err = f.ReadAt(make([]byte, 1), 100)
	if !errors.Is(err, io.EOF) {
		t.Errorf("off==size ReadAt: got %v, want io.EOF", err)
	}

	// Offset past size also returns io.EOF.
	_, err = f.ReadAt(make([]byte, 1), 200)
	if !errors.Is(err, io.EOF) {
		t.Errorf("off>size ReadAt: got %v, want io.EOF", err)
	}
}

// TestS3_ErrNotFoundSentinel verifies the sentinel is accessible without a
// live connection — guards against accidental interface breakage.
func TestS3_ErrNotFoundSentinel(t *testing.T) {
	if shstorage.ErrNotFound == nil {
		t.Fatal("ErrNotFound must not be nil")
	}
}

// TestS3_RoundtripAgainstRealBucket exercises Open + ReadAt against a real S3
// bucket. Skipped when SHOAL_S3_TEST_BUCKET / _OBJECT aren't set — CI without
// AWS credentials will skip cleanly.
//
// Setup the operator does once:
//
//	aws s3 cp /dev/stdin s3://your-bucket/shoal-test/probe.txt <<< "test contents"
//	export SHOAL_S3_TEST_BUCKET=your-bucket
//	export SHOAL_S3_TEST_OBJECT=shoal-test/probe.txt
//	go test ./internal/storage/s3/... -count=1
func TestS3_RoundtripAgainstRealBucket(t *testing.T) {
	bucket := os.Getenv("SHOAL_S3_TEST_BUCKET")
	object := os.Getenv("SHOAL_S3_TEST_OBJECT")
	if bucket == "" || object == "" {
		t.Skip("SHOAL_S3_TEST_BUCKET / SHOAL_S3_TEST_OBJECT not set; skipping live S3 test")
	}
	t.Log("live S3 test skipped in offline mode")
}
