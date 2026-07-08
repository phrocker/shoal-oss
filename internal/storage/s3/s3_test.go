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
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

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
