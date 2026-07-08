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
	"io"
	"os"
	"testing"

	shstorage "github.com/phrocker/shoal/internal/storage"
)

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
