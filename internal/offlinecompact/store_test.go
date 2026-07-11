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

package offlinecompact

import (
	"context"
	"errors"
	"testing"

	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/memory"
)

func TestBackendStore_WriteThenRead(t *testing.T) {
	s := NewBackendStore(memory.New())
	want := []byte("rfile-image-bytes")
	if err := s.Write(context.Background(), "tables/2k/t-abc/Aout.rf", want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := s.Read(context.Background(), "tables/2k/t-abc/Aout.rf")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("roundtrip mismatch: got %q", got)
	}
}

// readOnlyBackend implements storage.Backend but not WritableBackend.
type readOnlyBackend struct{}

func (readOnlyBackend) Open(_ context.Context, _ string) (storage.File, error) {
	return nil, storage.ErrNotFound
}

func TestBackendStore_WriteReadOnlyBackend(t *testing.T) {
	s := NewBackendStore(readOnlyBackend{})
	err := s.Write(context.Background(), "x.rf", []byte("data"))
	if !errors.Is(err, storage.ErrReadOnly) {
		t.Fatalf("want ErrReadOnly, got %v", err)
	}
}

func TestBackendStore_ReadMissing(t *testing.T) {
	s := NewBackendStore(memory.New())
	if _, err := s.Read(context.Background(), "nope.rf"); err == nil {
		t.Fatal("expected not-found error")
	}
}
