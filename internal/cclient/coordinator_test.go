//go:build !embed

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

package cclient

import (
	"strings"
	"testing"
)

// DialCoordinator's happy path requires a live coordinator; we only
// exercise the input-validation surface here. The wire-level behaviour
// is covered by integration tests against a running cluster (out of
// scope for the C3 groundwork pass).

func TestDialCoordinator_RejectsEmptyAddr(t *testing.T) {
	_, err := DialCoordinator("", "inst-uuid", "4.0.0-SNAPSHOT")
	if err == nil || !strings.Contains(err.Error(), "empty coordinator addr") {
		t.Fatalf("expected empty-addr error, got %v", err)
	}
}

func TestDialCoordinator_RejectsEmptyInstance(t *testing.T) {
	_, err := DialCoordinator("manager:9999", "", "4.0.0-SNAPSHOT")
	if err == nil || !strings.Contains(err.Error(), "empty instanceID") {
		t.Fatalf("expected empty-instance error, got %v", err)
	}
}

func TestDialCoordinator_RejectsEmptyVersion(t *testing.T) {
	_, err := DialCoordinator("manager:9999", "inst-uuid", "")
	if err == nil || !strings.Contains(err.Error(), "empty accumuloVersion") {
		t.Fatalf("expected empty-version error, got %v", err)
	}
}
