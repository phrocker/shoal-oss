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

package conformance

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type fakeExecutor struct {
	results map[string]CommandResult
}

func (f fakeExecutor) Run(_ context.Context, _ string, command []string) CommandResult {
	if result, ok := f.results[strings.Join(command, "\x00")]; ok {
		return result
	}
	return CommandResult{}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestReplayFixturesProduceDeterministicPassingVerdictWithoutDocker(t *testing.T) {
	opts := Options{
		Root:     repositoryRoot(t),
		Mode:     "replay",
		Commit:   strings.Repeat("a", 40),
		Executor: fakeExecutor{},
	}
	first, code, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("exit code = %d, want 0: %#v", code, first.Gates)
	}
	for _, gate := range first.Gates {
		if gate.State != Pass || !gate.Required || gate.ReplayFixture == "" || gate.FixtureSHA256 == "" {
			t.Fatalf("incomplete passing gate: %#v", gate)
		}
	}
	second, _, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := Marshal(first)
	b, _ := Marshal(second)
	if string(a) != string(b) {
		t.Fatalf("replay output is not deterministic:\n%s\n%s", a, b)
	}
}

func TestReplayFailureMakesRequiredGateFail(t *testing.T) {
	command := []string{"go", "test", "./internal/tserver", "-run", "^TestLockLossDropsEverything$", "-count=1"}
	verdict, code, err := Run(context.Background(), Options{
		Root:   repositoryRoot(t),
		Mode:   "replay",
		Commit: strings.Repeat("b", 40),
		Executor: fakeExecutor{results: map[string]CommandResult{
			strings.Join(command, "\x00"): {ExitCode: 1, Output: "fixture mismatch"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if code != 1 || verdict.Gates[0].State != Fail {
		t.Fatalf("code=%d gate=%#v", code, verdict.Gates[0])
	}
}

func TestUnavailableLiveEnvironmentIsUnsupportedNotPass(t *testing.T) {
	command := []string{"python", "test/accumulo/harness.py", "test"}
	verdict, code, err := Run(context.Background(), Options{
		Root:     repositoryRoot(t),
		Mode:     "live",
		Required: []string{"client"},
		Commit:   strings.Repeat("c", 40),
		Executor: fakeExecutor{results: map[string]CommandResult{
			strings.Join(command, "\x00"): {ExitCode: 2, Output: "SKIP (needs-docker)"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	for _, gate := range verdict.Gates {
		if gate.Role == "client" && gate.State != Unsupported {
			t.Fatalf("client state = %q, want unsupported", gate.State)
		}
	}
}

func TestMalformedFixtureFailsClosed(t *testing.T) {
	root := t.TempDir()
	fixtures := filepath.Join(root, "test", "conformance", "fixtures")
	if err := os.MkdirAll(fixtures, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, role := range Roles {
		fixture := Fixture{
			SchemaVersion:    SchemaVersion,
			Role:             role,
			AccumuloVersion:  AccumuloVersion,
			AccumuloRevision: AccumuloRevision,
			Summary:          "fixture",
			Evidence: []Evidence{{
				Kind:          "go-test",
				Reference:     "missing_test.go#TestMissing",
				ReplayCommand: []string{"go", "test"},
			}},
		}
		data, _ := json.Marshal(fixture)
		if err := os.WriteFile(filepath.Join(fixtures, role+".json"), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	verdict, code, err := Run(context.Background(), Options{
		Root:     root,
		Mode:     "replay",
		Commit:   strings.Repeat("d", 40),
		Executor: fakeExecutor{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if code != 1 || verdict.Gates[0].State != Fail {
		t.Fatalf("code=%d gate=%#v", code, verdict.Gates[0])
	}
}
