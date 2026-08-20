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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

const (
	SchemaVersion    = 1
	AccumuloVersion  = "4.0.0-SNAPSHOT"
	AccumuloRevision = "1a716b2c1bb5762ead4b46d2bc4f53e13873b314"
)

var Roles = []string{"tserver", "scanserver", "compactor", "promotion", "client"}

type State string

const (
	Pass        State = "pass"
	Fail        State = "fail"
	Unsupported State = "unsupported"
	Skipped     State = "skipped"
)

type Environment struct {
	Mode   string `json:"mode"`
	GOOS   string `json:"goos"`
	GOARCH string `json:"goarch"`
	Docker string `json:"docker"`
}

type Metadata struct {
	ShoalCommit      string      `json:"shoal_commit"`
	AccumuloVersion  string      `json:"accumulo_version"`
	AccumuloRevision string      `json:"accumulo_revision"`
	Environment      Environment `json:"environment"`
}

type Evidence struct {
	Kind          string   `json:"kind"`
	Reference     string   `json:"reference"`
	ReplayCommand []string `json:"replay_command,omitempty"`
}

type Gate struct {
	Role          string     `json:"role"`
	Required      bool       `json:"required"`
	State         State      `json:"state"`
	Summary       string     `json:"summary"`
	Evidence      []Evidence `json:"evidence"`
	ReplayFixture string     `json:"replay_fixture"`
	FixtureSHA256 string     `json:"fixture_sha256"`
}

type Verdict struct {
	SchemaVersion int      `json:"schema_version"`
	Metadata      Metadata `json:"metadata"`
	Gates         []Gate   `json:"gates"`
}

type Fixture struct {
	SchemaVersion    int        `json:"schema_version"`
	Role             string     `json:"role"`
	AccumuloVersion  string     `json:"accumulo_version"`
	AccumuloRevision string     `json:"accumulo_revision"`
	Summary          string     `json:"summary"`
	Evidence         []Evidence `json:"evidence"`
}

type CommandResult struct {
	ExitCode int
	Output   string
}

type Executor interface {
	Run(context.Context, string, []string) CommandResult
}

type OSExecutor struct{}

func (OSExecutor) Run(ctx context.Context, root string, command []string) CommandResult {
	if len(command) == 0 {
		return CommandResult{ExitCode: 1, Output: "empty replay command"}
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = root
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()
	if err == nil {
		return CommandResult{Output: output.String()}
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return CommandResult{ExitCode: exitErr.ExitCode(), Output: output.String()}
	}
	return CommandResult{ExitCode: 1, Output: err.Error()}
}

type Options struct {
	Root     string
	Mode     string
	Required []string
	Commit   string
	Executor Executor
}

func Run(ctx context.Context, opts Options) (Verdict, int, error) {
	if opts.Root == "" {
		return Verdict{}, 1, errors.New("repository root is required")
	}
	if opts.Mode != "replay" && opts.Mode != "live" {
		return Verdict{}, 1, fmt.Errorf("unsupported mode %q", opts.Mode)
	}
	if opts.Executor == nil {
		opts.Executor = OSExecutor{}
	}
	required, err := requiredRoles(opts.Required)
	if err != nil {
		return Verdict{}, 1, err
	}
	commit := opts.Commit
	if commit == "" {
		result := opts.Executor.Run(ctx, opts.Root, []string{"git", "rev-parse", "HEAD"})
		if result.ExitCode != 0 {
			return Verdict{}, 1, fmt.Errorf("resolve Shoal commit: %s", strings.TrimSpace(result.Output))
		}
		commit = strings.TrimSpace(result.Output)
	}

	verdict := Verdict{
		SchemaVersion: SchemaVersion,
		Metadata: Metadata{
			ShoalCommit:      commit,
			AccumuloVersion:  AccumuloVersion,
			AccumuloRevision: AccumuloRevision,
			Environment: Environment{
				Mode:   opts.Mode,
				GOOS:   runtime.GOOS,
				GOARCH: runtime.GOARCH,
				Docker: "not_checked",
			},
		},
	}

	for _, role := range Roles {
		fixturePath := filepath.Join("test", "conformance", "fixtures", role+".json")
		gate, fixture, loadErr := loadFixture(opts.Root, fixturePath)
		gate.Required = required[role]
		if loadErr != nil {
			gate.Role = role
			gate.State = Fail
			gate.Summary = loadErr.Error()
			verdict.Gates = append(verdict.Gates, gate)
			continue
		}
		if fixture.Role != role {
			gate.Role = role
			gate.State = Fail
			gate.Summary = fmt.Sprintf("replay fixture declares role %q", fixture.Role)
			verdict.Gates = append(verdict.Gates, gate)
			continue
		}
		if !gate.Required {
			gate.State = Skipped
			gate.Summary = "gate not selected as required"
		} else if opts.Mode == "live" {
			runLive(ctx, opts, &verdict, &gate)
		} else {
			runReplay(ctx, opts, fixture, &gate)
		}
		verdict.Gates = append(verdict.Gates, gate)
	}
	return verdict, ExitCode(verdict), nil
}

func requiredRoles(names []string) (map[string]bool, error) {
	if len(names) == 0 {
		names = Roles
	}
	result := make(map[string]bool, len(names))
	for _, name := range names {
		if !slices.Contains(Roles, name) {
			return nil, fmt.Errorf("unknown required role %q", name)
		}
		result[name] = true
	}
	return result, nil
}

func loadFixture(root, relative string) (Gate, Fixture, error) {
	path := filepath.Join(root, relative)
	data, err := os.ReadFile(path)
	gate := Gate{ReplayFixture: filepath.ToSlash(relative)}
	if err != nil {
		return gate, Fixture{}, fmt.Errorf("read replay fixture %s: %w", relative, err)
	}
	sum := sha256.Sum256(data)
	gate.FixtureSHA256 = hex.EncodeToString(sum[:])
	var fixture Fixture
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		return gate, Fixture{}, fmt.Errorf("decode replay fixture %s: %w", relative, err)
	}
	if fixture.SchemaVersion != SchemaVersion {
		return gate, Fixture{}, fmt.Errorf("replay fixture %s has schema version %d", relative, fixture.SchemaVersion)
	}
	if fixture.AccumuloVersion != AccumuloVersion || fixture.AccumuloRevision != AccumuloRevision {
		return gate, Fixture{}, fmt.Errorf("replay fixture %s targets a different Accumulo build", relative)
	}
	if fixture.Role == "" || fixture.Summary == "" || len(fixture.Evidence) == 0 {
		return gate, Fixture{}, fmt.Errorf("replay fixture %s is incomplete", relative)
	}
	for _, evidence := range fixture.Evidence {
		if evidence.Kind == "" || evidence.Reference == "" || len(evidence.ReplayCommand) == 0 {
			return gate, Fixture{}, fmt.Errorf("replay fixture %s contains incomplete evidence", relative)
		}
		referencePath := strings.SplitN(evidence.Reference, "#", 2)[0]
		if filepath.IsAbs(referencePath) || strings.HasPrefix(filepath.Clean(referencePath), "..") {
			return gate, Fixture{}, fmt.Errorf("replay fixture %s contains a non-repository evidence path", relative)
		}
		content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(referencePath)))
		if err != nil {
			return gate, Fixture{}, fmt.Errorf("read evidence %s: %w", evidence.Reference, err)
		}
		if fragment := strings.SplitN(evidence.Reference, "#", 2); len(fragment) == 2 &&
			!bytes.Contains(content, []byte(fragment[1])) {
			return gate, Fixture{}, fmt.Errorf("evidence symbol %s is absent", evidence.Reference)
		}
	}
	gate.Role = fixture.Role
	gate.Summary = fixture.Summary
	gate.Evidence = fixture.Evidence
	return gate, fixture, nil
}

func runReplay(ctx context.Context, opts Options, fixture Fixture, gate *Gate) {
	for _, evidence := range fixture.Evidence {
		result := opts.Executor.Run(ctx, opts.Root, evidence.ReplayCommand)
		if result.ExitCode != 0 {
			gate.State = Fail
			gate.Summary = fmt.Sprintf(
				"replay failed for %s (exit %d): %s",
				evidence.Reference,
				result.ExitCode,
				strings.TrimSpace(result.Output),
			)
			return
		}
	}
	gate.State = Pass
}

func runLive(ctx context.Context, opts Options, verdict *Verdict, gate *Gate) {
	if gate.Role != "client" {
		gate.State = Unsupported
		gate.Summary = "the live Accumulo harness has no Shoal role adapter for this gate"
		return
	}
	command := []string{"python", "test/accumulo/harness.py", "test"}
	result := opts.Executor.Run(ctx, opts.Root, command)
	switch result.ExitCode {
	case 0:
		gate.State = Pass
		verdict.Metadata.Environment.Docker = "available"
	case 2:
		gate.State = Unsupported
		gate.Summary = "live Docker environment unavailable: " + strings.TrimSpace(result.Output)
		verdict.Metadata.Environment.Docker = "unavailable"
	default:
		gate.State = Fail
		gate.Summary = fmt.Sprintf("live Accumulo client harness failed (exit %d): %s", result.ExitCode, strings.TrimSpace(result.Output))
		verdict.Metadata.Environment.Docker = "available"
	}
	gate.Evidence = append(gate.Evidence, Evidence{
		Kind:          "live-command",
		Reference:     "test/accumulo/harness.py#full_run",
		ReplayCommand: command,
	})
}

func ExitCode(verdict Verdict) int {
	incomplete := false
	for _, gate := range verdict.Gates {
		if !gate.Required {
			continue
		}
		if gate.State == Fail {
			return 1
		}
		if gate.State != Pass {
			incomplete = true
		}
	}
	if incomplete {
		return 2
	}
	return 0
}

func Marshal(verdict Verdict) ([]byte, error) {
	data, err := json.MarshalIndent(verdict, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
