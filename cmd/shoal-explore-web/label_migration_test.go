// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/labelmigration"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
)

// The startup wiring of the label migration (#570, PR4). What the migration
// does to the catalog is pinned in pkg/explorer/authorized
// (TestLabelMigration*); these tests pin that this command runs it before
// serving, records it, fails closed, and reports it.

func labelMigrationConfig(root string) serviceConfig {
	now := time.Now().UTC()
	return serviceConfig{
		backend: "embedded", data: filepath.Join(root, "corpus"),
		policyDir: filepath.Join(root, "policy"),
		resolver:  auth.NewAuthority().Resolver(),
		clock:     func() time.Time { return now },
	}
}

func TestStartupRunsTheLabelMigrationOnce(t *testing.T) {
	root := t.TempDir()
	config := labelMigrationConfig(root)
	opened, err := openService(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	first := opened.labelMigration
	opened.close()
	if !first.ran || first.record.Version != authorized.LabelMigrationVersion {
		t.Fatalf("first start did not run the migration: %+v", first)
	}
	var log bytes.Buffer
	printLabelMigration(&log, first)
	if !strings.Contains(log.String(), "Label migration v1: examined") {
		t.Fatalf("startup log = %q", log.String())
	}

	// The marker is in the catalog, so the listing reads it.
	var listing bytes.Buffer
	if err := listUntranslatableLabels(
		context.Background(), &listing, config.policyDir); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(listing.String(), "0 untranslatable") {
		t.Fatalf("listing = %q", listing.String())
	}

	// A restart finds the marker and does not run it again.
	opened, err = openService(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	second := opened.labelMigration
	opened.close()
	if second.ran || second.record.Version != authorized.LabelMigrationVersion {
		t.Fatalf("restart re-ran the migration: %+v", second)
	}
	log.Reset()
	printLabelMigration(&log, second)
	if !strings.Contains(log.String(), "already applied") {
		t.Fatalf("restart log = %q", log.String())
	}
}

type failingMigrator struct{}

func (failingMigrator) MigrateLabelledDocuments(
	context.Context, *labelmigration.Capability,
) (authorized.LabelMigrationRecord, bool, error) {
	return authorized.LabelMigrationRecord{}, false, errors.New("injected store failure")
}

func TestLabelMigrationFailureRefusesToServe(t *testing.T) {
	_, err := runLabelMigration(context.Background(), failingMigrator{})
	if err == nil || !strings.Contains(err.Error(), "refusing to serve") ||
		!strings.Contains(err.Error(), "injected store failure") {
		t.Fatalf("migration failure = %v, want a refusal to serve", err)
	}
	if _, err := runLabelMigration(context.Background(), nil); err == nil {
		t.Fatal("a nil client was accepted")
	}
}

func TestListUntranslatableLabelsFlag(t *testing.T) {
	root := t.TempDir()
	var output bytes.Buffer
	// No catalog: refused, and none is created.
	err := run(context.Background(),
		[]string{"-list-untranslatable-labels", "-state-dir", root}, &output)
	if err == nil || !strings.Contains(err.Error(), "no policy catalog") {
		t.Fatalf("listing without a catalog = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "policy")); !os.IsNotExist(statErr) {
		t.Fatalf("the listing created a policy catalog: %v", statErr)
	}

	// A catalog the migration has not completed on.
	store, err := authorized.OpenDurablePolicyStore(filepath.Join(root, "policy"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run(context.Background(),
		[]string{"-list-untranslatable-labels", "-state-dir", root}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "has not completed") {
		t.Fatalf("listing before the migration = %q", output.String())
	}

	// A completed migration with an untranslatable document.
	store, err = authorized.OpenDurablePolicyStore(filepath.Join(root, "policy"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutLabelMigration(context.Background(), authorized.LabelMigrationRecord{
		Version: authorized.LabelMigrationVersion, Documents: 1, Tightened: 1,
		Untranslatable: []authorized.UntranslatableLabel{{
			DocumentID: "doc-1", RevisionID: "rev-1", SourceURI: "file:///old.md",
			EscapedLabel: `"top secret!"`, Reason: "document node visibility does not parse",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := run(context.Background(),
		[]string{"-list-untranslatable-labels", "-state-dir", root}, &output); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"1 untranslatable", "document doc-1 revision rev-1",
		`source "file:///old.md"`, `label "top secret!"`, "does not parse",
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("listing %q lacks %q", output.String(), want)
		}
	}
}

// TestLabelMigrationCapabilityMintSites pins that the migration capability is
// minted only by the two startup paths that serve the authorized store, and
// called there rather than taken as a value.
func TestLabelMigrationCapabilityMintSites(t *testing.T) {
	const (
		capabilityPackage = "github.com/phrocker/shoal-oss/internal/labelmigration"
		constructor       = "NewCapability"
	)
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	var sites []mintSite
	fileSet := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		parsed, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		locals, unnamed := importedNames(parsed, capabilityPackage)
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if unnamed {
			t.Errorf("%s dot- or blank-imports %s", relative, capabilityPackage)
		}
		if len(locals) > 0 {
			sites = append(sites, mintSites(parsed, relative, locals, constructor)...)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(sites, func(left, right int) bool { return sites[left].file < sites[right].file })
	want := []mintSite{
		{file: filepath.Join("cmd", "shoal-explore-web", "label_migration.go"),
			function: "runLabelMigration", called: true},
		{file: filepath.Join("cmd", "shoal-mcp", "workspace.go"),
			function: "migrateLabels", called: true},
	}
	if len(sites) != len(want) {
		t.Fatalf("the label migration capability is minted at %+v, want %+v", sites, want)
	}
	for index := range want {
		if sites[index] != want[index] {
			t.Errorf("mint site %d = %+v, want %+v", index, sites[index], want[index])
		}
	}
}
