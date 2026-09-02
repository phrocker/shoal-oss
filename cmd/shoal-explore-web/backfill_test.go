// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/internal/devbackfill"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/webapi"
)

// otherAuthenticator stands in for a real authentication integration. It must
// never be given the development-only corpus backfill.
type otherAuthenticator struct{}

func (otherAuthenticator) Authenticate(*http.Request) (auth.Decision, error) {
	return auth.Decision{}, errors.New("not authenticated")
}

// TestDevelopmentBackfillGate pins the exact conditions under which the
// development corpus backfill may run: the development authenticator, which
// only -dev-auth produces, and a loopback listener.
func TestDevelopmentBackfillGate(t *testing.T) {
	authority := auth.NewAuthority()
	development, err := selectAuthenticator(true, "127.0.0.1:8080", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name          string
		authenticator webapi.Authenticator
		address       string
		binder        auth.Binder
		allowed       bool
	}{
		{"development on loopback", development, "127.0.0.1:8080", authority.Binder(), true},
		{"development on IPv6 loopback", development, "[::1]:8080", authority.Binder(), true},
		{"development on localhost", development, "localhost:8080", authority.Binder(), true},
		{"development on all interfaces", development, "0.0.0.0:8080", authority.Binder(), false},
		{"development on IPv6 wildcard", development, "[::]:8080", authority.Binder(), false},
		{"development on a bare port", development, ":8080", authority.Binder(), false},
		{"development on a routable address", development, "10.0.0.5:8080", authority.Binder(), false},
		{"real authenticator on loopback", otherAuthenticator{}, "127.0.0.1:8080", authority.Binder(), false},
		{"no authenticator on loopback", nil, "127.0.0.1:8080", authority.Binder(), false},
		{"development without a binder", development, "127.0.0.1:8080", nil, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			backfill := newDevelopmentBackfill(
				testCase.authenticator, testCase.address, testCase.binder)
			if allowed := backfill != nil; allowed != testCase.allowed {
				t.Fatalf(
					"newDevelopmentBackfill(%s) allowed = %t, want %t",
					testCase.address, allowed, testCase.allowed)
			}
		})
	}
}

// recordingBackfiller records the context it was called with so the test can
// prove the backfill runs under a bound development decision.
type recordingBackfiller struct {
	registered int
	err        error
	ctx        context.Context
	capability *devbackfill.Capability
}

func (b *recordingBackfiller) BackfillExistingDocumentsForDevelopment(
	ctx context.Context,
	capability *devbackfill.Capability,
) (int, error) {
	b.ctx = ctx
	b.capability = capability
	if b.err != nil {
		return 0, b.err
	}
	return b.registered, nil
}

// TestDevelopmentBackfillBindsAndFailsClosed proves the backfill runs under a
// bound decision and that a failure is surfaced instead of swallowed.
func TestDevelopmentBackfillBindsAndFailsClosed(t *testing.T) {
	authority := auth.NewAuthority()
	development, err := selectAuthenticator(true, "127.0.0.1:8080", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	backfill := newDevelopmentBackfill(
		development, "127.0.0.1:8080", authority.Binder())
	if backfill == nil {
		t.Fatal("the development backfill was refused on loopback")
	}

	succeeding := &recordingBackfiller{registered: 3}
	registered, err := backfill.run(context.Background(), succeeding)
	if err != nil {
		t.Fatal(err)
	}
	if registered != 3 {
		t.Fatalf("registered = %d, want 3", registered)
	}
	decision, err := authority.Resolver().Resolve(succeeding.ctx)
	if err != nil {
		t.Fatalf("the backfill ran without a bound decision: %v", err)
	}
	if decision.Subject() != developmentSubject {
		t.Fatalf("backfill subject = %q", decision.Subject())
	}

	if !succeeding.capability.Granted() {
		t.Fatal("the backfill ran without the development capability")
	}

	failing := &recordingBackfiller{err: errors.New("catalog write failed")}
	registered, err = backfill.run(context.Background(), failing)
	if err == nil {
		t.Fatal("a failed backfill was reported as success")
	}
	if registered != 0 {
		t.Fatalf("a failed backfill registered = %d, want 0", registered)
	}
	if !strings.Contains(err.Error(), "refusing to serve") {
		t.Fatalf("unclear diagnostic: %v", err)
	}

	var absent *developmentBackfill
	if registered, err := absent.run(context.Background(), succeeding); err != nil ||
		registered != 0 {
		t.Fatalf("an absent backfill ran: registered = %d err = %v", registered, err)
	}
}

// TestBackfillGrantsPreExistingCorpusOnlyWhenGated proves both halves of the
// bargain: with the gate open a corpus that predates the process is served,
// and with the gate closed the same corpus stays hidden.
func TestBackfillGrantsPreExistingCorpusOnlyWhenGated(t *testing.T) {
	data := preExistingCorpus(t)
	for _, testCase := range []struct {
		name    string
		gated   bool
		visible int
	}{
		{"backfilled", true, 1},
		{"not backfilled", false, 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			authority := auth.NewAuthority()
			var backfill *developmentBackfill
			if testCase.gated {
				development, err := selectAuthenticator(
					true, "127.0.0.1:8080", time.Now)
				if err != nil {
					t.Fatal(err)
				}
				backfill = newDevelopmentBackfill(
					development, "127.0.0.1:8080", authority.Binder())
				if backfill == nil {
					t.Fatal("the development backfill was refused on loopback")
				}
			}
			opened, err := openService(ctx, serviceConfig{
				backend:  "embedded",
				data:     data,
				resolver: authority.Resolver(),
				clock:    time.Now,
				backfill: backfill,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer opened.close()
			if testCase.gated && opened.backfilled != 1 {
				t.Fatalf("backfilled = %d, want 1", opened.backfilled)
			}
			if !testCase.gated && opened.backfilled != 0 {
				t.Fatalf("backfilled = %d without the gate", opened.backfilled)
			}

			authenticator, err := newDevelopmentAuthenticator(time.Now)
			if err != nil {
				t.Fatal(err)
			}
			decision, err := authenticator.mint()
			if err != nil {
				t.Fatal(err)
			}
			bound, err := authority.Binder().Bind(ctx, decision)
			if err != nil {
				t.Fatal(err)
			}
			response, err := opened.service.Documents(
				bound, webapi.DocumentsRequest{Page: webapi.PageRequest{Limit: 10}})
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Documents) != testCase.visible {
				t.Fatalf(
					"visible documents = %d, want %d",
					len(response.Documents), testCase.visible)
			}
		})
	}
}

// TestRunBackfillsPreExistingCorpusOverHTTP is the end-to-end proof that a
// restarted workspace serves the corpus it already holds.
func TestRunBackfillsPreExistingCorpusOverHTTP(t *testing.T) {
	data := preExistingCorpus(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	done := make(chan error, 1)
	go func() {
		done <- run(
			ctx,
			[]string{"-listen", "127.0.0.1:0", "-dev-auth", "-data", data},
			writer,
		)
	}()
	address := awaitListenAddress(t, reader)
	go func() { _, _ = io.Copy(io.Discard, reader) }()

	listed := postJSON(
		t, "http://"+address+"/api/v1/documents", `{"page":{"limit":10}}`)
	if !strings.Contains(string(listed), "file:///pre-existing.md") {
		t.Fatalf("the pre-existing corpus was not served: %s", listed)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server exit = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the server did not shut down")
	}
}

// TestOpenServiceRefusesWhenBackfillFails proves a failed backfill stops the
// workspace from opening at all, rather than serving a corpus it did not
// finish authorizing. The backfill is bound by a different authority than the
// client resolves with, so the registration attempt fails closed.
func TestOpenServiceRefusesWhenBackfillFails(t *testing.T) {
	data := preExistingCorpus(t)
	serving := auth.NewAuthority()
	foreign := auth.NewAuthority()
	development, err := selectAuthenticator(true, "127.0.0.1:8080", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	backfill := newDevelopmentBackfill(
		development, "127.0.0.1:8080", foreign.Binder())
	if backfill == nil {
		t.Fatal("the development backfill was refused on loopback")
	}
	opened, err := openService(context.Background(), serviceConfig{
		backend:  "embedded",
		data:     data,
		resolver: serving.Resolver(),
		clock:    time.Now,
		backfill: backfill,
	})
	if err == nil {
		opened.close()
		t.Fatal("the workspace opened despite a failed backfill")
	}
	if opened.service != nil {
		t.Fatal("a failed backfill still produced a service")
	}
	if !strings.Contains(err.Error(), "refusing to serve") {
		t.Fatalf("unclear diagnostic: %v", err)
	}
}

// mintSite is one reference to the capability constructor, with the function
// it appears in. An empty function means package scope.
type mintSite struct {
	file     string
	function string
	called   bool
}

// TestBackfillCapabilityHasOneMintSite enforces, across every non-test .go
// file in the repository, that there is exactly one reference to
// devbackfill.NewCapability, that it is a call rather than a value taken, that
// it is in cmd/shoal-explore-web/backfill.go, and that it is inside
// (*developmentBackfill).run -- the body reached only after
// newDevelopmentBackfill has established the development authenticator and a
// loopback listener.
//
// Package scope is rejected specifically. A package-level var would mint a
// valid capability at process init, before any gate runs, in every build.
// References are matched by resolving each file's import of
// internal/devbackfill to its local name, so renaming the import does not
// hide a mint site.
//
// Test files are excluded: they legitimately mint capabilities, and they are
// not production reachability. Outside the module the barrier is the compiler
// -- the capability type cannot be named at all -- and this test is the
// in-module half of that.
//
// TEMPORARY (issue #284): delete with the backfill.
func TestBackfillCapabilityHasOneMintSite(t *testing.T) {
	const (
		capabilityPackage = "github.com/phrocker/shoal-oss/internal/devbackfill"
		constructor       = "NewCapability"
		gateFunction      = "(*developmentBackfill).run"
	)
	gateFile := filepath.Join("cmd", "shoal-explore-web", "backfill.go")
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
			if entry.Name() == ".git" || entry.Name() == "vendor" {
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
		local := importedName(parsed, capabilityPackage)
		if local == "" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sites = append(sites, mintSites(parsed, relative, local, constructor)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(sites) != 1 {
		t.Fatalf(
			"the development backfill capability is minted %d times, want "+
				"exactly 1: %+v", len(sites), sites)
	}
	site := sites[0]
	where := site.function
	if where == "" {
		where = "package scope"
	}
	if !site.called {
		t.Fatalf(
			"the capability constructor is referenced without calling it at "+
				"%s in %s, which can defer minting past the gate",
			site.file, where)
	}
	if site.file != gateFile || site.function != gateFunction {
		t.Fatalf(
			"the capability is minted at %s in %s, want %s in %s",
			site.file, where, gateFile, gateFunction)
	}
}

// importedName returns the name the file refers to path by, or "" if the file
// does not import it. A blank or dot import returns "" because neither can
// name the constructor.
func importedName(file *ast.File, path string) string {
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil || value != path {
			continue
		}
		if spec.Name == nil {
			return filepath.Base(path)
		}
		if spec.Name.Name == "_" || spec.Name.Name == "." {
			return ""
		}
		return spec.Name.Name
	}
	return ""
}

// mintSites reports every reference to local.constructor in the file, whether
// called or merely taken as a value, together with the enclosing function.
func mintSites(
	file *ast.File,
	relative, local, constructor string,
) []mintSite {
	var sites []mintSite
	record := func(function string, node ast.Node) {
		called := map[ast.Node]bool{}
		ast.Inspect(node, func(inner ast.Node) bool {
			call, ok := inner.(*ast.CallExpr)
			if ok {
				called[call.Fun] = true
			}
			selector, ok := inner.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != constructor {
				return true
			}
			ident, ok := selector.X.(*ast.Ident)
			if !ok || ident.Name != local {
				return true
			}
			sites = append(sites, mintSite{
				file:     relative,
				function: function,
				called:   called[ast.Expr(selector)],
			})
			return true
		})
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			// Package scope: a var, const, or type declaration.
			record("", declaration)
			continue
		}
		record(functionName(function), function)
	}
	return sites
}

// functionName renders a function as Name or (Receiver).Name.
func functionName(function *ast.FuncDecl) string {
	if function.Recv == nil || len(function.Recv.List) != 1 {
		return function.Name.Name
	}
	var receiver bytes.Buffer
	if err := printer.Fprint(
		&receiver, token.NewFileSet(), function.Recv.List[0].Type,
	); err != nil {
		return function.Name.Name
	}
	return "(" + receiver.String() + ")." + function.Name.Name
}

// preExistingCorpus writes one document straight to a corpus directory and
// closes it, standing in for content ingested before this process started.
func preExistingCorpus(t *testing.T) string {
	t.Helper()
	data := t.TempDir()
	corpus, err := explorer.Open(data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corpus.Ingest(context.Background(), explorer.Source{
		URI:       "file:///pre-existing.md",
		MediaType: explorer.MediaTypeMarkdown,
		Content:   "# Pre-existing\n\nIngested before this process started.\n",
	}); err != nil {
		corpus.Close()
		t.Fatal(err)
	}
	if err := corpus.Close(); err != nil {
		t.Fatal(err)
	}
	return data
}
