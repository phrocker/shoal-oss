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
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

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
}

func (b *recordingBackfiller) BackfillExistingDocuments(
	ctx context.Context,
) (int, error) {
	b.ctx = ctx
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
