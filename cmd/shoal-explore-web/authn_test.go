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
	"bufio"
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestListenAddressIsLoopback pins the classification that decides whether the
// development principal may be minted at all.
func TestListenAddressIsLoopback(t *testing.T) {
	cases := []struct {
		address  string
		loopback bool
	}{
		{":8080", false},
		{"0.0.0.0:8080", false},
		{"127.0.0.1:8080", true},
		{"127.0.0.2:8080", true},
		{"[::1]:8080", true},
		{"[::]:8080", false},
		{"localhost:8080", true},
		{"192.168.1.10:8080", false},
		{"[fe80::1]:8080", false},
		{"", false},
		{"127.0.0.1", false},
		{"nonexistent.invalid:8080", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.address, func(t *testing.T) {
			if got := listenAddressIsLoopback(testCase.address); got != testCase.loopback {
				t.Fatalf(
					"listenAddressIsLoopback(%q) = %t, want %t",
					testCase.address, got, testCase.loopback)
			}
		})
	}
}

// TestSelectAuthenticatorFailsClosed proves the workspace never serves without
// an authenticator and never mints the development principal off-host.
func TestSelectAuthenticatorFailsClosed(t *testing.T) {
	if _, err := selectAuthenticator(false, "127.0.0.1:8080", time.Now); err == nil {
		t.Fatal("loopback without an authenticator was accepted")
	}
	if _, err := selectAuthenticator(false, "0.0.0.0:8080", time.Now); err == nil {
		t.Fatal("non-loopback without an authenticator was accepted")
	}
	for _, address := range []string{":8080", "0.0.0.0:8080", "[::]:8080", "10.0.0.5:8080"} {
		authenticator, err := selectAuthenticator(true, address, time.Now)
		if err == nil {
			t.Fatalf("-dev-auth was accepted on %s", address)
		}
		if authenticator != nil {
			t.Fatalf("refused %s still returned an authenticator", address)
		}
		if !strings.Contains(err.Error(), "refusing to serve") {
			t.Fatalf("unclear diagnostic for %s: %v", address, err)
		}
	}
	authenticator, err := selectAuthenticator(true, "127.0.0.1:8080", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, "http://127.0.0.1/api/v1/meta", nil)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := authenticator.Authenticate(request)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Subject() != developmentSubject {
		t.Fatalf("development subject = %q", decision.Subject())
	}
	second, err := authenticator.Authenticate(request)
	if err != nil {
		t.Fatal(err)
	}
	if second.RequestID() == decision.RequestID() {
		t.Fatal("two requests shared one request identity")
	}
}

// TestRunRefusesNonLoopbackWithoutRealAuthenticator proves the server exits
// with a diagnostic instead of exposing the development principal.
func TestRunRefusesNonLoopbackWithoutRealAuthenticator(t *testing.T) {
	err := runBounded(
		t, []string{"-listen", "0.0.0.0:0", "-dev-auth", "-data", t.TempDir()})
	if err == nil {
		t.Fatal("the server started on a non-loopback address with -dev-auth")
	}
	if !strings.Contains(err.Error(), "refusing to serve") {
		t.Fatalf("unclear diagnostic: %v", err)
	}
}

// TestRunRefusesLoopbackWithoutAuthenticator proves that omitting every
// authenticator is refused rather than serving anonymously.
func TestRunRefusesLoopbackWithoutAuthenticator(t *testing.T) {
	err := runBounded(t, []string{"-listen", "127.0.0.1:0", "-data", t.TempDir()})
	if err == nil {
		t.Fatal("the server started without authentication")
	}
	if !strings.Contains(err.Error(), "without authentication") {
		t.Fatalf("unclear diagnostic: %v", err)
	}
}

// TestRunRefusesRemoteBackend records the deliberate decision to close the
// remote path rather than forward calls with no caller identity.
func TestRunRefusesRemoteBackend(t *testing.T) {
	err := runBounded(t, []string{
		"-listen", "127.0.0.1:0", "-dev-auth",
		"-backend", "remote", "-remote", "http://127.0.0.1:9/",
	})
	if err == nil {
		t.Fatal("the remote backend served without propagating identity")
	}
	if !strings.Contains(err.Error(), "backend remote is unavailable") {
		t.Fatalf("unclear diagnostic: %v", err)
	}
}

// runBounded runs the command and requires it to return on its own. A refusal
// returns immediately; a server that starts is shut down and reported as
// having started, so a weakened guard fails instead of hanging the suite.
func runBounded(t *testing.T, args []string) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, args, io.Discard) }()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the server did not shut down")
	}
	return nil
}

// TestRunServesLoopbackDevelopmentPrincipal is the end-to-end counterpart: on
// a loopback listener, -dev-auth authenticates real requests.
func TestRunServesLoopbackDevelopmentPrincipal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	done := make(chan error, 1)
	go func() {
		done <- run(
			ctx,
			[]string{"-listen", "127.0.0.1:0", "-dev-auth", "-data", t.TempDir()},
			writer,
		)
	}()
	address := awaitListenAddress(t, reader)
	go func() { _, _ = io.Copy(io.Discard, reader) }()

	client := http.Client{Timeout: 10 * time.Second}
	response, err := client.Post(
		"http://"+address+"/api/v1/documents",
		"application/json",
		strings.NewReader(`{"page":{"limit":10}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("documents status = %s body = %s", response.Status, body)
	}
	uploadDevelopmentDocument(t, address)
	listed := postJSON(t, "http://"+address+"/api/v1/documents", `{"page":{"limit":10}}`)
	if !strings.Contains(string(listed), "uploaded.md") {
		t.Fatalf("workspace upload was not authorized for listing: %s", listed)
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

func awaitListenAddress(t *testing.T, reader io.Reader) string {
	t.Helper()
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		_, address, found := strings.Cut(line, "listening at http://")
		if found {
			return strings.TrimSpace(address)
		}
	}
	t.Fatal("the server never reported a listen address")
	return ""
}

// uploadDevelopmentDocument ingests through the running workspace so the
// authorized client registers policy for the new revision.
func uploadDevelopmentDocument(t *testing.T, address string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("files", "uploaded.md")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("# Uploaded\n\nauthorized content\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(
		http.MethodPost, "http://"+address+"/api/v1/ingest", &body)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-Shoal-Workspace-Request", "1")
	client := http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("upload status = %s body = %s", response.Status, payload)
	}
}
