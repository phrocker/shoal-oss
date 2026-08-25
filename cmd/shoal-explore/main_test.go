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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIngestListQueryWorkflow(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "guide.md")
	if err := os.WriteFile(source, []byte(
		"# Guide\n\nUse exponential backoff for retries.\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(dir, "data")
	var output bytes.Buffer
	if err := run(context.Background(), []string{
		"ingest", "-data", data, "-file", source,
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"Disposition": "applied"`) {
		t.Fatalf("ingest output = %s", output.String())
	}
	output.Reset()
	if err := run(context.Background(), []string{
		"query", "-data", data, "-text", "exponential backoff",
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Use exponential backoff for retries.") {
		t.Fatalf("query output = %s", output.String())
	}
}
