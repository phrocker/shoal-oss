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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/workspace"
)

func TestOpenServiceWiresDurableWorkspaceSettings(t *testing.T) {
	now := time.Date(2026, 9, 5, 18, 0, 0, 0, time.UTC)
	root := t.TempDir()
	corpus := filepath.Join(root, "corpus")
	policy := filepath.Join(root, "policy")
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: "owner", Actor: "actor",
		AuthorizationDomain: workspaceAuthorizationDomain,
		AllowedOperations: []auth.Operation{
			auth.OperationRead,
			auth.OperationWorkspaceSettingsRead,
			auth.OperationWorkspaceSettingsWrite,
		},
		PermittedSourceIDs:    [][]byte{workspaceSourceID},
		PermittedPolicyIDs:    [][]byte{workspaceGrantPolicyID},
		PolicyGeneration:      workspacePolicyGeneration,
		AuthenticationExpires: now.Add(time.Hour),
		RequestID:             "request",
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := auth.NewStaticResolverWithClock(
		decision, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	config := serviceConfig{
		backend: "embedded", data: corpus, policyDir: policy,
		resolver: resolver, clock: func() time.Time { return now },
	}
	blocker, err := workspace.OpenDurableStore(
		workspaceSettingsStoreDir(corpus))
	if err != nil {
		t.Fatal(err)
	}
	if failed, err := openService(
		context.Background(), config,
	); err == nil {
		failed.close()
		t.Fatal("startup succeeded while the settings directory was owned")
	}
	if err := blocker.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err := openService(context.Background(), config)
	if err != nil {
		t.Fatalf("startup failure retained the runtime lock: %v", err)
	}
	if opened.settings == nil {
		t.Fatal("embedded startup did not expose workspace settings")
	}
	topK := uint32(5)
	created, err := opened.settings.Update(
		context.Background(), "started-workspace", workspace.UpdateRequest{
			MutationID: "started-mutation",
			Narrowing: workspace.UpdateNarrowing{
				Budgets: workspace.Budgets{RetrievalTopK: &topK},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	opened.close()
	if _, err := os.Stat(filepath.Join(root, "settings")); err != nil {
		t.Fatalf("durable settings directory: %v", err)
	}

	reopened, err := openService(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	loaded, err := reopened.settings.Get(
		context.Background(), "started-workspace")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != created.Revision ||
		loaded.SettingsID != created.SettingsID ||
		loaded.Narrowing.Budgets.RetrievalTopK == nil ||
		*loaded.Narrowing.Budgets.RetrievalTopK != topK {
		t.Fatalf("restarted settings = %#v", loaded)
	}
}
