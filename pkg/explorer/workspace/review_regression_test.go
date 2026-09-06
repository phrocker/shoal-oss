/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package workspace

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/accumulo"
	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type reviewBlockingStore struct {
	Store
	afterLoad func()
	afterCAS  func()
}

func (s *reviewBlockingStore) Load(
	ctx context.Context,
	id shoal.ID,
) (Settings, error) {
	value, err := s.Store.Load(ctx, id)
	if s.afterLoad != nil {
		s.afterLoad()
	}
	return value, err
}

func (s *reviewBlockingStore) CompareAndSwap(
	ctx context.Context,
	workspaceID, owner shoal.ID,
	authorizationDomain []byte,
	expectedRevision uint64,
	mutationID shoal.ID,
	narrowing Narrowing,
) (Settings, error) {
	value, err := s.Store.CompareAndSwap(
		ctx, workspaceID, owner, authorizationDomain,
		expectedRevision, mutationID, narrowing)
	if s.afterCAS != nil {
		s.afterCAS()
	}
	return value, err
}

type mutableGenerationReader struct {
	generation int64
	afterRead  func()
}

type reviewOntologyChoices struct {
	identity       ontology.OntologyIdentity
	afterAuthorize func()
	afterList      func()
}

func (c reviewOntologyChoices) ListOntologyChoices(
	context.Context,
	auth.Decision,
) ([]OntologyChoice, error) {
	if c.afterList != nil {
		c.afterList()
	}
	return []OntologyChoice{{Identity: c.identity, Active: true}}, nil
}

func (c reviewOntologyChoices) AuthorizeOntology(
	context.Context,
	auth.Decision,
	ontology.OntologyIdentity,
) error {
	if c.afterAuthorize != nil {
		c.afterAuthorize()
	}
	return nil
}

type reviewCeilingResolver struct {
	ceiling auth.ServiceCeiling
	after   func()
}

func (r reviewCeilingResolver) ResolveServiceCeiling(
	context.Context,
	auth.Decision,
) (auth.ServiceCeiling, error) {
	if r.after != nil {
		r.after()
	}
	return r.ceiling, nil
}

func (r *mutableGenerationReader) CurrentPolicyGeneration(
	ctx context.Context,
	_ []byte,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	generation := r.generation
	if r.afterRead != nil {
		r.afterRead()
	}
	return generation, nil
}

func TestReviewSettingsRequiresGenerationReader(t *testing.T) {
	store, err := OpenDurableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = NewProvider(store, ProviderOptions{
		Resolver: &mutableResolver{
			decision: testDecision(t, decisionOptions{}),
		},
		Clock: func() time.Time { return testNow },
	})
	if !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("missing generation reader error = %v", err)
	}
}

func TestReviewSettingsRejectExpiryDuringStoreRead(t *testing.T) {
	for _, operation := range []string{"get", "update", "apply"} {
		t.Run(operation, func(t *testing.T) {
			durable, err := OpenDurableStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			store := &reviewBlockingStore{Store: durable}
			base := testDecision(t, decisionOptions{})
			now := testNow
			provider, err := NewProvider(store, ProviderOptions{
				Resolver:         &mutableResolver{decision: base},
				GenerationReader: testGenerationReader{generation: 7},
				Clock:            func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.Update(context.Background(), "review-workspace", UpdateRequest{
				MutationID: "create",
			})
			if err != nil {
				t.Fatal(err)
			}
			store.afterLoad = func() { now = base.AuthenticationExpires() }
			switch operation {
			case "get":
				_, err = provider.Get(context.Background(), "review-workspace")
			case "update":
				_, err = provider.Update(
					context.Background(), "review-workspace", UpdateRequest{
						ExpectedRevision: 1, MutationID: "update",
					})
			case "apply":
				_, err = provider.Apply(
					context.Background(), "review-workspace",
					MaximumLimits(), nil)
			}
			if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
				t.Fatalf("%s expiry error = %v", operation, err)
			}
			stored, loadErr := durable.Load(
				context.Background(), "review-workspace")
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if stored.Revision != 1 {
				t.Fatalf("%s wrote revision %d after expiry", operation, stored.Revision)
			}
		})
	}
}

func TestReviewSettingsRejectGenerationChangeDuringStoreRead(t *testing.T) {
	for _, operation := range []string{"get", "update", "apply"} {
		t.Run(operation, func(t *testing.T) {
			durable, err := OpenDurableStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			store := &reviewBlockingStore{Store: durable}
			reader := &mutableGenerationReader{generation: 7}
			provider, err := NewProvider(store, ProviderOptions{
				Resolver: &mutableResolver{
					decision: testDecision(t, decisionOptions{}),
				},
				GenerationReader: reader,
				Clock:            func() time.Time { return testNow },
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := provider.Update(
				context.Background(), "generation-workspace",
				UpdateRequest{MutationID: "create"},
			); err != nil {
				t.Fatal(err)
			}
			store.afterLoad = func() { reader.generation = 8 }
			switch operation {
			case "get":
				_, err = provider.Get(
					context.Background(), "generation-workspace")
			case "update":
				_, err = provider.Update(
					context.Background(), "generation-workspace",
					UpdateRequest{
						ExpectedRevision: 1, MutationID: "revoked",
					})
			case "apply":
				_, err = provider.Apply(
					context.Background(), "generation-workspace",
					MaximumLimits(), nil)
			}
			if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
				t.Fatalf("%s generation change error = %v", operation, err)
			}
			stored, err := durable.Load(
				context.Background(), "generation-workspace")
			if err != nil {
				t.Fatal(err)
			}
			if stored.Revision != 1 {
				t.Fatalf(
					"%s generation-revoked call wrote revision %d",
					operation, stored.Revision)
			}
		})
	}
}

func TestReviewSettingsRecheckAfterOntologyAndCeilingCalls(t *testing.T) {
	t.Run("ontology generation revoke", func(t *testing.T) {
		store, err := OpenDurableStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		identity, _ := testOntologies(t)
		reader := &mutableGenerationReader{generation: 7}
		choices := reviewOntologyChoices{
			identity: identity,
			afterAuthorize: func() {
				reader.generation = 8
			},
		}
		provider, err := NewProvider(store, ProviderOptions{
			Resolver: &mutableResolver{
				decision: testDecision(t, decisionOptions{}),
			},
			GenerationReader: reader,
			OntologyChoices:  choices,
			Clock:            func() time.Time { return testNow },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := provider.SelectOntology(
			context.Background(), "ontology-revoke", 0,
			"select", identity,
		); !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
			t.Fatalf("ontology generation revoke error = %v", err)
		}
		if _, err := store.Load(
			context.Background(), "ontology-revoke",
		); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("ontology-revoked selection persisted: %v", err)
		}
	})

	t.Run("ontology list expiry", func(t *testing.T) {
		store, err := OpenDurableStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		identity, _ := testOntologies(t)
		base := testDecision(t, decisionOptions{})
		now := testNow
		provider, err := NewProvider(store, ProviderOptions{
			Resolver:         &mutableResolver{decision: base},
			GenerationReader: testGenerationReader{generation: 7},
			OntologyChoices: reviewOntologyChoices{
				identity: identity,
				afterList: func() {
					now = base.AuthenticationExpires()
				},
			},
			Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := provider.ListOntologyChoices(
			context.Background(), "ontology-list-expiry",
		); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			t.Fatalf("ontology list expiry error = %v", err)
		}
	})

	t.Run("ceiling expiry", func(t *testing.T) {
		store, err := OpenDurableStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		base := testDecision(t, decisionOptions{
			serviceRole: auth.ServiceRoleWorkspaceSettingsWrite,
			ceilingID:   "settings-writer-ceiling",
			operations: []auth.Operation{
				auth.OperationWorkspaceSettingsWrite,
			},
		})
		policy := testPolicy(t, base, "source-a", "policy-a", 1)
		visibility, err := policy.Encode()
		if err != nil {
			t.Fatal(err)
		}
		ceiling, err := auth.NewServiceCeiling(auth.ServiceCeilingConfig{
			Identity: "settings-writer-ceiling",
			Role:     auth.ServiceRoleWorkspaceSettingsWrite,
			Authorizations: accumulo.NewAuthorizations(
				bytes.Split(visibility, []byte("&"))...),
		})
		if err != nil {
			t.Fatal(err)
		}
		now := testNow
		provider, err := NewProvider(store, ProviderOptions{
			Resolver:         &mutableResolver{decision: base},
			GenerationReader: testGenerationReader{generation: 7},
			CeilingResolver: reviewCeilingResolver{
				ceiling: ceiling,
				after: func() {
					now = base.AuthenticationExpires()
				},
			},
			Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := provider.Update(
			context.Background(), "ceiling-expiry",
			UpdateRequest{
				MutationID: "ceiling",
				Narrowing: UpdateNarrowing{
					OutputPolicies: []OutputPolicySpec{{
						SourceID:      []byte("source-a"),
						GrantPolicyID: []byte("policy-a"),
						Epoch:         1,
					}},
				},
			},
		); !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
			t.Fatalf("ceiling expiry error = %v", err)
		}
		if _, err := store.Load(
			context.Background(), "ceiling-expiry",
		); !shoal.IsErrorCode(err, shoal.ErrorNotFound) {
			t.Fatalf("ceiling-expired update persisted: %v", err)
		}
	})
}

func TestReviewSettingsPostCommitExpiryReturnsResultAndIndeterminate(t *testing.T) {
	durable, err := OpenDurableStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	store := &reviewBlockingStore{Store: durable}
	base := testDecision(t, decisionOptions{})
	now := testNow
	provider, err := NewProvider(store, ProviderOptions{
		Resolver:         &mutableResolver{decision: base},
		GenerationReader: testGenerationReader{generation: 7},
		Clock:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Update(
		context.Background(), "postcommit-workspace",
		UpdateRequest{MutationID: "create"},
	); err != nil {
		t.Fatal(err)
	}
	store.afterCAS = func() { now = base.AuthenticationExpires() }
	result, err := provider.Update(
		context.Background(), "postcommit-workspace",
		UpdateRequest{ExpectedRevision: 1, MutationID: "postcommit"},
	)
	if result.Revision != 2 || !explorer.IsIndeterminateCommit(err) ||
		!shoal.IsErrorCode(err, shoal.ErrorUnauthorized) {
		t.Fatalf("postcommit result = %#v, error = %v", result, err)
	}
	stored, loadErr := durable.Load(
		context.Background(), "postcommit-workspace")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if stored.Revision != 2 ||
		stored.LastMutationID != "postcommit" {
		t.Fatalf("postcommit durable state = %#v", stored)
	}
}

func TestReviewSettingsConditionalWriteErrorReadback(t *testing.T) {
	t.Run("exact committed mutation", func(t *testing.T) {
		store, err := OpenDurableStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		write := store.conditionalWrite
		store.conditionalWrite = func(
			table string,
			mutations []engine.ConditionalMutation,
		) ([]bool, error) {
			if _, err := write(table, mutations); err != nil {
				return nil, err
			}
			return nil, errors.New("response lost after WAL append")
		}
		result, err := store.CompareAndSwap(
			context.Background(), "readback", "owner", []byte("domain"),
			0, "mutation", Narrowing{})
		if err != nil || result.Revision != 1 {
			t.Fatalf("readback result = %#v, error = %v", result, err)
		}
	})

	t.Run("unknown outcome", func(t *testing.T) {
		store, err := OpenDurableStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		store.conditionalWrite = func(
			string,
			[]engine.ConditionalMutation,
		) ([]bool, error) {
			return nil, errors.New("write response unavailable")
		}
		result, err := store.CompareAndSwap(
			context.Background(), "unknown", "owner", []byte("domain"),
			0, "mutation", Narrowing{})
		if result.WorkspaceID != "" || result.Revision != 0 ||
			!explorer.IsIndeterminateCommit(err) ||
			!shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
			t.Fatalf("unknown result = %#v, error = %v", result, err)
		}
	})
}

func TestReviewSettingsRejectConcurrentDirectoryOpen(t *testing.T) {
	directory := t.TempDir()
	first, err := OpenDurableStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenDurableStore(directory)
	if err == nil {
		defer second.Close()
		t.Fatal("two independent settings engines accepted the same WAL directory")
	}
}
