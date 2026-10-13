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

package authorized_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestOnlyAnApplicationRegistrationLosesAnIntrinsicEdgeID pins that the
// precedence #732 gives an intrinsic edge applies to an application holder
// only: an extracted or materialized registration under the ID still refuses
// the revision, while the same revision over an application holder of the
// same other content registers.
func TestOnlyAnApplicationRegistrationLosesAnIntrinsicEdgeID(t *testing.T) {
	for _, kind := range policyStoreKinds {
		for _, holder := range []authorized.RegistrationKind{
			authorized.RegistrationApplication,
			authorized.RegistrationExtracted, authorized.RegistrationMaterialized,
		} {
			t.Run(fmt.Sprintf("%s/holder=%d", kind.name, holder), func(t *testing.T) {
				ctx := context.Background()
				f := newFixture(t)
				store, _ := kind.open(t)
				rule := f.sourceRule(t, f.sourceA, f.policyA)
				// An exact document contains edge, so only the holder's kind
				// decides.
				intrinsic := rootContainsEdge(t, "file:///holder/b.md", "# b\n\nbody\n")
				registration := authorized.EdgeRegistration{
					Edge: graph.Edge{
						ID: intrinsic.ID, From: "elsewhere-a", To: "elsewhere-b",
						Type: "uses", Weight: 1,
					},
					Rule: rule, Kind: holder,
				}
				if holder == authorized.RegistrationExtracted {
					registration.DocumentID, registration.RevisionID = "doc-z", "rev-z"
				}
				if err := store.PutEdge(ctx, registration); err != nil {
					t.Fatal(err)
				}
				err := store.PutRevision(ctx, authorized.RevisionRegistration{
					DocumentID: intrinsic.From, RevisionID: "rev-b",
					NodeIDs:        []shoal.ID{intrinsic.From, intrinsic.To},
					IntrinsicEdges: []graph.Edge{intrinsic},
					ContentDigest:  auth.DigestBytes("holder-test", []byte("rev-b")),
					Rule:           rule, Current: true,
				})
				if holder == authorized.RegistrationApplication {
					if err != nil {
						t.Fatalf("revision over an application holder = %v", err)
					}
					return
				}
				if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
					t.Fatalf("revision over a %d holder = %v, want conflict", holder, err)
				}
			})
		}
	}
}
