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

package reasoning

import (
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestResponseFingerprintIncludesPerSourceVisibility(t *testing.T) {
	data := responseData{
		sources: []SourceReference{{
			id:         "source",
			anchorIDs:  []shoal.ID{"anchor"},
			visibility: []string{"internal"},
		}},
	}
	internal, err := responseFingerprint(data)
	if err != nil {
		t.Fatal(err)
	}
	data.sources[0].visibility = []string{"secret"}
	secret, err := responseFingerprint(data)
	if err != nil {
		t.Fatal(err)
	}
	if secret == internal {
		t.Fatal("per-source visibility did not change response fingerprint")
	}
}

func TestCanonicalResponseIdentityRejectsMalformedEvidence(t *testing.T) {
	identity := ResponseIdentity{
		RetrievedEvidence: []interaction.EvidenceReference{{
			AnchorID: "anchor",
			Kind:     interaction.EvidenceDocument,
		}},
	}
	if _, err := ResponseFingerprint(identity); err == nil ||
		!strings.Contains(err.Error(), "retrieved evidence") {
		t.Fatalf("fingerprint error = %v", err)
	}
	if _, err := CanonicalResponseID(
		"session", time.Unix(1, 0), identity,
	); err == nil || !strings.Contains(err.Error(), "retrieved evidence") {
		t.Fatalf("response ID error = %v", err)
	}
}
