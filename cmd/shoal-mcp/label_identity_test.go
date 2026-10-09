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

package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// TestIdentityPolicyRefusesLabelNamespace: label grants come only from the
// label grant file, so neither -identity-policy nor the -dev-auth identity
// may name a policy in the label namespace.
func TestIdentityPolicyRefusesLabelNamespace(t *testing.T) {
	emptyEnvironment := func(string) string { return "" }
	for _, policy := range []string{
		"shoal.label/v1/6/source/secret",
		"shoal.label/v1/!untranslatable",
		"shoal.label/v2/anything",
	} {
		if _, err := configureIdentity(identityOptions{
			subject: "subject", actor: "actor", domain: "domain",
			sourceID: "source", policyID: policy,
			policyGeneration: 1, lifetime: time.Hour,
		}); err == nil || !strings.Contains(err.Error(), "label namespace") {
			t.Fatalf("configureIdentity(policy %q) = %v, want refusal", policy, err)
		}
		if _, err := parseCommandConfig(
			[]string{
				"-identity-subject", "subject",
				"-identity-actor", "actor",
				"-identity-domain", "domain",
				"-identity-source", "source",
				"-identity-policy", policy,
			},
			io.Discard,
			emptyEnvironment,
		); err == nil || !strings.Contains(err.Error(), "label namespace") {
			t.Fatalf("-identity-policy %q = %v, want refusal", policy, err)
		}
		if _, err := parseCommandConfig(
			[]string{
				"-identity-subject", "subject",
				"-identity-actor", "actor",
				"-identity-domain", "domain",
				"-identity-source", "source",
			},
			io.Discard,
			func(name string) string {
				if name == "SHOAL_MCP_IDENTITY_POLICY" {
					return policy
				}
				return ""
			},
		); err == nil || !strings.Contains(err.Error(), "label namespace") {
			t.Fatalf("SHOAL_MCP_IDENTITY_POLICY=%q = %v, want refusal", policy, err)
		}
	}
	development, err := configureIdentity(identityOptions{
		development: true, policyGeneration: 1, lifetime: time.Hour,
	})
	if err != nil {
		t.Fatalf("configureIdentity(-dev-auth) = %v", err)
	}
	if auth.IsLabelPolicyID(development.policyID) ||
		auth.IsLabelPolicyID([]byte(defaultPolicyID)) {
		t.Fatalf("-dev-auth identity policy %q is in the label namespace",
			development.policyID)
	}
}
