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

package authorized

import (
	"bytes"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// AccessRule is a canonical conjunction of structured policies. Its
// components are privately and defensively owned.
type AccessRule struct {
	policies []auth.Policy
	encoded  [][]byte
}

// NewAccessRule validates, canonicalizes, and deduplicates a nonempty policy
// conjunction. One rule cannot span authorization domains or generations.
func NewAccessRule(policies ...auth.Policy) (AccessRule, error) {
	if len(policies) == 0 {
		return AccessRule{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "at least one access policy is required")
	}
	if len(policies) > auth.MaxPolicyTerms {
		return AccessRule{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "access rule exceeds the policy bound")
	}
	type component struct {
		policy  auth.Policy
		encoded []byte
	}
	components := make([]component, 0, len(policies))
	var domain []byte
	var epoch int64
	for index, policy := range policies {
		cloned, encoded, err := clonePolicy(policy)
		if err != nil {
			return AccessRule{}, err
		}
		if index == 0 {
			domain = cloned.AuthorizationDomain()
			epoch = cloned.Epoch()
		} else if !bytes.Equal(domain, cloned.AuthorizationDomain()) ||
			epoch != cloned.Epoch() {
			return AccessRule{}, shoal.NewError(
				shoal.ErrorInvalidArgument,
				"access rule policies must share a domain and generation",
			)
		}
		components = append(components, component{policy: cloned, encoded: encoded})
	}
	sort.Slice(components, func(left, right int) bool {
		return bytes.Compare(components[left].encoded, components[right].encoded) < 0
	})
	rule := AccessRule{
		policies: make([]auth.Policy, 0, len(components)),
		encoded:  make([][]byte, 0, len(components)),
	}
	for _, component := range components {
		if len(rule.encoded) > 0 &&
			bytes.Equal(rule.encoded[len(rule.encoded)-1], component.encoded) {
			continue
		}
		rule.policies = append(rule.policies, component.policy)
		rule.encoded = append(rule.encoded, append([]byte(nil), component.encoded...))
	}
	return rule, nil
}

// Authorize requires every policy component to authorize the exact operation
// under the current decision and generation.
func (r AccessRule) Authorize(
	decision auth.Decision,
	operation auth.Operation,
	now time.Time,
) error {
	if len(r.policies) == 0 || len(r.policies) != len(r.encoded) ||
		decision.PolicyGeneration() <= 0 {
		return authorizationDenied()
	}
	for _, policy := range r.policies {
		if policy.Epoch() != decision.PolicyGeneration() {
			return authorizationDenied()
		}
		if role := policy.ServiceRole(); role != "" {
			if !decision.TrustedService() || decision.ServiceRole() != role ||
				!role.Allows(operation) {
				return authorizationDenied()
			}
		}
		if err := decision.Authorize(operation, auth.ResourceRequest{
			AuthorizationDomain: policy.AuthorizationDomain(),
			SourceID:            policy.SourceID(),
			PolicyID:            policy.GrantPolicyID(),
		}, now); err != nil {
			return err
		}
	}
	return nil
}

// String renders only a digest of the canonical conjunction.
func (r AccessRule) String() string {
	if len(r.policies) == 0 {
		return "access-rule{invalid}"
	}
	conjunction, err := auth.ConjoinPolicies(r.policies...)
	if err != nil {
		return "access-rule{invalid}"
	}
	return "access-rule{" +
		auth.DigestBytes("explorer-access-rule-v1", conjunction).String() + "}"
}

func (r AccessRule) clone() (AccessRule, error) {
	return NewAccessRule(r.policies...)
}

func (r AccessRule) components() []auth.Policy {
	cloned, err := r.clone()
	if err != nil {
		return nil
	}
	return cloned.policies
}

func (r AccessRule) equal(other AccessRule) bool {
	if len(r.encoded) != len(other.encoded) {
		return false
	}
	for index := range r.encoded {
		if !bytes.Equal(r.encoded[index], other.encoded[index]) {
			return false
		}
	}
	return len(r.encoded) > 0
}

func clonePolicy(policy auth.Policy) (auth.Policy, []byte, error) {
	encoded, err := policy.Encode()
	if err != nil {
		return auth.Policy{}, nil, err
	}
	cloned, err := auth.DecodePolicy(encoded)
	if err != nil {
		return auth.Policy{}, nil, err
	}
	return cloned, append([]byte(nil), encoded...), nil
}
