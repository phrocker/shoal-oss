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

package auth

import "bytes"

// Grant-policy identities in the label namespace stand for one free-form
// visibility label on one source (issue #570). They are minted only by
// authorized.LabelPolicyID and granted only from the operator's label grant
// file, so every other path that names a grant policy must keep out of this
// namespace. The namespace is defined here, beside Decision, so that both the
// decision constructor and the ingest selectors can refuse it.
const (
	// LabelPolicyNamespace opens every grant-policy identity that is, or
	// could be mistaken for, a label policy, in any encoding version.
	LabelPolicyNamespace = "shoal.label/"

	// LabelPolicyIDPrefix opens every version-1 label policy identity.
	LabelPolicyIDPrefix = LabelPolicyNamespace + "v1/"

	// ReservedLabelPolicyIDPrefix opens the reserved, never-grantable part of
	// the label namespace. A canonical label policy ID has a decimal length
	// where this prefix has '!', and '!' is not a label character, so no
	// label can reach it.
	ReservedLabelPolicyIDPrefix = LabelPolicyIDPrefix + "!"
)

// IsLabelPolicyID reports whether id lies anywhere in the label namespace,
// canonical, reserved or malformed.
func IsLabelPolicyID(id []byte) bool {
	return bytes.HasPrefix(id, []byte(LabelPolicyNamespace))
}

// IsReservedLabelPolicyID reports whether id lies in the reserved part of the
// label namespace. Nobody may ever hold such an ID: NewDecision refuses it as
// a permitted policy identity.
func IsReservedLabelPolicyID(id []byte) bool {
	return bytes.HasPrefix(id, []byte(ReservedLabelPolicyIDPrefix))
}
