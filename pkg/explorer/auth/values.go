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

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	// MaxPolicyComponentBytes is the maximum decoded domain, source, or grant
	// policy identity accepted by the visibility codec.
	MaxPolicyComponentBytes = 128
	// MaxEncodedPolicyComponentBytes is the corresponding no-padding base32
	// bound.
	MaxEncodedPolicyComponentBytes = (MaxPolicyComponentBytes*8 + 4) / 5
	// MaxPolicyExpressionBytes bounds one flattened Accumulo visibility.
	MaxPolicyExpressionBytes = 4 * 1024
	// MaxPolicyTerms bounds one flattened Accumulo visibility.
	MaxPolicyTerms = 64
	// MaxOnBehalfOfEntries bounds one trusted delegation chain.
	MaxOnBehalfOfEntries = 64
	// MaxDecisionOperations bounds trusted constructor work before operation
	// deduplication.
	MaxDecisionOperations = 64
	// MaxDecisionGrantIDs applies the repository's public scope-ID bound to
	// each trusted source and policy grant set.
	MaxDecisionGrantIDs = retrieval.MaxScopeIDs
)

// Operation is an exact Explorer authorization action.
type Operation string

const (
	OperationIngest       Operation = "ingest"
	OperationList         Operation = "list"
	OperationRead         Operation = "read"
	OperationConnect      Operation = "connect"
	OperationNeighborhood Operation = "neighborhood"
	OperationRetrieve     Operation = "retrieve"
	OperationValidate     Operation = "validation"
)

// Validate rejects unknown operations.
func (o Operation) Validate() error {
	switch o {
	case OperationIngest, OperationList, OperationRead, OperationConnect,
		OperationNeighborhood, OperationRetrieve, OperationValidate:
		return nil
	default:
		return shoal.NewError(shoal.ErrorInvalidArgument, "unknown authorization operation")
	}
}

// ServiceRole identifies one least-privilege Accumulo service-account role.
type ServiceRole string

const (
	ServiceRoleDataRead      ServiceRole = "data_read"
	ServiceRoleDataWrite     ServiceRole = "data_write"
	ServiceRoleCoordination  ServiceRole = "coordination"
	ServiceRoleDerivation    ServiceRole = "derivation"
	ServiceRoleMigration     ServiceRole = "migration"
	ServiceRoleSecurityAdmin ServiceRole = "security_admin"
)

// Validate rejects an empty or unknown service role.
func (r ServiceRole) Validate() error {
	switch r {
	case ServiceRoleDataRead, ServiceRoleDataWrite, ServiceRoleCoordination,
		ServiceRoleDerivation, ServiceRoleMigration, ServiceRoleSecurityAdmin:
		return nil
	default:
		return shoal.NewError(shoal.ErrorInvalidArgument, "unknown service role")
	}
}

// Allows reports whether the role may be used for an Explorer operation.
func (r ServiceRole) Allows(operation Operation) bool {
	if operation.Validate() != nil {
		return false
	}
	switch r {
	case ServiceRoleDataRead:
		return operation == OperationList || operation == OperationRead ||
			operation == OperationNeighborhood || operation == OperationRetrieve ||
			operation == OperationValidate
	case ServiceRoleDataWrite:
		return operation == OperationIngest || operation == OperationConnect ||
			operation == OperationValidate
	case ServiceRoleCoordination, ServiceRoleSecurityAdmin:
		return operation == OperationValidate
	case ServiceRoleDerivation:
		return operation == OperationRead || operation == OperationConnect ||
			operation == OperationValidate
	case ServiceRoleMigration:
		return true
	default:
		return false
	}
}

func roleRequiresServiceVisibility(role ServiceRole) bool {
	return role != ServiceRoleDataRead && role != ServiceRoleDataWrite
}

// ResourceRequest is an untrusted logical resource description. Metadata and
// scope are cloned and validated but never interpreted as authorization grants.
type ResourceRequest struct {
	AuthorizationDomain []byte
	SourceID            []byte
	PolicyID            []byte
	ObjectID            shoal.ID
	Metadata            shoal.Metadata
	Scope               []shoal.ID
}

// Normalize returns an independently owned, validated resource request.
func (r ResourceRequest) Normalize() (ResourceRequest, error) {
	if err := validatePolicyComponent(
		"authorization domain", r.AuthorizationDomain, true,
	); err != nil {
		return ResourceRequest{}, err
	}
	if err := validatePolicyComponent("source identity", r.SourceID, false); err != nil {
		return ResourceRequest{}, err
	}
	if err := validatePolicyComponent("policy identity", r.PolicyID, false); err != nil {
		return ResourceRequest{}, err
	}
	if err := shoal.ValidateOptionalID("object ID", r.ObjectID); err != nil {
		return ResourceRequest{}, err
	}
	if err := shoal.ValidateMetadata("resource metadata", r.Metadata); err != nil {
		return ResourceRequest{}, err
	}
	if len(r.Scope) > retrieval.MaxScopeIDs {
		return ResourceRequest{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "resource scope has too many IDs")
	}

	normalized := ResourceRequest{
		AuthorizationDomain: cloneBytes(r.AuthorizationDomain),
		SourceID:            cloneBytes(r.SourceID),
		PolicyID:            cloneBytes(r.PolicyID),
		ObjectID:            r.ObjectID,
		Metadata:            cloneMetadata(r.Metadata),
		Scope:               make([]shoal.ID, 0, len(r.Scope)),
	}
	seen := make(map[shoal.ID]struct{}, len(r.Scope))
	for _, id := range r.Scope {
		if err := shoal.ValidateRequiredID("resource scope ID", id); err != nil {
			return ResourceRequest{}, err
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		normalized.Scope = append(normalized.Scope, id)
	}
	return normalized, nil
}

// Validate checks resource invariants.
func (r ResourceRequest) Validate() error {
	_, err := r.Normalize()
	return err
}

func validatePolicyComponent(name string, value []byte, required bool) error {
	if len(value) == 0 {
		if required {
			return shoal.NewError(shoal.ErrorInvalidArgument, name+" is required")
		}
		return nil
	}
	if len(value) > MaxPolicyComponentBytes {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, name+" exceeds the policy component byte bound")
	}
	return nil
}

func validateOptionalSemantic(name, value string) error {
	if value == "" {
		return nil
	}
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, name+" must be valid nonblank UTF-8")
	}
	return shoal.ValidateSemanticString(name, value)
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func cloneByteSlices(values [][]byte) [][]byte {
	if values == nil {
		return nil
	}
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = cloneBytes(value)
	}
	return cloned
}

func cloneMetadata(value shoal.Metadata) shoal.Metadata {
	if value == nil {
		return nil
	}
	cloned := make(shoal.Metadata, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func sortedUniqueBytes(
	name string,
	values [][]byte,
	requiredComponents bool,
) ([][]byte, error) {
	cloned := cloneByteSlices(values)
	for _, value := range cloned {
		if err := validatePolicyComponent(name, value, requiredComponents); err != nil {
			return nil, err
		}
	}
	sort.Slice(cloned, func(left, right int) bool {
		return bytes.Compare(cloned[left], cloned[right]) < 0
	})
	result := cloned[:0]
	for _, value := range cloned {
		if len(result) > 0 && bytes.Equal(result[len(result)-1], value) {
			continue
		}
		result = append(result, value)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func containsBytes(sorted [][]byte, value []byte) bool {
	index := sort.Search(len(sorted), func(index int) bool {
		return bytes.Compare(sorted[index], value) >= 0
	})
	return index < len(sorted) && bytes.Equal(sorted[index], value)
}

func contextFailure(ctx context.Context) error {
	if ctx == nil {
		return shoal.NewError(shoal.ErrorInvalidArgument, "context is required")
	}
	switch err := ctx.Err(); {
	case errors.Is(err, context.Canceled):
		return shoal.WrapError(shoal.ErrorCanceled, "operation canceled", err)
	case errors.Is(err, context.DeadlineExceeded):
		return shoal.WrapError(shoal.ErrorDeadline, "operation deadline exceeded", err)
	default:
		return nil
	}
}
