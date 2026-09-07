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

// Package fleet defines the durable, authorization-enforcing product-agent
// registry. Descriptors contain only declarative schemas and an opaque
// host-owned executor reference; they can never carry commands, environment,
// working directories, caller URLs, or transferable sessions.
package fleet

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxScopes             = 64
	MaxCapabilities       = 64
	MaxActions            = 256
	MaxNameBytes          = 128
	MaxExecutorRefBytes   = 1024
	MaxSchemaBytes        = 64 << 10
	MaxDescriptorBytes    = 1 << 20
	MaxLease              = 24 * time.Hour
	MaxReasonCodeBytes    = 64
	MaxRegistrationKeyLen = 1024
	DefaultListPageSize   = 25
	MaxListPageSize       = 32
	MaxListCursorBytes    = 4096
)

type Scope struct {
	SourceID []byte `json:"source_id"`
	PolicyID []byte `json:"policy_id"`
}

type Action struct {
	Name         string          `json:"name"`
	InputSchema  json.RawMessage `json:"input_schema"`
	OutputSchema json.RawMessage `json:"output_schema"`
}

type Capability struct {
	Name    string   `json:"name"`
	Actions []Action `json:"actions"`
}

// Descriptor is the immutable-at-a-generation durable agent description.
// Actor and Subject are supplied exclusively by the trusted auth decision.
type Descriptor struct {
	ID                  shoal.ID     `json:"id"`
	Generation          int64        `json:"generation"`
	Subject             shoal.ID     `json:"subject"`
	Actor               shoal.ID     `json:"actor"`
	ParentID            shoal.ID     `json:"parent_id,omitempty"`
	AuthorizationDomain []byte       `json:"authorization_domain"`
	Scopes              []Scope      `json:"scopes"`
	ExecutorRef         string       `json:"executor_ref"`
	Capabilities        []Capability `json:"capabilities"`
	LeaseExpiresAt      time.Time    `json:"lease_expires_at"`
	UpdatedAt           time.Time    `json:"updated_at"`
	RevokedAt           time.Time    `json:"revoked_at,omitempty"`
	RegistrationDigest  [32]byte     `json:"-"`
}

type Spec struct {
	ID                  shoal.ID     `json:"id"`
	ParentID            shoal.ID     `json:"parent_id,omitempty"`
	AuthorizationDomain []byte       `json:"authorization_domain"`
	Scopes              []Scope      `json:"scopes"`
	ExecutorRef         string       `json:"executor_ref"`
	Capabilities        []Capability `json:"capabilities"`
	LeaseExpiresAt      time.Time    `json:"lease_expires_at"`
}

type RequestContext struct {
	RequestID     shoal.ID  `json:"request_id"`
	CorrelationID shoal.ID  `json:"correlation_id,omitempty"`
	ReasonCode    string    `json:"reason_code"`
	ReasonDetail  string    `json:"reason_detail,omitempty"`
	Deadline      time.Time `json:"deadline"`
}

type RegisterRequest struct {
	Context            RequestContext `json:"context"`
	RegistrationKey    shoal.ID       `json:"registration_key"`
	ExpectedGeneration int64          `json:"expected_generation"`
	Spec               Spec           `json:"descriptor"`
}

type HeartbeatRequest struct {
	Context            RequestContext `json:"context"`
	RegistrationKey    shoal.ID       `json:"registration_key"`
	ID                 shoal.ID       `json:"id"`
	ExpectedGeneration int64          `json:"expected_generation"`
	LeaseExpiresAt     time.Time      `json:"lease_expires_at"`
}

type RevokeRequest struct {
	Context            RequestContext `json:"context"`
	RegistrationKey    shoal.ID       `json:"registration_key"`
	ID                 shoal.ID       `json:"id"`
	ExpectedGeneration int64          `json:"expected_generation"`
}

type ResolveRequest struct {
	Context RequestContext `json:"context"`
	ID      shoal.ID       `json:"id"`
}

type ListRequest struct {
	Context   RequestContext `json:"context"`
	SourceIDs [][]byte       `json:"source_ids,omitempty"`
	PolicyIDs [][]byte       `json:"policy_ids,omitempty"`
	Limit     uint32         `json:"limit,omitempty"`
	Cursor    string         `json:"cursor,omitempty"`
}

type ListPage struct {
	Descriptors []Descriptor
	NextCursor  string
}

type Executor interface{}

type ExecutorRegistry interface {
	ResolveExecutor(string) (Executor, bool)
}

type Resolved struct {
	Descriptor Descriptor
	Executor   Executor
}

func (r RequestContext) validate(now time.Time) error {
	if err := shoal.ValidateRequiredID("request ID", r.RequestID); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID("correlation ID", r.CorrelationID); err != nil {
		return err
	}
	if r.Deadline.IsZero() || r.Deadline.Location() != time.UTC ||
		!now.Before(r.Deadline) {
		return shoal.NewError(shoal.ErrorDeadline, "request deadline has elapsed")
	}
	if r.ReasonCode == "" || len(r.ReasonCode) > MaxReasonCodeBytes ||
		strings.TrimSpace(r.ReasonCode) != r.ReasonCode {
		return shoal.NewError(shoal.ErrorInvalidArgument, "reason code is outside its bound")
	}
	return nil
}

func (s Spec) canonical(now time.Time) (Spec, error) {
	if err := shoal.ValidateRequiredID("agent ID", s.ID); err != nil {
		return Spec{}, err
	}
	if err := shoal.ValidateOptionalID("parent agent ID", s.ParentID); err != nil {
		return Spec{}, err
	}
	if len(s.AuthorizationDomain) == 0 || len(s.AuthorizationDomain) > 1024 {
		return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "authorization domain is outside its bound")
	}
	if len(s.Scopes) == 0 || len(s.Scopes) > MaxScopes {
		return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent scopes are outside their bound")
	}
	if s.ExecutorRef == "" || len(s.ExecutorRef) > MaxExecutorRefBytes ||
		strings.TrimSpace(s.ExecutorRef) != s.ExecutorRef {
		return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "executor reference is outside its bound")
	}
	if s.LeaseExpiresAt.IsZero() || s.LeaseExpiresAt.Location() != time.UTC ||
		!now.Before(s.LeaseExpiresAt) || s.LeaseExpiresAt.Sub(now) > MaxLease {
		return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent lease is outside its bound")
	}
	result := s
	result.AuthorizationDomain = append([]byte(nil), s.AuthorizationDomain...)
	result.Scopes = make([]Scope, len(s.Scopes))
	for i, scope := range s.Scopes {
		if len(scope.SourceID) == 0 || len(scope.SourceID) > 1024 ||
			len(scope.PolicyID) == 0 || len(scope.PolicyID) > 1024 {
			return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent scope identity is outside its bound")
		}
		result.Scopes[i] = Scope{
			SourceID: append([]byte(nil), scope.SourceID...),
			PolicyID: append([]byte(nil), scope.PolicyID...),
		}
	}
	sort.Slice(result.Scopes, func(i, j int) bool {
		if compared := bytes.Compare(result.Scopes[i].SourceID, result.Scopes[j].SourceID); compared != 0 {
			return compared < 0
		}
		return bytes.Compare(result.Scopes[i].PolicyID, result.Scopes[j].PolicyID) < 0
	})
	for i := 1; i < len(result.Scopes); i++ {
		if bytes.Equal(result.Scopes[i-1].SourceID, result.Scopes[i].SourceID) &&
			bytes.Equal(result.Scopes[i-1].PolicyID, result.Scopes[i].PolicyID) {
			return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent scopes must be unique")
		}
	}
	capabilities, err := canonicalCapabilities(s.Capabilities)
	if err != nil {
		return Spec{}, err
	}
	result.Capabilities = capabilities
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > MaxDescriptorBytes {
		return Spec{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent descriptor exceeds its bound")
	}
	return result, nil
}

func canonicalCapabilities(input []Capability) ([]Capability, error) {
	if len(input) == 0 || len(input) > MaxCapabilities {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "capabilities are outside their bound")
	}
	result := make([]Capability, len(input))
	totalActions := 0
	for i, capability := range input {
		if err := validateName("capability", capability.Name); err != nil {
			return nil, err
		}
		if len(capability.Actions) == 0 {
			return nil, shoal.NewError(shoal.ErrorInvalidArgument, "capability actions are required")
		}
		result[i].Name = capability.Name
		result[i].Actions = make([]Action, len(capability.Actions))
		totalActions += len(capability.Actions)
		if totalActions > MaxActions {
			return nil, shoal.NewError(shoal.ErrorInvalidArgument, "actions exceed their bound")
		}
		for j, action := range capability.Actions {
			if err := validateName("action", action.Name); err != nil {
				return nil, err
			}
			inputSchema, err := canonicalSchema(action.InputSchema)
			if err != nil {
				return nil, err
			}
			outputSchema, err := canonicalSchema(action.OutputSchema)
			if err != nil {
				return nil, err
			}
			result[i].Actions[j] = Action{
				Name: action.Name, InputSchema: inputSchema, OutputSchema: outputSchema,
			}
		}
		sort.Slice(result[i].Actions, func(a, b int) bool {
			return result[i].Actions[a].Name < result[i].Actions[b].Name
		})
		for j := 1; j < len(result[i].Actions); j++ {
			if result[i].Actions[j-1].Name == result[i].Actions[j].Name {
				return nil, shoal.NewError(shoal.ErrorInvalidArgument, "action names must be unique")
			}
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	for i := 1; i < len(result); i++ {
		if result[i-1].Name == result[i].Name {
			return nil, shoal.NewError(shoal.ErrorInvalidArgument, "capability names must be unique")
		}
	}
	return result, nil
}

func canonicalSchema(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || len(raw) > MaxSchemaBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "action schema is outside its bound")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "action schema is invalid JSON")
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "action schema must be an object")
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > MaxSchemaBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "action schema exceeds its bound")
	}
	return json.RawMessage(encoded), nil
}

func validateName(kind, value string) error {
	if value == "" || len(value) > MaxNameBytes || strings.TrimSpace(value) != value {
		return shoal.NewError(shoal.ErrorInvalidArgument, kind+" name is outside its bound")
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == '.' || character == ':' {
			continue
		}
		return shoal.NewError(shoal.ErrorInvalidArgument, kind+" name contains an unsupported character")
	}
	return nil
}

func cloneDescriptor(input Descriptor) Descriptor {
	result := input
	result.AuthorizationDomain = append([]byte(nil), input.AuthorizationDomain...)
	result.Scopes = make([]Scope, len(input.Scopes))
	for i := range input.Scopes {
		result.Scopes[i] = Scope{
			SourceID: append([]byte(nil), input.Scopes[i].SourceID...),
			PolicyID: append([]byte(nil), input.Scopes[i].PolicyID...),
		}
	}
	result.Capabilities = make([]Capability, len(input.Capabilities))
	for i := range input.Capabilities {
		result.Capabilities[i].Name = input.Capabilities[i].Name
		result.Capabilities[i].Actions = make([]Action, len(input.Capabilities[i].Actions))
		for j := range input.Capabilities[i].Actions {
			action := input.Capabilities[i].Actions[j]
			result.Capabilities[i].Actions[j] = Action{
				Name:         action.Name,
				InputSchema:  append(json.RawMessage(nil), action.InputSchema...),
				OutputSchema: append(json.RawMessage(nil), action.OutputSchema...),
			}
		}
	}
	return result
}

func descriptorDigest(descriptor Descriptor) [sha256.Size]byte {
	encoded, _ := json.Marshal(descriptor)
	return sha256.Sum256(encoded)
}
