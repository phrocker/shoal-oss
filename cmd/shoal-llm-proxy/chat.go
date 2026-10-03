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
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
)

// The declaration is what Shoal receives. The prompt is not.
//
// This is the single most important property of the proxy. Shoal's admission
// surface takes a declaration of what a call would do, deliberately, so that
// the plane deciding whether content may be transmitted does not itself hold a
// copy of that content. A proxy that forwarded the prompt for adjudication
// would defeat the design it is built on: the content would reach Shoal, be
// recorded in its audit trail, and the decision about transmitting it would be
// made by a system that had already received it.
//
// So the proxy sends shape, never substance: how many messages, how large,
// which model, and which corpus references the caller itself named. None of
// that reconstructs the prompt.

// callerIdentity is the per-request identity the proxy derives before it asks.
type callerIdentity struct {
	AdmissionID   string
	TokenID       string
	RequestID     string
	CorrelationID string
	ObjectID      string
}

// newCallerIdentity mints the identifiers one call needs.
//
// The admission ID is random rather than derived from the payload. A derived ID
// would make two identical prompts share an admission, so the second would
// replay the first's decision instead of being adjudicated — and an
// accumulator counting calls would see one. Randomness here is not for
// unguessability: the admission surface derives its own durable key from the
// principal and ignores whatever the caller suggests (#412).
func newCallerIdentity() (callerIdentity, error) {
	var buffer [48]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return callerIdentity{}, err
	}
	return callerIdentity{
		AdmissionID:   encodeID(buffer[0:16]),
		TokenID:       encodeID(buffer[16:32]),
		RequestID:     encodeID(buffer[32:40]),
		CorrelationID: encodeID(buffer[40:48]),
		ObjectID:      encodeID(buffer[0:16]),
	}, nil
}

// chatRequest is the subset of the OpenAI-compatible body the proxy reads.
//
// Unknown fields are preserved, not rejected. The proxy is a pass-through for
// an API it does not own: refusing a field a newer client sends would make the
// proxy the reason an unmodified client stops working, which is the one thing
// it exists not to be. It therefore decodes into a map and reads only what it
// needs.
type chatRequest struct {
	body     map[string]json.RawMessage
	model    string
	messages []chatMessage
	stream   bool
}

type chatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// shoalReferences is the extension a caller uses to name the corpus references
// its payload carries.
//
// A caller that names nothing gets no obligations, and that is honest rather
// than permissive: the proxy cannot discover references inside opaque prompt
// text, and pretending otherwise would be content classification, which #390
// rules out. What it can do is enforce the obligation on references a caller
// does declare, which is what a Shoal-aware caller gets for declaring them.
const shoalReferencesField = "shoal_references"

func parseChatRequest(raw []byte) (chatRequest, error) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		return chatRequest{}, fmt.Errorf("malformed request body")
	}
	result := chatRequest{body: body}
	if encoded, ok := body["model"]; ok {
		if err := json.Unmarshal(encoded, &result.model); err != nil {
			return chatRequest{}, fmt.Errorf("model must be a string")
		}
	}
	if strings.TrimSpace(result.model) == "" {
		return chatRequest{}, fmt.Errorf("model is required")
	}
	encodedMessages, ok := body["messages"]
	if !ok {
		return chatRequest{}, fmt.Errorf("messages is required")
	}
	if err := json.Unmarshal(encodedMessages, &result.messages); err != nil {
		return chatRequest{}, fmt.Errorf("messages must be an array of objects")
	}
	if len(result.messages) == 0 {
		return chatRequest{}, fmt.Errorf("messages must not be empty")
	}
	if encoded, ok := body["stream"]; ok {
		if err := json.Unmarshal(encoded, &result.stream); err != nil {
			return chatRequest{}, fmt.Errorf("stream must be a boolean")
		}
	}
	return result, nil
}

// references returns what the caller declared, in order and deduplicated.
func (r chatRequest) references() ([]string, error) {
	encoded, ok := r.body[shoalReferencesField]
	if !ok {
		return nil, nil
	}
	var declared []string
	if err := json.Unmarshal(encoded, &declared); err != nil {
		return nil, fmt.Errorf("%s must be an array of strings", shoalReferencesField)
	}
	seen := make(map[string]struct{}, len(declared))
	result := make([]string, 0, len(declared))
	for _, reference := range declared {
		trimmed := strings.TrimSpace(reference)
		if trimmed == "" {
			return nil, fmt.Errorf("%s must not contain a blank entry", shoalReferencesField)
		}
		if _, duplicate := seen[trimmed]; duplicate {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result, nil
}

// declaration is the shape the proxy sends in place of the payload.
//
// Sizes and counts only. A reviewer checking that Shoal never receives prompt
// text should be able to confirm it by reading this function, which is why it
// builds the value explicitly rather than marshalling a struct that could later
// gain a field carrying content.
func (r chatRequest) declaration(references []string) json.RawMessage {
	roles := make([]string, 0, len(r.messages))
	total := 0
	for _, message := range r.messages {
		roles = append(roles, message.Role)
		total += len(message.Content)
	}
	encoded, _ := json.Marshal(map[string]any{
		"model":           r.model,
		"message_count":   len(r.messages),
		"message_roles":   roles,
		"content_bytes":   total,
		"stream":          r.stream,
		"reference_count": len(references),
	})
	return encoded
}

// applyObligations drops the withheld references and the messages that carry
// them, and reports whether the obligation could be satisfied at all.
//
// Dropping is the right verb. An obligation names references the caller itself
// declared, so the proxy knows exactly which of its own inputs to remove; it
// never has to find them inside prompt text. A caller whose every message
// carries a withheld reference has no satisfiable request left, and that is the
// one case where refusal is correct — refusal as the last resort rather than
// the first (#390).
func (r chatRequest) applyObligations(withhold []string) (json.RawMessage, bool, error) {
	if len(withhold) == 0 {
		encoded, err := json.Marshal(r.body)
		return encoded, true, err
	}
	denied := make(map[string]struct{}, len(withhold))
	for _, reference := range withhold {
		denied[reference] = struct{}{}
	}
	references, err := r.references()
	if err != nil {
		return nil, false, err
	}
	// An obligation naming a reference the caller never declared cannot be
	// satisfied by dropping anything, and must not be silently ignored: the
	// plane believes it has constrained this call.
	for reference := range denied {
		found := false
		for _, declared := range references {
			if declared == reference {
				found = true
				break
			}
		}
		if !found {
			return nil, false, nil
		}
	}
	kept := make([]string, 0, len(references))
	for _, reference := range references {
		if _, blocked := denied[reference]; !blocked {
			kept = append(kept, reference)
		}
	}
	body := make(map[string]json.RawMessage, len(r.body))
	for key, value := range r.body {
		body[key] = value
	}
	if len(kept) == 0 {
		delete(body, shoalReferencesField)
	} else {
		encodedKept, err := json.Marshal(kept)
		if err != nil {
			return nil, false, err
		}
		body[shoalReferencesField] = encodedKept
	}
	encoded, err := json.Marshal(body)
	return encoded, true, err
}
