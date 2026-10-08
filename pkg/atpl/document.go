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

package atpl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	// Version is the only format version this build reads. A file declaring
	// any other value is refused before any other field is read, so a later
	// version's fields are never misread as unknown ones.
	Version = "shoal.atpl/v1"
	// Origin is the credit every file carries, verbatim.
	Origin = "Derived from the Agent Trust Policy Language, " +
		"github.com/SentriusLLC/atpl (Apache-2.0)"
	// FileSuffix names the files a policy directory is read from.
	FileSuffix = ".atpl.json"

	// MaxFiles bounds how many files one policy may span.
	MaxFiles = 256
	// MaxFileBytes bounds one file. It is the registry's bound on one encoded
	// descriptor, so any single agent the registry accepts fits in a file.
	MaxFileBytes = fleet.MaxDescriptorBytes
	// MaxAgents bounds the agents one policy may declare.
	MaxAgents = 4096
	// MaxExecutors bounds the executors one policy may declare.
	MaxExecutors = 1024
)

// Document is one decoded policy file. Its fields are exactly the file's; no
// value has been checked against the registry until Compile.
type Document struct {
	ATPL      string     `json:"atpl"`
	Origin    string     `json:"origin"`
	Executors []Executor `json:"executors,omitempty"`
	Agents    []Agent    `json:"agents,omitempty"`

	name string
}

// Name is the file name the document was decoded from, used to prefix every
// refusal about its contents.
func (d Document) Name() string { return d.name }

// WithName returns a copy carrying name, for documents built in code.
func (d Document) WithName(name string) Document {
	d.name = name
	return d
}

// Executor asserts what the host binds an executor reference to. The registry
// keeps this as host configuration with no read API, so a policy declares it
// and compilation checks every action against it; the server stays
// authoritative and refuses on its own binding.
type Executor struct {
	Ref        string   `json:"ref"`
	MaxEffects []string `json:"max_effects"`
	MinEffects []string `json:"min_effects"`
}

// MarshalJSON emits both effect lists even when empty, so an evidence-only
// executor reads as a declaration rather than an omission.
func (e Executor) MarshalJSON() ([]byte, error) {
	type fields struct {
		Ref        string   `json:"ref"`
		MaxEffects []string `json:"max_effects"`
		MinEffects []string `json:"min_effects"`
	}
	return json.Marshal(fields{
		Ref: e.Ref, MaxEffects: nonNil(e.MaxEffects), MinEffects: nonNil(e.MinEffects),
	})
}

// Agent declares one registration. IDs, the authorization domain and scope
// identities are UTF-8 strings in the file; they compile to the registry's
// opaque bytes unchanged.
type Agent struct {
	ID                  string       `json:"id"`
	Parent              string       `json:"parent,omitempty"`
	AuthorizationDomain string       `json:"authorization_domain"`
	Scopes              []Scope      `json:"scopes"`
	ExecutorRef         string       `json:"executor_ref"`
	LeaseTTL            string       `json:"lease_ttl"`
	Capabilities        []Capability `json:"capabilities"`
}

type Scope struct {
	SourceID string `json:"source_id"`
	PolicyID string `json:"policy_id"`
}

type Capability struct {
	Name    string   `json:"name"`
	Actions []Action `json:"actions"`
}

// Action declares one action, or with Inherit copies the same-named action of
// the same-named parent capability: its schemas and effects, exactly.
type Action struct {
	Name         string          `json:"name"`
	Inherit      bool            `json:"inherit,omitempty"`
	Effects      []string        `json:"effects,omitempty"`
	InputSchema  json.RawMessage `json:"input_schema,omitempty"`
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`
}

// MarshalJSON emits an inherited action as its reference alone and a declared
// action with its effect list even when empty.
func (a Action) MarshalJSON() ([]byte, error) {
	if a.Inherit {
		return json.Marshal(struct {
			Name    string `json:"name"`
			Inherit bool   `json:"inherit"`
		}{Name: a.Name, Inherit: true})
	}
	return json.Marshal(struct {
		Name         string          `json:"name"`
		Effects      []string        `json:"effects"`
		InputSchema  json.RawMessage `json:"input_schema"`
		OutputSchema json.RawMessage `json:"output_schema"`
	}{
		Name: a.Name, Effects: nonNil(a.Effects),
		InputSchema: a.InputSchema, OutputSchema: a.OutputSchema,
	})
}

func nonNil(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// reserved names fields ATPL defines that this version does not compile, with
// the reason each is refused. A name refused generically would read as a typo;
// these are deliberate, and the message says why.
var reserved = map[string]string{
	"approval":    "requires a later ATPL version (#451)",
	"obligations": "requires a later ATPL version (admission obligations, #452)",
	"attestation": "requires a later ATPL version (#446)",
	"runtime":     "requires a later ATPL version (runtime attestation, #446)",
	"trust_score": "is not part of ATPL in Shoal: trust in an agent is a typed " +
		"decision, not a policy field (see docs/gateways.md, \"What comes from ATPL\")",
	"behavior": "is not part of ATPL in Shoal: fixed behavior thresholds are not " +
		"gates (see docs/gateways.md, \"What comes from ATPL\")",
}

// Decode reads one policy file strictly.
//
// It refuses, before interpreting any field: a file over MaxFileBytes, bytes
// that are not UTF-8, a top level that is not one JSON object, duplicate keys
// at any depth, and trailing data. It then refuses an unsupported version, a
// missing or altered origin credit, reserved fields by name, and every other
// unknown field. Values are not checked against the registry here; Compile
// does that across all files at once.
func Decode(name string, reader io.Reader) (Document, error) {
	if reader == nil {
		return Document{}, refuse(name, "", "policy reader is required")
	}
	data, err := io.ReadAll(io.LimitReader(reader, MaxFileBytes+1))
	if err != nil {
		return Document{}, shoal.WrapError(shoal.ErrorUnavailable,
			name+": policy file could not be read", err)
	}
	if len(data) > MaxFileBytes {
		return Document{}, refuse(name, "",
			fmt.Sprintf("policy file exceeds %d bytes", MaxFileBytes))
	}
	if !utf8.Valid(data) {
		return Document{}, refuse(name, "", "policy file is not valid UTF-8")
	}
	if err := scanStrict(data); err != nil {
		return Document{}, refuse(name, "", err.Error())
	}
	document, err := decodeDocument(name, data)
	if err != nil {
		return Document{}, err
	}
	document.name = name
	return document, nil
}

// scanStrict walks the token stream once to refuse what encoding/json accepts
// silently: a repeated key (the last one would win, so a reviewer reading the
// first would be misled) and a second top-level value.
func scanStrict(data []byte) error {
	type frame struct {
		object  bool
		wantKey bool
		keys    map[string]struct{}
		key     string
		index   int
	}
	var stack []*frame
	// container renders the path of the innermost open container, from the
	// key or index each enclosing container is currently reading.
	container := func() string {
		var builder strings.Builder
		for _, current := range stack[:len(stack)-1] {
			if current.object {
				if builder.Len() > 0 {
					builder.WriteByte('.')
				}
				builder.WriteString(current.key)
			} else {
				builder.WriteString("[" + strconv.Itoa(current.index) + "]")
			}
		}
		if builder.Len() == 0 {
			return "the top level"
		}
		return builder.String()
	}
	// finished advances the enclosing container past one completed value and
	// reports whether that value was the top-level one.
	finished := func() bool {
		if len(stack) == 0 {
			return true
		}
		top := stack[len(stack)-1]
		if top.object {
			top.wantKey = true
		} else {
			top.index++
		}
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	completed := false
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("policy file is not valid JSON: %v", err)
		}
		if completed {
			return errors.New("policy file contains trailing data after its object")
		}
		delim, isDelim := token.(json.Delim)
		if len(stack) == 0 && (!isDelim || delim != '{') {
			return errors.New("policy file must be one JSON object")
		}
		if len(stack) > 0 {
			top := stack[len(stack)-1]
			if top.object && top.wantKey && !isDelim {
				key, _ := token.(string)
				if _, duplicate := top.keys[key]; duplicate {
					return fmt.Errorf("duplicate key %q in %s", key, container())
				}
				top.keys[key] = struct{}{}
				top.key = key
				top.wantKey = false
				continue
			}
		}
		switch {
		case isDelim && delim == '{':
			stack = append(stack, &frame{object: true, wantKey: true, keys: map[string]struct{}{}})
		case isDelim && delim == '[':
			stack = append(stack, &frame{})
		case isDelim:
			stack = stack[:len(stack)-1]
			completed = finished()
		default:
			completed = finished()
		}
	}
	if !completed {
		return errors.New("policy file is truncated")
	}
	return nil
}

func decodeDocument(name string, data []byte) (Document, error) {
	root, err := decodeObject(name, "", json.RawMessage(data))
	if err != nil {
		return Document{}, err
	}
	// The version gate comes first, so a later version's fields are refused
	// as a version mismatch rather than as unknown fields.
	version, present, err := root.str("atpl")
	if err != nil {
		return Document{}, err
	}
	if !present {
		return Document{}, refuse(name, "atpl", "format version is required; this build reads "+Version)
	}
	if version != Version {
		return Document{}, refuse(name, "atpl", fmt.Sprintf(
			"unsupported format version %q; this build reads %s", version, Version))
	}
	if err := root.only("atpl", "origin", "executors", "agents"); err != nil {
		return Document{}, err
	}
	origin, present, err := root.str("origin")
	if err != nil {
		return Document{}, err
	}
	if !present || origin != Origin {
		return Document{}, refuse(name, "origin", fmt.Sprintf(
			"must credit the format's origin verbatim: %q", Origin))
	}
	document := Document{ATPL: version, Origin: origin}

	executors, _, err := root.array("executors")
	if err != nil {
		return Document{}, err
	}
	if len(executors) > MaxExecutors {
		return Document{}, refuse(name, "executors",
			fmt.Sprintf("declares more than %d executors", MaxExecutors))
	}
	for i, raw := range executors {
		executor, err := decodeExecutor(name, i, raw)
		if err != nil {
			return Document{}, err
		}
		document.Executors = append(document.Executors, executor)
	}

	agents, _, err := root.array("agents")
	if err != nil {
		return Document{}, err
	}
	if len(agents) > MaxAgents {
		return Document{}, refuse(name, "agents",
			fmt.Sprintf("declares more than %d agents", MaxAgents))
	}
	for i, raw := range agents {
		agent, err := decodeAgent(name, i, raw)
		if err != nil {
			return Document{}, err
		}
		document.Agents = append(document.Agents, agent)
	}
	return document, nil
}

func decodeExecutor(name string, index int, raw json.RawMessage) (Executor, error) {
	object, err := decodeObject(name, "executors"+selector("", "", index), raw)
	if err != nil {
		return Executor{}, err
	}
	ref, _, err := object.str("ref")
	if err != nil {
		return Executor{}, err
	}
	object.path = "executors" + selector("ref", ref, index)
	if err := object.only("ref", "max_effects", "min_effects"); err != nil {
		return Executor{}, err
	}
	if err := object.require("ref", "max_effects", "min_effects"); err != nil {
		return Executor{}, err
	}
	executor := Executor{Ref: ref}
	if executor.MaxEffects, err = object.strs("max_effects"); err != nil {
		return Executor{}, err
	}
	if executor.MinEffects, err = object.strs("min_effects"); err != nil {
		return Executor{}, err
	}
	return executor, nil
}

func decodeAgent(name string, index int, raw json.RawMessage) (Agent, error) {
	object, err := decodeObject(name, "agents"+selector("", "", index), raw)
	if err != nil {
		return Agent{}, err
	}
	id, _, err := object.str("id")
	if err != nil {
		return Agent{}, err
	}
	object.path = "agents" + selector("id", id, index)
	if err := object.only("id", "parent", "authorization_domain", "scopes",
		"executor_ref", "lease_ttl", "capabilities"); err != nil {
		return Agent{}, err
	}
	if err := object.require("id", "authorization_domain", "scopes",
		"executor_ref", "lease_ttl", "capabilities"); err != nil {
		return Agent{}, err
	}
	agent := Agent{ID: id}
	if agent.Parent, _, err = object.str("parent"); err != nil {
		return Agent{}, err
	}
	if agent.AuthorizationDomain, _, err = object.str("authorization_domain"); err != nil {
		return Agent{}, err
	}
	if agent.ExecutorRef, _, err = object.str("executor_ref"); err != nil {
		return Agent{}, err
	}
	if agent.LeaseTTL, _, err = object.str("lease_ttl"); err != nil {
		return Agent{}, err
	}
	scopes, _, err := object.array("scopes")
	if err != nil {
		return Agent{}, err
	}
	if len(scopes) > fleet.MaxScopes {
		return Agent{}, refuse(name, object.path+".scopes",
			fmt.Sprintf("declares more than %d scopes", fleet.MaxScopes))
	}
	for i, rawScope := range scopes {
		scope, err := decodeObject(name, object.path+".scopes"+selector("", "", i), rawScope)
		if err != nil {
			return Agent{}, err
		}
		if err := scope.only("source_id", "policy_id"); err != nil {
			return Agent{}, err
		}
		if err := scope.require("source_id", "policy_id"); err != nil {
			return Agent{}, err
		}
		source, _, err := scope.str("source_id")
		if err != nil {
			return Agent{}, err
		}
		policy, _, err := scope.str("policy_id")
		if err != nil {
			return Agent{}, err
		}
		agent.Scopes = append(agent.Scopes, Scope{SourceID: source, PolicyID: policy})
	}
	capabilities, _, err := object.array("capabilities")
	if err != nil {
		return Agent{}, err
	}
	if len(capabilities) > fleet.MaxCapabilities {
		return Agent{}, refuse(name, object.path+".capabilities",
			fmt.Sprintf("declares more than %d capabilities", fleet.MaxCapabilities))
	}
	for i, rawCapability := range capabilities {
		capability, err := decodeCapability(name, object.path, i, rawCapability)
		if err != nil {
			return Agent{}, err
		}
		agent.Capabilities = append(agent.Capabilities, capability)
	}
	return agent, nil
}

func decodeCapability(name, agentPath string, index int, raw json.RawMessage) (Capability, error) {
	object, err := decodeObject(name, agentPath+".capabilities"+selector("", "", index), raw)
	if err != nil {
		return Capability{}, err
	}
	capabilityName, _, err := object.str("name")
	if err != nil {
		return Capability{}, err
	}
	object.path = agentPath + ".capabilities" + selector("name", capabilityName, index)
	if err := object.only("name", "actions"); err != nil {
		return Capability{}, err
	}
	if err := object.require("name", "actions"); err != nil {
		return Capability{}, err
	}
	actions, _, err := object.array("actions")
	if err != nil {
		return Capability{}, err
	}
	if len(actions) > fleet.MaxActions {
		return Capability{}, refuse(name, object.path+".actions",
			fmt.Sprintf("declares more than %d actions", fleet.MaxActions))
	}
	capability := Capability{Name: capabilityName}
	for i, rawAction := range actions {
		action, err := decodeAction(name, object.path, i, rawAction)
		if err != nil {
			return Capability{}, err
		}
		capability.Actions = append(capability.Actions, action)
	}
	return capability, nil
}

func decodeAction(name, capabilityPath string, index int, raw json.RawMessage) (Action, error) {
	object, err := decodeObject(name, capabilityPath+".actions"+selector("", "", index), raw)
	if err != nil {
		return Action{}, err
	}
	actionName, _, err := object.str("name")
	if err != nil {
		return Action{}, err
	}
	object.path = capabilityPath + ".actions" + selector("name", actionName, index)
	if err := object.only("name", "inherit", "effects", "input_schema", "output_schema"); err != nil {
		return Action{}, err
	}
	if err := object.require("name"); err != nil {
		return Action{}, err
	}
	action := Action{Name: actionName}
	if action.Inherit, err = object.boolean("inherit"); err != nil {
		return Action{}, err
	}
	if action.Inherit {
		// An inherited action is a reference, not a declaration. Accepting a
		// local declaration beside it would leave a reader guessing which one
		// registers.
		for _, key := range []string{"effects", "input_schema", "output_schema"} {
			if _, present := object.fields[key]; present {
				return Action{}, refuse(name, object.path+"."+key,
					"must be omitted when inherit is true; the parent's declaration is copied exactly")
			}
		}
		return action, nil
	}
	if err := object.require("input_schema", "output_schema"); err != nil {
		return Action{}, err
	}
	if _, present := object.fields["effects"]; present {
		if action.Effects, err = object.strs("effects"); err != nil {
			return Action{}, err
		}
	}
	for _, key := range []string{"input_schema", "output_schema"} {
		value := object.fields[key]
		trimmed := bytes.TrimSpace(value)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			return Action{}, refuse(name, object.path+"."+key, "must be a JSON object")
		}
		if key == "input_schema" {
			action.InputSchema = append(json.RawMessage(nil), trimmed...)
		} else {
			action.OutputSchema = append(json.RawMessage(nil), trimmed...)
		}
	}
	return action, nil
}

// object is one decoded JSON object with the path refusals about it carry.
type object struct {
	file   string
	path   string
	fields map[string]json.RawMessage
}

func decodeObject(file, path string, raw json.RawMessage) (object, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return object{}, refuse(file, path, "must be a JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return object{}, refuse(file, path, "must be a JSON object")
	}
	return object{file: file, path: path, fields: fields}, nil
}

// only refuses every key outside allowed, reserved names first and with their
// specific reason, then others generically, in sorted order so the first
// refusal reported for a file never depends on map iteration.
func (o object) only(allowed ...string) error {
	permitted := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		permitted[key] = struct{}{}
	}
	keys := make([]string, 0, len(o.fields))
	for key := range o.fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if reason, ok := reserved[key]; ok {
			return refuse(o.file, o.child(key), reason)
		}
	}
	for _, key := range keys {
		if _, ok := permitted[key]; !ok {
			return refuse(o.file, o.child(key), "unknown field")
		}
	}
	return nil
}

func (o object) require(keys ...string) error {
	for _, key := range keys {
		if _, ok := o.fields[key]; !ok {
			return refuse(o.file, o.child(key), "is required")
		}
	}
	return nil
}

func (o object) child(key string) string {
	if o.path == "" {
		return key
	}
	return o.path + "." + key
}

func (o object) str(key string) (string, bool, error) {
	raw, ok := o.fields[key]
	if !ok {
		return "", false, nil
	}
	var value *string
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return "", true, refuse(o.file, o.child(key), "must be a string")
	}
	return *value, true, nil
}

func (o object) strs(key string) ([]string, error) {
	raw, ok := o.fields[key]
	if !ok {
		return nil, nil
	}
	var values *[]*string
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, refuse(o.file, o.child(key), "must be an array of strings")
	}
	result := make([]string, 0, len(*values))
	for _, value := range *values {
		if value == nil {
			return nil, refuse(o.file, o.child(key), "must be an array of strings")
		}
		result = append(result, *value)
	}
	return result, nil
}

func (o object) array(key string) ([]json.RawMessage, bool, error) {
	raw, ok := o.fields[key]
	if !ok {
		return nil, false, nil
	}
	var values *[]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil || values == nil {
		return nil, true, refuse(o.file, o.child(key), "must be an array")
	}
	return *values, true, nil
}

func (o object) boolean(key string) (bool, error) {
	raw, ok := o.fields[key]
	if !ok {
		return false, nil
	}
	var value *bool
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		return false, refuse(o.file, o.child(key), "must be a boolean")
	}
	return *value, nil
}

// selector renders one path element: [key=value] when the value is a plain
// name, [key="quoted"] when it is not, and [index] when it is empty.
func selector(key, value string, index int) string {
	if value == "" {
		return "[" + strconv.Itoa(index) + "]"
	}
	return "[" + key + "=" + pathValue(value) + "]"
}

func pathValue(value string) string {
	for _, character := range value {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '_' || character == '-' || character == ':' {
			continue
		}
		return strconv.Quote(value)
	}
	return value
}

// refuse builds the single refusal shape every check in this package returns:
// an invalid-argument error naming the file and the offending path.
func refuse(file, path, message string) error {
	var builder strings.Builder
	if file != "" {
		builder.WriteString(file)
		builder.WriteString(": ")
	}
	if path != "" {
		builder.WriteString(path)
		builder.WriteString(": ")
	}
	builder.WriteString(message)
	return shoal.NewError(shoal.ErrorInvalidArgument, builder.String())
}
