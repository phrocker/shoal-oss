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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Export writes live registrations back as a policy document.
//
// Every action is written out in full, never as inherit, so the export reads
// on its own. Each lease TTL is the live lease minus the time the registration
// was last written, rounded to the second, which is the TTL it was registered
// with until a heartbeat moves the lease.
//
// executors supplies the host's executor assertions. When it is nil there is
// nothing to read them from, so each referenced executor is exported with the
// union of its actions' declared effects as its ceiling and an empty floor: the
// narrowest assertion the live registrations are consistent with, not what the
// host actually binds. Callers should say so when they pass nil.
//
// The file format holds IDs, domains and scope identities as UTF-8 strings. A
// live value that is not UTF-8 cannot be written without changing it, so it is
// refused, naming the field.
func Export(live map[shoal.ID]fleet.Descriptor, executors []Executor) (Document, error) {
	document := Document{ATPL: Version, Origin: Origin}
	ids := make([]shoal.ID, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	if len(ids) > MaxAgents {
		return Document{}, refuse("", "agents", fmt.Sprintf("more than %d live agents", MaxAgents))
	}
	ordered, err := parentsFirst(live, ids)
	if err != nil {
		return Document{}, err
	}
	used := make(map[string]fleet.Effects)
	for _, id := range ordered {
		agent, err := exportAgent(live[id])
		if err != nil {
			return Document{}, err
		}
		document.Agents = append(document.Agents, agent)
		descriptor := live[id]
		effects := used[descriptor.ExecutorRef]
		for _, capability := range descriptor.Capabilities {
			for _, action := range capability.Actions {
				effects = append(effects, action.Effects...)
			}
		}
		used[descriptor.ExecutorRef] = effects
	}

	if executors == nil {
		refs := make([]string, 0, len(used))
		for ref := range used {
			refs = append(refs, ref)
		}
		sort.Strings(refs)
		for _, ref := range refs {
			ceiling, err := fleet.CanonicalEffects(used[ref])
			if err != nil {
				return Document{}, refuse("", "executors"+selector("ref", ref, 0), errorMessage(err))
			}
			document.Executors = append(document.Executors, Executor{
				Ref: ref, MaxEffects: effectStrings(ceiling), MinEffects: []string{},
			})
		}
		return document, nil
	}
	declared := make(map[string]struct{}, len(executors))
	for _, executor := range executors {
		declared[executor.Ref] = struct{}{}
		document.Executors = append(document.Executors, Executor{
			Ref:        executor.Ref,
			MaxEffects: append([]string{}, executor.MaxEffects...),
			MinEffects: append([]string{}, executor.MinEffects...),
		})
	}
	sort.Slice(document.Executors, func(i, j int) bool {
		return document.Executors[i].Ref < document.Executors[j].Ref
	})
	refs := make([]string, 0, len(used))
	for ref := range used {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	for _, ref := range refs {
		if _, ok := declared[ref]; !ok {
			return Document{}, refuse("", "executors", fmt.Sprintf(
				"live agents reference executor %s, which the executor manifest does not declare",
				pathValue(ref)))
		}
	}
	return document, nil
}

// parentsFirst orders live IDs so every parent precedes its children, then by
// ID, matching the order Compile emits.
func parentsFirst(live map[shoal.ID]fleet.Descriptor, ids []shoal.ID) ([]shoal.ID, error) {
	depth := make(map[shoal.ID]int, len(ids))
	for _, id := range ids {
		steps := 0
		current := live[id]
		for current.ParentID != "" {
			parent, ok := live[current.ParentID]
			if !ok {
				break
			}
			steps++
			if steps > len(ids) {
				return nil, refuse("", exportPath(id)+".parent", "live delegation forms a cycle")
			}
			current = parent
		}
		depth[id] = steps
	}
	sort.Slice(ids, func(i, j int) bool {
		if depth[ids[i]] != depth[ids[j]] {
			return depth[ids[i]] < depth[ids[j]]
		}
		return shoal.CompareID(ids[i], ids[j]) < 0
	})
	return ids, nil
}

func exportAgent(descriptor fleet.Descriptor) (Agent, error) {
	path := exportPath(descriptor.ID)
	text := func(field string, value []byte) (string, error) {
		if !utf8.Valid(value) {
			return "", refuse("", path+"."+field,
				"is not valid UTF-8 and cannot be written to a policy file without changing it")
		}
		return string(value), nil
	}
	id, err := text("id", []byte(descriptor.ID))
	if err != nil {
		return Agent{}, err
	}
	parent, err := text("parent", []byte(descriptor.ParentID))
	if err != nil {
		return Agent{}, err
	}
	domain, err := text("authorization_domain", descriptor.AuthorizationDomain)
	if err != nil {
		return Agent{}, err
	}
	// Rounded to the second: the registry stamps UpdatedAt with its own
	// clock while the lease was computed from the client's, so the raw
	// difference carries the clock skew and request latency between them.
	ttl := descriptor.LeaseExpiresAt.Sub(descriptor.UpdatedAt).Round(time.Second)
	if ttl <= 0 || ttl > fleet.MaxLease {
		return Agent{}, refuse("", path+".lease_ttl", fmt.Sprintf(
			"live lease minus last write is %s, outside (0, %s]", ttl, fleet.MaxLease))
	}
	agent := Agent{
		ID: id, Parent: parent, AuthorizationDomain: domain,
		ExecutorRef: descriptor.ExecutorRef, LeaseTTL: ttl.String(),
	}
	for i, scope := range descriptor.Scopes {
		source, err := text(fmt.Sprintf("scopes[%d].source_id", i), scope.SourceID)
		if err != nil {
			return Agent{}, err
		}
		policy, err := text(fmt.Sprintf("scopes[%d].policy_id", i), scope.PolicyID)
		if err != nil {
			return Agent{}, err
		}
		agent.Scopes = append(agent.Scopes, Scope{SourceID: source, PolicyID: policy})
	}
	for _, capability := range descriptor.Capabilities {
		exported := Capability{Name: capability.Name}
		for _, action := range capability.Actions {
			exported.Actions = append(exported.Actions, Action{
				Name:         action.Name,
				Effects:      effectStrings(action.Effects),
				InputSchema:  append(json.RawMessage(nil), action.InputSchema...),
				OutputSchema: append(json.RawMessage(nil), action.OutputSchema...),
			})
		}
		agent.Capabilities = append(agent.Capabilities, exported)
	}
	return agent, nil
}

// exportPath names a live agent in a refusal. A non-UTF-8 ID is shown as
// unpadded base64url, the registry's wire spelling, so the operator can find
// it.
func exportPath(id shoal.ID) string {
	if utf8.ValidString(string(id)) {
		return agentPath(id)
	}
	return "agents[id(base64url)=" + base64.RawURLEncoding.EncodeToString([]byte(id)) + "]"
}
