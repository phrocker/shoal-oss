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

// Package coordination defines the common authority vocabulary used by Shoal
// coordination adapters.
package coordination

import (
	"context"
	"errors"
	"fmt"
)

type Domain string

const (
	DomainEmbedded   Domain = "embedded"
	DomainKubernetes Domain = "kubernetes"
	DomainAccumulo   Domain = "accumulo"
)

var (
	ErrInvalidToken = errors.New("coordination: invalid authority token")
	ErrLeaseHeld    = errors.New("coordination: lease already held")
	ErrLeaseLost    = errors.New("coordination: lease lost")
	ErrStaleToken   = errors.New("coordination: stale authority token")
)

// AuthorityToken is immutable proof of one authority tenure. Backend-native
// proof belongs in LeaseID while Epoch and Generation provide durable fencing.
type AuthorityToken struct {
	Domain     Domain `json:"domain"`
	Resource   string `json:"resource"`
	Owner      string `json:"owner"`
	LeaseID    string `json:"lease_id"`
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
	Attempt    string `json:"attempt"`
}

func (t AuthorityToken) Validate() error {
	if t.Domain == "" || t.Resource == "" || t.Owner == "" || t.LeaseID == "" ||
		t.Epoch == 0 || t.Generation == 0 || t.Attempt == "" {
		return fmt.Errorf("%w: all fields are required", ErrInvalidToken)
	}
	return nil
}

type Member struct {
	ID    string
	Token AuthorityToken
}

type EventKind string

const (
	EventAcquired EventKind = "acquired"
	EventRenewed  EventKind = "renewed"
	EventFenced   EventKind = "fenced"
	EventReleased EventKind = "released"
	EventLost     EventKind = "lost"
)

type Event struct {
	Kind   EventKind
	Member Member
}

// Coordinator is the backend-neutral membership, election, and watch surface.
type Coordinator interface {
	Acquire(context.Context, string) (Lease, error)
	Watch(context.Context, string) (<-chan Event, error)
	Members(context.Context, string) ([]Member, error)
}

// Lease represents one authority tenure. Token returns a value copy; a token
// already returned never changes when the lease advances its durable fence.
type Lease interface {
	Token() AuthorityToken
	Renew(context.Context) error
	Release(context.Context) error
	Lost() <-chan struct{}
}

// Validator rejects work stamped by a tenure that is no longer current.
type Validator interface {
	Validate(context.Context, AuthorityToken) error
}

// EpochLease is implemented by coordinators that can durably advance a
// logical-table authority epoch without releasing their backend lease.
type EpochLease interface {
	Lease
	AdvanceEpoch(context.Context, uint64) (AuthorityToken, error)
}
