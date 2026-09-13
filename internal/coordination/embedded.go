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

package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/phrocker/shoal-oss/internal/dirlock"
	"github.com/phrocker/shoal-oss/internal/storage"
	"github.com/phrocker/shoal-oss/internal/storage/local"
)

const (
	embeddedLockFile     = ".shoal-authority.lock"
	embeddedManifestFile = ".shoal-authority.json"
	embeddedManifestSize = 64 << 10
	embeddedVersion      = 1
	// Bound per-watcher memory while allowing routine bursts without resync.
	embeddedWatchQueue = 64
)

type embeddedManifest struct {
	Version    int    `json:"version"`
	Domain     Domain `json:"domain"`
	Resource   string `json:"resource"`
	Owner      string `json:"owner"`
	LeaseID    string `json:"lease_id"`
	Epoch      uint64 `json:"epoch"`
	Generation uint64 `json:"generation"`
	Attempt    string `json:"attempt"`
	Active     bool   `json:"active"`
}

func (m embeddedManifest) token() AuthorityToken {
	return AuthorityToken{
		Domain:     m.Domain,
		Resource:   m.Resource,
		Owner:      m.Owner,
		LeaseID:    m.LeaseID,
		Epoch:      m.Epoch,
		Generation: m.Generation,
		Attempt:    m.Attempt,
	}
}

// EmbeddedCoordinator provides one-process authority for a data directory.
// The OS lock is the live exclusion primitive; the manifest generation is the
// durable fence that makes tokens from earlier process incarnations stale.
type EmbeddedCoordinator struct {
	directory string
	owner     string

	mu          sync.Mutex
	current     *embeddedLease
	watchers    map[uint64]*embeddedWatcher
	nextWatcher uint64
}

type embeddedWatcher struct {
	resource string
	events   chan Event
	pending  []Event
	wake     chan struct{}
}

func NewEmbeddedCoordinator(directory, owner string) (*EmbeddedCoordinator, error) {
	if directory == "" {
		return nil, errors.New("coordination: embedded directory is required")
	}
	if owner == "" {
		owner = uuid.NewString()
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("coordination: create embedded directory: %w", err)
	}
	canonical, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("coordination: resolve embedded directory: %w", err)
	}
	return &EmbeddedCoordinator{
		directory: filepath.Clean(canonical),
		owner:     owner,
		watchers:  make(map[uint64]*embeddedWatcher),
	}, nil
}

func (c *EmbeddedCoordinator) Acquire(ctx context.Context, resource string) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resource == "" {
		return nil, errors.New("coordination: resource is required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil && c.current.active {
		return nil, ErrLeaseHeld
	}

	lock, err := dirlock.Acquire(c.directory, embeddedLockFile)
	if err != nil {
		if errors.Is(err, dirlock.ErrLocked) {
			return nil, errors.Join(ErrLeaseHeld, err)
		}
		return nil, fmt.Errorf("coordination: acquire embedded authority: %w", err)
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			_ = lock.Close()
		}
	}()

	previous, found, err := readEmbeddedManifest(c.directory)
	if err != nil {
		return nil, err
	}
	epoch := uint64(1)
	generation := uint64(1)
	if found {
		if previous.Domain != DomainEmbedded || previous.Resource != resource {
			return nil, fmt.Errorf(
				"coordination: embedded manifest identifies %s/%q, want %s/%q",
				previous.Domain, previous.Resource, DomainEmbedded, resource,
			)
		}
		if previous.Epoch == 0 || previous.Generation == 0 {
			return nil, errors.New("coordination: embedded manifest has a zero fence")
		}
		if previous.Generation == math.MaxUint64 {
			return nil, errors.New("coordination: embedded manifest generation exhausted")
		}
		epoch = previous.Epoch
		generation = previous.Generation + 1
	}

	leaseID := uuid.NewString()
	token := AuthorityToken{
		Domain:     DomainEmbedded,
		Resource:   resource,
		Owner:      c.owner,
		LeaseID:    leaseID,
		Epoch:      epoch,
		Generation: generation,
		Attempt:    leaseID,
	}
	if err := writeEmbeddedManifest(c.directory, manifestForToken(token, true)); err != nil {
		return nil, err
	}
	lease := &embeddedLease{
		coordinator: c,
		lock:        lock,
		token:       token,
		active:      true,
		lost:        make(chan struct{}),
	}
	c.current = lease
	releaseOnError = false
	c.publishLocked(Event{Kind: EventAcquired, Member: Member{ID: token.Owner, Token: token}})
	return lease, nil
}

func (c *EmbeddedCoordinator) Watch(ctx context.Context, resource string) (<-chan Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resource == "" {
		return nil, errors.New("coordination: resource is required")
	}
	watcher := &embeddedWatcher{
		resource: resource,
		events:   make(chan Event, 4),
		pending:  make([]Event, 0, embeddedWatchQueue),
		wake:     make(chan struct{}, 1),
	}
	c.mu.Lock()
	id := c.nextWatcher
	c.nextWatcher++
	c.watchers[id] = watcher
	if c.current != nil && c.current.active && c.current.token.Resource == resource {
		watcher.pending = append(watcher.pending, Event{
			Kind:   EventAcquired,
			Member: Member{ID: c.current.token.Owner, Token: c.current.token},
		})
	}
	c.mu.Unlock()
	go c.deliverWatcher(ctx, id, watcher)
	return watcher.events, nil
}

func (c *EmbeddedCoordinator) Members(ctx context.Context, resource string) ([]Member, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || !c.current.active || c.current.token.Resource != resource {
		return nil, nil
	}
	return []Member{{ID: c.current.token.Owner, Token: c.current.token}}, nil
}

func (c *EmbeddedCoordinator) Validate(ctx context.Context, token AuthorityToken) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := token.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current == nil || !c.current.active || c.current.token != token {
		return ErrStaleToken
	}
	return nil
}

func (c *EmbeddedCoordinator) publishLocked(event Event) {
	for _, watcher := range c.watchers {
		if watcher.resource != event.Member.Token.Resource {
			continue
		}
		if len(watcher.pending) >= embeddedWatchQueue {
			// Resync membership is informational; observers must call Members.
			watcher.pending = append(watcher.pending[:0], Event{
				Kind:   EventResync,
				Member: event.Member,
			})
		} else {
			watcher.pending = append(watcher.pending, event)
		}
		select {
		case watcher.wake <- struct{}{}:
		default:
		}
	}
}

func (c *EmbeddedCoordinator) deliverWatcher(
	ctx context.Context,
	id uint64,
	watcher *embeddedWatcher,
) {
	defer close(watcher.events)
	for {
		c.mu.Lock()
		if len(watcher.pending) == 0 {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				c.mu.Lock()
				delete(c.watchers, id)
				c.mu.Unlock()
				return
			case <-watcher.wake:
			}
			continue
		}
		event := watcher.pending[0]
		copy(watcher.pending, watcher.pending[1:])
		last := len(watcher.pending) - 1
		watcher.pending[last] = Event{}
		watcher.pending = watcher.pending[:last]
		c.mu.Unlock()

		select {
		case watcher.events <- event:
		case <-ctx.Done():
			c.mu.Lock()
			delete(c.watchers, id)
			c.mu.Unlock()
			return
		}
	}
}

type embeddedLease struct {
	coordinator *EmbeddedCoordinator
	lock        *dirlock.Lock

	mu       sync.Mutex
	token    AuthorityToken
	active   bool
	lost     chan struct{}
	lostOnce sync.Once
}

func (l *embeddedLease) Token() AuthorityToken {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.token
}

func (l *embeddedLease) Lost() <-chan struct{} { return l.lost }

func (l *embeddedLease) Renew(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// The held OS lock is the live lease. The manifest is durable fencing state
	// and is revalidated only while acquiring or advancing that fence.
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return ErrLeaseLost
	}
	return nil
}

func (l *embeddedLease) AdvanceEpoch(ctx context.Context, next uint64) (AuthorityToken, error) {
	if err := ctx.Err(); err != nil {
		return AuthorityToken{}, err
	}
	l.coordinator.mu.Lock()
	defer l.coordinator.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active || l.coordinator.current != l {
		return AuthorityToken{}, ErrLeaseLost
	}
	if next <= l.token.Epoch {
		return AuthorityToken{}, fmt.Errorf(
			"coordination: epoch %d does not advance current epoch %d",
			next, l.token.Epoch,
		)
	}
	if l.token.Generation == math.MaxUint64 {
		return AuthorityToken{}, errors.New("coordination: embedded manifest generation exhausted")
	}
	current, found, err := readEmbeddedManifest(l.coordinator.directory)
	if err != nil {
		return AuthorityToken{}, errors.Join(ErrLeaseLost, err, l.loseLocked(EventLost))
	}
	if !found || !current.Active || current.token() != l.token {
		return AuthorityToken{}, errors.Join(ErrLeaseLost, l.loseLocked(EventLost))
	}
	nextToken := l.token
	nextToken.Epoch = next
	nextToken.Generation++
	if err := writeEmbeddedManifest(
		l.coordinator.directory,
		manifestForToken(nextToken, true),
	); err != nil {
		return AuthorityToken{}, errors.Join(ErrLeaseLost, err, l.loseLocked(EventLost))
	}
	l.token = nextToken
	l.coordinator.publishLocked(Event{
		Kind:   EventFenced,
		Member: Member{ID: nextToken.Owner, Token: nextToken},
	})
	return nextToken, nil
}

func (l *embeddedLease) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.coordinator.mu.Lock()
	defer l.coordinator.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.active {
		return nil
	}

	token := l.token
	var persistErr error
	current, found, err := readEmbeddedManifest(l.coordinator.directory)
	if err != nil {
		persistErr = err
	} else if !found || !current.Active || current.token() != token {
		persistErr = ErrLeaseLost
	} else {
		current.Active = false
		persistErr = writeEmbeddedManifest(l.coordinator.directory, current)
	}
	lockErr := l.loseLocked(EventReleased)
	return errors.Join(persistErr, lockErr)
}

func (l *embeddedLease) loseLocked(kind EventKind) error {
	if !l.active {
		return nil
	}
	l.active = false
	if l.coordinator.current == l {
		l.coordinator.current = nil
	}
	l.lostOnce.Do(func() { close(l.lost) })
	l.coordinator.publishLocked(Event{
		Kind:   kind,
		Member: Member{ID: l.token.Owner, Token: l.token},
	})
	lock := l.lock
	l.lock = nil
	return lock.Close()
}

func manifestForToken(token AuthorityToken, active bool) embeddedManifest {
	return embeddedManifest{
		Version:    embeddedVersion,
		Domain:     token.Domain,
		Resource:   token.Resource,
		Owner:      token.Owner,
		LeaseID:    token.LeaseID,
		Epoch:      token.Epoch,
		Generation: token.Generation,
		Attempt:    token.Attempt,
		Active:     active,
	}
}

func readEmbeddedManifest(directory string) (embeddedManifest, bool, error) {
	path := filepath.Join(directory, embeddedManifestFile)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return embeddedManifest{}, false, nil
	}
	if err != nil {
		return embeddedManifest{}, false, fmt.Errorf("coordination: open embedded manifest: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return embeddedManifest{}, false, fmt.Errorf("coordination: stat embedded manifest: %w", err)
	}
	if info.Size() > embeddedManifestSize {
		return embeddedManifest{}, false, errors.New("coordination: embedded manifest is too large")
	}
	decoder := json.NewDecoder(io.LimitReader(file, embeddedManifestSize))
	decoder.DisallowUnknownFields()
	var manifest embeddedManifest
	if err := decoder.Decode(&manifest); err != nil {
		return embeddedManifest{}, false, fmt.Errorf("coordination: decode embedded manifest: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return embeddedManifest{}, false, err
	}
	if manifest.Version != embeddedVersion {
		return embeddedManifest{}, false, fmt.Errorf(
			"coordination: unsupported embedded manifest version %d",
			manifest.Version,
		)
	}
	if err := manifest.token().Validate(); err != nil {
		return embeddedManifest{}, false, fmt.Errorf("coordination: invalid embedded manifest: %w", err)
	}
	return manifest, true, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("coordination: embedded manifest has trailing JSON")
		}
		return fmt.Errorf("coordination: decode embedded manifest trailer: %w", err)
	}
	return nil
}

func writeEmbeddedManifest(directory string, manifest embeddedManifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("coordination: encode embedded manifest: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(directory, embeddedManifestFile)
	if err := storage.WriteAll(context.Background(), local.New(), path, data); err != nil {
		return fmt.Errorf("coordination: persist embedded manifest: %w", err)
	}
	return nil
}
