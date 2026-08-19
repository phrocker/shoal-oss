// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tserver

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	gozk "github.com/go-zookeeper/zk"
)

// zManagerLock is the manager's lock directory under the instance root. It
// mirrors Constants.ZMANAGER_LOCK, and is the same path internal/zk reads to
// resolve the manager's Thrift address.
const zManagerLock = "managers/lock"

// ErrNoManagerLock means the manager lock directory holds no lock: there is
// currently no manager. It is a fact about the cluster, not a read failure —
// the two are kept apart because only the first is grounds for withdrawing a
// manager's authority.
var ErrNoManagerLock = errors.New("tserver: no manager lock held")

// ManagerLockReader is the ZooKeeper read surface needed to see which manager
// holds the manager lock. *internal/zk.Locator satisfies it, so the same
// session that resolves tablet locations observes manager authority.
type ManagerLockReader interface {
	InstancePath() string
	Children(ctx context.Context, path string) ([]string, error)
}

// ReadManagerLock returns the identity of the manager that currently holds the
// manager ServiceLock.
//
// The holder is the lowest-sequence node in the lock directory, which is the
// same rule Accumulo applies and the same one internal/zk applies when it
// resolves the manager's address. Queued candidates — the standby managers
// waiting to take over — are not authority and are ignored.
//
// Returns ErrNoManagerLock when the directory is missing or holds no valid
// lock node.
func ReadManagerLock(ctx context.Context, reader ManagerLockReader) (LockID, error) {
	if reader == nil {
		return LockID{}, errors.New("tserver: nil manager lock reader")
	}
	if err := ctx.Err(); err != nil {
		return LockID{}, err
	}
	lockPath := path.Join(reader.InstancePath(), zManagerLock)
	children, err := reader.Children(ctx, lockPath)
	if err != nil {
		if errors.Is(err, gozk.ErrNoNode) {
			return LockID{}, ErrNoManagerLock
		}
		return LockID{}, fmt.Errorf("list manager locks %s: %w", lockPath, err)
	}
	sorted := sortLockNodes(children)
	if len(sorted) == 0 {
		return LockID{}, ErrNoManagerLock
	}
	id, ok := ParseLockNode(sorted[0])
	if !ok {
		// sortLockNodes only returns parseable names.
		return LockID{}, fmt.Errorf("%w: %q in %s", ErrInvalidLock, sorted[0], lockPath)
	}
	return id, nil
}

// WatchManagerLock keeps a Host's view of live manager authority in step with
// ZooKeeper until ctx ends, polling every interval.
//
// This is what makes manager-directed transitions possible at all: Host
// refuses every one of them until it has been told which manager is live, and
// it decides authority from this observation rather than from the requests it
// receives. A manager cannot talk its way into authority it does not hold.
//
// A read that fails leaves the previous observation in place. Failing to read
// ZooKeeper is not evidence that the manager changed, and withdrawing
// authority on a transient failure would refuse the live manager's assignments
// for no reason. A directory that is readable and holds no lock is evidence,
// and clears the observation.
//
// An observation the host refuses — an epoch older than one it has already
// seen — is dropped rather than retried. The host keeps the newer epoch, which
// is the safe direction: authority never moves backwards.
func WatchManagerLock(
	ctx context.Context,
	reader ManagerLockReader,
	host *Host,
	interval time.Duration,
) error {
	if reader == nil {
		return errors.New("tserver: nil manager lock reader")
	}
	if host == nil {
		return errors.New("tserver: nil host")
	}
	if interval <= 0 {
		return fmt.Errorf("tserver: manager lock poll interval %s must be positive", interval)
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		observeManagerLockOnce(ctx, reader, host)
		if err := ctx.Err(); err != nil {
			return err
		}
		timer.Reset(interval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// observeManagerLockOnce applies one reading of the manager lock to the host.
func observeManagerLockOnce(ctx context.Context, reader ManagerLockReader, host *Host) {
	id, err := ReadManagerLock(ctx, reader)
	switch {
	case err == nil:
		// A refusal means the host already knows a newer manager than this
		// reading; keeping the newer one is the fail-closed direction.
		_ = host.ObserveManagerLock(id)
	case errors.Is(err, ErrNoManagerLock):
		_ = host.ObserveManagerLock(LockID{})
	default:
		// Unreadable is not the same as absent: hold the last observation.
	}
}
