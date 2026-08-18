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

import "fmt"

// LockID identifies one held Accumulo ServiceLock. It mirrors the ephemeral
// ZooKeeper node a lock holder creates — "zlock#<uuid>#<sequence>" — where
// UUID is the holder's lock identity and Sequence is the ZooKeeper sequence
// number of the node.
//
// Sequence is the generation counter the fence relies on. ZooKeeper hands out
// strictly increasing sequence numbers per parent znode, so a lock that was
// lost and re-acquired always carries a higher sequence than the one it
// replaced, and a request stamped with a lower sequence is provably stale.
type LockID struct {
	UUID     string
	Sequence int64
}

// Valid reports whether the lock identity is usable for fencing. A lock with
// no UUID or a negative sequence names nothing and is never trusted.
func (l LockID) Valid() bool {
	return l.UUID != "" && l.Sequence >= 0
}

// Equal reports whether two lock identities are the same held lock.
func (l LockID) Equal(other LockID) bool {
	return l.UUID == other.UUID && l.Sequence == other.Sequence
}

// Supersedes reports whether l is a later generation than other.
func (l LockID) Supersedes(other LockID) bool {
	return l.Sequence > other.Sequence
}

// String renders the lock in its ZooKeeper node form.
func (l LockID) String() string {
	if !l.Valid() {
		return "zlock#<none>"
	}
	return fmt.Sprintf("zlock#%s#%010d", l.UUID, l.Sequence)
}

// Fence is the authority stamp that must accompany every manager-directed
// lifecycle transition.
//
// Server is the tablet-server ServiceLock the manager believed this process
// held when it made the decision. It must match the lock actually held now:
// a request minted against an earlier generation was made against a view of
// the cluster that no longer exists, and applying it could re-host a tablet
// the manager has since given to somebody else.
//
// Manager is the ServiceLock of the manager that issued the request. It exists
// to keep a superseded manager from countermanding the live one — not to
// second-guess the live manager, whose decisions are always applied. A newer
// manager lock is adopted on sight.
type Fence struct {
	Server  LockID
	Manager LockID
}
