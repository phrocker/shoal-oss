// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package devbackfill holds the capability that admits the development-only
// policy-catalog backfill.
//
// TEMPORARY (issue #284): the backfill exists only because PolicyStore has no
// durable implementation, so a corpus that outlives the process becomes
// invisible. Delete this package, and the method that requires it, when #284
// lands a durable catalog.
//
// The package is module-internal on purpose. It is the reachability barrier
// for the backfill: no module outside this repository can import it, so no
// external consumer of pkg/explorer/authorized can name *Capability, and the
// method that takes one is therefore uncallable to them with anything but nil,
// which is refused.
package devbackfill

// Capability is unforgeable evidence that the development-only backfill was
// requested from the sanctioned startup gate. Only NewCapability produces a
// granted one: the field is unexported, so a zero value obtained through
// reflection is not granted, and only code inside this module can reach the
// constructor at all.
type Capability struct {
	granted bool
}

// NewCapability mints the capability. Callers must have already established
// that the process is running the development principal on a loopback
// listener; this constructor asserts nothing on its own.
func NewCapability() *Capability {
	return &Capability{granted: true}
}

// Granted reports whether this capability came from NewCapability. A nil or
// zero-valued capability is not granted, so the backfill fails closed.
func (c *Capability) Granted() bool {
	return c != nil && c.granted
}
