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

// Package lexiconscope carries the one capability that turns an authorized
// node filter's output into lexicon.ScopedNodes. pkg/lexicon installs Seal at
// init; pkg/explorer/authorized calls it. Being internal to the module, it is
// not reachable by API consumers, so a pinned lexicon bundle cannot be built
// from a node set that did not pass the authorized filter.
package lexiconscope

import (
	"time"

	"github.com/phrocker/shoal-oss/pkg/graph"
)

// Seal returns a lexicon.ScopedNodes (as any, to avoid an import cycle). It
// is set when pkg/lexicon is initialized, which importing pkg/lexicon
// guarantees happens first.
var Seal func(
	nodes []graph.Node, snapshotID string, asOf time.Time, frontier uint64,
	digest [32]byte,
) any
