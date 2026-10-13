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

package engine

import (
	"crypto/sha256"
	"encoding/hex"
)

// shipKey names the bytes an export of one file produces: the producer
// instance the file's name was minted under (see internal/tablet/instance.go;
// empty for a file the tablet did not mint) and the export transform (a
// stamped copy has different bytes than a plain one under the same name).
// Two exports of one path with the same key ship the same bytes; with a
// different key they may not, so incremental sync re-ships instead of reusing
// the copy it shipped earlier. It does not depend on the ProducerID: a
// recreated or cloned producer reissues names with or without one.
func shipKey(instance string, opts RFileExportOptions) string {
	if opts.StampVisibilityLabel == "" {
		if instance == "" {
			return "copy"
		}
		return "copy:" + instance
	}
	sum := sha256.Sum256([]byte("stamp\x00" + instance + "\x00" +
		opts.StampVisibilityLabel + "\x00" + opts.StampMode))
	return "stamp:" + hex.EncodeToString(sum[:])
}

// producerEpoch is the name epoch an export records for a file
// (RFileExportFile.ProducerEpoch): the producer instance the file was minted
// under, bound to the export transform. Empty without a ProducerID (names are
// only unique per producer, and only the prefix says which producer) or for a
// file the tablet did not mint (an import, an adopted stray).
func producerEpoch(instance string, opts RFileExportOptions) string {
	if opts.ProducerID == "" || instance == "" {
		return ""
	}
	if opts.StampVisibilityLabel == "" {
		return instance
	}
	sum := sha256.Sum256([]byte("stamp\x00" + instance + "\x00" +
		opts.StampVisibilityLabel + "\x00" + opts.StampMode))
	return "s" + hex.EncodeToString(sum[:])[:47]
}
