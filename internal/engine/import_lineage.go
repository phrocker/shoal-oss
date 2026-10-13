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
	"path"
	"regexp"
	"strings"

	"github.com/phrocker/shoal-oss/internal/tablet"
)

// producerEpochRe bounds what an importer accepts as a producer epoch.
var producerEpochRe = regexp.MustCompile(`^[0-9a-z]{16,64}$`)

// producerEpoch is the name epoch an export records for a file
// (RFileExportFile.ProducerEpoch): the instance the producer tablet minted
// the file's name under (see internal/tablet/lineage.go), bound to the
// export transform, since a stamped copy has different bytes than a plain
// one under the same name. Empty — no lineage, so the importer keeps an
// explicit entry — without a ProducerID (names are only per-producer
// monotonic, and only a prefix says which producer) or for a file the tablet
// did not mint itself (an import, an adopted stray).
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

// importLineage is a manifest file's place in its producer's name lineage
// (tablet.ImportedFile.Lineage), or the zero value when it has none: no
// epoch, no relative path, or a name that is not "<producer>~F<ms>" /
// "<producer>~C<ms>-<n>".
func importLineage(file RFileExportFile) tablet.ProducerLineage {
	if file.RelativePath == "" || !producerEpochRe.MatchString(file.ProducerEpoch) {
		return tablet.ProducerLineage{}
	}
	dir, base := path.Split(file.RelativePath)
	producer, name, ok := strings.Cut(base, "~")
	if !ok || validateProducerID(producer) != nil || producer == "" {
		return tablet.ProducerLineage{}
	}
	ms, ok := tablet.LocalFileNameMillis(name)
	if !ok {
		return tablet.ProducerLineage{}
	}
	return tablet.ProducerLineage{
		Producer: dir + producer,
		Epoch:    file.ProducerEpoch,
		Millis:   ms,
	}
}
