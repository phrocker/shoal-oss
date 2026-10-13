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
	"testing"

	"github.com/phrocker/shoal-oss/internal/tablet"
)

func TestImportLineage(t *testing.T) {
	const epoch = "0123456789abcdef0123456789abcdef"
	want := tablet.ProducerLineage{Producer: "graph/t-0000/agentA", Epoch: epoch, Millis: 1700000000000}
	if got := importLineage(RFileExportFile{RelativePath: "graph/t-0000/agentA~F1700000000000.rf", ProducerEpoch: epoch}); got != want {
		t.Fatalf("flush name: %+v", got)
	}
	want.Millis = 1700000000001
	if got := importLineage(RFileExportFile{RelativePath: "graph/t-0000/agentA~C1700000000001-002.parquet", ProducerEpoch: epoch}); got != want {
		t.Fatalf("compaction name: %+v", got)
	}
	for name, file := range map[string]RFileExportFile{
		"no epoch":         {RelativePath: "graph/t-0000/agentA~F1700000000000.rf"},
		"bad epoch":        {RelativePath: "graph/t-0000/agentA~F1700000000000.rf", ProducerEpoch: "UPPER-case/../x"},
		"short epoch":      {RelativePath: "graph/t-0000/agentA~F1700000000000.rf", ProducerEpoch: "abc"},
		"no prefix":        {RelativePath: "graph/t-0000/F1700000000000.rf", ProducerEpoch: epoch},
		"empty prefix":     {RelativePath: "graph/t-0000/~F1700000000000.rf", ProducerEpoch: epoch},
		"chained import":   {RelativePath: "graph/t-0000/agentA~.shoal-import-" + epoch + "-00.rf", ProducerEpoch: epoch},
		"relayed prefix":   {RelativePath: "graph/t-0000/agentA~agentB~F1700000000000.rf", ProducerEpoch: epoch},
		"no relative path": {ProducerEpoch: epoch},
	} {
		if got := importLineage(file); got != (tablet.ProducerLineage{}) {
			t.Errorf("%s: lineage %+v, want none", name, got)
		}
	}
}

func TestProducerEpoch(t *testing.T) {
	const instance = "0123456789abcdef0123456789abcdef"
	if got := producerEpoch(instance, RFileExportOptions{}); got != "" {
		t.Fatalf("epoch without a ProducerID: %q", got)
	}
	if got := producerEpoch("", RFileExportOptions{ProducerID: "p"}); got != "" {
		t.Fatalf("epoch for a file the producer did not mint: %q", got)
	}
	plain := producerEpoch(instance, RFileExportOptions{ProducerID: "p"})
	one := producerEpoch(instance, RFileExportOptions{ProducerID: "p", StampVisibilityLabel: "one"})
	two := producerEpoch(instance, RFileExportOptions{ProducerID: "p", StampVisibilityLabel: "two"})
	mode := producerEpoch(instance, RFileExportOptions{ProducerID: "p", StampVisibilityLabel: "one", StampMode: "whenEmpty"})
	seen := map[string]bool{}
	for _, e := range []string{plain, one, two, mode} {
		if !producerEpochRe.MatchString(e) || seen[e] {
			t.Fatalf("epochs %q %q %q %q must be valid and distinct", plain, one, two, mode)
		}
		seen[e] = true
	}
}
