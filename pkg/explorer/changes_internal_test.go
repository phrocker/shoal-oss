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

package explorer

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestChangesBelowRetentionFloorResynchronises exercises the retention-floor
// branch directly. The embedded corpus never prunes today, so the floor is
// raised manually to simulate a retention pass. A cursor that names changes
// below the floor cannot be answered without a silent gap and must be refused
// with a resynchronise conflict rather than returning a partial answer.
func TestChangesBelowRetentionFloorResynchronises(t *testing.T) {
	ctx := context.Background()
	corpus, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer corpus.Close()

	for i := 1; i <= 5; i++ {
		source := Source{
			URI:       fmt.Sprintf("file:///floor-%02d.md", i),
			MediaType: MediaTypeMarkdown,
			Content:   fmt.Sprintf("# Floor %02d\n\nSynthetic body.\n", i),
		}
		if _, err := corpus.Ingest(ctx, source); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}

	corpus.mu.Lock()
	corpus.changeHistoryFloor = 3
	corpus.mu.Unlock()

	// Since 1 asks for changes starting at sequence 2, which is below the floor
	// of 3: sequence 2 has been pruned, so answering would drop it silently.
	_, err = corpus.Changes(ctx, ChangeRequest{Since: 1})
	if err == nil {
		t.Fatal("expected resynchronise conflict for an aged-out cursor, got nil")
	}
	if !shoal.IsErrorCode(err, shoal.ErrorConflict) {
		t.Fatalf("error code = %v, want conflict", err)
	}
	if !strings.Contains(err.Error(), "retained history floor") {
		t.Fatalf("error %q does not name the retained history floor", err.Error())
	}

	// A cursor exactly at the floor boundary (Since 2 asks for sequence 3) is
	// still fully answerable and must succeed.
	feed, err := corpus.Changes(ctx, ChangeRequest{Since: 2})
	if err != nil {
		t.Fatalf("cursor at the floor boundary must succeed, got %v", err)
	}
	if len(feed.Changes) == 0 || feed.Changes[0].Sequence != 3 {
		t.Fatalf("boundary feed = %+v, want first change at sequence 3", feed.Changes)
	}
}
