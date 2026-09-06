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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type rejectingPublicationAdapter struct {
	published bool
}

func (a *rejectingPublicationAdapter) PublishRecord(
	context.Context,
	RecordPublication,
) (RecordPublicationResult, error) {
	a.published = true
	return RecordPublicationResult{}, nil
}

func (*rejectingPublicationAdapter) RecordCommitted(
	context.Context,
	RecordPublication,
) (bool, error) {
	return false, nil
}

func (*rejectingPublicationAdapter) RecordHead(
	context.Context,
	byte,
	[]byte,
) (*RecordPublicationHead, error) {
	return nil, nil
}

func (*rejectingPublicationAdapter) RecordAttempt(
	context.Context,
	RecordPublication,
) (*RecordPublicationAttempt, error) {
	return nil, nil
}

func TestConfiguredPublicationRejectsOversizedDocumentWithoutDirectWrite(t *testing.T) {
	source := Source{
		URI: "file:///oversized.txt", MediaType: MediaTypeText,
		Content: strings.Repeat("x", 1<<20),
	}
	parsed, err := parseSource(source, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	record := &persistedDocument{
		Document: parsed.document, Revision: parsed.revision, Source: parsed.source,
		Sections: parsed.sections, Spans: parsed.spans, Nodes: parsed.nodes, Edges: parsed.edges,
		PublishedAt: time.Now().UTC(), PublicationSequence: 1,
	}
	adapter := &rejectingPublicationAdapter{}
	corpus := &Explorer{publication: adapter}
	err = corpus.writeDocumentRecord(
		context.Background(),
		documentRecordRow(record.Document.ID, record.Revision.ID),
		record,
		nil,
		nil,
	)
	if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) {
		t.Fatalf("configured oversized write = %v", err)
	}
	if adapter.published {
		t.Fatal("oversized record reached the publication adapter")
	}

	unconfigured, err := Open(publicationTestDirectory(t))
	if err != nil {
		t.Fatal(err)
	}
	defer unconfigured.Close()
	if _, err := unconfigured.Ingest(context.Background(), source); err != nil {
		t.Fatalf("legacy unconfigured oversized ingest = %v", err)
	}
}

func publicationTestDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp(".", ".publication-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}
