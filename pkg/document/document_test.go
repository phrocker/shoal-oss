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

package document_test

import (
	"testing"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestCitationRequiresRevisionSpecificSource(t *testing.T) {
	citation := document.Citation{
		DocumentID: "doc-1",
		RevisionID: "rev-2",
		SectionID:  "section-3",
		Range: document.SourceRange{
			Start: document.SourcePosition{Offset: 12, Page: 2},
			End:   document.SourcePosition{Offset: 24, Page: 2},
		},
	}
	if err := citation.Validate(); err != nil {
		t.Fatalf("expected valid citation: %v", err)
	}

	citation.RevisionID = ""
	if err := citation.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestSourceRangeRejectsReversedCoordinates(t *testing.T) {
	sourceRange := document.SourceRange{
		Start: document.SourcePosition{Offset: 20, Page: 3},
		End:   document.SourcePosition{Offset: 10, Page: 2},
	}
	if err := sourceRange.Validate(); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}
