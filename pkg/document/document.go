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

// Package document defines Shoal's public hierarchical document contract.
package document

import (
	"fmt"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// SourcePosition identifies a zero-based content offset and, when known, a
// one-based source page. Page zero means the source has no page information.
type SourcePosition struct {
	Offset int64
	Page   int32
}

// SourceRange is a half-open source interval [Start, End).
type SourceRange struct {
	Start SourcePosition
	End   SourcePosition
}

// Validate checks the ordering and coordinate invariants of a source range.
func (r SourceRange) Validate() error {
	if r.Start.Offset < 0 || r.End.Offset < r.Start.Offset {
		return shoal.NewError(shoal.ErrorInvalidArgument, "invalid source offsets")
	}
	if r.Start.Page < 0 || r.End.Page < 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "source pages cannot be negative")
	}
	if r.Start.Page > 0 && r.End.Page > 0 && r.End.Page < r.Start.Page {
		return shoal.NewError(shoal.ErrorInvalidArgument, "invalid source page range")
	}
	return nil
}

// Revision identifies an immutable version of a document.
type Revision struct {
	ID            shoal.ID
	DocumentID    shoal.ID
	CreatedAt     time.Time
	SourceVersion string
	Metadata      shoal.Metadata
}

// Document is the revision-specific root of a hierarchical source.
type Document struct {
	ID            shoal.ID
	RevisionID    shoal.ID
	Title         string
	RootSectionID shoal.ID
	Metadata      shoal.Metadata
}

// Section is an ordered node in a document tree. ParentID is empty only for
// the document's root section.
type Section struct {
	ID         shoal.ID
	DocumentID shoal.ID
	RevisionID shoal.ID
	ParentID   shoal.ID
	Order      uint32
	Heading    string
	Range      SourceRange
	Metadata   shoal.Metadata
}

// Span is an ordered, directly attributable piece of source content.
type Span struct {
	ID         shoal.ID
	DocumentID shoal.ID
	RevisionID shoal.ID
	SectionID  shoal.ID
	Order      uint32
	Range      SourceRange
	Text       string
	Metadata   shoal.Metadata
}

// Citation is an exact reference to evidence in one document revision.
type Citation struct {
	DocumentID shoal.ID
	RevisionID shoal.ID
	SectionID  shoal.ID
	SpanID     shoal.ID
	Range      SourceRange
}

// Validate checks that the citation can identify immutable source evidence.
func (c Citation) Validate() error {
	if c.DocumentID == "" || c.RevisionID == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "citation requires document and revision IDs")
	}
	if c.SectionID == "" && c.SpanID == "" {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "citation requires a section or span ID")
	}
	if err := c.Range.Validate(); err != nil {
		return fmt.Errorf("citation: %w", err)
	}
	return nil
}
