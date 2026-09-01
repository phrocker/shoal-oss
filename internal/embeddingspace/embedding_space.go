// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package embeddingspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	MaxIdentityBytes = 16 * 1024

	StateHasEmbeddings State = "has_embeddings"
	StateNoEmbeddings  State = "no_embeddings"
	StateUnknown       State = "unknown"

	RFileMetaBlockName  = "shoal.embedding-space"
	ParquetMetadataKey  = "shoal.embedding_space"
	TableTargetProperty = "table.shoal.embedding.target_space"

	// JobFileStatesProperty carries the per-file embedding-space column
	// through an external compaction job.
	//
	// Accumulo's TExternalCompactionJob describes each input with a
	// DataFileValue and nothing else, and Phase 1 deliberately refused to
	// extend DataFileValue, so there is no per-file slot on the wire. The
	// job's property-override map is the only extension point that does
	// not change the Thrift contract, which is why the column travels as
	// one property whose value is a JSON object keyed by the exact
	// metadata file entry each input was named by.
	//
	// An input the property does not mention is *absent*, which is
	// weaker than an explicit unknown: nothing was recorded about it at
	// all. Absence must not be turned into a recorded "unknown", because
	// a recorded state is cross-checked against the file footer and a
	// synthesised one would fail that check for every file written
	// before this column existed. Absence means "no cross-check
	// available"; the footer alone decides.
	JobFileStatesProperty = "table.shoal.embedding.file_states"

	// JobEpochProperty carries the migration epoch a compaction belongs
	// to through an external compaction job.
	//
	// It is what binds an output to the migration that produced it. A
	// converger serving epoch E refuses a compaction stamped E', so a
	// target change that opens a new epoch cannot have in-flight
	// compactions from the old one publish files labelled with the old
	// target — the corpus converges toward one target at a time instead
	// of oscillating between two.
	JobEpochProperty = "table.shoal.embedding.epoch"

	// MaxJobEpochBytes bounds the encoded epoch identifier. Like the
	// per-file column it arrives from a coordinator before any budget
	// check has run.
	MaxJobEpochBytes = 512

	// MaxJobFileStatesBytes bounds the encoded per-file column. The value
	// arrives from a coordinator and is decoded before any budget check
	// has run, so it carries its own cap.
	MaxJobFileStatesBytes = 1 << 20
)

var (
	ErrInvalidState = errors.New("embedding space: invalid state")
	ErrMismatch     = errors.New("embedding space: identity mismatch")
	ErrIntegrity    = errors.New("embedding space: metadata integrity error")
)

type State string

type FileState struct {
	State    State  `json:"state"`
	Identity string `json:"identity,omitempty"`
}

type MismatchError struct {
	Operation string
	Left      FileState
	Right     FileState
}

func (e *MismatchError) Error() string {
	if e == nil {
		return "<nil>"
	}
	op := e.Operation
	if strings.TrimSpace(op) == "" {
		op = "compare"
	}
	return fmt.Sprintf("%s: %v: %s vs %s", op, ErrMismatch, e.Left.String(), e.Right.String())
}

func (e *MismatchError) Unwrap() error { return ErrMismatch }

type IntegrityError struct {
	Metadata FileState
	Footer   FileState
}

func (e *IntegrityError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v: metadata entry %s disagrees with file footer %s",
		ErrIntegrity, e.Metadata.String(), e.Footer.String())
}

func (e *IntegrityError) Unwrap() error { return ErrIntegrity }

func Has(identity string) FileState {
	return FileState{State: StateHasEmbeddings, Identity: strings.TrimSpace(identity)}
}

func NoEmbeddings() FileState { return FileState{State: StateNoEmbeddings} }

func Unknown() FileState { return FileState{State: StateUnknown} }

func (s FileState) HasEmbeddings() bool { return s.State == StateHasEmbeddings }

func (s FileState) Known() bool { return s.State == StateHasEmbeddings || s.State == StateNoEmbeddings }

func (s FileState) String() string {
	switch s.State {
	case StateHasEmbeddings:
		return string(s.State) + ":" + s.Identity
	case StateNoEmbeddings, StateUnknown:
		return string(s.State)
	default:
		return "<invalid>"
	}
}

func (s FileState) Validate() error {
	switch s.State {
	case StateHasEmbeddings:
		identity := strings.TrimSpace(s.Identity)
		if identity == "" || !utf8.ValidString(identity) || len(identity) > MaxIdentityBytes {
			return fmt.Errorf("%w: invalid embedding identity", ErrInvalidState)
		}
		if identity != s.Identity {
			return fmt.Errorf("%w: identity has surrounding whitespace", ErrInvalidState)
		}
	case StateNoEmbeddings, StateUnknown:
		if s.Identity != "" {
			return fmt.Errorf("%w: identity set for %s", ErrInvalidState, s.State)
		}
	default:
		return fmt.Errorf("%w: %q", ErrInvalidState, s.State)
	}
	return nil
}

func Encode(s FileState) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(s)
}

func Decode(b []byte) (FileState, error) {
	if len(b) == 0 {
		return FileState{}, fmt.Errorf("%w: empty value", ErrInvalidState)
	}
	var s FileState
	if err := json.Unmarshal(b, &s); err != nil {
		return FileState{}, fmt.Errorf("%w: decode JSON: %v", ErrInvalidState, err)
	}
	if err := s.Validate(); err != nil {
		return FileState{}, err
	}
	return s, nil
}

func Compatible(operation string, states ...FileState) (FileState, error) {
	for _, state := range states {
		if err := state.Validate(); err != nil {
			return FileState{}, err
		}
	}
	// Conflicting identities are reported even when an unknown or
	// embedding-free input sits between them. Folding the merge and the
	// conflict check into one pass would let the absorbing unknown state
	// mask a real mismatch, so that a misconfiguration surfaces or hides
	// depending on input order.
	var embedded FileState
	for _, state := range states {
		if state.State != StateHasEmbeddings {
			continue
		}
		if embedded.State != StateHasEmbeddings {
			embedded = state
			continue
		}
		if embedded.Identity != state.Identity {
			return FileState{}, &MismatchError{Operation: operation, Left: embedded, Right: state}
		}
	}
	unresolved := false
	for _, state := range states {
		if state.State != StateHasEmbeddings {
			unresolved = true
			break
		}
	}
	switch {
	case len(states) == 0:
		return Unknown(), nil
	case embedded.State == StateHasEmbeddings && !unresolved:
		return embedded, nil
	case embedded.State == StateHasEmbeddings:
		return Unknown(), nil
	}
	for _, state := range states {
		if state.State == StateUnknown {
			return Unknown(), nil
		}
	}
	return NoEmbeddings(), nil
}

func EnsureSameIdentity(operation string, left, right string) error {
	l := Has(left)
	r := Has(right)
	if err := l.Validate(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if l.Identity != r.Identity {
		return &MismatchError{Operation: operation, Left: l, Right: r}
	}
	return nil
}

func VerifyMetadataMatchesFooter(metadataState, footerState FileState) error {
	if err := metadataState.Validate(); err != nil {
		return err
	}
	if err := footerState.Validate(); err != nil {
		return err
	}
	if metadataState != footerState {
		return &IntegrityError{Metadata: metadataState, Footer: footerState}
	}
	return nil
}
