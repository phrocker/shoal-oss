// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package explorerfleet

import (
	"time"

	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/evidencelabels"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// ComposeDispatch constructs the production dispatch service over the shared
// embedded runtime. Recorder and events are explicit fail-closed host seams:
// production startup must provide the durable interaction and fleet-event
// adapters rather than silently substituting no-op implementations.
func ComposeDispatch(
	runtime *explorercoord.Runtime,
	registry *fleet.Service,
	resolver auth.Resolver,
	recorder fleet.ActionRecorder,
	events fleet.ActionEventPublisher,
	visibility []byte,
	clock func() time.Time,
) (*fleet.DispatchService, error) {
	return ComposeDispatchWithAttestations(runtime, registry, resolver,
		recorder, events, visibility, clock, nil, DispatchLabels{})
}

// DispatchLabels is the reader label evaluator the dispatch plane uses
// (#564). Both are optional and fail closed: with no Visibility every
// labelled evidence reference is withheld from every read, and with no
// Translator an executor's free-form labels are recorded as reported, which
// no reader holds.
type DispatchLabels struct {
	// Visibility decides, per reader, whether a stored evidence reference's
	// labels are held (authorized.LabelVisibility).
	Visibility evidencelabels.Visibility
	// Translator rewrites an executor's free-form labels into structured
	// label-policy terms when its evidence is recorded
	// (authorized.LabelTranslator).
	Translator evidencelabels.Translator
	// Nodes decides a reference that names nodes by their current access
	// rules (authorized.NodeGate). Nil withholds every such reference.
	Nodes evidencelabels.NodeGate
}

// ComposeDispatchWithAttestations is ComposeDispatch with the executor
// attestation read every claim on an attestation-required action makes. Nil
// means none is ever current, so those claims are refused. labels wires the
// reader label evaluator into every dispatch read and the record-time label
// translation into every completion.
func ComposeDispatchWithAttestations(
	runtime *explorercoord.Runtime,
	registry *fleet.Service,
	resolver auth.Resolver,
	recorder fleet.ActionRecorder,
	events fleet.ActionEventPublisher,
	visibility []byte,
	clock func() time.Time,
	attestations fleet.ExecutorAttestations,
	labels DispatchLabels,
	options ...DispatchOption,
) (*fleet.DispatchService, error) {
	store, err := NewDispatchStore(runtime, visibility)
	if err != nil {
		return nil, err
	}
	var optional dispatchOptions
	for _, option := range options {
		option(&optional)
	}
	return fleet.NewDispatchService(fleet.DispatchConfig{
		Store: store, Registry: registry, Resolver: resolver,
		Recorder: recorder, Events: events, Clock: clock,
		Attestations:          attestations,
		EvidenceVisibility:    labels.Visibility,
		EvidenceLabels:        labels.Translator,
		EvidenceNodes:         labels.Nodes,
		ClosedErrorVocabulary: optional.closedErrorVocabulary,
	})
}

// DispatchOption is optional dispatch configuration. An option rather than
// another positional parameter, because the one thing it carries is a bool
// and a bare bool at a call site says nothing about which setting it is.
type DispatchOption func(*dispatchOptions)

type dispatchOptions struct{ closedErrorVocabulary bool }

// WithClosedErrorVocabulary adjudicates an executor-reported failure code
// the action's descriptor does not declare, which is what closes the
// channel #740 is about (phase 2). Without it a reported code is checked
// only against the token shape, which narrows that channel without closing
// it: a lowercase token can still read "patient_4471_hiv_positive".
//
// Enable it only after every action an executor fails has declared its
// codes. Declaring changes the registry digest and so the descriptor's
// generation, and a record pinned to the previous generation is refused at
// claim — so the sequence is declare, let in-flight work settle, then flip.
func WithClosedErrorVocabulary() DispatchOption {
	return func(options *dispatchOptions) {
		options.closedErrorVocabulary = true
	}
}
