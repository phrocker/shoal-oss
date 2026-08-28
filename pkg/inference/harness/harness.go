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

// Package harness adapts a trusted, provider-neutral tool-using agent runtime
// to the grounded inference Generator contract.
package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxLogicalIDBytes  = 4096
	MaxToolQueryBytes  = 16 * 1024
	MaxTranscriptSteps = 1024
)

var (
	ErrInvalid           = errors.New("invalid agent harness value")
	ErrBudgetExhausted   = errors.New("agent harness budget exhausted")
	ErrRunnerUnavailable = errors.New("agent harness runner unavailable")
)

type ActionKind string

const (
	ActionRetrieve    ActionKind = "retrieve"
	ActionOpenSection ActionKind = "open_section"
	ActionNeighbors   ActionKind = "neighbors"
	ActionStop        ActionKind = "stop"
)

type Budgets struct {
	MaxSteps          int
	MaxElapsed        time.Duration
	MaxInputTokens    int
	MaxOutputTokens   int
	MaxGraphHops      int
	MaxFanout         int
	MaxRepeatedAction int
}

func (b Budgets) validate() error {
	if b.MaxSteps <= 0 || b.MaxSteps > MaxTranscriptSteps {
		return invalid("max steps is outside the supported range")
	}
	if b.MaxElapsed <= 0 {
		return invalid("max elapsed must be positive")
	}
	if b.MaxInputTokens < 0 || b.MaxOutputTokens < 0 || b.MaxGraphHops < 0 ||
		b.MaxFanout <= 0 || b.MaxRepeatedAction <= 0 {
		return invalid("budget values are outside the supported range")
	}
	return nil
}

type Provenance struct {
	harness    string
	provider   string
	model      string
	prompt     inference.PromptProvenance
	toolPolicy string
}

func NewProvenance(harness, provider, model string, prompt inference.PromptProvenance, toolPolicy string) (Provenance, error) {
	p := Provenance{
		harness: strings.TrimSpace(harness), provider: strings.TrimSpace(provider),
		model: strings.TrimSpace(model), prompt: prompt, toolPolicy: strings.TrimSpace(toolPolicy),
	}
	for name, value := range map[string]string{
		"harness": p.harness, "provider": p.provider, "model": p.model, "tool policy": p.toolPolicy,
	} {
		if err := boundedString(name, value, shoal.MaxSemanticStringBytes); err != nil {
			return Provenance{}, err
		}
	}
	if err := prompt.Validate(); err != nil {
		return Provenance{}, err
	}
	return p, nil
}

func (p Provenance) Harness() string                    { return p.harness }
func (p Provenance) Provider() string                   { return p.provider }
func (p Provenance) Model() string                      { return p.model }
func (p Provenance) Prompt() inference.PromptProvenance { return p.prompt }
func (p Provenance) ToolPolicy() string                 { return p.toolPolicy }
func (p Provenance) validate() error {
	_, err := NewProvenance(p.harness, p.provider, p.model, p.prompt, p.toolPolicy)
	return err
}

type SessionRequest struct {
	id         shoal.ID
	context    inference.ContextPack
	budgets    Budgets
	provenance Provenance
}

func newSessionRequest(pack inference.ContextPack, budgets Budgets, provenance Provenance) (SessionRequest, error) {
	if err := pack.Validate(); err != nil {
		return SessionRequest{}, err
	}
	if err := budgets.validate(); err != nil {
		return SessionRequest{}, err
	}
	if err := provenance.validate(); err != nil {
		return SessionRequest{}, err
	}
	id := deriveID("session-request", string(pack.ID()), canonicalBudgets(budgets),
		provenance.harness, provenance.provider, provenance.model,
		provenance.prompt.TemplateID(), provenance.prompt.Version(), provenance.prompt.Hash(),
		provenance.toolPolicy)
	return SessionRequest{id: id, context: pack, budgets: budgets, provenance: provenance}, nil
}

func (r SessionRequest) ID() shoal.ID                   { return r.id }
func (r SessionRequest) Context() inference.ContextPack { return clonePack(r.context) }
func (r SessionRequest) Budgets() Budgets               { return r.budgets }
func (r SessionRequest) Provenance() Provenance         { return r.provenance }

type RetrieveRequest struct {
	query string
	limit int
}

func NewRetrieveRequest(query string, limit int) (RetrieveRequest, error) {
	query = strings.Join(strings.Fields(query), " ")
	if err := boundedString("retrieve query", query, MaxToolQueryBytes); err != nil {
		return RetrieveRequest{}, err
	}
	if limit <= 0 {
		return RetrieveRequest{}, invalid("retrieve limit must be positive")
	}
	return RetrieveRequest{query: query, limit: limit}, nil
}
func (r RetrieveRequest) Query() string { return r.query }
func (r RetrieveRequest) Limit() int    { return r.limit }

type OpenSectionRequest struct {
	documentID shoal.ID
	sectionID  shoal.ID
}

func NewOpenSectionRequest(documentID, sectionID shoal.ID) (OpenSectionRequest, error) {
	if err := validateLogicalID("document ID", documentID); err != nil {
		return OpenSectionRequest{}, err
	}
	if err := validateLogicalID("section ID", sectionID); err != nil {
		return OpenSectionRequest{}, err
	}
	return OpenSectionRequest{documentID: documentID, sectionID: sectionID}, nil
}
func (r OpenSectionRequest) DocumentID() shoal.ID { return r.documentID }
func (r OpenSectionRequest) SectionID() shoal.ID  { return r.sectionID }

type NeighborsRequest struct {
	nodeID shoal.ID
	hops   int
	fanout int
}

func NewNeighborsRequest(nodeID shoal.ID, hops, fanout int) (NeighborsRequest, error) {
	if err := validateLogicalID("node ID", nodeID); err != nil {
		return NeighborsRequest{}, err
	}
	if hops <= 0 || fanout <= 0 {
		return NeighborsRequest{}, invalid("neighbors hops and fanout must be positive")
	}
	return NeighborsRequest{nodeID: nodeID, hops: hops, fanout: fanout}, nil
}
func (r NeighborsRequest) NodeID() shoal.ID { return r.nodeID }
func (r NeighborsRequest) Hops() int        { return r.hops }
func (r NeighborsRequest) Fanout() int      { return r.fanout }

type Usage struct {
	InputTokens  int
	OutputTokens int
}

type Action struct {
	kind        ActionKind
	correlation shoal.ID
	retrieve    RetrieveRequest
	open        OpenSectionRequest
	neighbors   NeighborsRequest
	result      inference.InferenceResult
	usage       Usage
}

func NewRetrieveAction(correlation shoal.ID, request RetrieveRequest, usage Usage) (Action, error) {
	a := Action{kind: ActionRetrieve, correlation: correlation, retrieve: request, usage: usage}
	return validatedAction(a)
}
func NewOpenSectionAction(correlation shoal.ID, request OpenSectionRequest, usage Usage) (Action, error) {
	a := Action{kind: ActionOpenSection, correlation: correlation, open: request, usage: usage}
	return validatedAction(a)
}
func NewNeighborsAction(correlation shoal.ID, request NeighborsRequest, usage Usage) (Action, error) {
	a := Action{kind: ActionNeighbors, correlation: correlation, neighbors: request, usage: usage}
	return validatedAction(a)
}
func NewStopAction(correlation shoal.ID, result inference.InferenceResult, usage Usage) (Action, error) {
	a := Action{kind: ActionStop, correlation: correlation, result: result, usage: usage}
	return validatedAction(a)
}
func validatedAction(a Action) (Action, error) {
	if err := a.validate(); err != nil {
		return Action{}, err
	}
	return a, nil
}
func (a Action) Kind() ActionKind                        { return a.kind }
func (a Action) CorrelationID() shoal.ID                 { return a.correlation }
func (a Action) Usage() Usage                            { return a.usage }
func (a Action) Retrieve() (RetrieveRequest, bool)       { return a.retrieve, a.kind == ActionRetrieve }
func (a Action) OpenSection() (OpenSectionRequest, bool) { return a.open, a.kind == ActionOpenSection }
func (a Action) Neighbors() (NeighborsRequest, bool)     { return a.neighbors, a.kind == ActionNeighbors }
func (a Action) Result() (inference.InferenceResult, bool) {
	return cloneResult(a.result), a.kind == ActionStop
}

func (a Action) validate() error {
	if err := validateLogicalID("correlation ID", a.correlation); err != nil {
		return err
	}
	if a.usage.InputTokens < 0 || a.usage.OutputTokens < 0 {
		return invalid("token usage cannot be negative")
	}
	switch a.kind {
	case ActionRetrieve:
		if _, err := NewRetrieveRequest(a.retrieve.query, a.retrieve.limit); err != nil {
			return err
		}
	case ActionOpenSection:
		if _, err := NewOpenSectionRequest(a.open.documentID, a.open.sectionID); err != nil {
			return err
		}
	case ActionNeighbors:
		if _, err := NewNeighborsRequest(a.neighbors.nodeID, a.neighbors.hops, a.neighbors.fanout); err != nil {
			return err
		}
	case ActionStop:
		if err := a.result.Validate(); err != nil {
			return err
		}
	default:
		return invalid("unknown or forbidden action")
	}
	return nil
}

type ToolResult struct {
	correlation shoal.ID
	kind        ActionKind
	anchors     []inference.EvidenceAnchor
	snapshot    inference.SnapshotPin
	auth        inference.AuthPin
}

func NewToolResult(correlation shoal.ID, kind ActionKind, anchors []inference.EvidenceAnchor, snapshot inference.SnapshotPin, auth inference.AuthPin) (ToolResult, error) {
	r := ToolResult{correlation: correlation, kind: kind, anchors: append([]inference.EvidenceAnchor(nil), anchors...), snapshot: snapshot, auth: auth}
	sort.Slice(r.anchors, func(i, j int) bool { return shoal.CompareID(r.anchors[i].ID(), r.anchors[j].ID()) < 0 })
	if err := r.validate(); err != nil {
		return ToolResult{}, err
	}
	return r, nil
}
func (r ToolResult) CorrelationID() shoal.ID { return r.correlation }
func (r ToolResult) Kind() ActionKind        { return r.kind }
func (r ToolResult) Anchors() []inference.EvidenceAnchor {
	return append([]inference.EvidenceAnchor(nil), r.anchors...)
}
func (r ToolResult) Snapshot() inference.SnapshotPin  { return r.snapshot }
func (r ToolResult) Authorization() inference.AuthPin { return r.auth }
func (r ToolResult) validate() error {
	if err := validateLogicalID("tool result correlation ID", r.correlation); err != nil {
		return err
	}
	switch r.kind {
	case ActionRetrieve, ActionOpenSection, ActionNeighbors:
	default:
		return invalid("tool result kind is not a callable action")
	}
	if err := r.snapshot.Validate(); err != nil {
		return err
	}
	if err := r.auth.Validate(); err != nil {
		return err
	}
	if len(r.anchors) == 0 || len(r.anchors) > inference.MaxEvidenceAnchors {
		return invalid("tool result anchor count is outside the supported range")
	}
	for i, anchor := range r.anchors {
		if err := anchor.Validate(); err != nil {
			return err
		}
		if i > 0 && shoal.CompareID(r.anchors[i-1].ID(), anchor.ID()) >= 0 {
			return invalid("tool result anchors must be unique and canonical")
		}
	}
	return nil
}

type Exchange struct {
	action Action
	result ToolResult
}

func (e Exchange) Action() Action     { return e.action }
func (e Exchange) Result() ToolResult { return cloneToolResult(e.result) }

type Transcript struct {
	id        shoal.ID
	requestID shoal.ID
	context   inference.ContextPack
	exchanges []Exchange
	final     *Action
}

func newTranscript(request SessionRequest) Transcript {
	t := Transcript{requestID: request.id, context: request.context}
	t.id = transcriptID(t)
	return t
}
func (t Transcript) ID() shoal.ID                   { return t.id }
func (t Transcript) RequestID() shoal.ID            { return t.requestID }
func (t Transcript) Context() inference.ContextPack { return clonePack(t.context) }
func (t Transcript) Exchanges() []Exchange {
	out := make([]Exchange, len(t.exchanges))
	for i := range t.exchanges {
		out[i] = Exchange{action: t.exchanges[i].action, result: cloneToolResult(t.exchanges[i].result)}
	}
	return out
}
func (t Transcript) Final() (Action, bool) {
	if t.final == nil {
		return Action{}, false
	}
	return *t.final, true
}

type Runner interface {
	Start(context.Context, SessionRequest) (Session, error)
}
type Session interface {
	Next(context.Context, Transcript) (Action, error)
}

type ToolHost interface {
	Retrieve(context.Context, RetrieveRequest, shoal.ID) (ToolResult, error)
	OpenSection(context.Context, OpenSectionRequest, shoal.ID) (ToolResult, error)
	Neighbors(context.Context, NeighborsRequest, shoal.ID) (ToolResult, error)
}

type Record struct {
	Request    SessionRequest
	Transcript Transcript
	Result     inference.InferenceResult
}

// EvaluationRecord is a redacted deterministic execution record. It contains
// identities and digests, never raw prompts, authorization grants, or tool
// payloads.
type EvaluationRecord struct {
	RequestID      shoal.ID
	ContextPackID  shoal.ID
	TranscriptID   shoal.ID
	ResultID       shoal.ID
	Provenance     Provenance
	ActionKinds    []ActionKind
	CorrelationIDs []shoal.ID
	ActionDigests  []string
}

type Recorder interface {
	Record(context.Context, EvaluationRecord) error
}

type Generator struct {
	runner     Runner
	tools      ToolHost
	budgets    Budgets
	provenance Provenance
	recorder   Recorder
	now        func() time.Time
}

func NewGenerator(runner Runner, tools ToolHost, budgets Budgets, provenance Provenance, recorder Recorder) (*Generator, error) {
	if runner == nil || tools == nil {
		return nil, invalid("runner and tool host are required")
	}
	if err := budgets.validate(); err != nil {
		return nil, err
	}
	if err := provenance.validate(); err != nil {
		return nil, err
	}
	return &Generator{runner: runner, tools: tools, budgets: budgets, provenance: provenance, recorder: recorder, now: time.Now}, nil
}

func (g *Generator) Generate(ctx context.Context, pack inference.ContextPack) (inference.InferenceResult, error) {
	record, err := g.Run(ctx, pack)
	return record.Result, err
}

func (g *Generator) Run(ctx context.Context, pack inference.ContextPack) (Record, error) {
	request, err := newSessionRequest(pack, g.budgets, g.provenance)
	if err != nil {
		return Record{}, err
	}
	if !g.now().Before(pack.Authorization().ExpiresAt()) {
		return Record{}, invalid("authorization pin is stale")
	}
	runCtx, cancel := context.WithTimeout(ctx, g.budgets.MaxElapsed)
	defer cancel()
	session, err := callBounded(runCtx, func() (Session, error) {
		return g.runner.Start(runCtx, request)
	})
	if err != nil {
		if runCtx.Err() != nil {
			return Record{}, runCtx.Err()
		}
		return Record{}, fmt.Errorf("%w: %v", ErrRunnerUnavailable, err)
	}
	if session == nil {
		return Record{}, ErrRunnerUnavailable
	}
	transcript := newTranscript(request)
	seenCorrelation := map[shoal.ID]struct{}{}
	repeats := map[string]int{}
	inputTokens, outputTokens, hops, fanout := 0, 0, 0, 0
	for step := 0; step < g.budgets.MaxSteps; step++ {
		if !g.now().Before(pack.Authorization().ExpiresAt()) {
			return Record{}, invalid("authorization pin expired during execution")
		}
		action, nextErr := callBounded(runCtx, func() (Action, error) {
			return session.Next(runCtx, cloneTranscript(transcript))
		})
		if nextErr != nil {
			if runCtx.Err() != nil {
				return Record{}, runCtx.Err()
			}
			return Record{}, fmt.Errorf("%w: %v", ErrRunnerUnavailable, nextErr)
		}
		if err := runCtx.Err(); err != nil {
			return Record{}, err
		}
		if err := action.validate(); err != nil {
			return Record{}, err
		}
		if _, duplicate := seenCorrelation[action.correlation]; duplicate {
			return Record{}, invalid("duplicate action correlation ID")
		}
		seenCorrelation[action.correlation] = struct{}{}
		inputTokens += action.usage.InputTokens
		outputTokens += action.usage.OutputTokens
		if inputTokens > g.budgets.MaxInputTokens || outputTokens > g.budgets.MaxOutputTokens {
			return Record{}, budget("token")
		}
		key := actionKey(action)
		repeats[key]++
		if repeats[key] > g.budgets.MaxRepeatedAction {
			return Record{}, budget("repeated action/cycle")
		}
		if action.kind == ActionStop {
			result := cloneResult(action.result)
			if err := result.ValidateFor(transcript.context); err != nil {
				return Record{}, fmt.Errorf("%w: final result: %v", ErrInvalid, err)
			}
			if transcript.context.Snapshot() != pack.Snapshot() || transcript.context.Authorization() != pack.Authorization() {
				return Record{}, invalid("final context pins changed")
			}
			final := action
			transcript.final = &final
			transcript.id = transcriptID(transcript)
			record := Record{Request: request, Transcript: cloneTranscript(transcript), Result: result}
			if g.recorder != nil {
				if err := g.recorder.Record(runCtx, evaluationRecord(record)); err != nil {
					return Record{}, err
				}
			}
			return record, nil
		}
		var toolResult ToolResult
		switch action.kind {
		case ActionRetrieve:
			if action.retrieve.limit > g.budgets.MaxFanout {
				return Record{}, budget("retrieve fanout")
			}
			toolResult, err = callBounded(runCtx, func() (ToolResult, error) {
				return g.tools.Retrieve(runCtx, action.retrieve, action.correlation)
			})
		case ActionOpenSection:
			toolResult, err = callBounded(runCtx, func() (ToolResult, error) {
				return g.tools.OpenSection(runCtx, action.open, action.correlation)
			})
		case ActionNeighbors:
			hops += action.neighbors.hops
			if hops > g.budgets.MaxGraphHops || action.neighbors.fanout > g.budgets.MaxFanout {
				return Record{}, budget("graph traversal")
			}
			toolResult, err = callBounded(runCtx, func() (ToolResult, error) {
				return g.tools.Neighbors(runCtx, action.neighbors, action.correlation)
			})
		default:
			return Record{}, invalid("unknown or forbidden action")
		}

		if err != nil {
			if runCtx.Err() != nil {
				return Record{}, runCtx.Err()
			}
			return Record{}, err
		}
		if err := runCtx.Err(); err != nil {
			return Record{}, err
		}
		if err := validateToolResult(action, toolResult, pack); err != nil {
			return Record{}, err
		}
		switch action.kind {
		case ActionRetrieve:
			if len(toolResult.anchors) > action.retrieve.limit {
				return Record{}, budget("retrieve result fanout")
			}
		case ActionNeighbors:
			if len(toolResult.anchors) > action.neighbors.fanout {
				return Record{}, budget("neighbors result fanout")
			}
		}
		fanout += len(toolResult.anchors)
		if fanout > g.budgets.MaxFanout {
			return Record{}, budget("tool result fanout")
		}
		nextPack, err := addAnchors(transcript.context, toolResult.anchors)
		if err != nil {
			return Record{}, err
		}
		transcript.context = nextPack
		transcript.exchanges = append(transcript.exchanges, Exchange{action: action, result: cloneToolResult(toolResult)})
		transcript.id = transcriptID(transcript)
	}
	return Record{}, budget("step")
}

type boundedResult[T any] struct {
	value T
	err   error
}

func callBounded[T any](ctx context.Context, call func() (T, error)) (T, error) {
	completed := make(chan boundedResult[T], 1)
	go func() {
		value, err := call()
		completed <- boundedResult[T]{value: value, err: err}
	}()
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case result := <-completed:
		return result.value, result.err
	}
}

func evaluationRecord(record Record) EvaluationRecord {
	evaluation := EvaluationRecord{
		RequestID:      record.Request.ID(),
		ContextPackID:  record.Request.context.ID(),
		TranscriptID:   record.Transcript.ID(),
		ResultID:       record.Result.ID(),
		Provenance:     record.Request.provenance,
		ActionKinds:    make([]ActionKind, 0, len(record.Transcript.exchanges)+1),
		CorrelationIDs: make([]shoal.ID, 0, len(record.Transcript.exchanges)+1),
		ActionDigests:  make([]string, 0, len(record.Transcript.exchanges)+1),
	}
	add := func(action Action) {
		evaluation.ActionKinds = append(evaluation.ActionKinds, action.kind)
		evaluation.CorrelationIDs = append(evaluation.CorrelationIDs, action.correlation)
		sum := sha256.Sum256([]byte(actionKey(action)))
		evaluation.ActionDigests = append(evaluation.ActionDigests, hex.EncodeToString(sum[:]))
	}
	for _, exchange := range record.Transcript.exchanges {
		add(exchange.action)
	}
	if record.Transcript.final != nil {
		add(*record.Transcript.final)
	}
	return evaluation
}

func validateToolResult(action Action, result ToolResult, original inference.ContextPack) error {
	if err := result.validate(); err != nil {
		return err
	}
	if result.correlation != action.correlation || result.kind != action.kind {
		return invalid("tool result does not match action")
	}
	if result.snapshot != original.Snapshot() || result.auth != original.Authorization() {
		return invalid("tool result snapshot or authorization pin is stale")
	}
	return nil
}

func addAnchors(pack inference.ContextPack, additions []inference.EvidenceAnchor) (inference.ContextPack, error) {
	anchors := pack.Evidence()
	seen := make(map[shoal.ID]struct{}, len(anchors)+len(additions))
	for _, a := range anchors {
		seen[a.ID()] = struct{}{}
	}
	for _, a := range additions {
		if _, duplicate := seen[a.ID()]; duplicate {
			return inference.ContextPack{}, invalid("tool result repeats existing evidence")
		}
		seen[a.ID()] = struct{}{}
		anchors = append(anchors, a)
	}
	ontology, ok := pack.Ontology()
	var ontologyPtr *inference.OntologyIdentity
	if ok {
		ontologyPtr = &ontology
	}
	return inference.NewContextPack(pack.Query(), anchors, ontologyPtr, pack.Snapshot(), pack.Authorization(), pack.Metadata())
}

func actionKey(a Action) string {
	switch a.kind {
	case ActionRetrieve:
		return string(a.kind) + "\x00" + a.retrieve.query + "\x00" + strconv.Itoa(a.retrieve.limit)
	case ActionOpenSection:
		return string(a.kind) + "\x00" + string(a.open.documentID) + "\x00" + string(a.open.sectionID)
	case ActionNeighbors:
		return string(a.kind) + "\x00" + string(a.neighbors.nodeID) + "\x00" + strconv.Itoa(a.neighbors.hops) + "\x00" + strconv.Itoa(a.neighbors.fanout)
	default:
		return string(a.kind)
	}
}

func transcriptID(t Transcript) shoal.ID {
	parts := []string{string(t.requestID), string(t.context.ID())}
	for _, e := range t.exchanges {
		parts = append(parts, string(e.action.kind), string(e.action.correlation), actionKey(e.action))
		for _, a := range e.result.anchors {
			parts = append(parts, string(a.ID()))
		}
	}
	if t.final != nil {
		parts = append(parts, string(t.final.kind), string(t.final.correlation), actionKey(*t.final), string(t.final.result.ID()))
	}
	return deriveID("transcript", parts...)
}
func deriveID(namespace string, parts ...string) shoal.ID {
	h := sha256.New()
	for _, part := range append([]string{namespace}, parts...) {
		fmt.Fprintf(h, "%d:", len(part))
		h.Write([]byte(part))
	}
	return shoal.ID(namespace + ":" + hex.EncodeToString(h.Sum(nil)))
}
func canonicalBudgets(b Budgets) string {
	return fmt.Sprintf("%d/%d/%d/%d/%d/%d/%d", b.MaxSteps, b.MaxElapsed.Nanoseconds(), b.MaxInputTokens, b.MaxOutputTokens, b.MaxGraphHops, b.MaxFanout, b.MaxRepeatedAction)
}
func validateLogicalID(name string, id shoal.ID) error {
	if len(id) > MaxLogicalIDBytes {
		return invalid(name + " exceeds the byte bound")
	}
	if err := shoal.ValidateRequiredID(name, id); err != nil {
		return err
	}
	return nil
}
func boundedString(name, value string, limit int) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return invalid(name + " is required and must be valid UTF-8")
	}
	if len(value) > limit {
		return invalid(name + " exceeds the byte bound")
	}
	return nil
}
func invalid(message string) error { return fmt.Errorf("%w: %s", ErrInvalid, message) }
func budget(name string) error     { return fmt.Errorf("%w: %s", ErrBudgetExhausted, name) }
func clonePack(pack inference.ContextPack) inference.ContextPack {
	ontology, ok := pack.Ontology()
	var ptr *inference.OntologyIdentity
	if ok {
		ptr = &ontology
	}
	cloned, _ := inference.NewContextPack(pack.Query(), pack.Evidence(), ptr, pack.Snapshot(), pack.Authorization(), pack.Metadata())
	return cloned
}
func cloneResult(result inference.InferenceResult) inference.InferenceResult { return result }
func cloneToolResult(r ToolResult) ToolResult {
	r.anchors = append([]inference.EvidenceAnchor(nil), r.anchors...)
	return r
}
func cloneTranscript(t Transcript) Transcript {
	t.context = clonePack(t.context)
	t.exchanges = append([]Exchange(nil), t.exchanges...)
	for i := range t.exchanges {
		t.exchanges[i].result = cloneToolResult(t.exchanges[i].result)
	}
	if t.final != nil {
		final := *t.final
		t.final = &final
	}
	return t
}

var _ inference.Generator = (*Generator)(nil)
