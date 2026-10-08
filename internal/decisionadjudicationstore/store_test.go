// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package decisionadjudicationstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/phrocker/shoal-oss/internal/engine"
	"github.com/phrocker/shoal-oss/internal/explorercoord"
	"github.com/phrocker/shoal-oss/pkg/decision"
	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/inference"
	"github.com/phrocker/shoal-oss/pkg/shoal"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func predictionFixture(t *testing.T) (decision.PredictionRecord, auth.Decision, context.Context, auth.Resolver, func() time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	var d auth.Decision
	var authority *auth.Authority
	var ctx context.Context
	cfg := auth.DecisionConfig{Subject: "principal", Actor: "actor", ClientID: "client", AuthorizationDomain: []byte("domain"), AllowedOperations: []auth.Operation{auth.OperationInvoke, auth.OperationRead, auth.OperationRetrieve, auth.OperationIngest}, PermittedSourceIDs: [][]byte{[]byte("tasks"), []byte("evidence")}, PermittedPolicyIDs: [][]byte{[]byte("task-policy"), []byte("evidence-policy")}, PolicyGeneration: 1, AuthenticationExpires: now.Add(time.Hour), RequestID: "http-request", CorrelationID: "correlation", AuditPurpose: "decision-test"}
	var err error
	d, err = auth.NewDecision(cfg)
	if err != nil {
		t.Fatal(err)
	}
	authority, err = auth.NewAuthorityWithClock(clock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = authority.Binder().Bind(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.AuthorizationFingerprint(d)
	if err != nil {
		t.Fatal(err)
	}
	ep, err := decision.NewEvidencePolicy(decision.EvidencePolicyConfig{MaxObservationAge: time.Hour, AllowedRoles: []decision.EvidenceRole{decision.Observation}, AllowedControls: []decision.Control{decision.CandidateControlled}, AuthorityPolicyIDs: []shoal.ID{"authority:1"}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := decision.NewRankingPlan(decision.RankingConfig{Terms: []decision.RankingTerm{{QuestionID: "review", Weight: 1, Labels: []decision.LabelPriority{{Label: "inspect", Value: 1}, {Label: "ordinary", Value: 0}}}}})
	if err != nil {
		t.Fatal(err)
	}
	task, err := decision.NewTaskSpec(decision.TaskConfig{OwnerID: "owner", Name: "review-priority", Version: "1", InputSchemaID: "schema", EvidencePolicyID: ep.ID(), LabelPolicyID: "labels", EvaluationPolicyID: "evaluation", PredictionUnit: "subject", LabelUnit: "finding", ActionUnit: "inspection", AggregationID: plan.ID(), Questions: []decision.Question{{ID: "review", Kind: decision.Choice, RubricID: "rubric", Labels: []string{"inspect", "ordinary"}}}})
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := inference.NewDocumentAnchor(document.Citation{DocumentID: "source", RevisionID: "revision", SectionID: "section", SpanID: "span", Range: document.SourceRange{End: document.SourcePosition{Offset: 4}}}, "code")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := inference.NewSnapshotPin("snapshot", now.Add(-time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	pin, err := inference.NewAuthPin(shoal.ID(fingerprint.String()), cfg.AuthenticationExpires)
	if err != nil {
		t.Fatal(err)
	}
	pack, err := inference.NewContextPack("inspect", []inference.EvidenceAnchor{anchor}, nil, snapshot, pin, nil)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("code")
	digest := sha256.Sum256(input)
	observed := now.Add(-time.Minute)
	picture, err := decision.NewPictureManifest(pack, decision.PictureConfig{TaskID: task.ID(), ObservationID: "observation", EnumerationID: "inventory", ScopeID: "scope", BuilderID: "builder", OntologyProjectionID: "projection", InputDigest: hex.EncodeToString(digest[:]), TokenizerID: "tokenizer", InputTokens: 1, TokenBudget: 100, Cutoff: now, Sources: []decision.Source{{ID: "s", ArtifactID: "source", RevisionID: "revision", Digest: hex.EncodeToString(digest[:]), OriginID: "origin", AuthorityPolicyID: "authority:1", Role: decision.Observation, Control: decision.CandidateControlled, ObservedAt: observed, ReceivedAt: now}}, Subjects: []decision.Subject{{ID: "subject", SourceID: "s", Disposition: decision.Supported, EvidenceIDs: []shoal.ID{anchor.ID()}}}})
	if err != nil {
		t.Fatal(err)
	}
	model, err := decision.NewPredictorIdentity(decision.PredictorConfig{Provider: "local", RuntimeID: "runtime", WeightsDigest: strings.Repeat("a", 64), TokenizerDigest: strings.Repeat("b", 64), EnvironmentDigest: strings.Repeat("c", 64), FormattingID: "format", PreprocessingID: "preprocess", CalibrationID: "uncalibrated", Device: "cpu", Precision: "float64", BatchPolicyID: "batch"})
	if err != nil {
		t.Fatal(err)
	}
	request, err := decision.NewDecisionRequest(task, picture, model, decision.RequestConfig{PrincipalID: cfg.Subject, ReleaseID: "release:1", CorrelationID: "call", RequestedAt: now, Deadline: now.Add(time.Minute), SubjectIDs: []shoal.ID{"subject"}})
	if err != nil {
		t.Fatal(err)
	}
	p, e := decision.NewPredictionRecord(request, decision.ResultConfig{RequestID: request.ID(), PredictorID: model.ID(), EffectiveDevice: "cpu", Status: decision.Completed, CompletedAt: now, Answers: []decision.Answer{{SubjectID: "subject", QuestionID: "review", Status: decision.Answered, Label: "inspect"}}})
	if e != nil {
		t.Fatal(e)
	}
	return p, d, ctx, authority.Resolver(), clock
}

type memoryCAS struct {
	mu        sync.Mutex
	cells     map[string]allocator.Cell
	writes    atomic.Int64
	beforeCAS func()
	afterCAS  func()
	unknown   bool
	readFail  atomic.Bool
}

func (b *memoryCAS) ReadExact(_ context.Context, coords []allocator.Coordinate) ([]allocator.Cell, error) {
	if b.readFail.Load() {
		return nil, errors.New("private read failure")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var result []allocator.Cell
	for _, c := range coords {
		if cell, ok := b.cells[string(c.Row)]; ok {
			cell.Value = append([]byte(nil), cell.Value...)
			result = append(result, cell)
		}
	}
	return result, nil
}
func (b *memoryCAS) CompareAndMutate(_ context.Context, m allocator.Mutation) (allocator.Status, error) {
	b.writes.Add(1)
	if b.beforeCAS != nil {
		b.beforeCAS()
	}
	b.mu.Lock()
	cell, exists := b.cells[string(m.Row)]
	condition := m.Conditions[0]
	if (condition.Absent && exists) || (!condition.Absent && (!exists || !bytes.Equal(cell.Value, condition.Value) || condition.TimestampSet && cell.Timestamp != condition.Timestamp)) {
		b.mu.Unlock()
		return allocator.StatusRejected, nil
	}
	if b.cells == nil {
		b.cells = map[string]allocator.Cell{}
	}
	u := m.Updates[0]
	b.cells[string(m.Row)] = allocator.Cell{Coordinate: u.Coordinate, Value: append([]byte(nil), u.Value...), Timestamp: u.Timestamp}
	b.mu.Unlock()
	if b.afterCAS != nil {
		b.afterCAS()
	}
	if b.unknown {
		return allocator.StatusUnknown, errors.New("private lost acknowledgment")
	}
	return allocator.StatusAccepted, nil
}
func fixture(t *testing.T) (decision.LabelPolicy, decision.PredictionRecord, decision.AdjudicationProposalConfig, func() time.Time) {
	t.Helper()
	old, _, _, _, clock := predictionFixture(t)
	policy, e := decision.NewLabelPolicy(decision.LabelPolicyConfig{OwnerID: "authority", Version: "1", AdjudicatorRoleID: "adjudicator", DisputeResolverRoleID: "resolver", TrainingPurposeID: "training", MinIndependentWitnesses: 1})
	if e != nil {
		t.Fatal(e)
	}
	tc := old.Request().Task().Config()
	tc.LabelPolicyID = policy.ID()
	task, e := decision.NewTaskSpec(tc)
	if e != nil {
		t.Fatal(e)
	}
	pc := old.Request().Picture().Config()
	pc.TaskID = task.ID()
	picture, e := decision.NewPictureManifest(old.Request().Picture().ContextPack(), pc)
	if e != nil {
		t.Fatal(e)
	}
	request, e := decision.NewDecisionRequest(task, picture, old.Request().Predictor(), old.Request().Config())
	if e != nil {
		t.Fatal(e)
	}
	rc := old.Config()
	rc.RequestID = request.ID()
	p, e := decision.NewPredictionRecord(request, rc)
	if e != nil {
		t.Fatal(e)
	}
	cfg := decision.AdjudicationProposalConfig{RequestID: request.ID(), PredictionID: p.ID(), SubjectID: "subject", QuestionID: "review", ObservationReceiptIDs: []shoal.ID{shoal.ID("outcome-receipt:" + strings.Repeat("a", 64))}, WitnessIDs: []shoal.ID{"witness"}, Disposition: decision.AdjudicationVerified, Label: "inspect"}
	return policy, p, cfg, clock
}
func proposed(t *testing.T, policy decision.LabelPolicy, p decision.PredictionRecord, c decision.AdjudicationProposalConfig) decision.AdjudicationProposal {
	t.Helper()
	v, e := decision.NewAdjudicationProposal(policy, p, c)
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func attribution() Attribution {
	return Attribution{SubjectID: "adjudicator", ActorID: "actor", ClientID: "client", AuthorizationFingerprint: "auth-sha256:" + strings.Repeat("b", 64)}
}
func TestHistoryAndExactRetryAfterHeadAdvances(t *testing.T) {
	policy, p, c, clock := fixture(t)
	b := &memoryCAS{}
	s, e := New(Config{Backend: b, Clock: clock})
	if e != nil {
		t.Fatal(e)
	}
	scope := Scope{[]byte("domain")}
	firstProposal := proposed(t, policy, p, c)
	first, e := s.Append(context.Background(), scope, []byte("key"), firstProposal, attribution())
	if e != nil {
		t.Fatal(e)
	}
	c.ExpectedHeadID = first.ID
	c.ExpectedVersion = 1
	c.ObservationReceiptIDs = []shoal.ID{shoal.ID("outcome-receipt:" + strings.Repeat("c", 64))}
	secondAttrib := attribution()
	secondAttrib.ActorID = "other-actor"
	second, e := s.Append(context.Background(), scope, []byte("other-key"), proposed(t, policy, p, c), secondAttrib)
	if e != nil || second.Version != 2 || second.TargetID != first.TargetID {
		t.Fatal("report subset or principal forked target", e)
	}
	changedGrant := attribution()
	changedGrant.AuthorizationFingerprint = "auth-sha256:" + strings.Repeat("d", 64)
	retry, e := s.Append(context.Background(), scope, []byte("key"), firstProposal, changedGrant)
	if e != nil || !reflect.DeepEqual(first, retry) || b.writes.Load() != 2 {
		t.Fatal("exact stale-head retry changed original receipt", e)
	}
	c.ExpectedHeadID = ""
	c.ExpectedVersion = 0
	if _, e = s.Append(context.Background(), scope, []byte("key"), proposed(t, policy, p, c), attribution()); !errors.Is(e, ErrConflict) {
		t.Fatal("same-key changed proposal accepted", e)
	}
	history, e := s.History(context.Background(), scope, first.TargetID)
	if e != nil || len(history) != 2 || history[1].ID != second.ID {
		t.Fatal("history lost entries", e)
	}
	history[0].ProposalConfig.WitnessIDs[0] = "mutated"
	again, e := s.History(context.Background(), scope, first.TargetID)
	if e != nil || again[0].ProposalConfig.WitnessIDs[0] != "witness" {
		t.Fatal("history aliases storage", e)
	}
	other, e := s.History(context.Background(), Scope{[]byte("other")}, first.TargetID)
	if e != nil || len(other) != 0 {
		t.Fatal("domain histories crossed", e)
	}
}
func TestConcurrentExpectedHeadHasOneWinner(t *testing.T) {
	policy, p, c, clock := fixture(t)
	b := &memoryCAS{}
	var ready sync.WaitGroup
	ready.Add(2)
	b.beforeCAS = func() { ready.Done(); ready.Wait() }
	s, _ := New(Config{Backend: b, Clock: clock})
	proposal := proposed(t, policy, p, c)
	results := make(chan error, 2)
	for _, key := range []string{"one", "two"} {
		go func(k string) {
			_, e := s.Append(context.Background(), Scope{[]byte("domain")}, []byte(k), proposal, attribution())
			results <- e
		}(key)
	}
	wins, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		e := <-results
		if e == nil {
			wins++
		} else if errors.Is(e, ErrConflict) && !errors.Is(e, ErrIndeterminate) {
			conflicts++
		} else {
			t.Fatal(e)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatal(wins, conflicts)
	}
	history, e := s.History(context.Background(), Scope{[]byte("domain")}, proposal.TargetID())
	if e != nil || len(history) != 1 {
		t.Fatal(e, len(history))
	}
}
func TestAllDispositionsRemainInHistory(t *testing.T) {
	policy, p, c, clock := fixture(t)
	s, _ := New(Config{Backend: &memoryCAS{}, Clock: clock})
	for i, disposition := range []decision.AdjudicationDisposition{decision.AdjudicationUnresolved, decision.AdjudicationDisputed, decision.AdjudicationVerified} {
		c.Disposition = disposition
		c.Label = ""
		c.Reason = "needs evidence"
		if disposition == decision.AdjudicationVerified {
			c.Label = "inspect"
			c.Reason = ""
		}
		proposal := proposed(t, policy, p, c)
		r, e := s.Append(context.Background(), Scope{[]byte("domain")}, []byte{byte(i)}, proposal, attribution())
		if e != nil {
			t.Fatal(e)
		}
		c.ExpectedHeadID = r.ID
		c.ExpectedVersion = r.Version
	}
	history, e := s.History(context.Background(), Scope{[]byte("domain")}, proposed(t, policy, p, c).TargetID())
	if e != nil || len(history) != 3 {
		t.Fatal(e)
	}
	for i, status := range []decision.AdjudicationDisposition{decision.AdjudicationUnresolved, decision.AdjudicationDisputed, decision.AdjudicationVerified} {
		if history[i].ProposalConfig.Disposition != status {
			t.Fatal("history collapsed")
		}
	}
}
func TestUnknownAcknowledgementAndReadback(t *testing.T) {
	for _, kind := range []string{"reconcile", "read-fail", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			policy, p, c, clock := fixture(t)
			b := &memoryCAS{unknown: true}
			s, _ := New(Config{Backend: b, Clock: clock})
			ctx := context.Background()
			if kind == "read-fail" {
				b.afterCAS = func() { b.readFail.Store(true) }
			} else if kind == "cancel" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				b.afterCAS = cancel
			}
			r, e := s.Append(ctx, Scope{[]byte("domain")}, []byte("key"), proposed(t, policy, p, c), attribution())
			if kind == "reconcile" {
				if e != nil || r.ID == "" {
					t.Fatal("lost acknowledgment not reconciled", e)
				}
			} else if !errors.Is(e, ErrIndeterminate) || r.ID != "" {
				t.Fatal("write uncertainty lost", e)
			}
		})
	}
}
func TestBoundsAndClockNeverTruncateHistory(t *testing.T) {
	policy, p, c, clock := fixture(t)
	b := &memoryCAS{}
	s, _ := New(Config{Backend: b, Clock: clock})
	scope := Scope{[]byte("domain")}
	var first Receipt
	for i := 0; i < MaxEntries; i++ {
		r, e := s.Append(context.Background(), scope, []byte{byte(i)}, proposed(t, policy, p, c), attribution())
		if e != nil {
			t.Fatal(i, e)
		}
		if i == 0 {
			first = r
		}
		c.ExpectedHeadID = r.ID
		c.ExpectedVersion = r.Version
	}
	if _, e := s.Append(context.Background(), scope, []byte("overflow"), proposed(t, policy, p, c), attribution()); !errors.Is(e, ErrLimit) || b.writes.Load() != MaxEntries {
		t.Fatal("entry bound not enforced", e)
	}
	history, e := s.History(context.Background(), scope, first.TargetID)
	if e != nil || len(history) != MaxEntries || history[0].ID != first.ID {
		t.Fatal("history truncated", e)
	}
}
func TestByteBoundAndNondecreasingServerClock(t *testing.T) {
	policy, p, c, clock := fixture(t)
	b := &memoryCAS{}
	s, _ := New(Config{Backend: b, Clock: clock})
	c.Disposition = decision.AdjudicationUnresolved
	c.Label = ""
	c.Reason = "needs evidence"
	c.WitnessIDs = make([]shoal.ID, 500)
	for i := range c.WitnessIDs {
		c.WitnessIDs[i] = shoal.ID(fmt.Sprintf("%04d", i) + strings.Repeat("x", 1020))
	}
	scope := Scope{[]byte("domain")}
	lastVersion := int64(0)
	for i := 0; i < 25; i++ {
		before := b.writes.Load()
		r, e := s.Append(context.Background(), scope, []byte{byte(i)}, proposed(t, policy, p, c), attribution())
		if errors.Is(e, ErrLimit) {
			if b.writes.Load() != before || lastVersion == 0 {
				t.Fatal("overflow changed journal")
			}
			return
		}
		if e != nil {
			t.Fatal(e)
		}
		lastVersion = r.Version
		c.ExpectedHeadID = r.ID
		c.ExpectedVersion = r.Version
	}
	t.Fatal("byte bound not exercised")
}
func TestClockRollbackAndInvalidClock(t *testing.T) {
	policy, p, c, clock := fixture(t)
	current := clock()
	b := &memoryCAS{}
	s, _ := New(Config{Backend: b, Clock: func() time.Time { return current }})
	scope := Scope{[]byte("domain")}
	r, e := s.Append(context.Background(), scope, []byte("first"), proposed(t, policy, p, c), attribution())
	if e != nil {
		t.Fatal(e)
	}
	c.ExpectedHeadID = r.ID
	c.ExpectedVersion = 1
	for _, bad := range []time.Time{current.Add(-time.Nanosecond), {}, time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		current = bad
		if _, e = s.Append(context.Background(), scope, []byte("next"), proposed(t, policy, p, c), attribution()); !errors.Is(e, ErrUnavailable) || b.writes.Load() != 1 {
			t.Fatal("invalid server clock accepted", e)
		}
	}
}
func TestRealEngineBinaryAttributionRestart(t *testing.T) {
	policy, p, c, clock := fixture(t)
	proposal := proposed(t, policy, p, c)
	domain := Scope{[]byte{'d', 0, 255}}
	opaque := shoal.ID(string([]byte{'a', 0, 255}))
	a := Attribution{SubjectID: opaque, ActorID: opaque, ClientID: opaque, OnBehalfOf: []shoal.ID{opaque}, AuthorizationFingerprint: "auth-sha256:" + strings.Repeat("a", 64)}
	dir := t.TempDir()
	var original Receipt
	for run := 0; run < 2; run++ {
		eng, e := engine.Open(dir, engine.Options{})
		if e != nil {
			t.Fatal(e)
		}
		if run == 0 {
			if e = eng.CreateTable(Table, engine.TableOptions{}); e != nil {
				t.Fatal(e)
			}
		}
		backend, e := explorercoord.NewEngineStore(eng, Table)
		if e != nil {
			t.Fatal(e)
		}
		s, e := New(Config{Backend: backend, Clock: clock})
		if e != nil {
			t.Fatal(e)
		}
		r, e := s.Append(context.Background(), domain, []byte("key"), proposal, a)
		if e != nil {
			t.Fatal(e)
		}
		if run == 0 {
			original = r
		} else if !reflect.DeepEqual(original, r) {
			t.Fatal("restart changed receipt")
		}
		if !reflect.DeepEqual(a, r.Adjudicator) {
			t.Fatal("opaque attribution changed")
		}
		if e = eng.Flush(Table); e != nil {
			t.Fatal(e)
		}
		files := 0
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			if !d.IsDir() && filepath.Ext(path) == ".rf" {
				files++
			}
			return nil
		})
		if files == 0 {
			t.Fatal("no RFile")
		}
		if e = eng.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
func TestCorruptOrderingAndBindingFailClosed(t *testing.T) {
	policy, p, c, clock := fixture(t)
	b := &memoryCAS{}
	s, _ := New(Config{Backend: b, Clock: clock})
	scope := Scope{[]byte("domain")}
	r, e := s.Append(context.Background(), scope, []byte("one"), proposed(t, policy, p, c), attribution())
	if e != nil {
		t.Fatal(e)
	}
	c.ExpectedHeadID = r.ID
	c.ExpectedVersion = 1
	if _, e = s.Append(context.Background(), scope, []byte("two"), proposed(t, policy, p, c), attribution()); e != nil {
		t.Fatal(e)
	}
	sd, _ := scopeDigest(scope)
	coord := s.coordinate(sd, r.TargetID)
	original := b.cells[string(coord.Row)]
	for _, mutate := range []func(*journal){func(j *journal) { j.Entries[0], j.Entries[1] = j.Entries[1], j.Entries[0] }, func(j *journal) { j.Entries[1].Receipt.ProposalConfig.ExpectedHeadID = "" }, func(j *journal) { j.Entries[1].Receipt.TargetID = "other" }, func(j *journal) { j.Entries[1].Receipt.ProposalID = "forged" }, func(j *journal) { j.Entries[1].KeyDigest = strings.Repeat("a", 64) }, func(j *journal) { j.Entries[1].Receipt.ProposalConfig.Label = "other" }, func(j *journal) { j.ScopeDigest = strings.Repeat("b", 64) }} {
		j, e := decode(original.Value)
		if e != nil {
			t.Fatal(e)
		}
		mutate(&j)
		raw, e := encode(j)
		if e != nil {
			t.Fatal(e)
		}
		cell := original
		cell.Value = raw
		b.cells[string(coord.Row)] = cell
		if _, e = s.History(context.Background(), scope, r.TargetID); !errors.Is(e, ErrCorrupt) {
			t.Fatal("corrupt journal accepted", e)
		}
	}
	for _, raw := range [][]byte{[]byte(`{}`), append(append([]byte{}, original.Value...), byte(' ')), bytes.Replace(original.Value, []byte(`"Schema":1`), []byte(`"Schema":1,"Schema":1`), 1)} {
		cell := original
		cell.Value = raw
		b.cells[string(coord.Row)] = cell
		if _, e = s.History(context.Background(), scope, r.TargetID); !errors.Is(e, ErrCorrupt) {
			t.Fatal("ambiguous codec accepted", e)
		}
	}
}

func TestCodecRejectsOversizedCollectionsBeforeWideDecode(t *testing.T) {
	entries := strings.TrimSuffix(strings.Repeat("{},", MaxEntries+1), ",")
	if e := boundedShape([]byte(`{"Entries":[` + entries + `]}`)); e == nil {
		t.Fatal("oversized entry collection accepted")
	}
	if e := boundedShape([]byte(strings.Repeat("[", 17) + "0" + strings.Repeat("]", 17))); e == nil {
		t.Fatal("excessive nesting accepted")
	}
	if e := boundedShape([]byte(`{"Entries":[],"Entries":[]}`)); e == nil {
		t.Fatal("duplicate collection accepted")
	}
}
