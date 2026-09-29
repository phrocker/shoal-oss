// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type admissionHarness struct {
	now       time.Time
	clock     func() time.Time
	authority *auth.Authority
	service   *AdmissionService
	dispatch  *DispatchService
	store     *memoryDispatchStore
	events    *controlledDispatchEvents
	recorder  *dispatchRecorder
	registry  *Service
}

type stubRestrictor struct {
	calls   int
	seen    [][]shoal.ID
	allowed []shoal.ID
	err     error
}

func (r *stubRestrictor) RestrictDisclosure(
	_ context.Context, references []shoal.ID,
) ([]shoal.ID, error) {
	r.calls++
	r.seen = append(r.seen, append([]shoal.ID(nil), references...))
	if r.err != nil {
		return nil, r.err
	}
	return append([]shoal.ID(nil), r.allowed...), nil
}

// wideningRestrictor admits one more reference on every call, standing in for
// the co-occurrence budget's window moving between a grant and a retry. A
// service that re-adjudicates a replay hands the later, weaker answer back.
type wideningRestrictor struct {
	calls int
}

func (r *wideningRestrictor) RestrictDisclosure(
	_ context.Context, references []shoal.ID,
) ([]shoal.ID, error) {
	r.calls++
	if r.calls > len(references) {
		return append([]shoal.ID(nil), references...), nil
	}
	return append([]shoal.ID(nil), references[:r.calls-1]...), nil
}

// admissionDescriptor declares one action per effect class combination the
// tests need, bound to an executor whose ceiling covers all of them, so a
// denial in these tests is always the action's declaration and never the
// executor's.
func admissionDescriptor(now time.Time) Descriptor {
	schema := json.RawMessage(`{"type":"object"}`)
	return Descriptor{
		ID: "agent", Generation: 1, Subject: "owner", Actor: "actor",
		AuthorizationDomain: []byte("domain"),
		Scopes: []Scope{
			{SourceID: []byte("source"), PolicyID: []byte("policy")},
		},
		ExecutorRef: "exec", LeaseExpiresAt: now.Add(time.Hour), UpdatedAt: now,
		Capabilities: []Capability{{Name: "model", Actions: []Action{
			{
				Name: "complete", InputSchema: schema, OutputSchema: schema,
				Effects: Effects{EffectEgressesContent, EffectReadsCorpus},
			},
			{
				Name: "summarize", InputSchema: schema, OutputSchema: schema,
				Effects: Effects{EffectReadsCorpus},
			},
			{
				Name: "publish", InputSchema: schema, OutputSchema: schema,
				Effects: Effects{EffectMutatesExternal},
			},
		}}},
	}
}

func newAdmissionHarness(
	t *testing.T, restrictor DisclosureRestrictor,
) *admissionHarness {
	t.Helper()
	harness := &admissionHarness{
		now: time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC),
	}
	harness.clock = func() time.Time { return harness.now }
	authority, err := auth.NewAuthorityWithClock(harness.clock)
	if err != nil {
		t.Fatal(err)
	}
	registryStore := newMemoryStore()
	registry, err := NewService(Config{
		Store: registryStore, Resolver: authority.Resolver(),
		Recorder: &memoryRecorder{}, Snapshots: fixedSnapshot{harness.now},
		Executors: executorMap{"exec": ceilingExecutor{ceiling: Effects{
			EffectReadsCorpus, EffectEgressesContent, EffectMutatesExternal,
		}}},
		Clock: harness.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	registryStore.records["agent"] = Stored{
		Descriptor: admissionDescriptor(harness.now),
	}
	harness.registry = registry
	harness.store = newMemoryDispatchStore()
	harness.recorder = &dispatchRecorder{}
	harness.events = &controlledDispatchEvents{}
	dispatch, err := NewDispatchService(DispatchConfig{
		Store: harness.store, Registry: registry, Resolver: authority.Resolver(),
		Recorder: harness.recorder, Events: harness.events, Clock: harness.clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	harness.dispatch = dispatch
	service, err := NewAdmissionService(AdmissionConfig{
		Dispatch: dispatch, Restrictor: restrictor,
	})
	if err != nil {
		t.Fatal(err)
	}
	harness.service = service
	harness.authority = authority
	return harness
}

func (h *admissionHarness) context(t *testing.T, request string) context.Context {
	t.Helper()
	return bindDecision(t, h.authority, dispatchDecision(
		t, "owner", "actor", request, auth.OperationInvoke,
		auth.OperationRetrieve))
}

// invokeOnlyContext holds no retrieve authority, which is the posture of a
// caller that may act but may not read.
func (h *admissionHarness) invokeOnlyContext(
	t *testing.T, request string,
) context.Context {
	t.Helper()
	return bindDecision(t, h.authority, dispatchDecision(
		t, "owner", "actor", request, auth.OperationInvoke))
}

func (h *admissionHarness) request(
	request, id, action string,
	effects Effects,
	disclosures []shoal.ID,
) AdmissionRequest {
	return AdmissionRequest{
		ID: []byte(id), IdempotencyKey: []byte("idempotency-" + id),
		TokenID: []byte("token-" + id), AgentID: "agent", AgentGeneration: 1,
		Capability: "model", Action: action,
		SourceID: []byte("source"), PolicyID: []byte("policy"),
		ObjectID: "object", Effects: effects,
		Input: json.RawMessage(`{"prompt_digest":"abc"}`), Disclosures: disclosures,
		Lease: time.Minute, Context: dispatchContext(h.now, request),
	}
}

// TestAdmissionDeniesEffectBeyondDeclaredCapability pins the stop: a caller
// asking to egress corpus content through an action that declares only a corpus
// read is refused, the refusal is durable, and it names nothing.
func TestAdmissionDeniesEffectBeyondDeclaredCapability(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	ctx := harness.context(t, "request")
	grant, err := harness.service.Request(ctx, harness.request(
		"request", "admission", "summarize",
		Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatalf("admission request = %v", err)
	}
	if grant.Outcome != AdmissionDenied {
		t.Fatalf("outcome = %q", grant.Outcome)
	}
	if len(grant.Token.ActionID) != 0 || len(grant.Token.TokenID) != 0 ||
		grant.Token.Version != 0 {
		t.Fatalf("denial issued a token: %#v", grant.Token)
	}
	stored, err := harness.store.GetAction(ctx, []byte("admission"))
	if err != nil {
		t.Fatalf("stored denial = %v", err)
	}
	if stored.State != DispatchCanceled {
		t.Fatalf("denial state = %q", stored.State)
	}
	var canceled bool
	for _, kind := range harness.events.kinds {
		if kind == "action.canceled" {
			canceled = true
		}
	}
	if !canceled {
		t.Fatalf("denial published no lifecycle event: %v", harness.events.kinds)
	}
}

// TestAdmissionDenialAnswersFromTheRecordOnReplay pins that a refused admission
// stays refused. Re-adjudicating a replay would let a caller retry a denial
// until whatever it depended on moved.
func TestAdmissionDenialAnswersFromTheRecordOnReplay(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	first := harness.request(
		"request", "admission", "summarize", Effects{EffectEgressesContent}, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), first,
	); err != nil {
		t.Fatal(err)
	}
	replay := harness.request(
		"replay", "admission", "summarize", Effects{EffectEgressesContent}, nil)
	replay.IdempotencyKey = first.IdempotencyKey
	replay.Context = dispatchContext(harness.now, "request")
	grant, err := harness.service.Request(harness.context(t, "request"), replay)
	if err != nil {
		t.Fatalf("replayed denial = %v", err)
	}
	if grant.Outcome != AdmissionDenied {
		t.Fatalf("replayed outcome = %q", grant.Outcome)
	}
}

// TestAdmissionRequiresADeclaredEffect pins the fail-closed reading of the
// least classifiable request there is.
func TestAdmissionRequiresADeclaredEffect(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"),
		harness.request("request", "admission", "complete", nil, nil),
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("empty effect set = %v", err)
	}
	if _, err := harness.service.Request(
		harness.context(t, "request"),
		harness.request(
			"request", "admission", "complete", Effects{"invented"}, nil),
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("unrecognised effect = %v", err)
	}
}

// TestAdmissionAllowsDeclaredEffectAndIssuesALiveToken pins the allow path and
// that the token names the durable claim it was granted under.
func TestAdmissionAllowsDeclaredEffectAndIssuesALiveToken(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	ctx := harness.context(t, "request")
	grant, err := harness.service.Request(ctx, harness.request(
		"request", "admission", "complete",
		Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatalf("admission request = %v", err)
	}
	if grant.Outcome != AdmissionAllowed {
		t.Fatalf("outcome = %q", grant.Outcome)
	}
	// Version one, not two. A grant is a single durable write: the record is
	// born claimed and never passes through the queued state where an
	// ungranted admission would be claimable work.
	if string(grant.Token.ActionID) != "admission" ||
		string(grant.Token.TokenID) != "token-admission" ||
		grant.Token.Version != 1 {
		t.Fatalf("token = %#v", grant.Token)
	}
	if !grant.Token.ExpiresAt.Equal(harness.now.Add(time.Minute)) {
		t.Fatalf("token expiry = %s", grant.Token.ExpiresAt)
	}
	stored, err := harness.store.GetAction(ctx, []byte("admission"))
	if err != nil || stored.State != DispatchClaimed {
		t.Fatalf("stored grant = %#v, %v", stored, err)
	}
}

// TestAdmissionObligationsWithholdWithoutSayingWhy pins the obligation path:
// references the caller is not authorized for and references the restrictor
// withheld come back in one undifferentiated list, and the call is allowed
// rather than refused.
func TestAdmissionObligationsWithholdWithoutSayingWhy(t *testing.T) {
	restrictor := &stubRestrictor{allowed: []shoal.ID{"doc-a"}}
	harness := newAdmissionHarness(t, restrictor)
	ctx := harness.context(t, "request")
	// Declared out of order and with a repeat, because the obligation has to be
	// the same answer either way: a caller comparing two responses must be
	// comparing decisions, not its own iteration order, and a reference named
	// twice is one reference.
	grant, err := harness.service.Request(ctx, harness.request(
		"request", "admission", "complete", Effects{EffectEgressesContent},
		[]shoal.ID{"doc-c", "doc-a", "doc-b", "doc-c"}))
	if err != nil {
		t.Fatalf("admission request = %v", err)
	}
	if grant.Outcome != AdmissionObligated {
		t.Fatalf("outcome = %q", grant.Outcome)
	}
	if len(grant.Token.ActionID) == 0 {
		t.Fatal("obligated admission issued no token")
	}
	if len(grant.Obligations.Withhold) != 2 ||
		grant.Obligations.Withhold[0] != "doc-b" ||
		grant.Obligations.Withhold[1] != "doc-c" {
		t.Fatalf("withhold = %#v", grant.Obligations.Withhold)
	}
	if restrictor.calls != 1 || len(restrictor.seen[0]) != 3 {
		t.Fatalf("restrictor saw %#v", restrictor.seen)
	}
}

// TestAdmissionWithholdsEveryReferenceWithoutRetrieveAuthority pins that a
// caller which may act but may not read is obliged to withhold everything
// rather than being handed permission to transmit content it cannot read.
func TestAdmissionWithholdsEveryReferenceWithoutRetrieveAuthority(t *testing.T) {
	restrictor := &stubRestrictor{allowed: []shoal.ID{"doc-a", "doc-b"}}
	harness := newAdmissionHarness(t, restrictor)
	grant, err := harness.service.Request(
		harness.invokeOnlyContext(t, "request"), harness.request(
			"request", "admission", "complete", Effects{EffectEgressesContent},
			[]shoal.ID{"doc-a", "doc-b"}))
	if err != nil {
		t.Fatalf("admission request = %v", err)
	}
	if grant.Outcome != AdmissionObligated ||
		len(grant.Obligations.Withhold) != 2 {
		t.Fatalf("grant = %#v", grant)
	}
	// The restrictor is consulted, and learns about nothing. A reference
	// authorization already withheld must not be handed on to a downstream
	// control, which would charge a budget for a disclosure that cannot happen.
	if restrictor.calls != 1 || len(restrictor.seen[0]) != 0 {
		t.Fatalf("restrictor saw unauthorized references: %#v", restrictor.seen)
	}
}

// TestAdmissionRestrictorCannotWidenAuthorization pins the direction of the
// intersection. A restrictor is a narrowing control: a reference it returns
// that authorization already refused must stay refused, and a reference the
// caller never declared must not appear at all.
func TestAdmissionRestrictorCannotWidenAuthorization(t *testing.T) {
	restrictor := &stubRestrictor{
		allowed: []shoal.ID{"doc-a", "doc-elsewhere"},
	}
	harness := newAdmissionHarness(t, restrictor)
	grant, err := harness.service.Request(
		harness.invokeOnlyContext(t, "request"), harness.request(
			"request", "admission", "complete", Effects{EffectEgressesContent},
			[]shoal.ID{"doc-a"}))
	if err != nil {
		t.Fatalf("admission request = %v", err)
	}
	if grant.Outcome != AdmissionObligated {
		t.Fatalf("outcome = %q", grant.Outcome)
	}
	if len(grant.Obligations.Withhold) != 1 ||
		grant.Obligations.Withhold[0] != "doc-a" {
		t.Fatalf("withhold = %#v", grant.Obligations.Withhold)
	}
}

// TestAdmissionRestrictorFailureIsNotAnEmptyObligation pins that a restrictor
// which cannot answer stops the request rather than being read as "withhold
// nothing".
func TestAdmissionRestrictorFailureIsNotAnEmptyObligation(t *testing.T) {
	restrictor := &stubRestrictor{err: errors.New("ledger unavailable")}
	harness := newAdmissionHarness(t, restrictor)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete", Effects{EffectEgressesContent},
			[]shoal.ID{"doc-a"}),
	); err == nil {
		t.Fatal("restrictor failure produced an answer")
	}
}

// TestAdmissionRefusesAGrantHeldUnderAnotherToken pins that a live admission is
// not handed to a second caller replaying the same admission identity with a
// token of its own.
func TestAdmissionRefusesAGrantHeldUnderAnotherToken(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	first := harness.request(
		"request", "admission", "complete", Effects{EffectEgressesContent}, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.TokenID = []byte("token-stolen")
	if _, err := harness.service.Request(
		harness.context(t, "request"), second,
	); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("second token = %v", err)
	}
}

// TestReportClosesTheLoopAndSpendsItsToken pins the report path: the outcome is
// recorded durably, an identical replay is idempotent, and a differing report
// against the same token is refused.
func TestReportClosesTheLoopAndSpendsItsToken(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	ctx := harness.context(t, "request")
	grant, err := harness.service.Request(ctx, harness.request(
		"request", "admission", "complete",
		Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	report := AdmissionReport{
		Token: grant.Token, Outcome: json.RawMessage(`{"tokens":41}`),
		Context: dispatchContext(harness.now, "report"),
	}
	record, err := harness.service.Report(
		harness.context(t, "report"), report)
	if err != nil || record.State != DispatchSucceeded {
		t.Fatalf("report = %#v, %v", record, err)
	}
	replay, err := harness.service.Report(
		harness.context(t, "report"), report)
	if err != nil || replay.Version != record.Version {
		t.Fatalf("identical replay = %#v, %v", replay, err)
	}
	different := report
	different.Outcome = json.RawMessage(`{"tokens":9999}`)
	if _, err := harness.service.Report(
		harness.context(t, "report"), different,
	); !errors.Is(err, ErrAdmissionSpent) {
		t.Fatalf("differing replay = %v", err)
	}
	failed := report
	failed.Failed = true
	failed.ErrorCode = "upstream_refused"
	failed.Outcome = nil
	if _, err := harness.service.Report(
		harness.context(t, "report"), failed,
	); !errors.Is(err, ErrAdmissionSpent) {
		t.Fatalf("failure against a spent token = %v", err)
	}
	// A malformed report is refused as malformed even against a spent token.
	// Answering "spent" here would tell a caller its report was well formed and
	// merely late, and it would send the same malformed body to the next
	// admission expecting it to land.
	reasonless := failed
	reasonless.ErrorCode = ""
	if _, err := harness.service.Report(
		harness.context(t, "report"), reasonless,
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("reasonless failure against a spent token = %v", err)
	}
}

// TestReportAfterTheTokenExpiresIsSpent pins that a caller which held a grant,
// went silent past its lease, and came back cannot write the record. By then
// the admission is abandoned and whether the effect happened is unknown.
func TestReportAfterTheTokenExpiresIsSpent(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	harness.now = harness.now.Add(2 * time.Minute)
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		},
	); !errors.Is(err, ErrAdmissionSpent) {
		t.Fatalf("report past the lease = %v", err)
	}
}

// TestReportRejectsATokenSpentOnADenial pins that a refused admission's record
// cannot be reported against. A report is a statement that an effect occurred.
func TestReportRejectsATokenSpentOnADenial(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "summarize",
			Effects{EffectEgressesContent}, nil),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: AdmissionToken{
				ActionID: []byte("admission"),
				TokenID:  []byte("token-admission"), Version: 1,
			},
			Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		},
	); !errors.Is(err, ErrAdmissionSpent) {
		t.Fatalf("report against a denial = %v", err)
	}
}

// TestReportRefusesAFailureWithoutAReasonOrAnOutcomeWithOne pins that a report
// is either an outcome or a failure, never both and never neither.
func TestReportRefusesAFailureWithoutAReasonOrAnOutcomeWithOne(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Failed: true,
			Context: dispatchContext(harness.now, "report"),
		},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("failure with no reason = %v", err)
	}
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, ErrorCode: "quiet_failure",
			Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("outcome carrying an error code = %v", err)
	}
}

// TestReportDoesNotDistinguishAbsentFromAnotherPrincipals pins the disclosure
// shape: a token naming someone else's admission is not-found, exactly as a
// token naming nothing is.
func TestReportDoesNotDistinguishAbsentFromAnotherPrincipals(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil)); err != nil {
		t.Fatal(err)
	}
	intruder := bindDecision(t, harness.authority, dispatchDecision(
		t, "mallory", "mallory", "probe", auth.OperationInvoke))
	present := AdmissionReport{
		Token: AdmissionToken{
			ActionID: []byte("admission"),
			TokenID:  []byte("token-admission"), Version: 2,
		},
		Outcome: json.RawMessage(`{"tokens":1}`),
		Context: dispatchContext(harness.now, "probe"),
	}
	absent := present
	absent.Token.ActionID = []byte("never-existed")
	_, presentErr := harness.service.Report(intruder, present)
	_, absentErr := harness.service.Report(intruder, absent)
	if !shoal.IsErrorCode(presentErr, shoal.ErrorNotFound) ||
		!shoal.IsErrorCode(absentErr, shoal.ErrorNotFound) {
		t.Fatalf("probe errors = %v / %v", presentErr, absentErr)
	}
	if presentErr.Error() != absentErr.Error() {
		t.Fatalf("probe distinguished existence: %q vs %q",
			presentErr, absentErr)
	}
}

// TestOutstandingShowsAGrantNobodyReported pins the observability requirement:
// an admitted call that never comes back is visible, stays visible after its
// token expires, and disappears only when it is reported.
func TestOutstandingShowsAGrantNobodyReported(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	listRequest := OutstandingAdmissionsRequest{
		Limit: 10, Context: dispatchContext(harness.now, "list"),
	}
	page, err := harness.service.Outstanding(
		harness.context(t, "list"), listRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Admissions) != 1 ||
		string(page.Admissions[0].ActionID) != "admission" ||
		string(page.Admissions[0].TokenID) != "token-admission" ||
		page.Admissions[0].Expired {
		t.Fatalf("outstanding = %#v", page.Admissions)
	}

	harness.now = harness.now.Add(2 * time.Minute)
	listRequest.Context = dispatchContext(harness.now, "list")
	page, err = harness.service.Outstanding(
		harness.context(t, "list"), listRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Admissions) != 1 || !page.Admissions[0].Expired {
		t.Fatalf("expired outstanding = %#v", page.Admissions)
	}

	harness.now = harness.now.Add(-2 * time.Minute)
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		}); err != nil {
		t.Fatal(err)
	}
	listRequest.Context = dispatchContext(harness.now, "list")
	page, err = harness.service.Outstanding(
		harness.context(t, "list"), listRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Admissions) != 0 {
		t.Fatalf("reported admission still outstanding: %#v", page.Admissions)
	}
}

// TestOutstandingHidesOtherPrincipalsGrants pins that the outstanding list is
// not a window on to another caller's admissions.
func TestOutstandingHidesOtherPrincipalsGrants(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil)); err != nil {
		t.Fatal(err)
	}
	intruder := bindDecision(t, harness.authority, dispatchDecision(
		t, "mallory", "mallory", "probe", auth.OperationInvoke))
	page, err := harness.service.Outstanding(
		intruder, OutstandingAdmissionsRequest{
			Limit: 10, Context: dispatchContext(harness.now, "probe"),
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Admissions) != 0 {
		t.Fatalf("leaked admissions: %#v", page.Admissions)
	}
}

// TestAdmissionReplayCannotChangeWhatWasAdmitted pins the durable declaration.
//
// The laundering it prevents: ask with corpus references, receive obligations
// restricting them, then replay the same action ID, idempotency key and token
// with the references removed. Before the declaration was part of the record's
// identity that replay was recognised as the same request, obligations were
// recomputed over nothing, and the answer was an unrestricted allow for a token
// that was already live.
func TestAdmissionReplayCannotChangeWhatWasAdmitted(t *testing.T) {
	restrictor := &stubRestrictor{allowed: []shoal.ID{"doc-a"}}
	harness := newAdmissionHarness(t, restrictor)
	first := harness.request(
		"request", "admission", "complete", Effects{EffectEgressesContent},
		[]shoal.ID{"doc-a", "doc-b"})
	grant, err := harness.service.Request(harness.context(t, "request"), first)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Outcome != AdmissionObligated ||
		len(grant.Obligations.Withhold) != 1 {
		t.Fatalf("first grant = %#v", grant)
	}

	laundered := first
	laundered.Disclosures = nil
	if _, err := harness.service.Request(
		harness.context(t, "request"), laundered,
	); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("replay without disclosures = %v", err)
	}
	widened := first
	widened.Effects = Effects{EffectEgressesContent, EffectReadsCorpus}
	if _, err := harness.service.Request(
		harness.context(t, "request"), widened,
	); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("replay with a wider effect set = %v", err)
	}
	// Same size, different contents. A comparison that stopped at the length
	// would let a caller admitted to egress replay as a corpus read and keep
	// the live token, so the audit record would name an effect nobody was
	// granted.
	swapped := first
	swapped.Effects = Effects{EffectReadsCorpus}
	if _, err := harness.service.Request(
		harness.context(t, "request"), swapped,
	); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("replay with a swapped effect set = %v", err)
	}
	narrowed := first
	narrowed.Disclosures = []shoal.ID{"doc-a"}
	if _, err := harness.service.Request(
		harness.context(t, "request"), narrowed,
	); !errors.Is(err, ErrAdmissionConflict) {
		t.Fatalf("replay with fewer disclosures = %v", err)
	}

	// The identical request still replays on to its own grant, or a caller that
	// lost a response could never recover its token.
	again, err := harness.service.Request(harness.context(t, "request"), first)
	if err != nil {
		t.Fatalf("identical replay = %v", err)
	}
	if again.Outcome != AdmissionObligated ||
		!bytes.Equal(again.Token.TokenID, grant.Token.TokenID) ||
		again.Token.Version != grant.Token.Version {
		t.Fatalf("identical replay = %#v", again)
	}
}

// TestDispatchEnqueueCannotReuseAnAdmissionIdentity pins the same binding from
// the other side. The declaration is part of enqueue equivalence, so a plain
// dispatch enqueue cannot land on an admission's record and inherit its token.
func TestDispatchEnqueueCannotReuseAnAdmissionIdentity(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil)); err != nil {
		t.Fatal(err)
	}
	dispatcher := bindDecision(t, harness.authority, dispatchDecision(
		t, "owner", "actor", "request", auth.OperationDispatch))
	if _, err := harness.dispatch.Enqueue(dispatcher, EnqueueRequest{
		ID:      []byte("admission"),
		AgentID: "agent", AgentGeneration: 1,
		IdempotencyKey: []byte("idempotency-admission"),
		Capability:     "model", Action: "complete",
		SourceID: []byte("source"), PolicyID: []byte("policy"),
		ObjectID: "object",
		Input:    json.RawMessage(`{"prompt_digest":"abc"}`),
		Context:  dispatchContext(harness.now, "request"),
	},
	); !errors.Is(err, ErrActionConflict) {
		t.Fatalf("dispatch enqueue over an admission = %v", err)
	}
}

// TestAdmissionNeverLeavesClaimableWork pins the stop.
//
// No admission outcome — granted, refused, or abandoned part way through
// adjudication — may leave a record Pull will hand back. A queued record is
// claimable through the dispatch surface, and under the execution boundary a
// claim is permission to perform the declared effect out of process, so a
// refused or never-decided admission sitting in the queue is the refusal being
// laundered into permission through a different door.
func TestAdmissionNeverLeavesClaimableWork(t *testing.T) {
	failing := &stubRestrictor{err: errors.New("ledger unavailable")}
	harness := newAdmissionHarness(t, failing)

	// Adjudication fails after every check that precedes the write.
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "aborted", "complete", Effects{EffectEgressesContent},
			[]shoal.ID{"doc-a"}),
	); err == nil {
		t.Fatal("restrictor failure produced an answer")
	}
	if _, err := harness.store.GetAction(
		context.Background(), []byte("aborted"),
	); !errors.Is(err, ErrActionNotFound) {
		t.Fatalf("aborted admission left a record: %v", err)
	}

	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "refused", "summarize", Effects{EffectEgressesContent},
			nil)); err != nil {
		t.Fatal(err)
	}
	refused, err := harness.store.GetAction(
		context.Background(), []byte("refused"))
	if err != nil || refused.State != DispatchCanceled || refused.Version != 1 {
		t.Fatalf("refusal record = %#v, %v", refused, err)
	}

	harness.service.restrictor = nil
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "granted", "complete", Effects{EffectEgressesContent},
			nil)); err != nil {
		t.Fatal(err)
	}

	page, err := harness.dispatch.Pull(
		harness.context(t, "request"), PullActionsRequest{
			Limit: 16, Context: dispatchContext(harness.now, "request"),
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Actions) != 0 {
		t.Fatalf("admission left claimable work: %#v", page.Actions)
	}
}

// TestReportRefusesAFailureCarryingAnOutcome pins the half of "either an
// outcome or a failure" that the completion path cannot enforce for us.
//
// The completion path discards a failed report's outcome, so the durable record
// says nothing about it and the replay comparison would read any two failures
// with the same error code as the same report. A caller could then report a
// failure, then replace the outcome it carried and be told the second landed.
func TestReportRefusesAFailureCarryingAnOutcome(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Failed: true, ErrorCode: "upstream_refused",
			Outcome: json.RawMessage(`{"tokens":41}`),
			Context: dispatchContext(harness.now, "report"),
		},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("failure carrying an outcome = %v", err)
	}
	if stored, err := harness.store.GetAction(
		context.Background(), []byte("admission"),
	); err != nil || stored.State != DispatchClaimed {
		t.Fatalf("refused report moved the record: %#v, %v", stored, err)
	}
}

// TestReportingAFailureIsASuccessfulReport pins that the first response and the
// retry agree.
//
// The completion path is built for an executor, where a failed outcome is the
// executor's error and comes back alongside the committed record. Propagating
// it here made the first response an error and the identical retry a receipt,
// so what a caller saw depended on whether its own report had committed — the
// exact confusion the one-shot token exists to remove.
func TestReportingAFailureIsASuccessfulReport(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	report := AdmissionReport{
		Token: grant.Token, Failed: true, ErrorCode: "upstream_refused",
		Context: dispatchContext(harness.now, "report"),
	}
	record, err := harness.service.Report(harness.context(t, "report"), report)
	if err != nil {
		t.Fatalf("reported failure = %v", err)
	}
	if record.State != DispatchFailed ||
		record.ErrorCode != "upstream_refused" {
		t.Fatalf("failure record = %#v", record)
	}
	replay, err := harness.service.Report(harness.context(t, "report"), report)
	if err != nil {
		t.Fatalf("replayed failure = %v", err)
	}
	if replay.Version != record.Version || replay.State != record.State {
		t.Fatalf("replay disagreed with the first response: %#v vs %#v",
			replay, record)
	}
}

// TestReportKeepsAnUnconfirmedOutcomeAnError pins the boundary of that
// conversion. Only a record that is this report, committed, becomes a receipt;
// an outcome the service cannot confirm it wrote must stay an error, or a
// caller learns its report landed when nothing says it did.
func TestReportKeepsAnUnconfirmedOutcomeAnError(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	harness.recorder.failPhase = "effect_outcome"
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Failed: true, ErrorCode: "upstream_refused",
			Context: dispatchContext(harness.now, "report"),
		},
	); !errors.Is(err, ErrExecutionAmbiguous) {
		t.Fatalf("unrecordable failure = %v", err)
	}
}

// TestReplayReturnsTheObligationTheTokenWasGrantedUnder pins that the decision
// is replayed, not re-made.
//
// The restrictor is windowed and observes intervening calls, so recomputing on
// a retry can return a weaker obligation for a token that is already live — a
// caller could replay its way out of a restriction it was told to honour. The
// stub here returns a narrower answer on every call, which a recomputing
// implementation would hand straight back.
func TestReplayReturnsTheObligationTheTokenWasGrantedUnder(t *testing.T) {
	restrictor := &wideningRestrictor{}
	harness := newAdmissionHarness(t, restrictor)
	request := harness.request(
		"request", "admission", "complete", Effects{EffectEgressesContent},
		[]shoal.ID{"doc-a", "doc-b"})
	grant, err := harness.service.Request(
		harness.context(t, "request"), request)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Outcome != AdmissionObligated ||
		len(grant.Obligations.Withhold) != 2 {
		t.Fatalf("first grant = %#v", grant)
	}

	replayed, err := harness.service.Request(
		harness.context(t, "request"), request)
	if err != nil {
		t.Fatalf("replay = %v", err)
	}
	if replayed.Outcome != grant.Outcome ||
		len(replayed.Obligations.Withhold) != 2 ||
		replayed.Obligations.Withhold[0] != grant.Obligations.Withhold[0] ||
		replayed.Obligations.Withhold[1] != grant.Obligations.Withhold[1] {
		t.Fatalf("replay weakened the obligation: %#v, was %#v",
			replayed.Obligations, grant.Obligations)
	}
	if restrictor.calls != 1 {
		t.Fatalf("replay re-adjudicated: restrictor called %d times",
			restrictor.calls)
	}
}

// TestReplayRecoversAGrantWhileTheRestrictorIsDown pins the other half. An
// admission that was granted must stay recoverable: a caller that lost its
// response has an outstanding token it cannot report against until it can read
// the grant back, and a transient outage in a control that has already been
// consulted must not extend into one.
func TestReplayRecoversAGrantWhileTheRestrictorIsDown(t *testing.T) {
	restrictor := &stubRestrictor{allowed: []shoal.ID{"doc-a"}}
	harness := newAdmissionHarness(t, restrictor)
	request := harness.request(
		"request", "admission", "complete", Effects{EffectEgressesContent},
		[]shoal.ID{"doc-a", "doc-b"})
	grant, err := harness.service.Request(
		harness.context(t, "request"), request)
	if err != nil {
		t.Fatal(err)
	}

	restrictor.err = errors.New("ledger unavailable")
	replayed, err := harness.service.Request(
		harness.context(t, "request"), request)
	if err != nil {
		t.Fatalf("replay during a restrictor outage = %v", err)
	}
	if !bytes.Equal(replayed.Token.TokenID, grant.Token.TokenID) ||
		replayed.Token.Version != grant.Token.Version {
		t.Fatalf("replay = %#v, want the original token %#v",
			replayed.Token, grant.Token)
	}
	if len(replayed.Obligations.Withhold) != 1 ||
		replayed.Obligations.Withhold[0] != "doc-b" {
		t.Fatalf("replayed obligation = %#v", replayed.Obligations)
	}
}

// TestObligationBitmapRoundTripsPositions pins the encoding the replay depends
// on: the stored positions must name the same references when indexed back
// against the declared list, and nothing must be stored when nothing is
// withheld.
func TestObligationBitmapRoundTripsPositions(t *testing.T) {
	declared := make([]shoal.ID, 20)
	for i := range declared {
		declared[i] = shoal.ID("doc-" + string(rune('a'+i)))
	}
	withheld := Obligations{Withhold: []shoal.ID{
		declared[0], declared[7], declared[8], declared[19],
	}}
	bitmap := obligationBitmap(declared, withheld)
	if len(bitmap) != 3 {
		t.Fatalf("bitmap length = %d, want one bit per declared reference", len(bitmap))
	}
	rebuilt := obligationFromBitmap(bitmap, declared)
	if len(rebuilt.Withhold) != len(withheld.Withhold) {
		t.Fatalf("rebuilt = %#v, want %#v", rebuilt, withheld)
	}
	for i := range withheld.Withhold {
		if rebuilt.Withhold[i] != withheld.Withhold[i] {
			t.Fatalf("rebuilt[%d] = %q, want %q",
				i, rebuilt.Withhold[i], withheld.Withhold[i])
		}
	}
	if obligationBitmap(declared, Obligations{}) != nil {
		t.Fatal("an empty obligation must store nothing")
	}
	if len(obligationFromBitmap(nil, declared).Withhold) != 0 {
		t.Fatal("no stored obligation must rebuild as no obligation")
	}
}

// TestReportRefusesAMalformedFailureWithoutSpendingTheToken pins that a report
// the service will not record leaves the token usable.
//
// The completion path is written for an executor that has already performed the
// work, so it records a malformed result as invalid_executor_error rather than
// refusing it. Reaching that from here would spend a live one-shot token on a
// failure the caller was never told about, and leave it unable to report the
// outcome it actually has.
func TestReportRefusesAMalformedFailureWithoutSpendingTheToken(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{
		strings.Repeat("x", MaxActionErrorBytes+1),
		" untrimmed",
	} {
		if _, err := harness.service.Report(
			harness.context(t, "report"), AdmissionReport{
				Token: grant.Token, Failed: true, ErrorCode: code,
				Context: dispatchContext(harness.now, "report"),
			},
		); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
			t.Fatalf("malformed error code %q = %v", code, err)
		}
	}
	// A malformed outcome is the same shape of problem and gets the same
	// answer: the completion path would commit invalid_executor_output.
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Outcome: json.RawMessage(`"not-an-object"`),
			Context: dispatchContext(harness.now, "report"),
		},
	); !shoal.IsErrorCode(err, shoal.ErrorInvalidArgument) {
		t.Fatalf("malformed outcome = %v", err)
	}

	stored, err := harness.store.GetAction(
		context.Background(), []byte("admission"))
	if err != nil || stored.State != DispatchClaimed || stored.Version != 1 {
		t.Fatalf("a refused report moved the record: %#v, %v", stored, err)
	}
	// The token still works.
	if _, err := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: grant.Token, Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		}); err != nil {
		t.Fatalf("token was spent by the refused reports: %v", err)
	}
}

// TestAdmissionSurfaceRefusesOrdinaryDispatchActions pins the read side of the
// admission marker.
//
// A claimed dispatch action owned by the same principal passes every other
// check on the report path — the registry resolves it, the claim is live — and
// would then be completed under this surface's semantics rather than its own. A
// reported failure is a receipt here and the executor's error there, so a
// worker completing through this endpoint would be told its failed work had
// succeeded.
func TestAdmissionSurfaceRefusesOrdinaryDispatchActions(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	dispatcher := bindDecision(t, harness.authority, dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke))
	queued, err := harness.dispatch.Enqueue(dispatcher, EnqueueRequest{
		ID: []byte("dispatched"), IdempotencyKey: []byte("idempotency-dispatched"),
		AgentID: "agent", AgentGeneration: 1,
		Capability: "model", Action: "complete",
		SourceID: []byte("source"), PolicyID: []byte("policy"),
		ObjectID: "object",
		Input:    json.RawMessage(`{"prompt_digest":"abc"}`),
		Context:  dispatchContext(harness.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := harness.dispatch.Claim(dispatcher, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		ClaimID: []byte("worker"), Lease: time.Minute,
		Context: dispatchContext(harness.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Not-found, and identical to a token naming nothing at all: the caller
	// owns this record, and saying so would distinguish a dispatch action it
	// holds from an admission identity that does not exist.
	token := AdmissionToken{
		ActionID: claimed.ID, TokenID: claimed.ClaimID,
		Version: claimed.Version,
	}
	_, presentErr := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: token, Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		})
	absent := token
	absent.ActionID = []byte("never-existed")
	_, absentErr := harness.service.Report(
		harness.context(t, "report"), AdmissionReport{
			Token: absent, Outcome: json.RawMessage(`{"tokens":1}`),
			Context: dispatchContext(harness.now, "report"),
		})
	if !shoal.IsErrorCode(presentErr, shoal.ErrorNotFound) {
		t.Fatalf("reporting a dispatch action = %v", presentErr)
	}
	if presentErr.Error() != absentErr.Error() {
		t.Fatalf("refusal distinguished a dispatch action from nothing: %q vs %q",
			presentErr, absentErr)
	}
	if stored, err := harness.store.GetAction(
		context.Background(), []byte("dispatched"),
	); err != nil || stored.State != DispatchClaimed {
		t.Fatalf("refused report moved the dispatch record: %#v, %v", stored, err)
	}

	// Nor is it outstanding. Listing a record this surface refuses to close
	// would name work the caller cannot act on.
	page, err := harness.service.Outstanding(
		harness.context(t, "list"), OutstandingAdmissionsRequest{
			Limit: 16, Context: dispatchContext(harness.now, "list"),
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Admissions) != 0 {
		t.Fatalf("dispatch claim listed as an outstanding admission: %#v",
			page.Admissions)
	}
}

// TestDispatchReclaimAdvancesTheFence pins that the shared claim transition
// advances the fence rather than assigning it.
//
// An admission is born claimed at fence one, which an assignment would also
// produce — so the two cases only diverge on a re-claim, where a stale worker
// holding the previous fence must become detectable. Nothing pinned this before
// the two paths were merged into one function, and a shared assignment would
// have silently let a reclaimed action keep its old fence.
func TestDispatchReclaimAdvancesTheFence(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	dispatcher := bindDecision(t, harness.authority, dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke))
	queued, err := harness.dispatch.Enqueue(dispatcher, EnqueueRequest{
		ID: []byte("dispatched"), IdempotencyKey: []byte("idempotency-dispatched"),
		AgentID: "agent", AgentGeneration: 1,
		Capability: "model", Action: "complete",
		SourceID: []byte("source"), PolicyID: []byte("policy"),
		ObjectID: "object",
		Input:    json.RawMessage(`{"prompt_digest":"abc"}`),
		Context:  dispatchContext(harness.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := harness.dispatch.Claim(dispatcher, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		ClaimID: []byte("worker-one"), Lease: time.Minute,
		Context: dispatchContext(harness.now, "request"),
	})
	if err != nil || first.ClaimFence != 1 {
		t.Fatalf("first claim = %#v, %v", first, err)
	}

	// The lease lapses and a second worker takes the action.
	harness.now = harness.now.Add(2 * time.Minute)
	reclaimer := bindDecision(t, harness.authority, dispatchDecision(
		t, "owner", "actor", "retry",
		auth.OperationDispatch, auth.OperationInvoke))
	second, err := harness.dispatch.Claim(reclaimer, ClaimRequest{
		ID: queued.ID, ExpectedVersion: first.Version,
		ClaimID: []byte("worker-two"), Lease: time.Minute,
		Context: dispatchContext(harness.now, "retry"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ClaimFence != first.ClaimFence+1 {
		t.Fatalf("reclaim fence = %d, want %d",
			second.ClaimFence, first.ClaimFence+1)
	}
}

// TestAdmissionDisclosureDigestSeparatesDistinctSets pins the framing. Without
// a length prefix per element, two different declarations concatenate to the
// same bytes and a retry could swap one for the other under a live token.
func TestAdmissionDisclosureDigestSeparatesDistinctSets(t *testing.T) {
	left := disclosureDigest([]shoal.ID{"ab", "c"})
	right := disclosureDigest([]shoal.ID{"a", "bc"})
	if bytes.Equal(left, right) {
		t.Fatalf("distinct declarations digest identically: %x", left)
	}
	if disclosureDigest(nil) != nil {
		t.Fatal("an empty declaration must digest to nothing")
	}
	if !bytes.Equal(left, disclosureDigest([]shoal.ID{"ab", "c"})) {
		t.Fatal("the same declaration must digest identically")
	}
}

// TestAdmittedDeclarationShapeIsValidated pins what a durable record may say it
// admitted. Membership is deliberately not checked — an unrecognised class is
// refused at resolution, and refusing to decode would lose the audit trail for
// exactly the admissions most worth reading — but shape is, because shape is
// what makes two declarations comparable.
func TestAdmittedDeclarationShapeIsValidated(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	base := ActionRecord{
		ID: []byte("action"), IdempotencyKey: []byte("key"), Version: 1,
		State: DispatchQueued, AgentID: "agent", AgentGeneration: 1,
		Capability: "model", Action: "complete",
		SourceID: []byte("source"), PolicyID: []byte("policy"),
		ObjectID: "object", Input: json.RawMessage(`{}`),
		Subject: "owner", Actor: "actor", PolicyGeneration: 1,
		AuthorizationExpiresAt: now.Add(time.Hour), RequestID: "request",
		Reason:               mustReason(t),
		AuthorizedOperations: []auth.Operation{auth.OperationInvoke},
		ExecutorKey:          []byte("executor"),
		Deadline:             now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("baseline record = %v", err)
	}

	unordered := base
	unordered.AdmittedEffects = Effects{
		EffectReadsCorpus, EffectEgressesContent}
	if err := unordered.Validate(); err == nil {
		t.Fatal("accepted a non-canonical declared effect set")
	}
	duplicated := base
	duplicated.AdmittedEffects = Effects{
		EffectReadsCorpus, EffectReadsCorpus}
	if err := duplicated.Validate(); err == nil {
		t.Fatal("accepted a duplicated declared effect")
	}
	unknown := base
	unknown.AdmittedEffects = Effects{"invented"}
	if err := unknown.Validate(); err != nil {
		t.Fatalf("refused to decode an unrecognised class: %v", err)
	}

	stunted := base
	stunted.AdmittedEffects = Effects{EffectEgressesContent}
	stunted.AdmittedDisclosures = []byte("not-a-digest")
	if err := stunted.Validate(); err == nil {
		t.Fatal("accepted a disclosure field that is not a digest")
	}
	orphaned := base
	orphaned.AdmittedDisclosures = disclosureDigest([]shoal.ID{"doc-a"})
	if err := orphaned.Validate(); err == nil {
		t.Fatal("accepted declared references with no declared effect")
	}
	whole := base
	whole.AdmittedEffects = Effects{EffectEgressesContent}
	whole.AdmittedDisclosures = disclosureDigest([]shoal.ID{"doc-a"})
	if err := whole.Validate(); err != nil {
		t.Fatalf("complete declaration = %v", err)
	}

	// An obligation names positions in a declared list. Without the list there
	// is nothing for it to index, and a record carrying one has been assembled
	// by something that did not go through the grant path.
	unanchored := base
	unanchored.AdmittedEffects = Effects{EffectEgressesContent}
	unanchored.AdmittedObligation = []byte{0b0000_0001}
	if err := unanchored.Validate(); err == nil {
		t.Fatal("accepted an obligation with no declared references")
	}
	oversized := whole
	oversized.AdmittedObligation = make(
		[]byte, MaxAdmittedObligationBytes+1)
	if err := oversized.Validate(); err == nil {
		t.Fatal("accepted an obligation wider than the declaration bound")
	}
	obliged := whole
	obliged.AdmittedObligation = []byte{0b0000_0001}
	if err := obliged.Validate(); err != nil {
		t.Fatalf("complete obligation = %v", err)
	}
}

// TestAdmissionRecordDoesNotAliasItsDeclaration pins that a record handed out
// shares no backing array with the one the store holds.
//
// Copying the struct carries the slice headers on its own, so the declaration
// survives a clone that forgets to reallocate — what it does not survive is a
// caller writing through the header it was given. A record that aliases store
// state lets whatever holds it edit what was admitted after the fact.
func TestAdmissionRecordDoesNotAliasItsDeclaration(t *testing.T) {
	// A restrictor that withholds, so the record carries an obligation as well
	// as a declaration; all three fields have to be independently owned.
	restrictor := &stubRestrictor{allowed: []shoal.ID{"doc-a"}}
	harness := newAdmissionHarness(t, restrictor)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "complete",
			Effects{EffectEgressesContent},
			[]shoal.ID{"doc-a", "doc-b"})); err != nil {
		t.Fatal(err)
	}
	handed, err := harness.store.GetAction(
		context.Background(), []byte("admission"))
	if err != nil {
		t.Fatal(err)
	}
	if len(handed.AdmittedEffects) != 1 ||
		len(handed.AdmittedDisclosures) == 0 ||
		len(handed.AdmittedObligation) == 0 {
		t.Fatalf("stored declaration = %#v", handed)
	}
	handed.AdmittedEffects[0] = EffectMutatesExternal
	handed.AdmittedDisclosures[0] ^= 0xff
	handed.AdmittedObligation[0] ^= 0xff

	reread, err := harness.store.GetAction(
		context.Background(), []byte("admission"))
	if err != nil {
		t.Fatal(err)
	}
	if reread.AdmittedEffects[0] != EffectEgressesContent {
		t.Fatalf("declared effects were edited through the handed record: %#v",
			reread.AdmittedEffects)
	}
	if bytes.Equal(reread.AdmittedDisclosures, handed.AdmittedDisclosures) {
		t.Fatal("declared references were edited through the handed record")
	}
	// The obligation most of all: editing it through a handed-out record would
	// change what a replay tells the caller to withhold.
	if bytes.Equal(reread.AdmittedObligation, handed.AdmittedObligation) {
		t.Fatal("the obligation was edited through the handed record")
	}
}

func mustReason(t *testing.T) interaction.Reason {
	t.Helper()
	reason, err := interaction.NewReason("operator_request", "")
	if err != nil {
		t.Fatal(err)
	}
	return reason
}

// TestAdmissionClampsItsLeaseToTheActionDeadline pins that a token never
// outlives the action it was granted against. A token live past the deadline is
// one the report path will refuse, so issuing it tells a caller it has longer
// than it does.
func TestAdmissionClampsItsLeaseToTheActionDeadline(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	request := harness.request(
		"request", "admission", "complete", Effects{EffectEgressesContent}, nil)
	request.Lease = time.Minute
	request.Context.Deadline = harness.now.Add(30 * time.Second)
	grant, err := harness.service.Request(
		harness.context(t, "request"), request)
	if err != nil {
		t.Fatal(err)
	}
	if !grant.Token.ExpiresAt.Equal(harness.now.Add(30 * time.Second)) {
		t.Fatalf("token expiry = %s, want the action deadline",
			grant.Token.ExpiresAt)
	}
}

// TestAdmissionChargesAPossibleEffectExactlyAsAClaimDoes pins that a grant and
// a plain dispatch claim of the same action agree about what may already have
// happened.
//
// Egress counts. An earlier version of this test asserted the opposite,
// carrying forward a rationale that had already been corrected on the dispatch
// side — so the test defended the drift instead of catching it. A caller
// granted permission to transmit may have transmitted before it went silent,
// and content that left the host cannot be recalled, so a record asserting no
// effect was possible asserts the one thing nobody knows.
func TestAdmissionChargesAPossibleEffectExactlyAsAClaimDoes(t *testing.T) {
	for _, test := range []struct {
		name   string
		id     string
		action string
		effect Effect
	}{
		{"external", "mutating", "publish", EffectMutatesExternal},
		{"egress", "egressing", "complete", EffectEgressesContent},
	} {
		t.Run(test.name, func(t *testing.T) {
			harness := newAdmissionHarness(t, nil)
			if _, err := harness.service.Request(
				harness.context(t, "request"), harness.request(
					"request", test.id, test.action,
					Effects{test.effect}, nil)); err != nil {
				t.Fatal(err)
			}
			granted, err := harness.store.GetAction(
				context.Background(), []byte(test.id))
			if err != nil {
				t.Fatal(err)
			}
			if !granted.EffectPossible {
				t.Fatalf("%s admission left EffectPossible false", test.name)
			}
		})
	}

	// An action that neither transmits nor mutates leaves its whole outcome in
	// Shoal's own record, so nothing has to be assumed about it. Without this
	// the assertion above would pass against a grant that set the flag
	// unconditionally.
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "reading", "summarize",
			Effects{EffectReadsCorpus}, nil)); err != nil {
		t.Fatal(err)
	}
	reading, err := harness.store.GetAction(
		context.Background(), []byte("reading"))
	if err != nil || reading.EffectPossible {
		t.Fatalf("corpus-read admission = %#v, %v", reading, err)
	}
}

// TestAdmissionAndDispatchClaimAgreeOnTheRecord pins the structural fix behind
// that agreement: the two paths write the same claim state because they are the
// same function, not because two conditions were kept in step.
func TestAdmissionAndDispatchClaimAgreeOnTheRecord(t *testing.T) {
	harness := newAdmissionHarness(t, nil)
	if _, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admitted", "complete",
			Effects{EffectEgressesContent}, nil)); err != nil {
		t.Fatal(err)
	}
	admitted, err := harness.store.GetAction(
		context.Background(), []byte("admitted"))
	if err != nil {
		t.Fatal(err)
	}

	dispatcher := bindDecision(t, harness.authority, dispatchDecision(
		t, "owner", "actor", "request",
		auth.OperationDispatch, auth.OperationInvoke))
	queued, err := harness.dispatch.Enqueue(dispatcher, EnqueueRequest{
		ID: []byte("dispatched"), IdempotencyKey: []byte("idempotency-dispatched"),
		AgentID: "agent", AgentGeneration: 1,
		Capability: "model", Action: "complete",
		SourceID: []byte("source"), PolicyID: []byte("policy"),
		ObjectID: "object",
		Input:    json.RawMessage(`{"prompt_digest":"abc"}`),
		Context:  dispatchContext(harness.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := harness.dispatch.Claim(dispatcher, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		ClaimID: []byte("token-dispatched"), Lease: time.Minute,
		Context: dispatchContext(harness.now, "request"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if admitted.EffectPossible != claimed.EffectPossible {
		t.Fatalf("EffectPossible: admission %v, dispatch claim %v",
			admitted.EffectPossible, claimed.EffectPossible)
	}
	// The execution fingerprint is deliberately not compared. It digests the
	// decision, and the two paths cannot run under the same one: Enqueue
	// requires dispatch authority and an admission requires only invoke. Every
	// other part of the claim state is a function of the action and the lease,
	// so it must match exactly.
	if admitted.State != claimed.State ||
		admitted.ClaimFence != claimed.ClaimFence ||
		admitted.ClaimLease != claimed.ClaimLease ||
		!admitted.ClaimLeaseUntil.Equal(claimed.ClaimLeaseUntil) ||
		admitted.ExecutionPolicyGeneration != claimed.ExecutionPolicyGeneration ||
		!admitted.ExecutionExpiresAt.Equal(claimed.ExecutionExpiresAt) {
		t.Fatalf("claim state diverged:\n admission %#v\n dispatch  %#v",
			admitted, claimed)
	}
}

// TestAdmissionDenialNamesNothing pins the disclosure rule on every refusal
// this surface can produce.
func TestAdmissionDenialNamesNothing(t *testing.T) {
	forbidden := []string{
		"policy", "source", "compartment", "domain", "doc-", "owner", "agent",
	}
	harness := newAdmissionHarness(t, nil)
	grant, err := harness.service.Request(
		harness.context(t, "request"), harness.request(
			"request", "admission", "summarize",
			Effects{EffectEgressesContent}, nil))
	if err != nil {
		t.Fatalf("denial = %v", err)
	}
	if grant.Outcome != AdmissionDenied {
		t.Fatalf("outcome = %q", grant.Outcome)
	}
	for _, err := range []error{ErrAdmissionSpent, ErrAdmissionConflict} {
		for _, term := range forbidden {
			if strings.Contains(err.Error(), term) {
				t.Fatalf("error %q names %q", err, term)
			}
		}
	}
}
