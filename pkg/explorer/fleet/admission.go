// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Admission answers the one question no other surface in Shoal answers: an
// out-of-process caller holding a side effect it has not yet performed asking
// whether it may perform it.
//
// Every other enforcement surface here adjudicates work Shoal itself does, and
// every other control *withholds* — it removes evidence from a response that is
// produced anyway. A caller about to post a prompt to a hosted model cannot
// withhold: it does not assemble the response, and by the time content exists
// the transmission has already happened. It needs a decision before the effect,
// which is what this is.
//
// It is deliberately not a new durable object. An admission is a dispatch
// action in the claimed state, and the token is its claim: the same store, the
// same fence, the same lifecycle events, the same audit phases, the same replay
// semantics. That is not an economy — it is the same situation. A remote worker
// that has claimed an action and a proxy that has been admitted are both
// out-of-process actors holding permission Shoal granted and has not yet seen
// the outcome of, and inventing a second record for the second case would have
// given the two different durability, different reconciliation, and a second
// place for the two to disagree.
//
// Nothing here enforces that a caller honours what it is told. It cannot: no
// plane outside the caller's own process can. This is a declaration seam of the
// same kind as the execution boundary, and a caller that ignores an obligation
// is misreporting its own configuration exactly as a host that binds an
// external-effect executor under an evidence-only ceiling is.
var (
	// ErrAdmissionSpent reports a token that no longer names a live admission:
	// already reported, denied, cancelled, expired, or never issued. The cases
	// are deliberately one error. Telling a caller which one it hit would say
	// whether an admission it does not hold exists.
	ErrAdmissionSpent = errors.New("fleet admission: token is spent")
	// ErrAdmissionConflict reports an admission identity already granted to a
	// different token.
	ErrAdmissionConflict = errors.New("fleet admission: admission conflict")
)

// MaxAdmissionDisclosures bounds the corpus references one admission may
// declare. It is the evidence bound, not a new one: an admission declares what
// a single call would carry, and a call carrying more anchors than an action
// may record is not a call this plane can reason about.
const MaxAdmissionDisclosures = MaxActionEvidence

// AdmissionOutcome is what the caller is told. There are three, and the middle
// one is the one that matters: refusing a whole call is blunt, and "you may
// proceed without these" is a decision a caller can comply with instead of
// being stopped.
type AdmissionOutcome string

const (
	// AdmissionDenied means the call must not happen.
	AdmissionDenied AdmissionOutcome = "denied"
	// AdmissionAllowed means it may happen as declared.
	AdmissionAllowed AdmissionOutcome = "allowed"
	// AdmissionObligated means it may happen only with the obligations
	// satisfied.
	AdmissionObligated AdmissionOutcome = "allowed_with_obligations"
)

// AdmissionToken is what a caller returns on the matching report.
//
// It carries no authority of its own. Every field is re-checked against the
// durable action before a report is accepted — the principal through
// authorizedCurrent, the claim through the fence, the version through the
// store's compare-and-set — so a forged or edited token buys a caller nothing
// it could not already do with its own credentials. That is why it is a plain
// structure rather than a sealed blob: a token whose bytes were trusted would
// have to be unforgeable, and one whose bytes are never trusted does not.
type AdmissionToken struct {
	ActionID  []byte    `json:"action_id"`
	TokenID   []byte    `json:"token_id"`
	Version   uint64    `json:"version"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (t AdmissionToken) validate() error {
	if err := validateOpaque("admission action ID", t.ActionID, false); err != nil {
		return err
	}
	if err := validateOpaque("admission token ID", t.TokenID, false); err != nil {
		return err
	}
	if t.Version == 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "admission token version is invalid")
	}
	return nil
}

// Obligations is what an admitted caller must do before it performs the call.
//
// Withhold names only references the caller supplied in its own request, so the
// response discloses nothing the caller did not already know. Nothing here says
// *why* a reference must be withheld, and that is the point: separating "you
// are not authorized for this" from "your co-occurrence budget is spent" would
// turn admission into an authorization oracle over document identities, where a
// caller learns which of a guessed set it may read by reading the reason. The
// authorized read path reports those two classes apart because there the counts
// describe a corpus the caller is already reading; here they would describe
// identities the caller merely named.
type Obligations struct {
	// Withhold is the subset of the request's declared references that must not
	// appear in what the caller sends.
	Withhold []shoal.ID
}

func (o Obligations) empty() bool { return len(o.Withhold) == 0 }

// AdmissionRequest is a caller stating what it is about to do.
//
// Input is the declaration, not the payload. Shoal never receives the prompt:
// it would be a copy of exactly the content the caller is asking permission to
// transmit, held by the plane whose job is to decide whether transmitting it is
// allowed. What the declaration must contain is the action's registered input
// schema, so a host decides what an admission has to say for itself.
type AdmissionRequest struct {
	ID              []byte
	IdempotencyKey  []byte
	TokenID         []byte
	AgentID         shoal.ID
	AgentGeneration int64
	Capability      string
	Action          string
	SourceID        []byte
	PolicyID        []byte
	ObjectID        shoal.ID
	// Effects is what this call would do. It must be non-empty: an admission
	// that declares nothing is unclassifiable, and an unclassifiable input is
	// treated as the dangerous case everywhere else in this package.
	Effects Effects
	Input   json.RawMessage
	// Disclosures are the corpus references the caller says its payload would
	// carry. They are what obligations are computed over and expressed in.
	Disclosures []shoal.ID
	Lease       time.Duration
	Context     RequestContext
}

// AdmissionGrant is the answer.
//
// A denial carries no reason. Naming the control that refused would name a
// policy, and a caller that can enumerate denials by varying its request has
// read the policy out of the decision plane one bit at a time. A caller that
// needs to distinguish "Shoal refused" from "Shoal was unreachable" gets that
// from the transport: a refusal is a successful response carrying
// AdmissionDenied, an unreachable plane is a transport failure, and the two are
// never confusable.
type AdmissionGrant struct {
	Outcome     AdmissionOutcome
	Token       AdmissionToken
	Obligations Obligations
}

// AdmissionReport is the caller coming back with what actually happened.
//
// Without it admission is a stateless gate: the plane would grant permission
// and never learn whether the call occurred, what it cost, or whether the
// obligations were satisfiable. The report is the edge that makes this a loop,
// and it is the input a cross-call accumulator would charge. Nothing here
// accumulates; the record is durable and an accumulator attaches to the same
// lifecycle events every other dispatch transition publishes.
type AdmissionReport struct {
	Token AdmissionToken
	// Outcome is what happened, shaped by the action's registered output
	// schema.
	Outcome json.RawMessage
	// Failed reports that the call did not happen or did not succeed. ErrorCode
	// carries the reason and is required when Failed is set.
	Failed    bool
	ErrorCode string
	Context   RequestContext
}

// OutstandingAdmission is an admission that was granted and never reported.
//
// It is the observable form of the gap the report closes. A caller that is
// admitted and then goes silent has been told it may perform an effect, and the
// record must not read as though nothing was granted — otherwise a crashed
// proxy and a proxy that never asked are indistinguishable.
type OutstandingAdmission struct {
	ActionID   []byte
	TokenID    []byte
	Version    uint64
	AdmittedAt time.Time
	ExpiresAt  time.Time
	// Expired reports that the token can no longer be reported against. Such an
	// admission is not resolved, it is abandoned: whether the effect happened is
	// unknown and stays unknown.
	Expired bool
}

type OutstandingAdmissionsRequest struct {
	After   []byte
	Limit   int
	Context RequestContext
}

type OutstandingAdmissionsPage struct {
	Admissions []OutstandingAdmission
	Next       []byte
}

// DisclosureRestrictor narrows a caller-declared reference set to what that
// caller may still disclose, and is how an existing withholding control becomes
// an obligation instead of a refusal.
//
// The intended implementation is the authorized client's co-occurrence budget,
// which already computes exactly this and today can only express it by dropping
// documents out of a result it is assembling. A caller that assembles its own
// payload cannot be served that way; it has to be told.
//
// Returning a reference does not admit it: the result is intersected with what
// authorization already permitted, so an implementation can only ever narrow.
// An error is not an empty result — it propagates, because a restrictor that
// cannot answer has not said "withhold nothing".
type DisclosureRestrictor interface {
	RestrictDisclosure(context.Context, []shoal.ID) ([]shoal.ID, error)
}

type AdmissionConfig struct {
	Dispatch *DispatchService
	// Restrictor is optional. Absent, obligations come from authorization alone
	// — which matches the co-occurrence budget's own default, where a zero
	// bound disables the control. A deployment that enables the budget and does
	// not wire it here gets obligations narrower than the control it
	// configured, so the wiring is a deployment invariant rather than something
	// this service can check.
	Restrictor DisclosureRestrictor
}

type AdmissionService struct {
	dispatch   *DispatchService
	restrictor DisclosureRestrictor
}

func NewAdmissionService(config AdmissionConfig) (*AdmissionService, error) {
	if config.Dispatch == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "fleet admission dependencies are required")
	}
	return &AdmissionService{
		dispatch: config.Dispatch, restrictor: config.Restrictor,
	}, nil
}

// Request adjudicates a call that has not happened yet.
//
// The durable record is written before the answer is returned, in both
// directions. A granted admission is a claimed action; a denied one is a
// cancelled action. Answering without writing would leave the plane unable to
// say afterwards what it permitted, which is the whole of the audit
// requirement: this is the one surface where the work Shoal is accountable for
// is work Shoal did not perform and cannot observe.
func (s *AdmissionService) Request(
	ctx context.Context, request AdmissionRequest,
) (AdmissionGrant, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := dispatch.begin(ctx, auth.OperationInvoke, request.Context)
	if err != nil {
		return AdmissionGrant{}, err
	}
	if err := validateOpaque("admission token ID", request.TokenID, false); err != nil {
		return AdmissionGrant{}, err
	}
	if request.Lease <= 0 || request.Lease > MaxActionClaimTTL {
		return AdmissionGrant{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "admission lease is outside its bound")
	}
	// An empty declared set is refused rather than read as "nothing". The set is
	// the whole of what an admission is admitting, so an admission that declares
	// nothing would be the cheapest possible request and the most permissive
	// answer.
	if len(request.Effects) == 0 {
		return AdmissionGrant{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "admission must declare an effect")
	}
	declared, err := canonicalEffects(request.Effects)
	if err != nil {
		return AdmissionGrant{}, err
	}
	disclosures, err := canonicalDisclosures(request.Disclosures)
	if err != nil {
		return AdmissionGrant{}, err
	}
	queued, err := dispatch.enqueue(ctx, EnqueueRequest{
		ID: request.ID, IdempotencyKey: request.IdempotencyKey,
		AgentID: request.AgentID, AgentGeneration: request.AgentGeneration,
		Capability: request.Capability, Action: request.Action,
		SourceID: request.SourceID, PolicyID: request.PolicyID,
		ObjectID: request.ObjectID, Input: request.Input,
		Context: request.Context,
	}, auth.OperationInvoke)
	if err != nil {
		return AdmissionGrant{}, err
	}
	// A replayed request whose first attempt was refused answers from the record
	// rather than re-adjudicating. Re-running the decision would let a caller
	// retry a denial until the state it depended on moved, and the cancelled
	// record is the durable statement that this admission was refused.
	if queued.State == DispatchCanceled {
		return AdmissionGrant{Outcome: AdmissionDenied}, nil
	}
	if queued.State.terminal() {
		return AdmissionGrant{}, ErrActionTerminal
	}
	// The declaration is checked against what the descriptor permits, resolved
	// under this decision. The ceiling the executor was bound to is already
	// enforced inside resolveActionBinding, so an action can neither declare
	// more than its executor may do nor be admitted for more than it declares.
	_, action, _, err := dispatch.registry.resolveActionBinding(
		ctx, decision, queued.AgentID, queued.AgentGeneration,
		queued.Capability, queued.Action, queued.SourceID, queued.PolicyID,
		queued.ObjectID, auth.OperationInvoke, now,
	)
	if err != nil {
		return AdmissionGrant{}, err
	}
	if declared.exceeds(action.Effects) {
		return s.deny(ctx, request, queued)
	}
	obligations, err := s.obligations(ctx, decision, request, disclosures, now)
	if err != nil {
		return AdmissionGrant{}, err
	}
	if queued.State == DispatchClaimed {
		// A replayed request whose first attempt was granted. Claim recognises
		// its own replay only from the queued version, which this record is
		// past, so the live grant is returned directly. A live claim under a
		// different token is someone else's admission for the same identity and
		// must not be handed over.
		if !bytes.Equal(queued.ClaimID, request.TokenID) ||
			!now.Before(queued.ClaimLeaseUntil) {
			return AdmissionGrant{}, ErrAdmissionConflict
		}
		return grantFor(queued, obligations), nil
	}
	granted, err := dispatch.Claim(ctx, ClaimRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		ClaimID: request.TokenID, Lease: request.Lease,
		Context: request.Context,
	})
	if err != nil {
		return AdmissionGrant{}, err
	}
	return grantFor(granted, obligations), nil
}

// deny records the refusal durably before returning it.
//
// It cancels rather than leaving the request queued. A queued admission is
// indistinguishable from work waiting for a worker, and an operator reading the
// record later would see a backlog where there were refusals.
//
// The mutation key is the tuple digest over the admission's own identity — the
// same construction the executor key uses, reused rather than reinvented — so a
// retried request replays on to the same cancelled record instead of
// conflicting with its own first refusal.
func (s *AdmissionService) deny(
	ctx context.Context, request AdmissionRequest, queued ActionRecord,
) (AdmissionGrant, error) {
	if _, err := s.dispatch.cancel(ctx, CancelRequest{
		ID: queued.ID, ExpectedVersion: queued.Version,
		MutationKey: executorKey(queued.ID, []byte("shoal.fleet.admission-denied.v1")),
		Context:     request.Context,
	}, auth.OperationInvoke); err != nil {
		return AdmissionGrant{}, err
	}
	return AdmissionGrant{Outcome: AdmissionDenied}, nil
}

func grantFor(record ActionRecord, obligations Obligations) AdmissionGrant {
	outcome := AdmissionAllowed
	if !obligations.empty() {
		outcome = AdmissionObligated
	}
	return AdmissionGrant{
		Outcome: outcome, Obligations: obligations,
		Token: AdmissionToken{
			ActionID: append([]byte(nil), record.ID...),
			TokenID:  append([]byte(nil), record.ClaimID...),
			Version:  record.Version, ExpiresAt: record.ClaimLeaseUntil,
		},
	}
}

// obligations computes what the caller must withhold from its payload.
//
// Both stages narrow and neither widens. Authorization runs first, over the
// same object-scoped check the authorized read path applies before a document
// enters a candidate set, and the restrictor then narrows what survived. A
// reference the restrictor returns that authorization already withheld stays
// withheld, so a misbehaving restrictor cannot admit anything.
//
// The first stage is honestly a request-level gate, not a per-reference one. An
// admission names one source and one policy, and only the object identity
// varies across references, so every reference in a request gets the same
// answer: either this caller may retrieve within this scope or it may not.
// Per-reference authorization is the registration rule attached to each
// document, which is exactly what the restrictor resolves. That is why a
// deployment without a restrictor gets weaker obligations than one with it, and
// why the wiring is a deployment invariant this service cannot check.
func (s *AdmissionService) obligations(
	ctx context.Context,
	decision auth.Decision,
	request AdmissionRequest,
	disclosures []shoal.ID,
	now time.Time,
) (Obligations, error) {
	if len(disclosures) == 0 {
		return Obligations{}, nil
	}
	order := make([]shoal.ID, 0, len(disclosures))
	permitted := make(map[shoal.ID]struct{}, len(disclosures))
	for _, reference := range disclosures {
		if err := decision.AuthorizeObject(auth.OperationRetrieve, auth.ResourceRequest{
			AuthorizationDomain: decision.AuthorizationDomain(),
			SourceID:            request.SourceID,
			PolicyID:            request.PolicyID,
			ObjectID:            reference,
		}, now); err != nil {
			continue
		}
		permitted[reference] = struct{}{}
		order = append(order, reference)
	}
	admitted := permitted
	if s.restrictor != nil {
		allowed, err := s.restrictor.RestrictDisclosure(ctx, order)
		if err != nil {
			return Obligations{}, err
		}
		// Rebuilt by intersection rather than by removing entries from what
		// authorization decided. A restrictor is a narrowing control and this
		// is what holds it to that: subtracting from a withheld set would let a
		// restrictor's answer re-admit a reference authorization had already
		// refused, and a restrictor is not an authorization decision.
		narrowed := make(map[shoal.ID]struct{}, len(allowed))
		for _, reference := range allowed {
			if _, ok := permitted[reference]; ok {
				narrowed[reference] = struct{}{}
			}
		}
		admitted = narrowed
	}
	// Emitted in the canonical order of the declared set rather than map order,
	// so the same request produces the same answer and a caller comparing two
	// responses is comparing decisions rather than iteration.
	var result Obligations
	for _, reference := range disclosures {
		if _, ok := admitted[reference]; !ok {
			result.Withhold = append(result.Withhold, reference)
		}
	}
	return result, nil
}

// Report closes the loop for one admission.
//
// A token is one-shot. The dispatch completion path treats a terminal action at
// the reported version under the same claim as the reporter's own lost response
// and replays it, which is right for a worker retrying a delivery and wrong for
// an admission: it would let a caller report one outcome, then report a
// different one against the same token and be told the second was recorded. So
// the terminal case is resolved here first, and only a byte-identical replay of
// what is already committed is accepted. Anything else is spent.
func (s *AdmissionService) Report(
	ctx context.Context, report AdmissionReport,
) (ActionRecord, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, report.Context)
	defer cancel()
	decision, now, err := dispatch.begin(ctx, auth.OperationInvoke, report.Context)
	if err != nil {
		return ActionRecord{}, err
	}
	if err := report.Token.validate(); err != nil {
		return ActionRecord{}, err
	}
	if report.Failed && report.ErrorCode == "" {
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"a failed admission report requires an error code")
	}
	// A report is either an outcome or a failure. Allowing both would leave the
	// committed record's shape depending on which the completion path resolved
	// first, and the replay comparison below could then accept a report that
	// differed from what was stored.
	if !report.Failed && report.ErrorCode != "" {
		return ActionRecord{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"a successful admission report carries no error code")
	}
	current, err := dispatch.authorizedCurrent(
		ctx, decision, report.Token.ActionID, auth.OperationInvoke, now)
	if err != nil {
		// An admission that does not exist and an admission belonging to
		// someone else must be the same answer. They are not by default: the
		// store reports absence with its own sentinel, while authorizedCurrent
		// reports a principal mismatch with the object-not-found shape, and the
		// two reach a caller as different messages. A caller holding neither
		// could then tell which admission identities are in use by watching
		// which refusal comes back, one guess at a time.
		if errors.Is(err, ErrActionNotFound) {
			return ActionRecord{}, auth.ObjectNotFound()
		}
		return ActionRecord{}, err
	}
	// Resolved for the OutputSchema the report is validated against.
	// authorizedCurrent has already resolved and authorized this action and
	// discarded what it resolved; this is how the declared schema is obtained,
	// not a second authorization.
	_, action, _, err := dispatch.registry.resolveActionBinding(
		ctx, decision, current.AgentID, current.AgentGeneration,
		current.Capability, current.Action, current.SourceID, current.PolicyID,
		current.ObjectID, auth.OperationInvoke, now,
	)
	if err != nil {
		return ActionRecord{}, err
	}
	output, outputErr := reportedOutput(action, report)
	if current.State.terminal() {
		if outputErr != nil || !sameReportedOutcome(current, report, output) {
			return ActionRecord{}, ErrAdmissionSpent
		}
		if err := dispatch.publishTransition(
			context.WithoutCancel(ctx), actionEventKind(current), current,
		); err != nil {
			return ActionRecord{}, errors.Join(ErrActionCommitted, err)
		}
		return cloneActionRecord(current), nil
	}
	record, err := dispatch.CompleteClaim(ctx, CompletionRequest{
		ID: report.Token.ActionID, ExpectedVersion: report.Token.Version,
		ClaimID: report.Token.TokenID, Failed: report.Failed,
		Result:  ExecutionResult{Output: report.Outcome, ErrorCode: report.ErrorCode},
		Context: report.Context,
	})
	if errors.Is(err, ErrClaimLost) {
		return ActionRecord{}, ErrAdmissionSpent
	}
	return record, err
}

// reportedOutput returns the canonical stored form of a report's outcome, so a
// replay can be compared against what was committed rather than against the
// bytes the caller happened to send this time.
func reportedOutput(
	action Action, report AdmissionReport,
) (json.RawMessage, error) {
	if report.Failed {
		return nil, nil
	}
	return validateAgainstSchema(
		action.OutputSchema, report.Outcome,
		"admission outcome", MaxActionOutputBytes)
}

// sameReportedOutcome reports whether a terminal record is this exact report,
// already committed.
//
// Every clause narrows. A record that is terminal for any other reason —
// cancelled, or completed under a different claim, or at a version this token
// never named — is a token that has been spent on something else, and the
// caller is told nothing about which.
func sameReportedOutcome(
	current ActionRecord, report AdmissionReport, output json.RawMessage,
) bool {
	if current.Version != report.Token.Version+1 ||
		!bytes.Equal(current.ClaimID, report.Token.TokenID) {
		return false
	}
	if report.Failed {
		return current.State == DispatchFailed &&
			current.ErrorCode == report.ErrorCode && len(current.Output) == 0
	}
	return current.State == DispatchSucceeded &&
		current.ErrorCode == "" && bytes.Equal(current.Output, output)
}

// Outstanding lists admissions this caller was granted and has not reported.
//
// Claimed is the whole filter. An admission that was granted and never reported
// is a claimed action, and it stays claimed whether its lease is live or long
// gone — the record never quietly becomes something else. Expired says the
// token can no longer be reported against, which is not a resolution: it is the
// point at which Shoal permitted an effect and will never learn what happened.
func (s *AdmissionService) Outstanding(
	ctx context.Context, request OutstandingAdmissionsRequest,
) (OutstandingAdmissionsPage, error) {
	dispatch := s.dispatch
	ctx, cancel := dispatch.deadline(ctx, request.Context)
	defer cancel()
	decision, now, err := dispatch.begin(ctx, auth.OperationInvoke, request.Context)
	if err != nil {
		return OutstandingAdmissionsPage{}, err
	}
	if request.Limit <= 0 || request.Limit > MaxDispatchListResults {
		return OutstandingAdmissionsPage{}, shoal.NewError(
			shoal.ErrorInvalidArgument,
			"outstanding admission limit is outside its bound")
	}
	page, err := dispatch.store.ScanActions(ctx, request.After, request.Limit)
	if err != nil {
		return OutstandingAdmissionsPage{}, err
	}
	result := OutstandingAdmissionsPage{Next: append([]byte(nil), page.Next...)}
	for _, record := range page.Actions {
		if record.State != DispatchClaimed {
			continue
		}
		if !sameActionPrincipal(decision, record) {
			continue
		}
		// Binding, not execution: the caller whose admission this is runs out of
		// process by construction, so demanding a runnable executor would hide
		// every admission from the only principal entitled to see it.
		if _, _, _, authorizeErr := dispatch.registry.resolveActionBinding(
			ctx, decision, record.AgentID, record.AgentGeneration,
			record.Capability, record.Action, record.SourceID, record.PolicyID,
			record.ObjectID, auth.OperationInvoke, now,
		); authorizeErr != nil {
			if shoal.IsErrorCode(authorizeErr, shoal.ErrorUnauthorized) ||
				shoal.IsErrorCode(authorizeErr, shoal.ErrorNotFound) {
				continue
			}
			return OutstandingAdmissionsPage{}, authorizeErr
		}
		result.Admissions = append(result.Admissions, OutstandingAdmission{
			ActionID: append([]byte(nil), record.ID...),
			TokenID:  append([]byte(nil), record.ClaimID...),
			Version:  record.Version, AdmittedAt: record.UpdatedAt,
			ExpiresAt: record.ClaimLeaseUntil,
			Expired:   !now.Before(record.ClaimLeaseUntil),
		})
	}
	return result, nil
}

// canonicalDisclosures sorts and deduplicates a declared reference set.
//
// Canonical here is what makes an obligation reproducible: the same references
// declared in a different order are the same declaration, and a caller
// comparing two answers must not see a difference that came from its own
// iteration order.
func canonicalDisclosures(values []shoal.ID) ([]shoal.ID, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > MaxAdmissionDisclosures {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "admission disclosures exceed their bound")
	}
	result := make([]shoal.ID, 0, len(values))
	for _, value := range values {
		if err := shoal.ValidateRequiredID("admission disclosure", value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	sort.Slice(result, func(i, j int) bool {
		return shoal.CompareID(result[i], result[j]) < 0
	})
	deduplicated := result[:0]
	for _, value := range result {
		if len(deduplicated) == 0 ||
			deduplicated[len(deduplicated)-1] != value {
			deduplicated = append(deduplicated, value)
		}
	}
	return deduplicated, nil
}
