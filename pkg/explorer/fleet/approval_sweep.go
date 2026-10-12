// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The approval sweep (#655).
//
// A held request is moved on by the calls of the two parties to it: the
// requester's re-request, an approver's decision, and either party's Status.
// A request neither party ever touches again stays in its stored state, and
// before #655 nothing could move a request whose target or policy had moved,
// or whose action could never be written, at all. The sweep writes down what
// such a request has become: expired once its window has closed, stranded
// for one of the permanent causes approvalStrandable names. It never decides,
// never re-enables and never materializes anything.
//
// The sweep is neither party, so it acts as neither. It acts as the host's own
// system observer, ApprovalSweeperSubject, in a decision the host mints for
// one approval's scope at a time and binds with its own binder: subject and
// actor ApprovalSweeperSubject, OperationApprovalSweep and nothing else, on
// exactly the request's source and policy. No authenticator mints that
// operation and no service role allows it, so nothing outside the host
// process can present the observer. Each transition it writes is audited and
// published as that observer, through #653's per-transition outbox, and the
// event row says so (ApprovalObservedBySweeper). Nobody is impersonated: the
// observer is recorded as what it is.
//
// Sweeps on several replicas may run at once. Every write is a
// compare-and-set from the version the sweep read, so one writer wins, and a
// loser counts the race and moves on; the next pass reads the record final
// and leaves it.

// ApprovalSweeperSubject is the system observer's identity: the subject and
// actor of every decision a sweep acts under, and of every transition it
// writes. No authenticator names a principal this way.
const ApprovalSweeperSubject shoal.ID = "shoal-system:approval-sweeper"

// ApprovalSweeperClientID is the client the system observer is minted under,
// bound like its subject and actor.
const ApprovalSweeperClientID shoal.ID = "shoal-system:approval-sweeper"

// MaxApprovalSweepDecisionLifetime bounds how far ahead a bound sweep
// decision may expire. The host mints one per approval, for one write and
// its event; a longer one is not the observer's decision.
const MaxApprovalSweepDecisionLifetime = 2 * time.Minute

// ApprovalSweepScope is the one approval a sweep decision is minted for.
type ApprovalSweepScope struct {
	SourceID []byte
	PolicyID []byte
}

// ApprovalSweepBinder returns ctx carrying the host's system observer
// decision for one approval's scope: subject and actor
// ApprovalSweeperSubject, OperationApprovalSweep alone, on exactly that
// source and policy, under the policy generation in force, briefly. The
// service checks all of that before acting under it.
type ApprovalSweepBinder func(context.Context, ApprovalSweepScope) (context.Context, error)

// ApprovalSweepRequest is one page of the approval key space.
type ApprovalSweepRequest struct {
	After []byte
	Limit int
}

// ApprovalSweepResult counts what one page of a sweep did. Next continues
// the scan; empty means the key space has been covered.
type ApprovalSweepResult struct {
	Examined int
	Expired  int
	Stranded int
	// Unpublished counts transitions that committed and whose event did
	// not publish; the next pass retries them (sweepOne).
	Unpublished int
	// Raced counts records another writer moved first.
	Raced int
	// Failed counts records the sweep could not judge or write. They are
	// left as they were.
	Failed int
	Next   []byte
}

// Sweep examines one page of approvals and records, as the system observer,
// every one that can no longer progress. It fails only for a request it
// cannot serve at all — no sweeper configured, a bad page bound, a scan that
// failed, or a cancelled context. A record it cannot judge or write is
// counted and left for the next pass.
func (s *ApprovalService) Sweep(
	ctx context.Context, request ApprovalSweepRequest,
) (ApprovalSweepResult, error) {
	if s.sweeper == nil {
		return ApprovalSweepResult{}, shoal.NewError(
			shoal.ErrorUnavailable, "the approval sweep is not configured")
	}
	if request.Limit <= 0 || request.Limit > MaxApprovalListResults {
		return ApprovalSweepResult{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "approval sweep limit is outside its bound")
	}
	page, err := s.store.ScanApprovals(ctx, request.After, request.Limit)
	if err != nil {
		return ApprovalSweepResult{}, err
	}
	result := ApprovalSweepResult{Next: append([]byte(nil), page.Next...)}
	for _, record := range page.Approvals {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.Examined++
		s.sweepOne(ctx, record, &result)
	}
	return result, nil
}

func (s *ApprovalService) sweepOne(
	ctx context.Context, record ApprovalRecord, result *ApprovalSweepResult,
) {
	now := s.dispatch.clock().UTC()
	if record.State.terminal() {
		// A transition this observer wrote whose event did not publish is
		// retried here, inside #653's window. Only rows the observer
		// published are touched; the drain skips every other row, and closes
		// a lapsed one, as any drain does.
		if s.events != nil &&
			(record.State == ApprovalExpired || record.State == ApprovalStranded) &&
			now.Before(record.UpdatedAt.Add(ApprovalEventRetryWindow)) {
			bound, _, err := s.sweepDecision(ctx, record, now)
			if err != nil {
				result.Failed++
				return
			}
			s.drainApprovalEventsQuietly(bound, record.ID)
		}
		return
	}
	state, cause, err := s.approvalStrandable(ctx, record, now)
	if err != nil {
		result.Failed++
		return
	}
	if state == "" {
		return
	}
	bound, decision, err := s.sweepDecision(ctx, record, now)
	if err != nil {
		result.Failed++
		return
	}
	_, err = s.settle(
		bound, auth.OperationApprovalSweep, decision, record, state, cause, now)
	switch {
	case err == nil, errors.Is(err, ErrApprovalEventUnpublished):
		if err != nil {
			result.Unpublished++
		}
		if state == ApprovalExpired {
			result.Expired++
		} else {
			result.Stranded++
		}
	case errors.Is(err, ErrApprovalConflict):
		result.Raced++
	default:
		result.Failed++
	}
}

// sweepDecision binds the system observer for one record and checks that what
// the host bound is exactly that observer: its identity and client, on
// nobody's behalf, holding the sweep and nothing else, on exactly this
// record's source and policy, expiring within
// MaxApprovalSweepDecisionLifetime. A host that bound anything broader is
// refused rather than trusted.
func (s *ApprovalService) sweepDecision(
	ctx context.Context, record ApprovalRecord, now time.Time,
) (context.Context, auth.Decision, error) {
	bound, err := s.sweeper(ctx, ApprovalSweepScope{
		SourceID: append([]byte(nil), record.Request.SourceID...),
		PolicyID: append([]byte(nil), record.Request.PolicyID...),
	})
	if err != nil {
		return nil, auth.Decision{}, err
	}
	decision, err := s.dispatch.resolver.Resolve(bound)
	if err != nil {
		return nil, auth.Decision{}, err
	}
	operations := decision.AllowedOperations()
	sources, policies := decision.PermittedSourceIDs(), decision.PermittedPolicyIDs()
	if decision.Subject() != ApprovalSweeperSubject ||
		decision.Actor() != ApprovalSweeperSubject ||
		decision.ClientID() != ApprovalSweeperClientID ||
		len(sources) != 1 || !bytes.Equal(sources[0], record.Request.SourceID) ||
		len(policies) != 1 || !bytes.Equal(policies[0], record.Request.PolicyID) ||
		decision.AuthenticationExpires().After(now.Add(MaxApprovalSweepDecisionLifetime)) ||
		len(decision.OnBehalfOf()) > 0 ||
		len(operations) != 1 || operations[0] != auth.OperationApprovalSweep {
		return nil, auth.Decision{}, shoal.NewError(
			shoal.ErrorUnauthorized,
			"the approval sweep is not bound to the system observer")
	}
	if err := decision.AuthorizeObject(
		auth.OperationApprovalSweep, auth.ResourceRequest{
			AuthorizationDomain: decision.AuthorizationDomain(),
			SourceID:            record.Request.SourceID,
			PolicyID:            record.Request.PolicyID,
			ObjectID:            shoal.ID(record.ID),
		}, now); err != nil {
		return nil, auth.Decision{}, err
	}
	return bound, decision, nil
}
