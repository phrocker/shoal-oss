// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The approval sweep's system observer (#655).
//
// fleet.ApprovalService.Sweep records, as neither party, what a held request
// nobody will touch again has become. It acts under a decision this host mints
// for one approval at a time and binds with its own binder, exactly as the
// HTTP transport binds a request's decision:
//
//   - subject and actor fleet.ApprovalSweeperSubject, client this command;
//   - auth.OperationApprovalSweep and nothing else;
//   - exactly the approval's source and policy;
//   - the policy generation in force, and a one-minute expiry;
//   - a request and correlation ID of its own, so each write's audit and its
//     event's publication audit name the sweep that made it.
//
// This file is the one place in the command that denotes the operation
// (approval_sweep_grant_test.go holds that), no authenticator mints it, and
// the decision never leaves this process: nothing outside the host can
// present the observer.
type approvalSweepMinter struct {
	binder      auth.Binder
	generations auth.GenerationReader
	clock       func() time.Time
	instance    string
	sequence    atomic.Uint64
}

// approvalSweepClientID is the client the system observer is minted under.
const approvalSweepClientID shoal.ID = "shoal-explore-web"

// approvalSweepDecisionTTL bounds one minted decision. A row's write and its
// event's publication run well inside it.
const approvalSweepDecisionTTL = time.Minute

func newApprovalSweepMinter(
	binder auth.Binder, generations auth.GenerationReader, clock func() time.Time,
) (*approvalSweepMinter, error) {
	if binder == nil || generations == nil || clock == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "approval sweep dependencies are required")
	}
	var instance [8]byte
	if _, err := rand.Read(instance[:]); err != nil {
		return nil, err
	}
	return &approvalSweepMinter{
		binder: binder, generations: generations, clock: clock,
		instance: hex.EncodeToString(instance[:]),
	}, nil
}

// bind is the fleet.ApprovalSweepBinder.
func (m *approvalSweepMinter) bind(
	ctx context.Context, scope fleet.ApprovalSweepScope,
) (context.Context, error) {
	generation, err := m.generations.CurrentPolicyGeneration(
		ctx, workspaceAuthorizationDomain)
	if err != nil {
		return nil, err
	}
	id := shoal.ID(fmt.Sprintf(
		"approval-sweep-%s-%d", m.instance, m.sequence.Add(1)))
	decision, err := auth.NewDecision(auth.DecisionConfig{
		Subject: fleet.ApprovalSweeperSubject, Actor: fleet.ApprovalSweeperSubject,
		ClientID:              approvalSweepClientID,
		AuthorizationDomain:   workspaceAuthorizationDomain,
		AllowedOperations:     []auth.Operation{auth.OperationApprovalSweep},
		PermittedSourceIDs:    [][]byte{scope.SourceID},
		PermittedPolicyIDs:    [][]byte{scope.PolicyID},
		PolicyGeneration:      generation,
		AuthenticationExpires: m.clock().UTC().Add(approvalSweepDecisionTTL),
		RequestID:             id, CorrelationID: id,
	})
	if err != nil {
		return nil, err
	}
	return m.binder.Bind(ctx, decision)
}

// maxApprovalSweepPages bounds one pass, so a store whose continuation never
// ends cannot keep a pass running forever. At fleet.MaxApprovalListResults a
// page, that is a million approvals.
const maxApprovalSweepPages = 1 << 14

// sweepApprovals runs one pass over the whole approval key space and returns
// its totals. A page that fails ends the pass; the next pass starts again.
func sweepApprovals(
	ctx context.Context, service *fleet.ApprovalService,
) (fleet.ApprovalSweepResult, error) {
	var total fleet.ApprovalSweepResult
	var after []byte
	for page := 0; page < maxApprovalSweepPages; page++ {
		result, err := service.Sweep(ctx, fleet.ApprovalSweepRequest{
			After: after, Limit: fleet.MaxApprovalListResults,
		})
		total.Examined += result.Examined
		total.Expired += result.Expired
		total.Stranded += result.Stranded
		total.Unpublished += result.Unpublished
		total.Raced += result.Raced
		total.Failed += result.Failed
		if err != nil {
			return total, err
		}
		if len(result.Next) == 0 {
			return total, nil
		}
		after = result.Next
	}
	return total, nil
}

// startApprovalSweeps runs a pass every interval until ctx ends, and returns
// a function that stops it and waits for a pass in flight to finish. Each pass
// is bounded by the interval. Only a pass that recorded something or failed is
// reported.
func startApprovalSweeps(
	ctx context.Context, service *fleet.ApprovalService,
	interval time.Duration, output io.Writer,
) func() {
	ctx, cancel := context.WithCancel(ctx)
	var done sync.WaitGroup
	done.Add(1)
	go func() {
		defer done.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			pass, cancelPass := context.WithTimeout(ctx, interval)
			total, err := sweepApprovals(pass, service)
			cancelPass()
			if err != nil && ctx.Err() == nil {
				fmt.Fprintf(output, "approval sweep pass failed after %d "+
					"approval(s): %v\n", total.Examined, err)
				continue
			}
			if total.Expired+total.Stranded+total.Failed+total.Unpublished > 0 {
				fmt.Fprintf(output, "approval sweep: examined %d, expired %d, "+
					"stranded %d, event unpublished %d, raced %d, failed %d\n",
					total.Examined, total.Expired, total.Stranded,
					total.Unpublished, total.Raced, total.Failed)
			}
		}
	}()
	return func() {
		cancel()
		done.Wait()
	}
}
