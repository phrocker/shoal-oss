// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package webapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type FleetDispatchProvider interface {
	Enqueue(context.Context, fleet.EnqueueRequest) (fleet.ActionRecord, error)
	Claim(context.Context, fleet.ClaimRequest) (fleet.ActionRecord, error)
	Cancel(context.Context, fleet.CancelRequest) (fleet.ActionRecord, error)
	Status(context.Context, fleet.StatusRequest) (fleet.ActionRecord, error)
	Pull(context.Context, fleet.PullActionsRequest) (fleet.ActionPage, error)
	Invoke(context.Context, fleet.InvokeRequest) (fleet.ActionRecord, error)
	CompleteClaim(context.Context, fleet.CompletionRequest) (fleet.ActionRecord, error)
}

// NewFleetDispatchHandler returns the fleet dispatch HTTP surface without
// adding another authentication boundary. Mount it through
// Handler.MountAuthenticated at /api/v1/fleet/.
func NewFleetDispatchHandler(provider FleetDispatchProvider) (http.Handler, error) {
	if provider == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "fleet dispatch provider is required")
	}
	mux := http.NewServeMux()
	mountFleetDispatch(mux, provider)
	return mux, nil
}

// MountFleetDispatch is retained for dispatch-only tests and compatibility.
// Hosted startup must instead mount NewFleetHandler once at FleetRoutePrefix.
func (h *Handler) MountFleetDispatch(provider FleetDispatchProvider) error {
	if h == nil || provider == nil {
		return shoal.NewError(shoal.ErrorInvalidArgument, "fleet dispatch provider is required")
	}
	if isAbsentInterface(h.authenticator) || isAbsentInterface(h.binder) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "fleet dispatch requires authenticated transport")
	}
	mountFleetDispatch(h.mux, provider)
	return nil
}

func mountFleetDispatch(mux *http.ServeMux, provider FleetDispatchProvider) {
	mux.HandleFunc("POST /api/v1/fleet/actions", func(w http.ResponseWriter, r *http.Request) {
		var wire fleetEnqueueWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		request, err := wire.decode(nil)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		result, err := provider.Enqueue(r.Context(), request)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		writeResponse(w, http.StatusCreated, encodeFleetAction(result))
	})
	mux.HandleFunc("POST /api/v1/fleet/actions/invoke", func(w http.ResponseWriter, r *http.Request) {
		var wire fleetInvokeWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		enqueue, err := wire.Action.decode(nil)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		claimID, err := decodeWireBytes("claim ID", wire.ClaimID, false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		result, err := provider.Invoke(r.Context(), fleet.InvokeRequest{Enqueue: enqueue, ClaimID: claimID, Lease: wire.Lease})
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		writeResponse(w, http.StatusOK, encodeFleetAction(result))
	})
	mux.HandleFunc("POST /api/v1/fleet/actions/pull", func(w http.ResponseWriter, r *http.Request) {
		var wire fleetPullWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		contextValue, err := wire.Context.decode()
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		after, err := decodeWireBytes("action cursor", wire.After, true)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		page, err := provider.Pull(r.Context(), fleet.PullActionsRequest{After: after, Limit: wire.Limit, Context: contextValue})
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		actions := make([]fleetActionWire, len(page.Actions))
		for i := range page.Actions {
			actions[i] = encodeFleetAction(page.Actions[i])
		}
		writeResponse(w, http.StatusOK, struct {
			Actions []fleetActionWire `json:"actions"`
			Next    string            `json:"next,omitempty"`
		}{Actions: actions, Next: base64.RawURLEncoding.EncodeToString(page.Next)})
	})
	mux.HandleFunc("POST /api/v1/fleet/actions/{action}/claim", func(w http.ResponseWriter, r *http.Request) {
		actionID, err := decodeWireBytes("action ID", r.PathValue("action"), false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		var wire fleetClaimWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		contextValue, err := wire.Context.decode()
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		claimID, err := decodeWireBytes("claim ID", wire.ClaimID, false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		result, err := provider.Claim(r.Context(), fleet.ClaimRequest{ID: actionID, ExpectedVersion: wire.ExpectedVersion, ClaimID: claimID, Lease: wire.Lease, Context: contextValue})
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		writeResponse(w, http.StatusOK, encodeFleetAction(result))
	})
	mux.HandleFunc("POST /api/v1/fleet/actions/{action}/complete", func(w http.ResponseWriter, r *http.Request) {
		actionID, err := decodeWireBytes("action ID", r.PathValue("action"), false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		var wire fleetCompletionWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		contextValue, err := wire.Context.decode()
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		claimID, err := decodeWireBytes("claim ID", wire.ClaimID, false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		snapshotID, err := decodeOptionalID(wire.EvidenceSnapshotID)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		evidence, err := decodeEvidence(wire.Evidence)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		result, err := provider.CompleteClaim(r.Context(), fleet.CompletionRequest{
			ID: actionID, ExpectedVersion: wire.ExpectedVersion, ClaimID: claimID,
			Failed: wire.Failed, Context: contextValue,
			Result: fleet.ExecutionResult{
				Output: wire.Output, ErrorCode: wire.ErrorCode,
				EvidenceSnapshotID:   snapshotID,
				EvidenceSnapshotAsOf: wire.EvidenceSnapshotAsOf,
				Evidence:             evidence,
			},
		})
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		writeResponse(w, http.StatusOK, encodeFleetAction(result))
	})
	mux.HandleFunc("POST /api/v1/fleet/actions/{action}/cancel", func(w http.ResponseWriter, r *http.Request) {
		actionID, err := decodeWireBytes("action ID", r.PathValue("action"), false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		var wire fleetCancelWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		contextValue, err := wire.Context.decode()
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		mutationKey, err := decodeWireBytes(
			"cancel mutation key", wire.MutationKey, false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		result, err := provider.Cancel(r.Context(), fleet.CancelRequest{
			ID: actionID, ExpectedVersion: wire.ExpectedVersion,
			MutationKey: mutationKey, Context: contextValue,
		})
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		writeResponse(w, http.StatusOK, encodeFleetAction(result))
	})
	mux.HandleFunc("POST /api/v1/fleet/actions/{action}/status", func(w http.ResponseWriter, r *http.Request) {
		actionID, err := decodeWireBytes("action ID", r.PathValue("action"), false)
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		var wire fleetRequestContextWire
		if err := decodeRequest(w, r, &wire); err != nil {
			writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		contextValue, err := wire.decode()
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		result, err := provider.Status(r.Context(), fleet.StatusRequest{ID: actionID, Context: contextValue})
		if err != nil {
			writeError(w, fleetDispatchError(err))
			return
		}
		writeResponse(w, http.StatusOK, encodeFleetAction(result))
	})
}

func fleetDispatchError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fleet.ErrActionNotFound):
		return shoal.WrapError(shoal.ErrorNotFound, "fleet action not found", err)
	case errors.Is(err, fleet.ErrActionConflict), errors.Is(err, fleet.ErrClaimLost), errors.Is(err, fleet.ErrActionTerminal):
		return shoal.WrapError(shoal.ErrorConflict, "fleet action conflict", err)
	case errors.Is(err, fleet.ErrExecutionAmbiguous), errors.Is(err, fleet.ErrActionCommitted), errors.Is(err, fleet.ErrRecordingUnavailable):
		return shoal.WrapError(shoal.ErrorUnavailable, "fleet action outcome requires reconciliation", err)
	default:
		return err
	}
}

type fleetEnqueueWire struct {
	Context         fleetRequestContextWire `json:"context"`
	ID              string                  `json:"id"`
	IdempotencyKey  string                  `json:"idempotency_key"`
	AgentID         string                  `json:"agent_id"`
	AgentGeneration int64                   `json:"agent_generation"`
	Capability      string                  `json:"capability"`
	Action          string                  `json:"action"`
	SourceID        []byte                  `json:"source_id"`
	PolicyID        []byte                  `json:"policy_id"`
	ObjectID        string                  `json:"object_id"`
	Input           json.RawMessage         `json:"input"`
}

type fleetInvokeWire struct {
	Action  fleetEnqueueWire `json:"action"`
	ClaimID string           `json:"claim_id"`
	Lease   time.Duration    `json:"lease"`
}

type fleetClaimWire struct {
	Context         fleetRequestContextWire `json:"context"`
	ExpectedVersion uint64                  `json:"expected_version"`
	ClaimID         string                  `json:"claim_id"`
	Lease           time.Duration           `json:"lease"`
}

type fleetPullWire struct {
	Context fleetRequestContextWire `json:"context"`
	After   string                  `json:"after,omitempty"`
	Limit   int                     `json:"limit"`
}

type fleetCancelWire struct {
	Context         fleetRequestContextWire `json:"context"`
	ExpectedVersion uint64                  `json:"expected_version"`
	MutationKey     string                  `json:"mutation_key"`
}

// fleetCompletionWire is a remote worker reporting an outcome. It carries the
// same fields an in-process ActionExecutor returns, and the service validates
// them identically.
type fleetCompletionWire struct {
	Context              fleetRequestContextWire `json:"context"`
	ExpectedVersion      uint64                  `json:"expected_version"`
	ClaimID              string                  `json:"claim_id"`
	Output               json.RawMessage         `json:"output,omitempty"`
	ErrorCode            string                  `json:"error_code,omitempty"`
	Failed               bool                    `json:"failed,omitempty"`
	EvidenceSnapshotID   string                  `json:"evidence_snapshot_id,omitempty"`
	EvidenceSnapshotAsOf time.Time               `json:"evidence_snapshot_as_of,omitempty"`
	Evidence             []fleetEvidenceWire     `json:"evidence,omitempty"`
}

type fleetEvidenceWire struct {
	AnchorID   string                   `json:"anchor_id"`
	Kind       interaction.EvidenceKind `json:"kind"`
	Citation   *fleetCitationWire       `json:"citation,omitempty"`
	NodeIDs    []string                 `json:"node_ids"`
	EdgeIDs    []string                 `json:"edge_ids,omitempty"`
	Assertions []fleetAssertionWire     `json:"assertions,omitempty"`
	Visibility []string                 `json:"visibility"`
}

type fleetCitationWire struct {
	DocumentID string `json:"document_id"`
	RevisionID string `json:"revision_id"`
	SectionID  string `json:"section_id,omitempty"`
	SpanID     string `json:"span_id,omitempty"`
	Start      int64  `json:"start"`
	End        int64  `json:"end"`
	StartPage  int32  `json:"start_page,omitempty"`
	EndPage    int32  `json:"end_page,omitempty"`
}

type fleetAssertionWire struct {
	AssertionID string                   `json:"assertion_id"`
	EdgeID      string                   `json:"edge_id"`
	Origin      ontology.AssertionOrigin `json:"origin"`
}

type fleetActionWire struct {
	ID              string              `json:"id"`
	Version         uint64              `json:"version"`
	State           fleet.DispatchState `json:"state"`
	AgentID         string              `json:"agent_id"`
	AgentGeneration int64               `json:"agent_generation"`
	Capability      string              `json:"capability"`
	Action          string              `json:"action"`
	// Input is what the work actually is, and without it an out-of-process
	// worker can pull an action, claim it under a fence and complete it without
	// ever receiving the operation's parameters. It was absent from this wire
	// while being present on the enqueue wire, validated against the action's
	// InputSchema at enqueue, and stored on the record — so the data was
	// correct and simply never read back out (#435).
	//
	// Nothing on a worker's side recovers this. A worker reconstructing a
	// plausible request from the capability name would be fabricating the
	// parameters of an effect Shoal cannot undo, which is why the gap had to be
	// closed here rather than worked around there.
	//
	// Emitted unconditionally, and what bounds who sees it is the
	// authorization to take the work — not the identity of whoever enqueued
	// it.
	//
	// That distinction matters because an earlier version of this comment made
	// the narrower claim: that every consumer of this wire is scoped to the
	// action's own principal, since Pull filtered on sameActionPrincipal. True
	// when written and false as of #437, which lets a principal granted
	// OperationExecute on the action's descriptor pull and claim work it did
	// not enqueue — and therefore read this field. Verified by composing the
	// two branches and reading the input back as a non-enqueuing executor, so
	// it is a measured consequence rather than a guess.
	//
	// It is also the point: a worker that does not receive the operation's
	// parameters cannot perform it, which is the whole of #435. So the honest
	// statement of the exposure is that an action's input is readable by any
	// principal authorized to execute that descriptor within the action's
	// scope, and the thing to get right is that grant rather than this field.
	//
	// What remains true either way is the one read path that does not require
	// the reader to be the originating principal: DispatchService.TeamActions
	// does not use this encoder and pkg/explorer/teamoverview never reads
	// Input. A test pins that, because if it changes this field becomes
	// readable without any execute grant at all.
	Input         json.RawMessage `json:"input,omitempty"`
	Output        json.RawMessage `json:"output,omitempty"`
	ErrorCode     string          `json:"error_code,omitempty"`
	RequestID     string          `json:"request_id"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	Deadline      time.Time       `json:"deadline"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	// ExecutorKey is the idempotency key an in-process executor already
	// receives (dispatch_service.go hands it over as IdempotencyKey), derived
	// at enqueue as a length-prefixed digest over a domain tag, the action ID
	// and the caller's idempotency key. An out-of-process worker performing an
	// irreversible effect needs exactly that value for exactly that purpose: it
	// is the stable identity that lets a target deduplicate a re-claim after a
	// lease lapse.
	//
	// Exposing it is strictly better than having each worker invent a key. A
	// hand-written digest over the same inputs is easy to get subtly wrong —
	// omitting the length prefixes makes it non-injective, so a caller able to
	// enqueue against two surfaces can make one key collide with another's, and
	// a collision makes a provider return a cached success without performing
	// the effect. That is worse than a duplicate, because nothing records it.
	ExecutorKey          string              `json:"executor_key,omitempty"`
	ClaimID              string              `json:"claim_id,omitempty"`
	ClaimFence           uint64              `json:"claim_fence,omitempty"`
	ClaimLeaseUntil      time.Time           `json:"claim_lease_until,omitempty"`
	EffectPossible       bool                `json:"effect_possible"`
	EvidenceSnapshotID   string              `json:"evidence_snapshot_id,omitempty"`
	EvidenceSnapshotAsOf time.Time           `json:"evidence_snapshot_as_of,omitempty"`
	Evidence             []fleetEvidenceWire `json:"evidence,omitempty"`
}

func (w fleetEnqueueWire) decode(pathID []byte) (fleet.EnqueueRequest, error) {
	contextValue, err := w.Context.decode()
	if err != nil {
		return fleet.EnqueueRequest{}, err
	}
	id, err := decodeWireBytes("action ID", w.ID, false)
	if err != nil {
		return fleet.EnqueueRequest{}, err
	}
	if len(pathID) != 0 && string(id) != string(pathID) {
		return fleet.EnqueueRequest{}, shoal.NewError(shoal.ErrorInvalidArgument, "action ID does not match path")
	}
	key, err := decodeWireBytes("idempotency key", w.IdempotencyKey, false)
	if err != nil {
		return fleet.EnqueueRequest{}, err
	}
	agent, err := decodeID(w.AgentID)
	if err != nil {
		return fleet.EnqueueRequest{}, shoal.NewError(shoal.ErrorInvalidArgument, "agent ID "+err.Error())
	}
	object, err := decodeID(w.ObjectID)
	if err != nil {
		return fleet.EnqueueRequest{}, shoal.NewError(shoal.ErrorInvalidArgument, "object ID "+err.Error())
	}
	return fleet.EnqueueRequest{
		ID: id, IdempotencyKey: key, AgentID: agent, AgentGeneration: w.AgentGeneration,
		Capability: w.Capability, Action: w.Action, SourceID: append([]byte(nil), w.SourceID...),
		PolicyID: append([]byte(nil), w.PolicyID...), ObjectID: object,
		Input: append(json.RawMessage(nil), w.Input...), Context: contextValue,
	}, nil
}

func decodeWireBytes(name, encoded string, optional bool) ([]byte, error) {
	if encoded == "" && optional {
		return nil, nil
	}
	value, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(value) == 0 || len(value) > fleet.MaxActionIDBytes {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" is invalid")
	}
	return value, nil
}

func decodeEvidence(values []fleetEvidenceWire) ([]fleet.EvidenceRef, error) {
	result := make([]fleet.EvidenceRef, len(values))
	for i, value := range values {
		var err error
		if result[i].AnchorID, err = decodeOptionalID(value.AnchorID); err != nil {
			return nil, err
		}
		result[i].Kind = value.Kind
		result[i].NodeIDs = make([]shoal.ID, len(value.NodeIDs))
		for index, id := range value.NodeIDs {
			if result[i].NodeIDs[index], err = decodeOptionalID(id); err != nil {
				return nil, err
			}
		}
		result[i].EdgeIDs = make([]shoal.ID, len(value.EdgeIDs))
		for index, id := range value.EdgeIDs {
			if result[i].EdgeIDs[index], err = decodeOptionalID(id); err != nil {
				return nil, err
			}
		}
		result[i].Assertions = make(
			[]interaction.AssertionReference, len(value.Assertions))
		for index, assertion := range value.Assertions {
			if result[i].Assertions[index].AssertionID, err =
				decodeOptionalID(assertion.AssertionID); err != nil {
				return nil, err
			}
			if result[i].Assertions[index].EdgeID, err =
				decodeOptionalID(assertion.EdgeID); err != nil {
				return nil, err
			}
			result[i].Assertions[index].Origin = assertion.Origin
		}
		if value.Citation != nil {
			citation := document.Citation{Range: document.SourceRange{
				Start: document.SourcePosition{
					Offset: value.Citation.Start, Page: value.Citation.StartPage},
				End: document.SourcePosition{
					Offset: value.Citation.End, Page: value.Citation.EndPage},
			}}
			if citation.DocumentID, err =
				decodeOptionalID(value.Citation.DocumentID); err != nil {
				return nil, err
			}
			if citation.RevisionID, err =
				decodeOptionalID(value.Citation.RevisionID); err != nil {
				return nil, err
			}
			if citation.SectionID, err =
				decodeOptionalID(value.Citation.SectionID); err != nil {
				return nil, err
			}
			if citation.SpanID, err =
				decodeOptionalID(value.Citation.SpanID); err != nil {
				return nil, err
			}
			result[i].Citation = citation
		}
		result[i].Visibility = append([]string(nil), value.Visibility...)
	}
	return result, nil
}

func encodeFleetAction(record fleet.ActionRecord) fleetActionWire {
	evidence := make([]fleetEvidenceWire, len(record.Evidence))
	for i, item := range record.Evidence {
		evidence[i] = fleetEvidenceWire{
			AnchorID: encodeFleetID(item.AnchorID), Kind: item.Kind,
			NodeIDs: encodeFleetIDs(item.NodeIDs), EdgeIDs: encodeFleetIDs(item.EdgeIDs),
			Assertions: make([]fleetAssertionWire, len(item.Assertions)),
			Visibility: append([]string(nil), item.Visibility...),
		}
		for index, assertion := range item.Assertions {
			evidence[i].Assertions[index] = fleetAssertionWire{
				AssertionID: encodeFleetID(assertion.AssertionID),
				EdgeID:      encodeFleetID(assertion.EdgeID), Origin: assertion.Origin,
			}
		}
		if item.Kind == interaction.EvidenceDocument {
			evidence[i].Citation = &fleetCitationWire{
				DocumentID: encodeFleetID(item.Citation.DocumentID),
				RevisionID: encodeFleetID(item.Citation.RevisionID),
				SectionID:  encodeFleetID(item.Citation.SectionID),
				SpanID:     encodeFleetID(item.Citation.SpanID),
				Start:      item.Citation.Range.Start.Offset,
				End:        item.Citation.Range.End.Offset,
				StartPage:  item.Citation.Range.Start.Page,
				EndPage:    item.Citation.Range.End.Page,
			}
		}
	}
	return fleetActionWire{
		ID: base64.RawURLEncoding.EncodeToString(record.ID), Version: record.Version, State: record.State,
		AgentID: encodeFleetID(record.AgentID), AgentGeneration: record.AgentGeneration,
		Capability: record.Capability, Action: record.Action,
		Input:     append(json.RawMessage(nil), record.Input...),
		Output:    append(json.RawMessage(nil), record.Output...),
		ErrorCode: record.ErrorCode, RequestID: encodeFleetID(record.RequestID),
		CorrelationID: encodeFleetID(record.CorrelationID), Deadline: record.Deadline,
		CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
		ExecutorKey: base64.RawURLEncoding.EncodeToString(record.ExecutorKey),
		ClaimID:     base64.RawURLEncoding.EncodeToString(record.ClaimID), ClaimFence: record.ClaimFence,
		ClaimLeaseUntil: record.ClaimLeaseUntil, EffectPossible: record.EffectPossible,
		EvidenceSnapshotID:   encodeFleetID(record.EvidenceSnapshotID),
		EvidenceSnapshotAsOf: record.EvidenceSnapshotAsOf, Evidence: evidence,
	}
}

func encodeFleetIDs(values []shoal.ID) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = encodeFleetID(value)
	}
	return result
}
