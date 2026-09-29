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

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// AdmissionRoutePrefix is the authenticated mount for the pre-call admission
// seam.
//
// It is an HTTP surface rather than a Go interface because the caller is a
// different binary by construction. A process that holds the prompt and speaks
// to a third-party endpoint must not share an address space with the policy
// store and the corpus: that would put untrusted prompt content inside the
// decision plane.
const AdmissionRoutePrefix = "/api/v1/admission/"

type AdmissionProvider interface {
	Request(context.Context, fleet.AdmissionRequest) (fleet.AdmissionGrant, error)
	Report(context.Context, fleet.AdmissionReport) (fleet.ActionRecord, error)
	Outstanding(
		context.Context, fleet.OutstandingAdmissionsRequest,
	) (fleet.OutstandingAdmissionsPage, error)
}

// NewAdmissionHandler returns the admission surface without adding another
// authentication boundary. Mount it through Handler.MountAuthenticated at
// AdmissionRoutePrefix.
func NewAdmissionHandler(provider AdmissionProvider) (http.Handler, error) {
	if provider == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "admission provider is required")
	}
	mux := http.NewServeMux()
	mountAdmission(mux, provider)
	return mux, nil
}

func mountAdmission(mux *http.ServeMux, provider AdmissionProvider) {
	mux.HandleFunc(
		"POST /api/v1/admission/request",
		func(w http.ResponseWriter, r *http.Request) {
			var wire admissionRequestWire
			if err := decodeRequest(w, r, &wire); err != nil {
				writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
				return
			}
			request, err := wire.decode()
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			grant, err := provider.Request(r.Context(), request)
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			// A refusal is a successful response, not an error status. The
			// caller has to be able to tell "Shoal decided no" from "Shoal could
			// not be reached", because the two demand opposite handling: the
			// first is the answer, the second is an outage the caller must fail
			// closed on itself. Encoding a denial as a 4xx would collapse them.
			writeResponse(w, http.StatusOK, encodeAdmissionGrant(grant))
		})
	mux.HandleFunc(
		"POST /api/v1/admission/report",
		func(w http.ResponseWriter, r *http.Request) {
			var wire admissionReportWire
			if err := decodeRequest(w, r, &wire); err != nil {
				writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
				return
			}
			report, err := wire.decode()
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			record, err := provider.Report(r.Context(), report)
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			writeResponse(w, http.StatusOK, admissionReceiptWire{
				ActionID: base64.RawURLEncoding.EncodeToString(record.ID),
				Version:  record.Version, State: record.State,
				ReportedAt: record.UpdatedAt,
			})
		})
	mux.HandleFunc(
		"POST /api/v1/admission/outstanding",
		func(w http.ResponseWriter, r *http.Request) {
			var wire admissionOutstandingWire
			if err := decodeRequest(w, r, &wire); err != nil {
				writeError(w, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
				return
			}
			contextValue, err := wire.Context.decode()
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			after, err := decodeWireBytes("admission cursor", wire.After, true)
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			page, err := provider.Outstanding(
				r.Context(), fleet.OutstandingAdmissionsRequest{
					After: after, Limit: wire.Limit, Context: contextValue,
				})
			if err != nil {
				writeError(w, admissionError(err))
				return
			}
			admissions := make(
				[]outstandingAdmissionWire, len(page.Admissions))
			for i, admission := range page.Admissions {
				admissions[i] = outstandingAdmissionWire{
					ActionID: base64.RawURLEncoding.EncodeToString(
						admission.ActionID),
					TokenID: base64.RawURLEncoding.EncodeToString(
						admission.TokenID),
					Version: admission.Version, AdmittedAt: admission.AdmittedAt,
					ExpiresAt: admission.ExpiresAt, Expired: admission.Expired,
				}
			}
			writeResponse(w, http.StatusOK, struct {
				Admissions []outstandingAdmissionWire `json:"admissions"`
				Next       string                     `json:"next,omitempty"`
			}{
				Admissions: admissions,
				Next:       base64.RawURLEncoding.EncodeToString(page.Next),
			})
		})
}

// admissionError maps service failures on to transport shapes.
//
// A spent token is a conflict, never a not-found and never an unauthorized. All
// three would be true of different spent tokens — reported, cancelled, never
// issued — and distinguishing them would tell a caller whether an admission it
// does not hold exists.
func admissionError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fleet.ErrAdmissionSpent),
		errors.Is(err, fleet.ErrAdmissionConflict):
		return shoal.WrapError(
			shoal.ErrorConflict, "admission token is not live", err)
	default:
		return fleetDispatchError(err)
	}
}

type admissionRequestWire struct {
	Context         fleetRequestContextWire `json:"context"`
	ID              string                  `json:"id"`
	IdempotencyKey  string                  `json:"idempotency_key"`
	TokenID         string                  `json:"token_id"`
	AgentID         string                  `json:"agent_id"`
	AgentGeneration int64                   `json:"agent_generation"`
	Capability      string                  `json:"capability"`
	Action          string                  `json:"action"`
	SourceID        []byte                  `json:"source_id"`
	PolicyID        []byte                  `json:"policy_id"`
	ObjectID        string                  `json:"object_id"`
	Effects         []string                `json:"effects"`
	Input           json.RawMessage         `json:"input"`
	Disclosures     []string                `json:"disclosures,omitempty"`
	Lease           time.Duration           `json:"lease"`
}

func (w admissionRequestWire) decode() (fleet.AdmissionRequest, error) {
	contextValue, err := w.Context.decode()
	if err != nil {
		return fleet.AdmissionRequest{}, err
	}
	id, err := decodeWireBytes("admission ID", w.ID, false)
	if err != nil {
		return fleet.AdmissionRequest{}, err
	}
	key, err := decodeWireBytes("idempotency key", w.IdempotencyKey, false)
	if err != nil {
		return fleet.AdmissionRequest{}, err
	}
	tokenID, err := decodeWireBytes("admission token ID", w.TokenID, false)
	if err != nil {
		return fleet.AdmissionRequest{}, err
	}
	agent, err := decodeID(w.AgentID)
	if err != nil {
		return fleet.AdmissionRequest{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "agent ID "+err.Error())
	}
	object, err := decodeID(w.ObjectID)
	if err != nil {
		return fleet.AdmissionRequest{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "object ID "+err.Error())
	}
	disclosures, err := decodeIDs(w.Disclosures)
	if err != nil {
		return fleet.AdmissionRequest{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "admission disclosure "+err.Error())
	}
	effects := make(fleet.Effects, len(w.Effects))
	for i, effect := range w.Effects {
		// Decoded verbatim, including a class this build does not know. The
		// service refuses an unrecognised declaration; translating one to an
		// empty set or dropping it here would turn a request for something
		// unknown into a request for nothing, which is the most permissive
		// reading of the least classifiable input.
		effects[i] = fleet.Effect(effect)
	}
	return fleet.AdmissionRequest{
		ID: id, IdempotencyKey: key, TokenID: tokenID, AgentID: agent,
		AgentGeneration: w.AgentGeneration, Capability: w.Capability,
		Action:   w.Action,
		SourceID: append([]byte(nil), w.SourceID...),
		PolicyID: append([]byte(nil), w.PolicyID...),
		ObjectID: object, Effects: effects,
		Input:       append(json.RawMessage(nil), w.Input...),
		Disclosures: disclosures, Lease: w.Lease, Context: contextValue,
	}, nil
}

type admissionTokenWire struct {
	ActionID  string    `json:"action_id"`
	TokenID   string    `json:"token_id"`
	Version   uint64    `json:"version"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (w admissionTokenWire) decode() (fleet.AdmissionToken, error) {
	actionID, err := decodeWireBytes("admission action ID", w.ActionID, false)
	if err != nil {
		return fleet.AdmissionToken{}, err
	}
	tokenID, err := decodeWireBytes("admission token ID", w.TokenID, false)
	if err != nil {
		return fleet.AdmissionToken{}, err
	}
	return fleet.AdmissionToken{
		ActionID: actionID, TokenID: tokenID,
		Version: w.Version, ExpiresAt: w.ExpiresAt,
	}, nil
}

type admissionGrantWire struct {
	Outcome fleet.AdmissionOutcome `json:"outcome"`
	Token   *admissionTokenWire    `json:"token,omitempty"`
	// Withhold is the obligation. It is always present on an allowed response,
	// empty when there is nothing to withhold, so a caller that reads the field
	// without checking the outcome still sees the full obligation rather than a
	// missing key it might read as "no obligation".
	//
	// Encoded exactly as the request's disclosures are, because the obligation
	// is a subset of them and a caller has to be able to match the two by
	// equality. Emitting raw identity bytes here against base64url on the way in
	// would make every obligation unmatchable for any identity that is not
	// already printable ASCII.
	Withhold []string `json:"withhold"`
}

func encodeAdmissionGrant(grant fleet.AdmissionGrant) admissionGrantWire {
	result := admissionGrantWire{
		Outcome:  grant.Outcome,
		Withhold: make([]string, 0, len(grant.Obligations.Withhold)),
	}
	for _, reference := range grant.Obligations.Withhold {
		result.Withhold = append(result.Withhold, encodeFleetID(reference))
	}
	// A denial carries no token. Issuing one would mean a refused caller could
	// report against it, and a report is a statement that an effect occurred.
	if grant.Outcome == fleet.AdmissionDenied {
		return result
	}
	result.Token = &admissionTokenWire{
		ActionID: base64.RawURLEncoding.EncodeToString(grant.Token.ActionID),
		TokenID:  base64.RawURLEncoding.EncodeToString(grant.Token.TokenID),
		Version:  grant.Token.Version, ExpiresAt: grant.Token.ExpiresAt,
	}
	return result
}

type admissionReportWire struct {
	Context   fleetRequestContextWire `json:"context"`
	Token     admissionTokenWire      `json:"token"`
	Outcome   json.RawMessage         `json:"outcome,omitempty"`
	Failed    bool                    `json:"failed,omitempty"`
	ErrorCode string                  `json:"error_code,omitempty"`
}

func (w admissionReportWire) decode() (fleet.AdmissionReport, error) {
	contextValue, err := w.Context.decode()
	if err != nil {
		return fleet.AdmissionReport{}, err
	}
	token, err := w.Token.decode()
	if err != nil {
		return fleet.AdmissionReport{}, err
	}
	return fleet.AdmissionReport{
		Token:   token,
		Outcome: append(json.RawMessage(nil), w.Outcome...),
		Failed:  w.Failed, ErrorCode: w.ErrorCode, Context: contextValue,
	}, nil
}

type admissionReceiptWire struct {
	ActionID   string              `json:"action_id"`
	Version    uint64              `json:"version"`
	State      fleet.DispatchState `json:"state"`
	ReportedAt time.Time           `json:"reported_at"`
}

type admissionOutstandingWire struct {
	Context fleetRequestContextWire `json:"context"`
	After   string                  `json:"after,omitempty"`
	Limit   int                     `json:"limit"`
}

type outstandingAdmissionWire struct {
	ActionID   string    `json:"action_id"`
	TokenID    string    `json:"token_id"`
	Version    uint64    `json:"version"`
	AdmittedAt time.Time `json:"admitted_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Expired    bool      `json:"expired"`
}
