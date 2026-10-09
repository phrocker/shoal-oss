// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package webapi

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	decisionapi "github.com/phrocker/shoal-oss/pkg/decision/api"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const RegistrationsRoute = "/api/v1/decision-registrations"

// NewRegistrationHTTPHandler requires trusted authentication and a provider
// enforcing current registration rights. This handler grants no authority.
func NewRegistrationHTTPHandler(provider decisionapi.RegistrationProvider, resolver auth.Resolver) (http.Handler, error) {
	if isAbsentInterface(provider) || isAbsentInterface(resolver) {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "registration provider and trusted resolver required")
	}
	return &registrationHTTP{provider: provider, resolver: resolver}, nil
}

func (h *Handler) MountRegistrations(provider decisionapi.RegistrationProvider, resolver auth.Resolver) error {
	handler, err := NewRegistrationHTTPHandler(provider, resolver)
	if err != nil {
		return err
	}
	if h == nil || h.mux == nil || isAbsentInterface(h.authenticator) || isAbsentInterface(h.binder) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "registrations require authenticated transport")
	}
	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodDelete, http.MethodHead} {
		if exactMuxPatternRegistered(h.mux, method, RegistrationsRoute) {
			return authenticatedMountConflict()
		}
	}
	if err = h.MountAuthenticated(RegistrationsRoute+"/", handler); err != nil {
		return err
	}
	for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodDelete} {
		h.mux.Handle(method+" "+RegistrationsRoute, handler)
	}
	return nil
}

type registrationHTTP struct {
	provider decisionapi.RegistrationProvider
	resolver auth.Resolver
}

func registrationHeaderValues(r *http.Request, name string) ([]string, bool) {
	var values []string
	present := false
	for key, value := range r.Header {
		if strings.EqualFold(key, name) {
			present = true
			values = append(values, value...)
		}
	}
	return values, present
}

func invalidRegistrationWire() error {
	return shoal.NewError(shoal.ErrorInvalidArgument, "invalid registration request")
}

func registrationPath(r *http.Request) (shoal.ID, error) {
	if r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery {
		return "", invalidRegistrationWire()
	}
	if r.URL.Path == RegistrationsRoute {
		return "", nil
	}
	if !strings.HasPrefix(r.URL.Path, RegistrationsRoute+"/") {
		return "", invalidRegistrationWire()
	}
	id, err := decisionapi.DecodeID(strings.TrimPrefix(r.URL.Path, RegistrationsRoute+"/"))
	if err != nil {
		return "", invalidRegistrationWire()
	}
	return id, nil
}

func (h *registrationHTTP) ValidatePreAuthentication(r *http.Request) int {
	origins, originPresent := registrationHeaderValues(r, "Origin")
	sites, sitePresent := registrationHeaderValues(r, "Sec-Fetch-Site")
	if (originPresent && len(origins) != 1) || (sitePresent && len(sites) != 1) {
		return http.StatusForbidden
	}
	if sitePresent && sites[0] != "same-origin" && sites[0] != "none" {
		return http.StatusForbidden
	}
	if originPresent {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if origins[0] != scheme+"://"+r.Host {
			return http.StatusForbidden
		}
	}
	if _, present := registrationHeaderValues(r, "Cookie"); present {
		return http.StatusBadRequest
	}
	if _, err := registrationPath(r); err != nil {
		return http.StatusBadRequest
	}
	return 0
}

func registrationWireError(w http.ResponseWriter, err error, uncertain bool) {
	code, status, message := shoal.ErrorUnavailable, http.StatusServiceUnavailable, "registration service unavailable"
	switch {
	case shoal.IsErrorCode(err, shoal.ErrorInvalidArgument):
		code, status, message = shoal.ErrorInvalidArgument, http.StatusBadRequest, "invalid registration request"
	case shoal.IsErrorCode(err, shoal.ErrorUnauthorized):
		code, status, message = shoal.ErrorUnauthorized, http.StatusUnauthorized, "authentication required"
	case shoal.IsErrorCode(err, shoal.ErrorNotFound):
		code, status, message = shoal.ErrorNotFound, http.StatusNotFound, "registration not found"
	case shoal.IsErrorCode(err, shoal.ErrorConflict):
		code, status, message = shoal.ErrorConflict, http.StatusConflict, "registration conflict"
	}
	if uncertain {
		code, status, message = shoal.ErrorUnavailable, http.StatusServiceUnavailable, "registration requires reconciliation"
		w.Header().Set(CommitOutcomeHeader, CommitOutcomeIndeterminate)
	}
	writeResponse(w, status, struct {
		Code          shoal.ErrorCode `json:"code"`
		Message       string          `json:"message"`
		Indeterminate bool            `json:"indeterminate"`
	}{code, message, uncertain})
}

func (h *registrationHTTP) resolve(ctx context.Context) (auth.Fingerprint, error) {
	if ctx.Err() != nil {
		return auth.Fingerprint{}, authenticationDenied()
	}
	decision, err := h.resolver.Resolve(ctx)
	if err != nil || ctx.Err() != nil || len(decision.OnBehalfOf()) != 0 ||
		!time.Now().Before(decision.AuthenticationExpires()) {
		return auth.Fingerprint{}, authenticationDenied()
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return auth.Fingerprint{}, authenticationDenied()
	}
	return fingerprint, nil
}

func decodeRegistrationWire(r *http.Request) (decisionapi.RegistrationSelection, shoal.ID, []byte, error) {
	var zero decisionapi.RegistrationSelection
	target, err := registrationPath(r)
	if err != nil {
		return zero, "", nil, err
	}
	if _, present := registrationHeaderValues(r, "Content-Encoding"); present {
		return zero, "", nil, invalidRegistrationWire()
	}
	if r.Method == http.MethodGet {
		if _, present := registrationHeaderValues(r, "Idempotency-Key"); present {
			return zero, "", nil, invalidRegistrationWire()
		}
		if target == "" || r.ContentLength > 0 {
			return zero, "", nil, invalidRegistrationWire()
		}
		if r.Body != nil {
			body, readErr := io.ReadAll(io.LimitReader(r.Body, 1))
			if readErr != nil || len(body) != 0 {
				return zero, "", nil, invalidRegistrationWire()
			}
		}
		return zero, target, nil, nil
	}
	if r.Method != http.MethodPost || r.URL.Path != RegistrationsRoute || r.Body == nil || r.ContentLength > decisionapi.MaxRegistrationRequestBytes {
		return zero, "", nil, invalidRegistrationWire()
	}
	encodedKey, err := decisionHeader(r, "Idempotency-Key")
	if err != nil {
		return zero, "", nil, invalidRegistrationWire()
	}
	key, err := decisionapi.DecodeKey(encodedKey)
	if err != nil {
		return zero, "", nil, invalidRegistrationWire()
	}
	contentType, err := decisionHeader(r, "Content-Type")
	if err != nil {
		return zero, "", nil, invalidRegistrationWire()
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		return zero, "", nil, invalidRegistrationWire()
	}
	for name, value := range params {
		if name != "charset" || !strings.EqualFold(value, "utf-8") {
			return zero, "", nil, invalidRegistrationWire()
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, decisionapi.MaxRegistrationRequestBytes+1))
	if err != nil || len(body) > decisionapi.MaxRegistrationRequestBytes {
		return zero, "", nil, invalidRegistrationWire()
	}
	selection, err := decisionapi.DecodeRegistrationRequest(body)
	if err != nil {
		return zero, "", nil, invalidRegistrationWire()
	}
	return selection, "", key, nil
}

func (h *registrationHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if status := h.ValidatePreAuthentication(r); status != 0 {
		http.Error(w, http.StatusText(status), status)
		return
	}
	before, err := h.resolve(r.Context())
	if err != nil {
		registrationWireError(w, authenticationDenied(), false)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		writeResponse(w, http.StatusMethodNotAllowed, struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}{"invalid_argument", "method not allowed"})
		return
	}
	selection, target, key, err := decodeRegistrationWire(r)
	if err != nil {
		registrationWireError(w, err, false)
		return
	}
	current, err := h.resolve(r.Context())
	if err != nil || current != before {
		registrationWireError(w, authenticationDenied(), false)
		return
	}
	appendInvoked := r.Method == http.MethodPost
	expected, err := decisionapi.NormalizeRegistrationSelection(selection)
	if appendInvoked && err != nil {
		registrationWireError(w, invalidRegistrationWire(), false)
		return
	}
	var receipt decisionapi.RegistrationReceipt
	if appendInvoked {
		receipt, err = h.provider.Register(r.Context(), selection, key)
	} else {
		receipt, err = h.provider.Read(r.Context(), target)
	}
	current, authErr := h.resolve(r.Context())
	if authErr != nil || current != before {
		registrationWireError(w, authenticationDenied(), appendInvoked)
		return
	}
	if err != nil {
		registrationWireError(w, err, appendInvoked)
		return
	}
	if appendInvoked {
		if !decisionapi.MatchRegistrationSelection(expected, receipt) {
			registrationWireError(w, invalidRegistrationWire(), true)
			return
		}
	} else if receipt.ID != target || receipt.Snapshot.RequestID != target {
		registrationWireError(w, shoal.NewError(shoal.ErrorUnavailable, "invalid registration response"), false)
		return
	}
	encoded, err := decisionapi.EncodeRegistrationReceipt(receipt)
	if err != nil || uint64(len(encoded))+1 > responseLimitFor(w) || len(encoded) > decisionapi.MaxRegistrationResponseBytes {
		registrationWireError(w, shoal.NewError(shoal.ErrorUnavailable, "invalid registration response"), appendInvoked)
		return
	}
	current, authErr = h.resolve(r.Context())
	if authErr != nil || current != before {
		registrationWireError(w, authenticationDenied(), appendInvoked)
		return
	}
	writeResponse(w, http.StatusOK, json.RawMessage(encoded))
}
