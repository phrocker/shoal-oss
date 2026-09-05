// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package webapi

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"reflect"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const maxRequestBytes = 32 << 20

//go:embed static/*
var staticFiles embed.FS

// Handler exposes only the logical Explorer API and static workspace assets.
type Handler struct {
	service       Service
	mux           *http.ServeMux
	authority     hostAuthority
	authenticator Authenticator
	binder        auth.Binder
	browserAuth   *BrowserAuthConfig
}

// NewHandler constructs the standard HTTP transport without caller identity.
// It performs no authentication: the returned handler serves every request as
// one implicit anonymous caller and is safe only behind a transport that
// establishes identity itself. Use NewAuthenticatedHandler to require a
// per-request trusted decision.
//
// allowedAuthorities is the exact-match set of Host/:authority values the
// transport will serve; at least one is required and each must be a host or
// host:port. It is enforced centrally for every route (see ServeHTTP).
//
// The list is variadic rather than a required first authority plus a variadic
// remainder: the only caller that assembles authorities dynamically
// (cmd/shoal-explore-web) already holds a []string and spreads it, and a
// required-first shape would force that caller to hand-split the slice —
// reintroducing an index/empty-slice footgun at exactly the spot that computes
// the default. The empty case instead fails closed in newHostAuthority (see the
// len == 0 guard), turning the lost compile-time check into an equivalent
// construction-time error that every constructor path inherits.
func NewHandler(service Service, allowedAuthorities ...string) (*Handler, error) {
	if service == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "workspace service is required")
	}
	authority, err := newHostAuthority(allowedAuthorities)
	if err != nil {
		return nil, err
	}
	handler := &Handler{
		service: service, mux: http.NewServeMux(),
		authority: authority,
	}
	handler.routes()
	return handler, nil
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Referrer-Policy", "no-referrer")
	writer.Header().Set(
		"Content-Security-Policy",
		"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "+
			"connect-src "+h.connectSources()+"; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
	)
	if !h.authority.permits(request.Host) {
		http.Error(writer, "misdirected request", http.StatusMisdirectedRequest)
		return
	}
	if h.authenticator != nil && !h.publiclyReachable(request) {
		ctx, err := h.authenticate(request)
		if err != nil {
			// A failed authentication gate is a re-authenticate signal, not an
			// authorization denial. Mark it with the standard bearer challenge
			// so the browser can tell "present a fresh token" apart from
			// "authenticated but not permitted", which the service reports as
			// its own 401 with no challenge header. Both are 401, so app.js
			// keys re-authentication off this header, never off status alone.
			writer.Header().Set("WWW-Authenticate", "Bearer")
			writeError(writer, err)
			return
		}
		request = request.WithContext(ctx)
	}
	h.mux.ServeHTTP(writer, request)
}

func (h *Handler) routes() {
	h.mux.HandleFunc("GET /api/v1/meta", func(writer http.ResponseWriter, request *http.Request) {
		metadata, err := metadataFor(request.Context(), h.service)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, metadata)
	})
	h.mux.HandleFunc("GET /api/v1/identity", func(writer http.ResponseWriter, request *http.Request) {
		identity, ok := identityFromContext(request.Context())
		if !ok {
			identity = unauthenticatedIdentity()
		}
		writeResponse(writer, http.StatusOK, identity)
	})
	h.mux.HandleFunc("GET /api/v1/ontology", func(writer http.ResponseWriter, request *http.Request) {
		ontology, err := ontologyFor(request.Context(), h.service)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, ontology)
	})
	h.mux.HandleFunc("GET /api/v1/ontology/proposals", func(writer http.ResponseWriter, request *http.Request) {
		proposals, err := ontologyProposalsFor(request.Context(), h.service)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, proposals)
	})
	h.mux.HandleFunc("POST /api/v1/ontology/proposals", func(writer http.ResponseWriter, request *http.Request) {
		var input CreateOntologyProposalRequest
		if err := decodeRequest(writer, request, &input); err != nil {
			writeError(writer, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		proposal, err := createOntologyProposalFor(request.Context(), h.service, input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusCreated, proposal)
	})
	// This GET route is load-bearing; TestOntologyProposalBlastRadiusUsesStartedEmbeddedWorkspace
	// pins that the blast-radius surface is reachable through the real
	// cmd/shoal-explore-web startup path and EmbeddedService, not only test doubles.
	h.mux.HandleFunc("GET /api/v1/ontology/proposals/{proposal}/blast-radius", func(writer http.ResponseWriter, request *http.Request) {
		proposalID, err := decodeID(request.PathValue("proposal"))
		if err != nil {
			writeError(writer, shoal.NewError(
				shoal.ErrorInvalidArgument, "ontology proposal ID "+err.Error()))
			return
		}
		blastRadius, err := ontologyProposalBlastRadiusFor(
			request.Context(), h.service, proposalID)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, blastRadius)
	})
	h.mux.HandleFunc("POST /api/v1/ontology/proposals/{proposal}/transition", func(writer http.ResponseWriter, request *http.Request) {
		proposalID, err := decodeID(request.PathValue("proposal"))
		if err != nil {
			writeError(writer, shoal.NewError(
				shoal.ErrorInvalidArgument, "ontology proposal ID "+err.Error()))
			return
		}
		var input TransitionOntologyProposalRequest
		if err := decodeRequest(writer, request, &input); err != nil {
			writeError(writer, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		proposal, err := transitionOntologyProposalFor(
			request.Context(), h.service, proposalID, input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, proposal)
	})
	h.mux.HandleFunc("GET /api/v1/auth-config", h.authConfigEndpoint)
	h.mux.HandleFunc("POST /api/v1/ingest", ingestEndpoint(h.service))
	// This route registration is load-bearing; TestHTTPExtractPublishesUploadedSkillGraph pins extraction as an explicit user-triggered action.
	h.mux.HandleFunc("POST /api/v1/extract", extractEndpoint(h.service))
	// This route registration is load-bearing; TestHTTPRecomputeReDerivesLatentAssertion
	// pins that recomputing a derived edge is an explicit user-triggered action.
	h.mux.HandleFunc("POST /api/v1/derivation/recompute", recomputeEndpoint(h.service))
	h.mux.HandleFunc("POST /api/v1/changes", changesEndpoint(h.service))
	h.mux.HandleFunc("POST /api/v1/documents", endpoint(h.service.Documents))
	h.mux.HandleFunc("POST /api/v1/document", endpoint(h.service.Document))
	h.mux.HandleFunc("POST /api/v1/retrieve", endpoint(h.service.Retrieve))
	h.mux.HandleFunc("POST /api/v1/neighborhood", endpoint(h.service.Neighborhood))
	h.mux.HandleFunc("POST /api/v1/path", endpoint(h.service.Path))

	content, _ := fs.Sub(staticFiles, "static")
	h.mux.Handle("GET /assets/", http.StripPrefix("/assets/", http.FileServer(http.FS(content))))
	h.mux.HandleFunc("GET /", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeFileFS(writer, request, content, "index.html")
	})
}

func metadataFor(ctx context.Context, service Service) (MetadataResponse, error) {
	provider, ok := service.(MetadataProvider)
	if ok {
		metadata, err := provider.Metadata(ctx)
		if err != nil {
			return MetadataResponse{}, err
		}
		if _, ok := service.(IngestProvider); !ok {
			metadata.Capabilities.Ingest = false
		}
		if _, ok := service.(ExtractionProvider); !ok {
			metadata.Capabilities.Extraction = false
		}
		return metadata, nil
	}
	capabilities, err := capabilitiesFor(ctx, service)
	if err != nil {
		return MetadataResponse{}, err
	}
	return MetadataResponse{
		MaxPageSize: MaxPageSize, MaxTopK: MaxTopK, MaxDepth: MaxDepth,
		MaxFanout: MaxFanout, MaxNodes: MaxNodes, MaxEdgeTypes: MaxEdgeTypes,
		MaxResponseBytes: MaxResponseBytes, MaxUploadFiles: MaxUploadFiles,
		MaxUploadFileBytes: MaxUploadFileBytes, MaxUploadTotalBytes: MaxUploadTotalBytes,
		Capabilities: capabilities,
	}, nil
}

func capabilitiesFor(ctx context.Context, service Service) (Capabilities, error) {
	provider, ok := service.(CapabilityProvider)
	if !ok {
		capabilities := AllCapabilities()
		if _, ok := service.(IngestProvider); !ok {
			capabilities.Ingest = false
		}
		if _, ok := service.(ExtractionProvider); !ok {
			capabilities.Extraction = false
		}
		return capabilities, nil
	}
	capabilities, err := provider.Capabilities(ctx)
	if err != nil {
		return Capabilities{}, err
	}
	if _, ok := service.(IngestProvider); !ok {
		capabilities.Ingest = false
	}
	if _, ok := service.(ExtractionProvider); !ok {
		capabilities.Extraction = false
	}
	return capabilities, nil
}

func extractEndpoint(service Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		provider, ok := service.(ExtractionProvider)
		if !ok {
			writeError(writer, shoal.NewError(
				shoal.ErrorUnavailable, "workspace capability \"extraction\" is unavailable"))
			return
		}
		var input ExtractRequest
		if err := decodeRequest(writer, request, &input); err != nil {
			writeError(writer, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		response, err := provider.Extract(request.Context(), input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, response)
	}
}

// recomputeEndpoint re-runs the deterministic derivation behind a latent
// similarity assertion. It is gated on the optional RecomputeProvider extension
// so a backend that cannot re-derive reports the action as unavailable rather
// than answering with a fabricated result.
func recomputeEndpoint(service Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		provider, ok := service.(RecomputeProvider)
		if !ok {
			writeError(writer, shoal.NewError(
				shoal.ErrorUnavailable, "workspace capability \"recompute\" is unavailable"))
			return
		}
		var input RecomputeDerivationRequest
		if err := decodeRequest(writer, request, &input); err != nil {
			writeError(writer, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		response, err := provider.Recompute(request.Context(), input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, response)
	}
}

// changesEndpoint serves the resumable document change feed. It is a read, so
// it needs no workspace-mutation header, but it is gated on the optional
// ChangeProvider extension so a backend that cannot serve an ordered,
// authorized feed reports the capability as unavailable rather than serving an
// unfiltered one.
func changesEndpoint(service Service) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		provider, ok := service.(ChangeProvider)
		if !ok {
			writeError(writer, shoal.NewError(
				shoal.ErrorUnavailable, "workspace capability \"changes\" is unavailable"))
			return
		}
		var input ChangesRequest
		if err := decodeRequest(writer, request, &input); err != nil {
			writeError(writer, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		response, err := provider.Changes(request.Context(), input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, response)
	}
}

func endpoint[Request any, Response any](
	call func(context.Context, Request) (Response, error),
) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var input Request
		if err := decodeRequest(writer, request, &input); err != nil {
			writeError(writer, shoal.NewError(shoal.ErrorInvalidArgument, err.Error()))
			return
		}
		response, err := call(request.Context(), input)
		if err != nil {
			writeError(writer, err)
			return
		}
		writeResponse(writer, http.StatusOK, response)
	}
}

func decodeRequest(writer http.ResponseWriter, request *http.Request, value any) error {
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return errors.New("content type must be application/json")
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func writeResponse(writer http.ResponseWriter, status int, value any) {
	var body limitedResponseBuffer
	body.limit = int64(MaxResponseBytes)
	if err := json.NewEncoder(&body).Encode(value); err != nil {
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(writer).Encode(struct {
			Code    shoal.ErrorCode `json:"code"`
			Message string          `json:"message"`
		}{
			Code:    shoal.ErrorInternal,
			Message: shoal.NewError(shoal.ErrorInternal, "response exceeds max_response_bytes").Error(),
		})
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write(body.Bytes())
}

type limitedResponseBuffer struct {
	bytes.Buffer
	limit int64
}

func (b *limitedResponseBuffer) Write(p []byte) (int, error) {
	if int64(b.Len()+len(p)) > b.limit {
		return 0, errors.New("response exceeds max_response_bytes")
	}
	return b.Buffer.Write(p)
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := primaryErrorCode(err)
	switch code {
	case shoal.ErrorInvalidArgument:
		status, code = http.StatusBadRequest, shoal.ErrorInvalidArgument
	case shoal.ErrorNotFound:
		status, code = http.StatusNotFound, shoal.ErrorNotFound
	case shoal.ErrorConflict:
		status, code = http.StatusConflict, shoal.ErrorConflict
	case shoal.ErrorUnauthorized:
		status, code = http.StatusUnauthorized, shoal.ErrorUnauthorized
	case shoal.ErrorUnavailable:
		status, code = http.StatusServiceUnavailable, shoal.ErrorUnavailable
	case shoal.ErrorCanceled:
		status, code = 499, shoal.ErrorCanceled
	case shoal.ErrorDeadline:
		status, code = http.StatusGatewayTimeout, shoal.ErrorDeadline
	}
	writeResponse(writer, status, struct {
		Code    shoal.ErrorCode `json:"code"`
		Message string          `json:"message"`
	}{Code: code, Message: err.Error()})
}

func primaryErrorCode(err error) shoal.ErrorCode {
	pending := []error{err}
	seenComparable := make(map[error]struct{})
	seenReference := make(map[errorReference]struct{})
	for visited := 0; len(pending) > 0 && visited < 10_000; visited++ {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current == nil || errorAlreadyVisited(current, seenComparable, seenReference) {
			continue
		}
		if shoalErr, ok := current.(*shoal.Error); ok && shoalErr != nil {
			if !isKnownErrorCode(shoalErr.Code) {
				return shoal.ErrorInternal
			}
			return shoalErr.Code
		}
		switch wrapped := current.(type) {
		case interface{ Unwrap() []error }:
			values := wrapped.Unwrap()
			for index := len(values) - 1; index >= 0; index-- {
				pending = append(pending, values[index])
			}
		case interface{ Unwrap() error }:
			pending = append(pending, wrapped.Unwrap())
		}
	}
	return shoal.ErrorInternal
}

type errorReference struct {
	typ     reflect.Type
	pointer uintptr
}

func errorAlreadyVisited(
	err error,
	comparable map[error]struct{},
	references map[errorReference]struct{},
) bool {
	value := reflect.ValueOf(err)
	if value.Type().Comparable() {
		if _, ok := comparable[err]; ok {
			return true
		}
		comparable[err] = struct{}{}
		return false
	}
	var pointer uintptr
	switch value.Kind() {
	case reflect.Chan, reflect.Map, reflect.Pointer, reflect.Slice,
		reflect.UnsafePointer:
		pointer = uintptr(value.UnsafePointer())
	case reflect.Func:
		pointer = value.Pointer()
	default:
		return false
	}
	reference := errorReference{typ: value.Type(), pointer: pointer}
	if _, ok := references[reference]; ok {
		return true
	}
	references[reference] = struct{}{}
	return false
}
