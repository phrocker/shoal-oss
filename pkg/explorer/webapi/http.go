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
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"strings"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const maxRequestBytes = 1 << 20

//go:embed static/*
var staticFiles embed.FS

// Handler exposes only the logical Explorer API and static workspace assets.
type Handler struct {
	service          Service
	mux              *http.ServeMux
	allowedAuthority string
}

// NewHandler constructs the standard HTTP transport.
func NewHandler(service Service, allowedAuthority string) (*Handler, error) {
	if service == nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "workspace service is required")
	}
	host, port, err := net.SplitHostPort(allowedAuthority)
	if err != nil || host == "" || port == "" {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "workspace authority must be a host and port")
	}
	handler := &Handler{
		service: service, mux: http.NewServeMux(),
		allowedAuthority: allowedAuthority,
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
			"connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
	)
	if !strings.EqualFold(request.Host, h.allowedAuthority) {
		http.Error(writer, "misdirected request", http.StatusMisdirectedRequest)
		return
	}
	h.mux.ServeHTTP(writer, request)
}

func (h *Handler) routes() {
	h.mux.HandleFunc("GET /api/v1/meta", func(writer http.ResponseWriter, _ *http.Request) {
		writeResponse(writer, http.StatusOK, MetadataResponse{
			MaxPageSize: MaxPageSize, MaxTopK: MaxTopK, MaxDepth: MaxDepth,
			MaxFanout: MaxFanout, MaxNodes: MaxNodes,
		})
	})
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
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := shoal.ErrorInternal
	switch {
	case shoal.IsErrorCode(err, shoal.ErrorInvalidArgument):
		status, code = http.StatusBadRequest, shoal.ErrorInvalidArgument
	case shoal.IsErrorCode(err, shoal.ErrorNotFound):
		status, code = http.StatusNotFound, shoal.ErrorNotFound
	case shoal.IsErrorCode(err, shoal.ErrorConflict):
		status, code = http.StatusConflict, shoal.ErrorConflict
	case shoal.IsErrorCode(err, shoal.ErrorUnavailable):
		status, code = http.StatusServiceUnavailable, shoal.ErrorUnavailable
	case shoal.IsErrorCode(err, shoal.ErrorCanceled):
		status, code = 499, shoal.ErrorCanceled
	case shoal.IsErrorCode(err, shoal.ErrorDeadline):
		status, code = http.StatusGatewayTimeout, shoal.ErrorDeadline
	}
	writeResponse(writer, status, struct {
		Code    shoal.ErrorCode `json:"code"`
		Message string          `json:"message"`
	}{Code: code, Message: err.Error()})
}
