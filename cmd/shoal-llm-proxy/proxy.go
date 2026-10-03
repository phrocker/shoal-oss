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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// maxRequestBytes bounds what the proxy will read from a caller. An
// OpenAI-compatible body is prompt text; without a bound a caller could make
// the proxy hold arbitrary memory before any decision is taken.
const maxRequestBytes = 8 << 20

// proxy is the enforcement point: admit, apply, forward, report.
type proxy struct {
	admission  *admissionClient
	upstream   *url.URL
	client     *http.Client
	credential func() (string, error)
	clock      func() time.Time
	// log receives operational events only. It is never given prompt or
	// completion content; see logRefusal.
	log func(string, ...any)
}

func (p *proxy) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", p.completions)
	return mux
}

// completions is the whole contract in one function, in the order that matters:
// nothing reaches the upstream before Shoal has answered.
func (p *proxy) completions(writer http.ResponseWriter, request *http.Request) {
	raw, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes+1))
	if err != nil || len(raw) > maxRequestBytes {
		p.refuse(writer, http.StatusRequestEntityTooLarge, "request_too_large",
			"request body exceeds the proxy's bound")
		return
	}
	parsed, err := parseChatRequest(raw)
	if err != nil {
		p.refuse(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	references, err := parsed.references()
	if err != nil {
		p.refuse(writer, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	identity, err := newCallerIdentity()
	if err != nil {
		// No identity means no admission, and no admission means no call.
		p.refuse(writer, http.StatusServiceUnavailable, "plane_unavailable",
			"the decision plane could not be consulted")
		return
	}

	now := p.clock()
	granted, err := p.admission.request(
		request.Context(), identity, parsed.declaration(references), references, now)
	switch {
	case errors.Is(err, ErrDenied):
		// A policy denial. The body names no policy, compartment or document:
		// a refusal that explains itself is an authorization oracle over
		// whatever it names.
		p.log("admission denied request_id=%s", identity.RequestID)
		p.refuse(writer, http.StatusForbidden, "denied",
			"the decision plane denied this call")
		return
	case errors.Is(err, ErrPlaneUnreachable):
		// Fail closed, and say which kind of failure it is. A caller told
		// "denied" will not retry; one told "unavailable" should. The operator
		// needs the same distinction to tell an outage from a policy change.
		p.log("admission unavailable request_id=%s: %v", identity.RequestID, err)
		p.refuse(writer, http.StatusServiceUnavailable, "plane_unavailable",
			"the decision plane could not be consulted")
		return
	case err != nil:
		p.log("admission failed request_id=%s: %v", identity.RequestID, err)
		p.refuse(writer, http.StatusServiceUnavailable, "plane_unavailable",
			"the decision plane could not be consulted")
		return
	}

	outbound, satisfiable, err := parsed.applyObligations(granted.Withhold)
	if err != nil || !satisfiable {
		// The obligation could not be met. This is the one place refusal is
		// correct rather than lazy, and it is reported so the plane learns the
		// obligation was unsatisfiable rather than that the caller went dark.
		p.log("obligation unsatisfiable request_id=%s withheld=%d",
			identity.RequestID, len(granted.Withhold))
		p.reportFailure(request.Context(), granted.token, identity, "obligation_unsatisfiable")
		p.refuse(writer, http.StatusForbidden, "obligation_unsatisfiable",
			"the call cannot be made within the obligations returned")
		return
	}

	p.forward(writer, request, granted, identity, outbound, parsed.stream)
}

// forward sends the obligated request upstream and reports the outcome.
func (p *proxy) forward(
	writer http.ResponseWriter,
	request *http.Request,
	granted grant,
	identity callerIdentity,
	outbound json.RawMessage,
	stream bool,
) {
	endpoint := p.upstream.JoinPath("v1", "chat", "completions")
	upstreamRequest, err := http.NewRequestWithContext(
		request.Context(), http.MethodPost, endpoint.String(), bytes.NewReader(outbound))
	if err != nil {
		p.reportFailure(request.Context(), granted.token, identity, "upstream_unreachable")
		p.refuse(writer, http.StatusBadGateway, "upstream_unreachable",
			"the upstream provider could not be reached")
		return
	}
	credential, err := p.credential()
	if err != nil {
		p.reportFailure(request.Context(), granted.token, identity, "upstream_credential_unavailable")
		p.refuse(writer, http.StatusBadGateway, "upstream_unreachable",
			"the upstream provider could not be reached")
		return
	}
	upstreamRequest.Header.Set("Authorization", "Bearer "+credential)
	upstreamRequest.Header.Set("Content-Type", "application/json")
	if accept := request.Header.Get("Accept"); accept != "" {
		upstreamRequest.Header.Set("Accept", accept)
	}

	response, err := p.client.Do(upstreamRequest)
	if err != nil {
		p.reportFailure(request.Context(), granted.token, identity, "upstream_unreachable")
		p.refuse(writer, http.StatusBadGateway, "upstream_unreachable",
			"the upstream provider could not be reached")
		return
	}
	defer response.Body.Close()

	// The egress has now happened. Everything below reports it; nothing below
	// can prevent it, and the proxy does not pretend otherwise — no mid-stream
	// interception, because tokens already sent cannot be recalled (#390).
	for _, header := range []string{"Content-Type", "Cache-Control"} {
		if value := response.Header.Get(header); value != "" {
			writer.Header().Set(header, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	transferred, copyErr := p.relay(writer, response.Body, stream)

	failure := ""
	switch {
	case copyErr != nil:
		failure = "response_truncated"
	case response.StatusCode >= 400:
		failure = "upstream_error"
	}
	p.reportOutcome(
		context.WithoutCancel(request.Context()), granted.token, identity,
		response.StatusCode, transferred, failure)
}

// relay streams the response through, flushing per chunk so a streamed body
// reaches the caller as it arrives rather than when it completes.
func (p *proxy) relay(
	writer http.ResponseWriter, body io.Reader, stream bool,
) (int64, error) {
	if !stream {
		return io.Copy(writer, body)
	}
	flusher, _ := writer.(http.Flusher)
	buffer := make([]byte, 16<<10)
	var total int64
	for {
		read, err := body.Read(buffer)
		if read > 0 {
			written, writeErr := writer.Write(buffer[:read])
			total += int64(written)
			if flusher != nil {
				flusher.Flush()
			}
			if writeErr != nil {
				return total, writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
	}
}

// reportOutcome tells the plane what happened.
//
// The report carries status and size, never the completion — and that is
// structural rather than careful. This function is not given the response
// body, and relay streams it straight to the caller without buffering it, so
// there is no variable here holding a completion for a future edit to pass
// along. Leaking one would take a signature change, which is a different kind
// of mistake from forgetting to redact.
//
// A streamed response is reported once, after it finishes, which is why this
// is the only place a forwarded call reports: reporting before the stream ends
// would record an outcome the proxy had not yet observed.
func (p *proxy) reportOutcome(
	ctx context.Context,
	token admissionToken,
	identity callerIdentity,
	status int,
	transferred int64,
	failure string,
) {
	outcome, _ := json.Marshal(map[string]any{
		"upstream_status": status,
		"response_bytes":  transferred,
	})
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := p.admission.report(
		ctx, token, identity, outcome, failure, p.clock()); err != nil {
		// An unreported grant is the gap the report closes, so this is said out
		// loud. The plane already shows it as outstanding; the operator should
		// not have to find it there to learn the proxy could not report.
		p.log("report failed request_id=%s: %v", identity.RequestID, err)
	}
}

func (p *proxy) reportFailure(
	ctx context.Context, token admissionToken, identity callerIdentity, code string,
) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := p.admission.report(
		ctx, token, identity, nil, code, p.clock()); err != nil {
		p.log("report failed request_id=%s: %v", identity.RequestID, err)
	}
}

// refuse writes a structured refusal in the shape an OpenAI-compatible client
// already understands, so an unmodified client surfaces it as an API error
// rather than as a parse failure.
//
// The detail is a fixed string per class. It never names a policy, a
// compartment, a document or another principal: a refusal that explains itself
// is an oracle over whatever it explains.
func (p *proxy) refuse(
	writer http.ResponseWriter, status int, code, detail string,
) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{
		"error": map[string]any{
			"type":    code,
			"code":    code,
			"message": detail,
			"param":   nil,
		},
	})
}

func newProxy(
	admission *admissionClient,
	upstream string,
	credential func() (string, error),
	timeout time.Duration,
	clock func() time.Time,
	log func(string, ...any),
) (*proxy, error) {
	parsed, err := url.Parse(upstream)
	if err != nil || !parsed.IsAbs() || parsed.Hostname() == "" {
		return nil, fmt.Errorf("upstream base URL must be absolute")
	}
	return &proxy{
		admission: admission, upstream: parsed,
		client:     &http.Client{Timeout: timeout},
		credential: credential, clock: clock, log: log,
	}, nil
}
