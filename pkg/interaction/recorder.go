/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package interaction

import (
	"context"
	"reflect"
	"time"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Sink is the durable interaction boundary implemented by Explorer and its
// authorization-enforcing client.
type Sink interface {
	EnsureInteractionSink(context.Context) error
	RecordInteraction(context.Context, Session) error
}

// ResultSink extends Sink for product recorders that return the exact session
// accepted for persistence. Authorization-enforcing sinks use this result to
// expose trusted actor/reason enrichment rather than caller-supplied values.
type ResultSink interface {
	Sink
	RecordInteractionResult(context.Context, Session) (Session, error)
}

// Recorder is the product-level fail-closed recorder for retrieval, chat, MCP,
// and other non-harness adapters. It canonicalizes a typed Session, supplies a
// UTC timestamp when one is absent, and returns only after the sink accepts the
// durable record.
type Recorder struct {
	sink ResultSink
	now  func() time.Time
}

// NewRecorder verifies the sink during setup so a product surface cannot begin
// serving interactions without durable capture.
func NewRecorder(ctx context.Context, sink ResultSink) (*Recorder, error) {
	if ctx == nil {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "context is required")
	}
	if isNilSink(sink) {
		return nil, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction sink is required")
	}
	if err := sink.EnsureInteractionSink(ctx); err != nil {
		return nil, err
	}
	return &Recorder{sink: sink, now: time.Now}, nil
}

// SetClock configures the recorder clock for deterministic tests.
func (r *Recorder) SetClock(now func() time.Time) error {
	if r == nil {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction recorder is required")
	}
	if now == nil {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction recorder clock is required")
	}
	r.now = now
	return nil
}

// Record durably stores a typed interaction. A missing RecordedAt is filled
// from the recorder clock; IDs and all other security-relevant pins remain
// caller-supplied and validated rather than guessed.
func (r *Recorder) Record(
	ctx context.Context, session Session,
) (Session, error) {
	if r == nil || isNilSink(r.sink) {
		return Session{}, shoal.NewError(
			shoal.ErrorInvalidArgument, "interaction recorder is required")
	}
	if session.RecordedAt.IsZero() {
		session.RecordedAt = r.now().UTC()
	}
	canonical, err := session.Canonical()
	if err != nil {
		return Session{}, err
	}
	return r.sink.RecordInteractionResult(ctx, canonical)
}

func isNilSink(sink ResultSink) bool {
	if sink == nil {
		return true
	}
	value := reflect.ValueOf(sink)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
