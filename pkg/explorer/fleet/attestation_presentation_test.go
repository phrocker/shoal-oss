// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type fakePresenter struct {
	verifyErr, recordErr error
	verified, recorded   []AttestationPrincipal
}

func (p *fakePresenter) Verify(principal AttestationPrincipal, _ AttestationPresentation, now time.Time) (AttestationReceipt, error) {
	p.verified = append(p.verified, principal)
	if p.verifyErr != nil {
		return AttestationReceipt{}, p.verifyErr
	}
	return AttestationReceipt{AttestationID: "exattest:1", ExpiresAt: now.Add(time.Hour)}, nil
}

func (p *fakePresenter) Record(_ context.Context, principal AttestationPrincipal, _ AttestationPresentation, now time.Time) (AttestationReceipt, error) {
	p.recorded = append(p.recorded, principal)
	if p.recordErr != nil {
		return AttestationReceipt{}, p.recordErr
	}
	return AttestationReceipt{AttestationID: "exattest:1", ExpiresAt: now.Add(time.Hour)}, nil
}

type fakeAttestationRecorder struct {
	err    error
	audits []AttestationAudit
}

func (r *fakeAttestationRecorder) RecordAttestation(_ context.Context, audit AttestationAudit) error {
	r.audits = append(r.audits, audit)
	return r.err
}

func TestAttestationPresentation(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	authority, err := auth.NewAuthorityWithClock(func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	setup := func() (*AttestationService, *fakePresenter, *fakeAttestationRecorder) {
		presenter, recorder := &fakePresenter{}, &fakeAttestationRecorder{}
		service, err := NewAttestationService(AttestationConfig{
			Presenter: presenter, Resolver: authority.Resolver(), Recorder: recorder,
			Clock: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return service, presenter, recorder
	}
	caller := func(onBehalfOf []shoal.ID, operations ...auth.Operation) context.Context {
		return bindDecision(t, authority, dispatchDecisionFor(t, principal{
			subject: "alpha", actor: "alpha-actor", request: "present",
			clientID: "alpha-client", onBehalfOf: onBehalfOf,
		}, operations...))
	}
	presentation := AttestationPresentation{
		ExecutorRef: "exec", IdempotencyKey: []byte("key"), Report: []byte(`{}`),
	}
	refused := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, ErrAttestationRefused) || !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) ||
			err.Error() != "unauthorized: attestation refused" {
			t.Fatalf("err = %v, want the one opaque refusal", err)
		}
	}

	t.Run("requires execute", func(t *testing.T) {
		service, presenter, _ := setup()
		_, err := service.Present(caller(nil, auth.OperationInvoke, auth.OperationDispatch), presentation)
		if !shoal.IsErrorCode(err, shoal.ErrorUnauthorized) || errors.Is(err, ErrAttestationRefused) {
			t.Fatalf("err = %v", err)
		}
		if len(presenter.verified) != 0 {
			t.Fatal("verified for a caller without execute")
		}
	})
	t.Run("accepted, audited first, for the caller only", func(t *testing.T) {
		service, presenter, recorder := setup()
		receipt, err := service.Present(caller(nil, auth.OperationExecute), presentation)
		if err != nil || receipt.AttestationID == "" {
			t.Fatalf("present = %+v, %v", receipt, err)
		}
		want := AttestationPrincipal{Domain: []byte("domain"), Subject: "alpha", ClientID: "alpha-client"}
		if len(presenter.recorded) != 1 || !bytes.Equal(presenter.recorded[0].Domain, want.Domain) ||
			presenter.recorded[0].Subject != want.Subject || presenter.recorded[0].ClientID != want.ClientID {
			t.Fatalf("row key %+v, want the decision's %+v", presenter.recorded, want)
		}
		if len(recorder.audits) != 1 || recorder.audits[0].Phase != "attestation_presented" {
			t.Fatalf("audits = %+v", recorder.audits)
		}
	})
	t.Run("an unrecordable acceptance stores nothing", func(t *testing.T) {
		service, presenter, recorder := setup()
		recorder.err = errors.New("down")
		_, err := service.Present(caller(nil, auth.OperationExecute), presentation)
		if !shoal.IsErrorCode(err, shoal.ErrorUnavailable) || len(presenter.recorded) != 0 {
			t.Fatalf("err = %v, recorded = %d", err, len(presenter.recorded))
		}
	})
	t.Run("a refusal is opaque, audited with its reason, and stands", func(t *testing.T) {
		service, presenter, recorder := setup()
		presenter.verifyErr = &AttestationRefusal{Reason: "signature"}
		recorder.err = errors.New("down")
		_, err := service.Present(caller(nil, auth.OperationExecute), presentation)
		refused(t, err)
		if len(recorder.audits) != 1 || recorder.audits[0].Phase != "attestation_refused" ||
			recorder.audits[0].Reason != "signature" || len(presenter.recorded) != 0 {
			t.Fatalf("audits = %+v", recorder.audits)
		}
	})
	t.Run("a rollback at record time is refused", func(t *testing.T) {
		service, presenter, recorder := setup()
		presenter.recordErr = &AttestationRefusal{Reason: "rollback"}
		_, err := service.Present(caller(nil, auth.OperationExecute), presentation)
		refused(t, err)
		if last := recorder.audits[len(recorder.audits)-1]; last.Reason != "rollback" {
			t.Fatalf("audits = %+v", recorder.audits)
		}
	})
	t.Run("a delegated caller is refused", func(t *testing.T) {
		service, presenter, _ := setup()
		_, err := service.Present(caller([]shoal.ID{"delegator"}, auth.OperationExecute), presentation)
		refused(t, err)
		if len(presenter.verified) != 0 {
			t.Fatal("verified a delegated presentation")
		}
	})
	t.Run("a store failure is unavailable", func(t *testing.T) {
		service, presenter, _ := setup()
		presenter.recordErr = errors.New("down")
		_, err := service.Present(caller(nil, auth.OperationExecute), presentation)
		if !errors.Is(err, ErrAttestationUnavailable) || errors.Is(err, ErrAttestationRefused) {
			t.Fatalf("err = %v", err)
		}
	})
}
