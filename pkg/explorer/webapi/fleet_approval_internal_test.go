// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package webapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// TestApprovalErrorsNeverSurfaceAsInternal maps every approval sentinel,
// bare and wrapped, through the transport. writeError starts from 500, so a
// sentinel that reached it without a code would read to a client as a server
// fault — and "approval required" is the one answer a client must branch on.
func TestApprovalErrorsNeverSurfaceAsInternal(t *testing.T) {
	for _, probe := range []struct {
		name   string
		mapped error
		status int
		code   shoal.ErrorCode
	}{
		{"bare approval required", fleetDispatchError(fleet.ErrApprovalRequired),
			http.StatusConflict, shoal.ErrorConflict},
		{"wrapped approval required", fleetDispatchError(errors.Join(
			errors.New("context"), fleet.ErrApprovalRequired)),
			http.StatusConflict, shoal.ErrorConflict},
		{"approval conflict", fleetApprovalError(fleet.ErrApprovalConflict),
			http.StatusConflict, shoal.ErrorConflict},
		{"approval expired", fleetApprovalError(fleet.ErrApprovalExpired),
			http.StatusConflict, shoal.ErrorConflict},
		{"approval superseded", fleetApprovalError(fleet.ErrApprovalSuperseded),
			http.StatusConflict, shoal.ErrorConflict},
		{"approval not found", fleetApprovalError(fleet.ErrApprovalNotFound),
			http.StatusNotFound, shoal.ErrorNotFound},
	} {
		recorder := httptest.NewRecorder()
		writeError(recorder, probe.mapped)
		var body struct {
			Code shoal.ErrorCode `json:"code"`
		}
		if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if recorder.Code != probe.status || body.Code != probe.code {
			t.Fatalf("%s = %d %q, want %d %q", probe.name,
				recorder.Code, body.Code, probe.status, probe.code)
		}
	}
}
