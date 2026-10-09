// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package webapi

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	decisionapi "github.com/phrocker/shoal-oss/pkg/decision/api"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestRegistrationPathRejectsAmbiguousURLs(t *testing.T) {
	id := shoal.ID("decision-registration:" + strings.Repeat("a", 64))
	encoded := decisionapi.EncodeID(id)
	for _, tc := range []struct {
		name string
		path string
	}{
		{"query", RegistrationsRoute + "/" + encoded + "?x=1"},
		{"double", RegistrationsRoute + "/" + encoded + "/extra"},
		{"empty", RegistrationsRoute + "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if _, err := registrationPath(r); err == nil {
				t.Fatal("accepted ambiguous registration path")
			}
		})
	}
}

func TestRegistrationWireRejectsWriteAndReadConfusion(t *testing.T) {
	selection := decisionapi.RegistrationSelection{ProfileID: "profile", ProfileRevisionID: "revision", Sources: []decisionapi.RegistrationSourceInput{{ObservationID: shoal.ID("observation:" + strings.Repeat("a", 64)), Bytes: []byte("source")}}}
	body, err := decisionapi.EncodeRegistrationRequest(selection)
	if err != nil {
		t.Fatal(err)
	}
	get := httptest.NewRequest(http.MethodGet, RegistrationsRoute+"/"+decisionapi.EncodeID(shoal.ID("decision-registration:"+strings.Repeat("b", 64))), bytes.NewReader([]byte("body")))
	get.Header.Set("Idempotency-Key", decisionapi.EncodeKey([]byte("key")))
	if _, _, _, err := decodeRegistrationWire(get); err == nil {
		t.Fatal("accepted GET body/idempotency key")
	}
	post := httptest.NewRequest(http.MethodPost, RegistrationsRoute, bytes.NewReader(body))
	post.Header.Set("Content-Type", "application/json")
	if _, _, _, err := decodeRegistrationWire(post); err == nil {
		t.Fatal("accepted POST without idempotency key")
	}
	post.Header.Set("Idempotency-Key", decisionapi.EncodeKey([]byte("key")))
	post.Header.Set("Content-Encoding", "gzip")
	if _, _, _, err := decodeRegistrationWire(post); err == nil {
		t.Fatal("accepted compressed registration body")
	}
}
