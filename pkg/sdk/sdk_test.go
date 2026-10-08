// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package sdk

import (
	"context"
	"testing"
)

func TestNewValidatesSharedConfiguration(t *testing.T) {
	token := func(context.Context) (string, error) { return "t", nil }
	if _, e := New(Config{BaseURL: "http://user@host", Token: token}); e == nil {
		t.Fatal("credentials in base URL accepted")
	}
	if _, e := New(Config{BaseURL: "https://shoal.example"}); e == nil {
		t.Fatal("missing token accepted")
	}
	c, e := New(Config{BaseURL: "https://shoal.example", Token: token})
	if e != nil || c.Collectors() == nil || c.Decisions() == nil {
		t.Fatalf("client: %v", e)
	}
	if ProtocolVersion != 1 {
		t.Fatal("protocol version changed without review")
	}
}
