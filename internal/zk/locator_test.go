package zk

import (
	"strings"
	"testing"
)

func TestParseRootTabletMetadata_CurrentLocation(t *testing.T) {
	json := `{
		"version": 1,
		"columnValues": {
			"loc": {
				"/accumulo/abc/tservers/tserver-3:9997/lock-0000000123$deadbeef":
					"tserver-3.namespace.svc.cluster.local:9997"
			}
		}
	}`
	loc, err := parseRootTabletMetadata([]byte(json))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if loc == nil {
		t.Fatal("expected location, got nil")
	}
	if loc.HostPort != "tserver-3.namespace.svc.cluster.local:9997" {
		t.Errorf("HostPort = %q", loc.HostPort)
	}
	if !strings.HasSuffix(loc.Session, "$deadbeef") {
		t.Errorf("Session = %q, expected to end with $deadbeef", loc.Session)
	}
}

func TestParseRootTabletMetadata_NoLocation(t *testing.T) {
	// Tablet mid-move: "loc" absent, only "future" populated. Our V0 returns
	// (nil, nil) so the caller retries — we don't chase "future".
	json := `{
		"version": 1,
		"columnValues": {
			"future": {
				"sess1": "tserver-7:9997"
			}
		}
	}`
	loc, err := parseRootTabletMetadata([]byte(json))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc != nil {
		t.Errorf("expected nil location, got %+v", loc)
	}
}

func TestParseRootTabletMetadata_EmptyColumnValues(t *testing.T) {
	loc, err := parseRootTabletMetadata([]byte(`{"version":1,"columnValues":{}}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loc != nil {
		t.Errorf("expected nil location, got %+v", loc)
	}
}

func TestParseRootTabletMetadata_VersionMismatch(t *testing.T) {
	loc, err := parseRootTabletMetadata([]byte(`{"version":2,"columnValues":{}}`))
	if err == nil {
		t.Fatal("expected version error, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported RootTabletMetadata version") {
		t.Errorf("error = %v", err)
	}
	if loc != nil {
		t.Errorf("expected nil location on error, got %+v", loc)
	}
}

func TestParseRootTabletMetadata_MalformedJSON(t *testing.T) {
	_, err := parseRootTabletMetadata([]byte(`{not json`))
	if err == nil {
		t.Fatal("expected parse error, got nil")
	}
	if !strings.Contains(err.Error(), "parse RootTabletMetadata json") {
		t.Errorf("error = %v", err)
	}
}

func TestParseRootTabletMetadata_EmptyBytes(t *testing.T) {
	_, err := parseRootTabletMetadata(nil)
	if err == nil {
		t.Fatal("expected error on empty bytes, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error = %v", err)
	}
}

func TestParseRootTabletMetadata_MultipleLocations(t *testing.T) {
	// Java enforces at-most-one location across loc+future. If "loc" itself
	// has multiple entries, that's a bug elsewhere — we surface it.
	json := `{
		"version": 1,
		"columnValues": {
			"loc": {
				"sess1": "tserver-3:9997",
				"sess2": "tserver-5:9997"
			}
		}
	}`
	_, err := parseRootTabletMetadata([]byte(json))
	if err == nil {
		t.Fatal("expected error on multiple locations, got nil")
	}
	if !strings.Contains(err.Error(), "2 current locations") {
		t.Errorf("error = %v", err)
	}
}
