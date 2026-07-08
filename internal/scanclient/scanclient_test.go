package scanclient

import (
	"context"
	"strings"
	"testing"

	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
)

func TestDial_RejectsEmptyArgs(t *testing.T) {
	cases := []struct {
		name, addr, instance, version, want string
	}{
		{"no addr", "", "uuid", "4.0.0", "empty addr"},
		{"no instance", "host:9997", "", "4.0.0", "empty instanceID"},
		{"no version", "host:9997", "uuid", "", "empty accumuloVersion"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Dial(c.addr, c.instance, c.version)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error = %v, want substring %q", err, c.want)
			}
		})
	}
}

func TestDial_RejectsUnreachableAddr(t *testing.T) {
	// 127.0.0.1:1 — reserved low port that should be unreachable.
	_, err := Dial("127.0.0.1:1", "uuid", "4.0.0")
	if err == nil {
		t.Fatal("expected error connecting to 127.0.0.1:1, got nil")
	}
	if !strings.Contains(err.Error(), "open transport") {
		t.Errorf("error = %v, want substring %q", err, "open transport")
	}
}

func TestSimpleScan_RejectsNilFields(t *testing.T) {
	// Construct a Client with a nil transport just to exercise the
	// validation path. We never reach the wire so transport-nil is fine.
	c := &Client{}

	cases := []struct {
		name string
		req  SimpleScanRequest
		want string
	}{
		{"nil credentials", SimpleScanRequest{Extent: &data.TKeyExtent{}, Range: &data.TRange{}}, "nil Credentials"},
		{"nil extent", SimpleScanRequest{Credentials: &security.TCredentials{}, Range: &data.TRange{}}, "nil Extent"},
		{"nil range", SimpleScanRequest{Credentials: &security.TCredentials{}, Extent: &data.TKeyExtent{}}, "nil Range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.SimpleScan(context.Background(), tc.req)
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want substring %q", err, tc.want)
			}
		})
	}
}
