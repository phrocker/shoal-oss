// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package api

import (
	"encoding/json"
	"testing"
)

// TestDecodeRecordKeepsTheWithheldMarker pins the SDK's half of #696.
//
// A succeeded record whose Output was withheld carries OutputWithheld to say
// so. An SDK that decodes the field away hands its client the exact state the
// marker exists to rule out — succeeded, no output, no explanation — which a
// client then has to decode as a redaction, which is the inference the marker
// replaced.
//
// recordWire and both decode paths have to carry it. The field being absent
// from any one of them is silent: the client simply sees false.
func TestDecodeRecordKeepsTheWithheldMarker(t *testing.T) {
	for _, probe := range []struct {
		name     string
		action   Action
		withheld bool
		output   string
	}{
		{
			name: "withheld on a succeeded record",
			action: Action{
				ID: "YQ", State: "succeeded", OutputWithheld: true,
			},
			withheld: true,
		},
		{
			// The ordinary case must not gain a marker.
			name: "an output that was served",
			action: Action{
				ID: "YQ", State: "succeeded", Output: []byte(`{"ok":true}`),
			},
			output: `{"ok":true}`,
		},
	} {
		t.Run(probe.name, func(t *testing.T) {
			record, err := DecodeRecord(probe.action)
			if err != nil {
				t.Fatal(err)
			}
			if record.OutputWithheld != probe.withheld {
				t.Fatalf("OutputWithheld = %v, want %v: an SDK client that "+
					"loses the marker sees a succeeded record with no "+
					"output and no reason given",
					record.OutputWithheld, probe.withheld)
			}
			if string(record.Output) != probe.output {
				t.Fatalf("Output = %q, want %q",
					record.Output, probe.output)
			}
		})
	}
}

// TestTheWithheldMarkerSurvivesJSON pins the wire tag, which the decoder test
// above does not: DecodeRecord builds recordWire in Go, so renaming
// `json:"output_withheld"` leaves it passing while every client reading a
// real response silently loses the marker. recordWire is unmarshalled
// directly on the client paths (client.go:368 and :447), so the tag is what
// carries it.
func TestTheWithheldMarkerSurvivesJSON(t *testing.T) {
	const body = `{"id":"YQ","state":"succeeded","output_withheld":true}`
	var wire recordWire
	if err := json.Unmarshal([]byte(body), &wire); err != nil {
		t.Fatal(err)
	}
	if !wire.OutputWithheld {
		t.Fatalf("the wire did not decode output_withheld from %s; the tag "+
			"is what every client reads", body)
	}
	record, err := decodeRecord(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !record.OutputWithheld {
		t.Fatal("the marker was decoded from JSON and then dropped on the " +
			"way to the record")
	}

	// And it is omitted when false, so an ordinary record's wire is
	// unchanged by the field existing.
	encoded, err := json.Marshal(recordWire{ID: "YQ", State: "succeeded"})
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "" && contains(string(encoded), "output_withheld") {
		t.Fatalf("an unredacted record carries the marker: %s", encoded)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
