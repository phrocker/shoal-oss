// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package api

import "testing"

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
