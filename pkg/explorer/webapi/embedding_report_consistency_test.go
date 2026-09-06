package webapi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
)

func TestRetrievalResponseRejectsEmbeddingDisclosureFlagMismatch(t *testing.T) {
	for _, tc := range []struct {
		name       string
		suppressed uint32
		restricted uint32
		mutate     func(*authorized.EmbeddingQueryReport)
	}{
		{
			name: "suppressed count without flag", suppressed: 1,
			mutate: func(report *authorized.EmbeddingQueryReport) {
				report.Suppressed = false
			},
		},
		{
			name: "restricted flag without count",
			mutate: func(report *authorized.EmbeddingQueryReport) {
				report.Restricted = true
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := testEmbeddingReport()
			tc.mutate(&report)
			payload, err := json.Marshal(RetrievalResponse{
				Snapshot: Snapshot{
					ID:       "snapshot",
					AsOf:     time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
					Frontier: 1,
				},
				Retrieval:  retrieval.Response{},
				Suppressed: tc.suppressed,
				Restricted: tc.restricted,
				Embedding:  &report,
			})
			if err != nil {
				t.Fatal(err)
			}
			var decoded RetrievalResponse
			if err := json.Unmarshal(payload, &decoded); err == nil {
				t.Fatal("inconsistent embedding disclosure flags were accepted")
			}
		})
	}
}
