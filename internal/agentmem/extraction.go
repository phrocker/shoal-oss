package agentmem

import (
	"context"
	"errors"

	"github.com/phrocker/shoal-oss/pkg/extraction"
)

var ErrOntologyExtractionUnavailable = errors.New("agentmem: ontology extraction is not configured")

type OntologyExtractor interface {
	Extract(context.Context, extraction.Request) (extraction.Result, error)
}

type OntologyRequestFactory func(context.Context, string) (extraction.Request, error)

// StructuredEnricher adapts the ontology-guided extractor to the legacy
// agentmem entity shape. Publication remains the caller's responsibility.
type StructuredEnricher struct {
	Extractor      OntologyExtractor
	RequestFactory OntologyRequestFactory
	Summarizer     Enricher
}

func (e StructuredEnricher) Entities(ctx context.Context, text string) ([]Entity, error) {
	if e.Extractor == nil || e.RequestFactory == nil {
		return nil, ErrOntologyExtractionUnavailable
	}
	request, err := e.RequestFactory(ctx, text)
	if err != nil {
		return nil, err
	}
	result, err := e.Extractor.Extract(ctx, request)
	if err != nil {
		return nil, err
	}
	plan := result.PublicationPlan()
	entities := make([]Entity, 0, len(plan.Entities))
	for _, proposed := range plan.Entities {
		entities = append(entities, Entity{
			ID: string(proposed.ID), Label: proposed.Key, Type: string(proposed.TypeID),
		})
	}
	return entities, nil
}

func (e StructuredEnricher) Summarize(ctx context.Context, text string) (string, error) {
	if e.Summarizer != nil {
		return e.Summarizer.Summarize(ctx, text)
	}
	return HeuristicEnricher{}.Summarize(ctx, text)
}
