package model

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

const (
	DefaultFakeDimensions = 16
	maxFakeWorkBytes      = 64 << 20
)

type FakeGenerator struct {
	Model string
}

func (f FakeGenerator) CacheIdentity() (string, error) {
	name := strings.TrimSpace(f.Model)
	if name == "" {
		name = "deterministic"
	}
	return "model-fake-generator-v1:" + name, nil
}

func (f FakeGenerator) Generate(ctx context.Context, req GenerateRequest) (GenerateResult, error) {
	if err := ctx.Err(); err != nil {
		return GenerateResult{}, contextError("fake generate", err)
	}
	if err := validateTextRequest("fake generate", req.Prompt, req.MaxOutputTokens, DefaultMaxTextBytes); err != nil {
		return GenerateResult{}, &Error{Kind: ErrInvalidRequest, Operation: "fake generate"}
	}
	if isHarnessActionPrompt(req.Prompt) {
		return f.generateHarnessAction(req)
	}
	prompt := strings.ToLower(req.Prompt)
	parts := []string{"causal"}
	if strings.Contains(prompt, "entity") || strings.Contains(prompt, "user") || strings.Contains(prompt, "project") {
		parts = append(parts, "entity")
	}
	name := strings.TrimSpace(f.Model)
	if name == "" {
		name = "deterministic"
	}
	text := strings.Join(parts, ",")
	outputTokens := tokenEstimate(text)
	if req.MaxOutputTokens > 0 && outputTokens > req.MaxOutputTokens {
		return GenerateResult{}, &Error{Kind: ErrOversizedResponse, Operation: "fake generate"}
	}
	return GenerateResult{
		Text:       text,
		Provenance: Provenance{Provider: "fake", Model: name},
		Usage: Usage{
			InputTokens:  tokenEstimate(req.Prompt),
			OutputTokens: outputTokens,
			TotalTokens:  tokenEstimate(req.Prompt) + outputTokens,
		},
	}, nil
}

func (f FakeGenerator) generateHarnessAction(req GenerateRequest) (GenerateResult, error) {
	name := strings.TrimSpace(f.Model)
	if name == "" {
		name = "deterministic"
	}
	text := `{"action":"retrieve","correlation_id":"` + fakeProtocolID("fake-retrieve-1") + `","query":"entity","limit":1}`
	if !strings.Contains(req.Prompt, `"transcript":[]`) {
		evidenceID := firstHarnessEvidenceID(req.Prompt)
		if evidenceID == "" {
			text = `{"action":"stop","correlation_id":"` + fakeProtocolID("fake-stop") + `","unsupported":[{"input":"final claim","reason":"no evidence anchor was visible","evidence_ids":[]}]}`
		} else {
			payload := map[string]any{
				"action":         "stop",
				"correlation_id": fakeProtocolID("fake-stop"),
				"claims": []map[string]any{{
					"subject":      fakeProtocolID("entity:fake"),
					"predicate":    fakeProtocolID("predicate:summary"),
					"object":       map[string]any{"type": "string", "value": "grounded"},
					"confidence":   1,
					"evidence_ids": []string{evidenceID},
				}},
			}
			encoded, err := json.Marshal(payload)
			if err != nil {
				return GenerateResult{}, &Error{Kind: ErrMalformedResponse, Operation: "fake generate"}
			}
			text = string(encoded)
		}
	}
	outputTokens := tokenEstimate(text)
	if req.MaxOutputTokens > 0 && outputTokens > req.MaxOutputTokens {
		return GenerateResult{}, &Error{Kind: ErrOversizedResponse, Operation: "fake generate"}
	}
	return GenerateResult{
		Text:       text,
		Provenance: Provenance{Provider: "fake", Model: name},
		Usage: Usage{
			InputTokens:  tokenEstimate(req.Prompt),
			OutputTokens: outputTokens,
			TotalTokens:  tokenEstimate(req.Prompt) + outputTokens,
		},
	}, nil
}

func fakeProtocolID(id string) string {
	return base64.StdEncoding.EncodeToString([]byte(id))
}

func firstHarnessEvidenceID(prompt string) string {
	index := strings.Index(prompt, `"evidence":[{"id":"`)
	if index < 0 {
		return ""
	}
	start := index + len(`"evidence":[{"id":"`)
	end := strings.IndexByte(prompt[start:], '"')
	if end < 0 {
		return ""
	}
	return prompt[start : start+end]
}

func isHarnessActionPrompt(prompt string) bool {
	var envelope struct {
		Protocol string          `json:"protocol"`
		Tools    []string        `json:"tools"`
		Evidence json.RawMessage `json:"evidence"`
	}
	if err := json.Unmarshal([]byte(prompt), &envelope); err != nil {
		return false
	}
	if envelope.Protocol != "shoal-harness-action-json/v1" ||
		len(envelope.Tools) == 0 || len(envelope.Evidence) == 0 {
		return false
	}
	return true
}

type FakeEmbedder struct {
	Dimensions int
	Model      string
}

func (f FakeEmbedder) CacheIdentity() (string, error) {
	name := strings.TrimSpace(f.Model)
	if name == "" {
		name = "deterministic"
	}
	dim := f.Dimensions
	if dim == 0 {
		dim = DefaultFakeDimensions
	}
	return "model-fake-embedder-v1:" + name + ":" + strconv.Itoa(dim), nil
}

func (f FakeEmbedder) Embed(ctx context.Context, req EmbedRequest) (EmbedResult, error) {
	if err := ctx.Err(); err != nil {
		return EmbedResult{}, contextError("fake embed", err)
	}
	if err := validateTextRequest("fake embed", req.Text, 0, DefaultMaxTextBytes); err != nil {
		return EmbedResult{}, err
	}
	dim := f.Dimensions
	if dim == 0 {
		dim = DefaultFakeDimensions
	}
	if dim < 0 || dim > MaxVectorDimensions {
		return EmbedResult{}, &Error{Kind: ErrInvalidConfig, Operation: "fake embed"}
	}
	workPerDimension := len(req.Text) + 32
	if dim > maxFakeWorkBytes/workPerDimension {
		return EmbedResult{}, &Error{Kind: ErrInvalidRequest, Operation: "fake embed"}
	}
	vec := make([]float32, dim)
	text := strings.ToLower(req.Text)
	for i := range vec {
		h := sha256.Sum256([]byte(strconv.Itoa(i) + ":" + text))
		bits := binary.BigEndian.Uint32(h[:4])
		vec[i] = float32(bits%2000000)/1000000.0 - 1.0
		if err := ctx.Err(); err != nil {
			return EmbedResult{}, contextError("fake embed", err)
		}
	}
	normalize(vec)
	name := strings.TrimSpace(f.Model)
	if name == "" {
		name = "deterministic"
	}
	return EmbedResult{
		Vector:     append([]float32(nil), vec...),
		Provenance: Provenance{Provider: "fake", Model: name},
		Usage:      Usage{InputTokens: tokenEstimate(req.Text), TotalTokens: tokenEstimate(req.Text)},
	}, nil
}

func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x * x)
	}
	if sum == 0 {
		return
	}
	n := float32(math.Sqrt(sum))
	for i := range v {
		v[i] /= n
	}
}

func tokenEstimate(text string) int {
	if text == "" {
		return 0
	}
	return (len([]byte(text)) + 3) / 4
}
