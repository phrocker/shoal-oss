package model

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
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

func (f FakeGenerator) Generate(ctx context.Context, req GenerateRequest) (GenerateResult, error) {
	if err := ctx.Err(); err != nil {
		return GenerateResult{}, contextError("fake generate", err)
	}
	if err := validateTextRequest("fake generate", req.Prompt, req.MaxOutputTokens, DefaultMaxTextBytes); err != nil {
		return GenerateResult{}, &Error{Kind: ErrInvalidRequest, Operation: "fake generate"}
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

type FakeEmbedder struct {
	Dimensions int
	Model      string
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
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return EmbedResult{}, contextError("fake embed", err)
			}
		}
		h := sha256.Sum256([]byte(strconv.Itoa(i) + ":" + text))
		bits := binary.BigEndian.Uint32(h[:4])
		vec[i] = float32(bits%2000000)/1000000.0 - 1.0
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
