package model

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	DefaultOllamaBaseURL       = "http://localhost:11434"
	DefaultOllamaEmbedModel    = "nomic-embed-text"
	DefaultOllamaGenerateModel = "llama3"
	DefaultTimeout             = 30 * time.Second
	DefaultMaxTextBytes        = 1 << 20
	DefaultMaxRequestBytes     = (1 << 20) + (16 << 10)
	DefaultMaxResponseBytes    = 4 << 20
	DefaultErrorSnippetBytes   = 512
	MaxVectorDimensions        = 1 << 20
	maxConfiguredTextBytes     = 16 << 20
	maxConfiguredRequestBytes  = 17 << 20
	maxConfiguredResponseBytes = 64 << 20
	maxConfiguredSnippetBytes  = 4 << 10
	maxConfiguredTimeout       = 10 * time.Minute
	maxConfiguredOutputTokens  = 1 << 20
	maxReportedUsageTokens     = 16 << 20
	maxModelBytes              = 256
)

type OllamaConfig struct {
	BaseURL             string
	Model               string
	HTTPClient          *http.Client
	Timeout             time.Duration
	MaxTextBytes        int64
	MaxRequestBytes     int64
	MaxResponseBytes    int64
	MaxVectorDimensions int
	ErrorSnippetBytes   int64
}

type OllamaGenerator struct {
	cfg      OllamaConfig
	endpoint string
}

type OllamaEmbedder struct {
	cfg      OllamaConfig
	endpoint string
}

func NewOllamaGenerator(cfg OllamaConfig) (*OllamaGenerator, error) {
	cfg, endpoint, err := validateOllamaConfig(cfg, "/api/generate")
	if err != nil {
		return nil, err
	}
	return &OllamaGenerator{cfg: cfg, endpoint: endpoint}, nil
}

func NewOllamaEmbedder(cfg OllamaConfig) (*OllamaEmbedder, error) {
	cfg, endpoint, err := validateOllamaConfig(cfg, "/api/embeddings")
	if err != nil {
		return nil, err
	}
	return &OllamaEmbedder{cfg: cfg, endpoint: endpoint}, nil
}

func (o *OllamaGenerator) Generate(ctx context.Context, req GenerateRequest) (GenerateResult, error) {
	if err := validateTextRequest("ollama generate", req.Prompt, req.MaxOutputTokens, o.cfg.MaxTextBytes); err != nil {
		return GenerateResult{}, err
	}
	payload := struct {
		Model   string `json:"model"`
		Prompt  string `json:"prompt"`
		Stream  bool   `json:"stream"`
		Options *struct {
			NumPredict int `json:"num_predict"`
		} `json:"options,omitempty"`
	}{Model: o.cfg.Model, Prompt: req.Prompt, Stream: false}
	if req.MaxOutputTokens > 0 {
		payload.Options = &struct {
			NumPredict int `json:"num_predict"`
		}{NumPredict: req.MaxOutputTokens}
	}
	var out struct {
		Response        *string `json:"response"`
		PromptEvalCount int     `json:"prompt_eval_count"`
		EvalCount       int     `json:"eval_count"`
	}
	if err := ollamaPost(ctx, o.cfg, o.endpoint, "ollama generate", payload, &out); err != nil {
		return GenerateResult{}, err
	}
	if out.Response == nil {
		return GenerateResult{}, &Error{Kind: ErrMalformedResponse, Operation: "ollama generate", Detail: "missing response"}
	}
	if int64(len(*out.Response)) > o.cfg.MaxTextBytes {
		return GenerateResult{}, &Error{Kind: ErrOversizedResponse, Operation: "ollama generate"}
	}
	if !validUsage(out.PromptEvalCount, out.EvalCount) {
		return GenerateResult{}, &Error{Kind: ErrMalformedResponse, Operation: "ollama generate", Detail: "invalid usage"}
	}
	if req.MaxOutputTokens > 0 && out.EvalCount > req.MaxOutputTokens {
		return GenerateResult{}, &Error{Kind: ErrOversizedResponse, Operation: "ollama generate", Detail: "output token limit exceeded"}
	}
	return GenerateResult{
		Text:       *out.Response,
		Provenance: Provenance{Provider: "ollama", Model: o.cfg.Model},
		Usage: Usage{
			InputTokens:  nonNegative(out.PromptEvalCount),
			OutputTokens: nonNegative(out.EvalCount),
			TotalTokens:  nonNegative(out.PromptEvalCount) + nonNegative(out.EvalCount),
		},
	}, nil
}

func (o *OllamaEmbedder) Embed(ctx context.Context, req EmbedRequest) (EmbedResult, error) {
	if err := validateTextRequest("ollama embed", req.Text, 0, o.cfg.MaxTextBytes); err != nil {
		return EmbedResult{}, err
	}
	payload := struct {
		Model  string `json:"model"`
		Prompt string `json:"prompt"`
	}{Model: o.cfg.Model, Prompt: req.Text}
	var out struct {
		Embedding       []float64 `json:"embedding"`
		PromptEvalCount int       `json:"prompt_eval_count"`
	}
	if err := ollamaPost(ctx, o.cfg, o.endpoint, "ollama embed", payload, &out); err != nil {
		return EmbedResult{}, err
	}
	if len(out.Embedding) == 0 {
		return EmbedResult{}, &Error{Kind: ErrMalformedResponse, Operation: "ollama embed", Detail: "empty vector"}
	}
	if len(out.Embedding) > o.cfg.MaxVectorDimensions {
		return EmbedResult{}, &Error{Kind: ErrOversizedResponse, Operation: "ollama embed", Detail: "vector exceeds configured dimensions"}
	}
	if !validUsage(out.PromptEvalCount) {
		return EmbedResult{}, &Error{Kind: ErrMalformedResponse, Operation: "ollama embed", Detail: "invalid usage"}
	}
	vec := make([]float32, len(out.Embedding))
	for i, value := range out.Embedding {
		if math.IsNaN(value) || math.IsInf(value, 0) || value > math.MaxFloat32 || value < -math.MaxFloat32 {
			return EmbedResult{}, &Error{Kind: ErrMalformedResponse, Operation: "ollama embed", Detail: "invalid vector value"}
		}
		vec[i] = float32(value)
	}
	usage := nonNegative(out.PromptEvalCount)
	return EmbedResult{
		Vector:     append([]float32(nil), vec...),
		Provenance: Provenance{Provider: "ollama", Model: o.cfg.Model},
		Usage:      Usage{InputTokens: usage, TotalTokens: usage},
	}, nil
}

func validateOllamaConfig(cfg OllamaConfig, path string) (OllamaConfig, string, error) {
	op := "configure ollama"
	cfg.BaseURL = strings.TrimSpace(cfg.BaseURL)
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.BaseURL == "" || cfg.Model == "" || len(cfg.Model) > maxModelBytes || strings.ContainsAny(cfg.Model, "\r\n\x00") {
		return OllamaConfig{}, "", &Error{Kind: ErrInvalidConfig, Operation: op}
	}
	u, err := url.Parse(cfg.BaseURL)
	if err != nil || !u.IsAbs() || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return OllamaConfig{}, "", &Error{Kind: ErrInvalidConfig, Operation: op}
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && isLoopbackHost(u.Hostname())) {
		return OllamaConfig{}, "", &Error{Kind: ErrInvalidConfig, Operation: op}
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	client := *cfg.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	cfg.HTTPClient = &client
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.MaxTextBytes == 0 {
		cfg.MaxTextBytes = DefaultMaxTextBytes
	}
	if cfg.MaxRequestBytes == 0 {
		cfg.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = DefaultMaxResponseBytes
	}
	if cfg.MaxVectorDimensions == 0 {
		cfg.MaxVectorDimensions = MaxVectorDimensions
	}
	if cfg.ErrorSnippetBytes == 0 {
		cfg.ErrorSnippetBytes = DefaultErrorSnippetBytes
	}
	if cfg.Timeout < time.Millisecond || cfg.Timeout > maxConfiguredTimeout ||
		cfg.MaxTextBytes < 1 || cfg.MaxTextBytes > maxConfiguredTextBytes ||
		cfg.MaxRequestBytes < 1 || cfg.MaxRequestBytes > maxConfiguredRequestBytes ||
		cfg.MaxResponseBytes < 1 || cfg.MaxResponseBytes > maxConfiguredResponseBytes ||
		cfg.MaxVectorDimensions < 1 || cfg.MaxVectorDimensions > MaxVectorDimensions ||
		cfg.ErrorSnippetBytes < 1 || cfg.ErrorSnippetBytes > maxConfiguredSnippetBytes {
		return OllamaConfig{}, "", &Error{Kind: ErrInvalidConfig, Operation: op}
	}
	u.Path = path
	return cfg, u.String(), nil
}

func validateTextRequest(op, text string, maxOutputTokens int, maxTextBytes int64) error {
	if !utf8.ValidString(text) || int64(len(text)) > maxTextBytes ||
		maxOutputTokens < 0 || maxOutputTokens > maxConfiguredOutputTokens {
		return &Error{Kind: ErrInvalidRequest, Operation: op}
	}
	return nil
}

func ollamaPost(ctx context.Context, cfg OllamaConfig, endpoint, op string, payload, out interface{}) error {
	if err := ctx.Err(); err != nil {
		return contextError(op, err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return &Error{Kind: ErrInvalidRequest, Operation: op}
	}
	if int64(len(body)) > cfg.MaxRequestBytes {
		return &Error{Kind: ErrInvalidRequest, Operation: op}
	}
	callCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return &Error{Kind: ErrInvalidConfig, Operation: op}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := cfg.HTTPClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return contextError(op, ctx.Err())
		}
		if callCtx.Err() != nil || isTimeout(err) {
			return &Error{Kind: ErrTimeout, Operation: op}
		}
		return &Error{Kind: ErrUnavailable, Operation: op}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		snippet, truncated, err := readSnippet(response.Body, cfg.ErrorSnippetBytes)
		if err != nil {
			return classifyTransportError(ctx, callCtx, op, err)
		}
		detail := fmt.Sprintf("response body redacted (%d bytes", len(snippet))
		if truncated {
			detail += "+"
		}
		detail += ")"
		return &Error{Kind: ErrUnavailable, Operation: op, StatusCode: response.StatusCode, Detail: detail}
	}
	limited := &io.LimitedReader{R: response.Body, N: cfg.MaxResponseBytes + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return classifyTransportError(ctx, callCtx, op, err)
	}
	if int64(len(data)) > cfg.MaxResponseBytes {
		return &Error{Kind: ErrOversizedResponse, Operation: op}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(out); err != nil {
		return &Error{Kind: ErrMalformedResponse, Operation: op}
	}
	var trailing interface{}
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return &Error{Kind: ErrMalformedResponse, Operation: op}
	}
	return nil
}

func readSnippet(r io.Reader, limit int64) ([]byte, bool, error) {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(data)) > limit {
		return data[:limit], true, nil
	}
	return data, false, nil
}

func contextError(op string, err error) error {
	if errors.Is(err, context.Canceled) {
		return &Error{Kind: ErrCanceled, Operation: op}
	}
	return &Error{Kind: ErrTimeout, Operation: op}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func classifyTransportError(ctx, callCtx context.Context, op string, err error) error {
	if ctx.Err() != nil {
		return contextError(op, ctx.Err())
	}
	if callCtx.Err() != nil || isTimeout(err) {
		return &Error{Kind: ErrTimeout, Operation: op}
	}
	return &Error{Kind: ErrUnavailable, Operation: op}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func nonNegative(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func validUsage(values ...int) bool {
	total := 0
	for _, value := range values {
		if value < 0 || value > maxReportedUsageTokens || total > maxReportedUsageTokens-value {
			return false
		}
		total += value
	}
	return true
}
