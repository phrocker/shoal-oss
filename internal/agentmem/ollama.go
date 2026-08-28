package agentmem

import (
	"context"
	"net/http"
	"strings"
	"time"

	modelio "github.com/phrocker/shoal-oss/pkg/model"
)

const (
	DefaultOllamaHost       = modelio.DefaultOllamaBaseURL
	DefaultOllamaEmbedModel = modelio.DefaultOllamaEmbedModel
	DefaultOllamaLLMModel   = modelio.DefaultOllamaGenerateModel
	DefaultOllamaTimeout    = modelio.DefaultTimeout
)

type OllamaOption func(*modelio.OllamaConfig)

type OllamaEmbedder struct {
	embedder modelio.Embedder
	err      error
}

type ollamaLLM struct {
	generator modelio.TextGenerator
	err       error
}

func WithOllamaHost(host string) OllamaOption {
	return func(c *modelio.OllamaConfig) { c.BaseURL = host }
}

func WithOllamaModel(model string) OllamaOption {
	return func(c *modelio.OllamaConfig) { c.Model = model }
}

func WithOllamaTimeout(timeout time.Duration) OllamaOption {
	return func(c *modelio.OllamaConfig) { c.Timeout = timeout }
}

func WithOllamaHTTPClient(client *http.Client) OllamaOption {
	return func(c *modelio.OllamaConfig) { c.HTTPClient = client }
}

func NewOllamaEmbedder(opts ...OllamaOption) *OllamaEmbedder {
	cfg := modelio.OllamaConfig{BaseURL: DefaultOllamaHost, Model: DefaultOllamaEmbedModel}
	for _, opt := range opts {
		opt(&cfg)
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultOllamaHost
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = DefaultOllamaEmbedModel
	}
	embedder, err := modelio.NewOllamaEmbedder(cfg)
	return &OllamaEmbedder{embedder: embedder, err: err}
}

func NewOllamaLLM(opts ...OllamaOption) LLM {
	cfg := modelio.OllamaConfig{BaseURL: DefaultOllamaHost, Model: DefaultOllamaLLMModel}
	for _, opt := range opts {
		opt(&cfg)
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		cfg.BaseURL = DefaultOllamaHost
	}
	if strings.TrimSpace(cfg.Model) == "" {
		cfg.Model = DefaultOllamaLLMModel
	}
	generator, err := modelio.NewOllamaGenerator(cfg)
	return &ollamaLLM{generator: generator, err: err}
}

func (o *OllamaEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if o.err != nil {
		return nil, o.err
	}
	result, err := o.embedder.Embed(ctx, modelio.EmbedRequest{Text: text})
	return append([]float32(nil), result.Vector...), err
}

func (o *ollamaLLM) Infer(ctx context.Context, prompt string) (string, error) {
	if o.err != nil {
		return "", o.err
	}
	result, err := o.generator.Generate(ctx, modelio.GenerateRequest{Prompt: prompt})
	return result.Text, err
}
