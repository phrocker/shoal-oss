package agentmem

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultOllamaHost       = "http://localhost:11434"
	DefaultOllamaEmbedModel = "nomic-embed-text"
	DefaultOllamaLLMModel   = "llama3"
	DefaultOllamaTimeout    = 30 * time.Second
)

type OllamaOption func(*ollamaConfig)

type ollamaConfig struct {
	host    string
	model   string
	timeout time.Duration
	client  *http.Client
}

type OllamaEmbedder struct {
	Host    string
	Model   string
	Timeout time.Duration
	Client  *http.Client
}

type ollamaLLM struct {
	Host    string
	Model   string
	Timeout time.Duration
	Client  *http.Client
}

func WithOllamaHost(host string) OllamaOption {
	return func(c *ollamaConfig) {
		c.host = host
	}
}

func WithOllamaModel(model string) OllamaOption {
	return func(c *ollamaConfig) {
		c.model = model
	}
}

func WithOllamaTimeout(timeout time.Duration) OllamaOption {
	return func(c *ollamaConfig) {
		c.timeout = timeout
	}
}

func WithOllamaHTTPClient(client *http.Client) OllamaOption {
	return func(c *ollamaConfig) {
		c.client = client
	}
}

func NewOllamaEmbedder(opts ...OllamaOption) *OllamaEmbedder {
	cfg := defaultOllamaConfig(DefaultOllamaEmbedModel, opts...)
	return &OllamaEmbedder{Host: cfg.host, Model: cfg.model, Timeout: cfg.timeout, Client: cfg.client}
}

func NewOllamaLLM(opts ...OllamaOption) LLM {
	cfg := defaultOllamaConfig(DefaultOllamaLLMModel, opts...)
	return &ollamaLLM{Host: cfg.host, Model: cfg.model, Timeout: cfg.timeout, Client: cfg.client}
}

func (o *OllamaEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	host := ollamaHost(o.Host)
	model := strings.TrimSpace(o.Model)
	if model == "" {
		model = DefaultOllamaEmbedModel
	}
	var out struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := o.post(ctx, host+"/api/embeddings", map[string]string{
		"model":  model,
		"prompt": text,
	}, &out); err != nil {
		return nil, err
	}
	vec := make([]float32, len(out.Embedding))
	for i, v := range out.Embedding {
		vec[i] = float32(v)
	}
	return vec, nil
}

func (o *ollamaLLM) Infer(ctx context.Context, prompt string) (string, error) {
	host := ollamaHost(o.Host)
	model := strings.TrimSpace(o.Model)
	if model == "" {
		model = DefaultOllamaLLMModel
	}
	var out struct {
		Response string `json:"response"`
	}
	if err := o.post(ctx, host+"/api/generate", map[string]any{
		"model":  model,
		"prompt": prompt,
		"stream": false,
	}, &out); err != nil {
		return "", err
	}
	return out.Response, nil
}

func (o *OllamaEmbedder) post(ctx context.Context, url string, payload any, out any) error {
	return ollamaPost(ctx, o.Client, o.Timeout, url, payload, out)
}

func (o *ollamaLLM) post(ctx context.Context, url string, payload any, out any) error {
	return ollamaPost(ctx, o.Client, o.Timeout, url, payload, out)
}

func defaultOllamaConfig(model string, opts ...OllamaOption) ollamaConfig {
	cfg := ollamaConfig{host: DefaultOllamaHost, model: model, timeout: DefaultOllamaTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.host = ollamaHost(cfg.host)
	if strings.TrimSpace(cfg.model) == "" {
		cfg.model = model
	}
	return cfg
}

func ollamaHost(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		host = DefaultOllamaHost
	}
	return strings.TrimRight(host, "/")
}

func ollamaPost(ctx context.Context, client *http.Client, timeout time.Duration, url string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("ollama: encode request: %w", err)
	}
	if timeout <= 0 {
		timeout = DefaultOllamaTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ollama: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ollama: post %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("ollama: post %s: status %s: %s", url, resp.Status, strings.TrimSpace(string(snippet)))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("ollama: decode response from %s: %w", url, err)
	}
	return nil
}
