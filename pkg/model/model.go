// Package model defines provider-neutral, low-level model I/O contracts.
package model

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidConfig     = errors.New("model: invalid configuration")
	ErrInvalidRequest    = errors.New("model: invalid request")
	ErrUnavailable       = errors.New("model: provider unavailable")
	ErrTimeout           = fmt.Errorf("model: request timed out: %w", context.DeadlineExceeded)
	ErrCanceled          = fmt.Errorf("model: request canceled: %w", context.Canceled)
	ErrMalformedResponse = errors.New("model: malformed response")
	ErrOversizedResponse = errors.New("model: oversized response")
	ErrCredential        = errors.New("model: credential unavailable")
	ErrAuthentication    = errors.New("model: authentication failed")
	ErrRateLimited       = errors.New("model: rate limited")
)

// TextGenerator generates text from a bounded provider-neutral request.
type TextGenerator interface {
	Generate(context.Context, GenerateRequest) (GenerateResult, error)
}

// Embedder creates a vector embedding from a bounded provider-neutral request.
type Embedder interface {
	Embed(context.Context, EmbedRequest) (EmbedResult, error)
}

type GenerateRequest struct {
	Prompt          string
	MaxOutputTokens int
}

type GenerateResult struct {
	Text       string
	Provenance Provenance
	Usage      Usage
}

type EmbedRequest struct {
	Text string
}

type EmbedResult struct {
	Vector     []float32
	Provenance Provenance
	Usage      Usage
}

type Provenance struct {
	Provider string
	Model    string
}

type Usage struct {
	InputTokens  int
	OutputTokens int
	TotalTokens  int
}

type Error struct {
	Kind       error
	Operation  string
	StatusCode int
	Detail     string
	Retryable  bool
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	msg := "model: " + e.Operation + ": " + e.Kind.Error()
	if e.StatusCode != 0 {
		msg += " (status " + statusText(e.StatusCode) + ")"
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Kind
}

func statusText(code int) string {
	const digits = "0123456789"
	if code == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for code > 0 {
		i--
		buf[i] = digits[code%10]
		code /= 10
	}
	return string(buf[i:])
}
