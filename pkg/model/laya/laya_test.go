package laya

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeWorker struct {
	predict func(context.Context, WorkerRequest) (WorkerResponse, error)
}

func (f fakeWorker) Predict(ctx context.Context, req WorkerRequest) (WorkerResponse, error) {
	return f.predict(ctx, req)
}

func defaultConfig() Config {
	return Config{
		Identity: Identity{
			PackageVersion:      "1.2.3",
			Checkpoint:          "laya-choice-v1",
			Revision:            "commit-abc123",
			WeightsSHA256:       strings.Repeat("a", 64),
			TokenizerSHA256:     strings.Repeat("b", 64),
			ConfigurationSHA256: strings.Repeat("c", 64),
			CalibrationSHA256:   strings.Repeat("d", 64),
			RuntimeVersion:      "python-3.12/torch-2.7",
			BatchSize:           4,
		},
		Aliases:              []string{"pinned-choice"},
		MaxInputBytes:        4096,
		MaxOutputBytes:       2048,
		MaxConcurrent:        2,
		ProbabilityTolerance: 0.002,
	}
}

func newTestAdapter(t *testing.T, cfg Config, worker Worker) *Adapter {
	t.Helper()
	adapter, err := New(cfg, worker)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func respondingWorker(fn func(WorkerRequest) WorkerResponse) Worker {
	return fakeWorker{predict: func(ctx context.Context, req WorkerRequest) (WorkerResponse, error) {
		return fn(req), nil
	}}
}

func responseFor(req WorkerRequest) WorkerResponse {
	response := WorkerResponse{
		ModelAlias:        req.ModelAlias,
		Kind:              req.Kind,
		EffectiveIdentity: req.EffectiveIdentity,
	}
	switch req.Kind {
	case TaskChoice:
		response.Choice = &ChoiceAnswer{
			Selected: "yes",
			Probabilities: []OptionProbability{
				{Option: "yes", Probability: 0.5004},
				{Option: "no", Probability: 0.5004},
			},
		}
	case TaskScore:
		response.Score = &ScoreAnswer{Score: -0.37}
	case TaskPropositionProbability:
		response.PropositionProbability = &PropositionProbabilityAnswer{PTrue: 0.73}
	}
	return response
}

func TestPredictTypedAnswersAndPreservesNativeProbabilities(t *testing.T) {
	adapter := newTestAdapter(t, defaultConfig(), respondingWorker(responseFor))
	tests := []struct {
		name  string
		req   Request
		check func(*testing.T, Result)
	}{
		{
			name: "choice",
			req:  Request{ModelAlias: "pinned-choice", Kind: TaskChoice, Input: "question", Options: []string{"yes", "no"}},
			check: func(t *testing.T, got Result) {
				if got.Choice == nil || got.Choice.Selected != "yes" ||
					len(got.Choice.Probabilities) != 2 ||
					got.Choice.Probabilities[0].Probability != 0.5004 ||
					got.Choice.Probabilities[1].Probability != 0.5004 {
					t.Fatalf("choice answer = %#v", got.Choice)
				}
			},
		},
		{
			name: "score",
			req:  Request{ModelAlias: "pinned-choice", Kind: TaskScore, Input: "candidate"},
			check: func(t *testing.T, got Result) {
				if got.Score == nil || got.Score.Score != -0.37 || got.Choice != nil {
					t.Fatalf("score answer = %#v", got)
				}
			},
		},
		{
			name: "proposition probability",
			req:  Request{ModelAlias: "pinned-choice", Kind: TaskPropositionProbability, Input: "claim"},
			check: func(t *testing.T, got Result) {
				if got.PropositionProbability == nil || got.PropositionProbability.PTrue != 0.73 ||
					got.Score != nil || got.Choice != nil {
					t.Fatalf("proposition answer = %#v", got)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := adapter.Predict(context.Background(), test.req)
			if err != nil {
				t.Fatal(err)
			}
			test.check(t, got)
			if got.ModelAlias != test.req.ModelAlias || got.Kind != test.req.Kind ||
				got.EffectiveIdentity == "" {
				t.Fatalf("result identity = %#v", got)
			}
		})
	}
}

func TestCacheIdentityPinsArtifactsAndBehaviorSettings(t *testing.T) {
	worker := respondingWorker(responseFor)
	base, err := New(defaultConfig(), worker)
	if err != nil {
		t.Fatal(err)
	}
	baseID, err := base.CacheIdentity()
	if err != nil {
		t.Fatal(err)
	}
	variants := map[string]func(*Config){
		"weights":       func(c *Config) { c.Identity.WeightsSHA256 = strings.Repeat("e", 64) },
		"tokenizer":     func(c *Config) { c.Identity.TokenizerSHA256 = strings.Repeat("e", 64) },
		"configuration": func(c *Config) { c.Identity.ConfigurationSHA256 = strings.Repeat("e", 64) },
		"calibration":   func(c *Config) { c.Identity.CalibrationSHA256 = strings.Repeat("e", 64) },
		"checkpoint":    func(c *Config) { c.Identity.Checkpoint = "other-checkpoint" },
		"revision":      func(c *Config) { c.Identity.Revision = "commit-def456" },
		"runtime":       func(c *Config) { c.Identity.RuntimeVersion = "python-3.13/torch-2.7" },
		"batch":         func(c *Config) { c.Identity.BatchSize++ },
		"alias":         func(c *Config) { c.Aliases = append(c.Aliases, "alternate-pinned-choice") },
		"input limit":   func(c *Config) { c.MaxInputBytes++ },
		"output limit":  func(c *Config) { c.MaxOutputBytes++ },
		"concurrency":   func(c *Config) { c.MaxConcurrent++ },
		"tolerance":     func(c *Config) { c.ProbabilityTolerance += 0.001 },
	}
	for name, mutate := range variants {
		t.Run(name, func(t *testing.T) {
			cfg := defaultConfig()
			mutate(&cfg)
			adapter, err := New(cfg, worker)
			if err != nil {
				t.Fatal(err)
			}
			got, err := adapter.CacheIdentity()
			if err != nil {
				t.Fatal(err)
			}
			if got == baseID {
				t.Fatal("behavior change did not change cache identity")
			}
		})
	}
}

func TestCacheIdentityIgnoresAliasOrder(t *testing.T) {
	cfg := defaultConfig()
	cfg.Aliases = []string{"pinned-choice", "alternate"}
	first, err := New(cfg, respondingWorker(responseFor))
	if err != nil {
		t.Fatal(err)
	}
	cfg.Aliases = []string{"alternate", "pinned-choice"}
	second, err := New(cfg, respondingWorker(responseFor))
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := first.CacheIdentity()
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := second.CacheIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if firstID != secondID {
		t.Fatalf("alias order changed identity: %q != %q", firstID, secondID)
	}
}

func TestPredictRejectsUnknownAliasAndMalformedTypedAnswers(t *testing.T) {
	calls := atomic.Int32{}
	adapter := newTestAdapter(t, defaultConfig(), fakeWorker{predict: func(_ context.Context, req WorkerRequest) (WorkerResponse, error) {
		calls.Add(1)
		return responseFor(req), nil
	}})
	req := Request{ModelAlias: "not-configured", Kind: TaskScore, Input: "candidate"}
	if _, err := adapter.Predict(context.Background(), req); !errors.Is(err, ErrUnknownAlias) {
		t.Fatalf("unknown alias error = %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("worker called for unknown alias")
	}

	req.ModelAlias = "pinned-choice"
	req.Kind = TaskChoice
	req.Options = []string{"yes", "no"}
	tests := []struct {
		name string
		edit func(WorkerRequest, *WorkerResponse)
		want error
	}{
		{"missing answer", func(_ WorkerRequest, r *WorkerResponse) { r.Choice = nil }, ErrMalformedResponse},
		{"extra answer", func(_ WorkerRequest, r *WorkerResponse) { r.Score = &ScoreAnswer{Score: 1} }, ErrMalformedResponse},
		{"wrong kind", func(_ WorkerRequest, r *WorkerResponse) { r.Kind = TaskScore }, ErrMalformedResponse},
		{"wrong alias", func(_ WorkerRequest, r *WorkerResponse) { r.ModelAlias = "other" }, ErrMalformedResponse},
		{"wrong revision identity", func(_ WorkerRequest, r *WorkerResponse) { r.EffectiveIdentity = strings.Repeat("0", 64) }, ErrIdentityMismatch},
		{"missing option", func(_ WorkerRequest, r *WorkerResponse) { r.Choice.Probabilities = r.Choice.Probabilities[:1] }, ErrMalformedResponse},
		{"extra option", func(_ WorkerRequest, r *WorkerResponse) {
			r.Choice.Probabilities = append(r.Choice.Probabilities, OptionProbability{Option: "other", Probability: 0})
		}, ErrMalformedResponse},
		{"duplicate option", func(_ WorkerRequest, r *WorkerResponse) { r.Choice.Probabilities[1].Option = "yes" }, ErrMalformedResponse},
		{"unknown selected option", func(_ WorkerRequest, r *WorkerResponse) { r.Choice.Selected = "maybe" }, ErrMalformedResponse},
		{"probability out of range", func(_ WorkerRequest, r *WorkerResponse) { r.Choice.Probabilities[0].Probability = 1.01 }, ErrMalformedResponse},
		{"sum outside tolerance", func(_ WorkerRequest, r *WorkerResponse) { r.Choice.Probabilities[0].Probability = 0.4 }, ErrMalformedResponse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := newTestAdapter(t, defaultConfig(), respondingWorker(func(workerReq WorkerRequest) WorkerResponse {
				response := responseFor(workerReq)
				test.edit(workerReq, &response)
				return response
			}))
			if _, err := adapter.Predict(context.Background(), req); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPredictRejectsNonfiniteAndOutOfRangeValues(t *testing.T) {
	tests := []struct {
		name string
		kind TaskKind
		edit func(*WorkerResponse)
	}{
		{"NaN score", TaskScore, func(r *WorkerResponse) { r.Score.Score = math.NaN() }},
		{"infinite score", TaskScore, func(r *WorkerResponse) { r.Score.Score = math.Inf(1) }},
		{"negative P true", TaskPropositionProbability, func(r *WorkerResponse) { r.PropositionProbability.PTrue = -0.01 }},
		{"P true over one", TaskPropositionProbability, func(r *WorkerResponse) { r.PropositionProbability.PTrue = 1.01 }},
		{"NaN P true", TaskPropositionProbability, func(r *WorkerResponse) { r.PropositionProbability.PTrue = math.NaN() }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter := newTestAdapter(t, defaultConfig(), respondingWorker(func(req WorkerRequest) WorkerResponse {
				response := responseFor(req)
				test.edit(&response)
				return response
			}))
			req := Request{ModelAlias: "pinned-choice", Kind: test.kind, Input: "value"}
			if _, err := adapter.Predict(context.Background(), req); !errors.Is(err, ErrMalformedResponse) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestPredictRejectsInvalidAndOversizedRequestsOrAnswers(t *testing.T) {
	cfg := defaultConfig()
	cfg.MaxInputBytes = 512
	cfg.MaxOutputBytes = 512
	adapter := newTestAdapter(t, cfg, respondingWorker(func(req WorkerRequest) WorkerResponse {
		response := responseFor(req)
		if req.Kind == TaskChoice {
			response.Choice.Selected = strings.Repeat("x", 1024)
		}
		return response
	}))
	for _, req := range []Request{
		{ModelAlias: "pinned-choice", Kind: TaskChoice, Input: "q", Options: []string{"", "no"}},
		{ModelAlias: "pinned-choice", Kind: TaskChoice, Input: "q", Options: []string{"yes", "yes"}},
		{ModelAlias: "pinned-choice", Kind: TaskScore, Input: "q", Options: []string{"unexpected"}},
	} {
		if _, err := adapter.Predict(context.Background(), req); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid request error = %v", err)
		}
	}
	if _, err := adapter.Predict(context.Background(), Request{
		ModelAlias: "pinned-choice", Kind: TaskScore, Input: strings.Repeat("q", 1024),
	}); !errors.Is(err, ErrOversizedInput) {
		t.Fatalf("oversized input error = %v", err)
	}
	if _, err := adapter.Predict(context.Background(), Request{
		ModelAlias: "pinned-choice", Kind: TaskChoice, Input: "q", Options: []string{"yes", "no"},
	}); !errors.Is(err, ErrOversizedOutput) {
		t.Fatalf("oversized output error = %v", err)
	}
}

func TestPredictReturnsWorkerFailure(t *testing.T) {
	adapter := newTestAdapter(t, defaultConfig(), fakeWorker{predict: func(context.Context, WorkerRequest) (WorkerResponse, error) {
		return WorkerResponse{}, errors.New("private worker detail")
	}})
	_, err := adapter.Predict(context.Background(), Request{
		ModelAlias: "pinned-choice", Kind: TaskScore, Input: "candidate",
	})
	if !errors.Is(err, ErrWorkerFailure) || strings.Contains(err.Error(), "private worker detail") {
		t.Fatalf("worker error = %v", err)
	}
}

func TestPredictHonorsCancellationAndDeadlineAfterWorkerReturns(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
		want error
	}{
		{
			name: "cancellation",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
			want: context.Canceled,
		},
		{
			name: "deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				return ctx, cancel
			},
			want: context.DeadlineExceeded,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			adapter := newTestAdapter(t, defaultConfig(), fakeWorker{predict: func(_ context.Context, req WorkerRequest) (WorkerResponse, error) {
				close(started)
				<-release
				return responseFor(req), nil
			}})
			ctx, cancel := test.ctx()
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := adapter.Predict(ctx, Request{
					ModelAlias: "pinned-choice", Kind: TaskScore, Input: "candidate",
				})
				done <- err
			}()
			<-started
			if test.name == "cancellation" {
				cancel()
			} else {
				time.Sleep(20 * time.Millisecond)
			}
			close(release)
			if err := <-done; !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPredictRejectsConcurrentSaturation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	cfg := defaultConfig()
	cfg.MaxConcurrent = 1
	adapter := newTestAdapter(t, cfg, fakeWorker{predict: func(_ context.Context, req WorkerRequest) (WorkerResponse, error) {
		close(started)
		<-release
		return responseFor(req), nil
	}})
	req := Request{ModelAlias: "pinned-choice", Kind: TaskScore, Input: "candidate"}
	done := make(chan error, 1)
	go func() {
		_, err := adapter.Predict(context.Background(), req)
		done <- err
	}()
	<-started
	if _, err := adapter.Predict(context.Background(), req); !errors.Is(err, ErrSaturated) {
		t.Fatalf("saturated error = %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
