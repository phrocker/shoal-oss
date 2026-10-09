// Package laya defines a pinned typed-prediction contract for a local Laya
// worker. It deliberately does not start, provision, or download the worker.
package laya

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	maxAliases               = 64
	maxOptions               = 128
	maxAliasBytes            = 256
	maxConfiguredInputBytes  = 8 << 20
	maxConfiguredOutputBytes = 1 << 20
	maxConfiguredConcurrent  = 128
)

var (
	ErrInvalidConfig     = errors.New("laya: invalid configuration")
	ErrInvalidRequest    = errors.New("laya: invalid request")
	ErrUnknownAlias      = errors.New("laya: unknown model alias")
	ErrMalformedResponse = errors.New("laya: malformed worker response")
	ErrIdentityMismatch  = errors.New("laya: effective identity mismatch")
	ErrOversizedInput    = errors.New("laya: oversized input")
	ErrOversizedOutput   = errors.New("laya: oversized output")
	ErrWorkerFailure     = errors.New("laya: worker failure")
	ErrSaturated         = errors.New("laya: worker concurrency limit reached")
)

// TaskKind identifies one of the supported typed prediction operations.
type TaskKind string

const (
	TaskChoice                 TaskKind = "choice"
	TaskScore                  TaskKind = "score"
	TaskPropositionProbability TaskKind = "proposition_probability"
)

// Identity pins the package, selected checkpoint, model artifacts, and
// behavior-bearing runtime configuration used by a worker.
type Identity struct {
	PackageVersion      string
	Checkpoint          string
	Revision            string
	WeightsSHA256       string
	TokenizerSHA256     string
	ConfigurationSHA256 string
	CalibrationSHA256   string
	RuntimeVersion      string
	BatchSize           int
}

// Config sets the exact aliases accepted by the adapter and bounded serving
// behavior. All fields participate in CacheIdentity.
type Config struct {
	Identity             Identity
	Aliases              []string
	MaxInputBytes        int
	MaxOutputBytes       int
	MaxConcurrent        int
	ProbabilityTolerance float64
}

// Request contains the text to evaluate. Choice requests additionally require
// distinct Options; other task kinds reject Options.
type Request struct {
	ModelAlias string
	Kind       TaskKind
	Input      string
	Options    []string
}

// ChoiceAnswer returns the selected option and the worker's native
// probabilities. Probabilities are validated but never normalized.
type ChoiceAnswer struct {
	Selected      string
	Probabilities []OptionProbability
}

type OptionProbability struct {
	Option      string
	Probability float64
}

// ScoreAnswer carries a finite task score. Scores are not probabilities and
// are therefore not constrained to [0,1].
type ScoreAnswer struct {
	Score float64
}

// PropositionProbabilityAnswer is specifically P(true), not confidence or
// answer_confidence.
type PropositionProbabilityAnswer struct {
	PTrue float64
}

// Result contains exactly one answer matching Kind.
type Result struct {
	ModelAlias             string
	Kind                   TaskKind
	EffectiveIdentity      string
	Choice                 *ChoiceAnswer
	Score                  *ScoreAnswer
	PropositionProbability *PropositionProbabilityAnswer
}

// WorkerRequest gives the worker the expected pinned identity for the call.
type WorkerRequest struct {
	ModelAlias        string
	Kind              TaskKind
	Input             string
	Options           []string
	EffectiveIdentity string
}

// WorkerResponse is the typed response envelope implemented by a local
// worker bridge. A bridge decoding JSON should reject unknown fields before
// constructing this value.
type WorkerResponse struct {
	ModelAlias             string
	Kind                   TaskKind
	EffectiveIdentity      string
	Choice                 *ChoiceAnswer
	Score                  *ScoreAnswer
	PropositionProbability *PropositionProbabilityAnswer
}

// Worker executes a request against the already-loaded local model. Its
// response identity must attest to the verified effective serving identity,
// not merely echo WorkerRequest.EffectiveIdentity.
type Worker interface {
	Predict(context.Context, WorkerRequest) (WorkerResponse, error)
}

// Predictor is the typed counterpart to model.TextGenerator.
type Predictor interface {
	Predict(context.Context, Request) (Result, error)
}

type Adapter struct {
	worker               Worker
	aliases              map[string]struct{}
	effectiveIdentity    string
	maxInputBytes        int
	maxOutputBytes       int
	probabilityTolerance float64
	slots                chan struct{}
}

func New(cfg Config, worker Worker) (*Adapter, error) {
	if isNilWorker(worker) || !validIdentity(cfg.Identity) ||
		cfg.MaxInputBytes < 1 || cfg.MaxInputBytes > maxConfiguredInputBytes ||
		cfg.MaxOutputBytes < 1 || cfg.MaxOutputBytes > maxConfiguredOutputBytes ||
		cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > maxConfiguredConcurrent ||
		math.IsNaN(cfg.ProbabilityTolerance) || math.IsInf(cfg.ProbabilityTolerance, 0) ||
		cfg.ProbabilityTolerance < 0 || cfg.ProbabilityTolerance > 0.05 ||
		len(cfg.Aliases) == 0 || len(cfg.Aliases) > maxAliases {
		return nil, ErrInvalidConfig
	}
	aliases := make(map[string]struct{}, len(cfg.Aliases))
	for _, alias := range cfg.Aliases {
		if !validAlias(alias) {
			return nil, ErrInvalidConfig
		}
		if _, exists := aliases[alias]; exists {
			return nil, ErrInvalidConfig
		}
		aliases[alias] = struct{}{}
	}
	identity, err := effectiveIdentity(cfg)
	if err != nil {
		return nil, ErrInvalidConfig
	}
	return &Adapter{
		worker:               worker,
		aliases:              aliases,
		effectiveIdentity:    identity,
		maxInputBytes:        cfg.MaxInputBytes,
		maxOutputBytes:       cfg.MaxOutputBytes,
		probabilityTolerance: cfg.ProbabilityTolerance,
		slots:                make(chan struct{}, cfg.MaxConcurrent),
	}, nil
}

// CacheIdentity identifies all pinned artifacts and adapter settings that
// can change prediction behavior.
func (a *Adapter) CacheIdentity() (string, error) {
	if a == nil || a.effectiveIdentity == "" {
		return "", ErrInvalidConfig
	}
	return "laya-local-v1:" + a.effectiveIdentity, nil
}

func (a *Adapter) Predict(ctx context.Context, req Request) (Result, error) {
	if a == nil || a.worker == nil || ctx == nil {
		return Result{}, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if _, ok := a.aliases[req.ModelAlias]; !ok {
		return Result{}, ErrUnknownAlias
	}
	if !validRequest(req) {
		return Result{}, ErrInvalidRequest
	}
	inputBytes := len(req.ModelAlias)
	if len(req.Input) > a.maxInputBytes-inputBytes {
		return Result{}, ErrOversizedInput
	}
	inputBytes += len(req.Input)
	for _, option := range req.Options {
		if len(option) > a.maxInputBytes-inputBytes {
			return Result{}, ErrOversizedInput
		}
		inputBytes += len(option)
	}
	if inputBytes > a.maxInputBytes {
		return Result{}, ErrOversizedInput
	}
	workerReq := WorkerRequest{
		ModelAlias:        req.ModelAlias,
		Kind:              req.Kind,
		Input:             req.Input,
		Options:           append([]string(nil), req.Options...),
		EffectiveIdentity: a.effectiveIdentity,
	}
	encodedRequest, err := json.Marshal(workerReq)
	if err != nil {
		return Result{}, ErrInvalidRequest
	}
	if len(encodedRequest) > a.maxInputBytes {
		return Result{}, ErrOversizedInput
	}

	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	default:
		return Result{}, ErrSaturated
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	response, workerErr := a.worker.Predict(ctx, workerReq)
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if workerErr != nil {
		return Result{}, ErrWorkerFailure
	}
	if err := a.validateResponse(req, response); err != nil {
		return Result{}, err
	}
	return Result{
		ModelAlias:             response.ModelAlias,
		Kind:                   response.Kind,
		EffectiveIdentity:      response.EffectiveIdentity,
		Choice:                 cloneChoice(response.Choice),
		Score:                  cloneScore(response.Score),
		PropositionProbability: clonePropositionProbability(response.PropositionProbability),
	}, nil
}

func (a *Adapter) validateResponse(req Request, response WorkerResponse) error {
	if response.ModelAlias != req.ModelAlias || response.Kind != req.Kind {
		return ErrMalformedResponse
	}
	if response.EffectiveIdentity != a.effectiveIdentity {
		return ErrIdentityMismatch
	}
	variantCount := 0
	for _, present := range []bool{
		response.Choice != nil,
		response.Score != nil,
		response.PropositionProbability != nil,
	} {
		if present {
			variantCount++
		}
	}
	if variantCount != 1 ||
		(req.Kind == TaskChoice) != (response.Choice != nil) ||
		(req.Kind == TaskScore) != (response.Score != nil) ||
		(req.Kind == TaskPropositionProbability) != (response.PropositionProbability != nil) {
		return ErrMalformedResponse
	}
	if err := validateOutputSize(response, a.maxOutputBytes); err != nil {
		return err
	}
	switch req.Kind {
	case TaskChoice:
		if err := validateChoice(req, response.Choice, a.probabilityTolerance); err != nil {
			return err
		}
	case TaskScore:
		if math.IsNaN(response.Score.Score) || math.IsInf(response.Score.Score, 0) {
			return ErrMalformedResponse
		}
	case TaskPropositionProbability:
		if !validProbability(response.PropositionProbability.PTrue) {
			return ErrMalformedResponse
		}
	default:
		return ErrMalformedResponse
	}
	return nil
}

func validateChoice(req Request, answer *ChoiceAnswer, tolerance float64) error {
	if answer == nil || !validRequiredText(answer.Selected) {
		return ErrMalformedResponse
	}
	optionSet := make(map[string]struct{}, len(req.Options))
	for _, option := range req.Options {
		optionSet[option] = struct{}{}
	}
	if _, ok := optionSet[answer.Selected]; !ok || len(answer.Probabilities) != len(req.Options) {
		return ErrMalformedResponse
	}
	seen := make(map[string]struct{}, len(answer.Probabilities))
	total := 0.0
	for _, item := range answer.Probabilities {
		if _, ok := optionSet[item.Option]; !ok || !validProbability(item.Probability) {
			return ErrMalformedResponse
		}
		if _, duplicate := seen[item.Option]; duplicate {
			return ErrMalformedResponse
		}
		seen[item.Option] = struct{}{}
		total += item.Probability
	}
	if math.Abs(total-1) > tolerance {
		return ErrMalformedResponse
	}
	return nil
}

func validateOutputSize(response WorkerResponse, limit int) error {
	textBytes := len(response.ModelAlias) + len(response.EffectiveIdentity)
	if textBytes > limit {
		return ErrOversizedOutput
	}
	count := func(value string) bool {
		if len(value) > limit-textBytes {
			return false
		}
		textBytes += len(value)
		return true
	}
	if response.Choice != nil {
		if !count(response.Choice.Selected) || len(response.Choice.Probabilities) > maxOptions {
			return ErrOversizedOutput
		}
		for _, item := range response.Choice.Probabilities {
			if !count(item.Option) {
				return ErrOversizedOutput
			}
		}
	}
	if response.Score != nil && math.IsNaN(response.Score.Score) {
		return ErrMalformedResponse
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return ErrMalformedResponse
	}
	if len(encoded) > limit {
		return ErrOversizedOutput
	}
	return nil
}

func validRequest(req Request) bool {
	if !validText(req.Input) || len(req.Input) == 0 {
		return false
	}
	switch req.Kind {
	case TaskChoice:
		if len(req.Options) < 2 || len(req.Options) > maxOptions {
			return false
		}
		seen := make(map[string]struct{}, len(req.Options))
		for _, option := range req.Options {
			if !validRequiredText(option) {
				return false
			}
			if _, exists := seen[option]; exists {
				return false
			}
			seen[option] = struct{}{}
		}
		return true
	case TaskScore, TaskPropositionProbability:
		return len(req.Options) == 0
	default:
		return false
	}
}

func validIdentity(identity Identity) bool {
	return validRequiredText(identity.PackageVersion) &&
		validRequiredText(identity.Checkpoint) &&
		validRequiredText(identity.Revision) &&
		validDigest(identity.WeightsSHA256) &&
		validDigest(identity.TokenizerSHA256) &&
		validDigest(identity.ConfigurationSHA256) &&
		validDigest(identity.CalibrationSHA256) &&
		validRequiredText(identity.RuntimeVersion) &&
		identity.BatchSize > 0 && identity.BatchSize <= 1024
}

func effectiveIdentity(cfg Config) (string, error) {
	aliases := append([]string(nil), cfg.Aliases...)
	sort.Strings(aliases)
	material := struct {
		Identity             Identity
		Aliases              []string
		MaxInputBytes        int
		MaxOutputBytes       int
		MaxConcurrent        int
		ProbabilityTolerance float64
	}{
		Identity:             cfg.Identity,
		Aliases:              aliases,
		MaxInputBytes:        cfg.MaxInputBytes,
		MaxOutputBytes:       cfg.MaxOutputBytes,
		MaxConcurrent:        cfg.MaxConcurrent,
		ProbabilityTolerance: cfg.ProbabilityTolerance,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validAlias(alias string) bool {
	if len(alias) == 0 || len(alias) > maxAliasBytes || !utf8.ValidString(alias) ||
		strings.TrimSpace(alias) != alias {
		return false
	}
	for _, r := range alias {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

func validRequiredText(value string) bool {
	if len(value) == 0 || len(value) > 1024 || !validText(value) || strings.TrimSpace(value) == "" {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r == 0 {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func validProbability(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1
}

func cloneChoice(answer *ChoiceAnswer) *ChoiceAnswer {
	if answer == nil {
		return nil
	}
	return &ChoiceAnswer{
		Selected:      answer.Selected,
		Probabilities: append([]OptionProbability(nil), answer.Probabilities...),
	}
}

func cloneScore(answer *ScoreAnswer) *ScoreAnswer {
	if answer == nil {
		return nil
	}
	return &ScoreAnswer{Score: answer.Score}
}

func clonePropositionProbability(answer *PropositionProbabilityAnswer) *PropositionProbabilityAnswer {
	if answer == nil {
		return nil
	}
	return &PropositionProbabilityAnswer{PTrue: answer.PTrue}
}

func isNilWorker(worker Worker) bool {
	if worker == nil {
		return true
	}
	value := reflect.ValueOf(worker)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ Predictor = (*Adapter)(nil)
