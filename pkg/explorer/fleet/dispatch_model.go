// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const (
	MaxActionIDBytes       = 256
	MaxActionPayloadBytes  = 1 << 20
	MaxActionOutputBytes   = 1 << 20
	MaxActionEvidence      = 256
	MaxActionErrorBytes    = 1024
	MaxActionClaimTTL      = 5 * time.Minute
	MaxActionDeadline      = 24 * time.Hour
	MaxDispatchListResults = 256
)

var (
	ErrActionNotFound       = errors.New("fleet dispatch: action not found")
	ErrActionConflict       = errors.New("fleet dispatch: action conflict")
	ErrClaimLost            = errors.New("fleet dispatch: claim lost")
	ErrActionTerminal       = errors.New("fleet dispatch: action is terminal")
	ErrExecutionAmbiguous   = errors.New("fleet dispatch: execution may have occurred")
	ErrActionCommitted      = errors.New("fleet dispatch: action state committed")
	ErrRecordingUnavailable = errors.New("fleet dispatch: recording is unavailable")
)

type DispatchState string

const (
	DispatchQueued    DispatchState = "queued"
	DispatchClaimed   DispatchState = "claimed"
	DispatchSucceeded DispatchState = "succeeded"
	DispatchFailed    DispatchState = "failed"
	DispatchCanceled  DispatchState = "canceled"
)

func (s DispatchState) terminal() bool {
	return s == DispatchSucceeded || s == DispatchFailed || s == DispatchCanceled
}

// EvidenceRef is a complete, bounded evidence anchor returned by a trusted
// executor. It preserves the same immutable identity as interaction evidence.
type EvidenceRef struct {
	AnchorID   shoal.ID
	Kind       interaction.EvidenceKind
	Citation   document.Citation
	NodeIDs    []shoal.ID
	EdgeIDs    []shoal.ID
	Assertions []interaction.AssertionReference
	Visibility []string
}

// ActionRecord is the durable source of truth for one dispatch.
type ActionRecord struct {
	ID                        []byte
	IdempotencyKey            []byte
	Version                   uint64
	State                     DispatchState
	AgentID                   shoal.ID
	AgentGeneration           int64
	Capability                string
	Action                    string
	SourceID                  []byte
	PolicyID                  []byte
	ObjectID                  shoal.ID
	Input                     json.RawMessage
	Output                    json.RawMessage
	ErrorCode                 string
	Subject                   shoal.ID
	Actor                     shoal.ID
	ClientID                  shoal.ID
	OnBehalfOf                []shoal.ID
	AuthorizationFingerprint  auth.Fingerprint
	PolicyGeneration          int64
	AuthorizationExpiresAt    time.Time
	ExecutionFingerprint      auth.Fingerprint
	ExecutionPolicyGeneration int64
	ExecutionExpiresAt        time.Time
	AuthorizedOperations      []auth.Operation
	RequestID                 shoal.ID
	CorrelationID             shoal.ID
	Reason                    interaction.Reason
	Deadline                  time.Time
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
	ClaimID                   []byte
	ClaimFence                uint64
	ClaimLease                time.Duration
	ClaimLeaseUntil           time.Time
	CancelKey                 []byte
	ExecutorKey               []byte
	EvidenceSnapshotID        shoal.ID
	EvidenceSnapshotAsOf      time.Time
	Evidence                  []EvidenceRef
	EffectPossible            bool
}

type EnqueueRequest struct {
	ID              []byte
	IdempotencyKey  []byte
	AgentID         shoal.ID
	AgentGeneration int64
	Capability      string
	Action          string
	SourceID        []byte
	PolicyID        []byte
	ObjectID        shoal.ID
	Input           json.RawMessage
	Context         RequestContext
}

type ClaimRequest struct {
	ID              []byte
	ExpectedVersion uint64
	// ClaimID is both the durable claimant identity and this mutation's
	// caller-supplied idempotency key.
	ClaimID []byte
	Lease   time.Duration
	Context RequestContext
}

type CancelRequest struct {
	ID              []byte
	ExpectedVersion uint64
	// MutationKey must be stable across retries of the same cancellation.
	MutationKey []byte
	Context     RequestContext
}

type StatusRequest struct {
	ID      []byte
	Context RequestContext
}

type PullActionsRequest struct {
	After   []byte
	Limit   int
	Context RequestContext
}

type ActionPage struct {
	Actions []ActionRecord
	Next    []byte
}

type InvokeRequest struct {
	Enqueue EnqueueRequest
	ClaimID []byte
	Lease   time.Duration
}

type Invocation struct {
	ActionID        []byte
	IdempotencyKey  []byte
	ClaimFence      uint64
	AgentID         shoal.ID
	AgentGeneration int64
	Capability      string
	Action          string
	SourceID        []byte
	PolicyID        []byte
	ObjectID        shoal.ID
	Input           json.RawMessage
	Subject         shoal.ID
	Actor           shoal.ID
	ClientID        shoal.ID
	OnBehalfOf      []shoal.ID
	RequestID       shoal.ID
	CorrelationID   shoal.ID
	Deadline        time.Time
}

type ExecutionResult struct {
	Output               json.RawMessage
	ErrorCode            string
	EvidenceSnapshotID   shoal.ID
	EvidenceSnapshotAsOf time.Time
	Evidence             []EvidenceRef
}

type ActionExecutor interface {
	Execute(context.Context, Invocation) (ExecutionResult, error)
}

type DispatchMutation struct {
	Token           []byte
	ExpectedVersion uint64
	ExpectedFence   uint64
	Record          ActionRecord
}

type DispatchStore interface {
	GetAction(context.Context, []byte) (ActionRecord, error)
	ApplyAction(context.Context, DispatchMutation) (ActionRecord, error)
	ScanActions(context.Context, []byte, int) (ActionPage, error)
}

type ActionAudit struct {
	Phase       string
	Operation   auth.Operation
	Record      ActionRecord
	EffectError error
}

type ActionRecorder interface {
	// RecordAction must be idempotent for phase, action ID, and record version.
	RecordAction(context.Context, ActionAudit) error
}

type ActionEventPublisher interface {
	// PublishActionEvent must be idempotent for kind, action ID, and version.
	PublishActionEvent(context.Context, string, ActionRecord) error
}

type DispatchConfig struct {
	Store    DispatchStore
	Registry *Service
	Resolver auth.Resolver
	Recorder ActionRecorder
	Events   ActionEventPublisher
	Clock    func() time.Time
}

func (r ActionRecord) Validate() error {
	if err := validateOpaque("action ID", r.ID, false); err != nil {
		return err
	}
	if err := validateOpaque("action idempotency key", r.IdempotencyKey, false); err != nil {
		return err
	}
	if r.Version == 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action version is required")
	}
	switch r.State {
	case DispatchQueued, DispatchClaimed, DispatchSucceeded, DispatchFailed, DispatchCanceled:
	default:
		return shoal.NewError(shoal.ErrorInvalidArgument, "action state is invalid")
	}
	if err := shoal.ValidateRequiredID("action agent ID", r.AgentID); err != nil {
		return err
	}
	if r.AgentGeneration <= 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action agent generation is invalid")
	}
	if err := validateName("capability", r.Capability); err != nil {
		return err
	}
	if err := validateName("action", r.Action); err != nil {
		return err
	}
	for name, value := range map[string][]byte{"action source": r.SourceID, "action policy": r.PolicyID} {
		if len(value) == 0 || len(value) > auth.MaxPolicyComponentBytes {
			return shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
		}
	}
	if err := shoal.ValidateRequiredID("action object ID", r.ObjectID); err != nil {
		return err
	}
	if _, _, err := validateJSONDocument("action input", r.Input, MaxActionPayloadBytes); err != nil {
		return err
	}
	if len(r.Output) > 0 {
		if _, _, err := validateJSONDocument("action output", r.Output, MaxActionOutputBytes); err != nil {
			return err
		}
	}
	if err := validateActionRecordError(r.ErrorCode); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID("action subject", r.Subject); err != nil {
		return err
	}
	if err := shoal.ValidateRequiredID("action actor", r.Actor); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID("action client", r.ClientID); err != nil {
		return err
	}
	if len(r.OnBehalfOf) > auth.MaxOnBehalfOfEntries {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action delegation chain exceeds its bound")
	}
	for _, identity := range r.OnBehalfOf {
		if err := shoal.ValidateRequiredID("action delegation identity", identity); err != nil {
			return err
		}
	}
	if r.PolicyGeneration <= 0 || r.AuthorizationExpiresAt.IsZero() {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action authorization provenance is incomplete")
	}
	if err := shoal.ValidateRequiredID("action request ID", r.RequestID); err != nil {
		return err
	}
	if err := shoal.ValidateOptionalID("action correlation ID", r.CorrelationID); err != nil {
		return err
	}
	if err := r.Reason.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]time.Time{
		"deadline": r.Deadline, "created time": r.CreatedAt, "updated time": r.UpdatedAt,
	} {
		if value.IsZero() || value.Location() != time.UTC {
			return shoal.NewError(shoal.ErrorInvalidArgument, "action "+name+" must be UTC")
		}
	}
	if r.UpdatedAt.Before(r.CreatedAt) || !r.CreatedAt.Before(r.Deadline) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action timestamps are inconsistent")
	}
	if len(r.AuthorizedOperations) == 0 {
		return shoal.NewError(shoal.ErrorInvalidArgument, "authorized action operations are required")
	}
	for i, operation := range r.AuthorizedOperations {
		if err := operation.Validate(); err != nil {
			return err
		}
		if i > 0 && r.AuthorizedOperations[i-1] >= operation {
			return shoal.NewError(shoal.ErrorInvalidArgument, "authorized action operations are not canonical")
		}
	}
	if err := validateOpaque("executor idempotency key", r.ExecutorKey, false); err != nil {
		return err
	}
	if r.State == DispatchCanceled {
		if err := validateOpaque("cancel mutation key", r.CancelKey, false); err != nil {
			return err
		}
	} else if len(r.CancelKey) != 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"non-canceled action carries a cancel mutation key",
		)
	}
	if r.State == DispatchQueued {
		if len(r.ClaimID) != 0 || r.ClaimFence != 0 ||
			r.ClaimLease != 0 || !r.ClaimLeaseUntil.IsZero() {
			return shoal.NewError(shoal.ErrorInvalidArgument, "queued action carries claim state")
		}
	} else if r.State != DispatchCanceled || r.ClaimFence != 0 {
		if err := validateOpaque("action claim ID", r.ClaimID, false); err != nil {
			return err
		}
		if r.ClaimFence == 0 || r.ClaimLease <= 0 ||
			r.ClaimLease > MaxActionClaimTTL || r.ClaimLeaseUntil.IsZero() ||
			r.ClaimLeaseUntil.Location() != time.UTC {
			return shoal.NewError(shoal.ErrorInvalidArgument, "action claim state is incomplete")
		}
	}
	if len(r.ClaimID) == 0 && r.ClaimLease != 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument, "action claim lease lacks a claim ID")
	}
	if (r.State == DispatchClaimed || r.State == DispatchSucceeded || r.State == DispatchFailed) &&
		(r.ExecutionPolicyGeneration <= 0 || r.ExecutionExpiresAt.IsZero()) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action execution authorization is incomplete")
	}
	if r.State == DispatchSucceeded && (len(r.Output) == 0 || !r.EffectPossible) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "successful action outcome is incomplete")
	}
	if r.State == DispatchFailed && (r.ErrorCode == "" || !r.EffectPossible) {
		return shoal.NewError(shoal.ErrorInvalidArgument, "failed action outcome is incomplete")
	}
	if len(r.Evidence) > 0 {
		if err := shoal.ValidateRequiredID(
			"action evidence snapshot ID", r.EvidenceSnapshotID); err != nil {
			return err
		}
		if r.EvidenceSnapshotAsOf.IsZero() {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"action evidence snapshot time is required")
		}
		if r.UpdatedAt.Before(r.EvidenceSnapshotAsOf) ||
			r.ExecutionExpiresAt.Before(r.EvidenceSnapshotAsOf) {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"action evidence snapshot time is outside execution bounds")
		}
	} else if r.EvidenceSnapshotID != "" ||
		!r.EvidenceSnapshotAsOf.IsZero() {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"action evidence snapshot requires evidence")
	}
	return validateEvidence(r.Evidence)
}

func validateActionRecordError(value string) error {
	if len(value) > MaxActionErrorBytes || strings.TrimSpace(value) != value {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action error code is invalid")
	}
	return nil
}

func validateOpaque(name string, value []byte, optional bool) error {
	if optional && len(value) == 0 {
		return nil
	}
	if len(value) == 0 || len(value) > MaxActionIDBytes {
		return shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
	}
	return nil
}

func validateEvidence(values []EvidenceRef) error {
	if len(values) > MaxActionEvidence {
		return shoal.NewError(shoal.ErrorInvalidArgument, "action evidence exceeds its bound")
	}
	totalMembers := 0
	for index, value := range values {
		reference, err := value.interactionReference().Canonical()
		if err != nil {
			return err
		}
		if len(value.Visibility) == 0 {
			return shoal.NewError(shoal.ErrorInvalidArgument, "evidence visibility is required")
		}
		normalized, err := interaction.Conjoin(value.Visibility)
		if err != nil {
			return err
		}
		if len(normalized) != len(value.Visibility) {
			return shoal.NewError(shoal.ErrorInvalidArgument, "evidence visibility must be canonical")
		}
		for i := range normalized {
			if normalized[i] != value.Visibility[i] {
				return shoal.NewError(shoal.ErrorInvalidArgument, "evidence visibility must be canonical")
			}
		}
		totalMembers += len(reference.NodeIDs) + len(reference.EdgeIDs) +
			len(reference.Assertions)
		if totalMembers > MaxActionEvidence {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"action evidence members exceed their bound")
		}
		for prior := 0; prior < index; prior++ {
			if equalEvidenceRef(values[prior], value) {
				return shoal.NewError(shoal.ErrorInvalidArgument, "action evidence contains duplicates")
			}
		}
	}
	return nil
}

func equalEvidenceRef(left, right EvidenceRef) bool {
	leftCanonical, leftErr := left.interactionReference().Canonical()
	rightCanonical, rightErr := right.interactionReference().Canonical()
	return leftErr == nil && rightErr == nil &&
		reflect.DeepEqual(leftCanonical, rightCanonical) &&
		len(left.Visibility) == len(right.Visibility) &&
		equalEvidenceVisibility(left.Visibility, right.Visibility)
}

func (r EvidenceRef) interactionReference() interaction.EvidenceReference {
	return interaction.EvidenceReference{
		AnchorID: r.AnchorID, Kind: r.Kind, Citation: r.Citation,
		NodeIDs:    append([]shoal.ID(nil), r.NodeIDs...),
		EdgeIDs:    append([]shoal.ID(nil), r.EdgeIDs...),
		Assertions: append([]interaction.AssertionReference(nil), r.Assertions...),
	}
}

func equalEvidenceVisibility(left, right []string) bool {
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validateJSONDocument(name string, raw json.RawMessage, limit int) (json.RawMessage, any, error) {
	if len(raw) == 0 || len(raw) > limit {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" is outside its byte bound")
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" is invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" has trailing JSON")
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > limit {
		return nil, nil, shoal.NewError(shoal.ErrorInvalidArgument, name+" exceeds its byte bound")
	}
	return encoded, value, nil
}

// validateAgainstSchema implements the bounded declarative subset supported by
// fleet descriptors: type, properties, required, items, enum, and
// additionalProperties. Unsupported schema keywords fail closed.
func validateAgainstSchema(schema json.RawMessage, document json.RawMessage, name string, limit int) (json.RawMessage, error) {
	canonical, value, err := validateJSONDocument(name, document, limit)
	if err != nil {
		return nil, err
	}
	var root map[string]any
	decoder := json.NewDecoder(bytes.NewReader(schema))
	decoder.UseNumber()
	if err := decoder.Decode(&root); err != nil {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "registered action schema is invalid")
	}
	if err := validateSchemaValue(root, value, "$", 0); err != nil {
		return nil, shoal.WrapError(shoal.ErrorInvalidArgument, name+" does not match its schema", err)
	}
	return canonical, nil
}

func validateSchemaValue(schema map[string]any, value any, path string, depth int) error {
	if depth > 32 {
		return fmt.Errorf("%s exceeds schema depth", path)
	}
	allowed := map[string]bool{"type": true, "properties": true, "required": true, "items": true, "enum": true, "additionalProperties": true}
	for key := range schema {
		if !allowed[key] {
			return fmt.Errorf("%s uses unsupported schema keyword %q", path, key)
		}
	}
	if rawType, exists := schema["type"]; exists {
		if _, ok := rawType.(string); !ok {
			return fmt.Errorf("%s type must be a string", path)
		}
	}
	if rawProperties, exists := schema["properties"]; exists {
		if _, ok := rawProperties.(map[string]any); !ok {
			return fmt.Errorf("%s properties must be an object", path)
		}
	}
	if rawRequired, exists := schema["required"]; exists {
		if _, ok := rawRequired.([]any); !ok {
			return fmt.Errorf("%s required must be an array", path)
		}
	}
	if rawItems, exists := schema["items"]; exists {
		if _, ok := rawItems.(map[string]any); !ok {
			return fmt.Errorf("%s items must be an object", path)
		}
	}
	if rawAdditional, exists := schema["additionalProperties"]; exists {
		if _, ok := rawAdditional.(bool); !ok {
			return fmt.Errorf("%s additionalProperties must be a boolean", path)
		}
	}
	if enums, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enums {
			left, _ := json.Marshal(candidate)
			right, _ := json.Marshal(value)
			if bytes.Equal(left, right) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is not an allowed value", path)
		}
	} else if _, exists := schema["enum"]; exists {
		return fmt.Errorf("%s enum must be an array", path)
	}
	kind, _ := schema["type"].(string)
	if kind == "" {
		if _, ok := schema["items"]; ok {
			kind = "array"
		} else if _, properties := schema["properties"]; properties {
			kind = "object"
		} else if _, required := schema["required"]; required {
			kind = "object"
		} else if _, additional := schema["additionalProperties"]; additional {
			kind = "object"
		}
	}
	switch kind {
	case "", "object", "array", "string", "number", "integer", "boolean", "null":
	default:
		return fmt.Errorf("%s has unsupported type %q", path, kind)
	}
	switch kind {
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s must be an object", path)
		}
		properties, _ := schema["properties"].(map[string]any)
		required, requiredOK := schema["required"].([]any)
		if _, exists := schema["required"]; exists && !requiredOK {
			return fmt.Errorf("%s required must be an array", path)
		}
		for _, entry := range required {
			key, ok := entry.(string)
			if !ok {
				return fmt.Errorf("%s required must contain strings", path)
			}
			if _, ok := object[key]; !ok {
				return fmt.Errorf("%s.%s is required", path, key)
			}
		}
		additional, hasAdditional := schema["additionalProperties"].(bool)
		for key, child := range object {
			rawChild, found := properties[key]
			if !found {
				if hasAdditional && !additional {
					return fmt.Errorf("%s.%s is not allowed", path, key)
				}
				continue
			}
			childSchema, ok := rawChild.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s schema is invalid", path, key)
			}
			if err := validateSchemaValue(childSchema, child, path+"."+key, depth+1); err != nil {
				return err
			}
		}
	case "array":
		array, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s must be an array", path)
		}
		if rawItems, ok := schema["items"]; ok {
			itemSchema, ok := rawItems.(map[string]any)
			if !ok {
				return fmt.Errorf("%s items schema is invalid", path)
			}
			for index, item := range array {
				if err := validateSchemaValue(itemSchema, item, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
					return err
				}
			}
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
	case "number":
		if _, ok := value.(json.Number); !ok {
			return fmt.Errorf("%s must be a number", path)
		}
	case "integer":
		number, ok := value.(json.Number)
		if !ok || strings.ContainsAny(number.String(), ".eE") {
			return fmt.Errorf("%s must be an integer", path)
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case "null":
		if value != nil {
			return fmt.Errorf("%s must be null", path)
		}
	}
	return nil
}

func cloneActionRecord(input ActionRecord) ActionRecord {
	result := input
	result.ID = append([]byte(nil), input.ID...)
	result.IdempotencyKey = append([]byte(nil), input.IdempotencyKey...)
	result.SourceID = append([]byte(nil), input.SourceID...)
	result.PolicyID = append([]byte(nil), input.PolicyID...)
	result.Input = append(json.RawMessage(nil), input.Input...)
	result.Output = append(json.RawMessage(nil), input.Output...)
	result.OnBehalfOf = append([]shoal.ID(nil), input.OnBehalfOf...)
	result.AuthorizedOperations = append([]auth.Operation(nil), input.AuthorizedOperations...)
	result.ClaimID = append([]byte(nil), input.ClaimID...)
	result.CancelKey = append([]byte(nil), input.CancelKey...)
	result.ExecutorKey = append([]byte(nil), input.ExecutorKey...)
	result.Evidence = make([]EvidenceRef, len(input.Evidence))
	for i, evidence := range input.Evidence {
		result.Evidence[i] = evidence
		result.Evidence[i].NodeIDs = append(
			[]shoal.ID(nil), evidence.NodeIDs...)
		result.Evidence[i].EdgeIDs = append(
			[]shoal.ID(nil), evidence.EdgeIDs...)
		result.Evidence[i].Assertions = append(
			[]interaction.AssertionReference(nil), evidence.Assertions...)
		result.Evidence[i].Visibility = append([]string(nil), evidence.Visibility...)
	}
	return result
}

func canonicalOperations(values []auth.Operation) []auth.Operation {
	result := append([]auth.Operation(nil), values...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	out := result[:0]
	for _, operation := range result {
		if len(out) == 0 || out[len(out)-1] != operation {
			out = append(out, operation)
		}
	}
	return out
}
