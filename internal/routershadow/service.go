// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

// Package routershadow runs the language router in shadow mode (#500): it
// routes a caller's text to a proposal under the caller's current
// authorization, runs the lexical baseline beside it, and records both. Nothing
// it produces is executed, enqueued, invoked, evaluated or registered; a test
// walks this package's syntax and fails if it calls any of those.
package routershadow

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
	"github.com/phrocker/shoal-oss/pkg/extraction"
	"github.com/phrocker/shoal-oss/pkg/lexicon"
	"github.com/phrocker/shoal-oss/pkg/ontology"
	"github.com/phrocker/shoal-oss/pkg/router"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// MaxListPages bounds the fleet pages one enumeration reads. A caller who can
// see more descriptors than fit is refused, as for too many targets.
const MaxListPages = 64

// ReasonCode is the caller-asserted reason the router's fleet reads record.
const ReasonCode = "router_shadow"

// DecisionTarget is a host-provisioned decision profile the router may
// propose. It is visible to a caller only when the caller may invoke its
// task resource, exactly as decision registration checks.
type DecisionTarget struct {
	ProfileID         shoal.ID
	ProfileRevisionID shoal.ID
	TaskID            shoal.ID
	Name              string
	TaskResource      auth.ResourceRequest
	// SlotSchema is the router-side input schema, in the fleet subset.
	SlotSchema json.RawMessage
}

// OntologyBinding names the published ontology the lexicon bundle's lookup
// templates were derived from. Templates are visible only when the caller may
// see that published identity.
type OntologyBinding struct {
	Configured ontology.OntologyVersion
	Identity   ontology.OntologyIdentity
}

// Config wires the shadow router. Every dependency is the real authorized
// component; the router reads through them as the caller.
type Config struct {
	Client    *authorized.Client
	Fleet     *fleet.Service
	Resolver  auth.Resolver
	Lexicon   *lexicon.Bundle
	Grammars  *router.GrammarSet
	Decisions []DecisionTarget
	Ontology  *OntologyBinding
	Decider   *Decider
	Recorder  Recorder
	// HostKey keys the utterance HMAC. At least 32 bytes.
	HostKey []byte
	Clock   func() time.Time
}

// Service is the shadow router.
type Service struct {
	config Config
}

// New validates the configuration.
func New(config Config) (*Service, error) {
	if config.Client == nil || config.Fleet == nil || config.Resolver == nil || config.Lexicon == nil ||
		config.Decider == nil || config.Decider.Provider == nil || config.Recorder == nil ||
		config.Clock == nil || len(config.HostKey) < 32 {
		return nil, shoal.NewError(shoal.ErrorInvalidArgument, "router shadow dependencies are required")
	}
	config.HostKey = append([]byte(nil), config.HostKey...)
	config.Decisions = append([]DecisionTarget(nil), config.Decisions...)
	return &Service{config: config}, nil
}

// Route routes text for the caller bound to ctx, records the result, and
// returns the router's proposal. It never acts on it.
func (s *Service) Route(ctx context.Context, text string) (router.Proposal, error) {
	start := s.config.Clock()
	var latency StageLatency
	stage := func(into *int64, from time.Time) time.Time {
		now := s.config.Clock()
		*into = now.Sub(from).Nanoseconds()
		return now
	}
	decision, err := s.config.Resolver.Resolve(ctx)
	if err != nil {
		return router.Proposal{}, err
	}
	fingerprint, err := auth.AuthorizationFingerprint(decision)
	if err != nil {
		return router.Proposal{}, err
	}
	targets, err := s.visibleTargets(ctx, decision)
	if err != nil {
		return router.Proposal{}, err
	}
	catalog, err := router.NewCatalog(targets, s.config.Grammars)
	if err != nil {
		return router.Proposal{}, err
	}
	t := stage(&latency.EnumerateNS, start)

	tokens := lexicon.Tokenize(text)
	input := router.Input{Tokens: tokens, Catalog: catalog, NodeConcepts: map[shoal.ID]shoal.ID{}}
	if len(text) > authorized.MaxMentionBytes || len(tokens) > authorized.MaxMentionTokens {
		input.OutOfBounds = true
	} else if len(tokens) > 0 {
		input.Mentions, err = s.config.Client.ResolveMentions(ctx, s.config.Lexicon, text)
		if err != nil {
			return router.Proposal{}, err
		}
		if err := s.concepts(ctx, catalog, input.Mentions, input.NodeConcepts); err != nil {
			return router.Proposal{}, err
		}
	}
	t = stage(&latency.ResolveNS, t)

	analysis, err := router.Analyze(input)
	if err != nil {
		return router.Proposal{}, err
	}
	t = stage(&latency.AnalyzeNS, t)
	correlation := decision.CorrelationID()
	if correlation == "" {
		correlation = decision.RequestID()
	}
	decided, err := s.config.Decider.Decide(ctx, DecideInput{
		Analysis: analysis, PrincipalID: decision.Subject(), CorrelationID: correlation,
		AuthFingerprint: hex.EncodeToString(fingerprint[:]), AuthExpiresAt: decision.AuthenticationExpires(),
	})
	if err != nil {
		return router.Proposal{}, err
	}
	t = stage(&latency.DecideNS, t)
	baseline, err := analysis.Baseline()
	if err != nil {
		return router.Proposal{}, err
	}
	stage(&latency.BaselineNS, t)
	latency.TotalNS = s.config.Clock().Sub(start).Nanoseconds()

	record := Record{
		Version:         RecordVersion,
		RecordedAt:      s.config.Clock().UTC(),
		Principal:       decision.Subject(),
		AuthFingerprint: hex.EncodeToString(fingerprint[:]),
		UtteranceKey:    UtteranceKey(s.config.HostKey, decision.Subject(), fingerprint, tokens),
		TokenCount:      len(tokens),
		LexiconBundleID: s.config.Lexicon.ID().String(),
		Proposal:        decided.Proposal,
		Baseline:        baseline,
		Agree:           sameOutcome(decided.Proposal, baseline),
		Latency:         latency,
	}
	for _, m := range input.Mentions {
		record.Mentions = append(record.Mentions, MentionRecord{
			TokenSpan: m.TokenSpan, NodeIDs: append([]shoal.ID(nil), m.NodeIDs...), Ambiguous: m.Ambiguous,
		})
	}
	if err := s.config.Recorder.Record(ctx, record); err != nil {
		return router.Proposal{}, err
	}
	return decided.Proposal, nil
}

// visibleTargets enumerates what the caller can currently see: actions on
// every descriptor the fleet lists for it, decision profiles whose task
// resource it may invoke, and lookup templates when it may see the bundle's
// published ontology. A target it cannot see is absent exactly as one that
// does not exist. More than router.MaxTargets fails closed.
func (s *Service) visibleTargets(ctx context.Context, decision auth.Decision) ([]router.Target, error) {
	var targets []router.Target
	add := func(t router.Target) error {
		if len(targets) == router.MaxTargets {
			return router.ErrTooManyTargets
		}
		targets = append(targets, t)
		return nil
	}
	var cursor []byte
	for page := 0; ; page++ {
		if page == MaxListPages {
			return nil, router.ErrTooManyTargets
		}
		now := s.config.Clock().UTC()
		listed, err := s.config.Fleet.List(ctx, fleet.ListRequest{
			Context: fleet.RequestContext{
				RequestID: decision.RequestID(), CorrelationID: decision.CorrelationID(),
				ReasonCode: ReasonCode, Deadline: now.Add(30 * time.Second),
			},
			Cursor: cursor, Limit: fleet.MaxListResults,
		})
		if err != nil {
			return nil, err
		}
		for _, d := range listed.Descriptors {
			for _, c := range d.Capabilities {
				for _, a := range c.Actions {
					action := a
					if err := add(router.Target{
						Ref: router.TargetRef{Kind: router.KindAction, Action: &router.ActionRef{
							AgentID: d.ID, AgentGeneration: d.Generation, Capability: c.Name,
							Action: a.Name, RequiresApproval: a.RequiresApproval,
						}},
						Action: &action, Name: a.Name,
					}); err != nil {
						return nil, err
					}
				}
			}
		}
		if len(listed.Next) == 0 {
			break
		}
		cursor = listed.Next
	}
	now := s.config.Clock().UTC()
	for _, d := range s.config.Decisions {
		err := decision.AuthorizeObject(auth.OperationInvoke, d.TaskResource, now)
		if invisible(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := add(router.Target{
			Ref: router.TargetRef{Kind: router.KindDecision, Decision: &router.DecisionRef{
				ProfileID: d.ProfileID, ProfileRevisionID: d.ProfileRevisionID, TaskID: d.TaskID,
			}},
			SlotSchema: d.SlotSchema, Name: d.Name,
		}); err != nil {
			return nil, err
		}
	}
	if s.config.Ontology != nil {
		err := s.config.Client.AuthorizePublishedOntology(ctx, s.config.Ontology.Configured, s.config.Ontology.Identity, auth.OperationNeighborhood)
		switch {
		case invisible(err):
		case err != nil:
			return nil, err
		default:
			for _, t := range s.config.Lexicon.Templates() {
				if err := add(router.Target{
					Ref: router.TargetRef{Kind: router.KindLookup, Lookup: &router.LookupRef{
						TemplateID: t.ID, RelationKey: t.RelationKey, Direction: t.Direction.String(),
					}},
					SubjectConcepts: t.SubjectConcepts, Name: t.RelationKey,
				}); err != nil {
					return nil, err
				}
			}
		}
	}
	return targets, nil
}

// invisible is the answer for an object the caller may not see or that does
// not exist: the authorization layer gives both the same not-found or
// unauthorized shape, and the router treats both as absent.
func invisible(err error) bool {
	return shoal.IsErrorCode(err, shoal.ErrorNotFound) || shoal.IsErrorCode(err, shoal.ErrorUnauthorized)
}

// concepts reads the ontology concept of each unambiguous mentioned node,
// through the authorized neighborhood read, when a lookup is a candidate.
// Every mentioned node is one the caller may see, so the read depends only on
// visible nodes.
func (s *Service) concepts(ctx context.Context, catalog *router.Catalog, mentions []lexicon.Mention, into map[shoal.ID]shoal.ID) error {
	lookup := false
	for _, key := range catalog.Keys() {
		lookup = lookup || len(key) > 7 && key[:7] == "lookup:"
	}
	var ids []shoal.ID
	for _, m := range mentions {
		if !m.Ambiguous {
			ids = append(ids, m.NodeIDs[0])
		}
	}
	if !lookup || len(ids) == 0 {
		return nil
	}
	neighborhood, err := s.config.Client.Neighborhood(ctx, explorer.NeighborhoodRequest{NodeIDs: ids})
	if err != nil {
		return err
	}
	wanted := map[shoal.ID]bool{}
	for _, id := range ids {
		wanted[id] = true
	}
	for _, n := range neighborhood.Nodes {
		if wanted[n.ID] {
			if concept := n.Properties[extraction.GraphPropertyOntologyConceptID]; concept != "" {
				into[n.ID] = shoal.ID(concept)
			}
		}
	}
	return nil
}

func sameOutcome(a, b router.Proposal) bool {
	if a.Kind != b.Kind || string(a.Input) != string(b.Input) {
		return false
	}
	if (a.Target == nil) != (b.Target == nil) {
		return false
	}
	if a.Target != nil && a.Target.Key() != b.Target.Key() {
		return false
	}
	if len(a.Reasons) != len(b.Reasons) {
		return false
	}
	for i := range a.Reasons {
		if a.Reasons[i] != b.Reasons[i] {
			return false
		}
	}
	return true
}
