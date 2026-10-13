// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package router

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

const dependsOutGrammar = `{
  "schema": "shoal.router.grammar/v1",
  "target": {"kind": "lookup", "template": "lookup:depends_on:out"},
  "cues": ["depend"],
  "patterns": ["what does {subject} depend on"],
  "slots": [{"name": "subject"}]
}`

const dependsInGrammar = `{
  "schema": "shoal.router.grammar/v1",
  "target": {"kind": "lookup", "template": "lookup:depends_on:in"},
  "cues": ["depends"],
  "patterns": ["what depends on {subject}"],
  "slots": [{"name": "subject"}]
}`

func dependsTarget(direction string) Target {
	return Target{
		Ref:             TargetRef{Kind: KindLookup, Lookup: &LookupRef{TemplateID: "lookup:depends_on:" + direction, RelationKey: "depends_on", Direction: direction}},
		SubjectConcepts: []shoal.ID{"concept:service"},
		Name:            "depends_on",
	}
}

func v2Of(t *testing.T, a *Analysis, key string) []float64 {
	t.Helper()
	vectors, err := a.FeaturesFor(FeatureSchemaV2ID)
	if err != nil {
		t.Fatal(err)
	}
	for i, k := range a.catalog.Keys() {
		if k == key {
			return vectors[i]
		}
	}
	t.Fatalf("no candidate %s", key)
	return nil
}

func TestStem(t *testing.T) {
	for word, want := range map[string]string{
		"restart": "restart", "restarts": "restart", "restarting": "restart", "restarted": "restart",
		"scale": "scal", "scaling": "scal", "scaled": "scal",
		"rolled": "roll", "rolling": "roll", "running": "run", "runs": "run",
		"dependencies": "dependency", "owns": "own", "owned": "own",
		"status": "status", "is": "is", "was": "was", "readiness": "readiness",
	} {
		if got := stem(word); got != want {
			t.Errorf("stem(%q) = %q, want %q", word, got, want)
		}
	}
}

// TestFeaturesV2ExtendV1: router.pair/v2 starts with router.pair/v1's nine
// features, unchanged, and both artefacts encode their own schema.
func TestFeaturesV2ExtendV1(t *testing.T) {
	w := newWorld(t)
	c := catalog(t, grammarSet(t, restartGrammar, riskGrammar, ownerGrammar), restartTarget("agent:a"), riskTarget(), ownerTarget())
	a := analyze(t, w, c, "restart payments in prod")
	v1 := a.Features()
	v2, err := a.FeaturesFor(FeatureSchemaV2ID)
	if err != nil {
		t.Fatal(err)
	}
	for i := range v1 {
		if len(v2[i]) != NumFeaturesV2 || NumFeaturesV2 != len(FeatureNamesV2) {
			t.Fatalf("v2 width %d", len(v2[i]))
		}
		for k := range v1[i] {
			if v2[i][k] != v1[i][k] {
				t.Fatalf("candidate %d feature %s: v2 %v, v1 %v", i, FeatureNames[k], v2[i][k], v1[i][k])
			}
		}
		for k, x := range v2[i] {
			if x < 0 || x > 1 {
				t.Fatalf("feature %s = %v outside [0, 1]", FeatureNamesV2[k], x)
			}
		}
	}
	for schema, width := range map[string]int{FeatureSchemaID: NumFeatures, FeatureSchemaV2ID: NumFeaturesV2} {
		artifact, err := a.FeatureArtifactFor(schema)
		if err != nil {
			t.Fatal(err)
		}
		var decoded struct {
			Schema   string `json:"feature_schema_id"`
			Subjects []struct {
				Features []float64 `json:"features"`
			} `json:"subjects"`
		}
		if err := json.Unmarshal(artifact.Bytes, &decoded); err != nil || decoded.Schema != schema || len(decoded.Subjects[0].Features) != width {
			t.Fatalf("%s artifact = %s (%v)", schema, artifact.Bytes, err)
		}
		if bytes.Contains(artifact.Bytes, []byte("payments")) || bytes.Contains(artifact.Bytes, []byte("restart")) {
			t.Fatalf("%s artifact carries text", schema)
		}
	}
	if !bytes.Equal(a.FeatureArtifact().Bytes, mustArtifact(t, a, FeatureSchemaID)) {
		t.Fatal("FeatureArtifact is not the router.pair/v1 artifact")
	}
	if _, err := a.FeatureArtifactFor("router.pair/v9"); err == nil {
		t.Fatal("an unknown feature schema was encoded")
	}
}

func mustArtifact(t *testing.T, a *Analysis, schema string) []byte {
	t.Helper()
	artifact, err := a.FeatureArtifactFor(schema)
	if err != nil {
		t.Fatal(err)
	}
	return artifact.Bytes
}

// TestPartialMatchIsOrderedAndSlotAnchored: a pattern matches in order with
// gaps, slots only over whole fitting mentions, so word order still tells
// the two directions of one relation apart where no pattern covers the text.
func TestPartialMatchIsOrderedAndSlotAnchored(t *testing.T) {
	w := newWorld(t)
	c := catalog(t, grammarSet(t, dependsOutGrammar, dependsInGrammar), dependsTarget("out"), dependsTarget("in"))
	a := analyze(t, w, c, "so what does payments actually depend on")
	out, in := v2Of(t, a, "lookup:lookup:depends_on:out"), v2Of(t, a, "lookup:lookup:depends_on:in")
	if out[FeaturePatternFull] != 0 || out[FeatureV2PartialFull] != 1 || out[FeatureV2PartialBest] != 1 {
		t.Fatalf("out: pattern_full %v, partial_full %v, partial_best %v", out[FeaturePatternFull], out[FeatureV2PartialFull], out[FeatureV2PartialBest])
	}
	if in[FeatureV2PartialBest] >= out[FeatureV2PartialBest] {
		t.Fatalf("in-direction partial %v not below out-direction %v", in[FeatureV2PartialBest], out[FeatureV2PartialBest])
	}
	b := analyze(t, w, c, "tell me what depends on payments")
	out, in = v2Of(t, b, "lookup:lookup:depends_on:out"), v2Of(t, b, "lookup:lookup:depends_on:in")
	if in[FeatureV2PartialFull] != 1 || out[FeatureV2PartialBest] >= in[FeatureV2PartialBest] {
		t.Fatalf("reversed order: in %v, out %v", in[FeatureV2PartialBest], out[FeatureV2PartialBest])
	}
	// A subject of the wrong concept anchors nothing: prod is an environment.
	e := analyze(t, w, c, "what does prod depend on")
	if got := v2Of(t, e, "lookup:lookup:depends_on:out"); got[FeatureV2PartialFull] != 0 || got[FeatureV2SlotFill] != 0 {
		t.Fatalf("environment subject: partial_full %v, slot_fill %v", got[FeatureV2PartialFull], got[FeatureV2SlotFill])
	}
}

// TestEvidenceAndStemFeatures: an inflected cue counts, evidence is compared
// across the visible candidates, and words no candidate knows are counted.
func TestEvidenceAndStemFeatures(t *testing.T) {
	w := newWorld(t)
	c := catalog(t, grammarSet(t, restartGrammar, riskGrammar, ownerGrammar), restartTarget("agent:a"), riskTarget(), ownerTarget())
	a := analyze(t, w, c, "payments restarted in prod")
	restart := v2Of(t, a, "action:ops/restart")
	if restart[FeatureCueAny] != 0 || restart[FeatureV2CueStem] != 1 {
		t.Fatalf("inflected cue: cue_any %v, cue_stem %v", restart[FeatureCueAny], restart[FeatureV2CueStem])
	}
	if restart[FeatureV2EvidenceTop] != 1 || restart[FeatureV2EvidenceLead] <= 0 {
		t.Fatalf("restart evidence: top %v, lead %v", restart[FeatureV2EvidenceTop], restart[FeatureV2EvidenceLead])
	}
	b := analyze(t, w, c, "how risky is restarting payments")
	risk, restart := v2Of(t, b, "decision:risk"), v2Of(t, b, "action:ops/restart")
	if risk[FeatureV2EvidenceTop] != 1 || restart[FeatureV2EvidenceDeficit] <= 0 || risk[FeatureV2EvidenceShare] <= restart[FeatureV2EvidenceShare] {
		t.Fatalf("risk %v, restart %v", risk, restart)
	}
	u := analyze(t, w, c, "frobnicate payments")
	if got := v2Of(t, u, "action:ops/restart"); got[FeatureV2Unknown] != 1 || got[FeatureV2EvidenceTop] != 0 {
		t.Fatalf("unknown word: unknown %v, top %v", got[FeatureV2Unknown], got[FeatureV2EvidenceTop])
	}
}

func TestSlotFillAndMentionFit(t *testing.T) {
	w := newWorld(t)
	c := catalog(t, grammarSet(t, restartGrammar, ownerGrammar), restartTarget("agent:a"), ownerTarget())
	for text, want := range map[string]float64{
		"restart payments in prod": 1,
		"restart payments":         0.5,
		"restart":                  0,
	} {
		if got := v2Of(t, analyze(t, w, c, text), "action:ops/restart")[FeatureV2SlotFill]; got != want {
			t.Errorf("%q: slot_fill %v, want %v", text, got, want)
		}
	}
	owner := func(text string) []float64 { return v2Of(t, analyze(t, w, c, text), "lookup:lookup:owned_by:out") }
	if got := owner("who owns payments"); got[FeatureV2SlotFill] != 1 || got[FeatureV2MentionFit] != 1 {
		t.Fatalf("service subject: %v", got)
	}
	// The lookup's subject concepts, from the graph-derived template, decide
	// fit: an environment is no owned_by:out subject.
	if got := owner("who owns prod"); got[FeatureV2SlotFill] != 0 || got[FeatureV2MentionFit] != 0 {
		t.Fatalf("environment subject: %v", got)
	}
}

// TestFillRefusesASurplusMention: a slot the pattern filled is ambiguous when
// another unused mention of the same kind could fill it.
func TestFillRefusesASurplusMention(t *testing.T) {
	w := newWorld(t)
	c := catalog(t, grammarSet(t, restartGrammar), restartTarget("agent:a"))
	a := analyze(t, w, c, "restart payments in prod and payments api")
	p, err := a.Propose(choose(a, "action:ops/restart"), a.Receipt())
	if err != nil {
		t.Fatal(err)
	}
	if p.Kind != KindAbstain || p.Reasons[0] != ReasonAmbiguousMention || len(p.Missing) != 1 || p.Missing[0] != "service" {
		t.Fatalf("proposal = %+v", p)
	}
	b := analyze(t, w, c, "restart payments in prod")
	if p, _ := b.Propose(choose(b, "action:ops/restart"), b.Receipt()); p.Kind != KindAction {
		t.Fatalf("single service: %+v", p)
	}
}

func TestTrainVectorsMatchesTrain(t *testing.T) {
	w := newWorld(t)
	c := catalog(t, grammarSet(t, restartGrammar, riskGrammar, ownerGrammar), restartTarget("agent:a"), riskTarget(), ownerTarget())
	var examples []Example
	var vectors []VectorExample
	for _, text := range []string{"restart payments in prod", "how risky is restarting payments", "who owns payments", "tell me a joke"} {
		a := analyze(t, w, c, text)
		for i, f := range a.Features() {
			match := i == 0 && text == "restart payments in prod"
			examples = append(examples, Example{Features: f, Match: match})
			vectors = append(vectors, VectorExample{Features: append([]float64(nil), f[:]...), Match: match})
		}
	}
	w1, b1 := Train(examples, DefaultTrainConfig())
	w2, b2 := TrainVectors(vectors, NumFeatures, DefaultTrainConfig())
	for k := range w1 {
		if w1[k] != w2[k] {
			t.Fatalf("weight %d: %v vs %v", k, w1[k], w2[k])
		}
	}
	if b1 != b2 {
		t.Fatalf("bias %v vs %v", b1, b2)
	}
	config := DefaultTrainConfig()
	config.InterceptOffset = -1
	if _, b3 := TrainVectors(vectors, NumFeatures, config); b3 != b1-1 {
		t.Fatalf("offset bias %v, want %v", b3, b1-1)
	}
	if DefaultTrainConfig().Digest() != "f980ade09baeca799f961069942255665dd9fe65f64ce189e3b5db705e4040a6" {
		t.Fatal("v1's recipe digest changed")
	}
}

func TestTaskPerFeatureSchema(t *testing.T) {
	v1, err := TaskSpec()
	if err != nil {
		t.Fatal(err)
	}
	same, err := TaskSpecFor(FeatureSchemaID)
	if err != nil || same.ID() != v1.ID() {
		t.Fatalf("v1 task %s, TaskSpecFor %s (%v)", v1.ID(), same.ID(), err)
	}
	v2, err := TaskSpecFor(FeatureSchemaV2ID)
	if err != nil || v2.ID() == v1.ID() || v2.Config().InputSchemaID != FeatureSchemaV2ID {
		t.Fatalf("v2 task %+v (%v)", v2.Config(), err)
	}
	if _, err := TaskSpecFor("router.pair/v9"); err == nil {
		t.Fatal("an unknown feature schema got a task")
	}
	config := DefaultTrainConfig()
	config.Features = FeatureSchemaV2ID
	if _, err := VectorModelJSON(v2, make([]float64, NumFeatures), 0, "d", config); err == nil {
		t.Fatal("a model of the wrong width was encoded")
	}
	if _, err := VectorModelJSON(v1, make([]float64, NumFeaturesV2), 0, "d", config); err == nil {
		t.Fatal("a model whose task names another schema was encoded")
	}
	if _, err := VectorModelJSON(v2, make([]float64, NumFeaturesV2), 0, "d", config); err != nil {
		t.Fatal(err)
	}
}
