// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"testing"

	"github.com/phrocker/shoal-oss/internal/routerwire"
	"github.com/phrocker/shoal-oss/pkg/router"
)

// Router v2's model and its dev report (docs/router-evaluation-v2.md,
// "Amendments"). The model is trained on v2's train split alone, which holds
// v1's 292 spent cases and v2's own train cases; the dev split is used only
// to measure and to choose the recipe, and every such choice is recorded in
// the amendments. Nothing here names the sealed split.

var v2DevDetail = flag.Bool("v2-dev-detail", false, "log every dev case router v2 gets wrong")

const v2DevReportPath = v2Dir + "/report-dev.md"

// v2TrainConfig is router v2's recipe.
func v2TrainConfig() router.TrainConfig {
	return router.TrainConfig{
		Algorithm: "averaged-perceptron/v1", Epochs: 30, PositiveRepeats: 4,
		Features: router.FeatureSchemaV2ID, Order: "train-split file order, candidates in catalog order",
		// Calibrated on the dev split (amendment 2026-10-13).
		InterceptOffset: -1,
	}
}

func loadV2(t testing.TB) (*World, *Cases) {
	t.Helper()
	w, err := LoadWorld(os.DirFS(v2Dir))
	if err != nil {
		t.Fatal(err)
	}
	cases, err := LoadCases(os.DirFS(v2Dir))
	if err != nil {
		t.Fatal(err)
	}
	return w, cases
}

// vectorExamples are the router.pair/v2 training pairs of the given cases:
// one per (case, visible candidate), positive when the candidate is the
// labelled target (an abstention that names its target counts, as in v1).
func vectorExamples(t testing.TB, w *World, cases []Case) []router.VectorExample {
	t.Helper()
	r := &Runner{World: w, validator: routerwire.Validator(), catalogs: map[string]*router.Catalog{}}
	var out []router.VectorExample
	for _, c := range cases {
		a, err := r.Analyze(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, early := a.Early(); early {
			continue
		}
		vectors, err := a.FeaturesFor(router.FeatureSchemaV2ID)
		if err != nil {
			t.Fatal(err)
		}
		catalog, _ := r.Catalog(c.Caller)
		for i, key := range catalog.Keys() {
			out = append(out, router.VectorExample{Features: vectors[i], Match: key == c.Expected.Target})
		}
	}
	return out
}

func trainV2Model(t testing.TB, w *World, cases *Cases, config router.TrainConfig) []byte {
	t.Helper()
	train, err := cases.Split("train")
	if err != nil {
		t.Fatal(err)
	}
	weights, bias, err := router.TrainVectors(vectorExamples(t, w, train), router.NumFeaturesV2, config)
	if err != nil {
		t.Fatal(err)
	}
	task, err := router.TaskSpecFor(router.FeatureSchemaV2ID)
	if err != nil {
		t.Fatal(err)
	}
	model, err := router.VectorModelJSON(task, weights, bias, cases.Digest("train"), config)
	if err != nil {
		t.Fatal(err)
	}
	return model
}

// TestV2ModelIsReproducible retrains router v2 from v2's train split alone
// and requires the checked-in model to be byte-identical.
func TestV2ModelIsReproducible(t *testing.T) {
	w, cases := loadV2(t)
	for _, c := range cases.all {
		if c.Split == "test" {
			t.Fatalf("v2 cases.jsonl holds case %s of the test split", c.ID)
		}
	}
	model := trainV2Model(t, w, cases, v2TrainConfig())
	if *update {
		if err := os.MkdirAll(v2Dir+"/model", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(v2ModelPath, model, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := os.ReadFile(v2ModelPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, model) {
		t.Fatal("checked-in v2 model differs from a retrain on v2's train split; run with -update and review")
	}
	var fields map[string]any
	if err := json.Unmarshal(model, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["dataset_sha256"] != cases.Digest("train") || fields["recipe_sha256"] != v2TrainConfig().Digest() ||
		fields["feature_schema_id"] != router.FeatureSchemaV2ID {
		t.Fatal("v2 model does not pin v2's train split, recipe and feature schema")
	}
	provider, err := NewProvider(model)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(provider.Identity().Config().PreprocessingID); got != router.FeatureSchemaV2ID {
		t.Fatalf("v2 predictor preprocessing = %s", got)
	}
}

// TestV2DevReport runs the committed harness (v2Evaluate) on v2's dev split
// with the real v2 artefacts and keeps its report as a golden, as v1 did for
// its train and dev reports: a change that moves a dev number shows in
// review.
func TestV2DevReport(t *testing.T) {
	v2World, cases := loadV2(t)
	dev, err := cases.Split("dev")
	if err != nil {
		t.Fatal(err)
	}
	v2Model, err := os.ReadFile(v2ModelPath)
	if err != nil {
		t.Fatal(err)
	}
	v1World, _ := load(t)
	v1Model, err := os.ReadFile(modelPath)
	if err != nil {
		t.Fatal(err)
	}
	v2 := v2System{"router v2", v2World, v2Model}
	text, _ := v2Evaluate(t, "dev", dev, v2, v2System{"frozen v1", v1World, v1Model})
	t.Log("\n" + text)
	if *v2DevDetail {
		for _, o := range v2.run(t, dev) {
			if !v2World.Correct(o.Case, o.Router) {
				t.Logf("router v2 wrong %s %q (%s): want %s %s %v; got %s %s %v %v", o.Case.ID, o.Case.Text, o.Case.Caller,
					o.Case.Expected.Kind, o.Case.Expected.Target, o.Case.Expected.Slots,
					o.Router.Kind, targetKey(o.Router), v2World.SlotValues(o.Router), o.Router.Reasons)
			}
		}
	}
	if *update {
		if err := os.WriteFile(v2DevReportPath, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := os.ReadFile(v2DevReportPath)
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s is missing; run with -update", v2DevReportPath)
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(stored) != text {
		t.Fatalf("%s differs from this run; rerun with -update and review", v2DevReportPath)
	}
}
