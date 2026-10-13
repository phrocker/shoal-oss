// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package router

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/phrocker/shoal-oss/pkg/decision"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// The target choice is a registered typed decision: one Choice question per
// visible candidate subject, labelled match or no_match.
const (
	QuestionID   shoal.ID = "router.match"
	LabelMatch            = "match"
	LabelNoMatch          = "no_match"
)

// TaskConfig is the fixed task of the target-choice decision over the
// router.pair/v1 features. Its identity is what a trained model's task_id
// must name.
func TaskConfig() decision.TaskConfig {
	c, _ := TaskConfigFor(FeatureSchemaID)
	return c
}

// TaskConfigFor is the target-choice task over one feature schema. Tasks of
// two schemas differ only in their input schema, so a model trained on one
// schema's features is never served another's.
func TaskConfigFor(schema string) (decision.TaskConfig, error) {
	if _, ok := FeatureWidth(schema); !ok {
		return decision.TaskConfig{}, invalid("router feature schema is unknown")
	}
	return decision.TaskConfig{
		OwnerID: "shoal.router", Name: "router.target-choice", Version: "1",
		InputSchemaID: shoal.ID(schema), EvidencePolicyID: "router.evidence.feature-artifact/v1",
		LabelPolicyID: "router.labels.curated-fixtures/v1", EvaluationPolicyID: "router.evaluation/v1",
		PredictionUnit: "candidate", LabelUnit: "match", ActionUnit: "propose",
		AggregationID: "router.aggregate.exactly-one/v1",
		Questions: []decision.Question{{
			ID: QuestionID, Kind: decision.Choice, RubricID: "router.match.rubric/v1",
			Labels: []string{LabelMatch, LabelNoMatch},
		}},
	}, nil
}

// TaskSpec is the validated target-choice task over router.pair/v1.
func TaskSpec() (decision.TaskSpec, error) { return decision.NewTaskSpec(TaskConfig()) }

// TaskSpecFor is the validated target-choice task over one feature schema.
func TaskSpecFor(schema string) (decision.TaskSpec, error) {
	c, err := TaskConfigFor(schema)
	if err != nil {
		return decision.TaskSpec{}, err
	}
	return decision.NewTaskSpec(c)
}

// TrainConfig is the averaged-perceptron recipe. Its canonical JSON digest is
// the model's recipe_sha256. Fields added after v1 are omitted when unset, so
// v1's recipe digest is unchanged.
type TrainConfig struct {
	Algorithm string `json:"algorithm"`
	Epochs    int    `json:"epochs"`
	// PositiveRepeats applies each positive example's update this many
	// times, offsetting the one-positive-per-text class imbalance.
	PositiveRepeats int    `json:"positive_repeats"`
	Features        string `json:"feature_schema_id"`
	Order           string `json:"order"`
	// InterceptOffset is added to the trained intercept: the abstention
	// threshold, calibrated on the dev split and recorded in the recipe.
	InterceptOffset float64 `json:"intercept_offset,omitempty"`
}

// DefaultTrainConfig is the recipe of the checked-in v1 model.
func DefaultTrainConfig() TrainConfig {
	return TrainConfig{
		Algorithm: "averaged-perceptron/v1", Epochs: 30, PositiveRepeats: 4,
		Features: FeatureSchemaID, Order: "train-split file order, candidates in catalog order",
	}
}

// Digest is the recipe's SHA-256.
func (c TrainConfig) Digest() string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Example is one (text, candidate) pair of the training split, over the
// router.pair/v1 features.
type Example struct {
	Features [NumFeatures]float64
	Match    bool
}

// VectorExample is one (text, candidate) pair over any feature schema.
type VectorExample struct {
	Features []float64
	Match    bool
}

// Train fits an averaged perceptron over router.pair/v1 examples. It is
// TrainVectors on the same vectors.
func Train(examples []Example, c TrainConfig) (weights [NumFeatures]float64, bias float64) {
	vectors := make([]VectorExample, len(examples))
	for i, ex := range examples {
		vectors[i] = VectorExample{Features: append([]float64(nil), ex.Features[:]...), Match: ex.Match}
	}
	// Every vector has the width, so TrainVectors cannot refuse them.
	w, b, _ := TrainVectors(vectors, NumFeatures, c)
	copy(weights[:], w)
	return weights, b
}

// TrainVectors fits an averaged perceptron of the given width in the
// examples' order. It is deterministic: no randomness, and every product is
// rounded explicitly so a compiler cannot fuse operations differently on
// another architecture. The recipe's InterceptOffset is added to the
// averaged intercept. An example of another width is refused, never
// skipped.
func TrainVectors(examples []VectorExample, width int, c TrainConfig) (weights []float64, bias float64, err error) {
	for _, ex := range examples {
		if len(ex.Features) != width {
			return nil, 0, invalid("router training example has the wrong feature width")
		}
	}
	w, sum := make([]float64, width), make([]float64, width)
	weights = make([]float64, width)
	var b, sumB float64
	steps := 0.0
	repeats := c.PositiveRepeats
	if repeats < 1 {
		repeats = 1
	}
	for epoch := 0; epoch < c.Epochs; epoch++ {
		for _, ex := range examples {
			y := -1.0
			n := 1
			if ex.Match {
				y, n = 1, repeats
			}
			for r := 0; r < n; r++ {
				margin := b
				for k := range w {
					margin = float64(margin + float64(w[k]*ex.Features[k]))
				}
				if float64(y*margin) <= 0 {
					for k := range w {
						w[k] = float64(w[k] + float64(y*ex.Features[k]))
					}
					b = float64(b + y)
				}
				for k := range w {
					sum[k] = float64(sum[k] + w[k])
				}
				sumB = float64(sumB + b)
				steps++
			}
		}
	}
	if steps == 0 {
		return weights, c.InterceptOffset, nil
	}
	for k := range w {
		weights[k] = float64(sum[k] / steps)
	}
	return weights, float64(float64(sumB/steps) + c.InterceptOffset), nil
}

// TrainingRuntimeSHA256 names the trainer implementation.
var TrainingRuntimeSHA256 = func() string {
	sum := sha256.Sum256([]byte("shoal.router.train:averaged-perceptron:float64:v1"))
	return hex.EncodeToString(sum[:])
}()

// ModelJSON encodes a trained router.pair/v1 model as an
// internal/decisionlinear artifact: labels ordered so that a margin of zero
// or more selects match.
func ModelJSON(task decision.TaskSpec, weights [NumFeatures]float64, bias float64, datasetSHA256 string, c TrainConfig) ([]byte, error) {
	return VectorModelJSON(task, weights[:], bias, datasetSHA256, c)
}

// VectorModelJSON encodes a trained model of the recipe's feature schema.
// The weights must be that schema's width.
func VectorModelJSON(task decision.TaskSpec, weights []float64, bias float64, datasetSHA256 string, c TrainConfig) ([]byte, error) {
	width, ok := FeatureWidth(c.Features)
	if !ok || len(weights) != width || task.Config().InputSchemaID != shoal.ID(c.Features) {
		return nil, invalid("router model weights, task and recipe name different feature schemas")
	}
	model := struct {
		Schema                int       `json:"schema"`
		Kind                  string    `json:"kind"`
		TaskID                shoal.ID  `json:"task_id"`
		QuestionID            shoal.ID  `json:"question_id"`
		FeatureSchemaID       string    `json:"feature_schema_id"`
		DatasetSHA256         string    `json:"dataset_sha256"`
		RecipeSHA256          string    `json:"recipe_sha256"`
		TrainingRuntimeSHA256 string    `json:"training_runtime_sha256"`
		Labels                [2]string `json:"labels"`
		Coefficients          []float64 `json:"coefficients"`
		Intercept             float64   `json:"intercept"`
		Threshold             int       `json:"threshold"`
	}{1, "linear-svm", task.ID(), QuestionID, c.Features, datasetSHA256, c.Digest(), TrainingRuntimeSHA256,
		[2]string{LabelNoMatch, LabelMatch}, weights, bias, 0}
	b, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
