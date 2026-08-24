/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package retrievalgrpc

import (
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/internal/knowledgepb"
	"github.com/phrocker/shoal-oss/pkg/document"
	"github.com/phrocker/shoal-oss/pkg/graph"
	"github.com/phrocker/shoal-oss/pkg/retrieval"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func validateResponse(response retrieval.Response) error {
	if err := validateWireString("request ID", string(response.RequestID)); err != nil {
		return err
	}
	for resultIndex, result := range response.Results {
		resultName := fmt.Sprintf("result %d", resultIndex)
		if err := validateWireString(resultName+" ID", string(result.ID)); err != nil {
			return err
		}
		if err := validateFinite(
			fmt.Sprintf("result %d score", resultIndex), result.Score,
		); err != nil {
			return err
		}
		for evidenceIndex, evidence := range result.Evidence {
			evidenceName := fmt.Sprintf(
				"result %d evidence %d", resultIndex, evidenceIndex)
			if err := evidence.Citation.Validate(); err != nil {
				return fmt.Errorf(
					"result %d evidence %d citation: %w",
					resultIndex, evidenceIndex, err)
			}
			if err := validateCitationStrings(
				evidenceName+" citation", evidence.Citation,
			); err != nil {
				return err
			}
			if err := validateWireString(evidenceName+" quote", evidence.Quote); err != nil {
				return err
			}
			if err := validateFinite(
				fmt.Sprintf("result %d evidence %d score", resultIndex, evidenceIndex),
				evidence.Score,
			); err != nil {
				return err
			}
			if pathPresent(evidence.Path) {
				if err := evidence.Path.Validate(); err != nil {
					return fmt.Errorf(
						"result %d evidence %d path: %w",
						resultIndex, evidenceIndex, err)
				}
				if err := validatePathStrings(evidenceName+" path", evidence.Path); err != nil {
					return err
				}
				if err := validatePathScores(
					resultIndex, evidenceIndex, evidence.Path,
				); err != nil {
					return err
				}
			}
		}
		if result.Explanation != nil {
			if err := validateWireString(
				resultName+" explanation summary", result.Explanation.Summary,
			); err != nil {
				return err
			}
			for name, score := range result.Explanation.Scores {
				if err := validateWireString(
					resultName+" explanation score name", name,
				); err != nil {
					return err
				}
				if err := validateFinite(
					fmt.Sprintf("result %d explanation score %q", resultIndex, name),
					score,
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateCitationStrings(name string, citation document.Citation) error {
	fields := []struct {
		name  string
		value shoal.ID
	}{
		{name: "document ID", value: citation.DocumentID},
		{name: "revision ID", value: citation.RevisionID},
		{name: "section ID", value: citation.SectionID},
		{name: "span ID", value: citation.SpanID},
	}
	for _, field := range fields {
		if err := validateWireString(name+" "+field.name, string(field.value)); err != nil {
			return err
		}
	}
	return nil
}

func validatePathStrings(name string, path graph.Path) error {
	for nodeIndex, node := range path.Nodes {
		nodeName := fmt.Sprintf("%s node %d", name, nodeIndex)
		if err := validateWireString(nodeName+" ID", string(node.ID)); err != nil {
			return err
		}
		if err := validateWireString(nodeName+" kind", node.Kind); err != nil {
			return err
		}
		for labelIndex, label := range node.Labels {
			if err := validateWireString(
				fmt.Sprintf("%s label %d", nodeName, labelIndex), label,
			); err != nil {
				return err
			}
		}
		if err := validateMetadataStrings(nodeName+" property", node.Properties); err != nil {
			return err
		}
	}
	for edgeIndex, edge := range path.Edges {
		edgeName := fmt.Sprintf("%s edge %d", name, edgeIndex)
		fields := []struct {
			name  string
			value string
		}{
			{name: "ID", value: string(edge.ID)},
			{name: "from", value: string(edge.From)},
			{name: "to", value: string(edge.To)},
			{name: "type", value: edge.Type},
		}
		for _, field := range fields {
			if err := validateWireString(edgeName+" "+field.name, field.value); err != nil {
				return err
			}
		}
		if err := validateMetadataStrings(edgeName+" property", edge.Properties); err != nil {
			return err
		}
	}
	return nil
}

func validateMetadataStrings(name string, metadata shoal.Metadata) error {
	for key, value := range metadata {
		if err := validateWireString(name+" key", key); err != nil {
			return err
		}
		if err := validateWireString(name+" value", value); err != nil {
			return err
		}
	}
	return nil
}

func validateWireString(name, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s must be valid UTF-8", name)
	}
	return nil
}

func validateProtoResponse(response *knowledgepb.RetrieveResponse) error {
	if response == nil {
		return fmt.Errorf("knowledge retrieval response is required")
	}
	for resultIndex, result := range response.GetResults() {
		if result == nil {
			return fmt.Errorf("result %d is required", resultIndex)
		}
		if err := validateFinite(
			fmt.Sprintf("result %d score", resultIndex),
			shoal.Score(result.GetScore()),
		); err != nil {
			return err
		}
		for evidenceIndex, evidence := range result.GetEvidence() {
			if evidence == nil {
				return fmt.Errorf(
					"result %d evidence %d is required", resultIndex, evidenceIndex)
			}
			if evidence.GetCitation() == nil {
				return fmt.Errorf(
					"result %d evidence %d citation is required",
					resultIndex, evidenceIndex)
			}
			if err := citationFromProto(evidence.GetCitation()).Validate(); err != nil {
				return fmt.Errorf(
					"result %d evidence %d citation: %w",
					resultIndex, evidenceIndex, err)
			}
			if err := validateFinite(
				fmt.Sprintf("result %d evidence %d score", resultIndex, evidenceIndex),
				shoal.Score(evidence.GetScore()),
			); err != nil {
				return err
			}
			if evidence.GetPath() != nil {
				path := pathFromProto(evidence.GetPath())
				if err := path.Validate(); err != nil {
					return fmt.Errorf(
						"result %d evidence %d path: %w",
						resultIndex, evidenceIndex, err)
				}
				if err := validatePathScores(resultIndex, evidenceIndex, path); err != nil {
					return err
				}
			}
		}
		if explanation := result.GetExplanation(); explanation != nil {
			for modeIndex, mode := range explanation.GetModes() {
				if _, err := modeFromProto(mode); err != nil {
					return fmt.Errorf(
						"result %d explanation mode %d: %w",
						resultIndex, modeIndex, err)
				}
			}
			for name, score := range explanation.GetScores() {
				if err := validateFinite(
					fmt.Sprintf("result %d explanation score %q", resultIndex, name),
					shoal.Score(score),
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validatePathScores(resultIndex, evidenceIndex int, path graph.Path) error {
	for edgeIndex, edge := range path.Edges {
		if err := validateFinite(
			fmt.Sprintf(
				"result %d evidence %d path edge %d weight",
				resultIndex, evidenceIndex, edgeIndex),
			edge.Weight,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateFinite(name string, score shoal.Score) error {
	if math.IsNaN(float64(score)) || math.IsInf(float64(score), 0) {
		return fmt.Errorf("%s must be finite", name)
	}
	return nil
}

func pathPresent(path graph.Path) bool {
	return len(path.Nodes) > 0 || len(path.Edges) > 0
}
