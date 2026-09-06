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

package ontology

import "github.com/phrocker/shoal-oss/pkg/shoal"

func validateProposalEvolution(
	base, proposed OntologyVersion,
	morphisms []OntologyMorphism,
) error {
	if err := base.Validate(); err != nil {
		return err
	}
	if err := proposed.Validate(); err != nil {
		return err
	}
	if base.Schema().ID() != proposed.Schema().ID() {
		return invalid("proposal versions belong to different schemas")
	}

	targetProperties := make(map[shoal.ID]PropertyDefinition, len(proposed.properties))
	for _, property := range proposed.properties {
		targetProperties[property.ID()] = property
	}
	for _, property := range base.properties {
		target, retained := targetProperties[property.ID()]
		if retained && property.canonical() != target.canonical() {
			return invalid(
				"retained property changed meaning without a supported morphism")
		}
	}

	targetConcepts := make(map[shoal.ID]ConceptDefinition, len(proposed.concepts))
	for _, concept := range proposed.concepts {
		targetConcepts[concept.ID()] = concept
	}
	for _, concept := range base.concepts {
		target, retained := targetConcepts[concept.ID()]
		if !retained {
			continue
		}
		if concept.key != target.key || concept.name != target.name ||
			concept.description != target.description ||
			canonicalMetadata(concept.metadata) != canonicalMetadata(target.metadata) ||
			!safePropertyAddition(concept.properties, target.properties, targetProperties) {
			return invalid(
				"retained concept changed meaning without a supported morphism")
		}
	}

	targetRelationships := make(
		map[shoal.ID]RelationshipDefinition, len(proposed.relationships))
	for _, relationship := range proposed.relationships {
		targetRelationships[relationship.ID()] = relationship
	}
	for _, relationship := range base.relationships {
		target, retained := targetRelationships[relationship.ID()]
		if !retained {
			continue
		}
		if relationship.key != target.key ||
			relationship.name != target.name ||
			relationship.description != target.description ||
			relationship.directed != target.directed ||
			canonicalMetadata(relationship.metadata) != canonicalMetadata(target.metadata) ||
			!safePropertyAddition(
				relationship.properties, target.properties, targetProperties) {
			return invalid(
				"retained relationship changed unsupported semantics")
		}
		if equalIDs(relationship.fromConcepts, target.fromConcepts) &&
			equalIDs(relationship.toConcepts, target.toConcepts) {
			continue
		}
		required := MorphismKind("")
		switch {
		case idSubset(relationship.fromConcepts, target.fromConcepts) &&
			idSubset(relationship.toConcepts, target.toConcepts):
			required = MorphismWiden
		case idSubset(target.fromConcepts, relationship.fromConcepts) &&
			idSubset(target.toConcepts, relationship.toConcepts):
			required = MorphismNarrow
		default:
			return invalid(
				"relationship endpoint change requires separate supported morphisms")
		}
		if !hasRelationshipMorphism(morphisms, relationship.ID(), required) {
			return invalid(
				"relationship endpoint change requires an explicit morphism")
		}
	}
	return nil
}

func safePropertyAddition(
	before, after []shoal.ID,
	targetProperties map[shoal.ID]PropertyDefinition,
) bool {
	if !idSubset(before, after) {
		return false
	}
	for _, propertyID := range after {
		if containsID(before, propertyID) {
			continue
		}
		property, ok := targetProperties[propertyID]
		if !ok || propertyRequiresPresence(property) {
			return false
		}
	}
	return true
}

func propertyRequiresPresence(property PropertyDefinition) bool {
	for _, constraint := range property.constraints {
		switch constraint.Kind() {
		case ConstraintRequired:
			return true
		case ConstraintMinimumCount:
			count, _ := constraint.Count()
			if count > 0 {
				return true
			}
		}
	}
	return false
}

func equalIDs(left, right []shoal.ID) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func hasRelationshipMorphism(
	morphisms []OntologyMorphism,
	relationshipID shoal.ID,
	kind MorphismKind,
) bool {
	for _, morphism := range morphisms {
		if morphism.Kind() == kind &&
			len(morphism.sources) == 1 &&
			len(morphism.targets) == 1 &&
			morphism.sources[0] == relationshipID &&
			morphism.targets[0] == relationshipID {
			return true
		}
	}
	return false
}
