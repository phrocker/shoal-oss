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

package auth

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// validateExecutorBinding enforces that an executor binding appears exactly
// when the decision holds ServiceRoleActionExecution (#391). The role is the
// only one that may execute queued work, and the binding names the single
// executor reference that work must belong to; a binding on any other
// decision would claim a narrowing nothing enforces, and the role without one
// would name no executor.
//
// A set binding is bounded like every other identity (shoal.MaxIDBytes, which
// equals the fleet's executor-reference bound) and must be printable UTF-8
// with no surrounding whitespace, so it can always equal a valid fleet
// executor reference and never smuggles control bytes into logs.
//
// A bound decision also acts only as itself: it carries no on-behalf-of
// chain. Chains hold workspace subjects that may collide across issuers
// (#546), and a held claim is matched by comparing chains element for
// element, so a delegated worker could otherwise present another principal's
// chain and take over that principal's claim.
func validateExecutorBinding(
	role ServiceRole, binding string, onBehalfOf []shoal.ID,
) error {
	if role != ServiceRoleActionExecution {
		if binding != "" {
			return shoal.NewError(
				shoal.ErrorInvalidArgument,
				"executor binding requires the action execution service role",
			)
		}
		return nil
	}
	if err := shoal.ValidateRequiredID(
		"executor binding", shoal.ID(binding),
	); err != nil {
		return err
	}
	if !utf8.ValidString(binding) || strings.TrimSpace(binding) != binding {
		return invalidExecutorBinding()
	}
	for _, character := range binding {
		if !unicode.IsPrint(character) {
			return invalidExecutorBinding()
		}
	}
	if len(onBehalfOf) != 0 {
		return shoal.NewError(
			shoal.ErrorInvalidArgument,
			"an executor-bound decision cannot act on behalf of another identity",
		)
	}
	return nil
}

func invalidExecutorBinding() error {
	return shoal.NewError(
		shoal.ErrorInvalidArgument, "executor binding must be printable text")
}

// ExecutorBinding returns the one executor reference an action-execution
// decision may act for. It is empty for every other decision, and an empty
// binding claims nothing.
func (d Decision) ExecutorBinding() string { return d.executorBinding }

// executorBinding appends the binding to a fingerprint. It is called only
// for a set binding, so a decision without one keeps the fingerprint it had
// before bindings existed. The leading marker is 3, distinct from the
// selected-ontology marker 1 and the grant-provenance marker 2 that may
// precede it; see AuthorizationFingerprint for why that keeps the encoding
// injective.
func (e *digestEncoder) executorBinding(binding string) {
	e.uint64(3)
	e.text(binding)
}
