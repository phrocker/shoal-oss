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

package explorer

import "errors"

type indeterminateCommitError struct {
	cause error
}

func (e *indeterminateCommitError) Error() string {
	return e.cause.Error()
}

func (e *indeterminateCommitError) Unwrap() error {
	return e.cause
}

// MarkIndeterminateCommit marks a storage error whose durable commit outcome
// cannot be determined. The marker preserves the original error and its public
// Shoal error code through normal unwrapping.
func MarkIndeterminateCommit(err error) error {
	if err == nil || IsIndeterminateCommit(err) {
		return err
	}
	return &indeterminateCommitError{cause: err}
}

// IsIndeterminateCommit reports whether an error chain contains an
// indeterminate durable-commit marker.
func IsIndeterminateCommit(err error) bool {
	var marked *indeterminateCommitError
	return errors.As(err, &marked)
}

type committedInteractionError struct {
	cause error
}

func (e *committedInteractionError) Error() string {
	return e.cause.Error()
}

func (e *committedInteractionError) Unwrap() error {
	return e.cause
}

// MarkCommittedInteraction marks a post-commit failure: the interaction is
// durable even though a later authorization/generation check prevented the
// caller from receiving success. Callers must not retry the same stable ID as
// though the write rolled back.
func MarkCommittedInteraction(err error) error {
	if err == nil || IsCommittedInteraction(err) {
		return err
	}
	return &committedInteractionError{cause: err}
}

// IsCommittedInteraction reports whether an error occurred after the durable
// interaction write completed.
func IsCommittedInteraction(err error) bool {
	var marked *committedInteractionError
	return errors.As(err, &marked)
}
