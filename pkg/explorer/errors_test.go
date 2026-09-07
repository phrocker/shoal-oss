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

package explorer_test

import (
	"errors"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestIndeterminateCommitMarkerPreservesErrorChain(t *testing.T) {
	cause := errors.New("storage outcome unknown")
	public := shoal.WrapError(
		shoal.ErrorUnavailable, "write explorer record", cause)
	marked := explorer.MarkIndeterminateCommit(public)
	if !explorer.IsIndeterminateCommit(marked) {
		t.Fatal("marked error was not detected")
	}

	if !shoal.IsErrorCode(marked, shoal.ErrorUnavailable) {
		t.Fatalf("marked error lost public code: %v", marked)
	}
	if !errors.Is(marked, cause) {
		t.Fatalf("marked error lost cause: %v", marked)
	}
	if explorer.MarkIndeterminateCommit(nil) != nil {
		t.Fatal("nil error acquired a marker")
	}
	if explorer.IsIndeterminateCommit(errors.New("ordinary failure")) {
		t.Fatal("ordinary error reported indeterminate")
	}
}

func TestCommittedInteractionMarkerPreservesErrorChain(t *testing.T) {
	cause := errors.New("authorization generation changed after commit")
	public := shoal.WrapError(
		shoal.ErrorUnauthorized, "authorization changed", cause)
	marked := explorer.MarkCommittedInteraction(public)
	if !explorer.IsCommittedInteraction(marked) {
		t.Fatal("committed interaction error was not detected")
	}
	if !shoal.IsErrorCode(marked, shoal.ErrorUnauthorized) {
		t.Fatalf("committed interaction error lost public code: %v", marked)
	}
	if !errors.Is(marked, cause) {
		t.Fatalf("committed interaction error lost cause: %v", marked)
	}
	if explorer.MarkCommittedInteraction(nil) != nil {
		t.Fatal("nil error acquired a committed marker")
	}
}
