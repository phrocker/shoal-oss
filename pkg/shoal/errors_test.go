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

package shoal_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

func TestErrorCodeSurvivesWrapping(t *testing.T) {
	cause := errors.New("backend unavailable")
	err := shoal.WrapError(shoal.ErrorUnavailable, "knowledge store unavailable", cause)
	wrapped := fmt.Errorf("retrieve: %w", err)

	if !shoal.IsErrorCode(wrapped, shoal.ErrorUnavailable) {
		t.Fatalf("expected unavailable error, got %v", wrapped)
	}
	if !errors.Is(wrapped, cause) {
		t.Fatalf("expected wrapped cause %v, got %v", cause, wrapped)
	}
}
