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
	"context"
	"testing"

	"github.com/phrocker/shoal-oss/internal/explorerconformance"
	"github.com/phrocker/shoal-oss/pkg/explorer"
)

func TestEmbeddedPublicConformance(t *testing.T) {
	explorerconformance.Run(t, func(t testing.TB) (
		explorerconformance.Lifecycle, error,
	) {
		dataDir := t.TempDir()
		current, err := explorer.Open(dataDir)
		if err != nil {
			return explorerconformance.Lifecycle{}, err
		}
		return explorerconformance.Lifecycle{
			Client: current,
			Restart: func(context.Context) (explorer.Client, error) {
				if err := current.Close(); err != nil {
					return nil, err
				}
				reopened, err := explorer.Open(dataDir)
				if err != nil {
					return nil, err
				}
				current = reopened
				return current, nil
			},
			Close: func() error {
				return current.Close()
			},
		}, nil
	})
}
