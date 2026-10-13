// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package engine

import "testing"

func TestShipKeyAndProducerEpoch(t *testing.T) {
	const a, b = "0123456789abcdef0123456789abcdef", "fedcba9876543210fedcba9876543210"
	keys := map[string]string{}
	for name, opts := range map[string]RFileExportOptions{
		"plain":         {},
		"plain-id":      {ProducerID: "p"},
		"one":           {StampVisibilityLabel: "one"},
		"two":           {StampVisibilityLabel: "two"},
		"one-whenEmpty": {StampVisibilityLabel: "one", StampMode: "whenEmpty"},
	} {
		for _, instance := range []string{"", a, b} {
			keys[name+"/"+instance] = shipKey(instance, opts)
		}
	}
	seen := map[string]string{}
	for k, v := range keys {
		// The ProducerID does not take part in the key (it is in the path).
		if k == "plain-id/"+a || k == "plain-id/"+b || k == "plain-id/" {
			continue
		}
		if other, dup := seen[v]; dup {
			t.Fatalf("%s and %s share ship key %q", k, other, v)
		}
		seen[v] = k
	}
	if keys["plain/"+a] != keys["plain-id/"+a] {
		t.Fatal("ship key depends on the ProducerID")
	}

	if producerEpoch(a, RFileExportOptions{}) != "" || producerEpoch("", RFileExportOptions{ProducerID: "p"}) != "" {
		t.Fatal("epoch without a ProducerID or for an unminted file")
	}
	epochs := map[string]bool{}
	for _, opts := range []RFileExportOptions{
		{ProducerID: "p"}, {ProducerID: "p", StampVisibilityLabel: "one"},
		{ProducerID: "p", StampVisibilityLabel: "two"}, {ProducerID: "p", StampVisibilityLabel: "one", StampMode: "whenEmpty"},
	} {
		e := producerEpoch(a, opts)
		if e == "" || epochs[e] {
			t.Fatalf("epoch %q for %+v empty or repeated", e, opts)
		}
		epochs[e] = true
	}
}
