// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Package effectsgateway is the core of the HTTP effects gateway (#391): the
// Path A worker that pulls an action off the fleet dispatch queue, claims it
// under a fence, performs one HTTP request against a configured operational
// surface, and reports what happened.
//
// This package holds only the parts that can be correct before the worker
// loop exists: configuration and its arithmetic, the route table and the
// request binder, the response classifier, the clock anchoring and send
// predicates, ExecutorKey handling, the egress-restricted target transport, an
// internal client for the dispatch routes on main, and the logging policy.
//
// What is deliberately absent, and why, is in docs/effects-gateway-deploy.md:
// the worker loop and a command need a principal that can claim work it did
// not enqueue (#480), claim renewal (#430), an ambiguity route for a lost
// fence (#484), and a heartbeat that does not invalidate live claims (#486).
// Nothing in this package performs an effect on its own.
package effectsgateway
