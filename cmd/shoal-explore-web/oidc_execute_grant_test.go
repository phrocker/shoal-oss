// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.
package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
)

// TestTheFleetMappingDoesNotGrantExecute is the guard on an upgrade hazard, and
// it exists because adding one line to a list was the whole of the mistake.
//
// oidcFleetOperations is not a ceiling something else narrows: authority()
// grants it verbatim to any token whose claim matches -oidc-fleet-values. Every
// fleet principal is minted with the same authorization domain, source and
// policy, and authorizeResource compares only those three — AuthorizeObject
// validates ObjectID and never consults it. So the only thing separating two
// fleet principals' queued work is the principal check that OperationExecute
// skips by design.
//
// Granting execute here would therefore hand every existing fleet-mapped token,
// on upgrade, the ability to pull another principal's records, claim them, and
// complete them with a fabricated outcome — the exact property the operation
// was introduced to prevent.
//
// That stays true after #391. Execute now has a mapping of its own — the
// executor mint, on an audience and possibly an issuer of its own — and that
// mapping is the only one allowed to grant it. Every other list stays
// execute-free, and this asserts the absence over every list the command
// defines except the executor's, so a new list cannot be added unguarded.
func TestTheFleetMappingDoesNotGrantExecute(t *testing.T) {
	lists := []struct {
		name string
		set  []auth.Operation
	}{
		{"oidcFleetOperations", oidcFleetOperations},
		{"oidcContributorOperations", oidcContributorOperations},
		{"oidcReaderOperations", oidcReaderOperations},
		// The fourth list in this command, and the one an earlier version of
		// this test missed. workspaceOperations is the -dev-auth principal's
		// ceiling, so it is not OIDC-minted — which is exactly why asserting
		// only the three OIDC lists made the claim "unreachable by an
		// OIDC-minted token" true and the conclusion wrong. The dev
		// authenticator serves the same HTTP routes.
		{"workspaceOperations", workspaceOperations},
		{"oidcApproverOperations", oidcApproverOperations},
	}
	covered := map[string]bool{"oidcExecutorOperations": true}
	for _, list := range lists {
		covered[list.name] = true
		for _, operation := range list.set {
			if operation == auth.OperationExecute {
				t.Fatalf("the %s list grants OperationExecute. Every "+
					"principal it mints shares one authorization scope — "+
					"authorizeResource compares only domain, source and "+
					"policy and never matches ObjectID — so this lets any of "+
					"them take the routes that do not compare the caller to "+
					"the record, and claim and complete another's queued work",
					list.name)
			}
		}
	}
	for _, defined := range operationListsDefinedHere(t) {
		if !covered[defined] {
			t.Fatalf("the operation list %s has no row here; every list is "+
				"granted verbatim to some principal, and only the executor "+
				"mint may hold execute", defined)
		}
	}

	// And the operation is still a real one, so this is a statement about the
	// mapping rather than about the vocabulary. A test that passed because the
	// constant had been deleted would assert nothing.
	if err := auth.OperationExecute.Validate(); err != nil {
		t.Fatalf("OperationExecute is not a valid operation: %v", err)
	}
	if !auth.ServiceRoleActionExecution.Allows(auth.OperationExecute) {
		t.Fatal("ServiceRoleActionExecution no longer grants execute")
	}
}

// TestExecuteComesOnlyFromTheExecutorMint is the presence half (#391). The
// executor mint is the only source of OperationExecute in this command, in
// source and in behaviour, and every execute decision it produces carries
// ServiceRoleActionExecution and a non-empty executor binding: a role-less or
// unbound execute decision would claim nothing (#573) at best, and would be
// the cross-principal hazard above at worst.
func TestExecuteComesOnlyFromTheExecutorMint(t *testing.T) {
	// The list is exactly [execute].
	if len(oidcExecutorOperations) != 1 ||
		oidcExecutorOperations[0] != auth.OperationExecute {
		t.Fatalf("oidcExecutorOperations = %v, want [execute] alone",
			oidcExecutorOperations)
	}

	// In source: the identifier auth.OperationExecute appears in this
	// command's non-test code only inside the oidcExecutorOperations
	// declaration. A grant built anywhere else — a new list, an append, a
	// literal in a DecisionConfig — fails here.
	files := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(files, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var inside []ast.Node
		ast.Inspect(parsed, func(node ast.Node) bool {
			if spec, ok := node.(*ast.ValueSpec); ok {
				for _, identifier := range spec.Names {
					if identifier.Name == "oidcExecutorOperations" {
						inside = append(inside, spec)
					}
				}
			}
			return true
		})
		ast.Inspect(parsed, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "OperationExecute" {
				return true
			}
			if pkg, ok := selector.X.(*ast.Ident); !ok || pkg.Name != "auth" {
				return true
			}
			for _, spec := range inside {
				if selector.Pos() >= spec.Pos() && selector.End() <= spec.End() {
					found++
					return true
				}
			}
			t.Errorf("%s uses auth.OperationExecute outside "+
				"oidcExecutorOperations; the executor mint is the only "+
				"source of execute", files.Position(selector.Pos()))
			return true
		})
	}
	if found != 1 {
		t.Fatalf("auth.OperationExecute appears %d times in "+
			"oidcExecutorOperations, want once", found)
	}

	// In behaviour: every kind of token this authenticator mints, with
	// every mapping configured at once. Only the executor tokens hold
	// execute, and each carries the role and its binding.
	f := newExecutorFixture(t)
	everything := func(claims jwt.MapClaims) jwt.MapClaims {
		claims["access"] = []string{"reader", "writer", "fleet"}
		claims["groups"] = []string{labelGroupSecret}
		return claims
	}
	type minted struct {
		name     string
		token    string
		executor string
	}
	tokens := []minted{
		{name: "workspace", token: f.human.signRS256(t, testKID,
			everything(f.human.defaultClaims(f.now)))},
		{name: "approver", token: f.human.signRS256(t, testKID,
			approverClaims(f.human, f.now, "bob"))},
		{name: "executor", token: f.executorToken(t, everything(
			executorClaims(f.executor, f.now, testExecutorSubject))),
			executor: testExecutorRef},
		{name: "second executor", token: f.executorToken(t,
			executorClaims(f.executor, f.now, testOtherExecutorSubject)),
			executor: testOtherExecutorRef},
	}
	for _, sample := range tokens {
		decision, err := f.authn.Authenticate(bearerRequest(sample.token))
		if err != nil {
			t.Fatalf("%s token: %v", sample.name, err)
		}
		executes := false
		for _, operation := range decision.AllowedOperations() {
			executes = executes || operation == auth.OperationExecute
		}
		if executes != (sample.executor != "") {
			t.Fatalf("the %s decision holds execute = %v", sample.name, executes)
		}
		if !executes {
			if decision.ServiceRole() != "" || decision.ExecutorBinding() != "" {
				t.Fatalf("the %s decision carries role %q and binding %q",
					sample.name, decision.ServiceRole(), decision.ExecutorBinding())
			}
			continue
		}
		if decision.ServiceRole() != auth.ServiceRoleActionExecution ||
			decision.ExecutorBinding() == "" ||
			decision.ExecutorBinding() != sample.executor {
			t.Fatalf("the %s decision holds execute with role %q and binding "+
				"%q; every execute decision carries action_execution and "+
				"its own binding", sample.name, decision.ServiceRole(),
				decision.ExecutorBinding())
		}
	}

	// Without the mapping, no token mints execute at all: the executor
	// audience is simply not one this authenticator accepts.
	plain := newTestOIDCAuthenticator(t, approverTestConfig(
		t, f.human, fixedClock(f.now), approverMappingDocument(f.human.server.URL)))
	claims := executorClaims(f.human, f.now, testExecutorSubject)
	if _, err := plain.Authenticate(bearerRequest(
		f.human.signRS256(t, testKID, claims))); err == nil {
		t.Fatal("an executor-audience token was minted with no executor mapping")
	}
}
