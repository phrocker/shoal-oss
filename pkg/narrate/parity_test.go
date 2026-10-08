// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package narrate

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/phrocker/shoal-oss/internal/effectsgateway"
	"github.com/phrocker/shoal-oss/pkg/decision"
	"github.com/phrocker/shoal-oss/pkg/explorer/fleet"
)

// The vocabularies are read from their owners' source, not from this
// package's lists, so that a value added there and not here fails a test.

const (
	fleetDir           = "../explorer/fleet"
	decisionDir        = "../decision"
	gatewayDir         = "../../internal/effectsgateway"
	decisionServiceDir = "../../internal/decisionservice"
	approvalDoc        = "../../docs/approval.md"
)

// parseDir parses the non-test Go files of a directory.
func parseDir(t *testing.T, dir string) []*ast.File {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		t.Fatalf("no Go files in %s", dir)
	}
	return files
}

func stringLit(e ast.Expr) (string, bool) {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	return s, err == nil
}

// typedConsts returns name → value for every string constant declared with
// the given type.
func typedConsts(t *testing.T, dir, typeName string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, f := range parseDir(t, dir) {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				typ, ok := vs.Type.(*ast.Ident)
				if !ok || typ.Name != typeName {
					continue
				}
				for i, name := range vs.Names {
					if i < len(vs.Values) {
						if v, ok := stringLit(vs.Values[i]); ok {
							out[name.Name] = v
						}
					}
				}
			}
		}
	}
	if len(out) == 0 {
		t.Fatalf("no constants of type %s in %s", typeName, dir)
	}
	return out
}

func values(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func sorted[T ~string](in []T) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	sort.Strings(out)
	return out
}

func sameSet(t *testing.T, what string, source, mirror []string) {
	t.Helper()
	a := append([]string(nil), source...)
	b := append([]string(nil), mirror...)
	sort.Strings(a)
	sort.Strings(b)
	if strings.Join(a, "\x00") != strings.Join(b, "\x00") {
		t.Errorf("%s: source has %q, narrate has %q", what, a, b)
	}
}

// Source-derived vocabularies, shared with the coverage tests.

func sourceDispatchStates(t *testing.T) []string {
	return values(typedConsts(t, fleetDir, "DispatchState"))
}

func sourceApprovalStates(t *testing.T) []string {
	return values(typedConsts(t, fleetDir, "ApprovalState"))
}

func sourceApprovalConditions(t *testing.T) []string {
	return values(typedConsts(t, fleetDir, "ApprovalCondition"))
}

// sourceTransitionKinds reads the kinds NewActionTransition accepts.
func sourceTransitionKinds(t *testing.T) []string {
	t.Helper()
	var kinds []string
	for _, f := range parseDir(t, fleetDir) {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "NewActionTransition" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if clause, ok := n.(*ast.CaseClause); ok {
					for _, e := range clause.List {
						if s, ok := stringLit(e); ok {
							kinds = append(kinds, s)
						}
					}
				}
				return true
			})
		}
	}
	if len(kinds) == 0 {
		t.Fatal("NewActionTransition accepts no kinds; the parity reader is stale")
	}
	return kinds
}

// sourceFleetErrorCodes reads every string literal assigned to a field named
// ErrorCode in the fleet package.
func sourceFleetErrorCodes(t *testing.T) []string {
	t.Helper()
	seen := map[string]bool{}
	for _, f := range parseDir(t, fleetDir) {
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				for i, lhs := range n.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "ErrorCode" || i >= len(n.Rhs) {
						continue
					}
					if s, ok := stringLit(n.Rhs[i]); ok && s != "" {
						seen[s] = true
					}
				}
			case *ast.KeyValueExpr:
				if key, ok := n.Key.(*ast.Ident); ok && key.Name == "ErrorCode" {
					if s, ok := stringLit(n.Value); ok && s != "" {
						seen[s] = true
					}
				}
			}
			return true
		})
	}
	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// sourceGatewayCodes reads the effects gateway's error-code constants: every
// string constant named Error* in its package, and the rejected prefix.
func sourceGatewayCodes(t *testing.T) (codes []string, prefix string) {
	t.Helper()
	for _, f := range parseDir(t, gatewayDir) {
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs := spec.(*ast.ValueSpec)
				for i, name := range vs.Names {
					if i >= len(vs.Values) {
						continue
					}
					v, ok := stringLit(vs.Values[i])
					if !ok {
						continue
					}
					switch {
					case name.Name == "errorTargetRejectedPrefix":
						prefix = v
					case strings.HasPrefix(name.Name, "Error"):
						codes = append(codes, v)
					}
				}
			}
		}
	}
	if len(codes) == 0 || prefix == "" {
		t.Fatal("gateway error codes not found; the parity reader is stale")
	}
	return codes, prefix
}

// sourceDecisionServiceReasons reads terminal(request, decision.X, "reason", t)
// calls in the decision service.
func sourceDecisionServiceReasons(t *testing.T) map[decision.ResultStatus][]string {
	t.Helper()
	statuses := typedConsts(t, decisionDir, "ResultStatus")
	out := map[decision.ResultStatus][]string{}
	seen := map[string]bool{}
	for _, f := range parseDir(t, decisionServiceDir) {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 4 {
				return true
			}
			if fn, ok := call.Fun.(*ast.Ident); !ok || fn.Name != "terminal" {
				return true
			}
			sel, ok := call.Args[1].(*ast.SelectorExpr)
			if !ok {
				t.Errorf("terminal status is not a decision constant: %T", call.Args[1])
				return true
			}
			reason, ok := stringLit(call.Args[2])
			if !ok {
				t.Errorf("terminal reason is not a string literal")
				return true
			}
			status := decision.ResultStatus(statuses[sel.Sel.Name])
			if key := string(status) + "/" + reason; !seen[key] {
				seen[key] = true
				out[status] = append(out[status], reason)
			}
			return true
		})
	}
	if len(out) == 0 {
		t.Fatal("no terminal(...) calls found; the parity reader is stale")
	}
	return out
}

// docApprovalRows reads the status table in docs/approval.md.
func docApprovalRows(t *testing.T) [][2]string {
	t.Helper()
	f, err := os.Open(approvalDoc)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var rows [][2]string
	in := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "| `state` | `condition` |") {
			in = true
			continue
		}
		if !in {
			continue
		}
		if !strings.HasPrefix(line, "|") {
			break
		}
		cells := strings.Split(strings.Trim(line, "|"), "|")
		if len(cells) < 2 || strings.HasPrefix(strings.TrimSpace(cells[0]), "---") {
			continue
		}
		condition := strings.Trim(strings.TrimSpace(cells[1]), "`")
		if condition == "—" {
			condition = ""
		}
		for _, state := range strings.Split(cells[0], "/") {
			rows = append(rows, [2]string{strings.Trim(strings.TrimSpace(state), "`"), condition})
		}
	}
	if len(rows) == 0 {
		t.Fatal("approval status table not found in docs/approval.md")
	}
	return rows
}

func TestParityFleetVocabularies(t *testing.T) {
	sameSet(t, "DispatchState", sourceDispatchStates(t), sorted(DispatchStates))
	sameSet(t, "ApprovalState", sourceApprovalStates(t), sorted(ApprovalStates))
	sameSet(t, "ApprovalCondition", sourceApprovalConditions(t), sorted(ApprovalConditions))
	sameSet(t, "action transition kinds", sourceTransitionKinds(t), ActionTransitionKinds)
	sameSet(t, "fleet error codes", sourceFleetErrorCodes(t), FleetErrorCodes)
}

func TestParityDecisionVocabularies(t *testing.T) {
	sameSet(t, "ResultStatus", values(typedConsts(t, decisionDir, "ResultStatus")), sorted(ResultStatuses))
	sameSet(t, "AnswerStatus", values(typedConsts(t, decisionDir, "AnswerStatus")), sorted(AnswerStatuses))
	sameSet(t, "AnswerKind", values(typedConsts(t, decisionDir, "AnswerKind")), sorted(AnswerKinds))
	sameSet(t, "Disposition", values(typedConsts(t, decisionDir, "Disposition")), sorted(Dispositions))
	sameSet(t, "InspectionReason", values(typedConsts(t, decisionDir, "InspectionReason")), sorted(InspectionReasons))
	source := sourceDecisionServiceReasons(t)
	for _, status := range ResultStatuses {
		sameSet(t, "decision service reasons for "+string(status),
			source[status], DecisionServiceReasons[status])
	}
	for status := range source {
		if _, ok := DecisionServiceReasons[status]; !ok {
			t.Errorf("decision service writes status %q with reasons %q", status, source[status])
		}
	}
}

func TestParityGatewayErrorCodes(t *testing.T) {
	codes, prefix := sourceGatewayCodes(t)
	sameSet(t, "gateway error codes", codes, GatewayErrorCodes)
	if prefix != GatewayTargetRejectedPrefix {
		t.Errorf("gateway rejected prefix is %q, narrate has %q", prefix, GatewayTargetRejectedPrefix)
	}
	// The mirror and the gateway agree on every code either could accept.
	candidates := append([]string{}, GatewayErrorCodes...)
	for status := 0; status < 1000; status++ {
		candidates = append(candidates,
			fmt.Sprintf("%s%03d", prefix, status), fmt.Sprintf("%s%d", prefix, status))
	}
	candidates = append(candidates, prefix, prefix+"4x0", prefix+"+40", prefix+" 404",
		prefix+"0404", prefix+"404 ", "TARGET_REJECTED_404", "request_not_sent ")
	for _, code := range candidates {
		_, _, known := errorCodeKey(code)
		gateway := effectsgateway.ValidErrorCode(code)
		if known != gateway {
			t.Errorf("code %q: gateway valid=%v, narrate known=%v", code, gateway, known)
		}
	}
	if s, ok := TargetRejectedStatus(effectsgateway.TargetRejected(503)); !ok || s != 503 {
		t.Errorf("TargetRejected(503) parsed as %d, %v", s, ok)
	}
}

func TestParityApprovalStatusTable(t *testing.T) {
	var doc, mirror []string
	for _, row := range docApprovalRows(t) {
		doc = append(doc, row[0]+"/"+row[1])
	}
	for _, row := range EffectiveApprovals {
		mirror = append(mirror, string(row.State)+"/"+string(row.Condition))
	}
	sameSet(t, "docs/approval.md status table", doc, mirror)
}

func TestParityStateMachines(t *testing.T) {
	// Every dispatch transition kind has an edge, and every edge a known kind
	// and states.
	kinds := map[string]bool{}
	for _, edge := range DispatchEdges {
		kinds[edge.Kind] = true
	}
	for _, kind := range sourceTransitionKinds(t) {
		if !kinds[kind] {
			t.Errorf("transition kind %q has no edge", kind)
		}
	}
	states := map[string]bool{"": true}
	for _, s := range sourceDispatchStates(t) {
		states[s] = true
	}
	for _, edge := range DispatchEdges {
		if !states[edge.From] || !states[edge.To] || edge.To == "" {
			t.Errorf("dispatch edge %q names an unknown state", edge.Name)
		}
	}
	// Every stored approval state is reachable, and only the live ones have
	// outgoing edges.
	stored := map[string]bool{"": true}
	for _, s := range sourceApprovalStates(t) {
		if s != string(fleet.ApprovalUnresolvable) {
			stored[s] = true
		}
	}
	reached := map[string]bool{}
	leaves := map[string]bool{}
	for _, edge := range ApprovalEdges {
		if !stored[edge.From] || !stored[edge.To] {
			t.Errorf("approval edge %q names an unknown state", edge.Name)
		}
		reached[edge.To] = true
		leaves[edge.From] = true
	}
	for s := range stored {
		if s != "" && !reached[s] {
			t.Errorf("approval state %q is unreachable", s)
		}
	}
	for _, final := range []fleet.ApprovalState{fleet.ApprovalRefused, fleet.ApprovalExpired, fleet.ApprovalEnqueued} {
		if leaves[string(final)] {
			t.Errorf("final approval state %q has an outgoing edge", final)
		}
	}
}
