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

// See doc.go for the package-level doc comment.
package deploy

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// yamlDoc is one `---`-delimited document from a multi-document plain YAML
// file, along with its kind/name pulled out via a top-level-field regexp
// (deliberately not a full YAML parse — see the package doc comment).
type yamlDoc struct {
	kind string
	name string
	text string
}

var (
	kindPattern = regexp.MustCompile(`(?m)^kind:\s*(\S+)\s*$`)
	namePattern = regexp.MustCompile(`(?m)^\s*name:\s*(\S+)\s*$`)
)

// splitYAMLDocuments splits content on lines that are exactly "---" (a
// YAML document separator), which is sufficient for these manifests: none
// of them contain a literal "---" inside a string value.
func splitYAMLDocuments(content string) []yamlDoc {
	var docs []yamlDoc
	var cur strings.Builder
	flush := func() {
		text := cur.String()
		if strings.TrimSpace(text) != "" {
			doc := yamlDoc{text: text}
			if m := kindPattern.FindStringSubmatch(text); m != nil {
				doc.kind = m[1]
			}
			// The first top-level "name:" in a document is metadata.name
			// for every manifest in this repository (none of these
			// documents have another unindented "name:" field before it).
			if m := namePattern.FindStringSubmatch(text); m != nil {
				doc.name = m[1]
			}
			docs = append(docs, doc)
		}
		cur.Reset()
	}
	for _, line := range strings.Split(content, "\n") {
		if strings.TrimSpace(line) == "---" {
			flush()
			continue
		}
		cur.WriteString(line)
		cur.WriteString("\n")
	}
	flush()
	return docs
}

func readManifest(t *testing.T, relPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(relPath))
	if err != nil {
		t.Fatalf("read %s: %v", relPath, err)
	}
	return string(b)
}

// requireDoc finds the single document with the given kind and metadata
// name, failing the test with the full list of (kind, name) pairs found if
// it's missing — so a failure immediately shows what the file actually
// contains instead of just "not found".
func requireDoc(t *testing.T, docs []yamlDoc, kind, name string) yamlDoc {
	t.Helper()
	for _, d := range docs {
		if d.kind == kind && d.name == name {
			return d
		}
	}
	var found []string
	for _, d := range docs {
		found = append(found, d.kind+"/"+d.name)
	}
	t.Fatalf("no %s named %q found; documents present: %v", kind, name, found)
	return yamlDoc{}
}

func requireContains(t *testing.T, doc yamlDoc, want string) {
	t.Helper()
	if !strings.Contains(doc.text, want) {
		t.Errorf("%s/%s: want to contain %q, got:\n%s", doc.kind, doc.name, want, doc.text)
	}
}

// TestWriteTierManifestHasDisruptionAndRolloutHardening guards the
// write-tier plain-YAML manifest's disruption/rollout contract: a
// PodDisruptionBudget exists and targets the StatefulSet's own labels, and
// the StatefulSet declares its rollout strategy explicitly instead of
// relying on the apps/v1 default. It also re-asserts the pre-existing
// security/health hardening from PR #79/#100 to catch accidental removal.
func TestWriteTierManifestHasDisruptionAndRolloutHardening(t *testing.T) {
	docs := splitYAMLDocuments(readManifest(t, "k8s/write-tier.yaml"))
	if len(docs) != 3 {
		t.Fatalf("write-tier.yaml has %d documents, want 3 (Service, StatefulSet, PodDisruptionBudget)", len(docs))
	}

	sts := requireDoc(t, docs, "StatefulSet", "shoal-embed")
	requireContains(t, sts, "updateStrategy:")
	requireContains(t, sts, "runAsNonRoot: true")
	requireContains(t, sts, "allowPrivilegeEscalation: false")
	requireContains(t, sts, `drop: ["ALL"]`)
	requireContains(t, sts, "seccompProfile:")
	requireContains(t, sts, "readinessProbe:")
	requireContains(t, sts, "livenessProbe:")
	requireContains(t, sts, "terminationGracePeriodSeconds: 50")

	pdb := requireDoc(t, docs, "PodDisruptionBudget", "shoal-embed")
	requireContains(t, pdb, "maxUnavailable: 0")
	requireContains(t, pdb, "app.kubernetes.io/component: write-tier")
}

// TestReadFleetManifestHasDisruptionAndRolloutHardening is
// TestWriteTierManifestHasDisruptionAndRolloutHardening's read-fleet
// counterpart. It deliberately does NOT assert an HTTP health surface for
// this tier: cmd/shoal (the read-fleet binary) has no HTTP health/ready
// endpoints today, so the manifest correctly still uses tcpSocket probes —
// asserting otherwise here would overclaim a readiness contract this tier
// doesn't have.
func TestReadFleetManifestHasDisruptionAndRolloutHardening(t *testing.T) {
	docs := splitYAMLDocuments(readManifest(t, "k8s/read-fleet.yaml"))
	if len(docs) != 3 {
		t.Fatalf("read-fleet.yaml has %d documents, want 3 (Service, Deployment, PodDisruptionBudget)", len(docs))
	}

	dep := requireDoc(t, docs, "Deployment", "shoal-read")
	requireContains(t, dep, "strategy:")
	requireContains(t, dep, "maxUnavailable: 1")
	requireContains(t, dep, "maxSurge: 1")
	requireContains(t, dep, "podAntiAffinity:")
	requireContains(t, dep, "tcpSocket:")
	if strings.Contains(dep.text, "httpGet:") {
		t.Error("read-fleet Deployment now has an httpGet probe; if cmd/shoal gained an HTTP health surface, update this test (and deploy/README.md's readiness-contract section) to match — don't leave this assertion stale")
	}

	pdb := requireDoc(t, docs, "PodDisruptionBudget", "shoal-read")
	requireContains(t, pdb, "maxUnavailable: 1")
	requireContains(t, pdb, "app.kubernetes.io/component: read-fleet")
}

// TestPlainManifestsCarryNoUnconditionalTLS guards the plain-YAML
// documentation-only TLS example in write-tier.yaml: since plain
// manifests have no templating/conditionals, the only safe way to
// document the opt-in flags there is as commented-out YAML, not live
// config. This fails loudly if that guidance is ever uncommented without
// updating this test (and the accompanying Secret/volume) deliberately.
func TestPlainManifestsCarryNoUnconditionalTLS(t *testing.T) {
	content := readManifest(t, "k8s/write-tier.yaml")
	for _, line := range strings.Split(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(trimmed, "--tls-cert=") || strings.Contains(trimmed, "--tls-key=") || strings.Contains(trimmed, "--tls-client-ca=") {
			t.Errorf("found an uncommented TLS flag in write-tier.yaml: %q; the plain manifest's TLS example must stay commented-out documentation (see deploy/README.md's TLS section) since it has no Secret provisioned by default", trimmed)
		}
	}
}

// TestHelmValuesDeclareDisruptionRolloutAndTLSDefaults is
// values.yaml's regression guard: the new keys backing PDBs, explicit
// rollout strategies, and the TLS opt-in toggle must stay present and
// off/safe by default (podDisruptionBudget enabled with a conservative
// budget is a safety default, not a functional opt-in the way TLS is).
func TestHelmValuesDeclareDisruptionRolloutAndTLSDefaults(t *testing.T) {
	values := readManifest(t, "helm/shoal/values.yaml")
	for _, want := range []string{
		"updateStrategy:",
		"partition: 0",
		"podDisruptionBudget:",
		"tls:",
		"enabled: false",
		"strategy:",
		"maxUnavailable: 1",
		"maxSurge: 1",
	} {
		if !strings.Contains(values, want) {
			t.Errorf("helm/shoal/values.yaml: want to contain %q", want)
		}
	}
}

// TestHelmWriteTierTLSProbeTemplates guards the chart-specific probe wiring
// needed once the write tier's health listener is TLS-wrapped. Plain TLS
// needs kubelet HTTP probes to use HTTPS; mutual TLS cannot use kubelet
// httpGet probes because kubelet does not present a client certificate, so
// the chart intentionally falls back to TCP reachability probes in that mode.
func TestHelmWriteTierTLSProbeTemplates(t *testing.T) {
	template := readManifest(t, "helm/shoal/templates/embed-statefulset.yaml")
	for _, want := range []string{
		"scheme: HTTPS",
		"if and .Values.writeTier.tls.enabled .Values.writeTier.tls.requireClientCert",
		"tcpSocket:",
	} {
		if !strings.Contains(template, want) {
			t.Errorf("helm/shoal/templates/embed-statefulset.yaml: want to contain %q", want)
		}
	}
}

// TestHelmChartLintsAndRenders runs `helm lint` and `helm template`
// (default values, then TLS + mutual-TLS enabled) against the chart when
// a `helm` binary is available, and fails on any non-zero exit or on
// template output that doesn't parse as well-formed multi-document YAML
// (checked the same dependency-free way as the plain manifests above:
// every document must have a recognizable "kind:" line). It skips
// entirely when helm isn't on PATH — true in this repository's CI today —
// so this test only ever strengthens local/future-CI coverage, never
// blocks it.
func TestHelmChartLintsAndRenders(t *testing.T) {
	helmPath, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm binary not found on PATH; skipping (CI currently has no helm installed either — see .github/workflows/ci.yml)")
	}

	if out, err := exec.Command(helmPath, "lint", "helm/shoal").CombinedOutput(); err != nil {
		t.Fatalf("helm lint helm/shoal: %v\n%s", err, out)
	}

	cases := []struct {
		name string
		args []string
	}{
		{name: "defaults"},
		{name: "tls and mTLS enabled", args: []string{
			"--set", "writeTier.tls.enabled=true",
			"--set", "writeTier.tls.secretName=shoal-embed-tls",
			"--set", "writeTier.tls.requireClientCert=true",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"template", "shoal", "helm/shoal"}, tc.args...)
			out, err := exec.Command(helmPath, args...).CombinedOutput()
			if err != nil {
				t.Fatalf("helm %s: %v\n%s", strings.Join(args, " "), err, out)
			}
			docs := splitYAMLDocuments(string(out))
			if len(docs) == 0 {
				t.Fatal("helm template produced no documents")
			}
			for _, d := range docs {
				if d.kind == "" {
					t.Errorf("a rendered document has no recognizable \"kind:\" line:\n%s", d.text)
				}
			}
		})
	}
}
