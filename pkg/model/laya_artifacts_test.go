package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func artifactTestManifest(t *testing.T, root string) LayaArtifactManifest {
	t.Helper()
	path := filepath.Join(root, "weights.bin")
	data := []byte("pinned weights")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return LayaArtifactManifest{Revision: "rev-1", Artifacts: []LayaArtifact{{Path: "weights.bin", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}}}
}

func TestLayaArtifactManifestDigestIsOrderIndependent(t *testing.T) {
	m := LayaArtifactManifest{Revision: "rev-1", Artifacts: []LayaArtifact{{Path: "b", Size: 0, SHA256: strings.Repeat("b", 64)}, {Path: "a", Size: 0, SHA256: strings.Repeat("a", 64)}}}
	first, err := m.Digest()
	if err != nil {
		t.Fatal(err)
	}
	m.Artifacts[0], m.Artifacts[1] = m.Artifacts[1], m.Artifacts[0]
	second, err := m.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("digest changed with ordering: %s != %s", first, second)
	}
}

func TestVerifyLayaArtifactsPinsRevisionAndBytes(t *testing.T) {
	root := t.TempDir()
	manifest := artifactTestManifest(t, root)
	identity := testTypedIdentity()
	identity.Revision = "rev-1"
	if err := VerifyLayaArtifacts(root, manifest, identity); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLayaArtifacts(root, manifest, testTypedIdentity()); err == nil {
		t.Fatal("revision mismatch accepted")
	}
	if err := os.WriteFile(filepath.Join(root, "weights.bin"), []byte("substituted"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyLayaArtifacts(root, manifest, identity); err == nil {
		t.Fatal("substituted artifact accepted")
	}
}

func TestNewVerifiedLayaPredictorChecksBeforeConstruction(t *testing.T) {
	root := t.TempDir()
	manifest := artifactTestManifest(t, root)
	identity := testTypedIdentity()
	identity.Revision = "rev-1"
	predictor, err := NewVerifiedLayaPredictor(LayaConfig{BaseURL: "http://worker.invalid", BearerToken: "token", Identity: identity}, root, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if predictor.Identity() != identity {
		t.Fatalf("identity = %+v, want %+v", predictor.Identity(), identity)
	}
	manifest.Artifacts[0].SHA256 = strings.Repeat("a", 64)
	if _, err := NewVerifiedLayaPredictor(LayaConfig{BaseURL: "http://worker.invalid", BearerToken: "token", Identity: identity}, root, manifest); err == nil {
		t.Fatal("unverified artifact set accepted")
	}
}

func TestLayaArtifactManifestRejectsTraversalAndSymlink(t *testing.T) {
	bad := LayaArtifactManifest{Revision: "rev", Artifacts: []LayaArtifact{{Path: "../weights", Size: 0, SHA256: strings.Repeat("a", 64)}}}
	if err := bad.Validate(); err == nil {
		t.Fatal("path traversal accepted")
	}
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "weights")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	m := LayaArtifactManifest{Revision: "rev-1", Artifacts: []LayaArtifact{{Path: "weights", Size: 0, SHA256: strings.Repeat("a", 64)}}}
	identity := testTypedIdentity()
	identity.Revision = "rev-1"
	if err := VerifyLayaArtifacts(root, m, identity); err == nil {
		t.Fatal("symlink artifact accepted")
	}
}

func TestLoadLayaArtifactManifestIsStrictAndBounded(t *testing.T) {
	root := t.TempDir()
	manifest := artifactTestManifest(t, root)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadLayaArtifactManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != manifest.Revision || len(loaded.Artifacts) != 1 {
		t.Fatalf("loaded = %+v", loaded)
	}
	if err := os.WriteFile(path, append(encoded, []byte(`{"extra":true}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLayaArtifactManifest(path); err == nil {
		t.Fatal("trailing JSON accepted")
	}
	if err := os.WriteFile(path, []byte(`{"revision":"rev-1","artifacts":[],"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLayaArtifactManifest(path); err == nil {
		t.Fatal("unknown field accepted")
	}
	if err := os.WriteFile(path, []byte(`{"revision":"rev-1","revision":"rev-2","artifacts":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLayaArtifactManifest(path); err == nil {
		t.Fatal("duplicate field accepted")
	}
}
