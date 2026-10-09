package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const MaxLayaManifestArtifacts = 256

// LayaArtifact identifies one immutable file used by a local worker. Paths
// are relative to the provisioned artifact root and digests are SHA-256.
type LayaArtifact struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// LayaArtifactManifest is the verified artifact set for one pinned revision.
// It is intentionally independent of a model registry or hosted service.
type LayaArtifactManifest struct {
	Revision  string         `json:"revision"`
	Artifacts []LayaArtifact `json:"artifacts"`
}

func (m LayaArtifactManifest) Validate() error {
	if strings.TrimSpace(m.Revision) == "" || len(m.Artifacts) == 0 || len(m.Artifacts) > MaxLayaManifestArtifacts {
		return fmt.Errorf("%w: invalid Laya artifact manifest", ErrInvalidConfig)
	}
	seen := make(map[string]struct{}, len(m.Artifacts))
	for _, artifact := range m.Artifacts {
		path := filepath.Clean(artifact.Path)
		if artifact.Path == "" || path != artifact.Path || filepath.IsAbs(path) || path == "." || path == ".." || strings.HasPrefix(path, ".."+string(filepath.Separator)) {
			return fmt.Errorf("%w: artifact path must be relative and clean", ErrInvalidConfig)
		}
		if _, ok := seen[path]; ok {
			return fmt.Errorf("%w: duplicate artifact path", ErrInvalidConfig)
		}
		seen[path] = struct{}{}
		if artifact.Size < 0 || len(artifact.SHA256) != sha256.Size*2 {
			return fmt.Errorf("%w: invalid artifact metadata", ErrInvalidConfig)
		}
		if _, err := hex.DecodeString(artifact.SHA256); err != nil {
			return fmt.Errorf("%w: invalid artifact digest", ErrInvalidConfig)
		}
		if strings.ToLower(artifact.SHA256) != artifact.SHA256 {
			return fmt.Errorf("%w: artifact digest must be lowercase", ErrInvalidConfig)
		}
	}
	return nil
}

// Digest returns a stable content digest of the manifest metadata. Artifact
// order is canonicalized so callers can persist and compare it safely.
func (m LayaArtifactManifest) Digest() (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}
	copyManifest := m
	copyManifest.Artifacts = append([]LayaArtifact(nil), m.Artifacts...)
	sort.Slice(copyManifest.Artifacts, func(i, j int) bool { return copyManifest.Artifacts[i].Path < copyManifest.Artifacts[j].Path })
	encoded, err := json.Marshal(copyManifest)
	if err != nil {
		return "", fmt.Errorf("%w: encode artifact manifest", ErrInvalidConfig)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// VerifyLayaArtifacts checks the exact files used by a local worker before it
// is allowed to serve. It rejects missing, substituted, symlinked, or changed
// files and requires the manifest revision to match the pinned identity.
func VerifyLayaArtifacts(root string, manifest LayaArtifactManifest, identity TypedIdentity) error {
	if err := validateIdentity(identity); err != nil {
		return err
	}
	if manifest.Revision != identity.Revision {
		return fmt.Errorf("%w: artifact revision does not match model identity", ErrTypedContract)
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(root) == "" {
		return fmt.Errorf("%w: artifact root is required", ErrInvalidConfig)
	}
	for _, artifact := range manifest.Artifacts {
		path := filepath.Join(root, artifact.Path)
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("%w: artifact %s: %v", ErrUnavailable, artifact.Path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: artifact %s is not a regular file", ErrTypedContract, artifact.Path)
		}
		if info.Size() != artifact.Size {
			return fmt.Errorf("%w: artifact %s size mismatch", ErrTypedContract, artifact.Path)
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("%w: artifact %s: %v", ErrUnavailable, artifact.Path, err)
		}
		digest := sha256.New()
		_, copyErr := io.Copy(digest, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return fmt.Errorf("%w: read artifact %s", ErrUnavailable, artifact.Path)
		}
		if !strings.EqualFold(hex.EncodeToString(digest.Sum(nil)), artifact.SHA256) {
			return fmt.Errorf("%w: artifact %s digest mismatch", ErrTypedContract, artifact.Path)
		}
	}
	return nil
}
