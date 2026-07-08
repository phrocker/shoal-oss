package index

import (
	"bytes"
	"fmt"
	"io"

	"github.com/phrocker/shoal/internal/rfile/wire"
)

// Reader is the parsed RFile.index meta block: the version stamp and
// the list of locality groups (main + sample).
//
// Sample / vector / tessellation extensions are recorded as raw byte
// slices when present — Phase 3a doesn't decode them, but doesn't drop
// them either, so a higher-level RFile reader can pass them on.
type Reader struct {
	Version int32

	Groups []*LocalityGroup

	// SampleGroups parallels Groups when the RFile has sample data
	// attached (v8 only). nil when no samples were stored.
	SampleGroups []*LocalityGroup

	// SamplerConfigRaw is the unparsed bytes of the SamplerConfiguration
	// trailer. Phase 3a doesn't crack it open; surfaced as opaque so
	// readers can either ignore or hand off.
	SamplerConfigRaw []byte

	// VectorIndexRaw / TessellationFooterRaw — same opaque-preservation
	// strategy for the v8 vector index extension.
	VectorIndexRaw       []byte
	TessellationFooterRaw []byte
}

// Parse reads an RFile.index meta block from raw (already-decompressed)
// bytes. Callers obtain those bytes by:
//  1. opening a BCFile (`bcfile.NewReader`),
//  2. looking up the meta entry named "RFile.index",
//  3. decompressing it with `block.Decompressor.Block(...)`.
//
// We accept a byte slice rather than an io.Reader so we can both pin
// the trailer bytes (they're variable-length, version-conditional, and
// we'd otherwise need an unboundedly-buffered reader) and hand them off
// to opaque storage above.
func Parse(raw []byte) (*Reader, error) {
	r := bytes.NewReader(raw)

	magic, err := wire.ReadInt32(r)
	if err != nil {
		return nil, fmt.Errorf("RFile.index magic: %w", err)
	}
	if magic != RIndexMagic {
		return nil, fmt.Errorf("%w: got %#08x, want %#08x", ErrBadMagic, magic, RIndexMagic)
	}
	version, err := wire.ReadInt32(r)
	if err != nil {
		return nil, fmt.Errorf("RFile.index version: %w", err)
	}
	if !IsSupportedVersion(version) {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	out := &Reader{Version: version}

	groupCount, err := wire.ReadInt32(r)
	if err != nil {
		return nil, fmt.Errorf("RFile.index group count: %w", err)
	}
	if groupCount < 0 {
		return nil, fmt.Errorf("RFile.index: negative group count %d", groupCount)
	}
	out.Groups = make([]*LocalityGroup, 0, groupCount)
	for i := int32(0); i < groupCount; i++ {
		lg, err := ReadLocalityGroup(r, version)
		if err != nil {
			return nil, fmt.Errorf("RFile.index group %d: %w", i, err)
		}
		out.Groups = append(out.Groups, lg)
	}

	// v8-only trailers: samples + vector index + tessellation. Each is
	// gated by a leading bool.
	if version == V8 {
		if err := readV8Tail(r, out, groupCount); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func readV8Tail(r *bytes.Reader, out *Reader, groupCount int32) error {
	// Samples?
	hasSamples, err := readOptionalBool(r, "hasSamples")
	if err != nil {
		return err
	}
	if hasSamples {
		out.SampleGroups = make([]*LocalityGroup, 0, groupCount)
		for i := int32(0); i < groupCount; i++ {
			lg, err := ReadLocalityGroup(r, V8)
			if err != nil {
				return fmt.Errorf("RFile.index sample group %d: %w", i, err)
			}
			out.SampleGroups = append(out.SampleGroups, lg)
		}
		// SamplerConfiguration follows. We don't parse it; capture the
		// remaining bytes up to the next optional flag (vector index).
		// Java reads it via SamplerConfigurationImpl(mb), which consumes
		// a known-shaped record — but that record's size is variable and
		// only knowable by parsing it. For now we error out if a sample
		// is present, since shoal V0 doesn't need samples; flip this to
		// real parsing when sample-aware reads matter.
		return fmt.Errorf("rfile/index: sample groups present but SamplerConfiguration parsing is not yet implemented")
	}
	// Vector index?
	hasVector, err := readOptionalBool(r, "hasVectorIndex")
	if err != nil {
		return err
	}
	if hasVector {
		// VectorIndex has its own readFields; until ported, capture the
		// remainder. This is loss-of-information but not data corruption
		// — callers that need vectors get an explicit error elsewhere.
		remaining, _ := io.ReadAll(r)
		out.VectorIndexRaw = remaining
		return nil
	}
	return nil
}

// readOptionalBool returns false-without-error on EOF. v8 RFiles
// produced by older writers may end before the optional-trailer flags;
// Java treats that as "no trailer present" via mb.available()>0 checks.
// We mirror that.
func readOptionalBool(r *bytes.Reader, what string) (bool, error) {
	if r.Len() == 0 {
		return false, nil
	}
	b, err := wire.ReadBool(r)
	if err != nil {
		return false, fmt.Errorf("RFile.index %s: %w", what, err)
	}
	return b, nil
}
