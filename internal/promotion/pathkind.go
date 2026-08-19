package promotion

import (
	pathpkg "path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/phrocker/shoal/internal/storage"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

var windowsDrivePathRe = regexp.MustCompile(`^[A-Za-z]:(?:[\\/].*)?$`)
var publicationCaseFold = cases.Fold()

// Win32 short names permit a wider ASCII punctuation set than the original
// [A-Za-z0-9_] matcher used here. Accepting this conservative superset avoids
// false negatives for literal short-name spellings such as LONG$F~1.RF. "~" is
// intentionally excluded from the body-character set below because this matcher
// reserves it for the generated-short-name ordinal separator; components
// containing "~" are conservatively still treated as alias-family candidates by
// parseDOS83LiteralComponent / dos83AliasFamilyComponent rather than as
// definitely-plain 8.3 names.
const dos83BodyPunctuation = "!#$%&'()-@^_`{}"

type localPublicationIdentity struct {
	normalizedKey string
	prefix        string
	components    []localPublicationComponentIdentity
}

type localPublicationComponentIdentity struct {
	normalized          string
	dos83Prefix         string
	dos83Ext            string
	isDOS83Literal      bool
	hasDOS83AliasFamily bool
}

func looksLikeWindowsDrivePath(path string) bool {
	return windowsDrivePathRe.MatchString(path)
}

// pathUsesBackendSeparatorJoin reports whether path is a backend URL
// root that must be joined with a literal "/" (mirroring engine's own
// joinBackendPath) rather than filepath.Join. This is deliberately
// generic -- any scheme://... path, not only the four backends this
// package knows how to canonicalize (s3/gs/az/hdfs) -- so a custom or
// future backend whose paths use their own URI scheme (for example a
// test-only or in-memory backend) still joins and validates correctly
// instead of silently falling through to a native filepath.Join that
// would collapse the scheme's "//" or use OS-native separators.
// pathLooksURLLike already excludes Windows drive paths such as
// "C://data" from looking like a URL. hdfs:/ (single slash, no
// authority) is also treated as backend-style even though it has no
// "://" substring, matching Hadoop's own authorityless HDFS URI form.
func pathUsesBackendSeparatorJoin(path string) bool {
	return pathUsesBackendSeparatorJoinOnBackend(nil, path)
}

func pathLooksURLLike(path string) bool {
	return pathLooksURLLikeOnBackend(nil, path)
}

func pathUsesBackendSeparatorJoinOnBackend(backend storage.Backend, path string) bool {
	return storage.UsesBackendPathJoin(backend, path)
}

func pathLooksURLLikeOnBackend(backend storage.Backend, path string) bool {
	return storage.ExplicitPathScheme(backend, path) != ""
}

func normalizeLocalPathForAlias(path string) string {
	if normalized, ok := normalizeWindowsDrivePath(path); ok {
		return normalized
	}
	return filepath.Clean(path)
}

func normalizeWindowsDrivePath(path string) (string, bool) {
	if !looksLikeWindowsDrivePath(path) {
		return "", false
	}

	drive := strings.ToUpper(path[:1]) + ":"
	rest := strings.ReplaceAll(path[2:], `\`, `/`)
	rest = strings.TrimLeft(rest, "/")

	cleaned := pathpkg.Clean("/" + rest)
	if cleaned == "." {
		cleaned = "/"
	}
	return drive + strings.ReplaceAll(cleaned, "/", `\`), true
}

func isWindowsDriveRootPath(path string) bool {
	normalized, ok := normalizeWindowsDrivePath(path)
	return ok && len(normalized) == 3 && normalized[1] == ':' && normalized[2] == '\\'
}

// normalizeLocalPublicationComponent normalizes a single local path
// component for collision-safe publication-key comparison. NFC
// normalization and case-folding apply on every platform because they
// conservatively catch aliases on filesystems (Windows, and macOS's
// default HFS+/APFS) that fold case and normalize Unicode spellings;
// treating two components as equal when they might not be on some
// other filesystem only causes an unnecessary staging rejection, never
// a missed collision. Trailing-dot/space stripping is different: Win32
// silently discards trailing dots and spaces from every path component
// (so "A.rf", "A.rf.", and "A.rf " all name the same not-yet-created
// NTFS file), but POSIX filesystems store them as literal, significant
// bytes -- "A.rf." is a genuinely different filename from "A.rf" on
// Linux and macOS. Applying that stripping unconditionally would
// therefore treat truly distinct destination files as aliases outside
// Windows, so it is gated to runtime.GOOS == "windows".
func normalizeLocalPublicationComponent(component string) string {
	component = norm.NFC.String(component)
	if runtime.GOOS == "windows" {
		component = strings.TrimRight(component, " .")
	}
	return publicationCaseFold.String(component)
}

func buildLocalPublicationIdentity(path string) localPublicationIdentity {
	prefix, parts := splitLocalPath(path)
	components := make([]localPublicationComponentIdentity, len(parts))
	normalizedParts := make([]string, len(parts))
	for i, part := range parts {
		components[i] = buildLocalPublicationComponentIdentity(part)
		normalizedParts[i] = components[i].normalized
	}
	normalizedPrefix := normalizeLocalPublicationPrefix(prefix)
	return localPublicationIdentity{
		normalizedKey: normalizedPrefix + strings.Join(normalizedParts, "/"),
		prefix:        normalizedPrefix,
		components:    components,
	}
}

func normalizeLocalPublicationPrefix(prefix string) string {
	prefix = strings.ReplaceAll(prefix, `\`, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return prefix
}

func buildLocalPublicationComponentIdentity(component string) localPublicationComponentIdentity {
	normalized := normalizeLocalPublicationComponent(component)
	identity := localPublicationComponentIdentity{normalized: normalized}
	if runtime.GOOS != "windows" {
		return identity
	}
	if prefix, ext, ok := parseDOS83LiteralComponent(normalized); ok {
		identity.dos83Prefix = prefix
		identity.dos83Ext = ext
		identity.isDOS83Literal = true
		return identity
	}
	if prefix, ext, ok := dos83AliasFamilyComponent(normalized); ok {
		identity.dos83Prefix = prefix
		identity.dos83Ext = ext
		identity.hasDOS83AliasFamily = true
	}
	return identity
}

// dos83PathAliases conservatively rejects Windows DOS 8.3 short-name
// ambiguities for not-yet-created local paths. It intentionally catches
// only literal short-name spellings (for example LONGFI~1.RF) against a
// longer component that could generate that short-name family; two long
// components sharing a six-character prefix are not treated as aliases
// because NTFS assigns them distinct ~n ordinals rather than one path
// truncating the other.
func dos83PathAliases(left, right localPublicationIdentity) bool {
	if runtime.GOOS != "windows" {
		return false
	}
	if left.prefix != right.prefix || len(left.components) != len(right.components) {
		return false
	}
	usedDOS83 := false
	for i := range left.components {
		if left.components[i].normalized == right.components[i].normalized {
			continue
		}
		if !dos83ComponentsAlias(left.components[i], right.components[i]) {
			return false
		}
		usedDOS83 = true
	}
	return usedDOS83
}

func dos83ComponentsAlias(left, right localPublicationComponentIdentity) bool {
	return dos83LiteralMatchesFamily(left, right) || dos83LiteralMatchesFamily(right, left)
}

func dos83LiteralMatchesFamily(literal, family localPublicationComponentIdentity) bool {
	return literal.isDOS83Literal &&
		family.hasDOS83AliasFamily &&
		literal.dos83Prefix == family.dos83Prefix &&
		literal.dos83Ext == family.dos83Ext
}

func parseDOS83LiteralComponent(component string) (string, string, bool) {
	stem, ext := splitDOS83Component(component)
	tilde := strings.LastIndexByte(stem, '~')
	if tilde <= 0 || tilde > 6 || tilde >= len(stem)-1 {
		return "", "", false
	}
	prefix := stem[:tilde]
	ordinal := stem[tilde+1:]
	if !isDOS83Token(prefix, 1, 6) || strings.Contains(prefix, "~") {
		return "", "", false
	}
	if ordinal[0] == '0' || !isASCIIUnsignedInteger(ordinal) {
		return "", "", false
	}
	if !isDOS83Token(ext, 0, 3) {
		return "", "", false
	}
	return prefix, ext, true
}

func dos83AliasFamilyComponent(component string) (string, string, bool) {
	stem, ext := splitDOS83Component(component)
	if isPlainDOS83Component(stem, ext) {
		return "", "", false
	}
	prefix := sanitizeDOS83Token(stem)
	if prefix == "" {
		return "", "", false
	}
	if len(prefix) > 6 {
		prefix = prefix[:6]
	}
	ext = sanitizeDOS83Token(ext)
	if len(ext) > 3 {
		ext = ext[:3]
	}
	return prefix, ext, true
}

func splitDOS83Component(component string) (string, string) {
	lastDot := strings.LastIndexByte(component, '.')
	if lastDot <= 0 {
		return component, ""
	}
	return component[:lastDot], component[lastDot+1:]
}

func isPlainDOS83Component(stem, ext string) bool {
	if strings.Contains(stem, ".") {
		return false
	}
	return isDOS83Token(stem, 1, 8) && isDOS83Token(ext, 0, 3)
}

func isDOS83Token(token string, minLen, maxLen int) bool {
	if len(token) < minLen || len(token) > maxLen {
		return false
	}
	for i := 0; i < len(token); i++ {
		if !isDOS83Char(token[i]) {
			return false
		}
	}
	return true
}

func sanitizeDOS83Token(token string) string {
	var builder strings.Builder
	builder.Grow(len(token))
	for i := 0; i < len(token); i++ {
		if isDOS83Char(token[i]) {
			builder.WriteByte(token[i])
		}
	}
	return builder.String()
}

func isDOS83Char(b byte) bool {
	if (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') {
		return true
	}
	return strings.ContainsRune(dos83BodyPunctuation, rune(b))
}

func isASCIIUnsignedInteger(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}
