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
