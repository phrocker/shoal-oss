package promotion

import (
	pathpkg "path"
	"path/filepath"
	"regexp"
	"strings"
)

var windowsDrivePathRe = regexp.MustCompile(`^[A-Za-z]:(?:[\\/].*)?$`)

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
	if pathLooksURLLike(path) {
		return true
	}
	return strings.HasPrefix(path, "hdfs:/")
}

func pathLooksURLLike(path string) bool {
	return strings.Contains(path, "://") && !looksLikeWindowsDrivePath(path)
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
