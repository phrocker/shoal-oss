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

func pathUsesBackendSeparatorJoin(path string) bool {
	switch explicitBackendScheme(path) {
	case "s3", "gs", "az":
		return true
	case "hdfs":
		return strings.HasPrefix(path, "hdfs:/")
	default:
		return false
	}
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
