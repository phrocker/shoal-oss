package promotion

import (
	pathpkg "path"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

var windowsDrivePathRe = regexp.MustCompile(`^[A-Za-z]:(?:[\\/].*)?$`)
var urlStylePathRe = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9+.-]*):\/\/`)
var publicationCaseFold = cases.Fold()

func looksLikeWindowsDrivePath(path string) bool {
	return windowsDrivePathRe.MatchString(path)
}

func pathUsesBackendSeparatorJoin(path string) bool {
	if looksLikeWindowsDrivePath(path) {
		return false
	}
	if strings.HasPrefix(path, "hdfs:/") {
		return true
	}
	return urlStylePathRe.MatchString(path)
}

func pathLooksURLLike(path string) bool {
	return pathUsesBackendSeparatorJoin(path)
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

func normalizeLocalPublicationComponent(component string) string {
	component = norm.NFC.String(component)
	component = strings.TrimRight(component, " .")
	return publicationCaseFold.String(component)
}
