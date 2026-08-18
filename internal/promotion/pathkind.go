package promotion

import (
	pathpkg "path"
	"path/filepath"
	"regexp"
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

func normalizeLocalPublicationComponent(component string) string {
	component = norm.NFC.String(component)
	component = strings.TrimRight(component, " .")
	return publicationCaseFold.String(component)
}
