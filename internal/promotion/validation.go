package promotion

import (
	"fmt"
	"net/url"
	pathpkg "path"
	"path/filepath"
	"strings"

	"github.com/phrocker/shoal/accumulo"
	"github.com/phrocker/shoal/internal/storage"
)

func validatePromotionDestination(dst storage.Backend, tableName, bulkDir string) error {
	if err := validateTableName(tableName); err != nil {
		return err
	}
	return validateBulkDirOnBackend(dst, bulkDir)
}

func validateTableName(tableName string) error {
	trimmed := strings.TrimSpace(tableName)
	if trimmed == "" {
		return fmt.Errorf("%w: empty table name", accumulo.ErrInvalidTableName)
	}
	if trimmed != tableName {
		return fmt.Errorf("%w: %q has leading or trailing whitespace", accumulo.ErrInvalidTableName, tableName)
	}
	return nil
}

func validateBulkDir(bulkDir string) error {
	return validateBulkDirOnBackend(nil, bulkDir)
}

func validateBulkDirOnBackend(dst storage.Backend, bulkDir string) error {
	trimmed := strings.TrimSpace(bulkDir)
	if trimmed == "" {
		return fmt.Errorf("%w: empty bulk directory", accumulo.ErrInvalidBulkDir)
	}
	if trimmed != bulkDir {
		return fmt.Errorf("%w: %q has leading or trailing whitespace", accumulo.ErrInvalidBulkDir, bulkDir)
	}
	if isBackendRootOnBackend(dst, trimmed) {
		return fmt.Errorf("%w: backend root %q", accumulo.ErrInvalidBulkDir, bulkDir)
	}
	return nil
}

func isBackendRoot(bulkDir string) bool {
	return isBackendRootOnBackend(nil, bulkDir)
}

func isBackendRootOnBackend(dst storage.Backend, bulkDir string) bool {
	if pathUsesBackendSeparatorJoinOnBackend(dst, bulkDir) {
		u, err := url.Parse(bulkDir)
		if err != nil {
			return false
		}
		p := u.Path
		if storage.ExplicitPathScheme(dst, bulkDir) == "hdfs" {
			p = hdfsCleanRootPath(p)
		}
		return strings.Trim(p, "/\\") == ""
	}
	if isWindowsDriveRootPath(bulkDir) {
		return true
	}
	clean := filepath.Clean(bulkDir)
	sep := string(filepath.Separator)
	if clean == sep {
		return true
	}
	vol := filepath.VolumeName(clean)
	return vol != "" && clean == vol+sep
}

// hdfsCleanRootPath normalizes dot segments ("." and "..") in an HDFS URI
// path per HDFS/POSIX path semantics, so a root alias like "/tmp/.." --
// which HDFS itself resolves to "/" -- is recognized as the backend root
// even though its literal spelling is not empty. pathpkg.Clean maps an
// empty path to ".", which is mapped back to "" here so a bare authority
// root (hdfs://nn, with no path segment at all) still trims to the empty
// string and is correctly recognized as root. This normalization is
// intentionally scoped to the HDFS scheme by the caller: object-store and
// other custom backend URL schemes skip it, since their keys may
// legitimately contain literal "." or ".." segments that are not
// directory-traversal tokens.
func hdfsCleanRootPath(p string) string {
	cleaned := pathpkg.Clean(p)
	if cleaned == "." {
		return ""
	}
	return cleaned
}
