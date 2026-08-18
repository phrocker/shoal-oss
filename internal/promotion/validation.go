package promotion

import (
	"fmt"
	"net/url"
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
		return strings.Trim(u.Path, "/\\") == ""
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
