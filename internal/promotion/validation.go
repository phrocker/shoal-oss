package promotion

import (
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/phrocker/shoal/accumulo"
)

var windowsDriveRootRe = regexp.MustCompile(`^[A-Za-z]:[\\/]?$`)

func validatePromotionDestination(tableName, bulkDir string) error {
	if err := validateTableName(tableName); err != nil {
		return err
	}
	return validateBulkDir(bulkDir)
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
	trimmed := strings.TrimSpace(bulkDir)
	if trimmed == "" {
		return fmt.Errorf("%w: empty bulk directory", accumulo.ErrInvalidBulkDir)
	}
	if trimmed != bulkDir {
		return fmt.Errorf("%w: %q has leading or trailing whitespace", accumulo.ErrInvalidBulkDir, bulkDir)
	}
	if isBackendRoot(trimmed) {
		return fmt.Errorf("%w: backend root %q", accumulo.ErrInvalidBulkDir, bulkDir)
	}
	return nil
}

func isBackendRoot(bulkDir string) bool {
	if strings.Contains(bulkDir, "://") {
		u, err := url.Parse(bulkDir)
		if err != nil {
			return false
		}
		return strings.Trim(u.Path, "/\\") == ""
	}
	if windowsDriveRootRe.MatchString(bulkDir) {
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
