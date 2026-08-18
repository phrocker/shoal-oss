//go:build !windows && !linux && !darwin

package local

func preservePlatformXattrs(_, _ string) error {
	return nil
}
