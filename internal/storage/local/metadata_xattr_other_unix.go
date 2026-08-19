//go:build unix && !plan9 && !linux && !darwin

package local

func preservePlatformXattrs(_, _ string) error {
	return nil
}
