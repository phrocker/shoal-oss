//go:build aix || dragonfly || freebsd || netbsd || openbsd || solaris

package local

func preservePlatformXattrs(_, _ string) error {
	return nil
}
