//go:build linux || darwin

package local

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func preservePlatformXattrs(temp, target string) error {
	names, err := listXattrs(target)
	if err != nil {
		return fmt.Errorf("local: list extended attributes for %s: %w", target, err)
	}
	for _, name := range names {
		value, err := getXattr(target, name)
		if err != nil {
			return fmt.Errorf("local: read extended attribute %s for %s: %w", name, target, err)
		}
		if err := unix.Setxattr(temp, name, value, 0); err != nil {
			return fmt.Errorf("local: preserve extended attribute %s for %s: %w", name, target, err)
		}
	}
	return nil
}

func listXattrs(path string) ([]string, error) {
	size, err := unix.Listxattr(path, nil)
	if err != nil {
		if errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.EOPNOTSUPP) {
			return nil, nil
		}
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	n, err := unix.Listxattr(path, buf)
	if err != nil {
		return nil, err
	}
	return splitXattrNames(buf[:n]), nil
}

func getXattr(path, name string) ([]byte, error) {
	size, err := unix.Getxattr(path, name, nil)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, size)
	n, err := unix.Getxattr(path, name, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func splitXattrNames(raw []byte) []string {
	parts := strings.Split(string(raw), "\x00")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
