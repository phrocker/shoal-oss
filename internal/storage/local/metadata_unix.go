//go:build !windows

package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func preservePlatformMetadata(temp, target string) error {
	targetInfo, err := os.Lstat(target)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("local: inspect existing metadata for %s: %w", target, err)
	}
	tempInfo, err := os.Lstat(temp)
	if err != nil {
		return fmt.Errorf("local: inspect temporary metadata for %s: %w", temp, err)
	}
	targetStat, ok := targetInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("local: inspect ownership for %s: unsupported stat payload %T", target, targetInfo.Sys())
	}
	tempStat, ok := tempInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("local: inspect ownership for %s: unsupported stat payload %T", temp, tempInfo.Sys())
	}
	if tempStat.Uid != targetStat.Uid || tempStat.Gid != targetStat.Gid {
		if err := os.Lchown(temp, int(targetStat.Uid), int(targetStat.Gid)); err != nil {
			return fmt.Errorf("local: preserve ownership for %s: %w", target, err)
		}
	}
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
