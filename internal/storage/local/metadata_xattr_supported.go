//go:build linux || darwin

package local

import (
	"errors"
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func preservePlatformXattrs(temp, target string) error {
	return reconcilePlatformXattrs(temp, target, xattrOperations{
		list:   listXattrs,
		get:    getXattr,
		set:    unix.Setxattr,
		remove: removeXattr,
	})
}

type xattrOperations struct {
	list   func(string) ([]string, error)
	get    func(string, string) ([]byte, error)
	set    func(string, string, []byte, int) error
	remove func(string, string) error
}

func reconcilePlatformXattrs(temp, target string, ops xattrOperations) error {
	targetNames, err := ops.list(target)
	if err != nil {
		return fmt.Errorf("local: list extended attributes for %s: %w", target, err)
	}
	tempNames, err := ops.list(temp)
	if err != nil {
		return fmt.Errorf("local: list extended attributes for %s: %w", temp, err)
	}

	targetSet := make(map[string]struct{}, len(targetNames))
	for _, name := range targetNames {
		if !preserveContentXattr(name) {
			continue
		}
		targetSet[name] = struct{}{}
	}
	for _, name := range tempNames {
		if _, ok := targetSet[name]; ok {
			continue
		}
		if err := ops.remove(temp, name); err != nil {
			return fmt.Errorf("local: remove inherited extended attribute %s from %s: %w", name, temp, err)
		}
	}
	for _, name := range targetNames {
		if !preserveContentXattr(name) {
			continue
		}
		value, err := ops.get(target, name)
		if err != nil {
			return fmt.Errorf("local: read extended attribute %s for %s: %w", name, target, err)
		}
		if err := ops.set(temp, name, value, 0); err != nil {
			return fmt.Errorf("local: preserve extended attribute %s for %s: %w", name, target, err)
		}
	}
	return nil
}

func preserveContentXattr(name string) bool {
	switch name {
	case "security.capability", "security.ima", "security.evm":
		return false
	default:
		return true
	}
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

func removeXattr(path, name string) error {
	if err := unix.Removexattr(path, name); err != nil {
		if isMissingXattr(err) {
			return nil
		}
		return err
	}
	return nil
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
