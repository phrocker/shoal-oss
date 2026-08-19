//go:build aix || android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd || solaris

package local

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

var (
	linkReplacementPath   = os.Link
	renameReplacementPath = os.Rename
)

// platformAtomicReplace keeps target continuously visible: a hard link
// snapshots the old inode, then rename atomically switches the destination.
// Filesystems that reject hard links fall back to renaming the old target
// aside, publishing the replacement, and restoring the old target if publish
// fails. A crash leaves either the old target plus backup, or the new target
// plus backup; cleanup removes the backup only after the switch succeeds.
func platformAtomicReplace(temp, target, backup string, hadOld bool) error {
	if hadOld {
		if err := linkReplacementPath(target, backup); err != nil {
			if !isLinkSnapshotUnsupported(err) {
				return err
			}
			return platformRenameFallbackReplace(temp, target, backup, err)
		}
	}
	return renameReplacementPath(temp, target)
}

// platformAtomicRestore atomically switches target back to the linked backup.
func platformAtomicRestore(target, backup string) error {
	return renameReplacementPath(backup, target)
}

func isLinkSnapshotUnsupported(err error) bool {
	return errors.Is(err, syscall.EXDEV) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP) ||
		errors.Is(err, syscall.EPERM)
}

func platformRenameFallbackReplace(temp, target, backup string, linkErr error) error {
	if _, err := os.Lstat(backup); err == nil {
		return errors.Join(linkErr, &os.LinkError{Op: "rename", Old: target, New: backup, Err: fs.ErrExist})
	} else if !errors.Is(err, fs.ErrNotExist) {
		return errors.Join(linkErr, fmt.Errorf("local: inspect replacement backup %s: %w", backup, err))
	}
	if err := renameReplacementPath(target, backup); err != nil {
		return errors.Join(linkErr, err)
	}
	if err := renameReplacementPath(temp, target); err != nil {
		if restoreErr := renameReplacementPath(backup, target); restoreErr != nil {
			return errors.Join(
				linkErr,
				err,
				fmt.Errorf("local: restore %s from %s after failed replacement: %w", target, backup, restoreErr),
			)
		}
		return errors.Join(linkErr, err)
	}
	return nil
}
