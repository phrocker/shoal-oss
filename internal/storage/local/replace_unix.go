//go:build aix || android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd || solaris

package local

import "os"

// platformAtomicReplace keeps target continuously visible: a hard link
// snapshots the old inode, then rename atomically switches the destination.
// A crash leaves either the old target plus backup, or the new target plus
// backup; cleanup removes the backup only after the switch succeeds.
func platformAtomicReplace(temp, target, backup string, hadOld bool) error {
	if hadOld {
		if err := os.Link(target, backup); err != nil {
			return err
		}
	}
	return os.Rename(temp, target)
}

// platformAtomicRestore atomically switches target back to the linked backup.
func platformAtomicRestore(target, backup string) error {
	return os.Rename(backup, target)
}
