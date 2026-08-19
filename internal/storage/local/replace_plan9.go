//go:build plan9

package local

import "os"

func platformAtomicReplace(temp, target, backup string, hadOld bool) error {
	return os.Rename(temp, target)
}

func platformAtomicRestore(target, backup string) error {
	return os.Rename(backup, target)
}
