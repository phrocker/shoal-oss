// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.

//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package service

import "errors"

// ErrJournalLocked is returned when another process holds the journal.
var ErrJournalLocked = errors.New("journal directory is locked by another process")

type dirLock struct{}

// lockDir refuses: without an exclusive directory lock two processes could
// share a journal and each run the same key.
func lockDir(string) (*dirLock, error) {
	return nil, errors.New("the journal lock is not implemented on this platform")
}

func (*dirLock) release() error { return nil }
