/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Package dirlock provides process- and OS-exclusive ownership of a storage
// directory for embedded stores that cannot safely share WAL state.
package dirlock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrLocked = errors.New("directory lock: already held")

var held = struct {
	sync.Mutex
	paths map[string]struct{}
}{paths: make(map[string]struct{})}

type Lock struct {
	mu       sync.Mutex
	file     *os.File
	identity string
	closed   bool
}

// Acquire creates directory when needed, resolves its canonical path, rejects
// duplicate ownership in this process, and acquires a nonblocking OS lock.
// name must be one plain file name and should be stable for the store type.
func Acquire(directory, name string) (*Lock, error) {
	if strings.TrimSpace(directory) == "" {
		return nil, errors.New("directory lock: directory is required")
	}
	if name == "" || name == "." || name == ".." ||
		filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return nil, errors.New("directory lock: lock file name is invalid")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, fmt.Errorf("directory lock: create directory: %w", err)
	}
	canonical, err := canonicalDirectory(directory)
	if err != nil {
		return nil, err
	}
	identity := pathIdentity(filepath.Join(canonical, name))
	held.Lock()
	if _, exists := held.paths[identity]; exists {
		held.Unlock()
		return nil, ErrLocked
	}
	held.paths[identity] = struct{}{}
	held.Unlock()

	file, err := os.OpenFile(filepath.Join(canonical, name), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		releaseIdentity(identity)
		return nil, fmt.Errorf("directory lock: open lock file: %w", err)
	}
	if err := tryLockFile(file); err != nil {
		_ = file.Close()
		releaseIdentity(identity)
		return nil, errors.Join(ErrLocked, err)
	}
	return &Lock{file: file, identity: identity}, nil
}

func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	releaseIdentity(l.identity)
	return errors.Join(unlockErr, closeErr)
}

func canonicalDirectory(directory string) (string, error) {
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("directory lock: resolve absolute path: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("directory lock: resolve canonical path: %w", err)
	}
	return filepath.Clean(resolved), nil
}

func releaseIdentity(identity string) {
	held.Lock()
	delete(held.paths, identity)
	held.Unlock()
}
