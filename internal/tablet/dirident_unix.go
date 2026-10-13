// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build unix

package tablet

import (
	"fmt"
	"os"
	"syscall"
)

// platformDirIdentity is the directory's device and inode, so a copy of a
// data directory at the same path (another host, a restored volume) gets a
// new identity. Empty when the directory cannot be inspected.
func platformDirIdentity(dir string) string {
	info, err := os.Stat(dir)
	if err != nil {
		return ""
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ""
	}
	return fmt.Sprintf("|%d:%d", uint64(stat.Dev), uint64(stat.Ino))
}
