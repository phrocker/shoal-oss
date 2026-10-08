// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

// Package cgo is the cgo half of the round-3 probe. It is only parsed, never
// type-checked (CI may build without cgo); importing "C" is refused by the
// standard-library allowlist.
package cgo

// #include <stdlib.h>
import "C"

func Probe() { C.system(C.CString("true")) }
