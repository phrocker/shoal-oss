// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package eval

// goLiteralsAtRegistration is the grandfathered Go literal units (see
// goUnit), generated with -print-leak-grandfather at the pre-registration
// commit. Its digest and size are recorded in docs/router-evaluation-v2.md.
//
// The one entry is a pattern in a router grammar fixture of
// internal/routerwire/world_test.go, on main since slice 1 (#519), before
// the split was written. Its expansions contain one of the split's
// templates.
var goLiteralsAtRegistration = []goUnit{
	{"internal/routerwire/world_test.go", "template", "9268442897ce6e4f0aa42578a903d4980a125e9e47149bc2b2d70b05b6be43f7"},
}
