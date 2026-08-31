// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package metadata

import (
	"sort"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
)

type EmbeddingSpaceCount struct {
	State     embeddingspace.State
	Identity  string
	Files     int64
	SpanCount int64
}

func CountEmbeddingSpaces(tablets []TabletInfo) []EmbeddingSpaceCount {
	counts := map[string]*EmbeddingSpaceCount{}
	for _, tablet := range tablets {
		for _, file := range tablet.Files {
			state := file.Embedding
			if state.State == "" {
				state = embeddingspace.NoEmbeddings()
			}
			key := string(state.State) + "\x00" + state.Identity
			count := counts[key]
			if count == nil {
				count = &EmbeddingSpaceCount{State: state.State, Identity: state.Identity}
				counts[key] = count
			}
			count.Files++
			if file.NumEntries > 0 {
				count.SpanCount += file.NumEntries
			}
		}
	}
	out := make([]EmbeddingSpaceCount, 0, len(counts))
	for _, count := range counts {
		out = append(out, *count)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].State != out[j].State {
			return out[i].State < out[j].State
		}
		return out[i].Identity < out[j].Identity
	})
	return out
}
