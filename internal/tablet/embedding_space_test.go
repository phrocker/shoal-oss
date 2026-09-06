// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package tablet

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/phrocker/shoal-oss/internal/embeddingspace"
	"github.com/phrocker/shoal-oss/internal/iterrt"
)

func TestDirectVectorScanRequiresEngineMetadataValidation(t *testing.T) {
	tablet, err := Open(t.TempDir(), Options{
		DefaultEmbedding: embeddingspace.Has("space-a"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tablet.Close()

	_, err = tablet.Scan(
		iterrt.InfiniteRange(),
		nil,
		false,
		[]iterrt.IterSpec{{
			Name: iterrt.IterVectorKNN,
			Options: map[string]string{
				iterrt.VectorKNNQuery: base64.StdEncoding.EncodeToString(
					[]byte{0, 0, 0, 0}),
				iterrt.VectorKNNEmbeddingSpace: "space-a",
			},
		}},
		iterrt.IteratorEnvironment{Scope: iterrt.ScopeScan},
	)
	if !errors.Is(err, embeddingspace.ErrQueryMetadataMissing) {
		t.Fatalf("direct tablet vector scan error = %v", err)
	}
}
