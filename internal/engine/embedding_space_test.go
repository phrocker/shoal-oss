// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
package engine

import "testing"

func TestTableTargetEmbeddingSpacePersists(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.CreateTable("graph", TableOptions{TargetEmbeddingSpace: "space-a"}); err != nil {
		t.Fatal(err)
	}
	got, err := eng.TableTargetEmbeddingSpace("graph")
	if err != nil {
		t.Fatal(err)
	}
	if got != "space-a" {
		t.Fatalf("target = %q", got)
	}
	if err := eng.SetTableTargetEmbeddingSpace("graph", "space-b"); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err = reopened.TableTargetEmbeddingSpace("graph")
	if err != nil {
		t.Fatal(err)
	}
	if got != "space-b" {
		t.Fatalf("reopened target = %q", got)
	}
}
