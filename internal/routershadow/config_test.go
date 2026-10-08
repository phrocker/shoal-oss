// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package routershadow

import (
	"bytes"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/explorer/authorized"
	"github.com/phrocker/shoal-oss/pkg/ontology"
)

func TestNewRefusesWeakHostKeys(t *testing.T) {
	w := newWorld(t, authorized.NewMemoryPolicyStore(), false)
	for name, key := range map[string][]byte{
		"short":     []byte("short-key"),
		"all zero":  make([]byte, 32),
		"same byte": bytes.Repeat([]byte{7}, 64),
		"two bytes": bytes.Repeat([]byte{1, 2}, 32),
	} {
		config := w.service.config
		config.HostKey = key
		if _, err := New(config); err == nil {
			t.Errorf("%s key accepted", name)
		}
	}
	config := w.service.config
	if _, err := New(config); err != nil {
		t.Fatalf("valid config refused: %v", err)
	}
}

// TestNewRefusesAMismatchedOntologyBinding: a bundle whose templates do not
// derive from the bound published version, or a version whose identity is
// not the bound one, is refused.
func TestNewRefusesAMismatchedOntologyBinding(t *testing.T) {
	w := newWorld(t, authorized.NewMemoryPolicyStore(), false)
	_, base, target, _, _ := ontologyVersions(t)
	baseIdentity, err := ontology.NewOntologyIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	for name, binding := range map[string]*OntologyBinding{
		// v1's owned_by has other concepts than the bundle's v2 templates.
		"templates from another version": {Configured: base, Identity: baseIdentity, Published: base},
		"identity of another version":    {Configured: base, Identity: baseIdentity, Published: target},
	} {
		config := w.service.config
		config.Ontology = binding
		if _, err := New(config); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
