// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package evidencelabels

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type ref struct {
	name   string
	labels []string
	nodes  []shoal.ID
}

// labelsHeld answers from a fixed set of held labels.
type labelsHeld map[string]bool

func (h labelsHeld) VisibleToReader(_ context.Context, labels []string) (bool, error) {
	for _, label := range labels {
		if !h[label] {
			return false, nil
		}
	}
	return true, nil
}

// nodesVisible answers from a fixed set of currently visible nodes.
type nodesVisible struct {
	visible map[shoal.ID]bool
	err     error
}

func (n nodesVisible) NodesVisibleToReader(_ context.Context, ids []shoal.ID) (bool, error) {
	if n.err != nil {
		return false, n.err
	}
	for _, id := range ids {
		if !n.visible[id] {
			return false, nil
		}
	}
	return true, nil
}

func names(values []ref) []string {
	result := []string{}
	for _, value := range values {
		result = append(result, value.name)
	}
	return result
}

// TestFilterReferencesLetsCurrentNodesDecide pins the per-reference rule: a
// reference naming nodes is decided by the nodes' current rules alone, both
// after a tightening (stored labels held, node not visible) and after a
// loosening (stored labels not held, node visible); one naming none is
// decided by its stored labels; and a nil gate withholds every reference
// that names a node.
func TestFilterReferencesLetsCurrentNodesDecide(t *testing.T) {
	values := []ref{
		{name: "tightened", labels: []string{"a"}, nodes: []shoal.ID{"n-tight"}},
		{name: "loosened", labels: []string{"a", "b"}, nodes: []shoal.ID{"n-loose"}},
		{name: "nodeless-held", labels: []string{"a"}},
		{name: "nodeless-unheld", labels: []string{"b"}},
		{name: "nodeless-unlabelled"},
	}
	labels := func(value ref) []string { return value.labels }
	nodes := func(value ref) []shoal.ID { return value.nodes }
	held := labelsHeld{"a": true}
	gate := nodesVisible{visible: map[shoal.ID]bool{"n-loose": true}}

	kept, withheld, err := FilterReferences(context.Background(), held, gate, values, labels, nodes)
	if err != nil || !withheld {
		t.Fatalf("withheld = %v, err = %v", withheld, err)
	}
	if got, want := names(kept), []string{"loosened", "nodeless-held", "nodeless-unlabelled"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kept %v, want %v", got, want)
	}

	kept, _, err = FilterReferences(context.Background(), held, nil, values, labels, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := names(kept), []string{"nodeless-held", "nodeless-unlabelled"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("with no gate kept %v, want %v", got, want)
	}

	if _, _, err := FilterReferences(context.Background(), held,
		nodesVisible{err: errors.New("catalog down")}, values, labels, nodes); err == nil {
		t.Fatal("a failing gate was read as a refusal")
	}
}
