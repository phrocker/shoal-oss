package main

import "testing"

func TestConnectorRegistryLifecycle(t *testing.T) {
	registry := newConnectorRegistry()
	first := &ownedConnector{}
	second := &ownedConnector{}

	firstID, ok := registry.add(first)
	if !ok || firstID == 0 {
		t.Fatalf("add(first) = %d, %v", firstID, ok)
	}
	secondID, ok := registry.add(second)
	if !ok || secondID == 0 || secondID == firstID {
		t.Fatalf("add(second) = %d, %v; first = %d", secondID, ok, firstID)
	}
	if got, ok := registry.get(firstID); !ok || got != first {
		t.Fatalf("get(first) = %p, %v", got, ok)
	}
	if got, ok := registry.remove(firstID); !ok || got != first {
		t.Fatalf("remove(first) = %p, %v", got, ok)
	}
	if _, ok := registry.get(firstID); ok {
		t.Fatal("removed connector remains registered")
	}
}
