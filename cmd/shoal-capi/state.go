package main

import (
	"errors"
	"sync"

	"github.com/phrocker/shoal/accumulo"
)

type ownedConnector struct {
	connector *accumulo.Connector
	instance  accumulo.Instance
	once      sync.Once
	closeErr  error
}

func (c *ownedConnector) close() error {
	c.once.Do(func() {
		c.closeErr = errors.Join(c.connector.Close(), c.instance.Close())
	})
	return c.closeErr
}

type connectorRegistry struct {
	mu     sync.RWMutex
	nextID uint64
	items  map[uint64]*ownedConnector
}

func newConnectorRegistry() *connectorRegistry {
	return &connectorRegistry{
		nextID: 1,
		items:  make(map[uint64]*ownedConnector),
	}
}

func (r *connectorRegistry) add(connector *ownedConnector) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for attempts := uint64(0); attempts < ^uint64(0); attempts++ {
		id := r.nextID
		r.nextID++
		if r.nextID == 0 {
			r.nextID = 1
		}
		if id == 0 {
			continue
		}
		if _, exists := r.items[id]; !exists {
			r.items[id] = connector
			return id, true
		}
	}
	return 0, false
}

func (r *connectorRegistry) get(id uint64) (*ownedConnector, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	connector, ok := r.items[id]
	return connector, ok
}

func (r *connectorRegistry) remove(id uint64) (*ownedConnector, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	connector, ok := r.items[id]
	if ok {
		delete(r.items, id)
	}
	return connector, ok
}

var connectors = newConnectorRegistry()
