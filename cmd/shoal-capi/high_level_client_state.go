package main

import (
	"context"
	"sync"
	"time"

	"github.com/phrocker/shoal/accumulo"
)

type clientSnapshot struct {
	table          string
	authorizations [][]byte
	threadCount    int32
}

type ownedClient struct {
	mu             sync.Mutex
	connector      *ownedConnector
	table          string
	authorizations [][]byte
	threadCount    int32
	closed         bool
	closeOnce      sync.Once
	closeErr       error
}

func newOwnedClient(
	connector *ownedConnector,
	table string,
	authorizations [][]byte,
	threadCount int32,
) *ownedClient {
	return &ownedClient{
		connector:      connector,
		table:          table,
		authorizations: cloneClientAuthorizations(authorizations),
		threadCount:    threadCount,
	}
}

func (c *ownedClient) setThreads(threadCount int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return accumulo.ErrConnectorClosed
	}
	c.threadCount = threadCount
	return nil
}

func (c *ownedClient) setTable(table string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return accumulo.ErrConnectorClosed
	}
	c.table = table
	return nil
}

func (c *ownedClient) checkOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return accumulo.ErrConnectorClosed
	}
	return nil
}

func (c *ownedClient) setAuthorizations(authorizations [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return accumulo.ErrConnectorClosed
	}
	c.authorizations = cloneClientAuthorizations(authorizations)
	return nil
}

func (c *ownedClient) snapshot(requireTable bool) (clientSnapshot, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return clientSnapshot{}, nil, accumulo.ErrConnectorClosed
	}
	if requireTable && c.table == "" {
		c.mu.Unlock()
		return clientSnapshot{}, nil, accumulo.ErrTableNotFound
	}
	done, err := c.connector.retain()
	if err != nil {
		c.mu.Unlock()
		return clientSnapshot{}, nil, err
	}
	snapshot := clientSnapshot{
		table:          c.table,
		authorizations: cloneClientAuthorizations(c.authorizations),
		threadCount:    c.threadCount,
	}
	c.mu.Unlock()
	return snapshot, done, nil
}

func (c *ownedClient) begin(timeout time.Duration) (context.Context, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, accumulo.ErrConnectorClosed
	}
	ctx, done, err := c.connector.begin(timeout)
	c.mu.Unlock()
	return ctx, done, err
}

func (c *ownedClient) close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.closeErr = c.connector.close()
	})
	return c.closeErr
}

func (c *ownedClient) closeBounded(timeout time.Duration) error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.closeErr = c.connector.closeBounded(timeout)
	})
	return c.closeErr
}

func cloneClientAuthorizations(authorizations [][]byte) [][]byte {
	result := make([][]byte, len(authorizations))
	for index := range authorizations {
		result[index] = append([]byte(nil), authorizations[index]...)
	}
	return result
}

type clientRegistry struct {
	mu     sync.RWMutex
	nextID uint64
	items  map[uint64]*ownedClient
}

func newClientRegistry() *clientRegistry {
	return &clientRegistry{
		nextID: 1,
		items:  make(map[uint64]*ownedClient),
	}
}

func (r *clientRegistry) add(client *ownedClient) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for attempts := uint64(0); attempts < ^uint64(0); attempts++ {
		id := r.nextID
		r.nextID++
		if r.nextID == 0 {
			r.nextID = 1
		}
		if id != 0 {
			if _, exists := r.items[id]; !exists {
				r.items[id] = client
				return id, true
			}
		}
	}
	return 0, false
}

func (r *clientRegistry) get(id uint64) (*ownedClient, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	client, ok := r.items[id]
	return client, ok
}

func (r *clientRegistry) remove(id uint64) (*ownedClient, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client, ok := r.items[id]
	delete(r.items, id)
	return client, ok
}

var clients = newClientRegistry()
