package main

import (
	"context"
	"errors"
	"sync"
	"time"

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

type ownedScanner struct {
	single *accumulo.Scanner
	batch  *accumulo.BatchScanner

	mu      sync.Mutex
	closed  bool
	nextID  uint64
	cancels map[uint64]context.CancelFunc
	active  sync.WaitGroup
}

func newOwnedScanner(single *accumulo.Scanner, batch *accumulo.BatchScanner) *ownedScanner {
	return &ownedScanner{
		single:  single,
		batch:   batch,
		nextID:  1,
		cancels: make(map[uint64]context.CancelFunc),
	}
}

func (s *ownedScanner) begin(timeout time.Duration) (context.Context, func(), error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, accumulo.ErrConnectorClosed
	}
	var id uint64
	for attempts := uint64(0); attempts < ^uint64(0); attempts++ {
		id = s.nextID
		s.nextID++
		if s.nextID == 0 {
			s.nextID = 1
		}
		if id != 0 {
			if _, exists := s.cancels[id]; !exists {
				break
			}
		}
		id = 0
	}
	if id == 0 {
		s.mu.Unlock()
		return nil, nil, errors.New("shoal: scanner operation space exhausted")
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	s.cancels[id] = cancel
	s.active.Add(1)
	s.mu.Unlock()

	var once sync.Once
	done := func() {
		once.Do(func() {
			s.mu.Lock()
			delete(s.cancels, id)
			s.mu.Unlock()
			cancel()
			s.active.Done()
		})
	}
	return ctx, done, nil
}

func (s *ownedScanner) close() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		for _, cancel := range s.cancels {
			cancel()
		}
	}
	s.mu.Unlock()
	s.active.Wait()
}

type scannerRegistry struct {
	mu     sync.RWMutex
	nextID uint64
	items  map[uint64]*ownedScanner
}

func newScannerRegistry() *scannerRegistry {
	return &scannerRegistry{
		nextID: 1,
		items:  make(map[uint64]*ownedScanner),
	}
}

func (r *scannerRegistry) add(scanner *ownedScanner) (uint64, bool) {
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
			r.items[id] = scanner
			return id, true
		}
	}
	return 0, false
}

func (r *scannerRegistry) get(id uint64) (*ownedScanner, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	scanner, ok := r.items[id]
	return scanner, ok
}

func (r *scannerRegistry) remove(id uint64) (*ownedScanner, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	scanner, ok := r.items[id]
	if ok {
		delete(r.items, id)
	}
	return scanner, ok
}

var scanners = newScannerRegistry()
var batchScanners = newScannerRegistry()
