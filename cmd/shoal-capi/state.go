package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/phrocker/shoal/accumulo"
)

type connectorAPI interface {
	Close() error
	NewScanner(accumulo.Table, accumulo.ScannerOptions) (*accumulo.Scanner, error)
	NewBatchScanner(accumulo.Table, accumulo.ScannerOptions) (*accumulo.BatchScanner, error)
	NewBatchWriter(accumulo.Table, accumulo.BatchWriterOptions) (*accumulo.BatchWriter, error)
	Tables(context.Context) ([]accumulo.Table, error)
	TableExists(context.Context, string) (bool, error)
	CreateTable(context.Context, string) error
	DeleteTable(context.Context, string) error
	RenameTable(context.Context, string, string) error
	FlushTable(context.Context, string, bool) error
	SetTableProperty(context.Context, string, string, string) error
	RemoveTableProperty(context.Context, string, string) error
	EffectiveTableProperties(context.Context, string) (map[string]string, error)
}

type ownedConnector struct {
	connector connectorAPI
	instance  accumulo.Instance
	closed    atomic.Bool

	mu      sync.Mutex
	nextID  uint64
	cancels map[uint64]context.CancelFunc
	active  int
	idle    chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newOwnedConnector(connector connectorAPI, instance accumulo.Instance) *ownedConnector {
	idle := make(chan struct{})
	close(idle)
	return &ownedConnector{
		connector: connector,
		instance:  instance,
		nextID:    1,
		cancels:   make(map[uint64]context.CancelFunc),
		idle:      idle,
	}
}

func (c *ownedConnector) begin(timeout time.Duration) (context.Context, func(), error) {
	c.mu.Lock()
	c.ensureStateLocked()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, nil, accumulo.ErrConnectorClosed
	}
	var id uint64
	for attempts := uint64(0); attempts < ^uint64(0); attempts++ {
		id = c.nextID
		c.nextID++
		if c.nextID == 0 {
			c.nextID = 1
		}
		if id != 0 {
			if _, exists := c.cancels[id]; !exists {
				break
			}
		}
		id = 0
	}
	if id == 0 {
		c.mu.Unlock()
		return nil, nil, errors.New("shoal: connector operation space exhausted")
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	if c.active == 0 {
		c.idle = make(chan struct{})
	}
	c.cancels[id] = cancel
	c.active++
	c.mu.Unlock()

	var once sync.Once
	done := func() {
		once.Do(func() {
			c.mu.Lock()
			delete(c.cancels, id)
			c.active--
			if c.active == 0 {
				close(c.idle)
			}
			c.mu.Unlock()
			cancel()
		})
	}
	return ctx, done, nil
}

func (c *ownedConnector) close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.closeFirst()
	})
	return c.closeErr
}

func (c *ownedConnector) closeBounded(timeout time.Duration) error {
	c.closeOnce.Do(func() {
		c.closeErr = c.closeBoundedFirst(timeout)
	})
	return c.closeErr
}

func (c *ownedConnector) closeFirst() error {
	c.mu.Lock()
	c.ensureStateLocked()
	if !c.closed.Load() {
		c.closed.Store(true)
		for _, cancel := range c.cancels {
			cancel()
		}
	}
	idle := c.idle
	c.mu.Unlock()
	<-idle
	return c.finishClose()
}

func (c *ownedConnector) closeBoundedFirst(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	c.mu.Lock()
	c.ensureStateLocked()
	if !c.closed.Load() {
		c.closed.Store(true)
		for _, cancel := range c.cancels {
			cancel()
		}
	}
	idle := c.idle
	c.mu.Unlock()
	select {
	case <-idle:
		closeResult := make(chan error, 1)
		go func() {
			closeResult <- c.finishClose()
		}()
		select {
		case err := <-closeResult:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		go c.finishCloseAfterTimeout(idle)
		return ctx.Err()
	}
}

func (c *ownedConnector) finishCloseAfterTimeout(idle <-chan struct{}) {
	timer := time.NewTimer(connectorFreeTimeout)
	defer timer.Stop()
	select {
	case <-idle:
	case <-timer.C:
		return
	}
	_ = c.finishClose()
}

func (c *ownedConnector) finishClose() error {
	var err error
	if c.connector != nil {
		err = errors.Join(err, c.connector.Close())
	}
	if c.instance != nil {
		err = errors.Join(err, c.instance.Close())
	}
	return err
}

func (c *ownedConnector) ensureStateLocked() {
	if c.nextID == 0 {
		c.nextID = 1
	}
	if c.cancels == nil {
		c.cancels = make(map[uint64]context.CancelFunc)
	}
	if c.idle == nil {
		c.idle = make(chan struct{})
		close(c.idle)
	}
}

func (c *ownedConnector) isClosed() bool {
	return c != nil && c.closed.Load()
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

type mutationRegistry struct {
	mu     sync.RWMutex
	nextID uint64
	items  map[uint64]*accumulo.Mutation
}

func newMutationRegistry() *mutationRegistry {
	return &mutationRegistry{
		nextID: 1,
		items:  make(map[uint64]*accumulo.Mutation),
	}
}

func (r *mutationRegistry) add(mutation *accumulo.Mutation) (uint64, bool) {
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
			r.items[id] = mutation
			return id, true
		}
	}
	return 0, false
}

func (r *mutationRegistry) get(id uint64) (*accumulo.Mutation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	mutation, ok := r.items[id]
	return mutation, ok
}

func (r *mutationRegistry) remove(id uint64) (*accumulo.Mutation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mutation, ok := r.items[id]
	if ok {
		delete(r.items, id)
	}
	return mutation, ok
}

var mutations = newMutationRegistry()

type batchWriterAPI interface {
	Add(context.Context, *accumulo.Mutation) error
	Flush(context.Context) error
	Close(context.Context) error
}

type ownedBatchWriter struct {
	writer batchWriterAPI
	owner  *ownedConnector

	mu      sync.Mutex
	closed  bool
	nextID  uint64
	cancels map[uint64]context.CancelFunc
	active  int
	idle    chan struct{}

	closeOnce sync.Once
	closeErr  error
}

func newOwnedBatchWriter(writer batchWriterAPI, owner *ownedConnector) *ownedBatchWriter {
	idle := make(chan struct{})
	close(idle)
	return &ownedBatchWriter{
		writer:  writer,
		owner:   owner,
		nextID:  1,
		cancels: make(map[uint64]context.CancelFunc),
		idle:    idle,
	}
}

func (w *ownedBatchWriter) begin(timeout time.Duration) (context.Context, func(), error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, nil, accumulo.ErrBatchWriterClosed
	}
	if w.owner.isClosed() {
		w.mu.Unlock()
		return nil, nil, accumulo.ErrConnectorClosed
	}
	var id uint64
	for attempts := uint64(0); attempts < ^uint64(0); attempts++ {
		id = w.nextID
		w.nextID++
		if w.nextID == 0 {
			w.nextID = 1
		}
		if id != 0 {
			if _, exists := w.cancels[id]; !exists {
				break
			}
		}
		id = 0
	}
	if id == 0 {
		w.mu.Unlock()
		return nil, nil, errors.New("shoal: batch writer operation space exhausted")
	}
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	if w.active == 0 {
		w.idle = make(chan struct{})
	}
	w.cancels[id] = cancel
	w.active++
	w.mu.Unlock()

	var once sync.Once
	done := func() {
		once.Do(func() {
			w.mu.Lock()
			delete(w.cancels, id)
			w.active--
			if w.active == 0 {
				close(w.idle)
			}
			w.mu.Unlock()
			cancel()
		})
	}
	return ctx, done, nil
}

func (w *ownedBatchWriter) close(timeout time.Duration) error {
	w.closeOnce.Do(func() {
		w.closeErr = w.closeFirst(timeout)
	})
	return w.closeErr
}

func (w *ownedBatchWriter) closeFirst(timeout time.Duration) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout == 0 {
		ctx, cancel = context.WithCancel(context.Background())
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	defer cancel()

	w.mu.Lock()
	if !w.closed {
		w.closed = true
		for _, cancel := range w.cancels {
			cancel()
		}
	}
	idle := w.idle
	w.mu.Unlock()
	select {
	case <-idle:
	case <-ctx.Done():
		go w.finishCloseAfterTimeout(idle)
		return ctx.Err()
	}
	return w.writer.Close(ctx)
}

func (w *ownedBatchWriter) finishCloseAfterTimeout(idle <-chan struct{}) {
	timer := time.NewTimer(batchWriterFreeTimeout)
	defer timer.Stop()
	select {
	case <-idle:
	case <-timer.C:
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), batchWriterFreeTimeout)
	defer cancel()
	_ = w.writer.Close(ctx)
}

type batchWriterRegistry struct {
	mu     sync.RWMutex
	nextID uint64
	items  map[uint64]*ownedBatchWriter
}

func newBatchWriterRegistry() *batchWriterRegistry {
	return &batchWriterRegistry{
		nextID: 1,
		items:  make(map[uint64]*ownedBatchWriter),
	}
}

func (r *batchWriterRegistry) add(writer *ownedBatchWriter) (uint64, bool) {
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
			r.items[id] = writer
			return id, true
		}
	}
	return 0, false
}

func (r *batchWriterRegistry) get(id uint64) (*ownedBatchWriter, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	writer, ok := r.items[id]
	return writer, ok
}

func (r *batchWriterRegistry) remove(id uint64) (*ownedBatchWriter, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	writer, ok := r.items[id]
	if ok {
		delete(r.items, id)
	}
	return writer, ok
}

var batchWriters = newBatchWriterRegistry()
