package accumulo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/phrocker/shoal/internal/ingestclient"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
)

const (
	defaultBatchWriterMaxMemoryBytes int64 = 50 << 20
	defaultBatchWriterMaxBatchBytes  int64 = 128 << 10
	batchWriterCleanupTimeout              = 5 * time.Second
)

var (
	// ErrBatchWriterClosed indicates that Add or Flush was called after Close.
	ErrBatchWriterClosed = errors.New("accumulo: batch writer is closed")

	// ErrBatchWriterFailed indicates that a submission may have partially
	// committed. The writer cannot safely retry or accept more mutations.
	ErrBatchWriterFailed = errors.New("accumulo: batch writer failed")
)

// Durability selects the tablet server's write-ahead-log behavior.
type Durability uint8

const (
	DurabilityDefault Durability = iota
	DurabilitySync
	DurabilityFlush
	DurabilityLog
	DurabilityNone
)

// BatchWriterOptions configures a BatchWriter.
type BatchWriterOptions struct {
	// MaxMemoryBytes bounds buffered encoded mutation bytes. Zero uses 50 MiB.
	// A single larger mutation is accepted and submitted synchronously.
	MaxMemoryBytes int64

	// MaxBatchBytes bounds each applyUpdates mutation batch. Zero uses 128 KiB.
	MaxBatchBytes int64

	Durability Durability
}

type normalizedBatchWriterOptions struct {
	maxMemoryBytes int64
	maxBatchBytes  int64
	durability     ingestclient.Durability
}

// FailedExtent reports an Accumulo 4 failed extent and the committed prefix
// returned by closeUpdate.
type FailedExtent struct {
	Extent    TabletExtent
	Submitted int
	Committed int64
}

// ConstraintViolation reports one server-side mutation constraint failure.
type ConstraintViolation struct {
	ConstraintClass            string
	ViolationCode              int16
	Description                string
	NumberOfViolatingMutations int64
}

// AuthorizationFailure reports a server-side authorization rejection.
type AuthorizationFailure struct {
	Extent TabletExtent
	Code   string
}

// MutationRejectionError is the Go-native view of Accumulo closeUpdate
// failures. Generated Thrift types remain internal.
type MutationRejectionError struct {
	Server                string
	FailedExtents         []FailedExtent
	ConstraintViolations  []ConstraintViolation
	AuthorizationFailures []AuthorizationFailure
}

func (e *MutationRejectionError) Error() string {
	return fmt.Sprintf(
		"accumulo: tablet server %s rejected mutations: %d failed extents, %d constraint violations, %d authorization failures",
		e.Server,
		len(e.FailedExtents),
		len(e.ConstraintViolations),
		len(e.AuthorizationFailures),
	)
}

// BatchWriterCleanupError reports that a failed update session could not be
// conclusively cancelled.
type BatchWriterCleanupError struct {
	Server string
	Err    error
}

func (e *BatchWriterCleanupError) Error() string {
	return fmt.Sprintf("accumulo: cancel update session on %s: %v", e.Server, e.Err)
}

// Unwrap returns the update-session cleanup failure.
func (e *BatchWriterCleanupError) Unwrap() error { return e.Err }

type batchWriterSession interface {
	Apply(context.Context, *data.TKeyExtent, []*data.TMutation) error
	Close(context.Context) (*data.UpdateErrors, error)
	Cancel(context.Context) (bool, error)
}

type bufferedMutation struct {
	row  []byte
	wire *data.TMutation
	size int64
}

type extentPlan struct {
	extent    *data.TKeyExtent
	mutations []bufferedMutation
}

type serverPlan struct {
	address string
	extents []extentPlan
	indexes map[tabletExtentMapKey]int
}

// BatchWriter buffers mutations for one table and submits them through
// Accumulo 4 update sessions. Operations are serialized; concurrent callers
// waiting for the writer honor their contexts.
type BatchWriter struct {
	connector *Connector
	table     Table
	options   normalizedBatchWriterOptions

	gate chan struct{}

	pending      []bufferedMutation
	pendingBytes int64
	failure      error
	closed       bool
	closeErr     error

	startSession func(context.Context, string, ingestclient.Durability) (batchWriterSession, error)
}

// NewBatchWriter constructs a bounded writer for table.
func (c *Connector) NewBatchWriter(
	table Table,
	options BatchWriterOptions,
) (*BatchWriter, error) {
	if c == nil {
		return nil, errors.New("accumulo: connector is nil")
	}
	if table.ID == "" && table.Name == "" {
		return nil, fmt.Errorf("%w: batch writer table identity is empty", ErrTableNotFound)
	}
	normalized, err := normalizeBatchWriterOptions(options)
	if err != nil {
		return nil, err
	}
	if _, err := c.discoveryState(); err != nil {
		return nil, err
	}
	writer := &BatchWriter{
		connector: c,
		table: Table{
			Name: table.Name,
			ID:   table.ID,
		},
		options: normalized,
		gate:    make(chan struct{}, 1),
	}
	writer.gate <- struct{}{}
	writer.startSession = func(
		ctx context.Context,
		address string,
		durability ingestclient.Durability,
	) (batchWriterSession, error) {
		return c.ingest.Start(ctx, address, durability)
	}
	return writer, nil
}

// Add defensively snapshots mutation. When the configured memory bound is
// reached, Add synchronously submits buffered mutations and applies
// backpressure to the caller. An error before submission means mutation was
// not accepted; ErrBatchWriterFailed means it may have committed.
func (w *BatchWriter) Add(ctx context.Context, mutation *Mutation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mutation == nil {
		return errors.New("accumulo: mutation is nil")
	}
	if err := w.lock(ctx); err != nil {
		return err
	}
	defer w.unlock()

	if w.closed {
		return ErrBatchWriterClosed
	}
	if w.failure != nil {
		return w.failure
	}
	if mutation.Size() == 0 {
		return errors.New("accumulo: mutation must contain at least one update")
	}
	wire, err := mutation.toThrift()
	if err != nil {
		return err
	}
	buffered := bufferedMutation{
		row:  cloneRow(wire.Row),
		wire: wire,
		size: mutationWireSize(wire),
	}

	if w.pendingBytes > 0 &&
		w.pendingBytes+buffered.size > w.options.maxMemoryBytes {
		if err := w.flushLocked(ctx); err != nil {
			return err
		}
	}
	previousLen := len(w.pending)
	previousBytes := w.pendingBytes
	w.pending = append(w.pending, buffered)
	w.pendingBytes += buffered.size
	if w.pendingBytes >= w.options.maxMemoryBytes {
		err := w.flushLocked(ctx)
		if err != nil && w.failure == nil {
			w.pending = w.pending[:previousLen]
			w.pendingBytes = previousBytes
		}
		return err
	}
	return nil
}

// Flush commits every mutation accepted before Flush acquired the writer.
// A submission failure is sticky because Accumulo may have committed a prefix.
func (w *BatchWriter) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.lock(ctx); err != nil {
		return err
	}
	defer w.unlock()
	if w.closed {
		return ErrBatchWriterClosed
	}
	return w.flushLocked(ctx)
}

// Close stops accepting mutations and commits the remaining buffer. Repeated
// calls return the first Close result without submitting again.
func (w *BatchWriter) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.lock(ctx); err != nil {
		return err
	}
	defer w.unlock()
	if w.closed {
		return w.closeErr
	}
	w.closed = true
	w.closeErr = w.flushLocked(ctx)
	if w.closeErr != nil && w.failure == nil {
		w.pending = nil
		w.pendingBytes = 0
	}
	return w.closeErr
}

func (w *BatchWriter) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.gate:
		return nil
	}
}

func (w *BatchWriter) unlock() {
	w.gate <- struct{}{}
}

func (w *BatchWriter) flushLocked(ctx context.Context) error {
	if w.failure != nil {
		return w.failure
	}
	if len(w.pending) == 0 {
		return nil
	}
	table, plans, err := w.plan(ctx)
	if err != nil {
		return err
	}
	w.table = table
	for index := range plans {
		if err := w.sendServer(ctx, plans[index]); err != nil {
			w.failure = errors.Join(ErrBatchWriterFailed, err)
			w.pending = nil
			w.pendingBytes = 0
			return w.failure
		}
	}
	w.pending = nil
	w.pendingBytes = 0
	return nil
}

func (w *BatchWriter) plan(ctx context.Context) (Table, []serverPlan, error) {
	table, err := w.resolveTable(ctx)
	if err != nil {
		return Table{}, nil, err
	}
	var plans []serverPlan
	serverIndexes := make(map[string]int)
	for _, mutation := range w.pending {
		tablet, err := w.connector.LocateTablet(ctx, table, mutation.row)
		if err != nil {
			return Table{}, nil, err
		}
		address := tablet.Server.HostPort
		serverIndex, ok := serverIndexes[address]
		if !ok {
			serverIndex = len(plans)
			serverIndexes[address] = serverIndex
			plans = append(plans, serverPlan{
				address: address,
				indexes: make(map[tabletExtentMapKey]int),
			})
		}
		plan := &plans[serverIndex]
		extentKey := tabletExtentKey(tablet)
		extentIndex, ok := plan.indexes[extentKey]
		if !ok {
			extentIndex = len(plan.extents)
			plan.indexes[extentKey] = extentIndex
			plan.extents = append(plan.extents, extentPlan{
				extent: tabletExtentToThrift(tablet),
			})
		}
		plan.extents[extentIndex].mutations = append(
			plan.extents[extentIndex].mutations,
			mutation,
		)
	}
	return table, plans, nil
}

func (w *BatchWriter) resolveTable(ctx context.Context) (Table, error) {
	switch {
	case w.table.ID == "":
		return w.connector.TableByName(ctx, w.table.Name)
	case w.table.Name == "":
		return w.connector.TableByID(ctx, w.table.ID)
	default:
		return w.table, nil
	}
}

func (w *BatchWriter) sendServer(ctx context.Context, plan serverPlan) error {
	session, err := w.startSession(ctx, plan.address, w.options.durability)
	if err != nil {
		return fmt.Errorf("accumulo: start update session on %s: %w", plan.address, err)
	}
	for _, extent := range plan.extents {
		for _, mutations := range mutationBatches(extent.mutations, w.options.maxBatchBytes) {
			if err := session.Apply(ctx, extent.extent, mutations); err != nil {
				return errors.Join(
					fmt.Errorf("accumulo: apply mutations on %s: %w", plan.address, err),
					cancelWriterSession(ctx, plan.address, session),
				)
			}
		}
	}
	updateErrors, err := session.Close(ctx)
	if err != nil {
		return errors.Join(
			fmt.Errorf("accumulo: close update session on %s: %w", plan.address, err),
			cancelWriterSession(ctx, plan.address, session),
		)
	}
	rejection, err := mutationRejection(plan, updateErrors)
	if err != nil {
		return err
	}
	if rejection != nil {
		return rejection
	}
	return nil
}

func cancelWriterSession(
	ctx context.Context,
	server string,
	session batchWriterSession,
) error {
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		batchWriterCleanupTimeout,
	)
	defer cancel()
	cancelled, err := session.Cancel(cleanupCtx)
	if errors.Is(err, ingestclient.ErrSessionClosed) {
		return nil
	}
	if err == nil && !cancelled {
		err = errors.New("server did not confirm cancellation")
	}
	if err == nil {
		return nil
	}
	return &BatchWriterCleanupError{Server: server, Err: err}
}

func mutationBatches(
	mutations []bufferedMutation,
	maxBytes int64,
) [][]*data.TMutation {
	var batches [][]*data.TMutation
	for len(mutations) > 0 {
		var batch []*data.TMutation
		var size int64
		for len(mutations) > 0 {
			next := mutations[0]
			if len(batch) > 0 && size+next.size > maxBytes {
				break
			}
			batch = append(batch, next.wire)
			size += next.size
			mutations = mutations[1:]
		}
		batches = append(batches, batch)
	}
	return batches
}

func mutationRejection(
	plan serverPlan,
	updateErrors *data.UpdateErrors,
) (*MutationRejectionError, error) {
	if updateErrors == nil {
		return nil, nil
	}
	rejection := &MutationRejectionError{Server: plan.address}
	submitted := make(map[tabletExtentMapKey]int)
	for _, extent := range plan.extents {
		submitted[thriftTabletExtentKey(extent.extent)] = len(extent.mutations)
	}
	for extent, committed := range updateErrors.FailedExtents {
		if extent == nil {
			return nil, errors.New("accumulo: closeUpdate returned a nil failed extent")
		}
		count, ok := submitted[thriftTabletExtentKey(extent)]
		if !ok {
			return nil, errors.New("accumulo: closeUpdate returned an unknown failed extent")
		}
		if committed < 0 || committed > int64(count) {
			return nil, fmt.Errorf(
				"accumulo: closeUpdate returned invalid committed count %d for %d mutations",
				committed,
				count,
			)
		}
		rejection.FailedExtents = append(rejection.FailedExtents, FailedExtent{
			Extent:    publicTabletExtent(extent),
			Submitted: count,
			Committed: committed,
		})
	}
	for _, violation := range updateErrors.ViolationSummaries {
		if violation == nil {
			continue
		}
		rejection.ConstraintViolations = append(
			rejection.ConstraintViolations,
			ConstraintViolation{
				ConstraintClass:            violation.ConstrainClass,
				ViolationCode:              violation.ViolationCode,
				Description:                violation.ViolationDescription,
				NumberOfViolatingMutations: violation.NumberOfViolatingMutations,
			},
		)
	}
	for extent, code := range updateErrors.AuthorizationFailures {
		if extent == nil {
			return nil, errors.New("accumulo: closeUpdate returned a nil authorization extent")
		}
		rejection.AuthorizationFailures = append(
			rejection.AuthorizationFailures,
			AuthorizationFailure{
				Extent: publicTabletExtent(extent),
				Code:   code.String(),
			},
		)
	}
	sort.Slice(rejection.FailedExtents, func(i, j int) bool {
		return tabletExtentLess(
			rejection.FailedExtents[i].Extent,
			rejection.FailedExtents[j].Extent,
		)
	})
	sort.Slice(rejection.AuthorizationFailures, func(i, j int) bool {
		return tabletExtentLess(
			rejection.AuthorizationFailures[i].Extent,
			rejection.AuthorizationFailures[j].Extent,
		)
	})
	if len(rejection.FailedExtents) == 0 &&
		len(rejection.ConstraintViolations) == 0 &&
		len(rejection.AuthorizationFailures) == 0 {
		return nil, nil
	}
	return rejection, nil
}

func normalizeBatchWriterOptions(
	options BatchWriterOptions,
) (normalizedBatchWriterOptions, error) {
	if options.MaxMemoryBytes < 0 {
		return normalizedBatchWriterOptions{}, errors.New(
			"accumulo: batch writer MaxMemoryBytes must not be negative",
		)
	}
	if options.MaxMemoryBytes == 0 {
		options.MaxMemoryBytes = defaultBatchWriterMaxMemoryBytes
	}
	if options.MaxBatchBytes < 0 {
		return normalizedBatchWriterOptions{}, errors.New(
			"accumulo: batch writer MaxBatchBytes must not be negative",
		)
	}
	if options.MaxBatchBytes == 0 {
		options.MaxBatchBytes = defaultBatchWriterMaxBatchBytes
	}
	if options.Durability > DurabilityNone {
		return normalizedBatchWriterOptions{}, errors.New(
			"accumulo: batch writer durability is invalid",
		)
	}
	return normalizedBatchWriterOptions{
		maxMemoryBytes: options.MaxMemoryBytes,
		maxBatchBytes:  options.MaxBatchBytes,
		durability:     ingestclient.Durability(options.Durability),
	}, nil
}

func mutationWireSize(mutation *data.TMutation) int64 {
	size := int64(len(mutation.Row) + len(mutation.Data))
	for _, value := range mutation.Values {
		size += int64(len(value))
	}
	return size
}

func thriftTabletExtentKey(extent *data.TKeyExtent) tabletExtentMapKey {
	return tabletExtentMapKey{
		tableID:    string(extent.Table),
		prevRow:    string(extent.PrevEndRow),
		prevRowSet: extent.PrevEndRow != nil,
		endRow:     string(extent.EndRow),
		endRowSet:  extent.EndRow != nil,
	}
}

func publicTabletExtent(extent *data.TKeyExtent) TabletExtent {
	return TabletExtent{
		TableID: string(extent.Table),
		PrevRow: cloneRow(extent.PrevEndRow),
		EndRow:  cloneRow(extent.EndRow),
	}
}

func tabletExtentLess(left, right TabletExtent) bool {
	if left.TableID != right.TableID {
		return left.TableID < right.TableID
	}
	if comparison := optionalBytesCompare(left.PrevRow, right.PrevRow); comparison != 0 {
		return comparison < 0
	}
	return optionalBytesCompare(left.EndRow, right.EndRow) < 0
}

func optionalBytesCompare(left, right []byte) int {
	if left == nil && right != nil {
		return -1
	}
	if left != nil && right == nil {
		return 1
	}
	return bytes.Compare(left, right)
}
