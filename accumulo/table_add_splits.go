package accumulo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/phrocker/shoal/internal/managerclient"
	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/tablenames"
	"github.com/phrocker/shoal/internal/zk"
)

const (
	// splitRetryAttempts bounds how many times AddTableSplits re-resolves
	// tablets and resubmits the splits Accumulo reported as unapplied.
	// Accumulo's own putSplits loops forever; Shoal bounds the loop so a
	// permanently churning table surfaces ErrTableSplitsIncomplete instead
	// of hanging.
	splitRetryAttempts = 10

	// splitRetryInitialBackoff, splitRetryBackoffStep and
	// splitRetryMaxBackoff mirror Accumulo's split Retry builder
	// (retryAfter 100ms, incrementBy 100ms, maxWait 2s).
	splitRetryInitialBackoff = 100 * time.Millisecond
	splitRetryBackoffStep    = 100 * time.Millisecond
	splitRetryMaxBackoff     = 2 * time.Second
)

// splitTarget carries the once-resolved state a split round needs.
type splitTarget struct {
	tableName string
	tableID   string
	address   string
	manager   managerclient.Adapter
	discovery *connectorDiscovery
	retry     splitRetryPolicy
}

// splitRetryPolicy bounds how long AddTableSplits keeps re-resolving tablets
// for splits Accumulo reported as unapplied.
type splitRetryPolicy struct {
	attempts       int
	initialBackoff time.Duration
	backoffStep    time.Duration
	maxBackoff     time.Duration
}

func defaultSplitRetryPolicy() splitRetryPolicy {
	return splitRetryPolicy{
		attempts:       splitRetryAttempts,
		initialBackoff: splitRetryInitialBackoff,
		backoffStep:    splitRetryBackoffStep,
		maxBackoff:     splitRetryMaxBackoff,
	}
}

// splitGroup is one manager TABLE_SPLIT FATE operation: the tablet being
// split plus every new split row that falls strictly inside it.
type splitGroup struct {
	extent TabletExtent
	rows   [][]byte
}

// splitPlan is one round's mapping of the still-pending split rows onto the
// table's current tablets.
type splitPlan struct {
	// groups are new split points, grouped by their containing
	// (PrevRow, EndRow] tablet, in tablet order.
	groups []splitGroup
	// existing are requested rows that already are a tablet's end row.
	existing []TabletExtent
	// unresolved are rows no current tablet covers, which means the tablet
	// list is stale or the table has a metadata gap.
	unresolved [][]byte
}

// AddTableSplits adds split points to an existing table through Accumulo 4's
// manager FATE TABLE_SPLIT operation, the same protocol
// TableOperations.addSplits uses.
//
// splits holds raw, binary-safe split rows; NUL bytes and arbitrary
// non-UTF-8 bytes are preserved exactly. The slice and its rows are copied
// before use, then sorted by unsigned lexicographic row order and
// deduplicated, so callers may reuse or mutate their input afterwards.
// A nil or empty collection, or any nil or zero-length row, is rejected with
// ErrInvalidTableSplit.
//
// tableName must satisfy Accumulo's existing-table-name validator before it
// is resolved; malformed non-empty names fail with ErrInvalidTableName. The
// table also must not be OFFLINE when planning starts, even if every
// requested split row already exists and only mergeability refreshes are
// needed.
//
// Each requested row is mapped to the tablet whose (PrevRow, EndRow] range
// contains it and every row landing in the same tablet is submitted as a
// single FATE operation, exactly as Accumulo's putSplits does. Rows that
// already are a tablet's end row are not resubmitted: Accumulo's addSplits
// instead refreshes the existing tablet's mergeability metadata to the user
// default ("never"), which Shoal does through the manager's
// updateTabletMergeability RPC.
//
// The call waits for every FATE operation it starts, always finishes it —
// even when the operation failed or ctx was canceled — and removes only the
// groups Accumulo reported as split (status "SPLIT_SUCCEEDED"). Groups whose
// tablet moved or split underneath the client are retried against freshly
// resolved tablets with bounded backoff; when rows still remain the call
// fails with ErrTableSplitsIncomplete. Unlike Accumulo, which fans the
// per-tablet operations out across two thread pools, Shoal submits them
// sequentially. Table discovery and table-name caches are invalidated after
// the attempts regardless of outcome.
//
// Errors from the manager are mapped to ErrTableNotFound, ErrTableOffline,
// ErrPermissionDenied, ErrNamespaceNotFound, ErrInvalidTableName and
// ErrManagerUnavailable. The legacy tablet-server splitTablet RPC removed in
// Accumulo 4 is never used.
func (c *Connector) AddTableSplits(ctx context.Context, tableName string, splits [][]byte) error {
	if err := validateExistingTableName(tableName); err != nil {
		return err
	}
	pending, err := normalizeSplitRows(splits)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrConnectorClosed
	}
	discovery := c.discovery
	resolver := c.managerAddr
	manager := c.manager
	c.mu.RUnlock()
	if discovery == nil || resolver == nil {
		return ErrDiscoveryUnavailable
	}

	discovery.tables.Invalidate()
	tableID, err := discovery.tables.ResolveID(ctx, tableName)
	if errors.Is(err, tablenames.ErrTableNotFound) {
		return fmt.Errorf("%w: table name %q", ErrTableNotFound, tableName)
	}
	if err != nil {
		return fmt.Errorf("accumulo: resolve table name %q: %w", tableName, err)
	}
	if err := requireTableNotOffline(ctx, discovery.states, tableID, tableName); err != nil {
		return err
	}

	address, err := resolver.Address(ctx)
	if errors.Is(err, zk.ErrManagerUnavailable) {
		return ErrManagerUnavailable
	}
	if err != nil {
		return fmt.Errorf("accumulo: discover manager: %w", err)
	}

	defer func() {
		discovery.invalidateAll()
	}()

	return addSplits(ctx, splitTarget{
		tableName: tableName,
		tableID:   tableID,
		address:   address,
		manager:   manager,
		discovery: discovery,
		retry:     defaultSplitRetryPolicy(),
	}, pending)
}

func requireTableNotOffline(
	ctx context.Context,
	states tableStateReader,
	tableID, tableName string,
) error {
	if states == nil {
		return nil
	}
	state, err := states.TableState(ctx, tableID)
	if err != nil {
		return fmt.Errorf("accumulo: read table state for %q: %w", tableName, err)
	}
	if !state.Exists {
		return fmt.Errorf("%w: table %q", ErrTableNotFound, tableName)
	}
	if state.State == zk.TableStateOffline {
		return fmt.Errorf("%w: table %q", ErrTableOffline, tableName)
	}
	return nil
}

// addSplits runs bounded split rounds until nothing is pending, a manager
// operation fails, ctx ends, or the retry budget is exhausted.
func addSplits(ctx context.Context, target splitTarget, pending [][]byte) error {
	backoff := target.retry.initialBackoff
	for attempt := 0; attempt < target.retry.attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Accumulo's putSplits invalidates its tablet cache at the top of
		// every round so each round plans against real extents.
		target.discovery.tablets.InvalidateTable(target.tableID)
		tablets, err := target.discovery.tablets.LocateTable(ctx, target.tableID)
		if err != nil {
			return mapTabletDiscoveryError(target.tableID, nil, err)
		}

		plan := planTableSplits(target.tableID, tablets, pending)
		completed, opErr := applySplitPlan(ctx, target, plan)
		pending = removeSplitRows(pending, completed)
		if opErr != nil {
			return opErr
		}
		if len(pending) == 0 {
			return nil
		}
		if attempt+1 == target.retry.attempts {
			break
		}
		if err := waitForWriterRetry(ctx, backoff); err != nil {
			return err
		}
		backoff = nextSplitBackoff(backoff, target.retry)
	}
	return fmt.Errorf(
		"%w: table %q has %d unapplied split rows after %d attempts",
		ErrTableSplitsIncomplete,
		target.tableName,
		len(pending),
		target.retry.attempts,
	)
}

// applySplitPlan submits one round's mergeability refresh and FATE split
// operations, returning the rows Accumulo confirmed. Every group is
// attempted even after an earlier group fails, so a partial multi-tablet
// failure still reports — and retains — the groups that succeeded.
func applySplitPlan(
	ctx context.Context,
	target splitTarget,
	plan splitPlan,
) ([][]byte, error) {
	var (
		completed [][]byte
		failures  []error
	)
	if len(plan.existing) > 0 {
		updated, err := refreshExistingSplits(ctx, target, plan.existing)
		completed = append(completed, updated...)
		if err != nil {
			failures = append(failures, err)
		}
	}
	for _, group := range plan.groups {
		if err := ctx.Err(); err != nil {
			failures = append(failures, err)
			break
		}
		request, err := splitFateRequest(target, group)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		status, err := target.manager.ExecuteStatus(ctx, target.address, request)
		// A successful split whose FATE cleanup failed still split the
		// tablet: keep the rows, but surface the cleanup error.
		if status == managerclient.SplitSucceeded {
			completed = append(completed, group.rows...)
		}
		if err != nil {
			failures = append(failures, mapSplitError(target.tableName, err))
		}
	}
	return completed, errors.Join(failures...)
}

// refreshExistingSplits applies the user-default "never" mergeability to
// tablets whose end row was already the requested split point, mirroring
// the updateTabletMergeability call Accumulo's putSplits makes for existing
// splits. Only the extents the manager accepted are reported as completed;
// the rest are retried in the next round.
func refreshExistingSplits(
	ctx context.Context,
	target splitTarget,
	existing []TabletExtent,
) ([][]byte, error) {
	updates := make([]managerclient.MergeabilityUpdate, 0, len(existing))
	for _, extent := range existing {
		updates = append(updates, managerclient.MergeabilityUpdate{
			Extent: managerclient.TabletExtent{
				TableID:    extent.TableID,
				EndRow:     extent.EndRow,
				PrevEndRow: extent.PrevRow,
			},
			Mergeability: managerclient.NeverMergeable(),
		})
	}
	updated, err := target.manager.UpdateTabletMergeability(
		ctx,
		target.address,
		target.tableName,
		updates,
	)
	rows := make([][]byte, 0, len(updated))
	for _, extent := range updated {
		if len(extent.EndRow) == 0 {
			continue
		}
		rows = append(rows, extent.EndRow)
	}
	if err != nil {
		return rows, mapSplitError(target.tableName, err)
	}
	return rows, nil
}

// splitFateRequest builds the exact Accumulo 4 TABLE_SPLIT FATE request:
// the table's canonical ID, the tablet's end row (empty when unbounded),
// the tablet's previous end row (empty when unbounded), then one encoded
// split/mergeability payload per new split row.
func splitFateRequest(target splitTarget, group splitGroup) (managerclient.Request, error) {
	arguments := make([][]byte, 0, 3+len(group.rows))
	arguments = append(arguments,
		[]byte(target.tableID),
		splitBoundaryArgument(group.extent.EndRow),
		splitBoundaryArgument(group.extent.PrevRow),
	)
	for _, row := range group.rows {
		payload, err := encodeSplitMergeability(row, neverMergeable())
		if err != nil {
			return managerclient.Request{}, err
		}
		arguments = append(arguments, payload)
	}
	return managerclient.Request{
		Operation: managerclient.TableSplit,
		Instance:  fateInstanceForTable(target.tableName),
		Arguments: arguments,
		Options:   map[string]string{},
	}, nil
}

// splitBoundaryArgument encodes an extent boundary: Accumulo sends a
// zero-length buffer for an unbounded (null) end or previous end row.
func splitBoundaryArgument(row []byte) []byte {
	if len(row) == 0 {
		return []byte{}
	}
	return append([]byte{}, row...)
}

// planTableSplits maps every pending row onto the tablet that currently
// contains it. tablets must be in the ascending end-row order the locator
// cache maintains (a nil end row sorts last).
func planTableSplits(tableID string, tablets []metadata.TabletInfo, pending [][]byte) splitPlan {
	plan := splitPlan{}
	groups := make(map[int]int, len(tablets))
	seenExisting := make(map[int]struct{}, len(tablets))
	for _, row := range pending {
		index := findSplitTablet(tablets, row)
		if index < 0 {
			plan.unresolved = append(plan.unresolved, row)
			continue
		}
		tablet := tablets[index]
		extent := TabletExtent{
			TableID: tableID,
			PrevRow: cloneRow(tablet.PrevRow),
			EndRow:  cloneRow(tablet.EndRow),
		}
		if tablet.EndRow != nil && bytes.Equal(tablet.EndRow, row) {
			if _, ok := seenExisting[index]; !ok {
				seenExisting[index] = struct{}{}
				plan.existing = append(plan.existing, extent)
			}
			continue
		}
		position, ok := groups[index]
		if !ok {
			position = len(plan.groups)
			groups[index] = position
			plan.groups = append(plan.groups, splitGroup{extent: extent})
		}
		plan.groups[position].rows = append(plan.groups[position].rows, row)
	}
	return plan
}

// findSplitTablet returns the index of the tablet whose (PrevRow, EndRow]
// range contains row, or -1 when the tablet list does not cover it. It
// matches the locator cache's containment rule: a nil EndRow is positive
// infinity and a nil PrevRow is negative infinity.
func findSplitTablet(tablets []metadata.TabletInfo, row []byte) int {
	index := sort.Search(len(tablets), func(i int) bool {
		end := tablets[i].EndRow
		if end == nil {
			return true
		}
		return bytes.Compare(end, row) >= 0
	})
	if index == len(tablets) {
		return -1
	}
	if prev := tablets[index].PrevRow; prev != nil && bytes.Compare(row, prev) <= 0 {
		return -1
	}
	return index
}

// normalizeSplitRows validates, copies, sorts and deduplicates the caller's
// split rows.
func normalizeSplitRows(splits [][]byte) ([][]byte, error) {
	if len(splits) == 0 {
		return nil, fmt.Errorf("%w: no split rows", ErrInvalidTableSplit)
	}
	rows := make([][]byte, 0, len(splits))
	for i, row := range splits {
		if len(row) == 0 {
			return nil, fmt.Errorf("%w: split row %d is empty", ErrInvalidTableSplit, i)
		}
		rows = append(rows, append([]byte{}, row...))
	}
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i], rows[j]) < 0
	})
	deduplicated := rows[:1]
	for _, row := range rows[1:] {
		if bytes.Equal(deduplicated[len(deduplicated)-1], row) {
			continue
		}
		deduplicated = append(deduplicated, row)
	}
	return deduplicated, nil
}

// removeSplitRows drops every completed row from pending, preserving the
// sorted order of the remainder.
func removeSplitRows(pending, completed [][]byte) [][]byte {
	if len(completed) == 0 || len(pending) == 0 {
		return pending
	}
	done := make(map[string]struct{}, len(completed))
	for _, row := range completed {
		done[string(row)] = struct{}{}
	}
	remaining := pending[:0]
	for _, row := range pending {
		if _, ok := done[string(row)]; ok {
			continue
		}
		remaining = append(remaining, row)
	}
	return remaining
}

func nextSplitBackoff(backoff time.Duration, policy splitRetryPolicy) time.Duration {
	next := backoff + policy.backoffStep
	if next > policy.maxBackoff {
		return policy.maxBackoff
	}
	return next
}

// mapSplitError maps manager failures onto the package's public sentinels
// while preserving the original error chain, so a joined FATE cleanup
// failure stays reachable through errors.Is alongside the sentinel.
func mapSplitError(tableName string, err error) error {
	if managerclient.IsRetryableEndpointError(err) {
		return fmt.Errorf("%w: table %q: %w", ErrManagerUnavailable, tableName, err)
	}
	var managerErr *managerclient.Error
	if !errors.As(err, &managerErr) {
		return fmt.Errorf("accumulo: add splits to table %q: %w", tableName, err)
	}
	name := managerErr.TableName
	if name == "" {
		name = tableName
	}
	var sentinel error
	switch managerErr.Kind {
	case managerclient.ErrorTableNotFound:
		sentinel = ErrTableNotFound
	case managerclient.ErrorTableOffline:
		sentinel = ErrTableOffline
	case managerclient.ErrorNamespaceNotFound:
		sentinel = ErrNamespaceNotFound
	case managerclient.ErrorInvalidName:
		sentinel = ErrInvalidTableName
	case managerclient.ErrorSecurity:
		sentinel = ErrPermissionDenied
	case managerclient.ErrorNotActive:
		sentinel = ErrManagerUnavailable
	default:
		return fmt.Errorf("accumulo: add splits to table %q: %w", name, err)
	}
	return fmt.Errorf("%w: table %q: %w", sentinel, name, err)
}
