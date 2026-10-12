// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package collectorregistry

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/phrocker/shoal-oss/pkg/collector"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/interaction"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Collector administration (#446 slice 5).
//
// AdminProvision, AdminRevoke and AdminRegistration are the HTTP-facing
// counterparts of Provision, Revoke and Registration. They require
// auth.OperationCollectorAdmin in the caller's own authorization domain and
// act only on registrations in that domain. A delegated decision never acts
// as an admin, and an admin can never provision its own subject or actor as a
// collector.
//
// Every provisioning and revocation, through either API, appends one entry to
// the collector's change log, in the same one-row mutation as the
// registration write: a change is never durable without its entry, and an
// entry never without its change. Entries are immutable and live in their own
// column family of the registration's row, so no cell the existing flows
// write changes (testdata/persisted_rows.golden.json).

// ChangeAction is what a change log entry did.
type ChangeAction string

const (
	ChangeProvision ChangeAction = "provision"
	ChangeRevoke    ChangeAction = "revoke"
)

// ChangeSource is the API a change came through.
type ChangeSource string

const (
	// SourceOperatorAPI is the trusted Go API (Provision, Revoke), which has
	// no caller identity.
	SourceOperatorAPI ChangeSource = "operator_api"
	// SourceCollectorAdmin is the HTTP admin routes, attributed to the admin.
	SourceCollectorAdmin ChangeSource = "collector_admin"
)

// AdminAttribution identifies the admin decision behind a change.
type AdminAttribution struct {
	Subject                  shoal.ID
	Actor                    shoal.ID
	ClientID                 shoal.ID
	Domain                   []byte
	RequestID                shoal.ID
	CorrelationID            shoal.ID
	AuthorizationFingerprint string
}

// Change is one immutable change log entry: the registration as written, who
// wrote it and when.
type Change struct {
	Sequence     int64
	CollectorID  shoal.ID
	Action       ChangeAction
	Source       ChangeSource
	Registration collector.Registration
	// Admin is set exactly when Source is SourceCollectorAdmin.
	Admin *AdminAttribution
	At    time.Time
	// Attempt is random per write attempt. Two writers can compute
	// byte-identical registrations and entries (the same admin decision at
	// the same instant); after a lost acknowledgement only the attempt tells
	// a writer whether the stored entry is its own.
	Attempt string
	// Recovery is set on a revocation that started a new segment of the log
	// because the log could not be appended to normally (see prepareChange).
	// Entries before it are not part of this segment.
	Recovery *ChangeRecovery
}

// RecoveryReason says why a revocation started a new log segment.
type RecoveryReason string

const (
	// RecoveryUnreadableHead: the head cell exists but does not decode.
	RecoveryUnreadableHead RecoveryReason = "unreadable_head"
	// RecoveryOccupiedSequence: the head decodes, but the entry it would
	// append is already present, or the head is absent over stored entries.
	RecoveryOccupiedSequence RecoveryReason = "occupied_sequence"
	// RecoveryExhaustedHead: the head decodes but sits at the largest
	// timestamp, so no later head can supersede it.
	RecoveryExhaustedHead RecoveryReason = "exhausted_head"
)

// ChangeRecovery names the log state a recovery entry replaced: the head
// cell's timestamp and the SHA-256 of its exact stored bytes, so the damage
// stays identifiable after the head is overwritten.
type ChangeRecovery struct {
	Reason        RecoveryReason
	HeadTimestamp int64
	HeadSHA256    string
	// AcknowledgedOverflow is the hex of the highest overflow qualifier
	// stored when this recovery started its segment, if any: the overflow
	// entries it accounts for, written by an earlier revocation or stray.
	AcknowledgedOverflow string `json:",omitempty"`
}

// AdminResult is the outcome of an admin write. Changed reports whether this
// call made the change; a retry of a change that already applied reports
// false with the same registration.
type AdminResult struct {
	Registration collector.Registration
	Changed      bool
	Sequence     int64
}

// AdminView is a registration as an admin reads it.
type AdminView struct {
	Registration collector.Registration
	EnrollmentID shoal.ID
	// Changes are the most recent change log entries, newest first, ending
	// at the start of the current log segment when that is among them.
	Changes     []Change
	ChangeCount int64
	// SegmentStart is the sequence of the recovery entry that starts the
	// current log segment (0 when the log never needed recovery). Changes
	// and this view read only that segment. ChangeCount is the newest
	// sequence, which after a recovery is not a count of stored entries.
	SegmentStart int64
}

// MaxViewChanges bounds the entries AdminRegistration returns.
const MaxViewChanges = 16

// MaxChangesPage bounds one Changes page.
const MaxChangesPage = 256

type attribution struct {
	source ChangeSource
	admin  *AdminAttribution
	at     time.Time
}

func (r *Registry) operator() (attribution, error) {
	now, e := r.now()
	if e != nil {
		return attribution{}, e
	}
	return attribution{source: SourceOperatorAPI, at: now}, nil
}

func (a attribution) record(action ChangeAction) func(registrationRow) (Change, error) {
	return func(next registrationRow) (Change, error) {
		c := Change{CollectorID: next.Registration.CollectorID, Action: action, Source: a.source, Registration: next.Registration, At: a.at}
		if a.admin != nil {
			admin := *a.admin
			admin.Domain = bytes.Clone(admin.Domain)
			c.Admin = &admin
		}
		return c, nil
	}
}

type changeHead struct {
	CollectorID shoal.ID
	Sequence    int64
	// SegmentStart is the sequence of the recovery entry that started the
	// current segment, 0 when the log never needed one.
	SegmentStart int64
	// OverflowAck is the hex of the highest overflow qualifier a recovery
	// recorded (ChangeRecovery.AcknowledgedOverflow); overflow cells at or
	// below it are already accounted for and no longer damage.
	OverflowAck string `json:",omitempty"`
}

func (r *Registry) changeCoordinate(id shoal.ID, qualifier string) allocator.Coordinate {
	return allocator.Coordinate{Row: r.registrationCoordinate(id).Row, Family: []byte(changeFamily), Qualifier: []byte(qualifier), Visibility: bytes.Clone(r.config.Visibility)}
}
func (r *Registry) changeHeadCoordinate(id shoal.ID) allocator.Coordinate {
	return r.changeCoordinate(id, "change-head")
}
func (r *Registry) changeEntryCoordinate(id shoal.ID, sequence int64) allocator.Coordinate {
	return r.changeCoordinate(id, fmt.Sprintf("%s%020d", entryPrefix, sequence))
}

const (
	changeFamily = "a"
	entryPrefix  = "change:"
	// overflowPrefix holds a recovery entry when no sequence above the damage
	// is representable (see prepareChange).
	overflowPrefix = "change-overflow:"
	scanPage       = 1000
)

// headState is what one raw read of the head found.
type headState struct {
	head    changeHead
	present bool
	// damaged: present but not exactly a head this registry wrote: more or
	// fewer than one cell, a non-positive timestamp, or bytes that do not
	// decode to a head for this collector at its own timestamp.
	damaged   bool
	timestamp int64
	digest    string
	pin       allocator.Condition
}

// readHeadState reads the head without interpreting a malformed cell as a
// store failure: only an error from the store itself is returned as one.
func (r *Registry) readHeadState(ctx context.Context, id shoal.ID) (headState, error) {
	coord := r.changeHeadCoordinate(id)
	cells, e := r.config.Backend.ReadExact(ctx, []allocator.Coordinate{coord})
	if e != nil {
		return headState{}, ErrUnavailable
	}
	if len(cells) == 0 {
		return headState{pin: allocator.Condition{Coordinate: coord, Absent: true}}, nil
	}
	s := headState{present: true, timestamp: max(cells[0].Timestamp, 0), digest: digestBytes(cells[0].Value)}
	cell := cells[0]
	var head changeHead
	if len(cells) == 1 && sameCoordinate(cell.Coordinate, coord) && cell.Timestamp > 0 {
		// One stored cell can be pinned exactly, whatever its bytes.
		s.pin = allocator.Condition{Coordinate: coord, Value: bytes.Clone(cell.Value), Timestamp: cell.Timestamp, TimestampSet: true}
	}
	if len(cells) != 1 || !sameCoordinate(cell.Coordinate, coord) || cell.Timestamp <= 0 || decode(cell.Value, &head) != nil || head.CollectorID != id || head.Sequence <= 0 || head.SegmentStart < 0 || head.SegmentStart > head.Sequence || !validOverflowAck(head.OverflowAck) {
		s.damaged = true
		return s, nil
	}
	s.head = head
	s.pin = allocator.Condition{Coordinate: coord, Value: bytes.Clone(cell.Value), Timestamp: cell.Timestamp, TimestampSet: true}
	return s, nil
}

// readChangeHead returns the head of a healthy log (a zero head when the log
// is empty) and fails closed on a damaged one.
func (r *Registry) readChangeHead(ctx context.Context, id shoal.ID) (changeHead, error) {
	s, e := r.readHeadState(ctx, id)
	if e != nil {
		return changeHead{}, e
	}
	if s.damaged {
		return changeHead{}, ErrUnavailable
	}
	return s.head, nil
}

// entryPresent reports whether anything at all is stored at sequence's entry
// coordinate. A malformed cell is present: only a store error is an error.
func (r *Registry) entryPresent(ctx context.Context, id shoal.ID, sequence int64) (bool, error) {
	cells, e := r.config.Backend.ReadExact(ctx, []allocator.Coordinate{r.changeEntryCoordinate(id, sequence)})
	if e != nil {
		return false, ErrUnavailable
	}
	return len(cells) != 0, nil
}

type rowScanner interface {
	ScanRowPrefix(context.Context, []byte, []byte, []byte, []byte, int) ([]allocator.Cell, error)
}

// highestEntry returns the highest sequence with any cell stored at its entry
// coordinate, by paging through the log's entry qualifiers, which sort in
// sequence order. ok is false when the store cannot scan a row.
func (r *Registry) highestEntry(ctx context.Context, id shoal.ID) (highest int64, ok bool, err error) {
	scanner, ok := r.config.Backend.(rowScanner)
	if !ok {
		return 0, false, nil
	}
	row := r.registrationCoordinate(id).Row
	start := []byte(entryPrefix)
	for {
		cells, e := scanner.ScanRowPrefix(ctx, row, []byte(changeFamily), start, bytes.Clone(r.config.Visibility), scanPage)
		if e != nil {
			return 0, true, ErrUnavailable
		}
		for _, cell := range cells {
			q := string(cell.Coordinate.Qualifier)
			if !strings.HasPrefix(q, entryPrefix) {
				return highest, true, nil
			}
			if n, e := strconv.ParseInt(strings.TrimPrefix(q, entryPrefix), 10, 64); e == nil && n > highest {
				highest = n
			}
		}
		// Page until a scan returns nothing: a store may apply the limit
		// before filtering, so a short page does not mean the end.
		if len(cells) == 0 {
			return highest, true, nil
		}
		next := append(bytes.Clone(cells[len(cells)-1].Coordinate.Qualifier), 0)
		if bytes.Compare(next, start) <= 0 {
			return 0, true, ErrUnavailable
		}
		start = next
	}
}

// errLogMoved reports that the log head changed while a decision about it
// was being made: a concurrent audited write committed. The caller re-reads
// and decides again; it is never returned to a client.
var errLogMoved = errors.New("collector change log moved")

// sameHead reports whether two head reads saw the same stored cell.
func sameHead(a, b headState) bool {
	return a.present == b.present && a.timestamp == b.timestamp && a.digest == b.digest
}

// headStill re-reads the head and reports errLogMoved when it is no longer
// the one read as was.
func (r *Registry) headStill(ctx context.Context, id shoal.ID, was headState) error {
	now, e := r.readHeadState(ctx, id)
	if e != nil {
		return e
	}
	if !sameHead(was, now) {
		return errLogMoved
	}
	return nil
}

// logBeyondHead fails closed when the log holds an entry right after the
// head's sequence: an accepted change recorded without a head (an
// exhausted_head recovery) or a stray. Entry and head are written in one
// mutation, but this probe is a separate read, so an entry that appears
// because a concurrent write committed meanwhile is told apart by re-reading
// the head: errLogMoved then, damage only if the head is unchanged.
//
// An absent head over stored entries hides history and fails closed the
// same way, with the same re-read: a first logged change that commits
// between the head read and the probe is concurrency, not damage.
//
// Where the store can scan, any entry above the head's sequence (not only
// the next one) and any overflow entry are such changes too: an
// exhausted_head recovery lands above the highest stored entry, and an
// overflow entry has no sequence at all. A store without row scans is
// limited to the probe of the next sequence.
func (r *Registry) logBeyondHead(ctx context.Context, id shoal.ID, state headState) error {
	beyond, e := r.entriesBeyond(ctx, id, state)
	if e != nil {
		return e
	}
	if !beyond {
		return nil
	}
	if e := r.headStill(ctx, id, state); e != nil {
		return e
	}
	return ErrUnavailable
}

// entriesBeyond reports whether the log stores anything the head does not
// describe: see logBeyondHead.
func (r *Registry) entriesBeyond(ctx context.Context, id shoal.ID, state headState) (bool, error) {
	if _, scanned := r.config.Backend.(rowScanner); scanned {
		if above, e := r.entryAbove(ctx, id, state.head.Sequence); e != nil || above {
			return above, e
		}
		ack, e := hex.DecodeString(state.head.OverflowAck)
		if e != nil {
			return false, ErrUnavailable
		}
		overflow, e := r.highestOverflow(ctx, id, ack)
		return overflow != nil, e
	}
	if !state.present {
		return r.entryPresent(ctx, id, 1)
	}
	if state.head.Sequence == math.MaxInt64 {
		return false, nil
	}
	return r.entryPresent(ctx, id, state.head.Sequence+1)
}

// entryAbove reports whether any entry with a sequence above sequence is
// stored, scanning only from sequence+1 so that a read does not page through
// the whole log. (A recovery still scans every entry, see highestEntry.)
func (r *Registry) entryAbove(ctx context.Context, id shoal.ID, sequence int64) (bool, error) {
	if sequence == math.MaxInt64 {
		return false, nil
	}
	scanner := r.config.Backend.(rowScanner)
	row := r.registrationCoordinate(id).Row
	start := r.changeEntryCoordinate(id, sequence+1).Qualifier
	for {
		cells, e := scanner.ScanRowPrefix(ctx, row, []byte(changeFamily), start, bytes.Clone(r.config.Visibility), scanPage)
		if e != nil {
			return false, ErrUnavailable
		}
		if len(cells) == 0 {
			return false, nil
		}
		for _, cell := range cells {
			q := string(cell.Coordinate.Qualifier)
			if !strings.HasPrefix(q, entryPrefix) {
				return false, nil
			}
			if n, e := strconv.ParseInt(strings.TrimPrefix(q, entryPrefix), 10, 64); e == nil && n > sequence {
				return true, nil
			}
		}
		next := append(bytes.Clone(cells[len(cells)-1].Coordinate.Qualifier), 0)
		if bytes.Compare(next, start) <= 0 {
			return false, ErrUnavailable
		}
		start = next
	}
}

// highestOverflow returns the highest overflow qualifier above after (nil
// for none), paging until an empty page or a qualifier past the overflow
// prefix, since a store may apply the scan limit before filtering. The
// store can scan rows when this is called.
func (r *Registry) highestOverflow(ctx context.Context, id shoal.ID, after []byte) ([]byte, error) {
	scanner := r.config.Backend.(rowScanner)
	row := r.registrationCoordinate(id).Row
	start := []byte(overflowPrefix)
	if len(after) != 0 {
		start = append(bytes.Clone(after), 0)
	}
	var highest []byte
	for {
		cells, e := scanner.ScanRowPrefix(ctx, row, []byte(changeFamily), start, bytes.Clone(r.config.Visibility), scanPage)
		if e != nil {
			return nil, ErrUnavailable
		}
		if len(cells) == 0 {
			return highest, nil
		}
		for _, cell := range cells {
			q := cell.Coordinate.Qualifier
			if !bytes.HasPrefix(q, []byte(overflowPrefix)) {
				return highest, nil
			}
			if bytes.Compare(q, after) > 0 && bytes.Compare(q, highest) > 0 {
				highest = bytes.Clone(q)
			}
		}
		next := append(bytes.Clone(cells[len(cells)-1].Coordinate.Qualifier), 0)
		if bytes.Compare(next, start) <= 0 {
			return nil, ErrUnavailable
		}
		start = next
	}
}

type auditWrite struct {
	sequence  int64
	qualifier []byte
	value     []byte
	timestamp int64
	// headValue is the head written with the entry, nil when none is.
	headValue  []byte
	conditions []allocator.Condition
	updates    []allocator.Update
}

// prepareChange builds the conditions and updates that append next's change
// entry to id's log. Every audited write is also a write of the registration
// cell under its compare-and-set condition (updateRegistrationAudited), so
// audited writers are serialized by that condition; the head condition only
// pins what a normal append read.
//
// A damaged log never blocks a revocation, the emergency control. The log is
// damaged when the head is present but not a head this registry wrote, when
// the head is absent while entries exist, or when the entry it would append
// is already occupied. A provisioning then fails closed. A revocation starts
// a new segment:
//   - its entry carries a ChangeRecovery naming the damaged head (its
//     timestamp, clamped to 0, and the SHA-256 of its exact bytes, or of
//     nothing when absent);
//   - its sequence is one above the higher of the
//     head's sequence and every stored entry (found by scanning the row
//     where the store can; by probing upward otherwise), so no stored
//     entry sits above the new segment and later appends proceed normally;
//   - the new head records the new sequence as SegmentStart; cells are
//     written at timestamps described in the docs ("Delete markers");
//   - all of it, with the revocation, is one mutation.
//
// If no head can supersede the damaged one, the entry is written without a
// head; if no sequence above the damage is representable, the entry is written
// under an overflow qualifier and the head is left as it is: the revocation
// still applies and is still recorded, and the log reads as unavailable.
// Only a failure of the store itself fails a revocation, since then nothing
// can be written anyway.
func (r *Registry) prepareChange(ctx context.Context, id shoal.ID, next registrationRow, record func(registrationRow) (Change, error)) (auditWrite, error) {
	change, e := record(next)
	if e != nil {
		return auditWrite{}, e
	}
	head, e := r.readHeadState(ctx, id)
	if e != nil {
		return auditWrite{}, e
	}
	reason := RecoveryReason("")
	switch {
	case head.damaged:
		reason = RecoveryUnreadableHead
	case head.timestamp >= maxWriteTimestamp:
		reason = RecoveryExhaustedHead
	case head.head.Sequence == math.MaxInt64:
		reason = RecoveryOccupiedSequence
	default:
		// Anything stored that the head does not describe (the next
		// sequence, any higher entry or an overflow entry where the store
		// can scan, or entries under an absent head) is the damage the
		// readers fail closed on, so a write treats it the same way: a
		// provisioning fails closed and a revocation recovers above it,
		// leaving the log readable again.
		occupied, e := r.entriesBeyond(ctx, id, head)
		if e != nil {
			return auditWrite{}, e
		}
		if occupied {
			// A concurrent write may have committed between the head read
			// and the probe; that is not damage.
			if e := r.headStill(ctx, id, head); e != nil {
				return auditWrite{}, e
			}
			reason = RecoveryOccupiedSequence
		}
	}
	headCell := r.changeHeadCoordinate(id)
	nextHead := changeHead{CollectorID: id, Sequence: head.head.Sequence + 1, SegmentStart: head.head.SegmentStart, OverflowAck: head.head.OverflowAck}
	conditions := []allocator.Condition{head.pin}
	overflow := false
	if reason != "" {
		if change.Action != ChangeRevoke {
			return auditWrite{}, ErrUnavailable
		}
		recovery := &ChangeRecovery{Reason: reason, HeadTimestamp: head.timestamp, HeadSHA256: head.digest}
		if !head.present {
			recovery.HeadSHA256 = digestBytes(nil)
		}
		change.Recovery = recovery
		// Sequences are independent of timestamps: a damaged head's
		// timestamp says nothing about which sequences are taken.
		base := head.head.Sequence
		highest, scanned, e := r.highestEntry(ctx, id)
		if e != nil {
			return auditWrite{}, e
		}
		base = max(base, highest)
		sequence, e := r.freeAbove(ctx, id, base, scanned)
		if e != nil {
			return auditWrite{}, e
		}
		nextHead = changeHead{CollectorID: id, Sequence: sequence, SegmentStart: sequence}
		// Account for every overflow entry stored now, so the new segment
		// reads again; the recovery entry names them.
		if scanned {
			overflow, e := r.highestOverflow(ctx, id, nil)
			if e != nil {
				return auditWrite{}, e
			}
			if overflow != nil {
				// A qualifier longer than the bound is acknowledged by a
				// bounded value that still sorts above it in practice: its
				// prefix followed by 0xff bytes.
				if len(overflow) > shoal.MaxIDBytes {
					overflow = append(bytes.Clone(overflow[:shoal.MaxIDBytes-8]), bytes.Repeat([]byte{0xff}, 8)...)
				}
				nextHead.OverflowAck = hex.EncodeToString(overflow)
				recovery.AcknowledgedOverflow = nextHead.OverflowAck
			}
		}
		// The recovery is pinned to the head it judged, so it cannot commit
		// over a head that became healthy meanwhile (a concurrent write's
		// repair, which does not move the registration cell): an absent
		// head by an Absent condition, one stored cell by its exact value
		// and timestamp. Only a head that is not one pinnable cell (several
		// versions, a non-positive timestamp) is left unpinned, serialized
		// by the registration condition alone; the registry never writes
		// such a head, so no write of its own can race the recovery there.
		if head.pin.Coordinate.Row == nil {
			conditions = nil
		}
		if sequence <= 0 {
			overflow = true
		}
	}
	// A head at the largest timestamp cannot be superseded: the entry is
	// still written and the log reads as unavailable.
	writeHead := !overflow && head.timestamp < maxWriteTimestamp
	// Cells are written at a timestamp above the visible head's and at least
	// the clock's nanoseconds, so a delete marker left at a cell's own
	// timestamp, or at wall-clock milliseconds, does not hide them; any
	// marker above that is caught by ensureVisible after the write.
	timestamp := int64(maxWriteTimestamp)
	if head.timestamp < maxWriteTimestamp {
		timestamp = min(max(r.nanos(), head.timestamp+1, 1), maxWriteTimestamp)
	}
	change.Sequence = nextHead.Sequence
	qualifier := r.changeEntryCoordinate(id, change.Sequence).Qualifier
	var attempt [16]byte
	if _, e := rand.Read(attempt[:]); e != nil {
		return auditWrite{}, ErrUnavailable
	}
	change.Attempt = hex.EncodeToString(attempt[:])
	if overflow {
		change.Sequence = math.MaxInt64
		qualifier = []byte(overflowPrefix + change.Attempt)
	}
	if validChange(change, id) != nil {
		return auditWrite{}, invalid()
	}
	value, e := encode(change)
	if e != nil {
		return auditWrite{}, invalid()
	}
	headValue, e := encode(nextHead)
	if e != nil {
		return auditWrite{}, invalid()
	}
	// Never write what the readers would refuse: both cells must decode back
	// to exactly what was meant, under the same rules readChange and
	// readHeadState apply.
	var readBack Change
	var headBack changeHead
	if decode(value, &readBack) != nil || validChange(readBack, id) != nil || !reflect.DeepEqual(readBack, change) || decode(headValue, &headBack) != nil || headBack != nextHead {
		return auditWrite{}, invalid()
	}
	entry := r.changeCoordinate(id, string(qualifier))
	w := auditWrite{sequence: change.Sequence, qualifier: qualifier, value: value, timestamp: timestamp, conditions: append(conditions, allocator.Condition{Coordinate: entry, Absent: true}), updates: []allocator.Update{{Coordinate: entry, Timestamp: timestamp, Value: value}}}
	if writeHead {
		w.headValue = headValue
		w.updates = append(w.updates, allocator.Update{Coordinate: headCell, Timestamp: timestamp, Value: headValue})
	}
	return w, nil
}

// freeAbove returns the sequence a recovery entry takes: base+1 when the row
// was scanned (nothing stored lies above base), otherwise the first free
// sequence found probing upward in doubling steps. It returns 0 when no such
// sequence is representable.
func (r *Registry) freeAbove(ctx context.Context, id shoal.ID, base int64, scanned bool) (int64, error) {
	if base >= math.MaxInt64-1 {
		return 0, nil
	}
	if scanned {
		return base + 1, nil
	}
	for step := int64(1); ; step *= 2 {
		if base > math.MaxInt64-1-step {
			return 0, nil
		}
		occupied, e := r.entryPresent(ctx, id, base+step)
		if e != nil {
			return 0, e
		}
		if !occupied {
			return base + step, nil
		}
		if step > math.MaxInt64/4 {
			return 0, nil
		}
	}
}

// changeApplied reports whether the entry w would write is the one stored.
func (r *Registry) changeApplied(ctx context.Context, id shoal.ID, w auditWrite) (bool, error) {
	cell, e := r.read(ctx, r.changeCoordinate(id, string(w.qualifier)))
	if shoal.IsErrorCode(e, shoal.ErrorNotFound) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return cell.Timestamp > 0 && bytes.Equal(cell.Value, w.value), nil
}

func (r *Registry) nanos() int64 {
	return r.config.Clock().UTC().UnixNano()
}

// visible reports whether exactly value is what a read of coord returns.
func (r *Registry) visible(ctx context.Context, coord allocator.Coordinate, value []byte) (bool, error) {
	cells, e := r.config.Backend.ReadExact(ctx, []allocator.Coordinate{coord})
	if e != nil {
		return false, ErrUnavailable
	}
	return len(cells) == 1 && cells[0].Timestamp > 0 && bytes.Equal(cells[0].Value, value), nil
}

// maxWriteTimestamp is the highest timestamp the registry writes a log cell
// at, keeping the largest timestamp free for nothing but outside writes. A
// head that reaches it exhausts the log (docs/collectors.md, "Delete
// markers").
const maxWriteTimestamp = math.MaxInt64 - 1

// maxVisibilityRepairs bounds ensureVisible: up to 63 doubling steps above
// the write's own timestamp, then up to 63 halvings of the room left below
// maxWriteTimestamp.
const maxVisibilityRepairs = 128

// ensureVisible runs after an audited write was accepted and checks that its
// entry and head read back. A delete marker at or above the timestamp they
// were written at hides them from every read and condition, so the registry
// cannot see it beforehand.
//
// While the registration cell is still the one this write produced, a
// hidden cell is written again at the write's timestamp plus a doubling step
// (1, 2, 4, ...), kept below maxWriteTimestamp, conditioned on that cell, until
// it reads back. Stepping from the write's own timestamp, not doubling the
// absolute one, means a marker just above the clock costs a small jump, not
// a jump towards the largest timestamp.
//
// Once a later audited write has committed on top (the registration cell
// moved), this write's head has legitimately been superseded: it is then
// enough that the entry reads back with exactly this write's bytes and the
// visible head is healthy at or past this write's sequence.
//
// If neither can be established the outcome is reported for reconciliation,
// never as a clean change.
func (r *Registry) ensureVisible(ctx context.Context, id shoal.ID, registration allocator.Condition, w auditWrite) error {
	entry := r.changeCoordinate(id, string(w.qualifier))
	head := r.changeHeadCoordinate(id)
	superseded := func() (bool, error) {
		cells, e := r.config.Backend.ReadExact(ctx, []allocator.Coordinate{registration.Coordinate})
		if e != nil {
			return false, ErrUnavailable
		}
		return !(len(cells) == 1 && cells[0].Timestamp == registration.Timestamp && bytes.Equal(cells[0].Value, registration.Value)), nil
	}
	settledOnTop := func() (bool, error) {
		entryOK, e := r.visible(ctx, entry, w.value)
		if e != nil || !entryOK {
			return false, e
		}
		if w.headValue == nil {
			return true, nil
		}
		state, e := r.readHeadState(ctx, id)
		if e != nil {
			return false, e
		}
		// The superseding head must still describe this entry: a recovery
		// head that starts its segment past it has left it unreadable.
		return state.present && !state.damaged && state.head.Sequence >= w.sequence && state.head.SegmentStart <= w.sequence, nil
	}
	step, last := int64(1), w.timestamp
	for range maxVisibilityRepairs + 1 {
		entryOK, e := r.visible(ctx, entry, w.value)
		if e != nil {
			return e
		}
		headOK := w.headValue == nil
		if !headOK {
			if headOK, e = r.visible(ctx, head, w.headValue); e != nil {
				return e
			}
		}
		if entryOK && headOK {
			return nil
		}
		moved, e := superseded()
		if e != nil {
			return e
		}
		if moved {
			if ok, e := settledOnTop(); e != nil {
				return e
			} else if ok {
				return nil
			}
			return ErrUnavailable
		}
		// Doubling steps above the write's timestamp while they fit below
		// maxWriteTimestamp, then halving the remaining room, so a repair
		// lands below maxWriteTimestamp whenever the marker does and the
		// head is never left where nothing can supersede it.
		var timestamp int64
		switch {
		case last >= maxWriteTimestamp-1:
			return ErrUnavailable
		case step < maxWriteTimestamp-1-w.timestamp:
			timestamp = w.timestamp + step
			if step > math.MaxInt64/2 {
				step = math.MaxInt64
			} else {
				step *= 2
			}
		default:
			timestamp = last + max((maxWriteTimestamp-1-last)/2, 1)
		}
		last = timestamp
		mutation := allocator.Mutation{Row: entry.Row, Conditions: []allocator.Condition{registration}}
		if !entryOK {
			mutation.Conditions = append(mutation.Conditions, allocator.Condition{Coordinate: entry, Absent: true})
			mutation.Updates = append(mutation.Updates, allocator.Update{Coordinate: entry, Timestamp: timestamp, Value: w.value})
		}
		if !headOK {
			mutation.Updates = append(mutation.Updates, allocator.Update{Coordinate: head, Timestamp: timestamp, Value: w.headValue})
		}
		status, e := r.config.Backend.CompareAndMutate(ctx, mutation)
		if e != nil {
			return ErrUnavailable
		}
		_ = status // a rejection is re-evaluated at the top of the loop
	}
	return ErrUnavailable
}

func validChange(c Change, id shoal.ID) error {
	if c.Sequence <= 0 || c.CollectorID != id || c.Registration.CollectorID != id || c.At.IsZero() || len(c.Attempt) != 32 {
		return invalid()
	}
	if _, e := c.Registration.Canonical(); e != nil {
		return invalid()
	}
	if c.Action != ChangeProvision && c.Action != ChangeRevoke {
		return invalid()
	}
	if (c.Action == ChangeRevoke) != (c.Registration.State == collector.Revoked) {
		return invalid()
	}
	switch c.Source {
	case SourceOperatorAPI:
		if c.Admin != nil {
			return invalid()
		}
	case SourceCollectorAdmin:
		if c.Admin == nil || validAttribution(*c.Admin) != nil || !bytes.Equal(c.Admin.Domain, c.Registration.Domain) {
			return invalid()
		}
	default:
		return invalid()
	}
	if c.Recovery != nil {
		switch {
		case c.Action != ChangeRevoke,
			c.Recovery.Reason != RecoveryUnreadableHead && c.Recovery.Reason != RecoveryOccupiedSequence && c.Recovery.Reason != RecoveryExhaustedHead,
			c.Recovery.HeadTimestamp < 0,
			!validOverflowAck(c.Recovery.AcknowledgedOverflow),
			!collector.ValidDigest(c.Recovery.HeadSHA256):
			return invalid()
		}
	}
	return nil
}

// validAttribution applies one rule to every recorded admin identity:
// required where the decision requires it, within shoal.MaxIDBytes, valid
// UTF-8 and printable. encoding/json would silently rewrite invalid UTF-8 to
// U+FFFD, producing an entry that no longer decodes to itself, and the wire
// validator refuses such IDs. The correlation ID uses the interaction rule
// verbatim (interaction.ValidateCorrelationID). Subjects, actors, clients and
// request IDs may contain an ASCII space, as OIDC-mapped subjects do, so they
// use the same rule without the no-space clause.
func validAttribution(a AdminAttribution) error {
	for _, field := range []struct {
		id       shoal.ID
		required bool
	}{{a.Subject, true}, {a.Actor, true}, {a.ClientID, false}, {a.RequestID, true}} {
		if !printableID(field.id, field.required) {
			return invalid()
		}
	}
	if interaction.ValidateCorrelationID(a.CorrelationID) != nil || a.AuthorizationFingerprint == "" || !printableID(shoal.ID(a.AuthorizationFingerprint), true) || len(a.Domain) == 0 || len(a.Domain) > shoal.MaxIDBytes {
		return invalid()
	}
	return nil
}

func printableID(id shoal.ID, required bool) bool {
	if id == "" {
		return !required
	}
	if len(id) > shoal.MaxIDBytes || !utf8.ValidString(string(id)) {
		return false
	}
	for _, r := range string(id) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// validOverflowAck accepts "" or the lowercase hex of an overflow qualifier.
func validOverflowAck(ack string) bool {
	if ack == "" {
		return true
	}
	b, e := hex.DecodeString(ack)
	return e == nil && hex.EncodeToString(b) == ack && bytes.HasPrefix(b, []byte(overflowPrefix)) && len(b) <= shoal.MaxIDBytes
}

func digestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (r *Registry) readChange(ctx context.Context, id shoal.ID, sequence int64) (Change, error) {
	cell, e := r.read(ctx, r.changeEntryCoordinate(id, sequence))
	if e != nil {
		// The head names this entry, so its absence is corruption.
		return Change{}, ErrUnavailable
	}
	var c Change
	if cell.Timestamp <= 0 || decode(cell.Value, &c) != nil || c.Sequence != sequence || validChange(c, id) != nil {
		return Change{}, ErrUnavailable
	}
	return c, nil
}

// Changes is a trusted operator read of id's change log with no caller
// check: up to limit entries with a sequence after after, oldest first.
func (r *Registry) Changes(ctx context.Context, id shoal.ID, after int64, limit int) ([]Change, error) {
	if shoal.ValidateRequiredID("collector ID", id) != nil || after < 0 || limit <= 0 || limit > MaxChangesPage {
		return nil, invalid()
	}
	var state headState
	for attempt := 0; ; attempt++ {
		var e error
		if state, e = r.readHeadState(ctx, id); e != nil {
			return nil, e
		}
		if state.damaged {
			return nil, ErrUnavailable
		}
		e = r.logBeyondHead(ctx, id, state)
		if errors.Is(e, errLogMoved) {
			if attempt < maxCASAttempts {
				continue
			}
			// Concurrency, not damage: the caller retries.
			return nil, ErrConflict
		}
		if e != nil {
			return nil, ErrUnavailable
		}
		break
	}
	head := state.head
	// Only the current segment is readable; entries before its recovery
	// entry stay stored, for forensics, outside this API.
	after = max(after, head.SegmentStart-1)
	var out []Change
	// Counted without ever incrementing past head.Sequence, which may be
	// the largest sequence.
	for sequence := after; sequence < head.Sequence && len(out) < limit; {
		sequence++
		c, e := r.readChange(ctx, id, sequence)
		if e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, nil
}

// admin resolves a collector-admin decision. Delegated decisions, service
// decisions and executor-bound decisions never administer collectors.
func (r *Registry) admin(ctx context.Context) (auth.Decision, auth.Fingerprint, attribution, error) {
	d, fp, now, e := r.caller(ctx, auth.OperationCollectorAdmin)
	if e != nil {
		return auth.Decision{}, auth.Fingerprint{}, attribution{}, e
	}
	if len(d.OnBehalfOf()) != 0 || d.TrustedService() || d.ExecutorBinding() != "" {
		return auth.Decision{}, auth.Fingerprint{}, attribution{}, unauthorized()
	}
	admin := &AdminAttribution{
		Subject: d.Subject(), Actor: d.Actor(), ClientID: d.ClientID(), Domain: d.AuthorizationDomain(),
		RequestID: d.RequestID(), CorrelationID: d.CorrelationID(), AuthorizationFingerprint: fp.String(),
	}
	// An admin whose identity cannot be recorded faithfully is not an admin:
	// it is refused before anything is read or written.
	if validAttribution(*admin) != nil {
		return auth.Decision{}, auth.Fingerprint{}, attribution{}, unauthorized()
	}
	return d, fp, attribution{source: SourceCollectorAdmin, at: now, admin: admin}, nil
}

// selfProvisioning reports whether p would make the admin a collector: its
// subject is the admin's subject or actor. A collector's principal is its
// (domain, subject, client); refusing on the subject alone also refuses the
// admin's own subject under another client.
func selfProvisioning(p Provisioning, d auth.Decision) bool {
	return p.Subject == d.Subject() || p.Subject == d.Actor()
}

// finishAdmin re-checks the admin's authorization after the write. A changed
// authorization after a write leaves the outcome for the caller to reconcile.
func (r *Registry) finishAdmin(ctx context.Context, fp auth.Fingerprint, row registrationRow, sequence int64, e error) (AdminResult, error) {
	if e != nil {
		return AdminResult{}, e
	}
	if e := r.recheck(ctx, auth.OperationCollectorAdmin, fp); e != nil {
		if sequence > 0 {
			return AdminResult{}, indeterminate(e)
		}
		return AdminResult{}, e
	}
	return AdminResult{Registration: row.Registration, Changed: sequence > 0, Sequence: sequence}, nil
}

// AdminProvision is Provision for a collector-admin decision. generation is
// the generation the caller expects the registration to hold afterwards: 1 to
// create, the current generation to reactivate a revoked registration or to
// confirm an identical active one. Anything else, a different identity, or
// different terms on an active registration is a conflict; the request is
// never merged with what is stored. The collector must be in the admin's own
// authorization domain, and its subject may not be the admin's subject or
// actor.
func (r *Registry) AdminProvision(ctx context.Context, p Provisioning, generation int64) (AdminResult, error) {
	if _, e := (collector.Registration{CollectorID: p.CollectorID, Subject: p.Subject, ClientID: p.ClientID, Domain: p.Domain, AuthorityPolicyIDs: p.AuthorityPolicyIDs, Control: p.Control, Mode: p.Mode, Generation: 1, State: collector.Provisioned}).Canonical(); e != nil || generation <= 0 {
		return AdminResult{}, invalid()
	}
	d, fp, by, e := r.admin(ctx)
	if e != nil {
		return AdminResult{}, e
	}
	if !bytes.Equal(p.Domain, d.AuthorizationDomain()) {
		return AdminResult{}, denied("collector domain is not the admin's authorization domain")
	}
	if selfProvisioning(p, d) {
		return AdminResult{}, denied("an admin cannot provision itself as a collector")
	}
	row, sequence, e := r.provision(ctx, p, generation, by)
	return r.finishAdmin(ctx, fp, row, sequence, e)
}

// AdminRevoke is Revoke for a collector-admin decision. generation is the
// generation being revoked: revoking it again is a no-op, any other
// generation a conflict. A collector outside the admin's authorization domain
// reads as not found, exactly like one that does not exist.
func (r *Registry) AdminRevoke(ctx context.Context, id shoal.ID, generation int64) (AdminResult, error) {
	if shoal.ValidateRequiredID("collector ID", id) != nil || generation <= 0 {
		return AdminResult{}, invalid()
	}
	d, fp, by, e := r.admin(ctx)
	if e != nil {
		return AdminResult{}, e
	}
	domain := d.AuthorizationDomain()
	row, sequence, e := r.revoke(ctx, id, generation, by, func(reg collector.Registration) error {
		if !bytes.Equal(reg.Domain, domain) {
			return auth.ObjectNotFound()
		}
		return nil
	})
	return r.finishAdmin(ctx, fp, row, sequence, e)
}

// coherentRegistration reads id's registration between two reads of its log
// head that agree, retrying a bounded number of times, so the registration
// and the head describe the same logged state. A registration outside domain
// reads as not found before anything about its log is disclosed.
func (r *Registry) coherentRegistration(ctx context.Context, id shoal.ID, domain []byte) (registrationRow, changeHead, error) {
	// The domain is fixed for a collector's life, so checking it once, before
	// anything about the log is read, keeps every log state of another
	// domain's collector (including store errors on its cells) concealed.
	if row, _, e := r.readRegistration(ctx, id); e != nil {
		return registrationRow{}, changeHead{}, e
	} else if !bytes.Equal(row.Registration.Domain, domain) {
		return registrationRow{}, changeHead{}, auth.ObjectNotFound()
	}
	for range maxCASAttempts {
		before, e := r.readHeadState(ctx, id)
		if e != nil {
			return registrationRow{}, changeHead{}, e
		}
		row, _, e := r.readRegistration(ctx, id)
		if e != nil {
			return registrationRow{}, changeHead{}, e
		}
		if !bytes.Equal(row.Registration.Domain, domain) {
			return registrationRow{}, changeHead{}, auth.ObjectNotFound()
		}
		after, e := r.readHeadState(ctx, id)
		if e != nil {
			return registrationRow{}, changeHead{}, e
		}
		if before.present != after.present || before.digest != after.digest || before.timestamp != after.timestamp {
			continue
		}
		if after.damaged {
			return registrationRow{}, changeHead{}, ErrUnavailable
		}
		if e := r.logBeyondHead(ctx, id, after); errors.Is(e, errLogMoved) {
			continue
		} else if e != nil {
			return registrationRow{}, changeHead{}, e
		}
		return row, after.head, nil
	}
	// The log kept moving under the read: concurrency, not damage, so a
	// retryable conflict rather than unavailable.
	return registrationRow{}, changeHead{}, ErrConflict
}

// AdminRegistration reads a registration in the admin's own authorization
// domain with its most recent change log entries. Quarantine is not part of
// it: observation status is computed from the current registration at every
// read and never cached here.
func (r *Registry) AdminRegistration(ctx context.Context, id shoal.ID) (AdminView, error) {
	if shoal.ValidateRequiredID("collector ID", id) != nil {
		return AdminView{}, invalid()
	}
	d, fp, _, e := r.admin(ctx)
	if e != nil {
		return AdminView{}, e
	}
	row, head, e := r.coherentRegistration(ctx, id, d.AuthorizationDomain())
	if e != nil {
		return AdminView{}, e
	}
	view := AdminView{Registration: row.Registration, EnrollmentID: currentEnrollment(row), ChangeCount: head.Sequence, SegmentStart: head.SegmentStart}
	for sequence := head.Sequence; sequence > 0 && sequence >= head.SegmentStart && len(view.Changes) < MaxViewChanges; sequence-- {
		c, e := r.readChange(ctx, id, sequence)
		if e != nil {
			return AdminView{}, e
		}
		if !bytes.Equal(c.Registration.Domain, row.Registration.Domain) {
			return AdminView{}, ErrUnavailable
		}
		// A recovery entry is exactly the start of the current segment.
		if (c.Recovery != nil) != (c.Sequence == head.SegmentStart) {
			return AdminView{}, ErrUnavailable
		}
		view.Changes = append(view.Changes, c)
	}
	// Coherence: every provisioning and revocation writes the registration
	// and the head in one mutation, so a registration read between two
	// identical head reads is the state the newest entry describes (apart
	// from enrollment, which is not a logged change).
	if len(view.Changes) > 0 {
		newest := view.Changes[0].Registration
		current := row.Registration
		if newest.Generation != current.Generation || (newest.State == collector.Revoked) != (current.State == collector.Revoked) || !slices.Equal(newest.AuthorityPolicyIDs, current.AuthorityPolicyIDs) || newest.Control != current.Control || newest.Mode != current.Mode {
			return AdminView{}, ErrUnavailable
		}
	}
	if e := r.recheck(ctx, auth.OperationCollectorAdmin, fp); e != nil {
		return AdminView{}, auth.ObjectNotFound()
	}
	return view, nil
}
