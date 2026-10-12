// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package collectorregistry

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"slices"
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
	// append is already present.
	RecoveryOccupiedSequence RecoveryReason = "occupied_sequence"
)

// ChangeRecovery names the log state a recovery entry replaced: the head
// cell's timestamp and the SHA-256 of its exact stored bytes, so the damage
// stays identifiable after the head is overwritten.
type ChangeRecovery struct {
	Reason        RecoveryReason
	HeadTimestamp int64
	HeadSHA256    string
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
	// current segment when it is among Changes, otherwise 0. Changes with
	// after = SegmentStart-1 reads that segment.
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
}

func (r *Registry) changeCoordinate(id shoal.ID, qualifier string) allocator.Coordinate {
	return allocator.Coordinate{Row: r.registrationCoordinate(id).Row, Family: []byte("a"), Qualifier: []byte(qualifier), Visibility: bytes.Clone(r.config.Visibility)}
}
func (r *Registry) changeHeadCoordinate(id shoal.ID) allocator.Coordinate {
	return r.changeCoordinate(id, "change-head")
}
func (r *Registry) changeEntryCoordinate(id shoal.ID, sequence int64) allocator.Coordinate {
	return r.changeCoordinate(id, fmt.Sprintf("change:%020d", sequence))
}

// readChangeHead returns the latest sequence (0 when the log is empty) and
// the condition that pins it.
func (r *Registry) readChangeHead(ctx context.Context, id shoal.ID) (int64, allocator.Condition, error) {
	sequence, condition, _, e := r.readChangeHeadCell(ctx, id)
	return sequence, condition, e
}

// readChangeHeadCell is readChangeHead that also returns the stored cell.
// When the cell was read but does not decode, it returns ErrUnavailable with
// the cell set, so a revocation can pin and replace it (prepareChange); a
// failed read returns no cell.
func (r *Registry) readChangeHeadCell(ctx context.Context, id shoal.ID) (int64, allocator.Condition, *allocator.Cell, error) {
	coord := r.changeHeadCoordinate(id)
	cell, e := r.read(ctx, coord)
	if shoal.IsErrorCode(e, shoal.ErrorNotFound) {
		return 0, allocator.Condition{Coordinate: coord, Absent: true}, nil, nil
	}
	if e != nil {
		return 0, allocator.Condition{}, nil, e
	}
	pin := allocator.Condition{Coordinate: coord, Value: bytes.Clone(cell.Value), Timestamp: cell.Timestamp, TimestampSet: true}
	var head changeHead
	if decode(cell.Value, &head) != nil || head.CollectorID != id || head.Sequence <= 0 || head.Sequence != cell.Timestamp {
		return 0, pin, &cell, ErrUnavailable
	}
	return head.Sequence, pin, &cell, nil
}

// entryPresent reports whether a change entry exists at sequence.
func (r *Registry) entryPresent(ctx context.Context, id shoal.ID, sequence int64) (bool, error) {
	_, e := r.read(ctx, r.changeEntryCoordinate(id, sequence))
	if shoal.IsErrorCode(e, shoal.ErrorNotFound) {
		return false, nil
	}
	return e == nil, e
}

// maxRecoveryProbes bounds the search for a free sequence when recovering.
const maxRecoveryProbes = 64

type auditWrite struct {
	sequence   int64
	value      []byte
	conditions []allocator.Condition
	updates    []allocator.Update
}

// prepareChange builds the conditions and updates that append next's change
// entry to id's log.
//
// A damaged log never blocks a revocation, the emergency control. When the
// head does not decode, or the entry it would append already exists, a
// provisioning fails closed but a revocation starts a new segment: its entry
// carries a ChangeRecovery naming the head it replaced (timestamp and digest
// of its exact bytes), takes the first free sequence above both the head's
// timestamp and its sequence, and is written in the same mutation as the
// revocation and the new head, pinned to the damaged head's exact cell. A
// head that cannot be read at all (a store failure) still fails: nothing can
// be written then anyway.
func (r *Registry) prepareChange(ctx context.Context, id shoal.ID, next registrationRow, record func(registrationRow) (Change, error)) (auditWrite, error) {
	change, e := record(next)
	if e != nil {
		return auditWrite{}, e
	}
	previous, headCondition, cell, e := r.readChangeHeadCell(ctx, id)
	if e != nil && (cell == nil || change.Action != ChangeRevoke) {
		return auditWrite{}, e
	}
	if e != nil {
		change.Recovery = &ChangeRecovery{Reason: RecoveryUnreadableHead, HeadTimestamp: cell.Timestamp, HeadSHA256: digestBytes(cell.Value)}
		previous = cell.Timestamp
	} else if occupied, e := r.entryPresent(ctx, id, previous+1); e != nil {
		return auditWrite{}, e
	} else if occupied {
		if change.Action != ChangeRevoke {
			return auditWrite{}, ErrUnavailable
		}
		recovery := &ChangeRecovery{Reason: RecoveryOccupiedSequence, HeadTimestamp: previous}
		if cell != nil {
			recovery.HeadTimestamp, recovery.HeadSHA256 = cell.Timestamp, digestBytes(cell.Value)
		} else {
			recovery.HeadSHA256 = digestBytes(nil)
		}
		change.Recovery = recovery
	}
	if previous < 0 || previous > 1<<62 {
		return auditWrite{}, ErrUnavailable
	}
	change.Sequence = previous + 1
	if change.Recovery != nil {
		for probes := 0; ; probes++ {
			occupied, e := r.entryPresent(ctx, id, change.Sequence)
			if e != nil {
				return auditWrite{}, e
			}
			if !occupied {
				break
			}
			if probes == maxRecoveryProbes {
				return auditWrite{}, ErrUnavailable
			}
			change.Sequence++
		}
	}
	var attempt [16]byte
	if _, e := rand.Read(attempt[:]); e != nil {
		return auditWrite{}, ErrUnavailable
	}
	change.Attempt = hex.EncodeToString(attempt[:])
	if validChange(change, id) != nil {
		return auditWrite{}, invalid()
	}
	value, e := encode(change)
	if e != nil {
		return auditWrite{}, invalid()
	}
	head, e := encode(changeHead{CollectorID: id, Sequence: change.Sequence})
	if e != nil {
		return auditWrite{}, invalid()
	}
	// Never write what the readers would refuse: both cells must decode back
	// to exactly what was meant, under the same rules readChange and
	// readChangeHead apply.
	var readBack Change
	var headBack changeHead
	if decode(value, &readBack) != nil || validChange(readBack, id) != nil || !reflect.DeepEqual(readBack, change) || decode(head, &headBack) != nil || headBack != (changeHead{CollectorID: id, Sequence: change.Sequence}) {
		return auditWrite{}, invalid()
	}
	entry := r.changeEntryCoordinate(id, change.Sequence)
	return auditWrite{
		sequence:   change.Sequence,
		value:      value,
		conditions: []allocator.Condition{headCondition, {Coordinate: entry, Absent: true}},
		updates: []allocator.Update{
			{Coordinate: r.changeHeadCoordinate(id), Timestamp: change.Sequence, Value: head},
			{Coordinate: entry, Timestamp: 1, Value: value},
		},
	}, nil
}

// changeApplied reports whether the entry w would write is the one stored.
func (r *Registry) changeApplied(ctx context.Context, id shoal.ID, w auditWrite) (bool, error) {
	cell, e := r.read(ctx, r.changeEntryCoordinate(id, w.sequence))
	if shoal.IsErrorCode(e, shoal.ErrorNotFound) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return cell.Timestamp == 1 && bytes.Equal(cell.Value, w.value), nil
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
			c.Recovery.Reason != RecoveryUnreadableHead && c.Recovery.Reason != RecoveryOccupiedSequence,
			c.Recovery.HeadTimestamp < 0, c.Recovery.HeadTimestamp >= c.Sequence,
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
	if cell.Timestamp != 1 || decode(cell.Value, &c) != nil || c.Sequence != sequence || validChange(c, id) != nil {
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
	head, _, e := r.readChangeHead(ctx, id)
	if e != nil {
		return nil, e
	}
	var out []Change
	for sequence := after + 1; sequence <= head && len(out) < limit; sequence++ {
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
// reads as not found before the head is read.
func (r *Registry) coherentRegistration(ctx context.Context, id shoal.ID, domain []byte) (registrationRow, int64, error) {
	for range maxCASAttempts {
		before, pinBefore, e := r.readChangeHead(ctx, id)
		if e != nil {
			// Corruption elsewhere in the table is not disclosed across
			// domains: check the domain first when the head is unreadable.
			if row, _, readErr := r.readRegistration(ctx, id); readErr != nil {
				return registrationRow{}, 0, readErr
			} else if !bytes.Equal(row.Registration.Domain, domain) {
				return registrationRow{}, 0, auth.ObjectNotFound()
			}
			return registrationRow{}, 0, e
		}
		row, _, e := r.readRegistration(ctx, id)
		if e != nil {
			return registrationRow{}, 0, e
		}
		if !bytes.Equal(row.Registration.Domain, domain) {
			return registrationRow{}, 0, auth.ObjectNotFound()
		}
		after, pinAfter, e := r.readChangeHead(ctx, id)
		if e != nil {
			return registrationRow{}, 0, e
		}
		if before == after && pinBefore.Timestamp == pinAfter.Timestamp && bytes.Equal(pinBefore.Value, pinAfter.Value) {
			return row, after, nil
		}
	}
	return registrationRow{}, 0, ErrUnavailable
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
	view := AdminView{Registration: row.Registration, EnrollmentID: currentEnrollment(row), ChangeCount: head}
	for sequence := head; sequence > 0 && len(view.Changes) < MaxViewChanges; sequence-- {
		c, e := r.readChange(ctx, id, sequence)
		if e != nil {
			return AdminView{}, e
		}
		if !bytes.Equal(c.Registration.Domain, row.Registration.Domain) {
			return AdminView{}, ErrUnavailable
		}
		view.Changes = append(view.Changes, c)
		if c.Recovery != nil {
			// The start of the current segment; older entries belong to a
			// log this registry could not append to.
			view.SegmentStart = c.Sequence
			break
		}
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
