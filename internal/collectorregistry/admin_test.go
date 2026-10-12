// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package collectorregistry

import (
	"bytes"
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phrocker/shoal-oss/pkg/collector"
	"github.com/phrocker/shoal-oss/pkg/collector/api"
	"github.com/phrocker/shoal-oss/pkg/explorer/auth"
	"github.com/phrocker/shoal-oss/pkg/explorer/coordination/allocator"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

type principal struct {
	subject, actor, client, domain string
	onBehalfOf                     []shoal.ID
	ops                            []auth.Operation
}

func (v *env) bind(t *testing.T, p principal) context.Context {
	t.Helper()
	if p.actor == "" {
		p.actor = p.subject
	}
	if p.client == "" {
		p.client = "admin-client"
	}
	if p.domain == "" {
		p.domain = "domain"
	}
	if p.ops == nil {
		p.ops = []auth.Operation{auth.OperationCollectorAdmin}
	}
	d, e := auth.NewDecision(auth.DecisionConfig{Subject: shoal.ID(p.subject), Actor: shoal.ID(p.actor), ClientID: shoal.ID(p.client), OnBehalfOf: p.onBehalfOf, AuthorizationDomain: []byte(p.domain), AllowedOperations: p.ops, PolicyGeneration: 1, AuthenticationExpires: v.clock.Now().Add(24 * time.Hour), RequestID: "admin-request", CorrelationID: "admin-correlation"})
	if e != nil {
		t.Fatal(e)
	}
	ctx, e := v.authority.Binder().Bind(context.Background(), d)
	if e != nil {
		t.Fatal(e)
	}
	return ctx
}

func (v *env) admin(t *testing.T) context.Context {
	return v.bind(t, principal{subject: "admin"})
}

func grant(id shoal.ID, subject string, authority ...shoal.ID) Provisioning {
	return Provisioning{CollectorID: id, Subject: shoal.ID(subject), ClientID: "client", Domain: []byte("domain"), AuthorityPolicyIDs: authority, Control: collector.ExternalControlled, Mode: collector.ServerObserved}
}

func (v *env) changes(t *testing.T, id shoal.ID) []Change {
	t.Helper()
	out, e := v.registry.Changes(context.Background(), id, 0, MaxChangesPage)
	if e != nil {
		t.Fatal(e)
	}
	return out
}

func (v *env) wrote(t *testing.T, id shoal.ID) bool {
	t.Helper()
	_, e := v.registry.Registration(context.Background(), id)
	if e == nil {
		return true
	}
	if !shoal.IsErrorCode(e, shoal.ErrorNotFound) {
		t.Fatal(e)
	}
	if head, e := v.registry.readChangeHead(context.Background(), id); e != nil || head.Sequence != 0 {
		t.Fatalf("change log written without a registration: %+v %v", head, e)
	}
	return false
}

func TestAdminProvisionIsAuditedAndWorksEndToEnd(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	result, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 1)
	if e != nil {
		t.Fatal(e)
	}
	if !result.Changed || result.Sequence != 1 || result.Registration.State != collector.Provisioned || result.Registration.Generation != 1 {
		t.Fatalf("result %+v", result)
	}
	// The provisioned principal enrolls within the ceiling and nothing more.
	tail := v.as(t, "tail")
	if _, e := v.registry.Enroll(tail, []byte("k"), enrollRequest(collectorID, "authority:logs", "authority:admin")); !errors.Is(e, api.ErrPermissionDenied) {
		t.Fatalf("excess authority: %v", e)
	}
	if _, e := v.registry.Enroll(tail, []byte("k"), enrollRequest(collectorID, "authority:logs")); e != nil {
		t.Fatal(e)
	}
	changes := v.changes(t, collectorID)
	if len(changes) != 1 {
		t.Fatalf("changes %+v", changes)
	}
	c := changes[0]
	if c.Action != ChangeProvision || c.Source != SourceCollectorAdmin || c.Admin == nil || c.Admin.Subject != "admin" || c.Admin.ClientID != "admin-client" || c.Admin.RequestID != "admin-request" || c.Admin.CorrelationID != "admin-correlation" || c.Admin.AuthorizationFingerprint == "" || !bytes.Equal(c.Admin.Domain, []byte("domain")) {
		t.Fatalf("change %+v admin %+v", c, c.Admin)
	}
	view, e := v.registry.AdminRegistration(admin, collectorID)
	if e != nil {
		t.Fatal(e)
	}
	if view.Registration.State != collector.Enrolled || view.EnrollmentID == "" || view.ChangeCount != 1 || len(view.Changes) != 1 {
		t.Fatalf("view %+v", view)
	}
	// The Go API is audited too, without a caller identity.
	if _, e := v.registry.Provision(context.Background(), grant("collector:other", "other", "authority:logs")); e != nil {
		t.Fatal(e)
	}
	if got := v.changes(t, "collector:other"); len(got) != 1 || got[0].Source != SourceOperatorAPI || got[0].Admin != nil {
		t.Fatalf("operator change %+v", got)
	}
}

func TestAdminCannotProvisionItself(t *testing.T) {
	v := newEnv(t)
	admin := v.bind(t, principal{subject: "admin", actor: "admin-actor"})
	for name, p := range map[string]Provisioning{
		"own subject and client":    {CollectorID: collectorID, Subject: "admin", ClientID: "admin-client", Domain: []byte("domain"), AuthorityPolicyIDs: []shoal.ID{"authority:logs"}, Control: collector.ExternalControlled, Mode: collector.ServerObserved},
		"own subject, no client":    {CollectorID: collectorID, Subject: "admin", Domain: []byte("domain"), AuthorityPolicyIDs: []shoal.ID{"authority:logs"}, Control: collector.ExternalControlled, Mode: collector.ServerObserved},
		"own subject, other client": grant(collectorID, "admin", "authority:logs"),
		"own actor":                 grant(collectorID, "admin-actor", "authority:logs"),
	} {
		if _, e := v.registry.AdminProvision(admin, p, 1); !errors.Is(e, api.ErrPermissionDenied) {
			t.Fatalf("%s: %v", name, e)
		}
		if v.wrote(t, collectorID) {
			t.Fatalf("%s: self-provisioning wrote a registration", name)
		}
	}
	// A registration the operator made for the admin's subject cannot be
	// reactivated or confirmed by that admin either.
	if _, e := v.registry.Provision(context.Background(), grant(collectorID, "admin", "authority:logs")); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "admin", "authority:logs"), 1); !errors.Is(e, api.ErrPermissionDenied) {
		t.Fatalf("confirm own registration: %v", e)
	}
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "admin", "authority:logs", "authority:more"), 2); !errors.Is(e, api.ErrPermissionDenied) {
		t.Fatalf("reactivate own registration with more authority: %v", e)
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
		t.Fatalf("own registration reactivated: %+v", reg)
	}
}

func TestOnlyAnUndelegatedCollectorAdminAdministers(t *testing.T) {
	v := newEnv(t)
	v.provision(t, collectorID, "tail", "authority:logs")
	for name, ctx := range map[string]context.Context{
		"unauthenticated":         context.Background(),
		"collector principal":     v.as(t, "tail"),
		"every other operation":   v.bind(t, principal{subject: "other", ops: []auth.Operation{auth.OperationIngest, auth.OperationRead, auth.OperationWorkspaceSettingsWrite, auth.OperationAgentRegister, auth.OperationDelegate, auth.OperationDispatch, auth.OperationInvoke, auth.OperationAgentRevoke}}),
		"delegated admin":         v.bind(t, principal{subject: "admin", onBehalfOf: []shoal.ID{"someone"}}),
		"delegated for the admin": v.bind(t, principal{subject: "agent", onBehalfOf: []shoal.ID{"admin"}}),
	} {
		if _, e := v.registry.AdminProvision(ctx, grant("collector:new", "new", "authority:logs"), 1); !shoal.IsErrorCode(e, shoal.ErrorUnauthorized) {
			t.Fatalf("%s provision: %v", name, e)
		}
		if _, e := v.registry.AdminRevoke(ctx, collectorID, 1); !shoal.IsErrorCode(e, shoal.ErrorUnauthorized) {
			t.Fatalf("%s revoke: %v", name, e)
		}
		if _, e := v.registry.AdminRegistration(ctx, collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnauthorized) {
			t.Fatalf("%s read: %v", name, e)
		}
	}
	if v.wrote(t, "collector:new") {
		t.Fatal("a refused admin wrote a registration")
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Provisioned || len(v.changes(t, collectorID)) != 1 {
		t.Fatalf("a refused admin changed the registration: %+v", reg)
	}
	// Collector admin implies nothing on the collector routes.
	if _, e := v.registry.Enroll(v.bind(t, principal{subject: "tail", client: "client"}), []byte("k"), enrollRequest(collectorID, "authority:logs")); !shoal.IsErrorCode(e, shoal.ErrorUnauthorized) {
		t.Fatalf("admin enrolled as a collector: %v", e)
	}
}

// Another domain's collector is indistinguishable from a nonexistent one.
func TestAdminIsConfinedToItsDomain(t *testing.T) {
	v := newEnv(t)
	v.provision(t, collectorID, "tail", "authority:logs")
	other := v.bind(t, principal{subject: "admin", domain: "other-domain"})
	_, absentRevoke := v.registry.AdminRevoke(other, "collector:absent", 1)
	_, absentRead := v.registry.AdminRegistration(other, "collector:absent")
	_, revoke := v.registry.AdminRevoke(other, collectorID, 1)
	_, read := v.registry.AdminRegistration(other, collectorID)
	if !shoal.IsErrorCode(revoke, shoal.ErrorNotFound) || !shoal.IsErrorCode(read, shoal.ErrorNotFound) || revoke.Error() != absentRevoke.Error() || read.Error() != absentRead.Error() {
		t.Fatalf("cross-domain: revoke %v (absent %v), read %v (absent %v)", revoke, absentRevoke, read, absentRead)
	}
	// Provisioning into another domain is refused before anything is read.
	p := grant("collector:new", "new", "authority:logs")
	if _, e := v.registry.AdminProvision(other, p, 1); !errors.Is(e, api.ErrPermissionDenied) {
		t.Fatalf("provision into another domain: %v", e)
	}
	// Collector IDs are one namespace. An ID taken in another domain is a
	// conflict, answered exactly as an identity mismatch in the admin's own.
	p.Domain = []byte("other-domain")
	p.CollectorID = collectorID
	_, taken := v.registry.AdminProvision(other, p, 1)
	_, mismatch := v.registry.AdminProvision(v.admin(t), grant(collectorID, "someone-else", "authority:logs"), 1)
	if !shoal.IsErrorCode(taken, shoal.ErrorConflict) || taken.Error() != mismatch.Error() {
		t.Fatalf("taken %v, mismatch %v", taken, mismatch)
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Provisioned || len(v.changes(t, collectorID)) != 1 {
		t.Fatalf("cross-domain admin changed the registration: %+v", reg)
	}
}

func TestAdminProvisionIsIdempotentAndNeverMerges(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	p := grant(collectorID, "tail", "authority:logs")
	if _, e := v.registry.AdminProvision(admin, p, 1); e != nil {
		t.Fatal(e)
	}
	again, e := v.registry.AdminProvision(admin, p, 1)
	if e != nil || again.Changed || again.Sequence != 0 || again.Registration.Generation != 1 {
		t.Fatalf("retry %+v %v", again, e)
	}
	for name, change := range map[string]func(*Provisioning, *int64){
		"more authority":    func(p *Provisioning, _ *int64) { p.AuthorityPolicyIDs = append(p.AuthorityPolicyIDs, "authority:more") },
		"other authority":   func(p *Provisioning, _ *int64) { p.AuthorityPolicyIDs = []shoal.ID{"authority:other"} },
		"other control":     func(p *Provisioning, _ *int64) { p.Control = collector.RegistryControlled },
		"other mode":        func(p *Provisioning, _ *int64) { p.Mode = collector.Imported },
		"other subject":     func(p *Provisioning, _ *int64) { p.Subject = "someone" },
		"other client":      func(p *Provisioning, _ *int64) { p.ClientID = "other-client" },
		"future generation": func(_ *Provisioning, g *int64) { *g = 2 },
	} {
		q, generation := p, int64(1)
		q.AuthorityPolicyIDs = slices.Clone(p.AuthorityPolicyIDs)
		change(&q, &generation)
		if _, e := v.registry.AdminProvision(admin, q, generation); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
			t.Fatalf("%s: %v", name, e)
		}
	}
	reg, _ := v.registry.Registration(context.Background(), collectorID)
	if !slices.Equal(reg.AuthorityPolicyIDs, []shoal.ID{"authority:logs"}) || reg.Control != collector.ExternalControlled || reg.Mode != collector.ServerObserved || len(v.changes(t, collectorID)) != 1 {
		t.Fatalf("conflicting requests changed the registration: %+v", reg)
	}
	if _, e := v.registry.AdminProvision(admin, grant("collector:fresh", "fresh", "authority:logs"), 2); !shoal.IsErrorCode(e, shoal.ErrorConflict) || v.wrote(t, "collector:fresh") {
		t.Fatalf("create at generation 2: %v", e)
	}
}

func TestAdminRevokeQuarantinesAtReadTimeAndCannotBeUndoneByRetry(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	p := grant(collectorID, "tail", "authority:logs")
	if _, e := v.registry.AdminProvision(admin, p, 1); e != nil {
		t.Fatal(e)
	}
	tail := v.as(t, "tail")
	if _, e := v.registry.Enroll(tail, []byte("k"), enrollRequest(collectorID, "authority:logs")); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.SubmitArtifact(tail, collectorID, artifact(t, v, "artifact:1", "x")); e != nil {
		t.Fatal(e)
	}
	o, e := v.registry.SubmitObservation(tail, observation(t, v, "artifact:1", v1))
	if e != nil {
		t.Fatal(e)
	}
	if r, e := v.registry.ReadObservation(tail, o.Observation.ID()); e != nil || r.Quarantined {
		t.Fatalf("before revoke: %+v %v", r, e)
	}
	if _, e := v.registry.AdminRevoke(admin, collectorID, 2); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
		t.Fatalf("revoke a future generation: %v", e)
	}
	revoked, e := v.registry.AdminRevoke(admin, collectorID, 1)
	if e != nil || !revoked.Changed || revoked.Registration.State != collector.Revoked || revoked.Registration.Generation != 2 {
		t.Fatalf("revoke %+v %v", revoked, e)
	}
	if r, e := v.registry.ReadObservation(tail, o.Observation.ID()); e != nil || !r.Quarantined {
		t.Fatalf("after revoke: %+v %v", r, e)
	}
	if again, e := v.registry.AdminRevoke(admin, collectorID, 1); e != nil || again.Changed || again.Registration.Generation != 2 {
		t.Fatalf("revoke retry %+v %v", again, e)
	}
	for _, generation := range []int64{2, 3} {
		if _, e := v.registry.AdminRevoke(admin, collectorID, generation); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
			t.Fatalf("revoke generation %d of a registration revoked at 1: %v", generation, e)
		}
	}
	// A retried provisioning from before the revocation cannot undo it.
	if _, e := v.registry.AdminProvision(admin, p, 1); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
		t.Fatalf("stale provision retry: %v", e)
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
		t.Fatalf("revocation undone: %+v", reg)
	}
	// Reactivation at the current generation, with new terms; the old
	// generation stays quarantined, computed from the current registration.
	reactivated, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:metrics"), 2)
	if e != nil || !reactivated.Changed || reactivated.Registration.State != collector.Provisioned {
		t.Fatalf("reactivate %+v %v", reactivated, e)
	}
	if r, e := v.registry.ReadObservation(tail, o.Observation.ID()); e != nil || !r.Quarantined {
		t.Fatalf("after reactivation: %+v %v", r, e)
	}
	if _, e := v.registry.AdminRevoke(admin, collectorID, 1); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
		t.Fatalf("revoke an old generation of an active registration: %v", e)
	}
	got := v.changes(t, collectorID)
	if len(got) != 3 || got[0].Action != ChangeProvision || got[1].Action != ChangeRevoke || got[2].Action != ChangeProvision || got[1].Registration.Generation != 2 || got[2].Registration.Generation != 2 {
		t.Fatalf("changes %+v", got)
	}
	view, e := v.registry.AdminRegistration(admin, collectorID)
	if e != nil || view.ChangeCount != 3 || len(view.Changes) != 3 || view.Changes[0].Sequence != 3 || view.Changes[2].Sequence != 1 {
		t.Fatalf("view %+v %v", view, e)
	}
}

// Concurrent admins: the CAS serializes them, every change has exactly one
// entry, and the log has no gaps.
func TestConcurrentAdminChangesAreSerializedAndEachAudited(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	p := grant(collectorID, "tail", "authority:logs")
	var wg sync.WaitGroup
	var created, revoked atomic.Int32
	for range 8 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if r, e := v.registry.AdminProvision(admin, p, 1); e == nil && r.Changed {
				created.Add(1)
			}
		}()
		go func() {
			defer wg.Done()
			if r, e := v.registry.AdminRevoke(admin, collectorID, 1); e == nil && r.Changed {
				revoked.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 1 || revoked.Load() > 1 {
		t.Fatalf("created %d, revoked %d", created.Load(), revoked.Load())
	}
	got := v.changes(t, collectorID)
	if int32(len(got)) != created.Load()+revoked.Load() {
		t.Fatalf("%d changes for %d writes", len(got), created.Load()+revoked.Load())
	}
	for i, c := range got {
		if c.Sequence != int64(i+1) {
			t.Fatalf("gap at %d: %+v", i, c)
		}
	}
}

// lostAck applies a mutation and then reports that its outcome is unknown.
type lostAck struct {
	inner interface {
		ReadExact(context.Context, []allocator.Coordinate) ([]allocator.Cell, error)
		CompareAndMutate(context.Context, allocator.Mutation) (allocator.Status, error)
	}
	drop atomic.Bool
}

func (l *lostAck) ReadExact(ctx context.Context, c []allocator.Coordinate) ([]allocator.Cell, error) {
	return l.inner.ReadExact(ctx, c)
}
func (l *lostAck) CompareAndMutate(ctx context.Context, m allocator.Mutation) (allocator.Status, error) {
	status, e := l.inner.CompareAndMutate(ctx, m)
	if e == nil && l.drop.Load() {
		return allocator.StatusUnknown, allocator.ErrConditionalUnknown
	}
	return status, e
}

// A lost acknowledgement reconciles by reading the exact change entry back,
// so the write reports success once, with one entry.
func TestAdminWriteReconcilesALostAcknowledgement(t *testing.T) {
	v := newEnv(t)
	backend := &lostAck{inner: v.registry.config.Backend}
	config := v.registry.config
	config.Backend = backend
	r, e := New(config)
	if e != nil {
		t.Fatal(e)
	}
	backend.drop.Store(true)
	result, e := r.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 1)
	if e != nil || !result.Changed || result.Sequence != 1 {
		t.Fatalf("lost-ack provision %+v %v", result, e)
	}
	if got := v.changes(t, collectorID); len(got) != 1 {
		t.Fatalf("changes %+v", got)
	}
}

// The change log is read strictly: a head naming a missing or altered entry
// fails closed.
func TestCorruptChangeLogFailsClosed(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatal(e)
	}
	coord := v.registry.changeEntryCoordinate(collectorID, 1)
	cell, e := v.registry.read(context.Background(), coord)
	if e != nil {
		t.Fatal(e)
	}
	status, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Value: cell.Value, Timestamp: cell.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: cell.Timestamp + 1, Value: bytes.Replace(cell.Value, []byte(`"admin"`), []byte(`"other"`), 1)}}})
	if e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
	if _, e := v.registry.AdminRegistration(admin, collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("altered entry: %v", e)
	}
	if _, e := v.registry.Changes(context.Background(), collectorID, 0, 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("altered entry: %v", e)
	}
	// A provisioning cannot be appended over a corrupt head; it fails closed.
	// (Revoke first so that the provisioning below is a real change.)
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	corruptHead(t, v, collectorID)
	if _, e := v.registry.AdminProvision(admin, grant("collector:other", "other", "authority:logs"), 1); e != nil {
		t.Fatal(e) // another collector's log is independent
	}
	if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("provision over a corrupt head: %v", e)
	}
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 2); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("admin provision over a corrupt head: %v", e)
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
		t.Fatalf("a change was written without its entry: %+v", reg)
	}
}

// corruptHead overwrites id's change-log head with bytes that do not decode
// and returns the replaced cell.
func corruptHead(t *testing.T, v *env, id shoal.ID) allocator.Cell {
	t.Helper()
	head := v.registry.changeHeadCoordinate(id)
	cell, e := v.registry.read(context.Background(), head)
	if e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: head.Row, Conditions: []allocator.Condition{{Coordinate: head, Value: cell.Value, Timestamp: cell.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: head, Timestamp: cell.Timestamp + 1, Value: []byte("garbage")}}}); e != nil {
		t.Fatal(e)
	}
	stored, _ := v.registry.read(context.Background(), head)
	return stored
}

// Revocation is the emergency control: a damaged log never blocks it. It
// starts a new, recorded segment instead, atomically with the revocation.
func TestRevocationRecoversADamagedChangeLog(t *testing.T) {
	for _, viaAdmin := range []bool{true, false} {
		v := newEnv(t)
		admin := v.admin(t)
		if _, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 1); e != nil {
			t.Fatal(e)
		}
		damaged := corruptHead(t, v, collectorID)
		if _, e := v.registry.AdminRegistration(admin, collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
			t.Fatalf("read over a corrupt head: %v", e)
		}
		var e error
		if viaAdmin {
			var result AdminResult
			result, e = v.registry.AdminRevoke(admin, collectorID, 1)
			if e == nil && (!result.Changed || result.Sequence != 2) {
				t.Fatalf("recovery result %+v", result)
			}
		} else {
			_, e = v.registry.Revoke(context.Background(), collectorID)
		}
		if e != nil {
			t.Fatalf("revoke over a corrupt head (admin %v): %v", viaAdmin, e)
		}
		if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked || reg.Generation != 2 {
			t.Fatalf("not revoked: %+v", reg)
		}
		view, e := v.registry.AdminRegistration(admin, collectorID)
		if e != nil {
			t.Fatalf("read after recovery: %v", e)
		}
		if len(view.Changes) != 1 || view.SegmentStart != 2 || view.ChangeCount != 2 {
			t.Fatalf("view %+v", view)
		}
		c := view.Changes[0]
		if c.Recovery == nil || c.Recovery.Reason != RecoveryUnreadableHead || c.Recovery.HeadTimestamp != damaged.Timestamp || c.Recovery.HeadSHA256 != digestBytes(damaged.Value) || c.Action != ChangeRevoke {
			t.Fatalf("recovery entry %+v %+v", c, c.Recovery)
		}
		// The wire projection carries the segment.
		provider, _ := NewAdminProvider(v.registry)
		record, e := provider.ReadRegistration(admin, collectorID)
		if e != nil || record.SegmentStart != view.SegmentStart || len(record.Changes) != 1 || record.Changes[0].Recovery != api.RecoveryUnreadableHead {
			t.Fatalf("wire record %+v %v", record, e)
		}
		// The new segment reads through Changes, and the log appends
		// normally again.
		if got, e := v.registry.Changes(context.Background(), collectorID, view.SegmentStart-1, MaxChangesPage); e != nil || len(got) != 1 {
			t.Fatalf("segment %+v %v", got, e)
		}
		if r, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 2); e != nil || r.Sequence != view.SegmentStart+1 {
			t.Fatalf("append after recovery %+v %v", r, e)
		}
	}
}

// An entry already present where the head says the next one goes is also
// damage a revocation recovers from, and a provisioning refuses.
func TestRevocationRecoversAnOccupiedSequence(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatal(e)
	}
	for _, sequence := range []int64{2, 3} {
		coord := v.registry.changeEntryCoordinate(collectorID, sequence)
		if _, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Absent: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: 1, Value: []byte("stray")}}}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs", "authority:more")); e == nil {
		t.Fatal("a conflicting provision succeeded")
	}
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatalf("revoke over an occupied sequence: %v", e)
	}
	if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); e != nil {
		t.Fatalf("provision after recovery: %v", e)
	}
	view, e := v.registry.AdminRegistration(admin, collectorID)
	if e != nil || view.SegmentStart != 4 || len(view.Changes) != 2 || view.Changes[1].Recovery == nil || view.Changes[1].Recovery.Reason != RecoveryOccupiedSequence || view.Changes[1].Recovery.HeadTimestamp <= 0 {
		t.Fatalf("view %+v %v", view, e)
	}
}

// A provisioning over an occupied sequence fails closed and writes nothing.
func TestProvisionOverAnOccupiedSequenceFailsClosed(t *testing.T) {
	v := newEnv(t)
	coord := v.registry.changeEntryCoordinate(collectorID, 1)
	if _, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Absent: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: 1, Value: []byte("stray")}}}); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("provision: %v", e)
	}
	if _, e := v.registry.Registration(context.Background(), collectorID); !shoal.IsErrorCode(e, shoal.ErrorNotFound) {
		t.Fatalf("provision wrote: %v", e)
	}
}

// An admin identity that cannot be recorded faithfully is refused before
// anything is written, so no entry exists that its own read API would
// refuse.
func TestUnrecordableAdminIdentitiesAreRefused(t *testing.T) {
	for name, p := range map[string]principal{
		"actor not UTF-8":           {subject: "admin", actor: "adm\xff"},
		"actor all 0xff":            {subject: "admin", actor: string(bytes.Repeat([]byte{0xff}, 1000))},
		"subject not UTF-8":         {subject: "adm\xffin"},
		"client not UTF-8":          {subject: "admin", client: "cli\xff"},
		"subject control character": {subject: "adm\x01in"},
	} {
		v := newEnv(t)
		if _, e := v.registry.AdminProvision(v.bind(t, p), grant(collectorID, "tail", "authority:logs"), 1); !shoal.IsErrorCode(e, shoal.ErrorUnauthorized) {
			t.Fatalf("%s: %v", name, e)
		}
		if v.wrote(t, collectorID) {
			t.Fatalf("%s: wrote", name)
		}
	}
	for name, d := range map[string]auth.DecisionConfig{
		"request ID not UTF-8":     {RequestID: "req\xff"},
		"correlation ID not UTF-8": {RequestID: "request", CorrelationID: "corr\xff"},
		"correlation ID space":     {RequestID: "request", CorrelationID: "corr elation"},
	} {
		v := newEnv(t)
		d.Subject, d.Actor, d.ClientID, d.AuthorizationDomain, d.AllowedOperations, d.PolicyGeneration, d.AuthenticationExpires = "admin", "admin", "client", []byte("domain"), []auth.Operation{auth.OperationCollectorAdmin}, 1, v.clock.Now().Add(time.Hour)
		decision, e := auth.NewDecision(d)
		if e != nil {
			t.Fatalf("%s: %v", name, e)
		}
		ctx, _ := v.authority.Binder().Bind(context.Background(), decision)
		if _, e := v.registry.AdminProvision(ctx, grant(collectorID, "tail", "authority:logs"), 1); !shoal.IsErrorCode(e, shoal.ErrorUnauthorized) {
			t.Fatalf("%s: %v", name, e)
		}
		if v.wrote(t, collectorID) {
			t.Fatalf("%s: wrote", name)
		}
	}
	// An OIDC-style subject with spaces is a valid admin.
	v := newEnv(t)
	if _, e := v.registry.AdminProvision(v.bind(t, principal{subject: "oidc:https://issuer#sub# admin 7 "}), grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatalf("spaced subject: %v", e)
	}
	if _, e := v.registry.AdminRegistration(v.bind(t, principal{subject: "oidc:https://issuer#sub# admin 7 "}), collectorID); e != nil {
		t.Fatalf("spaced subject read: %v", e)
	}
}

// prepareChange refuses any entry that would not read back as itself, even
// one validChange accepts: here a timestamp with a monotonic reading and a
// non-UTC location, which encoding/json does not preserve.
func TestChangesThatWouldNotReadBackAreNeverWritten(t *testing.T) {
	v := newEnv(t)
	at := time.Now().In(time.FixedZone("x", 3600))
	_, _, e := v.registry.provision(context.Background(), grant(collectorID, "tail", "authority:logs"), 0, attribution{source: SourceOperatorAPI, at: at})
	if !shoal.IsErrorCode(e, shoal.ErrorInvalidArgument) {
		t.Fatalf("non-round-tripping entry: %v", e)
	}
	if v.wrote(t, collectorID) {
		t.Fatal("wrote an entry that cannot be read back")
	}
	// Below the admin check, validChange refuses an attribution that
	// round-trips through JSON but is not printable.
	bad := &AdminAttribution{Subject: "admin", Actor: "a\x01", RequestID: "request", Domain: []byte("domain"), AuthorizationFingerprint: "auth-sha256:00"}
	_, _, e = v.registry.provision(context.Background(), grant(collectorID, "tail", "authority:logs"), 1, attribution{source: SourceCollectorAdmin, admin: bad, at: v.clock.Now()})
	if !shoal.IsErrorCode(e, shoal.ErrorInvalidArgument) || v.wrote(t, collectorID) {
		t.Fatalf("unprintable attribution: %v", e)
	}
}

// interleave runs hook once, right after the first armed read of the
// registration cell returns, so a change lands between that read and the
// head read that follows it.
type interleave struct {
	lostAck
	armed atomic.Bool
	// skip is how many armed registration reads pass before the hook runs.
	skip atomic.Int32
	hook func()
}

func (i *interleave) ReadExact(ctx context.Context, c []allocator.Coordinate) ([]allocator.Cell, error) {
	cells, e := i.inner.ReadExact(ctx, c)
	if len(c) == 1 && string(c[0].Qualifier) == "registration" && i.armed.Load() && i.skip.Add(-1) < 0 && i.armed.CompareAndSwap(true, false) {
		i.hook()
	}
	return cells, e
}

// The admin read is coherent: a revocation landing between its registration
// read and its head read is not returned as a registration that disagrees
// with the newest change.
func TestAdminRegistrationIsCoherent(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatal(e)
	}
	backend := &interleave{lostAck: lostAck{inner: v.registry.config.Backend}}
	backend.hook = func() {
		if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
			t.Error(e)
		}
	}
	config := v.registry.config
	config.Backend = backend
	r, e := New(config)
	if e != nil {
		t.Fatal(e)
	}
	backend.skip.Store(1) // past the domain check, into the coherent loop
	backend.armed.Store(true)
	view, e := r.AdminRegistration(admin, collectorID)
	if e != nil {
		t.Fatalf("read across a concurrent revoke: %v", e)
	}
	if backend.armed.Load() {
		t.Fatal("the revocation did not interleave; the test is vacuous")
	}
	if view.Registration.State != collector.Revoked || view.Changes[0].Action != ChangeRevoke || view.ChangeCount != 2 {
		t.Fatalf("torn view: %+v", view)
	}
}

// Defense in depth on the read: an entry that claims a recovery on a
// provisioning, or a registration that disagrees with the newest entry
// (written outside the logged paths), fails closed.
func TestLogInconsistenciesFailClosed(t *testing.T) {
	v := newEnv(t)
	admin := v.admin(t)
	if _, e := v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatal(e)
	}
	// A registration change that bypassed the log.
	if _, e := v.registry.updateRegistration(context.Background(), collectorID, false, func(row *registrationRow, _ bool) (bool, error) {
		row.Registration.Generation++
		return true, nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.AdminRegistration(admin, collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("registration ahead of its log: %v", e)
	}

	w := newEnv(t)
	if _, e := w.registry.AdminProvision(w.admin(t), grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatal(e)
	}
	entries := w.changes(t, collectorID)
	forged := entries[0]
	forged.Sequence = 2
	forged.Recovery = &ChangeRecovery{Reason: RecoveryUnreadableHead, HeadTimestamp: 1, HeadSHA256: digestBytes(nil)}
	value, e := encode(forged)
	if e != nil {
		t.Fatal(e)
	}
	head, _ := encode(changeHead{CollectorID: collectorID, Sequence: 2})
	headCoord, entry := w.registry.changeHeadCoordinate(collectorID), w.registry.changeEntryCoordinate(collectorID, 2)
	headCell, _ := w.registry.read(context.Background(), headCoord)
	if status, e := w.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: headCoord.Row, Conditions: []allocator.Condition{{Coordinate: headCoord, Value: headCell.Value, Timestamp: headCell.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: headCoord, Timestamp: headCell.Timestamp + 1, Value: head}, {Coordinate: entry, Timestamp: 1, Value: value}}}); e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
	if _, e := w.registry.Changes(context.Background(), collectorID, 1, 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("recovery on a provisioning: %v", e)
	}
}

// put writes value at coord and timestamp unconditionally (test damage).
func put(t *testing.T, v *env, coord allocator.Coordinate, timestamp int64, value []byte) {
	t.Helper()
	cells, e := v.registry.config.Backend.ReadExact(context.Background(), []allocator.Coordinate{coord})
	if e != nil {
		t.Fatal(e)
	}
	condition := allocator.Condition{Coordinate: coord, Absent: true}
	if len(cells) > 0 {
		condition = allocator.Condition{Coordinate: coord, Value: cells[0].Value, Timestamp: cells[0].Timestamp, TimestampSet: true}
	}
	if status, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{condition}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: timestamp, Value: value}}}); e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
}

// deleteHead removes the change-log head.
func deleteHead(t *testing.T, v *env, id shoal.ID) {
	t.Helper()
	coord := v.registry.changeHeadCoordinate(id)
	cell, e := v.registry.read(context.Background(), coord)
	if e != nil {
		t.Fatal(e)
	}
	if status, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Value: cell.Value, Timestamp: cell.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: cell.Timestamp, Delete: true}}}); e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
}

// revokeRecovers revokes id through the Go API, then checks the recovery
// entry is the current segment's start at want, and that provisioning
// appends normally afterwards.
func revokeRecovers(t *testing.T, v *env, r *Registry, want int64, reason RecoveryReason) {
	t.Helper()
	if _, e := r.Revoke(context.Background(), collectorID); e != nil {
		t.Fatalf("revoke: %v", e)
	}
	if reg, _ := r.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
		t.Fatalf("not revoked: %+v", reg)
	}
	view, e := r.AdminRegistration(v.admin(t), collectorID)
	if e != nil {
		t.Fatalf("read after recovery: %v", e)
	}
	if view.SegmentStart != want || len(view.Changes) != 1 || view.Changes[0].Recovery == nil || view.Changes[0].Recovery.Reason != reason {
		t.Fatalf("view %+v", view)
	}
	if got, e := r.Changes(context.Background(), collectorID, 0, MaxChangesPage); e != nil || len(got) != 1 || got[0].Sequence != want {
		t.Fatalf("Changes reads beyond the current segment: %d %v", len(got), e)
	}
	reg, _ := r.Registration(context.Background(), collectorID)
	result, e := r.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), reg.Generation)
	if e != nil || result.Sequence != want+1 {
		t.Fatalf("append after recovery: %+v %v", result, e)
	}
}

func provisionedEnv(t *testing.T) *env {
	t.Helper()
	v := newEnv(t)
	if _, e := v.registry.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 1); e != nil {
		t.Fatal(e)
	}
	return v
}

// Every reachable shape of log damage leaves revocation working, and the
// log appending normally afterwards (#656 review round 3).
func TestRevocationRecoversEveryLogDamage(t *testing.T) {
	t.Run("deleted head over a long log", func(t *testing.T) {
		v := provisionedEnv(t)
		for range 35 {
			if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
				t.Fatal(e)
			}
			if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); e != nil {
				t.Fatal(e)
			}
		}
		deleteHead(t, v, collectorID) // 71 entries remain
		if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
			t.Fatalf("an absent head over entries read as an empty log: %v", e)
		}
		if _, e := v.registry.Changes(context.Background(), collectorID, 0, 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
			t.Fatalf("Changes over a deleted head: %v", e)
		}
		revokeRecovers(t, v, v.registry, 72, RecoveryOccupiedSequence)
	})
	t.Run("strays with a gap", func(t *testing.T) {
		v := provisionedEnv(t)
		for _, sequence := range []int64{2, 4} {
			put(t, v, v.registry.changeEntryCoordinate(collectorID, sequence), 1, []byte("stray"))
		}
		revokeRecovers(t, v, v.registry, 5, RecoveryOccupiedSequence)
	})
	t.Run("malformed stray entry", func(t *testing.T) {
		v := provisionedEnv(t)
		put(t, v, v.registry.changeEntryCoordinate(collectorID, 2), 0, []byte("stray"))
		revokeRecovers(t, v, v.registry, 3, RecoveryOccupiedSequence)
	})
	t.Run("garbage head at timestamp 0 with no log", func(t *testing.T) {
		v := newEnv(t)
		if _, e := v.registry.updateRegistration(context.Background(), collectorID, true, func(row *registrationRow, _ bool) (bool, error) {
			*row = registrationRow{Registration: collector.Registration{CollectorID: collectorID, Subject: "tail", ClientID: "client", Domain: []byte("domain"), AuthorityPolicyIDs: []shoal.ID{"authority:logs"}, Control: collector.ExternalControlled, Mode: collector.ServerObserved, Generation: 1, State: collector.Provisioned}}
			return true, nil
		}); e != nil {
			t.Fatal(e)
		}
		put(t, v, v.registry.changeHeadCoordinate(collectorID), 0, []byte("garbage"))
		revokeRecovers(t, v, v.registry, 1, RecoveryUnreadableHead)
	})
	t.Run("garbage head at a huge timestamp", func(t *testing.T) {
		v := provisionedEnv(t)
		put(t, v, v.registry.changeHeadCoordinate(collectorID), 1<<62+1, []byte("garbage"))
		revokeRecovers(t, v, v.registry, 2, RecoveryUnreadableHead)
	})
	t.Run("no store without a row scan", func(t *testing.T) {
		v := provisionedEnv(t)
		for _, sequence := range []int64{2, 3, 4} {
			put(t, v, v.registry.changeEntryCoordinate(collectorID, sequence), 1, []byte("stray"))
		}
		config := v.registry.config
		config.Backend = &lostAck{inner: v.registry.config.Backend} // no ScanRowPrefix
		r, e := New(config)
		if e != nil {
			t.Fatal(e)
		}
		revokeRecovers(t, v, r, 5, RecoveryOccupiedSequence)
	})
	t.Run("head at the largest timestamp", func(t *testing.T) {
		v := provisionedEnv(t)
		put(t, v, v.registry.changeHeadCoordinate(collectorID), math.MaxInt64, []byte("garbage"))
		if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
			t.Fatalf("revoke: %v", e)
		}
		if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
			t.Fatalf("not revoked: %+v", reg)
		}
		// The entry is recorded; no head can supersede the damaged one, so
		// the log reads as unavailable rather than as healthy.
		cell, e := v.registry.read(context.Background(), v.registry.changeEntryCoordinate(collectorID, 2))
		var c Change
		if e != nil || decode(cell.Value, &c) != nil || c.Recovery == nil || c.Recovery.HeadTimestamp != math.MaxInt64 || c.Action != ChangeRevoke {
			t.Fatalf("recovery entry %+v %v", c, e)
		}
		if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
			t.Fatalf("read: %v", e)
		}
	})
	t.Run("no representable sequence", func(t *testing.T) {
		v := provisionedEnv(t)
		put(t, v, v.registry.changeEntryCoordinate(collectorID, math.MaxInt64-1), 1, []byte("stray"))
		corruptHead(t, v, collectorID) // a far stray is damage only once the log is
		if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
			t.Fatalf("revoke: %v", e)
		}
		if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
			t.Fatalf("not revoked: %+v", reg)
		}
		scanner := v.registry.config.Backend.(rowScanner)
		cells, e := scanner.ScanRowPrefix(context.Background(), v.registry.registrationCoordinate(collectorID).Row, []byte(changeFamily), []byte(overflowPrefix), nil, 10)
		var overflow []allocator.Cell
		for _, cell := range cells {
			if strings.HasPrefix(string(cell.Coordinate.Qualifier), overflowPrefix) {
				overflow = append(overflow, cell)
			}
		}
		if e != nil || len(overflow) != 1 {
			t.Fatalf("overflow entries: %d %v", len(overflow), e)
		}
		var c Change
		if decode(overflow[0].Value, &c) != nil || c.Recovery == nil || c.Action != ChangeRevoke || validChange(c, collectorID) != nil {
			t.Fatalf("overflow entry %+v", c)
		}
	})
}

// deleteCell removes the newest cell at coord.
func deleteCell(t *testing.T, v *env, coord allocator.Coordinate) {
	t.Helper()
	cell, e := v.registry.read(context.Background(), coord)
	if e != nil {
		t.Fatal(e)
	}
	if status, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Value: cell.Value, Timestamp: cell.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: cell.Timestamp, Delete: true}}}); e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
}

// With the head and the first entry gone, the log looks empty at sequence 1
// but later entries exist. Appending there would shadow them: a provisioning
// fails closed and a revocation recovers above them.
func TestAbsentHeadOverLaterEntriesIsDamage(t *testing.T) {
	v := provisionedEnv(t)
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	deleteCell(t, v, v.registry.changeHeadCoordinate(collectorID))
	deleteCell(t, v, v.registry.changeEntryCoordinate(collectorID, 1))
	if _, e := v.registry.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 3); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("provision over hidden entries: %v", e)
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Revoked {
		t.Fatalf("provisioned over hidden entries: %+v", reg)
	}

	w := provisionedEnv(t)
	if _, e := w.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	if _, e := w.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); e != nil {
		t.Fatal(e)
	}
	deleteCell(t, w, w.registry.changeHeadCoordinate(collectorID))
	deleteCell(t, w, w.registry.changeEntryCoordinate(collectorID, 1))
	revokeRecovers(t, w, w.registry, 4, RecoveryOccupiedSequence)
}

// A head naming a segment start whose entry is not a recovery entry is not a
// log this registry wrote.
func TestSegmentStartMustBeARecoveryEntry(t *testing.T) {
	v := provisionedEnv(t)
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	entries := v.changes(t, collectorID)
	forged := entries[1]
	forged.Sequence = 3
	value, e := encode(forged)
	if e != nil {
		t.Fatal(e)
	}
	head, _ := encode(changeHead{CollectorID: collectorID, Sequence: 3, SegmentStart: 3})
	put(t, v, v.registry.changeEntryCoordinate(collectorID, 3), 1, value)
	headCell, _ := v.registry.read(context.Background(), v.registry.changeHeadCoordinate(collectorID))
	put(t, v, v.registry.changeHeadCoordinate(collectorID), headCell.Timestamp+1, head)
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("segment start without recovery: %v", e)
	}
}

// tombstone leaves a delete marker at coord and timestamp, whether or not a
// cell exists there, conditioned on the collector's registration cell.
func tombstone(t *testing.T, v *env, coord allocator.Coordinate, timestamp int64) {
	t.Helper()
	reg, e := v.registry.read(context.Background(), v.registry.registrationCoordinate(collectorID))
	if e != nil {
		t.Fatal(e)
	}
	if status, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: reg.Coordinate, Value: reg.Value, Timestamp: reg.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: timestamp, Delete: true}}}); e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
}

// Delete markers hide every write at or below their timestamp from reads,
// scans and conditions alike. Audited writes check that what they wrote reads
// back, and write it again higher until it does, so an accepted change is
// never reported with an invisible entry.
func TestDeleteMarkersNeverHideAnAuditedChange(t *testing.T) {
	t.Run("head and entry deleted at their own timestamps", func(t *testing.T) {
		v := provisionedEnv(t)
		deleteCell(t, v, v.registry.changeHeadCoordinate(collectorID))
		deleteCell(t, v, v.registry.changeEntryCoordinate(collectorID, 1))
		result, e := v.registry.AdminRevoke(v.admin(t), collectorID, 1)
		if e != nil || !result.Changed {
			t.Fatalf("revoke %+v %v", result, e)
		}
		view, e := v.registry.AdminRegistration(v.admin(t), collectorID)
		if e != nil || len(view.Changes) != 1 || view.Changes[0].Sequence != result.Sequence || view.Changes[0].Action != ChangeRevoke {
			t.Fatalf("view %+v %v", view, e)
		}
		// A marker just at the clock costs a small jump, not a leap towards
		// the largest timestamp.
		head, e := v.registry.read(context.Background(), v.registry.changeHeadCoordinate(collectorID))
		if e != nil || head.Timestamp > v.registry.nanos()+1<<20 {
			t.Fatalf("head repaired at %d (clock %d): %v", head.Timestamp, v.registry.nanos(), e)
		}
	})
	t.Run("markers far above the clock", func(t *testing.T) {
		v := provisionedEnv(t)
		tombstone(t, v, v.registry.changeEntryCoordinate(collectorID, 2), math.MaxInt64/2)
		tombstone(t, v, v.registry.changeHeadCoordinate(collectorID), math.MaxInt64/2)
		result, e := v.registry.AdminRevoke(v.admin(t), collectorID, 1)
		if e != nil || !result.Changed {
			t.Fatalf("revoke %+v %v", result, e)
		}
		view, e := v.registry.AdminRegistration(v.admin(t), collectorID)
		if e != nil || len(view.Changes) != 1 || view.Changes[0].Sequence != result.Sequence || view.Changes[0].Action != ChangeRevoke {
			t.Fatalf("view %+v %v", view, e)
		}
	})
	t.Run("marker at the largest timestamp", func(t *testing.T) {
		v := provisionedEnv(t)
		tombstone(t, v, v.registry.changeEntryCoordinate(collectorID, 2), math.MaxInt64)
		_, e := v.registry.AdminRevoke(v.admin(t), collectorID, 1)
		if !errors.Is(e, api.ErrIndeterminate) {
			t.Fatalf("an unrepairable invisible entry was not reported: %v", e)
		}
		if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
			t.Fatalf("read: %v", e)
		}
	})
}

// shortPages is a store whose row scan returns at most two cells per call,
// as a store that applies the limit before filtering may.
type shortPages struct {
	lostAck
	scanner rowScanner
}

func (s *shortPages) ScanRowPrefix(ctx context.Context, row, family, start, visibility []byte, limit int) ([]allocator.Cell, error) {
	cells, e := s.scanner.ScanRowPrefix(ctx, row, family, start, visibility, limit)
	return cells[:min(len(cells), 2)], e
}

// A short page does not end the scan: recovery still lands above every
// stored entry.
func TestRecoveryScansPastShortPages(t *testing.T) {
	v := provisionedEnv(t)
	for _, sequence := range []int64{2, 3, 4, 5, 6} {
		put(t, v, v.registry.changeEntryCoordinate(collectorID, sequence), 1, []byte("stray"))
	}
	inner := v.registry.config.Backend
	config := v.registry.config
	config.Backend = &shortPages{lostAck: lostAck{inner: inner}, scanner: inner.(rowScanner)}
	r, e := New(config)
	if e != nil {
		t.Fatal(e)
	}
	revokeRecovers(t, v, r, 7, RecoveryOccupiedSequence)
}

// A healthy head at the largest timestamp can never be superseded, so the
// log cannot be appended to: provisioning fails closed, and revocation is
// recorded with an exhausted_head recovery entry while the head stays.
func TestExhaustedHeadIsDamage(t *testing.T) {
	v := provisionedEnv(t)
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	head, _ := encode(changeHead{CollectorID: collectorID, Sequence: 2})
	put(t, v, v.registry.changeHeadCoordinate(collectorID), math.MaxInt64, head)
	// The log is still complete, so it reads; it just cannot grow.
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); e != nil {
		t.Fatalf("read over an exhausted head: %v", e)
	}
	if _, e := v.registry.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 2); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("provision over an exhausted head: %v", e)
	}
	// Reactivate out of band so there is something to revoke.
	if _, e := v.registry.updateRegistration(context.Background(), collectorID, false, func(row *registrationRow, _ bool) (bool, error) {
		row.Registration.State = collector.Provisioned
		return true, nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatalf("revoke over an exhausted head: %v", e)
	}
	cell, e := v.registry.read(context.Background(), v.registry.changeEntryCoordinate(collectorID, 3))
	var c Change
	if e != nil || decode(cell.Value, &c) != nil || c.Recovery == nil || c.Recovery.Reason != RecoveryExhaustedHead {
		t.Fatalf("recovery entry %+v %v", c, e)
	}
	// Neither read presents the log without that entry.
	if _, e := v.registry.Changes(context.Background(), collectorID, 0, MaxChangesPage); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("Changes over an exhausted head: %v", e)
	}
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("read over an exhausted head with a change beyond it: %v", e)
	}
}

// visible compares exact bytes, not mere presence.
func TestVisibleComparesBytes(t *testing.T) {
	v := provisionedEnv(t)
	coord := v.registry.changeEntryCoordinate(collectorID, 1)
	cell, e := v.registry.read(context.Background(), coord)
	if e != nil {
		t.Fatal(e)
	}
	if ok, e := v.registry.visible(context.Background(), coord, cell.Value); e != nil || !ok {
		t.Fatalf("own bytes: %v %v", ok, e)
	}
	if ok, e := v.registry.visible(context.Background(), coord, append(bytes.Clone(cell.Value), ' ')); e != nil || ok {
		t.Fatalf("other bytes: %v %v", ok, e)
	}
}

// onTop runs hook once, right after the first accepted mutation that writes
// the change log, so another audited write commits on top of it before its
// read-back.
type onTop struct {
	lostAck
	armed atomic.Bool
	hook  func()
}

func (o *onTop) CompareAndMutate(ctx context.Context, m allocator.Mutation) (allocator.Status, error) {
	status, e := o.inner.CompareAndMutate(ctx, m)
	logs := false
	for _, u := range m.Updates {
		logs = logs || string(u.Coordinate.Family) == changeFamily
	}
	if e == nil && status == allocator.StatusAccepted && logs && o.armed.CompareAndSwap(true, false) {
		o.hook()
	}
	return status, e
}

// A change that is superseded by a later audited write before it reads back
// is still a clean change.
func TestAChangeSupersededBeforeReadBackIsClean(t *testing.T) {
	v := newEnv(t)
	backend := &onTop{lostAck: lostAck{inner: v.registry.config.Backend}}
	backend.hook = func() {
		if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
			t.Error(e)
		}
	}
	config := v.registry.config
	config.Backend = backend
	r, e := New(config)
	if e != nil {
		t.Fatal(e)
	}
	backend.armed.Store(true)
	result, e := r.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 1)
	if e != nil || !result.Changed || result.Sequence != 1 {
		t.Fatalf("superseded provision %+v %v", result, e)
	}
	if got := v.changes(t, collectorID); len(got) != 2 || got[1].Action != ChangeRevoke {
		t.Fatalf("changes %+v", got)
	}
}

// A marker far above the clock costs a bounded jump, never the largest
// timestamp: the log keeps appending afterwards.
func TestRepairsNeverExhaustTheHead(t *testing.T) {
	v := provisionedEnv(t)
	tombstone(t, v, v.registry.changeHeadCoordinate(collectorID), 7_500_000_000_000_000_000)
	if _, e := v.registry.AdminRevoke(v.admin(t), collectorID, 1); e != nil {
		t.Fatalf("revoke: %v", e)
	}
	head, e := v.registry.read(context.Background(), v.registry.changeHeadCoordinate(collectorID))
	if e != nil || head.Timestamp >= maxWriteTimestamp {
		t.Fatalf("head at %d: %v", head.Timestamp, e)
	}
	for range 3 {
		reg, _ := v.registry.Registration(context.Background(), collectorID)
		if reg.State == collector.Revoked {
			if _, e := v.registry.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), reg.Generation); e != nil {
				t.Fatalf("provision after repair: %v", e)
			}
		} else if _, e := v.registry.AdminRevoke(v.admin(t), collectorID, reg.Generation); e != nil {
			t.Fatalf("revoke after repair: %v", e)
		}
	}
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); e != nil {
		t.Fatalf("read after repairs: %v", e)
	}
}

// An entry right after the head's sequence is a change the head does not
// describe: both reads fail closed rather than present the log without it.
func TestEntriesBeyondTheHeadFailClosed(t *testing.T) {
	v := provisionedEnv(t)
	put(t, v, v.registry.changeEntryCoordinate(collectorID, 2), 1, []byte("stray"))
	if _, e := v.registry.Changes(context.Background(), collectorID, 0, MaxChangesPage); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("Changes: %v", e)
	}
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("read: %v", e)
	}
}

// afterHeadRead runs hook once, right after the first armed read of the
// change-log head, so a concurrent audited write commits between a reader's
// head read and its next probe.
type afterHeadRead struct {
	lostAck
	armed atomic.Bool
	hook  func()
}

func (a *afterHeadRead) ReadExact(ctx context.Context, c []allocator.Coordinate) ([]allocator.Cell, error) {
	cells, e := a.inner.ReadExact(ctx, c)
	if len(c) == 1 && string(c[0].Qualifier) == "change-head" && a.armed.CompareAndSwap(true, false) {
		a.hook()
	}
	return cells, e
}

// A write that commits between a reader's head read and its probe of the
// next entry is concurrency, not damage: reads and provisioning retry.
func TestConcurrentWritesAreNotDamage(t *testing.T) {
	for _, step := range []string{"changes", "read", "provision"} {
		v := provisionedEnv(t)
		backend := &afterHeadRead{lostAck: lostAck{inner: v.registry.config.Backend}}
		backend.hook = func() {
			if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
				t.Error(e)
			}
		}
		if step == "provision" {
			// Start revoked; the concurrent write is the operator
			// reactivating the same terms.
			if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
				t.Fatal(e)
			}
			backend.hook = func() {
				if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); e != nil {
					t.Error(e)
				}
			}
		}
		config := v.registry.config
		config.Backend = backend
		r, e := New(config)
		if e != nil {
			t.Fatal(e)
		}
		backend.armed.Store(true)
		switch step {
		case "changes":
			if got, e := r.Changes(context.Background(), collectorID, 0, MaxChangesPage); e != nil || len(got) != 2 {
				t.Fatalf("changes %d %v", len(got), e)
			}
		case "read":
			if view, e := r.AdminRegistration(v.admin(t), collectorID); e != nil || view.Registration.State != collector.Revoked {
				t.Fatalf("read %+v %v", view, e)
			}
		case "provision":
			// The operator reactivates while this provisioning decides; it
			// is not refused as damage, but re-evaluated: the same terms
			// are now in place, a no-op.
			result, e := r.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 2)
			if e != nil || result.Changed {
				t.Fatalf("provision racing the operator: %+v %v", result, e)
			}
		}
		if backend.armed.Load() {
			t.Fatalf("%s: the write did not interleave", step)
		}
	}
}

// A change superseded by a recovery whose segment starts past it is not
// readable, so it is not reported clean.
func TestAChangeLeftOutsideTheSegmentIsNotClean(t *testing.T) {
	v := newEnv(t)
	backend := &onTop{lostAck: lostAck{inner: v.registry.config.Backend}}
	backend.hook = func() {
		tombstone(t, v, v.registry.changeHeadCoordinate(collectorID), v.registry.nanos()+1000)
		if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
			t.Error(e)
		}
	}
	config := v.registry.config
	config.Backend = backend
	r, e := New(config)
	if e != nil {
		t.Fatal(e)
	}
	backend.armed.Store(true)
	if _, e := r.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 1); !errors.Is(e, api.ErrIndeterminate) {
		t.Fatalf("provision left outside the segment: %v", e)
	}
}

// Ordinary churn — provisions, revocations and reads racing — never reads as
// damage: no unavailable and no indeterminate outcome.
func TestChurnIsNeverReportedAsDamage(t *testing.T) {
	v := provisionedEnv(t)
	admin := v.admin(t)
	var wg sync.WaitGroup
	var bad atomic.Int32
	report := func(e error) {
		if e != nil && !shoal.IsErrorCode(e, shoal.ErrorConflict) {
			bad.Add(1)
			t.Log(e)
		}
	}
	for worker := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 15 {
				switch worker % 3 {
				case 0:
					reg, e := v.registry.Registration(context.Background(), collectorID)
					report(e)
					if reg.State == collector.Revoked {
						_, e = v.registry.AdminProvision(admin, grant(collectorID, "tail", "authority:logs"), reg.Generation)
					} else {
						_, e = v.registry.AdminRevoke(admin, collectorID, reg.Generation)
					}
					report(e)
				case 1:
					_, e := v.registry.AdminRegistration(admin, collectorID)
					report(e)
				case 2:
					_, e := v.registry.Changes(context.Background(), collectorID, 0, MaxChangesPage)
					report(e)
				}
			}
		}()
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d outcomes reported damage under ordinary churn", bad.Load())
	}
}

// A marker near the top uses up the headroom, as documented: the writes it
// leaves room for stay clean and readable, then the log is exhausted —
// provisioning fails closed, revocation is still recorded.
func TestAMarkerNearTheTopExhaustsTheLogHonestly(t *testing.T) {
	v := provisionedEnv(t)
	tombstone(t, v, v.registry.changeHeadCoordinate(collectorID), maxWriteTimestamp-2)
	if _, e := v.registry.AdminRevoke(v.admin(t), collectorID, 1); e != nil {
		t.Fatalf("revoke: %v", e)
	}
	toggle := func() (collector.Registration, error) {
		reg, _ := v.registry.Registration(context.Background(), collectorID)
		var e error
		if reg.State == collector.Revoked {
			_, e = v.registry.AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), reg.Generation)
		} else {
			_, e = v.registry.AdminRevoke(v.admin(t), collectorID, reg.Generation)
		}
		return reg, e
	}
	for writes := 0; ; writes++ {
		head, e := v.registry.read(context.Background(), v.registry.changeHeadCoordinate(collectorID))
		if e != nil {
			t.Fatal(e)
		}
		if head.Timestamp >= maxWriteTimestamp {
			break
		}
		if writes > 4 {
			t.Fatal("the headroom was never used up; the test is vacuous")
		}
		if _, e := toggle(); e != nil {
			t.Fatalf("write within the headroom: %v", e)
		}
		if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); e != nil {
			t.Fatalf("a clean write left the log unreadable: %v", e)
		}
	}
	// Exhausted: the complete log still reads; then the next change is
	// either refused (provisioning) or recorded beyond the head
	// (revocation), after which reads fail closed.
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); e != nil {
		t.Fatalf("an exhausted complete log: %v", e)
	}
	reg, e := toggle()
	if reg.State == collector.Revoked {
		if !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
			t.Fatalf("provision over an exhausted log: %v", e)
		}
		return
	}
	if e != nil {
		t.Fatalf("revoke over an exhausted log: %v", e)
	}
	if _, e := v.registry.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("read after a revocation beyond the head: %v", e)
	}
}

// everyHeadRead runs hook on every change-head read, up to times.
type everyHeadRead struct {
	lostAck
	times atomic.Int32
	skip  atomic.Int32
	hook  func()
}

func (a *everyHeadRead) ReadExact(ctx context.Context, c []allocator.Coordinate) ([]allocator.Cell, error) {
	cells, e := a.inner.ReadExact(ctx, c)
	if len(c) == 1 && string(c[0].Qualifier) == "change-head" && a.skip.Add(-1) < 0 && a.times.Add(-1) >= 0 {
		a.hook()
	}
	return cells, e
}

func wrapped(t *testing.T, v *env, backend interface {
	ReadExact(context.Context, []allocator.Coordinate) ([]allocator.Cell, error)
	CompareAndMutate(context.Context, allocator.Mutation) (allocator.Status, error)
}) *Registry {
	t.Helper()
	config := v.registry.config
	config.Backend = backend
	r, e := New(config)
	if e != nil {
		t.Fatal(e)
	}
	return r
}

// With no head yet, a first logged change committing between the head read
// and the probe for hidden entries is concurrency, not damage.
func TestAFirstChangeRacingAnAbsentHeadReadIsNotDamage(t *testing.T) {
	v := newEnv(t)
	backend := &everyHeadRead{lostAck: lostAck{inner: v.registry.config.Backend}}
	backend.times.Store(1)
	backend.hook = func() {
		if _, e := v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs")); e != nil {
			t.Error(e)
		}
	}
	if got, e := wrapped(t, v, backend).Changes(context.Background(), collectorID, 0, MaxChangesPage); e != nil || len(got) != 1 {
		t.Fatalf("Changes racing the first change: %d %v", len(got), e)
	}

	// A legacy row (no log) whose first logged change lands at the read's
	// second head read.
	w := newEnv(t)
	if _, e := w.registry.updateRegistration(context.Background(), collectorID, true, func(row *registrationRow, _ bool) (bool, error) {
		*row = registrationRow{Registration: collector.Registration{CollectorID: collectorID, Subject: "tail", ClientID: "client", Domain: []byte("domain"), AuthorityPolicyIDs: []shoal.ID{"authority:logs"}, Control: collector.ExternalControlled, Mode: collector.ServerObserved, Generation: 1, State: collector.Provisioned}}
		return true, nil
	}); e != nil {
		t.Fatal(e)
	}
	legacy := &everyHeadRead{lostAck: lostAck{inner: w.registry.config.Backend}}
	legacy.skip.Store(1)
	legacy.times.Store(1)
	legacy.hook = func() {
		if _, e := w.registry.Revoke(context.Background(), collectorID); e != nil {
			t.Error(e)
		}
	}
	if view, e := wrapped(t, w, legacy).AdminRegistration(w.admin(t), collectorID); e != nil || view.Registration.State != collector.Revoked {
		t.Fatalf("read racing a legacy row's first change: %+v %v", view, e)
	}
}

// A log that keeps moving under a read is answered with a retryable
// conflict, never unavailable.
func TestPersistentChurnUnderAReadIsAConflict(t *testing.T) {
	v := provisionedEnv(t)
	backend := &everyHeadRead{lostAck: lostAck{inner: v.registry.config.Backend}}
	backend.times.Store(40)
	backend.hook = func() {
		reg, _ := v.registry.Registration(context.Background(), collectorID)
		if reg.State == collector.Revoked {
			_, _ = v.registry.Provision(context.Background(), grant(collectorID, "tail", "authority:logs"))
		} else {
			_, _ = v.registry.Revoke(context.Background(), collectorID)
		}
	}
	r := wrapped(t, v, backend)
	if _, e := r.AdminRegistration(v.admin(t), collectorID); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
		t.Fatalf("read: %v", e)
	}
	backend.times.Store(40)
	if _, e := r.Changes(context.Background(), collectorID, 0, MaxChangesPage); !shoal.IsErrorCode(e, shoal.ErrorConflict) {
		t.Fatalf("Changes: %v", e)
	}
}

// gate holds the first change-log mutation until released.
type gate struct {
	lostAck
	armed   atomic.Bool
	reached chan struct{}
	release chan struct{}
}

func (g *gate) CompareAndMutate(ctx context.Context, m allocator.Mutation) (allocator.Status, error) {
	logs := false
	for _, u := range m.Updates {
		logs = logs || string(u.Coordinate.Family) == changeFamily
	}
	if logs && g.armed.CompareAndSwap(true, false) {
		close(g.reached)
		<-g.release
	}
	return g.inner.CompareAndMutate(ctx, m)
}

// A recovery judged against a head that then becomes healthy (a concurrent
// write repairing its own head above a delete marker) must not commit: it is
// pinned to the head it judged, re-evaluates, and appends normally, so the
// concurrent write's entry stays in the readable segment.
func TestAStaleRecoveryCannotOrphanACleanChange(t *testing.T) {
	v := provisionedEnv(t)
	if _, e := v.registry.Revoke(context.Background(), collectorID); e != nil {
		t.Fatal(e)
	}
	held := &gate{lostAck: lostAck{inner: v.registry.config.Backend}, reached: make(chan struct{}), release: make(chan struct{})}
	revoker := wrapped(t, v, held)
	done := make(chan error, 1)
	first := &onTop{lostAck: lostAck{inner: v.registry.config.Backend}}
	first.hook = func() {
		head, e := v.registry.read(context.Background(), v.registry.changeHeadCoordinate(collectorID))
		if e != nil {
			t.Error(e)
			return
		}
		tombstone(t, v, v.registry.changeHeadCoordinate(collectorID), head.Timestamp+3)
		held.armed.Store(true)
		go func() {
			_, e := revoker.Revoke(context.Background(), collectorID)
			done <- e
		}()
		<-held.reached // the revoker has judged the head absent and is at its CAS
	}
	first.armed.Store(true)
	result, e := wrapped(t, v, first).AdminProvision(v.admin(t), grant(collectorID, "tail", "authority:logs"), 2)
	close(held.release)
	if e != nil || !result.Changed || result.Sequence != 3 {
		t.Fatalf("provision %+v %v", result, e)
	}
	if e := <-done; e != nil {
		t.Fatalf("revoke: %v", e)
	}
	got, e := v.registry.Changes(context.Background(), collectorID, 0, MaxChangesPage)
	if e != nil || len(got) != 4 || got[2].Sequence != 3 || got[3].Action != ChangeRevoke || got[3].Recovery != nil {
		t.Fatalf("changes %+v %v", got, e)
	}
}
