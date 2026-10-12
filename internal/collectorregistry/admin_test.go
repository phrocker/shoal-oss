// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package collectorregistry

import (
	"bytes"
	"context"
	"errors"
	"slices"
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
	if head, _, e := v.registry.readChangeHead(context.Background(), id); e != nil || head != 0 {
		t.Fatalf("change log written without a registration: %d %v", head, e)
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
	status, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Value: cell.Value, Timestamp: 1, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: 2, Value: bytes.Replace(cell.Value, []byte(`"admin"`), []byte(`"other"`), 1)}}})
	if e != nil || status != allocator.StatusAccepted {
		t.Fatal(status, e)
	}
	if _, e := v.registry.AdminRegistration(admin, collectorID); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("altered entry: %v", e)
	}
	if _, e := v.registry.Changes(context.Background(), collectorID, 0, 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("altered entry: %v", e)
	}
	// An entry is written once: a second version, even with the original's
	// exact bytes, is not an entry this registry wrote.
	if _, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: coord.Row, Conditions: []allocator.Condition{{Coordinate: coord, Timestamp: 2, TimestampSet: true, Value: bytes.Replace(cell.Value, []byte(`"admin"`), []byte(`"other"`), 1)}}, Updates: []allocator.Update{{Coordinate: coord, Timestamp: 3, Value: cell.Value}}}); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.Changes(context.Background(), collectorID, 0, 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("rewritten entry: %v", e)
	}
	// And a further change cannot be appended over a corrupt head.
	head := v.registry.changeHeadCoordinate(collectorID)
	headCell, _ := v.registry.read(context.Background(), head)
	if _, e := v.registry.config.Backend.CompareAndMutate(context.Background(), allocator.Mutation{Row: head.Row, Conditions: []allocator.Condition{{Coordinate: head, Value: headCell.Value, Timestamp: headCell.Timestamp, TimestampSet: true}}, Updates: []allocator.Update{{Coordinate: head, Timestamp: headCell.Timestamp + 1, Value: []byte("garbage")}}}); e != nil {
		t.Fatal(e)
	}
	if _, e := v.registry.AdminRevoke(admin, collectorID, 1); !shoal.IsErrorCode(e, shoal.ErrorUnavailable) {
		t.Fatalf("revoke over a corrupt head: %v", e)
	}
	if reg, _ := v.registry.Registration(context.Background(), collectorID); reg.State != collector.Provisioned {
		t.Fatalf("a change was written without its entry: %+v", reg)
	}
}
