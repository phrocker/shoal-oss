// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE.
package api

import (
	"context"
	"time"

	"github.com/phrocker/shoal-oss/pkg/collector"
	"github.com/phrocker/shoal-oss/pkg/shoal"
)

// Collector administration routes (#446 slice 5). They provision, revoke and
// read collector registrations and require the collector_admin operation,
// which no shipped authenticator mints; a host that mounts them mints its own
// admin decisions. See docs/collectors.md.
const (
	AdminRoute              = Route + "/admin"
	AdminProvisionRoute     = AdminRoute + "/provision"
	AdminRevokeRoute        = AdminRoute + "/revoke"
	AdminRegistrationsRoute = AdminRoute + "/registrations"

	// MaxAdminReceiptBytes bounds every admin write receipt, with the
	// collector ID at shoal.MaxIDBytes.
	MaxAdminReceiptBytes = 2048
	// MaxRegistrationChanges bounds the change summaries in one
	// RegistrationRecord.
	MaxRegistrationChanges = 16
)

// Change log actions and sources, as RegistrationRecord reports them.
const (
	ChangeProvision      = "provision"
	ChangeRevoke         = "revoke"
	SourceOperatorAPI    = "operator_api"
	SourceCollectorAdmin = "collector_admin"
)

// Provisioning is an admin's grant to one principal, in contract form. It
// mirrors the server's Go provisioning API, plus the generation the
// registration must hold afterwards: 1 to create, the current generation to
// reactivate a revoked registration or to confirm an identical active one.
type Provisioning struct {
	CollectorID        shoal.ID
	Subject            shoal.ID
	ClientID           shoal.ID
	Domain             []byte
	AuthorityPolicyIDs []shoal.ID
	Control            collector.Control
	Mode               collector.Mode
	Generation         int64
}

// Validate checks p without contacting the server.
func (p Provisioning) Validate() error {
	if _, e := p.registration().Canonical(); e != nil || p.Generation <= 0 {
		return invalidWire()
	}
	return nil
}

func (p Provisioning) registration() collector.Registration {
	return collector.Registration{CollectorID: p.CollectorID, Subject: p.Subject, ClientID: p.ClientID, Domain: p.Domain, AuthorityPolicyIDs: p.AuthorityPolicyIDs, Control: p.Control, Mode: p.Mode, Generation: 1, State: collector.Provisioned}
}

// AdminProvider is the server-side seam of the admin routes. Implementations
// resolve the admin from the trusted request context and own every
// authorization decision; nothing in these arguments carries authority.
type AdminProvider interface {
	ProvisionCollector(context.Context, Provisioning) (AdminReceipt, error)
	RevokeCollector(context.Context, shoal.ID, int64) (AdminReceipt, error)
	ReadRegistration(context.Context, shoal.ID) (RegistrationRecord, error)
}

type ProvisionRequest struct {
	CollectorID        string   `json:"collector_id"`
	Subject            string   `json:"subject"`
	ClientID           string   `json:"client_id,omitempty"`
	Domain             string   `json:"domain"`
	AuthorityPolicyIDs []string `json:"authority_policy_ids"`
	Control            string   `json:"control"`
	Mode               string   `json:"mode"`
	Generation         int64    `json:"generation"`
}

type RevokeRequest struct {
	CollectorID string `json:"collector_id"`
	Generation  int64  `json:"generation"`
}

// AdminReceipt is the receipt of an admin write. Like the collector write
// receipts it carries identifiers and counters only; the registration is read
// back through the registrations route. Changed reports whether this call
// made the change: a retry of a change that already applied reports false.
// Sequence is the change log entry the call wrote, 0 when it changed nothing.
type AdminReceipt struct {
	Schema      int    `json:"schema"`
	CollectorID string `json:"collector_id"`
	Generation  int64  `json:"generation"`
	State       string `json:"state"`
	Changed     bool   `json:"changed"`
	Sequence    int64  `json:"sequence"`
}

// AdminAttribution identifies the admin decision behind a change.
type AdminAttribution struct {
	Subject                  string `json:"subject"`
	Actor                    string `json:"actor"`
	ClientID                 string `json:"client_id,omitempty"`
	RequestID                string `json:"request_id"`
	CorrelationID            string `json:"correlation_id,omitempty"`
	AuthorizationFingerprint string `json:"authorization_fingerprint"`
}

// Recovery reasons, as ChangeSummary reports them.
const (
	RecoveryUnreadableHead   = "unreadable_head"
	RecoveryOccupiedSequence = "occupied_sequence"
)

// ChangeSummary is one change log entry. By is present exactly when Source is
// SourceCollectorAdmin; the trusted Go API has no caller identity. Recovery
// is set on a revocation that started a new log segment because the log
// could not be appended to (docs/collectors.md, "Audit").
type ChangeSummary struct {
	Sequence   int64             `json:"sequence"`
	Action     string            `json:"action"`
	Source     string            `json:"source"`
	Generation int64             `json:"generation"`
	State      string            `json:"state"`
	By         *AdminAttribution `json:"by,omitempty"`
	At         time.Time         `json:"at"`
	Recovery   string            `json:"recovery,omitempty"`
}

// RegistrationRecord is a registration as an admin reads it, with its most
// recent changes, newest first. It carries no quarantine status:
// observation status is computed from the current registration at each
// observation read.
type RegistrationRecord struct {
	Schema             int             `json:"schema"`
	CollectorID        string          `json:"collector_id"`
	Subject            string          `json:"subject"`
	ClientID           string          `json:"client_id,omitempty"`
	Domain             string          `json:"domain"`
	AuthorityPolicyIDs []string        `json:"authority_policy_ids"`
	Control            string          `json:"control"`
	Mode               string          `json:"mode"`
	Generation         int64           `json:"generation"`
	State              string          `json:"state"`
	EnrollmentID       string          `json:"enrollment_id,omitempty"`
	ChangeCount        int64           `json:"change_count"`
	Changes            []ChangeSummary `json:"changes"`
	// SegmentStart is the sequence of the recovery entry that starts the
	// current log segment, absent when the log never needed recovery. Only
	// that segment is listed. ChangeCount is the newest sequence, which after
	// a recovery is not a count of stored entries.
	SegmentStart int64 `json:"segment_start,omitempty"`
}

// EncodeProvision converts a contract provisioning to its wire form.
func EncodeProvision(p Provisioning) ProvisionRequest {
	out := ProvisionRequest{CollectorID: EncodeID(p.CollectorID), Subject: EncodeID(p.Subject), Domain: EncodeKey(p.Domain), AuthorityPolicyIDs: []string{}, Control: string(p.Control), Mode: string(p.Mode), Generation: p.Generation}
	if p.ClientID != "" {
		out.ClientID = EncodeID(p.ClientID)
	}
	for _, id := range p.AuthorityPolicyIDs {
		out.AuthorityPolicyIDs = append(out.AuthorityPolicyIDs, EncodeID(id))
	}
	return out
}

// DecodeProvision converts and validates a wire provisioning. Authority IDs
// are returned in canonical order.
func DecodeProvision(w ProvisionRequest) (Provisioning, error) {
	var p Provisioning
	var e error
	if p.CollectorID, e = DecodeID(w.CollectorID); e != nil {
		return Provisioning{}, invalidWire()
	}
	if p.Subject, e = DecodeID(w.Subject); e != nil {
		return Provisioning{}, invalidWire()
	}
	if w.ClientID != "" {
		if p.ClientID, e = DecodeID(w.ClientID); e != nil {
			return Provisioning{}, invalidWire()
		}
	}
	if p.Domain, e = DecodeKey(w.Domain); e != nil {
		return Provisioning{}, invalidWire()
	}
	if len(w.AuthorityPolicyIDs) > collector.MaxAuthorityPolicyIDs {
		return Provisioning{}, invalidWire()
	}
	for _, encoded := range w.AuthorityPolicyIDs {
		id, e := DecodeID(encoded)
		if e != nil {
			return Provisioning{}, invalidWire()
		}
		p.AuthorityPolicyIDs = append(p.AuthorityPolicyIDs, id)
	}
	p.Control, p.Mode, p.Generation = collector.Control(w.Control), collector.Mode(w.Mode), w.Generation
	canonical, e := p.registration().Canonical()
	if e != nil || p.Generation <= 0 {
		return Provisioning{}, invalidWire()
	}
	p.AuthorityPolicyIDs = canonical.AuthorityPolicyIDs
	return p, nil
}

// DecodeRevoke converts and validates a wire revocation.
func DecodeRevoke(w RevokeRequest) (shoal.ID, int64, error) {
	id, e := DecodeID(w.CollectorID)
	if e != nil || w.Generation <= 0 {
		return "", 0, invalidWire()
	}
	return id, w.Generation, nil
}

func validState(s string) bool {
	switch collector.State(s) {
	case collector.Provisioned, collector.Enrolled, collector.Revoked:
		return true
	}
	return false
}

func (r AdminReceipt) Validate() error {
	if r.Schema != Schema || !validEncodedID(r.CollectorID) || r.Generation <= 0 || !validState(r.State) || r.Sequence < 0 || r.Changed != (r.Sequence > 0) {
		return badResponse()
	}
	return nil
}

func (a AdminAttribution) validate() error {
	if !validEncodedID(a.Subject) || !validEncodedID(a.Actor) || !validEncodedID(a.RequestID) || a.AuthorizationFingerprint == "" || len(a.AuthorizationFingerprint) > 128 {
		return badResponse()
	}
	for _, optional := range []string{a.ClientID, a.CorrelationID} {
		if optional != "" && !validEncodedID(optional) {
			return badResponse()
		}
	}
	return nil
}

func (c ChangeSummary) validate() error {
	if c.Sequence <= 0 || c.Generation <= 0 || !validState(c.State) || !validTime(c.At) {
		return badResponse()
	}
	if (c.Action != ChangeProvision && c.Action != ChangeRevoke) || (c.Action == ChangeRevoke) != (c.State == string(collector.Revoked)) {
		return badResponse()
	}
	switch c.Source {
	case SourceOperatorAPI:
		if c.By != nil {
			return badResponse()
		}
	case SourceCollectorAdmin:
		if c.By == nil || c.By.validate() != nil {
			return badResponse()
		}
	default:
		return badResponse()
	}
	switch c.Recovery {
	case "":
	case RecoveryUnreadableHead, RecoveryOccupiedSequence:
		if c.Action != ChangeRevoke {
			return badResponse()
		}
	default:
		return badResponse()
	}
	return nil
}

// Validate checks wire integrity: the registration decodes canonically and
// the change summaries are well formed, newest first, and no more than the
// change count.
func (r RegistrationRecord) Validate() error {
	if r.Schema != Schema || !validState(r.State) || r.ChangeCount < 0 || len(r.Changes) > MaxRegistrationChanges || int64(len(r.Changes)) > r.ChangeCount {
		return badResponse()
	}
	p, e := DecodeProvision(ProvisionRequest{CollectorID: r.CollectorID, Subject: r.Subject, ClientID: r.ClientID, Domain: r.Domain, AuthorityPolicyIDs: r.AuthorityPolicyIDs, Control: r.Control, Mode: r.Mode, Generation: r.Generation})
	if e != nil {
		return badResponse()
	}
	for i, id := range p.AuthorityPolicyIDs {
		if EncodeID(id) != r.AuthorityPolicyIDs[i] {
			return badResponse()
		}
	}
	if (r.EnrollmentID != "") != (r.State == string(collector.Enrolled)) || (r.EnrollmentID != "" && !validEncodedID(r.EnrollmentID)) {
		return badResponse()
	}
	for i, c := range r.Changes {
		if c.validate() != nil || c.Sequence > r.ChangeCount || (i > 0 && c.Sequence >= r.Changes[i-1].Sequence) {
			return badResponse()
		}
		// Only the current segment is listed, and its recovery entry is
		// exactly the one SegmentStart names.
		if c.Sequence < r.SegmentStart || (c.Recovery != "") != (r.SegmentStart == c.Sequence) {
			return badResponse()
		}
	}
	if r.SegmentStart < 0 || r.SegmentStart > r.ChangeCount {
		return badResponse()
	}
	return nil
}
