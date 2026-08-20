package ingestclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	clientpkg "github.com/phrocker/shoal-oss/internal/thrift/gen/client"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/data"
	"github.com/phrocker/shoal-oss/internal/thrift/gen/tabletingest"
	"github.com/phrocker/shoal-oss/internal/transportpool"
)

// ConditionalStatus is the authoritative result of one conditional mutation.
type ConditionalStatus uint8

const (
	ConditionalUnknown ConditionalStatus = iota
	ConditionalAccepted
	ConditionalRejected
)

// ConditionalWrite sends one exact-row conditional mutation. Unknown means
// the caller must reread authoritative state before deciding whether to retry.
func (p *Pooled) ConditionalWrite(
	ctx context.Context,
	address, tableID string,
	extent *data.TKeyExtent,
	mutation *data.TConditionalMutation,
) (ConditionalStatus, error) {
	if err := ctx.Err(); err != nil {
		return ConditionalUnknown, err
	}
	if address == "" || tableID == "" || extent == nil || mutation == nil ||
		mutation.Mutation == nil || len(mutation.Mutation.Row) == 0 {
		return ConditionalUnknown, errors.New("ingestclient: invalid conditional mutation")
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return ConditionalUnknown, err
	}
	defer wipeCredentials(credentials)

	key := transportpool.Key{
		Address: address, Service: ingestServiceName,
		InstanceID: p.instanceID, ProtocolVersion: p.accumuloVersion,
	}
	lease, err := p.pool.Acquire(ctx, key, p.dial)
	if err != nil {
		return ConditionalUnknown, err
	}
	rawTransport, ok := lease.Transport().(thrift.TTransport)
	if !ok {
		return ConditionalUnknown, errors.Join(
			errors.New("ingestclient: conditional lease is not a thrift transport"),
			lease.Invalidate(),
		)
	}
	thriftClient := newThriftClient(rawTransport, p.instanceID, p.accumuloVersion)
	session, err := thriftClient.StartConditionalUpdate(
		ctx, clientpkg.NewTInfo(), credentials, nil, tableID,
		tabletingest.TDurability_SYNC, "",
	)
	if err != nil {
		return ConditionalUnknown, errors.Join(err, finishLease(lease, err))
	}
	if session == nil || session.SessionId == 0 || session.TserverLock == "" {
		_ = thriftClient.InvalidateConditionalUpdate(ctx, clientpkg.NewTInfo(), sessionID(session))
		return ConditionalUnknown, errors.Join(
			errors.New("ingestclient: invalid conditional session"),
			lease.Invalidate(),
		)
	}

	results, updateErr := thriftClient.ConditionalUpdate(
		ctx, clientpkg.NewTInfo(), data.UpdateID(session.SessionId),
		data.CMBatch{extent: []*data.TConditionalMutation{mutation}}, nil,
	)
	if updateErr != nil {
		_ = thriftClient.InvalidateConditionalUpdate(
			context.Background(), clientpkg.NewTInfo(), data.UpdateID(session.SessionId),
		)
		return ConditionalUnknown, errors.Join(updateErr, finishLease(lease, updateErr))
	}
	if len(results) != 1 || results[0] == nil || results[0].Cmid != mutation.ID {
		_ = thriftClient.InvalidateConditionalUpdate(
			context.Background(), clientpkg.NewTInfo(), data.UpdateID(session.SessionId),
		)
		return ConditionalUnknown, errors.Join(
			fmt.Errorf("ingestclient: invalid conditional result %#v", results),
			lease.Invalidate(),
		)
	}
	closeErr := thriftClient.CloseConditionalUpdate(
		context.Background(), clientpkg.NewTInfo(), data.UpdateID(session.SessionId),
	)
	cleanupErr := finishLease(lease, closeErr)
	switch results[0].Status {
	case data.TCMStatus_ACCEPTED:
		return ConditionalAccepted, errors.Join(closeErr, cleanupErr)
	case data.TCMStatus_REJECTED:
		return ConditionalRejected, errors.Join(closeErr, cleanupErr)
	default:
		return ConditionalUnknown, errors.Join(
			fmt.Errorf("ingestclient: conditional status %s", results[0].Status),
			closeErr, cleanupErr,
		)
	}
}

func sessionID(session *data.TConditionalSession) data.UpdateID {
	if session == nil {
		return 0
	}
	return data.UpdateID(session.SessionId)
}
