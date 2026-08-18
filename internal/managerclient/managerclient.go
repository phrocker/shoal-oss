package managerclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/protocol"
	clientgen "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/manager"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/transportpool"
)

const (
	fateServiceName      = "fate"
	managerServiceName   = "mgr"
	clientServiceName    = "client"
	fateFinishTimeout    = 5 * time.Second
	waitForFlushMaxLoops = int64(1<<63 - 1)
	noWaitFlushMaxLoops  = int64(1)
)

type Operation int

const (
	TableCreate Operation = iota
	TableDelete
	TableRename
	// TableBulkImport submits Accumulo's Bulk Import V2 FATE operation
	// (TABLE_BULK_IMPORT2). It takes exactly 3 arguments: the destination
	// table's canonical ID (not its name — unlike every other Operation
	// here), the bulk directory path, and a "true"/"false" setTime flag. The
	// caller is responsible for staging the bulk directory (files flat,
	// loadmap.json written) before submitting; see internal/promotion.
	TableBulkImport
	NamespaceCreate
	NamespaceDelete
	NamespaceRename
)

type FateInstance int

const (
	FateUser FateInstance = iota
	FateMeta
)

type Request struct {
	Operation Operation
	Instance  FateInstance
	Arguments [][]byte
	Options   map[string]string
}

type VersionedProperties struct {
	Version    int64
	Properties map[string]string
}

type ErrorKind int

const (
	ErrorUnknown ErrorKind = iota
	ErrorTableExists
	ErrorTableNotFound
	ErrorNamespaceExists
	ErrorNamespaceNotFound
	ErrorInvalidName
	ErrorInvalidProperty
	ErrorSecurity
	ErrorNotActive
)

type Error struct {
	Kind        ErrorKind
	TableID     string
	TableName   string
	Property    string
	Value       string
	Description string
	Code        string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	detail := e.Description
	if detail == "" {
		detail = e.Code
	}
	if detail == "" {
		detail = "operation failed"
	}
	return "managerclient: " + detail
}

type Adapter interface {
	Execute(context.Context, string, Request) error
	FlushTable(context.Context, string, string, bool) error
	GetTableConfiguration(context.Context, string, string) (map[string]string, error)
	GetNamespaceConfiguration(context.Context, string, string) (map[string]string, error)
	GetNamespaceProperties(context.Context, string, string) (map[string]string, error)
	GetVersionedNamespaceProperties(context.Context, string, string) (VersionedProperties, error)
	SetTableProperty(context.Context, string, string, string, string) error
	RemoveTableProperty(context.Context, string, string, string) error
	SetNamespaceProperty(context.Context, string, string, string, string) error
	RemoveNamespaceProperty(context.Context, string, string, string) error
	Close() error
}

type fateID struct {
	Type int32
	UUID string
}

type fateRPC interface {
	Begin(context.Context, *security.TCredentials, FateInstance) (fateID, error)
	Execute(context.Context, *security.TCredentials, fateID, Request) error
	Wait(context.Context, *security.TCredentials, fateID) (string, error)
	Finish(context.Context, *security.TCredentials, fateID) error
}

type managerRPC interface {
	InitiateFlush(context.Context, *security.TCredentials, string) (int64, error)
	WaitForFlush(context.Context, *security.TCredentials, string, int64, int64) error
	SetTableProperty(context.Context, *security.TCredentials, string, string, string) error
	RemoveTableProperty(context.Context, *security.TCredentials, string, string) error
	SetNamespaceProperty(context.Context, *security.TCredentials, string, string, string) error
	RemoveNamespaceProperty(context.Context, *security.TCredentials, string, string) error
}

type clientRPC interface {
	GetTableConfiguration(
		context.Context,
		*security.TCredentials,
		string,
	) (map[string]string, error)
	GetNamespaceConfiguration(
		context.Context,
		*security.TCredentials,
		string,
	) (map[string]string, error)
	GetNamespaceProperties(
		context.Context,
		*security.TCredentials,
		string,
	) (map[string]string, error)
	GetVersionedNamespaceProperties(
		context.Context,
		*security.TCredentials,
		string,
	) (VersionedProperties, error)
}

type Pooled struct {
	pool            *transportpool.Pool
	instanceID      string
	accumuloVersion string
	dialTimeout     time.Duration
	finishTimeout   time.Duration

	mu          sync.RWMutex
	credentials *security.TCredentials
	closed      bool

	dial             transportpool.DialFunc
	newClient        func(io.Closer) (fateRPC, error)
	newManagerClient func(io.Closer) (managerRPC, error)
	newServiceClient func(io.Closer) (clientRPC, error)
}

var _ Adapter = (*Pooled)(nil)

func NewPooled(
	pool *transportpool.Pool,
	instanceID, accumuloVersion string,
	credentials *security.TCredentials,
	dialTimeout time.Duration,
) (*Pooled, error) {
	switch {
	case pool == nil:
		return nil, errors.New("managerclient: nil transport pool")
	case instanceID == "":
		return nil, errors.New("managerclient: empty instanceID")
	case accumuloVersion == "":
		return nil, errors.New("managerclient: empty accumuloVersion")
	case credentials == nil:
		return nil, errors.New("managerclient: nil Credentials")
	case dialTimeout < 0:
		return nil, errors.New("managerclient: negative dial timeout")
	}
	p := &Pooled{
		pool:            pool,
		instanceID:      instanceID,
		accumuloVersion: accumuloVersion,
		dialTimeout:     dialTimeout,
		finishTimeout:   fateFinishTimeout,
		credentials:     cloneCredentials(credentials),
	}
	p.dial = p.dialThrift
	p.newClient = p.newThriftRPC
	p.newManagerClient = p.newThriftManagerRPC
	p.newServiceClient = p.newThriftClientRPC
	return p, nil
}

func (p *Pooled) Execute(ctx context.Context, address string, req Request) (err error) {
	if err := validateRequest(req); err != nil {
		return err
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return err
	}
	id, err := withClient(p, ctx, address, func(rpc fateRPC) (fateID, error) {
		return rpc.Begin(ctx, credentials, req.Instance)
	})
	if id.UUID != "" {
		defer func() {
			finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.finishTimeout)
			defer cancel()
			_, finishErr := withClient(p, finishCtx, address, func(rpc fateRPC) (struct{}, error) {
				return struct{}{}, rpc.Finish(finishCtx, credentials, id)
			})
			err = errors.Join(err, mapRPCError(finishErr))
		}()
	}
	if err != nil {
		return mapRPCError(err)
	}

	if _, err := withClient(p, ctx, address, func(rpc fateRPC) (struct{}, error) {
		return struct{}{}, rpc.Execute(ctx, credentials, id, req)
	}); err != nil {
		return mapRPCError(err)
	}
	if _, err := withClient(p, ctx, address, func(rpc fateRPC) (string, error) {
		return rpc.Wait(ctx, credentials, id)
	}); err != nil {
		return mapRPCError(err)
	}
	return nil
}

func (p *Pooled) SetTableProperty(
	ctx context.Context,
	address, tableName, property, value string,
) error {
	if err := validatePropertyRequest(tableName, property); err != nil {
		return err
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return err
	}
	_, err = withManagerClient(p, ctx, address, func(rpc managerRPC) (struct{}, error) {
		return struct{}{}, rpc.SetTableProperty(ctx, credentials, tableName, property, value)
	})
	return mapRPCError(err)
}

func (p *Pooled) FlushTable(
	ctx context.Context,
	address, tableID string,
	wait bool,
) error {
	if tableID == "" {
		return errors.New("managerclient: empty table ID")
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return err
	}
	flushID, err := withManagerClient(p, ctx, address, func(rpc managerRPC) (int64, error) {
		return rpc.InitiateFlush(ctx, credentials, tableID)
	})
	if err != nil {
		return mapRPCError(err)
	}
	maxLoops := noWaitFlushMaxLoops
	if wait {
		maxLoops = waitForFlushMaxLoops
	}
	_, err = withManagerClient(p, ctx, address, func(rpc managerRPC) (struct{}, error) {
		return struct{}{}, rpc.WaitForFlush(ctx, credentials, tableID, flushID, maxLoops)
	})
	return mapRPCError(err)
}

func (p *Pooled) GetTableConfiguration(
	ctx context.Context,
	address, tableName string,
) (map[string]string, error) {
	if tableName == "" {
		return nil, errors.New("managerclient: empty table name")
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return nil, err
	}
	properties, err := withClientService(p, ctx, address, func(rpc clientRPC) (map[string]string, error) {
		return rpc.GetTableConfiguration(ctx, credentials, tableName)
	})
	if err != nil {
		return nil, mapRPCError(err)
	}
	return cloneOptions(properties), nil
}

func (p *Pooled) GetNamespaceConfiguration(
	ctx context.Context,
	address, namespace string,
) (map[string]string, error) {
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return nil, err
	}
	properties, err := withClientService(p, ctx, address, func(rpc clientRPC) (map[string]string, error) {
		return rpc.GetNamespaceConfiguration(ctx, credentials, namespace)
	})
	if err != nil {
		return nil, mapRPCError(err)
	}
	return cloneOptions(properties), nil
}

func (p *Pooled) GetNamespaceProperties(
	ctx context.Context,
	address, namespace string,
) (map[string]string, error) {
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return nil, err
	}
	properties, err := withClientService(p, ctx, address, func(rpc clientRPC) (map[string]string, error) {
		return rpc.GetNamespaceProperties(ctx, credentials, namespace)
	})
	if err != nil {
		return nil, mapRPCError(err)
	}
	return cloneOptions(properties), nil
}

func (p *Pooled) GetVersionedNamespaceProperties(
	ctx context.Context,
	address, namespace string,
) (VersionedProperties, error) {
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return VersionedProperties{}, err
	}
	properties, err := withClientService(p, ctx, address, func(rpc clientRPC) (VersionedProperties, error) {
		return rpc.GetVersionedNamespaceProperties(ctx, credentials, namespace)
	})
	if err != nil {
		return VersionedProperties{}, mapRPCError(err)
	}
	properties.Properties = cloneOptions(properties.Properties)
	return properties, nil
}

func (p *Pooled) RemoveTableProperty(
	ctx context.Context,
	address, tableName, property string,
) error {
	if err := validatePropertyRequest(tableName, property); err != nil {
		return err
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return err
	}
	_, err = withManagerClient(p, ctx, address, func(rpc managerRPC) (struct{}, error) {
		return struct{}{}, rpc.RemoveTableProperty(ctx, credentials, tableName, property)
	})
	return mapRPCError(err)
}

func (p *Pooled) SetNamespaceProperty(
	ctx context.Context,
	address, namespace, property, value string,
) error {
	if property == "" {
		return errors.New("managerclient: empty property")
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return err
	}
	_, err = withManagerClient(p, ctx, address, func(rpc managerRPC) (struct{}, error) {
		return struct{}{}, rpc.SetNamespaceProperty(ctx, credentials, namespace, property, value)
	})
	return mapRPCError(err)
}

func (p *Pooled) RemoveNamespaceProperty(
	ctx context.Context,
	address, namespace, property string,
) error {
	if property == "" {
		return errors.New("managerclient: empty property")
	}
	credentials, err := p.credentialsForRPC()
	if err != nil {
		return err
	}
	_, err = withManagerClient(p, ctx, address, func(rpc managerRPC) (struct{}, error) {
		return struct{}{}, rpc.RemoveNamespaceProperty(ctx, credentials, namespace, property)
	})
	return mapRPCError(err)
}

func withClient[T any](
	p *Pooled,
	ctx context.Context,
	address string,
	call func(fateRPC) (T, error),
) (result T, err error) {
	return withServiceClient(p, ctx, address, fateServiceName, p.newClient, call)
}

func withManagerClient[T any](
	p *Pooled,
	ctx context.Context,
	address string,
	call func(managerRPC) (T, error),
) (result T, err error) {
	return withServiceClient(p, ctx, address, managerServiceName, p.newManagerClient, call)
}

func withClientService[T any](
	p *Pooled,
	ctx context.Context,
	address string,
	call func(clientRPC) (T, error),
) (result T, err error) {
	return withServiceClient(p, ctx, address, clientServiceName, p.newServiceClient, call)
}

func withServiceClient[T any, C any](
	p *Pooled,
	ctx context.Context,
	address, service string,
	newClient func(io.Closer) (C, error),
	call func(C) (T, error),
) (result T, err error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	key := transportpool.Key{
		Address:         address,
		Service:         service,
		InstanceID:      p.instanceID,
		ProtocolVersion: p.accumuloVersion,
	}
	lease, err := p.pool.Acquire(ctx, key, p.dial)
	if err != nil {
		return result, err
	}
	client, err := newClient(lease.Transport())
	if err != nil {
		return result, errors.Join(err, lease.Invalidate())
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(err, lease.Close())
	}
	result, rpcErr := call(client)
	var cleanupErr error
	if shouldInvalidateTransport(rpcErr) {
		cleanupErr = lease.Invalidate()
	} else {
		cleanupErr = lease.Close()
	}
	return result, errors.Join(rpcErr, cleanupErr)
}

func (p *Pooled) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.credentials != nil {
		for i := range p.credentials.Token {
			p.credentials.Token[i] = 0
		}
		p.credentials.Token = nil
	}
	return nil
}

func (p *Pooled) credentialsForRPC() (*security.TCredentials, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, errors.New("managerclient: pooled client is closed")
	}
	return cloneCredentials(p.credentials), nil
}

func (p *Pooled) dialThrift(ctx context.Context, key transportpool.Key) (io.Closer, error) {
	dialer := &net.Dialer{Timeout: p.dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", key.Address)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	conf := &thrift.TConfiguration{}
	socket := thrift.NewTSocketFromConnConf(conn, conf)
	return thrift.NewTFramedTransportConf(socket, conf), nil
}

func (p *Pooled) newThriftRPC(transport io.Closer) (fateRPC, error) {
	thriftTransport, ok := transport.(thrift.TTransport)
	if !ok {
		return nil, fmt.Errorf("managerclient: pooled transport %T is not a thrift transport", transport)
	}
	proto := protocol.NewClientFactory(p.instanceID, p.accumuloVersion).GetProtocol(thriftTransport)
	muxed := thrift.NewTMultiplexedProtocol(proto, fateServiceName)
	raw := manager.NewFateServiceClient(thrift.NewTStandardClient(muxed, muxed))
	return thriftFateRPC{raw: raw}, nil
}

func (p *Pooled) newThriftManagerRPC(transport io.Closer) (managerRPC, error) {
	thriftTransport, ok := transport.(thrift.TTransport)
	if !ok {
		return nil, fmt.Errorf("managerclient: pooled transport %T is not a thrift transport", transport)
	}
	proto := protocol.NewClientFactory(p.instanceID, p.accumuloVersion).GetProtocol(thriftTransport)
	muxed := thrift.NewTMultiplexedProtocol(proto, managerServiceName)
	raw := manager.NewManagerClientServiceClient(thrift.NewTStandardClient(muxed, muxed))
	return thriftManagerRPC{raw: raw}, nil
}

func (p *Pooled) newThriftClientRPC(transport io.Closer) (clientRPC, error) {
	thriftTransport, ok := transport.(thrift.TTransport)
	if !ok {
		return nil, fmt.Errorf("managerclient: pooled transport %T is not a thrift transport", transport)
	}
	proto := protocol.NewClientFactory(p.instanceID, p.accumuloVersion).GetProtocol(thriftTransport)
	muxed := thrift.NewTMultiplexedProtocol(proto, clientServiceName)
	raw := clientgen.NewClientServiceClient(thrift.NewTStandardClient(muxed, muxed))
	return thriftClientRPC{raw: raw}, nil
}

type thriftFateRPC struct {
	raw *manager.FateServiceClient
}

func (r thriftFateRPC) Begin(
	ctx context.Context,
	credentials *security.TCredentials,
	instance FateInstance,
) (fateID, error) {
	id, err := r.raw.BeginFateOperation(
		ctx,
		&clientgen.TInfo{},
		credentials,
		thriftFateInstance(instance),
	)
	if err != nil {
		return fateID{}, err
	}
	if id == nil {
		return fateID{}, errors.New("managerclient: begin returned nil FATE ID")
	}
	return fateID{Type: int32(id.Type), UUID: id.TxUUIDStr}, nil
}

type thriftManagerRPC struct {
	raw *manager.ManagerClientServiceClient
}

type thriftClientRPC struct {
	raw *clientgen.ClientServiceClient
}

func (r thriftClientRPC) GetTableConfiguration(
	ctx context.Context,
	credentials *security.TCredentials,
	tableName string,
) (map[string]string, error) {
	return r.raw.GetTableConfiguration(
		ctx,
		clientgen.NewTInfo(),
		credentials,
		tableName,
	)
}

func (r thriftClientRPC) GetNamespaceConfiguration(
	ctx context.Context,
	credentials *security.TCredentials,
	namespace string,
) (map[string]string, error) {
	return r.raw.GetNamespaceConfiguration(ctx, clientgen.NewTInfo(), credentials, namespace)
}

func (r thriftClientRPC) GetNamespaceProperties(
	ctx context.Context,
	credentials *security.TCredentials,
	namespace string,
) (map[string]string, error) {
	return r.raw.GetNamespaceProperties(ctx, clientgen.NewTInfo(), credentials, namespace)
}

func (r thriftClientRPC) GetVersionedNamespaceProperties(
	ctx context.Context,
	credentials *security.TCredentials,
	namespace string,
) (VersionedProperties, error) {
	properties, err := r.raw.GetVersionedNamespaceProperties(
		ctx,
		clientgen.NewTInfo(),
		credentials,
		namespace,
	)
	if err != nil {
		return VersionedProperties{}, err
	}
	if properties == nil {
		return VersionedProperties{}, errors.New("managerclient: versioned namespace properties returned nil")
	}
	return VersionedProperties{
		Version:    properties.Version,
		Properties: cloneOptions(properties.Properties),
	}, nil
}

func (r thriftManagerRPC) SetTableProperty(
	ctx context.Context,
	credentials *security.TCredentials,
	tableName, property, value string,
) error {
	return r.raw.SetTableProperty(
		ctx,
		&clientgen.TInfo{},
		credentials,
		tableName,
		property,
		value,
	)
}

func (r thriftManagerRPC) InitiateFlush(
	ctx context.Context,
	credentials *security.TCredentials,
	tableID string,
) (int64, error) {
	return r.raw.InitiateFlush(ctx, &clientgen.TInfo{}, credentials, tableID)
}

func (r thriftManagerRPC) WaitForFlush(
	ctx context.Context,
	credentials *security.TCredentials,
	tableID string,
	flushID, maxLoops int64,
) error {
	// Accumulo uses absent row fields for an unbounded full-table flush.
	return r.raw.WaitForFlush(
		ctx,
		&clientgen.TInfo{},
		credentials,
		tableID,
		nil,
		nil,
		flushID,
		maxLoops,
	)
}

func (r thriftManagerRPC) RemoveTableProperty(
	ctx context.Context,
	credentials *security.TCredentials,
	tableName, property string,
) error {
	return r.raw.RemoveTableProperty(
		ctx,
		&clientgen.TInfo{},
		credentials,
		tableName,
		property,
	)
}

func (r thriftManagerRPC) SetNamespaceProperty(
	ctx context.Context,
	credentials *security.TCredentials,
	namespace, property, value string,
) error {
	return r.raw.SetNamespaceProperty(
		ctx,
		&clientgen.TInfo{},
		credentials,
		namespace,
		property,
		value,
	)
}

func (r thriftManagerRPC) RemoveNamespaceProperty(
	ctx context.Context,
	credentials *security.TCredentials,
	namespace, property string,
) error {
	return r.raw.RemoveNamespaceProperty(
		ctx,
		&clientgen.TInfo{},
		credentials,
		namespace,
		property,
	)
}

func thriftFateInstance(instance FateInstance) manager.TFateInstanceType {
	switch instance {
	case FateUser:
		return manager.TFateInstanceType_USER
	case FateMeta:
		return manager.TFateInstanceType_META
	default:
		panic("managerclient: validated unknown FATE instance")
	}
}

func (r thriftFateRPC) Execute(
	ctx context.Context,
	credentials *security.TCredentials,
	id fateID,
	req Request,
) error {
	return r.raw.ExecuteFateOperation(
		ctx,
		&clientgen.TInfo{},
		credentials,
		thriftFateID(id),
		thriftOperation(req.Operation),
		cloneArguments(req.Arguments),
		cloneOptions(req.Options),
		false,
	)
}

func (r thriftFateRPC) Wait(
	ctx context.Context,
	credentials *security.TCredentials,
	id fateID,
) (string, error) {
	return r.raw.WaitForFateOperation(ctx, &clientgen.TInfo{}, credentials, thriftFateID(id))
}

func (r thriftFateRPC) Finish(
	ctx context.Context,
	credentials *security.TCredentials,
	id fateID,
) error {
	return r.raw.FinishFateOperation(ctx, &clientgen.TInfo{}, credentials, thriftFateID(id))
}

func thriftFateID(id fateID) *manager.TFateId {
	return &manager.TFateId{
		Type:      manager.TFateInstanceType(id.Type),
		TxUUIDStr: id.UUID,
	}
}

func thriftOperation(op Operation) manager.TFateOperation {
	switch op {
	case TableCreate:
		return manager.TFateOperation_TABLE_CREATE
	case TableDelete:
		return manager.TFateOperation_TABLE_DELETE
	case TableRename:
		return manager.TFateOperation_TABLE_RENAME
	case TableBulkImport:
		return manager.TFateOperation_TABLE_BULK_IMPORT2
	case NamespaceCreate:
		return manager.TFateOperation_NAMESPACE_CREATE
	case NamespaceDelete:
		return manager.TFateOperation_NAMESPACE_DELETE
	case NamespaceRename:
		return manager.TFateOperation_NAMESPACE_RENAME
	default:
		panic("managerclient: validated unknown operation")
	}
}

func validateRequest(req Request) error {
	switch req.Instance {
	case FateUser, FateMeta:
	default:
		return fmt.Errorf("managerclient: unknown FATE instance %d", req.Instance)
	}
	switch req.Operation {
	case TableCreate:
		if len(req.Arguments) != 5 {
			return fmt.Errorf("managerclient: create requires 5 arguments, got %d", len(req.Arguments))
		}
	case TableDelete:
		if len(req.Arguments) != 1 {
			return fmt.Errorf("managerclient: delete requires 1 argument, got %d", len(req.Arguments))
		}
	case TableRename:
		if len(req.Arguments) != 2 {
			return fmt.Errorf("managerclient: rename requires 2 arguments, got %d", len(req.Arguments))
		}
	case TableBulkImport:
		if len(req.Arguments) != 3 {
			return fmt.Errorf("managerclient: bulk import requires 3 arguments, got %d", len(req.Arguments))
		}
	case NamespaceCreate, NamespaceDelete:
		if len(req.Arguments) != 1 {
			return fmt.Errorf("managerclient: namespace operation requires 1 argument, got %d", len(req.Arguments))
		}
	case NamespaceRename:
		if len(req.Arguments) != 2 {
			return fmt.Errorf("managerclient: namespace rename requires 2 arguments, got %d", len(req.Arguments))
		}
	default:
		return fmt.Errorf("managerclient: unknown operation %d", req.Operation)
	}
	for i, argument := range req.Arguments {
		if argument == nil {
			return fmt.Errorf("managerclient: nil argument %d", i)
		}
	}
	return nil
}

func validatePropertyRequest(tableName, property string) error {
	if tableName == "" {
		return errors.New("managerclient: empty table name")
	}
	if property == "" {
		return errors.New("managerclient: empty property")
	}
	return nil
}

func mapRPCError(err error) error {
	if err == nil {
		return nil
	}
	var tableErr *clientgen.ThriftTableOperationException
	if errors.As(err, &tableErr) {
		kind := ErrorUnknown
		switch tableErr.Type {
		case clientgen.TableOperationExceptionType_EXISTS:
			kind = ErrorTableExists
		case clientgen.TableOperationExceptionType_NOTFOUND:
			kind = ErrorTableNotFound
		case clientgen.TableOperationExceptionType_NAMESPACE_EXISTS:
			kind = ErrorNamespaceExists
		case clientgen.TableOperationExceptionType_NAMESPACE_NOTFOUND:
			kind = ErrorNamespaceNotFound
		case clientgen.TableOperationExceptionType_INVALID_NAME:
			kind = ErrorInvalidName
		}
		return &Error{
			Kind:        kind,
			TableID:     tableErr.TableId,
			TableName:   tableErr.TableName,
			Description: tableErr.Description,
			Code:        tableErr.Type.String(),
		}
	}
	var propertyErr *manager.ThriftPropertyException
	if errors.As(err, &propertyErr) {
		return &Error{
			Kind:        ErrorInvalidProperty,
			Property:    propertyErr.Property,
			Value:       propertyErr.Value,
			Description: propertyErr.Description,
			Code:        "INVALID_PROPERTY",
		}
	}
	var securityErr *clientgen.ThriftSecurityException
	if errors.As(err, &securityErr) {
		kind := ErrorSecurity
		if securityErr.Code == clientgen.SecurityErrorCode_TABLE_DOESNT_EXIST {
			kind = ErrorTableNotFound
		} else if securityErr.Code == clientgen.SecurityErrorCode_NAMESPACE_DOESNT_EXIST {
			kind = ErrorNamespaceNotFound
		}
		return &Error{Kind: kind, Code: securityErr.Code.String()}
	}
	var inactiveErr *clientgen.ThriftNotActiveServiceException
	if errors.As(err, &inactiveErr) {
		return &Error{
			Kind:        ErrorNotActive,
			Description: inactiveErr.Description,
			Code:        inactiveErr.Serv,
		}
	}
	return err
}

func cloneCredentials(credentials *security.TCredentials) *security.TCredentials {
	if credentials == nil {
		return nil
	}
	clone := *credentials
	clone.Token = append([]byte(nil), credentials.Token...)
	return &clone
}

func cloneArguments(arguments [][]byte) [][]byte {
	cloned := make([][]byte, len(arguments))
	for i := range arguments {
		cloned[i] = append([]byte(nil), arguments[i]...)
	}
	return cloned
}

func cloneOptions(options map[string]string) map[string]string {
	cloned := make(map[string]string, len(options))
	for key, value := range options {
		cloned[key] = value
	}
	return cloned
}

func shouldInvalidateTransport(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// TTransportException is an interface; errors.As takes its address.
	var transportErr thrift.TTransportException
	if errors.As(err, &transportErr) {
		return true
	}
	var protocolErr thrift.TProtocolException
	if errors.As(err, &protocolErr) {
		return true
	}
	var thriftErr thrift.TException
	if !errors.As(err, &thriftErr) {
		return false
	}
	switch thriftErr.TExceptionType() {
	case thrift.TExceptionTypeProtocol, thrift.TExceptionTypeTransport:
		return true
	case thrift.TExceptionTypeApplication, thrift.TExceptionTypeCompiled:
		return false
	default:
		return true
	}
}

// IsRetryableEndpointError reports whether an RPC failed before receiving a
// valid application response and can be retried against another advertised
// ClientService endpoint.
func IsRetryableEndpointError(err error) bool {
	if err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var managerErr *Error
	if errors.As(err, &managerErr) {
		return false
	}
	var transportErr thrift.TTransportException
	if errors.As(err, &transportErr) {
		return true
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}
