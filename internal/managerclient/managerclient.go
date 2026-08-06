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

const fateServiceName = "fate"

type Operation int

const (
	TableCreate Operation = iota
	TableDelete
	TableRename
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

type ErrorKind int

const (
	ErrorUnknown ErrorKind = iota
	ErrorTableExists
	ErrorTableNotFound
	ErrorNamespaceExists
	ErrorNamespaceNotFound
	ErrorInvalidName
	ErrorSecurity
	ErrorNotActive
)

type Error struct {
	Kind        ErrorKind
	TableID     string
	TableName   string
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

type Pooled struct {
	pool            *transportpool.Pool
	instanceID      string
	accumuloVersion string
	dialTimeout     time.Duration

	mu          sync.RWMutex
	credentials *security.TCredentials
	closed      bool

	dial      transportpool.DialFunc
	newClient func(io.Closer) (fateRPC, error)
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
		credentials:     cloneCredentials(credentials),
	}
	p.dial = p.dialThrift
	p.newClient = p.newThriftRPC
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
			finishCtx := context.WithoutCancel(ctx)
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

func withClient[T any](
	p *Pooled,
	ctx context.Context,
	address string,
	call func(fateRPC) (T, error),
) (result T, err error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	key := transportpool.Key{
		Address:         address,
		Service:         fateServiceName,
		InstanceID:      p.instanceID,
		ProtocolVersion: p.accumuloVersion,
	}
	lease, err := p.pool.Acquire(ctx, key, p.dial)
	if err != nil {
		return result, err
	}
	client, err := p.newClient(lease.Transport())
	if err != nil {
		return result, errors.Join(err, lease.Invalidate())
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(err, lease.Close())
	}
	result, rpcErr := call(client)
	var cleanupErr error
	if isWireFailure(rpcErr) {
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
	var securityErr *clientgen.ThriftSecurityException
	if errors.As(err, &securityErr) {
		kind := ErrorSecurity
		if securityErr.Code == clientgen.SecurityErrorCode_TABLE_DOESNT_EXIST {
			kind = ErrorTableNotFound
		} else if securityErr.Code == clientgen.SecurityErrorCode_NAMESPACE_DOESNT_EXIST {
			kind = ErrorNamespaceNotFound
		}
		return &Error{Kind: kind, Description: securityErr.User, Code: securityErr.Code.String()}
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

func isWireFailure(err error) bool {
	if err == nil {
		return false
	}
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
