package managerclient

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	clientgen "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/manager"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/transportpool"
)

func TestPooledExecuteLifecycleAndReuse(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var dials atomic.Int32
	rpc := &fakeFateRPC{id: fateID{Type: 1, UUID: "abc"}}
	pooled.dial = func(_ context.Context, key transportpool.Key) (io.Closer, error) {
		dials.Add(1)
		want := transportpool.Key{
			Address:         "manager:9997",
			Service:         "fate",
			InstanceID:      "uuid-1",
			ProtocolVersion: "4.0.0-SNAPSHOT",
		}
		if key != want {
			t.Fatalf("key = %+v, want %+v", key, want)
		}
		return &fakeTransport{rpc: rpc}, nil
	}
	pooled.newClient = clientFromFakeTransport

	for range 2 {
		if err := pooled.Execute(context.Background(), "manager:9997", createRequest("events")); err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
	if rpc.begin.Load() != 2 || rpc.execute.Load() != 2 || rpc.waitCalls.Load() != 2 || rpc.finish.Load() != 2 {
		t.Fatalf("calls begin/execute/wait/finish = %d/%d/%d/%d",
			rpc.begin.Load(), rpc.execute.Load(), rpc.waitCalls.Load(), rpc.finish.Load())
	}
	if got := string(rpc.request.Arguments[0]); got != "events" {
		t.Fatalf("table argument = %q", got)
	}
	if rpc.instance != FateUser {
		t.Fatalf("FATE instance = %v, want user", rpc.instance)
	}
}

func TestPooledFinishesAfterOperationFailure(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	rpc := &fakeFateRPC{
		id: fateID{Type: 1, UUID: "abc"},
		executeErr: &clientgen.ThriftTableOperationException{
			Type:      clientgen.TableOperationExceptionType_EXISTS,
			TableName: "events",
		},
	}
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		return &fakeTransport{rpc: rpc}, nil
	}
	pooled.newClient = clientFromFakeTransport

	err := pooled.Execute(context.Background(), "manager:9997", createRequest("events"))
	var managerErr *Error
	if !errors.As(err, &managerErr) || managerErr.Kind != ErrorTableExists {
		t.Fatalf("error = %#v, want ErrorTableExists", err)
	}
	if rpc.finish.Load() != 1 {
		t.Fatalf("finish calls = %d, want 1", rpc.finish.Load())
	}
}

func TestPooledFinishesWithCancelledContext(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	rpc := &fakeFateRPC{id: fateID{Type: 1, UUID: "abc"}, wait: func() error {
		cancel()
		return context.Canceled
	}}
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		return &fakeTransport{rpc: rpc}, nil
	}
	pooled.newClient = clientFromFakeTransport

	if err := pooled.Execute(ctx, "manager:9997", createRequest("events")); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if rpc.finish.Load() != 1 || rpc.finishContextErr != nil {
		t.Fatalf("finish calls/context = %d/%v", rpc.finish.Load(), rpc.finishContextErr)
	}
}

func TestPooledBoundsFinishCleanup(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()
	pooled.finishTimeout = 20 * time.Millisecond

	rpc := &fakeFateRPC{
		id: fateID{Type: 1, UUID: "abc"},
		finishFunc: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		return &fakeTransport{rpc: rpc}, nil
	}
	pooled.newClient = clientFromFakeTransport

	started := time.Now()
	err := pooled.Execute(context.Background(), "manager:9997", createRequest("events"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded finish took %v", elapsed)
	}
}

func TestPooledInvalidatesWireFailureAndCloseRace(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var dials atomic.Int32
	var first *fakeTransport
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		n := dials.Add(1)
		rpc := &fakeFateRPC{id: fateID{Type: 1, UUID: "abc"}}
		if n == 1 {
			rpc.beginErr = thrift.NewTTransportExceptionFromError(errors.New("reset"))
		}
		transport := &fakeTransport{rpc: rpc}
		if n == 1 {
			first = transport
		}
		return transport, nil
	}
	pooled.newClient = clientFromFakeTransport

	if err := pooled.Execute(context.Background(), "manager:9997", createRequest("events")); err == nil {
		t.Fatal("expected wire error")
	}
	if first.closes.Load() != 1 {
		t.Fatalf("invalidated closes = %d, want 1", first.closes.Load())
	}
	if err := pooled.Execute(context.Background(), "manager:9997", createRequest("events")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = pooled.Close()
		}()
	}
	wg.Wait()
	if err := pooled.Execute(context.Background(), "manager:9997", createRequest("events")); err == nil {
		t.Fatal("expected closed error")
	}
}

func TestPooledConcurrentOperationsUseExclusiveTransports(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var active atomic.Int32
	var maxActive atomic.Int32
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(8)
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		return &fakeTransport{rpc: &fakeFateRPC{
			id: fateID{Type: 1, UUID: "abc"},
			beginHook: func() {
				now := active.Add(1)
				for {
					max := maxActive.Load()
					if now <= max || maxActive.CompareAndSwap(max, now) {
						break
					}
				}
				started.Done()
				<-release
			},
			finishHook: func() {
				active.Add(-1)
			},
		}}, nil
	}
	pooled.newClient = clientFromFakeTransport

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pooled.Execute(context.Background(), "manager:9997", createRequest("events")); err != nil {
				t.Errorf("Execute: %v", err)
			}
		}()
	}
	started.Wait()
	close(release)
	wg.Wait()
	if maxActive.Load() < 2 {
		t.Fatalf("max concurrent transports = %d, want at least 2", maxActive.Load())
	}
}

func TestPooledTablePropertyMutationsUseManagerServiceAndReuse(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var dials atomic.Int32
	rpc := &fakeManagerRPC{}
	pooled.dial = func(_ context.Context, key transportpool.Key) (io.Closer, error) {
		dials.Add(1)
		want := transportpool.Key{
			Address:         "manager:9997",
			Service:         "mgr",
			InstanceID:      "uuid-1",
			ProtocolVersion: "4.0.0-SNAPSHOT",
		}
		if key != want {
			t.Fatalf("key = %+v, want %+v", key, want)
		}
		return &fakeTransport{manager: rpc}, nil
	}
	pooled.newManagerClient = managerFromFakeTransport

	if err := pooled.SetTableProperty(
		context.Background(),
		"manager:9997",
		"events",
		"table.file.compress.type",
		"",
	); err != nil {
		t.Fatal(err)
	}
	if err := pooled.RemoveTableProperty(
		context.Background(),
		"manager:9997",
		"events",
		"table.file.compress.type",
	); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
	if rpc.setCalls.Load() != 1 || rpc.removeCalls.Load() != 1 {
		t.Fatalf("set/remove calls = %d/%d, want 1/1", rpc.setCalls.Load(), rpc.removeCalls.Load())
	}
	if rpc.tableName != "events" || rpc.property != "table.file.compress.type" || rpc.value != "" {
		t.Fatalf("set request = %q/%q/%q", rpc.tableName, rpc.property, rpc.value)
	}
}

func TestPooledTablePropertyWireFailureEvictsTransport(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var dials atomic.Int32
	var first *fakeTransport
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		n := dials.Add(1)
		rpc := &fakeManagerRPC{}
		if n == 1 {
			rpc.setErr = thrift.NewTTransportExceptionFromError(errors.New("reset"))
		}
		transport := &fakeTransport{manager: rpc}
		if n == 1 {
			first = transport
		}
		return transport, nil
	}
	pooled.newManagerClient = managerFromFakeTransport

	if err := pooled.SetTableProperty(
		context.Background(),
		"manager:9997",
		"events",
		"table.file.compress.type",
		"gz",
	); err == nil {
		t.Fatal("expected wire error")
	}
	if first.closes.Load() != 1 {
		t.Fatalf("invalidated closes = %d, want 1", first.closes.Load())
	}
	if err := pooled.RemoveTableProperty(
		context.Background(),
		"manager:9997",
		"events",
		"table.file.compress.type",
	); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}

func TestPooledTablePropertyCancellationAndClose(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var dials atomic.Int32
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		dials.Add(1)
		return &fakeTransport{manager: &fakeManagerRPC{}}, nil
	}
	pooled.newManagerClient = managerFromFakeTransport

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := pooled.SetTableProperty(
		ctx,
		"manager:9997",
		"events",
		"table.file.compress.type",
		"gz",
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled error = %v", err)
	}
	if dials.Load() != 0 {
		t.Fatalf("canceled operation dials = %d, want 0", dials.Load())
	}
	if err := pooled.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pooled.RemoveTableProperty(
		context.Background(),
		"manager:9997",
		"events",
		"table.file.compress.type",
	); err == nil {
		t.Fatal("expected closed error")
	}
}

func TestPooledFlushTableLifecycleAndWaitModes(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	var dials atomic.Int32
	rpc := &fakeManagerRPC{flushID: 41}
	pooled.dial = func(_ context.Context, key transportpool.Key) (io.Closer, error) {
		dials.Add(1)
		if key.Service != managerServiceName {
			t.Fatalf("service = %q, want %q", key.Service, managerServiceName)
		}
		return &fakeTransport{manager: rpc}, nil
	}
	pooled.newManagerClient = managerFromFakeTransport

	if err := pooled.FlushTable(context.Background(), "manager:9997", "1", false); err != nil {
		t.Fatal(err)
	}
	if err := pooled.FlushTable(context.Background(), "manager:9997", "1", true); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dials = %d, want 1", dials.Load())
	}
	if rpc.initiateCalls.Load() != 2 || rpc.flushWaitCalls.Load() != 2 {
		t.Fatalf(
			"initiate/wait calls = %d/%d, want 2/2",
			rpc.initiateCalls.Load(),
			rpc.flushWaitCalls.Load(),
		)
	}
	if rpc.flushTableID != "1" || rpc.waitFlushID != 41 {
		t.Fatalf("flush table/ID = %q/%d", rpc.flushTableID, rpc.waitFlushID)
	}
	if !slices.Equal(rpc.maxLoops, []int64{noWaitFlushMaxLoops, waitForFlushMaxLoops}) {
		t.Fatalf("max loops = %v", rpc.maxLoops)
	}
}

func TestPooledFlushTableStopsAfterInitiateFailure(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	rpc := &fakeManagerRPC{
		initiateErr: &clientgen.ThriftTableOperationException{
			Type:    clientgen.TableOperationExceptionType_NOTFOUND,
			TableId: "1",
		},
	}
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		return &fakeTransport{manager: rpc}, nil
	}
	pooled.newManagerClient = managerFromFakeTransport

	err := pooled.FlushTable(context.Background(), "manager:9997", "1", true)
	var managerErr *Error
	if !errors.As(err, &managerErr) ||
		managerErr.Kind != ErrorTableNotFound ||
		managerErr.TableID != "1" {
		t.Fatalf("error = %#v, want table-not-found for ID 1", err)
	}
	if rpc.flushWaitCalls.Load() != 0 {
		t.Fatalf("wait calls = %d, want 0", rpc.flushWaitCalls.Load())
	}
}

func TestPooledFlushCancellationEvictsTransport(t *testing.T) {
	pooled, pool := newTestPooled(t)
	defer pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var dials atomic.Int32
	var first *fakeTransport
	pooled.dial = func(context.Context, transportpool.Key) (io.Closer, error) {
		n := dials.Add(1)
		rpc := &fakeManagerRPC{flushID: 41}
		if n == 1 {
			rpc.waitFlush = func(context.Context) error {
				cancel()
				return context.Canceled
			}
		}
		transport := &fakeTransport{manager: rpc}
		if n == 1 {
			first = transport
		}
		return transport, nil
	}
	pooled.newManagerClient = managerFromFakeTransport

	if err := pooled.FlushTable(ctx, "manager:9997", "1", true); !errors.Is(err, context.Canceled) {
		t.Fatalf("flush error = %v, want context canceled", err)
	}
	if first.closes.Load() != 1 {
		t.Fatalf("invalidated closes = %d, want 1", first.closes.Load())
	}
	if err := pooled.SetTableProperty(
		context.Background(),
		"manager:9997",
		"events",
		"table.file.compress.type",
		"gz",
	); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 2 {
		t.Fatalf("dials = %d, want 2", dials.Load())
	}
}

func TestWaitForFlushNilRowsAreAbsentOnWire(t *testing.T) {
	nilRows := manager.NewManagerClientServiceWaitForFlushArgs()
	nilRows.Tinfo = &clientgen.TInfo{}
	nilRows.Credentials = &security.TCredentials{}
	nilRows.TableName = "1"
	nilRows.FlushID = 41
	nilRows.MaxLoops = 1
	if got, want := thriftFieldIDs(t, nilRows), []int16{1, 2, 3, 6, 7}; !slices.Equal(got, want) {
		t.Fatalf("nil-row field IDs = %v, want %v", got, want)
	}

	emptyRows := manager.NewManagerClientServiceWaitForFlushArgs()
	emptyRows.Tinfo = &clientgen.TInfo{}
	emptyRows.Credentials = &security.TCredentials{}
	emptyRows.TableName = "1"
	emptyRows.StartRow = []byte{}
	emptyRows.EndRow = []byte{}
	emptyRows.FlushID = 41
	emptyRows.MaxLoops = 1
	if got, want := thriftFieldIDs(t, emptyRows), []int16{1, 2, 3, 4, 5, 6, 7}; !slices.Equal(got, want) {
		t.Fatalf("empty-row field IDs = %v, want %v", got, want)
	}
}

func TestMapRPCError(t *testing.T) {
	tests := []struct {
		err  error
		kind ErrorKind
	}{
		{&clientgen.ThriftTableOperationException{
			Type: clientgen.TableOperationExceptionType_EXISTS,
		}, ErrorTableExists},
		{&clientgen.ThriftTableOperationException{
			Type: clientgen.TableOperationExceptionType_NOTFOUND,
		}, ErrorTableNotFound},
		{&clientgen.ThriftTableOperationException{
			Type: clientgen.TableOperationExceptionType_NAMESPACE_NOTFOUND,
		}, ErrorNamespaceNotFound},
		{&clientgen.ThriftTableOperationException{
			Type: clientgen.TableOperationExceptionType_INVALID_NAME,
		}, ErrorInvalidName},
		{&manager.ThriftPropertyException{
			Property:    "table.invalid",
			Value:       "x",
			Description: "property is not valid",
		}, ErrorInvalidProperty},
		{&clientgen.ThriftSecurityException{
			Code: clientgen.SecurityErrorCode_PERMISSION_DENIED,
		}, ErrorSecurity},
		{&clientgen.ThriftSecurityException{
			Code: clientgen.SecurityErrorCode_TABLE_DOESNT_EXIST,
		}, ErrorTableNotFound},
		{&clientgen.ThriftSecurityException{
			Code: clientgen.SecurityErrorCode_NAMESPACE_DOESNT_EXIST,
		}, ErrorNamespaceNotFound},
		{&clientgen.ThriftNotActiveServiceException{}, ErrorNotActive},
	}
	for _, tt := range tests {
		var got *Error
		if err := mapRPCError(tt.err); !errors.As(err, &got) || got.Kind != tt.kind {
			t.Fatalf("mapRPCError(%T) = %#v, want kind %d", tt.err, err, tt.kind)
		}
	}
	securityErr := mapRPCError(&clientgen.ThriftSecurityException{
		User: "root",
		Code: clientgen.SecurityErrorCode_PERMISSION_DENIED,
	})
	if got := securityErr.Error(); got != "managerclient: PERMISSION_DENIED" {
		t.Fatalf("security error = %q, want code without principal", got)
	}
	var propertyErr *Error
	if err := mapRPCError(&manager.ThriftPropertyException{
		Property:    "table.invalid",
		Value:       "x",
		Description: "property is not valid",
	}); !errors.As(err, &propertyErr) ||
		propertyErr.Property != "table.invalid" ||
		propertyErr.Value != "x" ||
		propertyErr.Description != "property is not valid" {
		t.Fatalf("property error = %#v", err)
	}
}

func newTestPooled(t *testing.T) (*Pooled, *transportpool.Pool) {
	t.Helper()
	pool, err := transportpool.New(transportpool.Config{MaxIdlePerEndpoint: 1})
	if err != nil {
		t.Fatal(err)
	}
	pooled, err := NewPooled(pool, "uuid-1", "4.0.0-SNAPSHOT", &security.TCredentials{
		Principal:      "root",
		TokenClassName: "PasswordToken",
		Token:          []byte("secret"),
		InstanceId:     "uuid-1",
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return pooled, pool
}

func createRequest(name string) Request {
	return Request{
		Operation: TableCreate,
		Instance:  FateUser,
		Arguments: [][]byte{[]byte(name), []byte("MILLIS"), []byte("ONLINE"), []byte("HOSTED"), []byte("0")},
		Options:   map[string]string{},
	}
}

type fakeTransport struct {
	rpc     fateRPC
	manager managerRPC
	closes  atomic.Int32
}

func (t *fakeTransport) Close() error {
	t.closes.Add(1)
	return nil
}

func clientFromFakeTransport(transport io.Closer) (fateRPC, error) {
	return transport.(*fakeTransport).rpc, nil
}

func managerFromFakeTransport(transport io.Closer) (managerRPC, error) {
	return transport.(*fakeTransport).manager, nil
}

type fakeManagerRPC struct {
	initiateErr    error
	waitFlushErr   error
	setErr         error
	removeErr      error
	waitFlush      func(context.Context) error
	tableName      string
	property       string
	value          string
	flushTableID   string
	flushID        int64
	waitFlushID    int64
	maxLoops       []int64
	initiateCalls  atomic.Int32
	flushWaitCalls atomic.Int32
	setCalls       atomic.Int32
	removeCalls    atomic.Int32
}

func (r *fakeManagerRPC) InitiateFlush(
	_ context.Context,
	_ *security.TCredentials,
	tableID string,
) (int64, error) {
	r.initiateCalls.Add(1)
	r.flushTableID = tableID
	return r.flushID, r.initiateErr
}

func (r *fakeManagerRPC) WaitForFlush(
	ctx context.Context,
	_ *security.TCredentials,
	tableID string,
	flushID, maxLoops int64,
) error {
	r.flushWaitCalls.Add(1)
	r.flushTableID = tableID
	r.waitFlushID = flushID
	r.maxLoops = append(r.maxLoops, maxLoops)
	if r.waitFlush != nil {
		return r.waitFlush(ctx)
	}
	return r.waitFlushErr
}

func (r *fakeManagerRPC) SetTableProperty(
	_ context.Context,
	_ *security.TCredentials,
	tableName, property, value string,
) error {
	r.setCalls.Add(1)
	r.tableName = tableName
	r.property = property
	r.value = value
	return r.setErr
}

func (r *fakeManagerRPC) RemoveTableProperty(
	_ context.Context,
	_ *security.TCredentials,
	tableName, property string,
) error {
	r.removeCalls.Add(1)
	r.tableName = tableName
	r.property = property
	return r.removeErr
}

func thriftFieldIDs(t *testing.T, value thrift.TStruct) []int16 {
	t.Helper()
	ctx := context.Background()
	buffer := thrift.NewTMemoryBuffer()
	writer := thrift.NewTCompactProtocol(buffer)
	if err := value.Write(ctx, writer); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	reader := thrift.NewTCompactProtocol(buffer)
	if _, err := reader.ReadStructBegin(ctx); err != nil {
		t.Fatal(err)
	}
	var ids []int16
	for {
		_, fieldType, fieldID, err := reader.ReadFieldBegin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if fieldType == thrift.STOP {
			break
		}
		ids = append(ids, fieldID)
		if err := reader.Skip(ctx, fieldType); err != nil {
			t.Fatal(err)
		}
		if err := reader.ReadFieldEnd(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := reader.ReadStructEnd(ctx); err != nil {
		t.Fatal(err)
	}
	return ids
}

type fakeFateRPC struct {
	id         fateID
	beginErr   error
	executeErr error
	wait       func() error
	finishErr  error
	finishFunc func(context.Context) error
	request    Request
	instance   FateInstance
	beginHook  func()
	finishHook func()

	begin            atomic.Int32
	execute          atomic.Int32
	waitCalls        atomic.Int32
	finish           atomic.Int32
	finishContextErr error
}

func (r *fakeFateRPC) Begin(
	_ context.Context,
	_ *security.TCredentials,
	instance FateInstance,
) (fateID, error) {
	r.begin.Add(1)
	r.instance = instance
	if r.beginHook != nil {
		r.beginHook()
	}
	return r.id, r.beginErr
}

func (r *fakeFateRPC) Execute(_ context.Context, _ *security.TCredentials, _ fateID, req Request) error {
	r.execute.Add(1)
	r.request = req
	return r.executeErr
}

func (r *fakeFateRPC) Wait(context.Context, *security.TCredentials, fateID) (string, error) {
	r.waitCalls.Add(1)
	if r.wait != nil {
		return "", r.wait()
	}
	return "", nil
}

func (r *fakeFateRPC) Finish(ctx context.Context, _ *security.TCredentials, _ fateID) error {
	r.finish.Add(1)
	r.finishContextErr = ctx.Err()
	if r.finishHook != nil {
		r.finishHook()
	}
	if r.finishFunc != nil {
		return r.finishFunc(ctx)
	}
	return r.finishErr
}
