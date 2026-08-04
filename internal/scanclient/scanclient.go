// Package scanclient implements the internal TabletScanClientService RPC
// boundary. It owns transport/protocol setup and keeps generated service
// clients behind a small start/continue/close lifecycle.
package scanclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/protocol"
	clientpkg "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletscan"
)

const defaultBatchSize int32 = 1024

// scanServiceName is the multiplex name Accumulo registers
// TabletScanClientService under on tservers.
const scanServiceName = "scan"

// Client is a connected, ready-to-issue Thrift scan client.
type Client struct {
	transport thrift.TTransport
	raw       *tabletscan.TabletScanClientServiceClient
	rpc       scanRPC
}

// Dial opens a Thrift connection to a tserver speaking
// TabletScanClientService.
func Dial(addr, instanceID, accumuloVersion string) (*Client, error) {
	return DialContext(context.Background(), addr, instanceID, accumuloVersion, 0)
}

// DialContext is Dial with context-aware TCP establishment and a connect
// timeout. Once connected, cancellation is checked before each RPC; Apache
// Thrift does not interrupt an already-blocked socket read from ctx alone.
func DialContext(
	ctx context.Context,
	addr, instanceID, accumuloVersion string,
	dialTimeout time.Duration,
) (*Client, error) {
	if addr == "" {
		return nil, errors.New("scanclient: empty addr")
	}
	if instanceID == "" {
		return nil, errors.New("scanclient: empty instanceID")
	}
	if accumuloVersion == "" {
		return nil, errors.New("scanclient: empty accumuloVersion")
	}
	if dialTimeout < 0 {
		return nil, errors.New("scanclient: negative dial timeout")
	}

	framed, err := dialTransport(ctx, addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("scanclient: open transport to %s: %w", addr, err)
	}
	raw := newThriftClient(framed, instanceID, accumuloVersion)
	return &Client{
		transport: framed,
		raw:       raw,
		rpc:       thriftScanRPC{raw: raw},
	}, nil
}

// Close terminates the underlying transport.
func (c *Client) Close() error {
	return c.transport.Close()
}

// Raw returns the generated Thrift client for legacy internal callers.
func (c *Client) Raw() *tabletscan.TabletScanClientServiceClient { return c.raw }

// StartRequest is the internal input for a single-tablet startScan RPC.
type StartRequest struct {
	Credentials        *security.TCredentials
	Extent             *data.TKeyExtent
	Range              *data.TRange
	Columns            []*data.TColumn
	BatchSize          int32
	Iterators          []*data.IterInfo
	IteratorOptions    map[string]map[string]string
	Authorizations     [][]byte
	WaitForWrites      bool
	Isolated           bool
	ReadaheadThreshold int64
	SamplerConfig      *tabletscan.TSamplerConfiguration
	BatchTimeout       int64
	ClassLoaderContext string
	ExecutionHints     map[string]string
	BusyTimeout        int64
}

// Start begins a tablet scan.
func (c *Client) Start(ctx context.Context, req StartRequest) (*data.InitialScan, error) {
	if err := validateStartRequest(req); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.rpc.Start(ctx, req)
}

// Continue fetches the next batch for scanID.
func (c *Client) Continue(
	ctx context.Context,
	scanID data.ScanID,
	busyTimeout int64,
) (*data.ScanResult_, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.rpc.Continue(ctx, scanID, busyTimeout)
}

// CloseScan releases the server-side scan session.
func (c *Client) CloseScan(ctx context.Context, scanID data.ScanID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.rpc.Close(ctx, scanID)
}

// SimpleScanRequest is the minimal set of fields a metadata-style scan needs.
type SimpleScanRequest struct {
	Credentials    *security.TCredentials
	Extent         *data.TKeyExtent
	Range          *data.TRange
	Authorizations [][]byte
	BatchSize      int32
}

// CleanupError reports that a scan produced a usable initial result but its
// server-side session could not be closed. Callers may consume the result while
// still logging or otherwise surfacing this cleanup failure.
type CleanupError struct {
	ScanID data.ScanID
	Err    error
}

func (e *CleanupError) Error() string {
	return fmt.Sprintf("scanclient: close scan %d: %v", e.ScanID, e.Err)
}

func (e *CleanupError) Unwrap() error { return e.Err }

// SimpleScan starts a metadata-style scan and closes its server-side session.
// A close failure is returned with the successful initial result.
func (c *Client) SimpleScan(ctx context.Context, req SimpleScanRequest) (*data.InitialScan, error) {
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	scan, err := c.Start(ctx, StartRequest{
		Credentials:        req.Credentials,
		Extent:             req.Extent,
		Range:              req.Range,
		BatchSize:          batchSize,
		Authorizations:     req.Authorizations,
		ReadaheadThreshold: int64(defaultBatchSize),
	})
	if err != nil {
		return nil, err
	}
	if scan != nil && scan.ScanID != 0 {
		if closeErr := c.CloseScan(ctx, scan.ScanID); closeErr != nil {
			return scan, &CleanupError{ScanID: scan.ScanID, Err: closeErr}
		}
	}
	return scan, nil
}

type scanRPC interface {
	Start(context.Context, StartRequest) (*data.InitialScan, error)
	Continue(context.Context, data.ScanID, int64) (*data.ScanResult_, error)
	Close(context.Context, data.ScanID) error
}

type thriftScanRPC struct {
	raw *tabletscan.TabletScanClientServiceClient
}

func (c thriftScanRPC) Start(ctx context.Context, req StartRequest) (*data.InitialScan, error) {
	return c.raw.StartScan(
		ctx,
		clientpkg.NewTInfo(),
		req.Credentials,
		req.Extent,
		req.Range,
		req.Columns,
		req.BatchSize,
		req.Iterators,
		req.IteratorOptions,
		req.Authorizations,
		req.WaitForWrites,
		req.Isolated,
		req.ReadaheadThreshold,
		req.SamplerConfig,
		req.BatchTimeout,
		req.ClassLoaderContext,
		req.ExecutionHints,
		req.BusyTimeout,
	)
}

func (c thriftScanRPC) Continue(
	ctx context.Context,
	scanID data.ScanID,
	busyTimeout int64,
) (*data.ScanResult_, error) {
	return c.raw.ContinueScan(ctx, clientpkg.NewTInfo(), scanID, busyTimeout)
}

func (c thriftScanRPC) Close(ctx context.Context, scanID data.ScanID) error {
	return c.raw.CloseScan(ctx, clientpkg.NewTInfo(), scanID)
}

func validateStartRequest(req StartRequest) error {
	switch {
	case req.Credentials == nil:
		return errors.New("scanclient: nil Credentials")
	case req.Extent == nil:
		return errors.New("scanclient: nil Extent")
	case req.Range == nil:
		return errors.New("scanclient: nil Range")
	default:
		return nil
	}
}

func dialTransport(ctx context.Context, addr string, timeout time.Duration) (thrift.TTransport, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return dialTransportWith(ctx, addr, dialer.DialContext)
}

func dialTransportWith(
	ctx context.Context,
	addr string,
	dial func(context.Context, string, string) (net.Conn, error),
) (thrift.TTransport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conn, err := dial(ctx, "tcp", addr)
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

func newThriftClient(
	transport thrift.TTransport,
	instanceID, accumuloVersion string,
) *tabletscan.TabletScanClientServiceClient {
	proto := protocol.NewClientFactory(instanceID, accumuloVersion).GetProtocol(transport)
	muxed := thrift.NewTMultiplexedProtocol(proto, scanServiceName)
	return tabletscan.NewTabletScanClientServiceClient(thrift.NewTStandardClient(muxed, muxed))
}
