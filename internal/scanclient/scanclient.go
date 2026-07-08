// Package scanclient is a thin client wrapper around Accumulo's Thrift
// TabletScanClientService.StartScan. It handles transport+protocol setup
// (TFramedTransport + AccumuloProtocol) and provides a SimpleScan
// convenience for metadata-style scans where most StartScan parameters
// take obvious defaults.
//
// Single-shot only — no startMultiScan / continueMultiScan / session state.
// Reference: sharkbite src/interconnect/accumulo/AccumuloServerOne.cpp:181-247
// for the invocation shape; the V0 spec calls out the single-shot constraint.
package scanclient

import (
	"context"
	"errors"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/protocol"
	clientpkg "github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletscan"
)

// defaultBatchSize is the StartScan batch size when SimpleScanRequest leaves
// it zero. Mirrors sharkbite's default (AccumuloServerOne.cpp:181).
const defaultBatchSize int32 = 1024

// scanServiceName is the multiplex name Accumulo registers
// TabletScanClientService under on tservers. From core/.../rpc/clients/
// ThriftClientTypes.java: TABLET_SCAN = ...ThriftClient("scan"). Without
// this, the server's TMultiplexedProcessor returns
// "Service name not found in message name".
const scanServiceName = "scan"

// Client is a connected, ready-to-issue Thrift scan client. Construct with
// Dial; close with Close.
type Client struct {
	transport thrift.TTransport
	raw       *tabletscan.TabletScanClientServiceClient
}

// Dial opens a Thrift connection to a tserver speaking
// TabletScanClientService and returns a Client. Wire layering:
// TSocket -> TFramedTransport -> AccumuloProtocol(TCompactProtocol).
func Dial(addr, instanceID, accumuloVersion string) (*Client, error) {
	if addr == "" {
		return nil, errors.New("scanclient: empty addr")
	}
	if instanceID == "" {
		return nil, errors.New("scanclient: empty instanceID")
	}
	if accumuloVersion == "" {
		return nil, errors.New("scanclient: empty accumuloVersion")
	}

	socket := thrift.NewTSocketConf(addr, &thrift.TConfiguration{})
	framed := thrift.NewTFramedTransportConf(socket, &thrift.TConfiguration{})
	if err := framed.Open(); err != nil {
		return nil, fmt.Errorf("scanclient: open transport to %s: %w", addr, err)
	}

	proto := protocol.NewClientFactory(instanceID, accumuloVersion).GetProtocol(framed)
	muxed := thrift.NewTMultiplexedProtocol(proto, scanServiceName)
	raw := tabletscan.NewTabletScanClientServiceClient(thrift.NewTStandardClient(muxed, muxed))

	return &Client{transport: framed, raw: raw}, nil
}

// Close terminates the underlying transport.
func (c *Client) Close() error {
	return c.transport.Close()
}

// Raw returns the generated Thrift client for callers that need access to
// the full StartScan parameter surface.
func (c *Client) Raw() *tabletscan.TabletScanClientServiceClient { return c.raw }

// SimpleScanRequest is the minimal set of fields a metadata-style scan
// needs. Other StartScan parameters take V0-appropriate defaults.
type SimpleScanRequest struct {
	Credentials    *security.TCredentials
	Extent         *data.TKeyExtent
	Range          *data.TRange
	Authorizations [][]byte // may be nil
	BatchSize      int32    // defaults to 1024 when zero
}

// SimpleScan issues a single StartScan with V0 defaults. It does not
// continue past the first batch — callers needing more rows must use
// Raw().StartScan plus continueScan/closeScan, or wait for V1+ where the
// fold/continue path lands.
//
// After receiving the InitialScan, SimpleScan fires-and-forgets a
// CloseScan with the returned ScanID so the tserver releases the
// server-side scan session immediately rather than waiting for its idle
// timeout. Without this, every metadata-walk tick leaks a scan that
// shows up IDLE in `listscans` for minutes; over time the tserver's
// scan-session table grows and queues new scans behind the dead ones.
// Errors from the close call are logged-and-swallowed because they
// don't affect correctness — the next session timeout cleans up.
func (c *Client) SimpleScan(ctx context.Context, req SimpleScanRequest) (*data.InitialScan, error) {
	if req.Credentials == nil {
		return nil, errors.New("scanclient: nil Credentials")
	}
	if req.Extent == nil {
		return nil, errors.New("scanclient: nil Extent")
	}
	if req.Range == nil {
		return nil, errors.New("scanclient: nil Range")
	}
	batchSize := req.BatchSize
	if batchSize == 0 {
		batchSize = defaultBatchSize
	}

	scan, err := c.raw.StartScan(
		ctx,
		clientpkg.NewTInfo(),    // empty TInfo — tracing wires in later
		req.Credentials,         // 1
		req.Extent,              // 2
		req.Range,               // 3
		nil,                     // 4 columns: nil = scan all
		batchSize,               // 5
		nil,                     // 6 ssiList: no server-side iterators in V0
		nil,                     // 7 ssio
		req.Authorizations,      // 8
		false,                   // 9 waitForWrites
		false,                   // 10 isolated
		int64(defaultBatchSize), // 12 readaheadThreshold
		nil,                     // 13 samplerConfig
		0,                       // 14 batchTimeOut
		"",                      // 15 classLoaderContext
		nil,                     // 16 executionHints
		0,                       // 17 busyTimeout
	)
	if err != nil {
		return nil, err
	}
	if scan != nil && scan.ScanID != 0 {
		if cerr := c.raw.CloseScan(ctx, clientpkg.NewTInfo(), scan.ScanID); cerr != nil {
			// Server-side session will idle-timeout regardless; this is
			// just hygiene. Log at debug rather than warn so a noisy
			// run-down (e.g. on shutdown ctx-cancel) doesn't drown
			// real signals.
			//
			// (We can't reach a structured logger from here without
			// threading one through; the package-level log import
			// would be the next step if this needs visibility.)
			_ = cerr
		}
	}
	return scan, nil
}
