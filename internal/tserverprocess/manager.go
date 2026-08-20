package tserverprocess

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/protocol"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/manager"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/tserver"
	"github.com/phrocker/shoal/internal/tserverrpc"
)

const managerServiceName = "mgr"

type AddressResolver interface {
	ManagerAddress(context.Context) (string, error)
}

type ReportConnector struct {
	Resolver        AddressResolver
	Credentials     *security.TCredentials
	InstanceID      string
	AccumuloVersion string
	ConnectTimeout  time.Duration
	RPCTimeout      time.Duration
}

func (c ReportConnector) Connect(ctx context.Context) (tserverrpc.ReportClient, error) {
	if c.Resolver == nil || c.Credentials == nil || c.InstanceID == "" || c.AccumuloVersion == "" {
		return nil, errors.New("tserverprocess: incomplete manager report connector")
	}
	address, err := c.Resolver.ManagerAddress(ctx)
	if err != nil {
		return nil, err
	}
	connectTimeout := c.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	rpcTimeout := c.RPCTimeout
	if rpcTimeout <= 0 {
		rpcTimeout = 30 * time.Second
	}
	conn, err := (&net.Dialer{Timeout: connectTimeout}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dial manager %s: %w", address, err)
	}
	conf := &thrift.TConfiguration{ConnectTimeout: connectTimeout, SocketTimeout: rpcTimeout}
	socket := thrift.NewTSocketFromConnConf(conn, conf)
	framed := thrift.NewTFramedTransportConf(socket, conf)
	proto := protocol.NewClientFactory(c.InstanceID, c.AccumuloVersion).GetProtocol(framed)
	mux := thrift.NewTMultiplexedProtocol(proto, managerServiceName)
	raw := manager.NewManagerClientServiceClient(thrift.NewTStandardClient(mux, mux))
	return &reportClient{transport: framed, raw: raw, credentials: cloneCredentials(c.Credentials)}, nil
}

type reportClient struct {
	transport   thrift.TTransport
	raw         *manager.ManagerClientServiceClient
	credentials *security.TCredentials
}

func (c *reportClient) ReportTabletStatus(
	ctx context.Context,
	server string,
	state manager.TabletLoadState,
	extent tserver.Extent,
) error {
	return c.raw.ReportTabletStatus(ctx, nil, c.credentials, server, state, &data.TKeyExtent{
		Table: []byte(extent.TableID), PrevEndRow: extent.PrevEndRow, EndRow: extent.EndRow,
	})
}

func (c *reportClient) Close() error { return c.transport.Close() }

func cloneCredentials(in *security.TCredentials) *security.TCredentials {
	if in == nil {
		return nil
	}
	out := *in
	out.Token = append([]byte(nil), in.Token...)
	return &out
}
