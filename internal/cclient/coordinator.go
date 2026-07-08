//go:build !embed

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package cclient

import (
	"errors"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/protocol"
	"github.com/phrocker/shoal/internal/thrift/gen/compactioncoordinator"
)

// coordinatorServiceName is the multiplex name the manager registers
// CompactionCoordinatorService under. From core/.../rpc/clients/
// ThriftClientTypes.java: COORDINATOR = ...ThriftClient("coordinator").
// The coordinator runs inside the manager process; the manager's
// TMultiplexedProcessor demuxes on this name.
const coordinatorServiceName = "coordinator"

// CoordinatorClient is a connected Thrift client to the
// CompactionCoordinatorService — the side of Bet 1 that doles out
// compaction jobs to external compactors. Construct with DialCoordinator;
// close with Close.
//
// Wire layering matches scanclient.Dial:
// TSocket → TFramedTransport → AccumuloProtocol(TCompactProtocol),
// multiplexed under "coordinator".
type CoordinatorClient struct {
	transport thrift.TTransport
	raw       *compactioncoordinator.CompactionCoordinatorServiceClient
}

// DialCoordinator opens a connection to the compaction coordinator at
// addr (host:port). The coordinator address is published in the manager's
// ServiceLock data in ZooKeeper under ThriftService.COORDINATOR; resolving
// it from ZK is the caller's job (see cmd/shoal-compactor — currently a
// flag, ZK lock-data parsing is the documented follow-up).
func DialCoordinator(addr, instanceID, accumuloVersion string) (*CoordinatorClient, error) {
	if addr == "" {
		return nil, errors.New("cclient: empty coordinator addr")
	}
	if instanceID == "" {
		return nil, errors.New("cclient: empty instanceID")
	}
	if accumuloVersion == "" {
		return nil, errors.New("cclient: empty accumuloVersion")
	}

	socket := thrift.NewTSocketConf(addr, &thrift.TConfiguration{})
	framed := thrift.NewTFramedTransportConf(socket, &thrift.TConfiguration{})
	if err := framed.Open(); err != nil {
		return nil, fmt.Errorf("cclient: open transport to %s: %w", addr, err)
	}

	proto := protocol.NewClientFactory(instanceID, accumuloVersion).GetProtocol(framed)
	muxed := thrift.NewTMultiplexedProtocol(proto, coordinatorServiceName)
	raw := compactioncoordinator.NewCompactionCoordinatorServiceClient(
		thrift.NewTStandardClient(muxed, muxed))

	return &CoordinatorClient{transport: framed, raw: raw}, nil
}

// Close terminates the underlying transport.
func (c *CoordinatorClient) Close() error {
	return c.transport.Close()
}

// Raw returns the generated Thrift client for the full coordinator
// surface (getCompactionJob, compactionCompleted, compactionFailed,
// updateCompactionStatus, …).
func (c *CoordinatorClient) Raw() *compactioncoordinator.CompactionCoordinatorServiceClient {
	return c.raw
}
