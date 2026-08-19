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

package tserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"

	"github.com/google/uuid"
)

// ErrInvalidLockData means the ServiceLockData could not describe a server the
// manager can talk to, so it is refused rather than published. A lock znode
// carrying an address the manager cannot dial is worse than no lock at all:
// the manager would see a live tablet server and route work into a black hole.
var ErrInvalidLockData = errors.New("tserver: invalid service lock data")

// DefaultResourceGroup is the resource group a server belongs to when none is
// configured, matching Accumulo's Constants.DEFAULT_RESOURCE_GROUP_NAME.
const DefaultResourceGroup = "default"

// placeholderAddress is the address Accumulo publishes before a server has
// bound its port. It names no endpoint, so it can never be advertised as one.
const placeholderAddress = "0.0.0.0:0"

// ThriftService names one service advertised on a ServiceLock znode. The
// values mirror ServiceLockData.ThriftService, which is what the manager and
// Accumulo clients match against when they read the lock.
type ThriftService string

// The subset of ThriftService a tablet server can publish. The full enum
// covers manager-side and compaction services too; those are advertised on
// other locks and are not a tablet server's to claim.
const (
	// ServiceClient is the ClientService endpoint scan clients dial. It is
	// the descriptor internal/zk reads when it enumerates live servers.
	ServiceClient ThriftService = "CLIENT"
	// ServiceTabletIngest is the write path: mutations, conditional writes,
	// and the WAL behind them.
	ServiceTabletIngest ThriftService = "TABLET_INGEST"
	// ServiceTabletManagement is the manager-facing lifecycle surface —
	// assignment, unassignment, split, and status.
	ServiceTabletManagement ThriftService = "TABLET_MANAGEMENT"
	// ServiceTabletScan is the scan path against hosted tablets.
	ServiceTabletScan ThriftService = "TABLET_SCAN"
	// ServiceTabletServer is the legacy combined tablet-server endpoint.
	ServiceTabletServer ThriftService = "TSERV"
)

// thriftServiceOrder is the declaration order of ServiceLockData.ThriftService.
// Accumulo holds descriptors in an EnumMap, so a Java-written lock znode lists
// them in this order; encoding in the same order keeps a Shoal-written znode
// byte-comparable with the equivalent Java one.
var thriftServiceOrder = map[ThriftService]int{
	"CLIENT":            0,
	"COORDINATOR":       1,
	"COMPACTOR":         2,
	"FATE_CLIENT":       3,
	"FATE_WORKER":       4,
	"GC":                5,
	"MANAGER":           6,
	"NONE":              7,
	"TABLET_INGEST":     8,
	"TABLET_MANAGEMENT": 9,
	"TABLET_SCAN":       10,
	"TSERV":             11,
}

// tabletServerServices is the descriptor set an Accumulo tablet server
// publishes (TabletServer.announceExistence). It is the set a Shoal process
// must reach before it can stand in for a Java tablet server.
var tabletServerServices = []ThriftService{
	ServiceClient,
	ServiceTabletIngest,
	ServiceTabletManagement,
	ServiceTabletScan,
	ServiceTabletServer,
}

// Known reports whether the service is one Accumulo defines. An unknown name
// is refused rather than published: Accumulo's reader parses the field as an
// enum, so a name it does not know makes the whole lock znode unreadable and
// the server invisible.
func (s ThriftService) Known() bool {
	_, ok := thriftServiceOrder[s]
	return ok
}

// TabletServerServices returns the descriptor set a Java tablet server
// publishes, in Accumulo's enum order.
//
// A Shoal process should advertise only the services it actually implements.
// Publishing the whole set before the matching endpoints exist does not make
// the process a tablet server; it makes the manager route assignments, scans,
// and writes to a server that cannot answer them.
func TabletServerServices() []ThriftService {
	return append([]ThriftService(nil), tabletServerServices...)
}

// ServiceDescriptor is one advertised endpoint on a ServiceLock znode: which
// process it belongs to, which service it speaks, where to reach it, and which
// resource group it serves.
//
// The JSON field names are the wire contract with Accumulo's Gson
// serialization of ServiceLockData.ServiceDescriptorGson.
type ServiceDescriptor struct {
	UUID    string        `json:"uuid"`
	Service ThriftService `json:"service"`
	Address string        `json:"address"`
	Group   string        `json:"group"`
}

// Validate reports whether the descriptor names a reachable service.
func (d ServiceDescriptor) Validate() error {
	if _, err := uuid.Parse(d.UUID); err != nil {
		return fmt.Errorf("%w: server uuid %q: %w", ErrInvalidLockData, d.UUID, err)
	}
	if !d.Service.Known() {
		return fmt.Errorf("%w: unknown service %q", ErrInvalidLockData, d.Service)
	}
	if d.Group == "" {
		return fmt.Errorf("%w: %s descriptor has no resource group", ErrInvalidLockData, d.Service)
	}
	if err := validateAdvertiseAddress(d.Address); err != nil {
		return fmt.Errorf("%w: %s descriptor: %w", ErrInvalidLockData, d.Service, err)
	}
	return nil
}

// validateAdvertiseAddress rejects anything that is not a host:port another
// process could dial.
func validateAdvertiseAddress(address string) error {
	if address == "" {
		return errors.New("empty address")
	}
	if address == placeholderAddress {
		return fmt.Errorf("address %q is the unbound placeholder", address)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("address %q is not host:port: %w", address, err)
	}
	if host == "" {
		return fmt.Errorf("address %q has no host", address)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("address %q has a non-numeric port: %w", address, err)
	}
	if number < 1 || number > 65535 {
		return fmt.Errorf("address %q port %d is out of range", address, number)
	}
	return nil
}

// ServiceLockData is the payload written to a ServiceLock znode: the set of
// services the lock holder advertises.
type ServiceLockData struct {
	Descriptors []ServiceDescriptor
}

// serviceLockDataJSON is the on-the-wire shape, matching Accumulo's
// ServiceLockData.ServiceDescriptorsGson.
type serviceLockDataJSON struct {
	Descriptors []ServiceDescriptor `json:"descriptors"`
}

// TabletServerLockData builds the payload a Shoal tablet server publishes for
// the services it implements, all pointing at one advertise address in one
// resource group under one server UUID — the shape
// TabletServer.announceExistence writes.
//
// An empty group means DefaultResourceGroup. Passing no services is refused:
// a lock that advertises nothing tells the manager a server is alive without
// telling it how to reach anything.
func TabletServerLockData(serverUUID, address, group string, services ...ThriftService) (ServiceLockData, error) {
	if group == "" {
		group = DefaultResourceGroup
	}
	if len(services) == 0 {
		return ServiceLockData{}, fmt.Errorf("%w: no services to advertise", ErrInvalidLockData)
	}
	data := ServiceLockData{Descriptors: make([]ServiceDescriptor, 0, len(services))}
	for _, service := range services {
		data.Descriptors = append(data.Descriptors, ServiceDescriptor{
			UUID:    serverUUID,
			Service: service,
			Address: address,
			Group:   group,
		})
	}
	if err := data.Validate(); err != nil {
		return ServiceLockData{}, err
	}
	return data, nil
}

// Validate reports whether the payload is one Accumulo can read and act on.
//
// A service may appear only once. Accumulo keys descriptors by service in an
// EnumMap, so a duplicate would be silently dropped and the surviving one
// chosen by iteration order — publishing two addresses for a service means
// publishing an arbitrary one of them.
func (d ServiceLockData) Validate() error {
	if len(d.Descriptors) == 0 {
		return fmt.Errorf("%w: no descriptors", ErrInvalidLockData)
	}
	seen := make(map[ThriftService]struct{}, len(d.Descriptors))
	for _, descriptor := range d.Descriptors {
		if err := descriptor.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[descriptor.Service]; duplicate {
			return fmt.Errorf("%w: %s advertised more than once",
				ErrInvalidLockData, descriptor.Service)
		}
		seen[descriptor.Service] = struct{}{}
	}
	return nil
}

// Address returns the address advertised for a service, and whether it is
// advertised at all. It mirrors ServiceLockData.getAddressString.
func (d ServiceLockData) Address(service ThriftService) (string, bool) {
	for _, descriptor := range d.Descriptors {
		if descriptor.Service == service {
			return descriptor.Address, true
		}
	}
	return "", false
}

// Encode serializes the payload for a lock znode. It validates first, so an
// unusable advertisement is never written to ZooKeeper.
func (d ServiceLockData) Encode() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	descriptors := append([]ServiceDescriptor(nil), d.Descriptors...)
	sort.Slice(descriptors, func(i, j int) bool {
		return thriftServiceOrder[descriptors[i].Service] < thriftServiceOrder[descriptors[j].Service]
	})
	encoded, err := json.Marshal(serviceLockDataJSON{Descriptors: descriptors})
	if err != nil {
		return nil, fmt.Errorf("encode service lock data: %w", err)
	}
	return encoded, nil
}

// DecodeServiceLockData parses a lock znode payload.
//
// It does not validate: reading is how this process observes servers it does
// not control, including Java ones and future Accumulo versions, and a
// descriptor it cannot use is not a reason to discard the ones it can. Callers
// that intend to act on the result check the fields they need.
func DecodeServiceLockData(raw []byte) (ServiceLockData, error) {
	if len(raw) == 0 {
		return ServiceLockData{}, fmt.Errorf("%w: empty payload", ErrInvalidLockData)
	}
	var decoded serviceLockDataJSON
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ServiceLockData{}, fmt.Errorf("decode service lock data: %w", err)
	}
	return ServiceLockData{Descriptors: decoded.Descriptors}, nil
}
