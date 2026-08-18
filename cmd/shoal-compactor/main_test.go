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

package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/compactioncoordinator"
	"github.com/phrocker/shoal/internal/thrift/gen/data"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
	"github.com/phrocker/shoal/internal/zk"
)

// scriptedResolver returns one result per call; the final entry repeats.
type scriptedResolver struct {
	results []resolveResult
	calls   int
}

type resolveResult struct {
	addr string
	err  error
}

func (r *scriptedResolver) Address(context.Context) (string, error) {
	i := r.calls
	if i >= len(r.results) {
		i = len(r.results) - 1
	}
	r.calls++
	return r.results[i].addr, r.results[i].err
}

// fakeCoordinator implements only the two RPCs the poll loop issues; the
// embedded interface leaves the rest nil so an unexpected call panics
// instead of silently succeeding.
type fakeCoordinator struct {
	compactioncoordinator.CompactionCoordinatorService

	getJob func(ecid string) (*compactioncoordinator.TNextCompactionJob, error)
	failed []failedCompaction
}

type failedCompaction struct {
	ecid  string
	class string
	state compactioncoordinator.TCompactionState
}

func (f *fakeCoordinator) GetCompactionJob(
	_ context.Context,
	_ *client.TInfo,
	_ *security.TCredentials,
	_ string,
	_ string,
	ecid string,
) (*compactioncoordinator.TNextCompactionJob, error) {
	return f.getJob(ecid)
}

func (f *fakeCoordinator) CompactionFailed(
	_ context.Context,
	_ *client.TInfo,
	_ *security.TCredentials,
	ecid string,
	_ *data.TKeyExtent,
	exceptionClassName string,
	failureState compactioncoordinator.TCompactionState,
) error {
	f.failed = append(f.failed, failedCompaction{
		ecid:  ecid,
		class: exceptionClassName,
		state: failureState,
	})
	return nil
}

type fakeConn struct {
	svc    *fakeCoordinator
	closed int
}

func (c *fakeConn) Raw() compactioncoordinator.CompactionCoordinatorService { return c.svc }

func (c *fakeConn) Close() error {
	c.closed++
	return nil
}

// idleReply is the coordinator's "no work for this group" answer: a job
// with no externalCompactionId set.
func idleReply() *compactioncoordinator.TNextCompactionJob {
	return &compactioncoordinator.TNextCompactionJob{
		Job:            tabletserver.NewTExternalCompactionJob(),
		CompactorCount: 1,
	}
}

func testLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func testPollConfig(resolver coordinatorResolver, dial func(string, string, string) (coordinatorConn, error)) pollConfig {
	return pollConfig{
		resolver:        resolver,
		dial:            dial,
		instanceID:      "uuid-1",
		accumuloVersion: "4.0.0-SNAPSHOT",
		groupName:       "shoal_default",
		advertiseAddr:   "compactor-1:9810",
		creds:           security.NewTCredentials(),
		minWait:         time.Millisecond,
		maxWait:         8 * time.Millisecond,
	}
}

// TestRunPollLoopFollowsManagerFailover walks the loop through a manager
// failover: it polls the old primary, rides out the window where no
// manager advertises a coordinator, and then dials the address the new
// primary published — without a restart and without ever dialing during
// the gap.
func TestRunPollLoopFollowsManagerFailover(t *testing.T) {
	resolver := &scriptedResolver{results: []resolveResult{
		{addr: "manager-a:9999"},
		{err: zk.ErrCoordinatorUnavailable},
		{err: zk.ErrCoordinatorUnavailable},
		{addr: "manager-b:9999"},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var dialed []string
	dial := func(addr, instanceID, accumuloVersion string) (coordinatorConn, error) {
		if instanceID != "uuid-1" || accumuloVersion != "4.0.0-SNAPSHOT" {
			t.Errorf("dial(%q) instance/version = %q/%q", addr, instanceID, accumuloVersion)
		}
		dialed = append(dialed, addr)
		return &fakeConn{svc: &fakeCoordinator{
			getJob: func(string) (*compactioncoordinator.TNextCompactionJob, error) {
				// Stop once the loop has reached the new primary.
				if addr == "manager-b:9999" {
					cancel()
				}
				return idleReply(), nil
			},
		}}, nil
	}

	logger, logs := testLogger()
	runPollLoop(ctx, logger, testPollConfig(resolver, dial))

	want := []string{"manager-a:9999", "manager-b:9999"}
	if strings.Join(dialed, ",") != strings.Join(want, ",") {
		t.Fatalf("dialed = %v, want %v (no dial may happen while the lock is unheld)", dialed, want)
	}
	out := logs.String()
	if !strings.Contains(out, `"coordinator address changed; following manager failover"`) ||
		!strings.Contains(out, `previous=manager-a:9999`) ||
		!strings.Contains(out, `current=manager-b:9999`) {
		t.Fatalf("failover not logged:\n%s", out)
	}
}

// TestRunPollLoopBacksOffWhileCoordinatorUnavailable pins the two
// discovery-failure shapes apart: a missing coordinator descriptor is an
// expected failover window (INFO, keep retrying), while a ZooKeeper
// transport failure is operator-visible (WARN).
func TestRunPollLoopBacksOffWhileCoordinatorUnavailable(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantLevel string
	}{
		{"failover window", zk.ErrCoordinatorUnavailable, "level=INFO"},
		{"zookeeper down", errors.New("list manager locks: connection refused"), "level=WARN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			attempts := 0
			resolver := resolverFunc(func(context.Context) (string, error) {
				attempts++
				if attempts == 3 {
					cancel()
				}
				return "", tt.err
			})
			dial := func(addr, _, _ string) (coordinatorConn, error) {
				t.Fatalf("dialed %q despite failed discovery", addr)
				return nil, nil
			}

			logger, logs := testLogger()
			runPollLoop(ctx, logger, testPollConfig(resolver, dial))

			if attempts < 3 {
				t.Fatalf("resolver attempts = %d, want retries until cancellation", attempts)
			}
			out := logs.String()
			if !strings.Contains(out, `"coordinator discovery failed; backing off"`) {
				t.Fatalf("discovery failure not logged:\n%s", out)
			}
			if !strings.Contains(out, tt.wantLevel) {
				t.Fatalf("discovery failure not logged at %s:\n%s", tt.wantLevel, out)
			}
			// Backoff must grow so a long outage does not hammer ZooKeeper.
			if got := retryWaits(out); strings.Join(got, ",") != "1ms,2ms" {
				t.Fatalf("backoff = %v, want [1ms 2ms]:\n%s", got, out)
			}
		})
	}
}

type resolverFunc func(context.Context) (string, error)

func (f resolverFunc) Address(ctx context.Context) (string, error) { return f(ctx) }

// retryWaits extracts the retry_in= backoff values, in order, from
// captured slog text output.
func retryWaits(logOutput string) []string {
	var waits []string
	for _, line := range strings.Split(logOutput, "\n") {
		_, rest, found := strings.Cut(line, "retry_in=")
		if !found {
			continue
		}
		wait, _, _ := strings.Cut(rest, " ")
		waits = append(waits, strings.TrimSpace(wait))
	}
	return waits
}

// TestRunPollLoopResetsBackoffAfterReconnect makes sure the failover
// backoff does not leak past recovery: once the loop is talking to a
// manager again, the next outage restarts at minWait.
func TestRunPollLoopResetsBackoffAfterReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := 0
	resolver := resolverFunc(func(context.Context) (string, error) {
		attempts++
		if attempts == 4 {
			return "manager-a:9999", nil
		}
		if attempts >= 6 {
			cancel()
		}
		return "", zk.ErrCoordinatorUnavailable
	})
	dial := func(string, string, string) (coordinatorConn, error) {
		return &fakeConn{svc: &fakeCoordinator{
			getJob: func(string) (*compactioncoordinator.TNextCompactionJob, error) {
				return idleReply(), nil
			},
		}}, nil
	}

	logger, logs := testLogger()
	runPollLoop(ctx, logger, testPollConfig(resolver, dial))

	// Attempts 1-3 fail (1ms, 2ms, 4ms), attempt 4 reconnects and resets
	// the failure backoff to minWait — the idle poll after it doubles to
	// 2ms, so attempt 5 must report 2ms. Without the reset the loop would
	// still be sitting at the 8ms cap.
	out := logs.String()
	if got := retryWaits(out); strings.Join(got, ",") != "1ms,2ms,4ms,2ms" {
		t.Fatalf("backoff = %v, want [1ms 2ms 4ms 2ms]:\n%s", got, out)
	}
}

// TestDrainCoordinatorRefusesJobToManager checks the current commit
// boundary: an assigned job is handed straight back with a sentinel
// failure so the manager can reschedule it onto a Java compactor. Shoal
// never commits metadata itself.
func TestDrainCoordinatorRefusesJobToManager(t *testing.T) {
	svc := &fakeCoordinator{}
	calls := 0
	svc.getJob = func(ecid string) (*compactioncoordinator.TNextCompactionJob, error) {
		calls++
		if calls > 1 {
			return idleReply(), nil
		}
		job := tabletserver.NewTExternalCompactionJob()
		job.ExternalCompactionId = ecid
		job.Extent = &data.TKeyExtent{Table: []byte("2"), EndRow: []byte("m")}
		job.Files = []*tabletserver.InputFile{{MetadataFileEntry: "hdfs://nn/t/2/F0001.rf"}}
		job.OutputFile = "hdfs://nn/t/2/C0002.rf"
		return &compactioncoordinator.TNextCompactionJob{Job: job, CompactorCount: 1}, nil
	}

	cfg := testPollConfig(&scriptedResolver{results: []resolveResult{{addr: "manager-a:9999"}}}, nil)
	logger, _ := testLogger()
	if idle := drainCoordinator(context.Background(), logger, &fakeConn{svc: svc}, cfg); !idle {
		t.Fatal("drain = busy, want idle after the coordinator ran out of jobs")
	}

	if len(svc.failed) != 1 {
		t.Fatalf("compactionFailed calls = %d, want 1", len(svc.failed))
	}
	got := svc.failed[0]
	if got.class != "org.apache.accumulo.shoal.NotYetImplemented" {
		t.Fatalf("exception class = %q", got.class)
	}
	if got.state != compactioncoordinator.TCompactionState_FAILED {
		t.Fatalf("failure state = %v, want FAILED", got.state)
	}
	if !strings.HasPrefix(got.ecid, ecidPrefix) {
		t.Fatalf("ecid = %q, want %s-prefixed", got.ecid, ecidPrefix)
	}
}

// TestDrainCoordinatorRejectsMismatchedECID guards against acting on a
// job this compactor did not request: the connection is dropped and no
// state is reported for the foreign compaction.
func TestDrainCoordinatorRejectsMismatchedECID(t *testing.T) {
	svc := &fakeCoordinator{
		getJob: func(string) (*compactioncoordinator.TNextCompactionJob, error) {
			job := tabletserver.NewTExternalCompactionJob()
			job.ExternalCompactionId = ecidPrefix + "0d1a4b0e-0000-0000-0000-000000000000"
			return &compactioncoordinator.TNextCompactionJob{Job: job, CompactorCount: 1}, nil
		},
	}

	cfg := testPollConfig(&scriptedResolver{results: []resolveResult{{addr: "manager-a:9999"}}}, nil)
	logger, logs := testLogger()
	if idle := drainCoordinator(context.Background(), logger, &fakeConn{svc: svc}, cfg); idle {
		t.Fatal("drain = idle, want reconnect after a mismatched ecid")
	}
	if len(svc.failed) != 0 {
		t.Fatalf("reported state for a foreign compaction: %+v", svc.failed)
	}
	if !strings.Contains(logs.String(), "mismatched ecid") {
		t.Fatalf("mismatch not logged:\n%s", logs.String())
	}
}

// lockReader is a fixed ZooKeeper view of the manager lock.
type lockReader struct {
	children []string
	data     []byte
}

func (l lockReader) InstancePath() string { return "/accumulo/uuid-1" }

func (l lockReader) Children(context.Context, string) ([]string, error) { return l.children, nil }

func (l lockReader) GetRaw(context.Context, string) ([]byte, error) { return l.data, nil }

// TestZKCoordinatorResolverReadsManagerLock pins discovery to the
// address the manager itself published — shoal never elects one.
func TestZKCoordinatorResolverReadsManagerLock(t *testing.T) {
	const lockNode = "zlock#407b5e9b-9d92-4aaa-bff0-d9ba9be206a6#0000000001"
	resolver := zkCoordinatorResolver{locator: lockReader{
		children: []string{lockNode},
		data: []byte(`{"descriptors":[
			{"service":"MANAGER","address":"manager-a:9999"},
			{"service":"COORDINATOR","address":"manager-a:9999"}]}`),
	}}
	addr, err := resolver.Address(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if addr != "manager-a:9999" {
		t.Fatalf("addr = %q, want manager-a:9999", addr)
	}

	// Bootstrap state: the manager holds the lock but has not published
	// its coordinator yet.
	resolver = zkCoordinatorResolver{locator: lockReader{
		children: []string{lockNode},
		data:     []byte(`{"descriptors":[{"service":"NONE","address":"0.0.0.0:0"}]}`),
	}}
	if _, err := resolver.Address(context.Background()); !errors.Is(err, zk.ErrCoordinatorUnavailable) {
		t.Fatalf("bootstrap error = %v, want ErrCoordinatorUnavailable", err)
	}
}

func TestStaticCoordinatorResolverPinsAddress(t *testing.T) {
	addr, err := staticCoordinatorResolver("manager-a:9999").Address(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if addr != "manager-a:9999" {
		t.Fatalf("addr = %q, want manager-a:9999", addr)
	}
	if _, err := staticCoordinatorResolver("").Address(context.Background()); err == nil {
		t.Fatal("empty pinned address = nil error, want failure")
	}
}

func TestNewECIDMatchesAccumuloFormat(t *testing.T) {
	first, second := newECID(), newECID()
	if !strings.HasPrefix(first, ecidPrefix) {
		t.Fatalf("ecid = %q, want %s prefix", first, ecidPrefix)
	}
	if first == second {
		t.Fatal("ecid repeated across calls")
	}
}
