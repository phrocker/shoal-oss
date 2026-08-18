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

// shoal-compactor is the Bet-1 external compactor binary (Phase C3 of
// platform/shoal/docs/compactor-and-wal-reads-design.md).
//
// Lifecycle (mirrors server/compactor/.../Compactor.java's main loop):
//
//  1. Connect to ZooKeeper, resolve the instance UUID.
//  2. Resolve the CompactionCoordinator address from the manager's
//     ServiceLock data in ZooKeeper (ThriftService.COORDINATOR under
//     /accumulo/<uuid>/managers/lock), the same lookup Java's
//     ExternalCompactionUtil.findCompactionCoordinator performs.
//  3. Dial that address over Thrift, multiplexed under "coordinator",
//     in the manager process. Both the handshake (-connect-timeout) and
//     every subsequent read/write (-rpc-timeout) are bounded, so a
//     manager that accepts the connection and then goes silent cannot
//     pin the pool to a dead primary.
//  4. Loop:
//     a. Generate a fresh externalCompactionId (ECID).
//     b. Call getCompactionJob(group, host:port, ecid).
//     c. If the returned job has no ECID set, sleep and retry.
//     d. Otherwise: log the job and (today) stop short of execution.
//
// The coordinator address is re-resolved before every connection
// attempt, so a manager failover is tolerated without a restart: while
// no manager holds the lock the lookup returns
// zk.ErrCoordinatorUnavailable and the loop backs off; once the new
// primary manager publishes its descriptors the loop dials the new
// address; a wedged connection is dropped once its RPC timeout fires,
// which is what lets a mid-poll failover be picked up at all.
// Discovery never elects a coordinator — it only reads the
// address the manager itself published, so the manager stays the sole
// authority over which process coordinates compactions.
//
// Java-side boundary (the gap this binary intentionally stops at):
//
// The Accumulo manager owns metadata commits. After a compaction
// produces an output RFile, the new file must be inserted into
// accumulo.metadata for the tablet and the inputs must be dereferenced,
// atomically and under the manager's constraint-enforcement /
// accumulo.root write authority. Today the JVM external compactor
// reaches this via coordinator.compactionCompleted(...) which the
// manager wires to its Ample API.
//
// For shoal we want the same write-authority guarantees without
// embedding Ample (and the metadata constraint stack) in Go. The
// design (decision #1, locked 2026-05-13) is: add a manager-side
// CompactionCommit Thrift RPC that:
//
//   - takes (ecid, extent, output_file_metadata_entry, output_file_size,
//     output_file_entries, stats, FateId)
//   - performs the same Ample commit the Java compactor's success path
//     does today (delete input refs, insert output ref, clear running
//     state), using the manager's existing privileged write path
//   - returns success/failure synchronously, so shoal can either
//     celebrate or trigger a compactionFailed
//
// We rejected "shoal writes accumulo.metadata directly" because it
// would require porting the metadata-constraint iterators + duplicating
// the accumulo.root write-authority lock (the 2026-05-13 wedge was
// exactly such authority bleeding) — keeping commit in one process is
// strictly safer.
//
// This binary leaves a documented hole at the commit boundary: on a
// successful drain it would log "would commit" with the file refs, then
// discards the output without touching metadata. The current skeleton
// stops earlier — it accepts the job, logs it, and calls compactionFailed
// with a sentinel exception so the coordinator routes the job to a Java
// compactor. Once Java-side CompactionCommit lands, the body of
// executeJob flips to: fetch inputs via internal/storage, translate
// IteratorSettings to []iterrt.IterSpec, call compaction.Compact, upload
// the output, call CompactionCommit, then compactionCompleted.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/cred"
	"github.com/phrocker/shoal/internal/thrift/gen/client"
	"github.com/phrocker/shoal/internal/thrift/gen/compactioncoordinator"
	"github.com/phrocker/shoal/internal/thrift/gen/security"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletserver"
	"github.com/phrocker/shoal/internal/zk"
)

var version = "dev"

// ecidPrefix matches Accumulo's ExternalCompactionId format. The Java
// generator produces "ECID:" + UUID; shoal does the same so logs and
// metadata are interchangeable across the two compactor pools.
const ecidPrefix = "ECID:"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	advertiseAddr := flag.String("advertise", "", "host:port the coordinator records as this compactor's address (e.g. POD_IP:9810). REQUIRED.")
	groupName := flag.String("group", "shoal_default", "compactor resource-group name; coordinator routes jobs by group")
	coordinatorAddr := flag.String("coordinator", "", "host:port override for the manager's CompactionCoordinator. Default (empty): discover it from the manager's ServiceLock data in /accumulo/<uuid>/managers/lock (ThriftService.COORDINATOR) and re-resolve it across manager failover.")
	zkServers := flag.String("zk", "", "comma-separated ZK quorum")
	instanceName := flag.String("instance", "accumulo", "Accumulo instance name")
	accVersion := flag.String("accumulo-version", "4.0.0-SNAPSHOT", "server major.minor must match")
	user := flag.String("user", "root", "principal for the coordinator RPC (root-equivalent — same trust path Java compactor uses)")
	password := flag.String("password", "", "password (prefer SHOAL_PASSWORD env)")
	zkTimeout := flag.Duration("zk-timeout", 30*time.Second, "ZK session timeout")
	connectTimeout := flag.Duration("connect-timeout", cclient.DefaultConnectTimeout, "cap on the TCP handshake to the coordinator; a manager that is unreachable at the network level fails fast so the address can be re-resolved")
	rpcTimeout := flag.Duration("rpc-timeout", cclient.DefaultRPCTimeout, "cap on each coordinator read/write; bounds getCompactionJob against a manager that accepts the connection and then goes silent (Java's general.rpc.timeout)")
	minWait := flag.Duration("min-wait", 1*time.Second, "minimum sleep when the coordinator has no job for this group")
	maxWait := flag.Duration("max-wait", 30*time.Second, "maximum sleep when idle (backoff cap)")
	logLevel := flag.String("log-level", "info", "slog level: debug, info, warn, error")
	flag.Parse()

	if *showVersion {
		fmt.Println("shoal-compactor", version)
		return
	}

	level := parseLogLevel(*logLevel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if *advertiseAddr == "" {
		die("shoal-compactor: -advertise is required (the address coordinator records on the running-compaction znode)")
	}
	if *zkServers == "" {
		die("shoal-compactor: -zk is required")
	}
	if *password == "" {
		*password = os.Getenv("SHOAL_PASSWORD")
	}
	if *password == "" {
		die("shoal-compactor: password required (-password or SHOAL_PASSWORD env)")
	}

	coordinatorSource := *coordinatorAddr
	if coordinatorSource == "" {
		coordinatorSource = "<zk-discovery>"
	}
	logger.Info("shoal-compactor startup",
		slog.String("version", version),
		slog.String("group", *groupName),
		slog.String("coordinator", coordinatorSource),
		slog.String("advertise", *advertiseAddr),
		slog.String("zk", *zkServers),
		slog.String("instance", *instanceName),
	)

	servers := strings.Split(*zkServers, ",")
	loc, err := zk.New(servers, *instanceName, *zkTimeout)
	if err != nil {
		die("shoal-compactor: zk.New: %v", err)
	}
	defer loc.Close()
	logger.Info("zk connected", slog.String("instance_id", loc.InstanceID()))

	creds := cred.NewPasswordCreds(*user, *password, loc.InstanceID())

	// An explicit -coordinator pins the address (useful for debugging a
	// specific manager); otherwise every dial attempt re-reads the
	// manager's published COORDINATOR descriptor.
	var resolver coordinatorResolver = zkCoordinatorResolver{locator: loc}
	if *coordinatorAddr != "" {
		logger.Warn("coordinator address pinned by -coordinator; ZK discovery and failover follow are disabled",
			slog.String("coordinator", *coordinatorAddr))
		resolver = staticCoordinatorResolver(*coordinatorAddr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-stopCh
		logger.Info("shutdown signal", slog.String("sig", sig.String()))
		cancel()
	}()

	dialOpts := cclient.DialOptions{
		ConnectTimeout: *connectTimeout,
		RPCTimeout:     *rpcTimeout,
	}

	runPollLoop(ctx, logger, pollConfig{
		resolver: resolver,
		dial: func(ctx context.Context, addr, instanceID, accumuloVersion string) (coordinatorConn, error) {
			return dialCoordinator(ctx, addr, instanceID, accumuloVersion, dialOpts)
		},
		instanceID:      loc.InstanceID(),
		accumuloVersion: *accVersion,
		groupName:       *groupName,
		advertiseAddr:   *advertiseAddr,
		creds:           creds,
		minWait:         *minWait,
		maxWait:         *maxWait,
	})

	logger.Info("shoal-compactor exit clean")
}

// coordinatorResolver yields the address of the manager process that
// currently hosts the CompactionCoordinator. Implementations must be
// safe to call repeatedly: the poll loop re-resolves before every
// connection attempt so manager failover is picked up automatically.
type coordinatorResolver interface {
	Address(ctx context.Context) (string, error)
}

// zkCoordinatorResolver reads the address the manager published on its
// ServiceLock znode. It is a pure reader — the manager remains the only
// process that decides where the coordinator lives.
type zkCoordinatorResolver struct {
	locator zk.LockReader
}

func (r zkCoordinatorResolver) Address(ctx context.Context) (string, error) {
	return zk.CoordinatorAddress(ctx, r.locator)
}

// staticCoordinatorResolver serves a fixed operator-supplied address.
type staticCoordinatorResolver string

func (r staticCoordinatorResolver) Address(context.Context) (string, error) {
	if r == "" {
		return "", errors.New("shoal-compactor: empty pinned coordinator address")
	}
	return string(r), nil
}

// coordinatorConn is the subset of *cclient.CoordinatorClient the poll
// loop uses, so tests can drive the loop without a live manager. Raw
// returns the generated service interface rather than the concrete
// client so a fake coordinator can be substituted.
type coordinatorConn interface {
	Raw() compactioncoordinator.CompactionCoordinatorService
	Close() error
}

// coordinatorClientConn adapts *cclient.CoordinatorClient (whose Raw
// returns the concrete generated client) to coordinatorConn.
type coordinatorClientConn struct {
	*cclient.CoordinatorClient
}

func (c coordinatorClientConn) Raw() compactioncoordinator.CompactionCoordinatorService {
	return c.CoordinatorClient.Raw()
}

func dialCoordinator(
	ctx context.Context,
	addr, instanceID, accumuloVersion string,
	opts cclient.DialOptions,
) (coordinatorConn, error) {
	cc, err := cclient.DialCoordinator(ctx, addr, instanceID, accumuloVersion, opts)
	if err != nil {
		return nil, err
	}
	return coordinatorClientConn{CoordinatorClient: cc}, nil
}

type pollConfig struct {
	resolver        coordinatorResolver
	dial            func(ctx context.Context, addr, instanceID, accumuloVersion string) (coordinatorConn, error)
	instanceID      string
	accumuloVersion string
	groupName       string
	advertiseAddr   string
	creds           *security.TCredentials
	minWait         time.Duration
	maxWait         time.Duration
}

// runPollLoop is the main service loop. It re-dials the coordinator on
// transport errors (matching Java's RetryableThriftCall semantics:
// coordinator restarts are tolerated), and uses exponential backoff
// between idle polls. Every attempt re-resolves the coordinator address
// first, so a manager failover moves the pool to the new primary without
// operator action. The loop only exits when ctx is cancelled.
func runPollLoop(ctx context.Context, logger *slog.Logger, cfg pollConfig) {
	wait := cfg.minWait
	lastAddr := ""
	// recovering tracks whether the previous attempt failed discovery or
	// dial, so the failure backoff is dropped once the pool is talking to
	// a manager again instead of leaking into the idle-poll cadence.
	recovering := false
	for {
		if ctx.Err() != nil {
			return
		}

		addr, err := cfg.resolver.Address(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			recovering = true
			// zk.ErrCoordinatorUnavailable is the expected steady state
			// during a manager failover: no manager holds the lock, or the
			// new one has not republished its descriptors yet.
			level := slog.LevelWarn
			if errors.Is(err, zk.ErrCoordinatorUnavailable) {
				level = slog.LevelInfo
			}
			logger.Log(ctx, level, "coordinator discovery failed; backing off",
				slog.String("err", err.Error()),
				slog.Duration("retry_in", wait))
			if !sleepCtx(ctx, wait) {
				return
			}
			wait = nextWait(wait, cfg.maxWait)
			continue
		}

		if addr != lastAddr {
			if lastAddr == "" {
				logger.Info("coordinator resolved", slog.String("addr", addr))
			} else {
				logger.Info("coordinator address changed; following manager failover",
					slog.String("previous", lastAddr),
					slog.String("current", addr))
			}
			lastAddr = addr
		}

		cc, err := cfg.dial(ctx, addr, cfg.instanceID, cfg.accumuloVersion)
		if err != nil {
			recovering = true
			logger.Warn("coordinator dial failed; backing off",
				slog.String("addr", addr),
				slog.String("err", err.Error()),
				slog.Duration("retry_in", wait))
			if !sleepCtx(ctx, wait) {
				return
			}
			wait = nextWait(wait, cfg.maxWait)
			continue
		}
		if recovering {
			wait = cfg.minWait
			recovering = false
		}

		// One connection, drain jobs until the coordinator says "no work"
		// or the transport errors. On any transport failure, drop the
		// connection and reconnect.
		//
		// Thrift transports are not context-aware once a call is in
		// flight: an RPC to a manager that accepted the connection and
		// then went silent only unblocks when the socket timeout fires.
		// Closing the transport from a watcher makes cancellation
		// immediate, so SIGTERM never has to wait out -rpc-timeout.
		watcherDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = cc.Close()
			case <-watcherDone:
			}
		}()
		idle := drainCoordinator(ctx, logger, cc, cfg)
		close(watcherDone)
		_ = cc.Close()

		if ctx.Err() != nil {
			return
		}
		if idle {
			if !sleepCtx(ctx, wait) {
				return
			}
			wait = nextWait(wait, cfg.maxWait)
		} else {
			wait = cfg.minWait
		}
	}
}

// drainCoordinator polls one open coordinator connection until either
// (a) it returns no job (idle = true), or (b) a transport error happens
// (idle = false). The bool drives the outer-loop sleep + reconnect.
func drainCoordinator(ctx context.Context, logger *slog.Logger, cc coordinatorConn, cfg pollConfig) bool {
	for {
		if ctx.Err() != nil {
			return false
		}

		ecid := newECID()
		next, err := cc.Raw().GetCompactionJob(
			ctx,
			client.NewTInfo(),
			cfg.creds,
			cfg.groupName,
			cfg.advertiseAddr,
			ecid,
		)
		if err != nil {
			logger.Warn("getCompactionJob failed; will reconnect",
				slog.String("err", err.Error()))
			return false
		}

		job := next.GetJob()
		if job == nil || job.GetExternalCompactionId() == "" {
			// Java's Compactor.java checks !job.isSetExternalCompactionId();
			// the Go generator drops isSet on required fields, so an unset
			// id reaches us as the zero value. Either form (nil job or empty
			// id) means "no work for this group right now".
			logger.Debug("coordinator: no job for group",
				slog.String("group", cfg.groupName),
				slog.Int("compactor_count", int(next.GetCompactorCount())))
			return true
		}

		if job.GetExternalCompactionId() != ecid {
			logger.Error("coordinator handed back mismatched ecid; aborting drain",
				slog.String("expected", ecid),
				slog.String("got", job.GetExternalCompactionId()))
			return false
		}

		executeJob(ctx, logger, cc, cfg, job)
	}
}

// executeJob logs the assignment and stops at the commit boundary.
// Phase C3 groundwork: the iterator-stack composer (internal/compaction)
// is fully built but the metadata-commit RPC does not yet exist on the
// manager. Until it does, this binary refuses to write to
// accumulo.metadata and instead:
//
//  1. Logs the job's input files + output file.
//  2. Reports compactionFailed with a sentinel exception class so the
//     coordinator releases the compaction slot and a Java compactor
//     picks it up.
//
// Once the Java-side CompactionCommit RPC lands (see file-level doc),
// the body of this function becomes:
//
//	a. fetch input RFile bytes via internal/storage (the same code shoal
//	   uses for scan-time RFile pulls)
//	b. translate job.GetIteratorSettings() into []iterrt.IterSpec via a
//	   registry that mirrors the Java iterator-name → factory mapping
//	   (today: iterrt only knows IterVersioning/IterVisibility; the C1
//	   iterator ports add to that registry)
//	c. call compaction.Compact(spec)
//	d. upload the output bytes to job.GetOutputFile() via storage
//	e. call coordinator.CompactionCommit(ecid, extent, file, size, ...)
//	f. on success, call compactionCompleted; on any failure,
//	   compactionFailed
func executeJob(ctx context.Context, logger *slog.Logger, cc coordinatorConn, cfg pollConfig, job *tabletserver.TExternalCompactionJob) {
	inputFiles := make([]string, 0, len(job.GetFiles()))
	for _, f := range job.GetFiles() {
		inputFiles = append(inputFiles, f.GetMetadataFileEntry())
	}
	logger.Info("compaction job received (NOT executing — awaits Java-side CompactionCommit RPC)",
		slog.String("ecid", job.GetExternalCompactionId()),
		slog.String("extent", extentString(job)),
		slog.Int("inputs", len(inputFiles)),
		slog.String("output_file", job.GetOutputFile()),
		slog.Bool("propagate_deletes", job.GetPropagateDeletes()),
		slog.Any("iterators", iteratorNames(job)),
	)
	logger.Info("would compact",
		slog.String("ecid", job.GetExternalCompactionId()),
		slog.Any("inputs", inputFiles),
		slog.String("output", job.GetOutputFile()))

	// Release the slot back to the coordinator so a Java compactor can
	// pick up this job. Sentinel class name signals a non-actionable
	// refusal — matches how Java compactors signal an internal error.
	err := cc.Raw().CompactionFailed(
		ctx,
		client.NewTInfo(),
		cfg.creds,
		job.GetExternalCompactionId(),
		job.GetExtent(),
		"org.apache.accumulo.shoal.NotYetImplemented",
		compactioncoordinator.TCompactionState_FAILED,
	)
	if err != nil {
		logger.Warn("compactionFailed rpc failed",
			slog.String("ecid", job.GetExternalCompactionId()),
			slog.String("err", err.Error()))
	}
}

// extentString is a defensive accessor — older coordinator builds have
// shipped jobs missing a fully-populated extent, and slog should not
// panic on a nil row.
func extentString(job *tabletserver.TExternalCompactionJob) string {
	if !job.IsSetExtent() {
		return "<no-extent>"
	}
	ex := job.GetExtent()
	tableID := string(ex.GetTable())
	end := "+inf"
	if r := ex.GetEndRow(); r != nil {
		end = fmt.Sprintf("%q", r)
	}
	prev := "-inf"
	if r := ex.GetPrevEndRow(); r != nil {
		prev = fmt.Sprintf("%q", r)
	}
	return fmt.Sprintf("table=%s prev=%s end=%s", tableID, prev, end)
}

func iteratorNames(job *tabletserver.TExternalCompactionJob) []string {
	if !job.IsSetIteratorSettings() || job.GetIteratorSettings() == nil {
		return nil
	}
	specs := job.GetIteratorSettings().GetIterators()
	out := make([]string, 0, len(specs))
	for _, it := range specs {
		out = append(out, it.GetName())
	}
	return out
}

// newECID generates an ExternalCompactionId in Accumulo's canonical
// "ECID:<uuid>" form. The coordinator echoes this back in the job
// (job.ExternalCompactionId), and we verify the echo to catch
// out-of-band assignments.
func newECID() string {
	return ecidPrefix + uuid.NewString()
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

// sleepCtx sleeps for d, returning true if it slept to completion or
// false if ctx was cancelled mid-sleep.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// nextWait doubles d up to a cap of maxWait. Matches Java's
// RetryableThriftCall exponential-backoff semantics.
func nextWait(d, maxWait time.Duration) time.Duration {
	d *= 2
	if d > maxWait {
		return maxWait
	}
	return d
}
