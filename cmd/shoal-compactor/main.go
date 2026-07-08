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
//  2. Dial the CompactionCoordinator over Thrift, multiplexed under
//     "coordinator", in the manager process.
//  3. Loop:
//     a. Generate a fresh externalCompactionId (ECID).
//     b. Call getCompactionJob(group, host:port, ecid).
//     c. If the returned job has no ECID set, sleep and retry.
//     d. Otherwise: log the job and (today) stop short of execution.
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
	coordinatorAddr := flag.String("coordinator", "", "host:port of the manager's CompactionCoordinator. Today: explicit flag (TODO: resolve via manager ServiceLock data in /accumulo/<uuid>/managers/lock, ThriftService.COORDINATOR address). REQUIRED.")
	zkServers := flag.String("zk", "", "comma-separated ZK quorum")
	instanceName := flag.String("instance", "accumulo", "Accumulo instance name")
	accVersion := flag.String("accumulo-version", "4.0.0-SNAPSHOT", "server major.minor must match")
	user := flag.String("user", "root", "principal for the coordinator RPC (root-equivalent — same trust path Java compactor uses)")
	password := flag.String("password", "", "password (prefer SHOAL_PASSWORD env)")
	zkTimeout := flag.Duration("zk-timeout", 30*time.Second, "ZK session timeout")
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
	if *coordinatorAddr == "" {
		die("shoal-compactor: -coordinator is required (host:port of manager's CompactionCoordinator)")
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

	logger.Info("shoal-compactor startup",
		slog.String("version", version),
		slog.String("group", *groupName),
		slog.String("coordinator", *coordinatorAddr),
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-stopCh
		logger.Info("shutdown signal", slog.String("sig", sig.String()))
		cancel()
	}()

	runPollLoop(ctx, logger, pollConfig{
		coordinatorAddr: *coordinatorAddr,
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

type pollConfig struct {
	coordinatorAddr string
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
// between idle polls. The loop only exits when ctx is cancelled.
func runPollLoop(ctx context.Context, logger *slog.Logger, cfg pollConfig) {
	wait := cfg.minWait
	for {
		if ctx.Err() != nil {
			return
		}

		cc, err := cclient.DialCoordinator(cfg.coordinatorAddr, cfg.instanceID, cfg.accumuloVersion)
		if err != nil {
			logger.Warn("coordinator dial failed; backing off",
				slog.String("err", err.Error()),
				slog.Duration("retry_in", wait))
			if !sleepCtx(ctx, wait) {
				return
			}
			wait = nextWait(wait, cfg.maxWait)
			continue
		}

		// One connection, drain jobs until the coordinator says "no work"
		// or the transport errors. On any transport failure, drop the
		// connection and reconnect.
		idle := drainCoordinator(ctx, logger, cc, cfg)
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
func drainCoordinator(ctx context.Context, logger *slog.Logger, cc *cclient.CoordinatorClient, cfg pollConfig) bool {
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
func executeJob(ctx context.Context, logger *slog.Logger, cc *cclient.CoordinatorClient, cfg pollConfig, job *tabletserver.TExternalCompactionJob) {
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
