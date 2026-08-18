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

// shoal-embed is the embedded storage engine CLI and HTTP server.
//
// Subcommands:
//
//	shoal-embed serve   — start the gRPC data plane plus an HTTP health/metrics
//	                      surface (/healthz, /readyz, /stats, /metrics) for
//	                      external consumers and orchestrators (use --address /
//	                      --metrics-address for container/k8s)
//
//	shoal-embed write   — write mutations from stdin (JSON lines)
//	shoal-embed scan    — scan a table and print results as JSON lines or Parquet
//	shoal-embed compact — run compaction (with optional REM iterator stack)
//	shoal-embed init    — create a table with split points
//	shoal-embed export  — flush and copy a table's RFiles plus manifest
//	shoal-embed import  — verify a manifest and register/open the table
//	shoal-embed status  — print table/tablet statistics
//
// Example:
//
//	shoal-embed init --table graph --splits "entity:,event:,knowledge:"
//	shoal-embed write --table graph < mutations.jsonl
//	shoal-embed scan --table graph --row-prefix "entity:"
//	shoal-embed compact --table graph
//	shoal-embed serve --data ~/.shoal/data --port 9876
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/engine"
	"github.com/phrocker/shoal/internal/iterrt"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		cmdInit(os.Args[2:])
	case "write":
		cmdWrite(os.Args[2:])
	case "scan":
		cmdScan(os.Args[2:])
	case "compact":
		cmdCompact(os.Args[2:])
	case "export":
		cmdExport(os.Args[2:])
	case "import":
		cmdImport(os.Args[2:])
	case "sync":
		cmdSync(os.Args[2:])
	case "status":
		cmdStatus(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	case "up":
		cmdUp(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Println("shoal-embed", version)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `shoal-embed — embedded Accumulo-model storage engine

Usage: shoal-embed <command> [flags]

Commands:
  init       Create a table with optional split points
  write      Write mutations (JSON lines from stdin)
  scan       Scan a table, print results as JSON lines or Parquet
  compact    Run compaction on a table
  export     Flush and copy a table's RFiles, writing manifest.json
  import     Verify a manifest and register/open the table
  sync       Continuously ship newly flushed/compacted RFiles to object storage
  status     Print table and tablet statistics
  serve      Start gRPC server for embedded storage
  up         Bring up the local profile (gRPC + observability) in one command
  version    Print version

`)
}

// cmdInit creates a table with optional splits.
func cmdInit(args []string) {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	tableName := fs.String("table", "", "table name (required)")
	splits := fs.String("splits", "", "comma-separated split points (e.g. 'entity:,event:,knowledge:')")
	fs.Parse(args)

	if *tableName == "" {
		die("init: --table is required")
	}

	eng, err := engine.Open(*dataDir, engine.Options{})
	if err != nil {
		die("init: %v", err)
	}
	defer eng.Close()

	opts := engine.TableOptions{}
	if *splits != "" {
		parts := strings.Split(*splits, ",")
		opts.Splits = engine.PrefixSplit(parts...)
	}

	if err := eng.CreateTable(*tableName, opts); err != nil {
		die("init: %v", err)
	}
	tabletCount := len(opts.Splits) + 1
	fmt.Printf("created table %q with %d tablet(s)\n", *tableName, tabletCount)
}

// MutationJSON is the JSON wire format for mutations.
type MutationJSON struct {
	Row     string      `json:"row"`
	Entries []EntryJSON `json:"entries"`
}

type EntryJSON struct {
	CF        string `json:"cf"`
	CQ        string `json:"cq"`
	CV        string `json:"cv,omitempty"`
	Timestamp int64  `json:"ts,omitempty"`
	Value     string `json:"value,omitempty"`
	Delete    bool   `json:"delete,omitempty"`
}

// cmdWrite reads JSON-line mutations from stdin and writes them.
func cmdWrite(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	tableName := fs.String("table", "", "table name (required)")
	fs.Parse(args)

	if *tableName == "" {
		die("write: --table is required")
	}

	eng, err := engine.Open(*dataDir, engine.Options{})
	if err != nil {
		die("write: %v", err)
	}
	defer eng.Close()

	dec := json.NewDecoder(os.Stdin)
	var count int
	for dec.More() {
		var mj MutationJSON
		if err := dec.Decode(&mj); err != nil {
			die("write: decode: %v", err)
		}
		m, err := cclient.NewMutation([]byte(mj.Row))
		if err != nil {
			die("write: mutation %q: %v", mj.Row, err)
		}
		for _, e := range mj.Entries {
			ts := e.Timestamp
			if ts == 0 {
				ts = cclient.MutationLatestTimestamp
			}
			if e.Delete {
				m.Delete([]byte(e.CF), []byte(e.CQ), []byte(e.CV), ts)
			} else {
				m.Put([]byte(e.CF), []byte(e.CQ), []byte(e.CV), ts, []byte(e.Value))
			}
		}
		if err := eng.Write(*tableName, []*cclient.Mutation{m}); err != nil {
			die("write: %v", err)
		}
		count++
	}
	fmt.Printf("wrote %d mutation(s)\n", count)
}

// CellJSON is the JSON output format for scan results.
type CellJSON struct {
	Row string `json:"row"`
	CF  string `json:"cf"`
	CQ  string `json:"cq"`
	CV  string `json:"cv,omitempty"`
	TS  int64  `json:"ts"`
	Val string `json:"value"`
}

// cmdScan scans a table and prints JSON lines.
func cmdScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	tableName := fs.String("table", "", "table name (required)")
	rowPrefix := fs.String("row-prefix", "", "filter by row prefix")
	limit := fs.Int("limit", 0, "max cells to return (0 = unlimited)")
	format := fs.String("format", "json", "output format: json | parquet")
	fs.Parse(args)

	if *tableName == "" {
		die("scan: --table is required")
	}
	output, err := newScanOutput(*format, os.Stdout)
	if err != nil {
		die("scan: %v", err)
	}

	eng, err := engine.Open(*dataDir, engine.Options{})
	if err != nil {
		die("scan: %v", err)
	}
	defer eng.Close()

	r := iterrt.InfiniteRange()
	if *rowPrefix != "" {
		// Prefix scan: start at prefix, end at prefix with last byte incremented
		startRow := []byte(*rowPrefix)
		endRow := make([]byte, len(startRow))
		copy(endRow, startRow)
		endRow[len(endRow)-1]++
		r = iterrt.Range{
			Start:          &iterrt.Key{Row: startRow},
			StartInclusive: true,
			End:            &iterrt.Key{Row: endRow},
			EndInclusive:   false,
		}
	}

	sc, err := eng.Scan(*tableName, r, engine.ScanOptions{})
	if err != nil {
		die("scan: %v", err)
	}
	defer sc.Close()

	count, err := writeScan(sc, output, *limit)
	if err != nil {
		die("scan: %v", err)
	}
	fmt.Fprintf(os.Stderr, "%d cell(s)\n", count)
}

// cmdCompact runs compaction on all tablets.
func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	tableName := fs.String("table", "", "table name (required)")
	fs.Parse(args)

	if *tableName == "" {
		die("compact: --table is required")
	}

	eng, err := engine.Open(*dataDir, engine.Options{})
	if err != nil {
		die("compact: %v", err)
	}
	defer eng.Close()

	// Flush before compacting to ensure all data is in RFiles
	if err := eng.Flush(*tableName); err != nil {
		die("compact: flush: %v", err)
	}

	if err := eng.Compact(*tableName, nil); err != nil {
		die("compact: %v", err)
	}
	fmt.Println("compaction complete")
}

// cmdStatus prints table and tablet statistics.
func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	fs.Parse(args)

	eng, err := engine.Open(*dataDir, engine.Options{
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		die("status: %v", err)
	}
	defer eng.Close()

	stats := eng.Stats()
	if len(stats) == 0 {
		fmt.Println("no tables")
		return
	}
	fmt.Printf("%-24s %8s %8s\n", "TABLE", "TABLETS", "RFILES")
	var totTablets, totRFiles int
	for _, st := range stats {
		fmt.Printf("%-24s %8d %8d\n", st.Name, st.Tablets, st.RFiles)
		totTablets += st.Tablets
		totRFiles += st.RFiles
	}
	fmt.Printf("%-24s %8d %8d\n", fmt.Sprintf("(%d tables)", len(stats)), totTablets, totRFiles)
}

// cmdServe starts the gRPC data-plane server plus the HTTP observability
// surface (/healthz, /readyz, /stats, /metrics) for the embedded storage
// engine — this is the binary the production write-tier manifests
// (deploy/k8s, deploy/helm) run.
//
// By default the gRPC server binds loopback (127.0.0.1:<--port>); the HTTP
// observability server, when enabled (see below), also defaults to
// loopback (127.0.0.1:<--metrics-port>). Use --address / --metrics-address
// to override either bind address verbatim (e.g. 0.0.0.0:9876) — required
// in containers/k8s pods, where loopback-only is unreachable from a
// Service.
//
// The gRPC data-plane listener always binds. The HTTP observability
// surface, however, is opt-in: it only starts if --metrics-port or
// --metrics-address is explicitly passed. This keeps a bare `shoal-embed
// serve --port N` — the top-level README's quick-start, and any other
// pre-existing single-port invocation — from silently gaining a second
// listening port as a side effect of this flag being added; production
// manifests (deploy/k8s, deploy/helm) pass --metrics-address explicitly
// and are unaffected.
//
// On SIGINT/SIGTERM the server drains before stopping: it flips /readyz to
// not-ready first, waits --quiesce-delay so a readiness-polling consumer
// has a chance to observe the change before anything stops accepting work,
// then gracefully stops the gRPC and HTTP servers (waiting for in-flight
// RPCs, bounded by --drain-timeout — a stuck-but-cancellable RPC is
// force-aborted rather than hanging shutdown forever; see serveHandle.Stop
// for the one case that isn't cancellable) and closes the engine. The
// process does not exit until this entire sequence finishes. This does not
// migrate tablet ownership — mixed-fleet tablet reassignment on drain
// remains owned by the Accumulo manager/coordinator and is out of scope
// here.
func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	port := fs.Int("port", 9876, "gRPC listen port (ignored when --address is non-empty)")
	address := fs.String("address", "", "override gRPC bind host:port verbatim (e.g. 0.0.0.0:9876 or :9876); when set, --port is ignored")
	metricsPort := fs.Int("metrics-port", 9877, "enable and bind the observability HTTP listen port for /healthz, /readyz, /stats, /metrics (ignored when --metrics-address is non-empty); the HTTP surface is off unless this or --metrics-address is set")
	metricsAddress := fs.String("metrics-address", "", "enable the observability HTTP surface and override its bind host:port verbatim (e.g. 0.0.0.0:9877); when set, --metrics-port is ignored")
	drainTimeout := fs.Duration("drain-timeout", 30*time.Second, "max time to wait for in-flight RPCs to finish during graceful shutdown")
	quiesceDelay := fs.Duration("quiesce-delay", 5*time.Second, "on shutdown, how long to wait after flipping /readyz to not-ready before draining connections — gives a readiness-polling consumer (e.g. a Kubernetes endpoint controller) a chance to stop routing new work first")
	fs.Parse(args)

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	grpcAddr := "127.0.0.1:" + strconv.Itoa(*port)
	if *address != "" {
		grpcAddr = *address
	}
	metricsAddr := metricsAddrFromFlags(fs, *metricsPort, *metricsAddress)

	h, err := startServe(serveConfig{
		DataDir:        *dataDir,
		GRPCAddress:    grpcAddr,
		MetricsAddress: metricsAddr,
		Logger:         logger,
	})
	if err != nil {
		die("serve: %v", err)
	}

	logger.Info("shoal-embed gRPC serve starting",
		slog.String("addr", h.GRPCAddr), slog.String("metrics", h.MetricsAddr), slog.String("data", *dataDir))
	fmt.Fprintf(os.Stderr, "shoal-embed serve: grpc://%s\n", h.GRPCAddr)
	if h.MetricsAddr != "" {
		fmt.Fprintf(os.Stderr, "  health/metrics: http://%s/{healthz,readyz,stats,metrics}\n", h.MetricsAddr)
	} else {
		fmt.Fprintf(os.Stderr, "  health/metrics: disabled (pass --metrics-port or --metrics-address to enable)\n")
	}

	// RunUntilSignal owns the full graceful-shutdown sequence (drain,
	// quiesce, bounded stop, engine close) and — critically — does not
	// return until that sequence has actually finished, so this call
	// cannot return (and the process cannot exit) mid-drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	if err := h.RunUntilSignal(sigCh, *quiesceDelay, *drainTimeout); err != nil {
		die("serve: %v", err)
	}
}

// metricsAddrFromFlags resolves the observability HTTP bind address from
// the --metrics-port / --metrics-address flags, returning "" (disabling
// the HTTP surface — see startServe) unless the caller explicitly set one
// of them. fs.Visit only visits flags that were actually set on the
// command line, so a --metrics-port value that happens to equal the
// default (9877) still counts as explicit, and an entirely bare `serve`
// invocation resolves to "".
func metricsAddrFromFlags(fs *flag.FlagSet, metricsPort int, metricsAddress string) string {
	if metricsAddress != "" {
		return metricsAddress
	}
	explicit := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "metrics-port" || f.Name == "metrics-address" {
			explicit = true
		}
	})
	if !explicit {
		return ""
	}
	return "127.0.0.1:" + strconv.Itoa(metricsPort)
}

func defaultDataDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".shoal-data"
	}
	return home + "/.shoal/data"
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "shoal-embed: "+format+"\n", args...)
	os.Exit(1)
}
