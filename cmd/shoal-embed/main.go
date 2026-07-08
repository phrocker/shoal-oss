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
//	shoal-embed serve   — start an HTTP/gRPC server for external consumers (use --address for container/k8s)
//
//	shoal-embed write   — write mutations from stdin (JSON lines)
//	shoal-embed scan    — scan a table and print results as JSON lines
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
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"github.com/phrocker/shoal/internal/cclient"
	"github.com/phrocker/shoal/internal/embedpb"
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
  scan       Scan a table, print results as JSON lines
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
	fs.Parse(args)

	if *tableName == "" {
		die("scan: --table is required")
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

	enc := json.NewEncoder(os.Stdout)
	count := 0
	for sc.Next() {
		k := sc.Key()
		cell := CellJSON{
			Row: string(k.Row),
			CF:  string(k.ColumnFamily),
			CQ:  string(k.ColumnQualifier),
			CV:  string(k.ColumnVisibility),
			TS:  k.Timestamp,
			Val: string(sc.Value()),
		}
		enc.Encode(cell)
		if err := sc.Advance(); err != nil {
			die("scan: advance: %v", err)
		}
		count++
		if *limit > 0 && count >= *limit {
			break
		}
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

// cmdServe starts the gRPC server for the embedded storage engine.
// By default the server binds loopback (127.0.0.1:<--port>).
// Use --address to override the bind address verbatim (e.g. 0.0.0.0:9876
// or :9876) — useful in containers/k8s pods where loopback-only is unusable.
func cmdServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	dataDir := fs.String("data", defaultDataDir(), "data directory")
	port := fs.Int("port", 9876, "gRPC listen port (ignored when --address is non-empty)")
	address := fs.String("address", "", "override bind host:port verbatim (e.g. 0.0.0.0:9876 or :9876); when set, --port is ignored")
	fs.Parse(args)

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	eng, err := engine.Open(*dataDir, engine.Options{Logger: logger})
	if err != nil {
		die("serve: %v", err)
	}

	addr := "127.0.0.1:" + strconv.Itoa(*port)
	if *address != "" {
		addr = *address
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		die("serve: listen: %v", err)
	}

	srv := grpc.NewServer()
	embedpb.RegisterShoalEmbedServer(srv, newEmbedServer(eng))

	logger.Info("shoal-embed gRPC serve starting", slog.String("addr", addr), slog.String("data", *dataDir))
	fmt.Fprintf(os.Stderr, "shoal-embed serve: grpc://%s\n", addr)

	// Graceful shutdown on SIGINT/SIGTERM
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down")
		srv.GracefulStop()
		eng.Close()
	}()

	if err := srv.Serve(lis); err != nil {
		die("serve: %v", err)
	}
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
