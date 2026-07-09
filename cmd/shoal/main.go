// shoal is the V0 read-fleet pod. Single Go binary that:
//
//  1. Connects to ZooKeeper to resolve instance UUID + root tablet
//     location (via internal/zk).
//  2. Wraps a metadata.Walker for tablet→file lookups, fronted by a
//     LocatorCache so repeated scans on the same tablet hit memory.
//  3. Maintains an LRU block cache of decompressed RFile blocks.
//  4. Serves Thrift TabletScanClientService.startScan via
//     internal/scanserver. ContinueScan/CloseScan are no-ops; each
//     StartScan is single-shot.
//
// Per the V0 spec, this binary is the only artifact deployed —
// fleet-mode is enabled by running N replicas behind a Kubernetes
// Service. Hedge coordination lives in the Java SDK side.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/phrocker/shoal/internal/cache"
	"github.com/phrocker/shoal/internal/cred"
	"github.com/phrocker/shoal/internal/metadata"
	"github.com/phrocker/shoal/internal/protocol"
	"github.com/phrocker/shoal/internal/scanserver"
	"github.com/phrocker/shoal/internal/storage"
	"github.com/phrocker/shoal/internal/storage/azure"
	"github.com/phrocker/shoal/internal/storage/diskcache"
	"github.com/phrocker/shoal/internal/storage/gcs"
	"github.com/phrocker/shoal/internal/storage/local"
	"github.com/phrocker/shoal/internal/storage/s3"
	"github.com/phrocker/shoal/internal/thrift/gen/tabletscan"
	"github.com/phrocker/shoal/internal/zk"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	listenAddr := flag.String("listen", ":9800", "Thrift listener address")
	zkServers := flag.String("zk", "", "comma-separated ZK quorum (e.g. zk-0:2181,zk-1:2181)")
	instanceName := flag.String("instance", "accumulo", "Accumulo instance name")
	accVersion := flag.String("accumulo-version", "4.0.0-SNAPSHOT", "server major.minor must match")
	user := flag.String("user", "root", "principal for metadata scans (NOT for served scan requests — those carry their own creds)")
	password := flag.String("password", "", "password (NOT recommended; prefer SHOAL_PASSWORD env)")
	zkTimeout := flag.Duration("zk-timeout", 30*time.Second, "ZK session timeout")
	storageScheme := flag.String("storage", "gs", "RFile storage scheme: gs (GCS, default), s3 (AWS S3), azure (Azure Blob), or local")
	cacheBytes := flag.Int64("cache-bytes", 256<<20, "block cache capacity in bytes; 0 disables")
	diskCacheDir := flag.String("disk-cache-dir", "", "local directory for the read-through disk cache in front of the remote store; empty disables it")
	diskCacheBytes := flag.Int64("disk-cache-bytes", 20<<30, "disk cache capacity in bytes; only used when -disk-cache-dir is set")
	logLevel := flag.String("log-level", "info", "slog level: debug, info, warn, error")
	prewarmTables := flag.String("prewarm-tables", "auto", "comma-separated table IDs to pre-warm into the file cache on startup. \"auto\" = walk metadata for all user tables. empty = disable prewarming.")
	prewarmParallelism := flag.Int("prewarm-parallelism", 8, "parallel GCS fetches during prewarm")
	flag.Parse()

	if *showVersion {
		fmt.Println("shoal", version)
		return
	}

	level := parseLogLevel(*logLevel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	if *zkServers == "" {
		die("shoal: -zk is required")
	}
	if *password == "" {
		*password = os.Getenv("SHOAL_PASSWORD")
	}
	if *password == "" {
		die("shoal: password required (-password or SHOAL_PASSWORD env)")
	}

	logger.Info("shoal startup",
		slog.String("version", version),
		slog.String("listen", *listenAddr),
		slog.String("zk", *zkServers),
		slog.String("instance", *instanceName),
		slog.Int64("cache_bytes", *cacheBytes),
	)

	// 1. ZK + instance UUID.
	servers := strings.Split(*zkServers, ",")
	loc, err := zk.New(servers, *instanceName, *zkTimeout)
	if err != nil {
		die("shoal: zk.New: %v", err)
	}
	defer loc.Close()
	logger.Info("zk connected", slog.String("instance_id", loc.InstanceID()))

	// 2. Credentials for the metadata-walking flow only. NOTE: scan
	// requests served by this pod carry their OWN credentials in the
	// TCredentials parameter — those are evaluated per request, not
	// here.
	creds := cred.NewPasswordCreds(*user, *password, loc.InstanceID())
	walker := metadata.NewWalker(loc, creds, *accVersion)
	// Wrap in a tablet-location cache so scan-time metadata walks hit
	// memory after the first lookup. Without this every scan pays
	// ~30ms in 3-hop metadata RPCs (root → !0 → user-table), which
	// makes hedge races unwinnable on warm reads.
	locator := cache.New(walker)

	// 3. Storage backend (GCS by default).
	var bk storage.Backend
	switch *storageScheme {
	case "gs", "gcs":
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		bk, err = gcs.New(ctx)
		cancel()
		if err != nil {
			die("shoal: gcs.New: %v", err)
		}
	case "s3":
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		bk, err = s3.New(ctx)
		cancel()
		if err != nil {
			die("shoal: s3.New: %v", err)
		}
	case "azure", "azblob", "az":
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		bk, err = azure.New(ctx)
		cancel()
		if err != nil {
			die("shoal: azure.New: %v", err)
		}
	case "local":
		bk = local.New()
	default:
		die("shoal: unknown -storage %q (expected gs, s3, azure, or local)", *storageScheme)
	}

	// 3b. Optional read-through local-FS disk cache in front of the
	// remote object store. Backend-agnostic: it wraps whichever backend
	// was selected above (GCS/S3/Azure/local) by composition.
	if *diskCacheDir != "" && *diskCacheBytes > 0 {
		dc, derr := diskcache.New(bk, *diskCacheDir, *diskCacheBytes)
		if derr != nil {
			die("shoal: diskcache.New: %v", derr)
		}
		bk = dc
		logger.Info("disk cache enabled",
			slog.String("dir", *diskCacheDir),
			slog.Int64("capacity_bytes", *diskCacheBytes),
		)
	}

	// 4. Block cache.
	bcache := cache.NewBlockCache(*cacheBytes)

	// 5. Scan server.
	srv, err := scanserver.NewServer(scanserver.Options{
		Locator:    locator,
		BlockCache: bcache,
		Storage:    bk,
		Logger:     logger,
	})
	if err != nil {
		die("shoal: scanserver.NewServer: %v", err)
	}

	// 6. Thrift listener. Multiplex under "scan" — Accumulo registers
	// TabletScanClientService via TMultiplexedProcessor with that name.
	processor := tabletscan.NewTabletScanClientServiceProcessor(srv)
	multiplexed := thrift.NewTMultiplexedProcessor()
	multiplexed.RegisterProcessor("scan", processor)

	transportFactory := thrift.NewTFramedTransportFactoryConf(
		thrift.NewTBufferedTransportFactory(8192),
		&thrift.TConfiguration{},
	)
	protocolFactory := protocol.NewServerFactory(loc.InstanceID(), *accVersion)

	serverSocket, err := thrift.NewTServerSocket(*listenAddr)
	if err != nil {
		die("shoal: TServerSocket %s: %v", *listenAddr, err)
	}
	tserver := thrift.NewTSimpleServer4(
		multiplexed,
		serverSocket,
		transportFactory,
		protocolFactory,
	)

	logger.Info("thrift listener up", slog.String("addr", *listenAddr))
	go func() {
		if err := tserver.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Error("thrift server failed", slog.Any("err", err))
		}
	}()

	// Pre-warm caches in the background. Eliminates the 600-800ms
	// GCS-pull penalty on the first scan after a pod restart. Runs
	// async so the readiness probe trips immediately after the
	// listener is up.
	if *prewarmTables != "" {
		go func() {
			ctx := context.Background()
			tableIDs := scanserver.ParseTableIDs(*prewarmTables)
			if *prewarmTables == "auto" {
				resolved, err := enumerateUserTables(ctx, walker, logger)
				if err != nil {
					logger.Warn("prewarm: auto-enumerate failed", slog.Any("err", err))
					return
				}
				tableIDs = resolved
			}
			if len(tableIDs) == 0 {
				logger.Info("prewarm: no tables to warm")
				return
			}
			srv.Prewarm(ctx, tableIDs, *prewarmParallelism)
		}()
	}

	// Graceful shutdown.
	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stopCh
	logger.Info("shutdown signal", slog.String("sig", sig.String()))
	if err := tserver.Stop(); err != nil {
		logger.Error("thrift Stop", slog.Any("err", err))
	}
	logger.Info("shoal exit clean")
}

// enumerateUserTables walks the metadata chain (root → !0 → user tables)
// and returns the discovered user-facing table IDs. System tables (the
// ones starting with "+" or "!") are filtered out — pre-warming them
// would burn cache budget on metadata RFiles that are already small
// enough to be cheap on cold reads.
func enumerateUserTables(ctx context.Context, walker *metadata.Walker, logger *slog.Logger) ([]string, error) {
	all, err := walker.BootstrapAll(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(all))
	for tableID := range all {
		// Skip system tables — they're either tiny (+r, +scanref) or
		// already-warm via the metadata-walk path itself (!0).
		if len(tableID) > 0 && (tableID[0] == '+' || tableID[0] == '!') {
			continue
		}
		out = append(out, tableID)
	}
	logger.Info("prewarm: auto-enumerated user tables", slog.Any("tables", out))
	return out, nil
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
