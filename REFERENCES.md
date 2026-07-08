<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Source references for the shoal port

Authoritative Java + sharkbite source files referenced during V0
implementation. Quote files here when porting; keep this list current as new
cribs land.

## Accumulo (this repo)

### ZK paths and constants
- `core/src/main/java/org/apache/accumulo/core/Constants.java`
  - `ZROOT = "/accumulo"`, `ZINSTANCES = "/instances"`
  - `ZTABLES`, `ZTABLE_STATE`, `ZTABLE_FLUSH_ID`, `ZTABLE_NAMESPACE`,
    `ZTABLE_DELETE_MARKER` (no `/loaded` — original spec was wrong)
- `core/src/main/java/org/apache/accumulo/core/metadata/RootTable.java`
  - `ZROOT_TABLET = "/root_tablet"`
  - `EXTENT.toMetaRow() = "+r<"` (root tablet self-row in metadata)
- `core/src/main/java/org/apache/accumulo/core/zookeeper/ZooUtil.java:144-145`
  - Path assembly: `ZROOT + "/" + instanceId + ZROOT_TABLET`
  - `:199` instance-name → instance-UUID lookup

### Root tablet znode parsing (JSON in 4.0)
- `core/.../metadata/schema/RootTabletMetadata.java` — JSON shape
- `core/.../client/clientImpl/RootClientTabletCache.java:115-148` — exact
  bootstrap chain we mirror: read znode → parse JSON → extract `loc` →
  return `Location(host, port)`

### Metadata table schema
- `core/src/main/java/org/apache/accumulo/core/metadata/schema/MetadataSchema.java`
  - `:63-76` `TabletsSection.encodeRow(tableId, endRow)`:
    `tableId + ';' + endRow`, or `tableId + '<'` if endRow is null
  - `:327` `CurrentLocationColumnFamily.STR_NAME = "loc"` (qualifier =
    encoded lock id, value = `host:port`)
  - `:381-387` `DataFileColumnFamily.STR_NAME = "file"` (qualifier = file
    path, value = `DataFileValue`)
  - `:361` `BulkFileColumnFamily.STR_NAME = "loaded"` (column family in
    metadata table, NOT a ZK znode)
- `core/.../metadata/schema/DataFileValue.java:48-89`
  - Encoded as `size,numEntries[,time]` (comma-separated longs)

### Lock id encoding
- `core/.../zookeeper/ZooUtil.java:61-81`
  - Lock-id format: `<path>/<node>$<hex_eid>`

### Scan server reference (read path)
- `server/tserver/.../ScanServer.java:148-180`
  - `TabletMetadataLoader.load()` → `ample.readTablet(keyExtent)`
  - `loadAll()` → batch via `ample.readTablets()`
  - Cache TTL: `SSERV_CACHED_TABLET_METADATA_EXPIRATION` (we use exception-
    driven invalidation instead of TTL — matches sharkbite, pairs better
    with hedged reads)
- `core/.../client/clientImpl/ClientTabletCacheImpl.java` — multi-level
  cache for user tables

### Thrift IDL
- `core/src/main/thrift/tabletscan.thrift:77-95` — `startScan` signature
- `core/src/main/thrift/data.thrift` — `TKeyExtent`, `TRange`, `TKey`,
  `TKeyValue`, `TColumn`, `IterInfo`
- `core/src/main/thrift/security.thrift` — `TCredentials`
- `core/src/main/thrift/client.thrift` — `TInfo`

### Wire protocol
- `core/.../rpc/AccumuloProtocolFactory.java`
  - `:49` `MAGIC_NUMBER = 0x41434355` ("ACCU" — A=41, C=43, C=43, U=55)
  - `:50` `PROTOCOL_VERSION = 1` (changes only when header format changes)
  - `:67-76` client-side `writeMessageBegin` prepends header before message
  - `:91-98` server-side `readMessageBegin` validates header, then proceeds
  - `:103-108` header layout (encoded via TCompactProtocol's own writers,
    NOT raw bytes): `i32` magic + `byte` version + `string` accumulo-version
    (e.g. `Constants.VERSION` = `"4.0.0-SNAPSHOT"`) + `string` instance-id
    (UUID canonical)
  - `:115-150` server-side validation: magic match → protocol version == 1
    → accumulo major.minor match → instance id match
  - `:184-190` major.minor extraction: substring before last `.` (so
    `"4.0.0-SNAPSHOT"` becomes `"4.0"`)
- `core/.../rpc/AccumuloTFramedTransportFactory.java:29` — TFramedTransport

### RFile (port target for V0)
- `core/src/main/java/org/apache/accumulo/core/file/rfile/RFile.java`
- `core/.../iterators/system/VisibilityFilter.java`
- `core/.../client/rfile/RFileScanner.java` — Marc's prior art (RFile reader
  upstream); reference, do not depend on

## Sharkbite

C++ Accumulo client (https://github.com/phrocker/sharkbite). Used as a
pattern reference, not a code dependency.

### What carries over
- `src/data/client/MetaDataLocationObtainer.cpp:95-135` — metadata-row
  decoding (`file:`, `loc:` column family loop)
- `include/data/client/LocatorCache.h:26-60` — exception-driven invalidation
  pattern (we reuse this; no TTL)
- `include/interconnect/transport/CachedTransport.h:44-91` — transport-level
  error → cache eviction
- `src/interconnect/accumulo/AccumuloServerOne.cpp:181-247` — single-shot
  `startScan` invocation shape
- `include/data/constructs/client/zookeeper/zookeepers.h:35-41` — ZK path
  constants and bootstrap sequence (path assembly carries; parsing does not
  — see below)

### What does NOT carry over
- **No AccumuloProtocol header wrapper.** Sharkbite targets pre-2.1
  Accumulo, before the magic header existed. We write that piece from
  scratch.
- **Pipe-delimited root znode parsing.** `RootTabletLocator.cpp:32-48` parses
  `host:port|sessionId` — that's the old format. Modern Accumulo (4.0) uses
  JSON `RootTabletMetadata`. Reuse the bootstrap *sequence*, not the parser.
- **No `DataFileValue` decoding.** Sharkbite ignores the `file:` value
  bytes. We need to parse `size,numEntries[,time]` for accurate split
  decisions and stats. (Optional in V0; required by V1.)
