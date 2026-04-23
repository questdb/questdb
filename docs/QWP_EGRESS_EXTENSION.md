# QuestWire Protocol (QWP) Egress Extension

This document specifies QWP's egress mode: SQL queries in, columnar query
results out, over a dedicated WebSocket endpoint. It extends the ingress
specification ([`QWP_SPECIFICATION.md`](QWP_SPECIFICATION.md)) by reusing the
header, type system, and column encodings unchanged. The deltas are limited to:

- a new endpoint and version namespace,
- a one-byte message kind discriminator at the start of each payload,
- five new message kinds for the request/response/cancel/credit lifecycle.

## Table of Contents

1. [Overview](#1-overview)
2. [Transport and Endpoint](#2-transport-and-endpoint)
3. [Version and Compression Negotiation](#3-version-and-compression-negotiation)
4. [Message Structure](#4-message-structure)
5. [Message Kinds](#5-message-kinds)
6. [QUERY_REQUEST (0x10)](#6-query_request-0x10)
7. [RESULT_BATCH (0x11)](#7-result_batch-0x11)
8. [RESULT_END (0x12)](#8-result_end-0x12)
9. [QUERY_ERROR (0x13)](#9-query_error-0x13)
10. [CANCEL (0x14)](#10-cancel-0x14)
11. [CREDIT (0x15)](#11-credit-0x15)
11.5. [NULL Sentinel Conventions](#115-null-sentinel-conventions-inherited-from-questdb)
11.5.1. [Array element NULLs](#1151-array-element-nulls)
11.6. [EXEC_DONE (0x16)](#116-exec_done-0x16)
12. [Schema and Symbol Dictionary Scope](#12-schema-and-symbol-dictionary-scope)
13. [Cursor Lifecycle](#13-cursor-lifecycle)
14. [Flow Control](#14-flow-control)
15. [Status Codes](#15-status-codes)
16. [Limits](#16-limits)
17. [Examples](#17-examples)
18. [Open Questions](#18-open-questions)

---

## 1. Overview

QWP egress streams SQL query results to clients using the same wire encoding
QWP ingress uses for inbound data. Key properties:

- **Columnar result batches.** Each batch is a single QWP table block: schema
  section followed by per-column data with null bitmaps. The decoder is the
  same code path as ingress.
- **Server-driven schemas.** The server assigns connection-scoped schema IDs.
  Mode `0x00` (full) on the first batch of a query; mode `0x01` (reference) on
  subsequent batches with the same column set.
- **Per-connection symbol dictionary.** The server accumulates symbol entries
  across all queries on the connection. Repeated queries reuse prior IDs
  without retransmitting the strings.
- **Byte-credit flow control.** The client grants the server permission to
  send up to `N` bytes of result data. The server pauses production once the
  credit window is exhausted. A row floor guarantees forward progress.
- **One result set per request.** Multi-statement scripts are out of scope;
  one `QUERY_REQUEST` produces zero or more `RESULT_BATCH` frames followed by
  exactly one terminator (`RESULT_END` or `QUERY_ERROR`).

## 2. Transport and Endpoint

Egress runs over RFC 6455 WebSocket binary frames on a **new endpoint**:

```
GET /read/v1
```

The new endpoint exists for two reasons:

1. Operators can route, scale, rate-limit, and authorize ingest and query
   workloads independently.
2. The protocol parsers on each endpoint stay narrow: `/write/v4` accepts
   only `DATA_BATCH`, `/read/v1` accepts only egress kinds.

Mixed-mode clients open one connection per direction.

## 3. Version and Compression Negotiation

Egress shares the QWP version namespace with ingress. Version and compression
are negotiated at the HTTP upgrade:

| Header                   | Direction | Required | Description                                                                       |
|--------------------------|-----------|----------|-----------------------------------------------------------------------------------|
| `X-QWP-Max-Version`      | C -> S    | No       | Maximum QWP version the client supports. Defaults to 1 if absent.                 |
| `X-QWP-Client-Id`        | C -> S    | No       | Free-form client identifier (e.g., `java-egress/1.0.0`).                          |
| `X-QWP-Accept-Encoding`  | C -> S    | No       | Comma-separated list of acceptable `RESULT_BATCH` body encodings (see below).     |
| `X-QWP-Max-Batch-Rows`   | C -> S    | No       | Client-preferred per-batch row cap. Decimal integer; `0` or absent = server default. The server clamps to its own hard limit, so this only ever asks for *smaller* batches (lower latency to first row, more per-batch overhead). |
| `X-QWP-Version`          | S -> C    | Yes      | Negotiated version = `min(clientMax, serverMax)`.                                 |
| `X-QWP-Content-Encoding` | S -> C    | No       | Server's selected encoding from the client's accept list. Omitted means `raw`.    |

Egress was introduced at version 1. Version 2 adds an unsolicited
`SERVER_INFO` frame (§11.7) delivered as the first WebSocket frame after the
upgrade response; the frame carries the server's replication role,
cluster/node identity, and a capabilities bitfield so clients can route reads
to primary vs replica. Ingest is pinned to version 1 because the v2 bump has
no ingest semantics.

The connection-level contract from the ingress spec applies: every message's
header version byte must equal the negotiated version, and the server rejects
mismatches with a parse error and closes the WebSocket.

### Batch body compression

`X-QWP-Accept-Encoding` is a comma-separated list of tokens. Each token is
`name` or `name;param=value`. First match wins. Supported names:

- `raw` (or `identity`) - no compression.
- `zstd` - whole-`RESULT_BATCH`-body zstd compression. Optional parameter
  `level=N` is a client-side hint; the server clamps to `[1, 9]` because
  levels 10+ drop to `<20 MB/s` compress throughput. The default level is 3.

The server echoes its choice in `X-QWP-Content-Encoding` (e.g.
`zstd;level=3`). When `zstd` is negotiated, individual `RESULT_BATCH` frames
set `FLAG_ZSTD` (§7) on a per-batch basis; a batch whose compressed form is
larger than its raw form ships raw. The region before the payload (msg_kind +
request_id + batch_seq) is never compressed so the client dispatcher can
route frames without paying the decompress cost.

Absent `X-QWP-Accept-Encoding` the server defaults to `raw`.

## 4. Message Structure

The egress header is **byte-identical** to the ingress header (12 bytes,
little-endian throughout):

```
Offset  Size  Type    Field           Description
--------------------------------------------------------
0       4     int32   magic           "QWP1" (0x31505751)
4       1     uint8   version         Negotiated QWP version
5       1     uint8   flags           See ingress §7
6       2     uint16  table_count     1 for RESULT_BATCH; 0 otherwise
8       4     uint32  payload_length  Payload size in bytes
```

The **first byte of the payload** is the message kind. The remaining payload
layout depends on the kind. For `RESULT_BATCH` the rest of the payload is the
existing ingress payload format (optional delta symbol dictionary followed
by exactly one table block); for control kinds the layout is defined per
kind in sections 6 through 11.

```
+-----------------------------------------+
| Header (12 bytes)                       |
+-----------------------------------------+
| Payload                                 |
|   +-----------------------------------+ |
|   | msg_kind: uint8                   | |
|   | (kind-specific body)              | |
|   +-----------------------------------+ |
+-----------------------------------------+
```

Putting `msg_kind` in the payload (rather than the header) keeps the codec
shared with ingress: header parsing, framing, and length checks are identical.
Endpoint disambiguation is sufficient because connections are direction-pure.

## 5. Message Kinds

| Code   | Name              | Direction | Body                                  |
|--------|-------------------|-----------|---------------------------------------|
| `0x00` | DATA_BATCH        | C -> S    | Defined in ingress spec (not used here)|
| `0x01` | RESPONSE          | S -> C    | Defined in ingress spec (not used here)|
| `0x10` | QUERY_REQUEST     | C -> S    | SQL plus bind parameters              |
| `0x11` | RESULT_BATCH      | S -> C    | One table block of result rows        |
| `0x12` | RESULT_END        | S -> C    | Cursor exhausted                      |
| `0x13` | QUERY_ERROR       | S -> C    | Mid-stream error                      |
| `0x14` | CANCEL            | C -> S    | Stop a running query                  |
| `0x15` | CREDIT            | C -> S    | Extend the byte window                |
| `0x16` | EXEC_DONE         | S -> C    | Non-SELECT statement acknowledgement  |
| `0x17` | SERVER_INFO       | S -> C    | Unsolicited, first frame on v2 only; carries role + cluster identity |

`0x18` through `0x1F` are reserved for future egress kinds (prepared
statements, transactions, server-driven keepalives). `0x20+` is reserved for
future protocol extensions.

## 6. QUERY_REQUEST (0x10)

Client to server. Initiates a new cursor.

```
+---------------------------------------------------------+
| msg_kind:        uint8       0x10                       |
| request_id:      int64       Client-assigned, unique    |
|                              within the connection      |
| sql_length:      varint      UTF-8 byte length          |
| sql_bytes:       bytes       SQL text                   |
| initial_credit:  varint      Bytes; 0 = unbounded       |
| bind_count:      varint      Number of bind parameters  |
| For each bind parameter (in declaration order):         |
|   bind_block:    column_data Reuses ingress column      |
|                              encoding with row_count = 1|
+---------------------------------------------------------+
```

### Bind parameters

A bind parameter is encoded **exactly as a one-row column** under the ingress
spec (§11 null bitmap, §12 column encoding). Each block begins with a
`type_code: uint8` from the type table, followed by the standard `null_flag`
byte and either zero or one value.

Reusing the ingress encoding has two consequences:

- A NULL bind parameter is `type_code` + `null_flag = 0x01` + `bitmap byte
  0x01`, with no value bytes following.
- DECIMAL binds carry the 1-byte scale prefix; ARRAY binds carry the
  per-row dimension header. Symbol bind parameters are encoded as STRING
  rather than SYMBOL (no dictionary for a single value).

**Server leniency note**: the Phase 1 server decoder accepts a SYMBOL wire
type code for a bind parameter and treats it identically to STRING (single
UTF-8 value, dispatched to {@code BindVariableService.setStr}). Compliant
clients should still send STRING. A future revision may tighten this to
reject SYMBOL bind type codes.

### request_id

64-bit client-assigned identifier. It is echoed back by every
server-to-client frame related to the query (`RESULT_BATCH`, `RESULT_END`,
`QUERY_ERROR`). The client may reuse a `request_id` only after it has
observed the terminator for the previous use.

### Concurrency

The wire protocol allows a connection to have multiple in-flight queries:
the server may interleave their `RESULT_BATCH` frames freely and clients
demultiplex using `request_id`. The protocol does not constrain ordering
between requests.

**Phase 1 (current implementation): a single in-flight query per connection.**
The server rejects any second `QUERY_REQUEST` that arrives before the active
query has terminated (`RESULT_END`, `EXEC_DONE`, or `QUERY_ERROR`) with a
`QUERY_ERROR` carrying `STATUS_PARSE_ERROR` and a message naming the
limitation. Multi-query multiplexing requires a fair scheduler on the server
and is tracked in the [Phase 2 backlog](QWP_EGRESS_PHASE2_BACKLOG.md). SDK authors targeting Phase 1 should
serialise queries on a per-connection basis or open additional connections.

## 7. RESULT_BATCH (0x11)

Server to client. Carries one table block of result rows.

```
+---------------------------------------------------------+
| msg_kind:        uint8       0x11                       |
| request_id:      int64       From the originating       |
|                              QUERY_REQUEST              |
| batch_seq:       varint      Monotonic per request,     |
|                              starting at 0              |
| (rest of payload is the ingress payload format:         |
|  optional delta symbol dictionary if FLAG_DELTA_SYMBOL_DICT
|  is set in the header flags, then exactly one table block.)
+---------------------------------------------------------+
```

The header's `table_count` field is `1`. The header's `flags` byte uses the
same bit definitions as ingress plus an egress-specific compression bit:

| Bit    | Name                    | Meaning                                                                                      |
|--------|-------------------------|----------------------------------------------------------------------------------------------|
| `0x04` | `FLAG_GORILLA`          | The batch may use per-column Gorilla delta-of-delta encoding on TIMESTAMP / TIMESTAMP_NANOS / DATE columns. |
| `0x08` | `FLAG_DELTA_SYMBOL_DICT`| The batch carries a connection-scoped delta symbol-dictionary section (§12).                 |
| `0x10` | `FLAG_ZSTD`             | The payload region after the `msg_kind` / `request_id` / `batch_seq` prelude is zstd-compressed. |

`FLAG_GORILLA` and `FLAG_DELTA_SYMBOL_DICT` are always set on `RESULT_BATCH`
frames under Phase 1 (both features are unconditionally active). `FLAG_ZSTD`
is set on a per-batch basis when compression has been negotiated (§3) and
the compressed form is smaller than the raw form.

When `FLAG_GORILLA` is set, every TIMESTAMP / TIMESTAMP_NANOS / DATE column
carries a 1-byte encoding discriminator immediately before its value region:
`0x00` = raw `int64` values; `0x01` = Gorilla bitstream. The server picks
Gorilla when the column has at least three non-null values and the
delta-of-delta bitstream is smaller than `nonNull * 8` bytes; unordered or
jumpy columns fall back to raw.

The schema section inside the table block follows the standard rules:

- **First batch for a query**: schema mode `0x00` (full), with a new
  schema_id assigned by the server.
- **Subsequent batches with the same column set**: schema mode `0x01`
  (reference), reusing the same schema_id.

If the result set is empty, the server still sends one `RESULT_BATCH` with
`row_count = 0` so the client receives the schema, followed by `RESULT_END`.

The table block's `name` field carries an empty string (`name_length = 0`).
Result sets do not have a table name; clients should ignore the field.

## 8. RESULT_END (0x12)

Server to client. Signals successful end of stream.

```
+---------------------------------------------------------+
| msg_kind:    uint8        0x12                          |
| request_id:  int64                                       |
| final_seq:   varint       Sequence number of the last   |
|                           RESULT_BATCH (or 0 if none)   |
| total_rows:  varint       Total rows produced; 0 if not |
|                           tracked by the server         |
+---------------------------------------------------------+
```

`table_count` in the header is `0`.

After `RESULT_END`, the server has no further state for this `request_id`
and the client may reuse it.

## 9. QUERY_ERROR (0x13)

Server to client. Signals failure at any point in the lifecycle. May arrive
before any `RESULT_BATCH` (parse failure, security failure) or partway
through a stream (storage failure, cancellation acknowledged, server
shutdown).

```
+---------------------------------------------------------+
| msg_kind:    uint8        0x13                          |
| request_id:  int64                                       |
| status:      uint8        See §15                       |
| msg_length:  uint16       UTF-8 byte length             |
| msg_bytes:   bytes        Human-readable error message  |
+---------------------------------------------------------+
```

`table_count` in the header is `0`. `QUERY_ERROR` is terminal: the client
must not expect any further frames for this `request_id`.

## 10. CANCEL (0x14)

Client to server. Requests termination of a running query.

```
+---------------------------------------------------------+
| msg_kind:    uint8        0x14                          |
| request_id:  int64                                       |
+---------------------------------------------------------+
```

The server acknowledges by emitting either `RESULT_END` (if the cursor
happened to finish first) or `QUERY_ERROR` with status `CANCELLED`. The
client must continue to drain any in-flight `RESULT_BATCH` frames the server
sent before processing the cancel; the terminator is the synchronization
point.

If `request_id` does not refer to an active query the server silently drops
the cancel.

## 11. CREDIT (0x15)

Client to server. Extends the byte-credit window.

```
+---------------------------------------------------------+
| msg_kind:        uint8     0x15                         |
| request_id:      int64                                   |
| additional_bytes: varint   Bytes to add to the window   |
+---------------------------------------------------------+
```

See §14 for the flow-control model.

## 11.5 NULL Sentinel Conventions Inherited from QuestDB

QuestDB uses sentinel values (not a separate null bitmap) for several primitive
types in its internal storage. The egress wire format inherits these conventions
verbatim — the per-cell value IS the sentinel, and the row appears as NULL in the
null bitmap. Implementations consuming egress should treat the following as
indistinguishable from explicit NULL:

| QuestDB type | NULL sentinel | Notes |
|---|---|---|
| INT, IPv4 | `Numbers.INT_NULL = Integer.MIN_VALUE` for INT; `Numbers.IPv4_NULL = 0` for IPv4 | The address `0.0.0.0` cannot be represented as a non-null IPv4. |
| LONG, DATE, TIMESTAMP, TIMESTAMP_NANOS, DECIMAL64 | `Numbers.LONG_NULL = Long.MIN_VALUE` | |
| FLOAT | `Float.NaN` | Any NaN, including `0.0f / 0.0f`, is treated as NULL. |
| DOUBLE | `Double.NaN` | Same as FLOAT — any NaN is NULL. |
| GEOHASH (all widths) | `-1` (sign-extends across BYTE/SHORT/INT/LONG storage) | A geohash whose bit pattern is "all ones" cannot be represented as non-null. |
| UUID | both halves `Numbers.LONG_NULL` | A UUID with both halves Long.MIN_VALUE is NULL. |
| LONG256 | all four longs `Numbers.LONG_NULL` | |
| BOOLEAN, BYTE, SHORT, CHAR | (no NULL sentinel) | These types **cannot** carry NULL in QuestDB; INSERT NULL stores `false`/`0` and the wire row has the null bitmap bit clear. |

Servers writing egress and clients reading it MUST agree on these sentinels. The
spec does not introduce a separate "QWP NaN" representation — a row carrying
`NaN` in the dense values array is simultaneously marked NULL in the null bitmap.

### 11.5.1 Array element NULLs

Array column types (`DOUBLE_ARRAY` 0x18, `LONG_ARRAY` 0x12) ship element bytes
verbatim with **no per-element null bitmap**. Element-level NULL is encoded by
re-using the element-type's row-level sentinel from the table above:

| Array element type | NULL element value | Round-trip risk |
|---|---|---|
| `DOUBLE_ARRAY` element | `Double.NaN` | A non-null `NaN` element (e.g. from `0.0 / 0.0`) is indistinguishable from a NULL element on the wire. |
| `LONG_ARRAY` element  | `Long.MIN_VALUE` (`Numbers.LONG_NULL`) | A non-null `Long.MIN_VALUE` element cannot be represented as non-null and round-trips as NULL. |

This matches QuestDB's in-engine semantics — the engine itself treats these
sentinels as NULL throughout — so the wire format does not lose information
relative to the source. Clients writing array values they expect to round-trip
as non-null MUST avoid the sentinel bit patterns.

The row-level null bitmap bit (set for the whole array cell) signals "the array
itself is NULL", distinct from "an array of zero or more elements, some of
which may be element-NULL". A non-NULL empty array is a valid value.

## 11.6 EXEC_DONE (0x16)

Server to client. Terminates a non-SELECT `QUERY_REQUEST` (DDL, INSERT,
UPDATE, ALTER, DROP, TRUNCATE, CREATE TABLE, CREATE MAT VIEW, and any
parse-time-executed statement). No `RESULT_BATCH` frames are sent for these
statements — the stream collapses to a single acknowledgement.

```
+---------------------------------------------------------+
| msg_kind:      uint8    0x16                            |
| request_id:    int64                                     |
| op_type:       uint8    CompiledQuery.TYPE_* discriminator |
| rows_affected: varint   INSERT / UPDATE row count;      |
|                         0 for statements without a row  |
|                         count (DDL, TRUNCATE, etc.)     |
+---------------------------------------------------------+
```

`table_count` in the header is `0`. `EXEC_DONE` is terminal: the client must
not expect any further frames for this `request_id`.

If the statement fails, the server sends `QUERY_ERROR` (§9) instead.

## 11.7 SERVER_INFO (0x17)

Server to client. Unsolicited frame delivered as the **first WebSocket frame
after the HTTP upgrade response**, and only when the negotiated version is 2
or above. A v1-only client never sees it; a v2 client must consume it before
submitting the first `QUERY_REQUEST`.

```
+---------------------------------------------------------+
| msg_kind:        uint8    0x17                          |
| role:            uint8    see role table below          |
| epoch:           uint64   monotonic role epoch          |
| capabilities:    uint32   reserved bitfield (0 in v2)   |
| server_wall_ns:  int64    server wall-clock, ns since   |
|                           1970-01-01T00:00Z             |
| cluster_id_len:  uint16   UTF-8 byte length             |
| cluster_id:      bytes    UTF-8, up to 65535 bytes      |
| node_id_len:     uint16   UTF-8 byte length             |
| node_id:         bytes    UTF-8, up to 65535 bytes      |
+---------------------------------------------------------+
```

| Value  | Role               | Semantics                                                                                |
|--------|--------------------|------------------------------------------------------------------------------------------|
| `0x00` | `STANDALONE`       | No replication configured. OSS single-node default; behaves like a primary for routing. |
| `0x01` | `PRIMARY`          | Authoritative write node; reads see latest commits.                                      |
| `0x02` | `REPLICA`          | Read-only replica; reads may lag the primary by up to the replication poll interval.    |
| `0x03` | `PRIMARY_CATCHUP`  | Promotion in flight; behaves like a primary but is still uploading in-flight segments.  |

The `epoch` field is monotonic across role transitions on the same node
(replica promoted to primary, primary demoted to replica). Clients tracking a
specific primary use it to refuse a stale reconnect that lands on a node
which no longer believes it is primary at the current cluster epoch. The
field is 0 on releases where no fencing has been wired up yet; it is safe
for clients to ignore it as a hint rather than a guarantee.

`cluster_id` and `node_id` are free-form identifiers supplied by the server
operator. Clients may surface them in diagnostics and in error messages
produced by the role filter (§"Client routing" below).

The `capabilities` field is a reserved bitfield for future protocol
extensions (freshness-watermark reads, multi-query multiplexing, etc.). v2
servers and clients set it to zero.

`SERVER_INFO` is delivered in the same TCP/WebSocket send buffer as the 101
upgrade response, so on a healthy connection the frame is already in the
client's kernel recv buffer by the time the client parses the upgrade. If the
server negotiates v1 it omits the frame entirely and clients fall back to
the "role unknown" path (equivalent to `STANDALONE` for routing purposes).

### Client routing (`target=` and `failover=`)

Clients that support v2 accept a comma-separated list of endpoints and a
`target=` filter on the connection string:

```
ws::addr=db-a:9000,db-b:9000,db-c:9000;target=primary;failover=on;
```

| Target        | Accepted roles                                                                                 |
|---------------|-----------------------------------------------------------------------------------------------|
| `any` (default) | `STANDALONE`, `PRIMARY`, `PRIMARY_CATCHUP`, `REPLICA`                                         |
| `primary`     | `STANDALONE`, `PRIMARY`, `PRIMARY_CATCHUP`                                                     |
| `replica`     | `REPLICA`                                                                                      |

On `connect()` the client walks the endpoint list in order, reads each
endpoint's `SERVER_INFO`, and picks the first one whose role passes the
filter. Endpoints that don't match are closed and skipped. When every
endpoint is tried and none matches the filter, the client raises a
role-mismatch error and attaches the most recent `SERVER_INFO` observed so
callers can distinguish "no primary available" from "all endpoints
unreachable".

With `failover=on` (the default), a transport failure mid-query triggers a
transparent reconnect to another endpoint that still matches the filter, and
the client re-submits the in-flight `QUERY_REQUEST`. Accumulating handlers
observe a `onFailoverReset` callback (in the Java client) just before
replayed batches start arriving with `batch_seq` restarting at 0 on the new
node. `failover=off` restores the pre-v2 behaviour where transport failures
surface directly through `onError`.

## 12. Schema and Symbol Dictionary Scope

### Schema registry (per connection)

The server maintains a per-connection schema registry keyed by `schema_id`.
The first `RESULT_BATCH` for a query registers a new schema in mode `0x00`;
subsequent batches with the same column set reference it in mode `0x01`.

A query whose column set differs from a prior registration receives a fresh
`schema_id`. The server may garbage-collect schema entries that no longer
correspond to any active query, but is not required to.

On disconnect, both sides reset the registry.

### Symbol dictionary (per connection)

Egress uses a **connection-scoped delta dictionary** (the `FLAG_DELTA_SYMBOL_DICT`
mechanic from ingress). The server maintains a global mapping of symbol strings
to sequential integer IDs starting at 0, shared across every query on the
connection. Each `RESULT_BATCH` carries a delta section listing newly added
symbols, placed between the `batch_seq` prelude and the table block:

```
delta_start: varint    First conn-id assigned in this batch
delta_count: varint    Number of new entries (may be 0)
For each new entry (in order):
  entry_length: varint
  entry_bytes:  UTF-8 bytes

Then, for every SYMBOL column in the table block's column-data section:
For each non-null row:
  conn_id:      varint  Index into the connection-scoped dictionary
```

`FLAG_DELTA_SYMBOL_DICT` is always set on `RESULT_BATCH` in Phase 1. The
client accumulates delta entries for the lifetime of the connection. On
disconnect, both sides reset the dictionary.

Per-connection scope pays off for repeated queries (BI dashboards refreshing
the same SELECTs) at the cost of unbounded growth on long-lived connections
that surface high-cardinality symbols. The server enforces the
symbol-dictionary limit (§16) by failing queries that would push the
connection's dictionary past the cap.

## 13. Cursor Lifecycle

```
                       QUERY_REQUEST
   client  -----------------------------------> server
                                                  |
                                                  v
                                          (parse, plan, open cursor)
                                                  |
                                                  v
   client  <---------------- RESULT_BATCH(seq=0) -----  schema mode 0x00
   client  <---------------- RESULT_BATCH(seq=1) -----  schema mode 0x01
   client  <---------------- RESULT_BATCH(seq=N) -----
                                                  |
                                                  v
   client  <----------------- RESULT_END -------------
```

Error path:

```
   client  <---------------- RESULT_BATCH(seq=K) -----
   client  <----------------- QUERY_ERROR -----------  (terminal)
```

Cancel path:

```
   client  ----------------- CANCEL ----------------->
   client  <------- (any in-flight RESULT_BATCH) -----
   client  <----------------- QUERY_ERROR -----------  status = CANCELLED
                                              (or RESULT_END if it raced)
```

A connection-level error (parse failure on the message frame itself,
authentication failure, malformed header) closes the WebSocket. The
server's last frame before close should be a `QUERY_ERROR` with
`request_id = -1` if the failure is not attributable to a specific request.

## 14. Flow Control

Egress uses **byte credits with a row floor**.

### Initial credit

The client sets `initial_credit` in `QUERY_REQUEST`. A value of `0` means
"unbounded": the server streams without waiting for credit. A nonzero value
is the byte budget the server may emit before pausing.

### Granting more credit

The client sends `CREDIT` frames to extend the window. The server adds
`additional_bytes` to the remaining budget. There is no upper bound on a
single grant.

### Accounting

The server decrements the budget by the **total wire length of each
`RESULT_BATCH`** (header + payload). When the budget would go non-positive,
the server pauses production for that `request_id`.

### Row floor

To prevent deadlock on rows larger than the remaining window, the server is
permitted to send **one additional `RESULT_BATCH` of at least one row** even
if doing so drives the budget negative. The next batch will not be sent
until credit returns to a positive value.

The row floor guarantees forward progress for any well-formed query
regardless of credit size, at the cost of the budget being a soft ceiling
(the client may briefly receive more bytes than it granted). Clients should
size buffers to absorb up to one extra batch.

### Independence per request

Each `request_id` has its own credit accounting. Granting credit on one
request does not unblock another.

## 15. Status Codes

`QUERY_ERROR` reuses the ingress status code namespace and adds two
egress-specific codes:

| Code | Hex    | Name              | Description                                          |
|------|--------|-------------------|------------------------------------------------------|
| 3    | `0x03` | SCHEMA_MISMATCH   | Bind parameter type incompatible with placeholder     |
| 5    | `0x05` | PARSE_ERROR       | Malformed message or SQL syntax error                |
| 6    | `0x06` | INTERNAL_ERROR    | Server-side execution failure                        |
| 8    | `0x08` | SECURITY_ERROR    | Authorization failure                                |
| 10   | `0x0A` | CANCELLED         | Query terminated in response to CANCEL               |
| 11   | `0x0B` | LIMIT_EXCEEDED    | A protocol limit was hit (see §16)                   |

`OK` is not used in egress; success terminates with `RESULT_END`, not a
status code.

## 16. Limits

Egress inherits ingress limits where they apply, with the following
additions and changes:

| Limit                          | Default Value | Notes                            |
|--------------------------------|---------------|----------------------------------|
| Max in-flight queries          | 1 (Phase 1)   | Per connection. Wire protocol allows up to 64; the Phase 1 server rejects any second concurrent QUERY_REQUEST. See §6 Concurrency. |
| Max SQL text length            | 1 MiB         | UTF-8 bytes                      |
| Max bind parameters            | 1,024         | Per QUERY_REQUEST                |
| Max RESULT_BATCH wire size     | 16 MiB        | Same as ingress batch ceiling    |
| Max symbol dictionary entries  | 1,000,000     | Per connection                   |
| Min initial credit             | 0             | 0 means unbounded                |

## 17. Examples

### Example 1: Simple unbounded query

Client sends `SELECT id, value FROM sensors LIMIT 2` with no bind
parameters and no credit window.

```
QUERY_REQUEST:
  Header:
    51 57 50 31         magic "QWP1"
    01                  version 1
    00                  flags
    00 00               table_count = 0
    XX XX XX XX         payload_length

  Payload:
    10                  msg_kind = QUERY_REQUEST
    01 00 00 00 00 00 00 00   request_id = 1
    24                  sql_length = 36
    53 45 4C 45 43 54 20 69 64 2C 20 76 61 6C 75 65
    20 46 52 4F 4D 20 73 65 6E 73 6F 72 73 20 4C 49
    4D 49 54 20 32     "SELECT id, value FROM sensors LIMIT 2"
    00                  initial_credit = 0 (unbounded)
    00                  bind_count = 0
```

Server responds:

```
RESULT_BATCH (seq=0):
  Header:
    51 57 50 31         magic "QWP1"
    01                  version 1
    00                  flags
    01 00               table_count = 1
    XX XX XX XX         payload_length

  Payload:
    11                  msg_kind = RESULT_BATCH
    01 00 00 00 00 00 00 00   request_id = 1
    00                  batch_seq = 0

    Table block:
      00                  name_length = 0 (anonymous)
      02                  row_count = 2
      02                  column_count = 2

      Schema (full mode):
        00                schema_mode = FULL
        00                schema_id = 0
        02 69 64    05    "id" : LONG
        05 76 61 6C 75 65  07    "value" : DOUBLE

      Column 0 (LONG):
        00                null_flag = 0
        01 00 00 00 00 00 00 00
        02 00 00 00 00 00 00 00

      Column 1 (DOUBLE):
        00                null_flag = 0
        CD CC CC CC CC CC F4 3F     1.3
        9A 99 99 99 99 99 01 40     2.2

RESULT_END:
  Header:
    51 57 50 31  01  00  00 00  XX XX XX XX

  Payload:
    12                  msg_kind = RESULT_END
    01 00 00 00 00 00 00 00   request_id = 1
    00                  final_seq = 0
    02                  total_rows = 2
```

### Example 2: Bind parameter

Bind parameter `?` of type LONG with value `42`:

```
Inside QUERY_REQUEST after bind_count = 1:

  05                       type_code = LONG
  00                       null_flag = 0 (no nulls)
  2A 00 00 00 00 00 00 00  value = 42 (one-row column)
```

A NULL LONG bind:

```
  05                       type_code = LONG
  01                       null_flag = nonzero (bitmap follows)
  01                       bitmap byte: bit 0 set = NULL
                           (no value bytes)
```

### Example 3: Credit-controlled streaming

Client opens a query with a 64 KiB initial credit:

```
QUERY_REQUEST: initial_credit = 65536, request_id = 7
```

Server emits `RESULT_BATCH` frames totaling 60 KiB, then pauses. Client
processes them and grants more:

```
CREDIT:
  Payload:
    15                          msg_kind = CREDIT
    07 00 00 00 00 00 00 00     request_id = 7
    80 80 04                    additional_bytes = 65536
```

Server resumes streaming.

## 18. Open Questions

These belong in a follow-up RFC, not the initial implementation:

1. **Server-pushed keepalives.** Long-running queries with sparse output
   may need a `RESULT_KEEPALIVE` kind so clients can distinguish a slow
   query from a stalled connection. WebSocket pings cover the connection
   but not per-request liveness.
2. **Prepared statements.** A `PREPARE` / `EXECUTE` split would let the
   server cache the parse and plan. Adds two more message kinds and a
   server-side handle scope.
3. **Backpressure feedback to the planner.** A query that materializes a
   large intermediate result before producing rows can starve credit. The
   server may want to reject queries whose first-row latency exceeds a
   threshold rather than exhaust the credit window before the first batch.
4. **Result spooling on cancel.** Whether the server discards or completes
   in-progress writes to the result buffer when a cancel arrives is
   implementation-defined; the protocol only requires the terminator.
