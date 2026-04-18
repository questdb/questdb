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
3. [Version Negotiation](#3-version-negotiation)
4. [Message Structure](#4-message-structure)
5. [Message Kinds](#5-message-kinds)
6. [QUERY_REQUEST (0x10)](#6-query_request-0x10)
7. [RESULT_BATCH (0x11)](#7-result_batch-0x11)
8. [RESULT_END (0x12)](#8-result_end-0x12)
9. [QUERY_ERROR (0x13)](#9-query_error-0x13)
10. [CANCEL (0x14)](#10-cancel-0x14)
11. [CREDIT (0x15)](#11-credit-0x15)
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

## 3. Version Negotiation

Egress shares the QWP version namespace with ingress. The same headers apply:

| Header              | Required | Description                                                                          |
|---------------------|----------|--------------------------------------------------------------------------------------|
| `X-QWP-Max-Version` | No       | Maximum QWP version the client supports. Defaults to 1 if absent.                    |
| `X-QWP-Client-Id`   | No       | Free-form client identifier (e.g., `java-egress/1.0.0`).                             |

The server replies with `X-QWP-Version` selecting `min(clientMax, serverMax)`.
Egress is introduced at version 1; future protocol changes bump the version
on both endpoints together.

The connection-level contract from the ingress spec applies: every message's
header version byte must equal the negotiated version, and the server rejects
mismatches with a parse error and closes the WebSocket.

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

`0x16` through `0x1F` are reserved for future egress kinds (prepared
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

### request_id

64-bit client-assigned identifier. It is echoed back by every
server-to-client frame related to the query (`RESULT_BATCH`, `RESULT_END`,
`QUERY_ERROR`). The client may reuse a `request_id` only after it has
observed the terminator for the previous use.

### Concurrency

A connection may have multiple in-flight queries. The server interleaves
their `RESULT_BATCH` frames freely; clients demultiplex using `request_id`.
The protocol does not constrain ordering between requests.

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
same bit definitions as ingress (`FLAG_GORILLA = 0x04`,
`FLAG_DELTA_SYMBOL_DICT = 0x08`).

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

Egress uses **global delta dictionary mode** (`FLAG_DELTA_SYMBOL_DICT`)
exclusively. The server accumulates symbol entries across all queries on the
connection. The lifecycle mirrors the ingress dictionary, with directions
reversed:

- The server maintains a global mapping of symbol strings to sequential
  integer IDs starting at 0.
- Each `RESULT_BATCH` may carry a delta dictionary section at the start of
  the payload (after the `msg_kind`/`request_id`/`batch_seq` header,
  immediately before the table block) listing newly added symbols.
- The client accumulates the delta entries for the lifetime of the
  connection.
- Symbol columns in result batches contain varint-encoded global IDs.
- On disconnect, both sides reset the dictionary.

Per-connection scope is the design choice that pays off for repeated queries
(BI dashboards, dashboards refreshing the same SELECTs) at the cost of
unbounded growth on long-lived connections that surface high-cardinality
symbols. The server enforces the symbol-dictionary limit (§16) by failing
queries that would push the connection's dictionary past the cap.

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
| Max in-flight queries          | 64            | Per connection                   |
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
