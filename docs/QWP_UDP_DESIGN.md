# QWP over UDP: Design Document

## Motivation

QuestDB needs a lightweight, fire-and-forget ingestion path for technical
metadata (metrics, internal telemetry, diagnostic events). This data is
high-volume but low-criticality -- if individual datagrams are lost, it is
acceptable. The existing UDP sender uses the legacy ILP text-based protocol. A
binary sender based on QWP (ILP v4) would offer better throughput, type
richness (22 column types vs. 5), and closer alignment with the WebSocket
transport.

## Design Constraints

- **Self-contained datagrams.** Each UDP datagram must be independently
  decodable. There is no application-level session, no connection state
  accumulated across datagrams.
- **Unicast and multicast.** The sender and receiver must work with both
  delivery modes.
- **No authentication or encryption.** Simplifies the protocol and is
  consistent with the existing UDP ILP sender.
- **Single table per datagram.** Each datagram carries exactly one table block.
  This keeps datagram construction simple and sizing predictable.
- **MTU-aware.** Datagrams should fit within a single IP packet to avoid
  fragmentation. The safe payload ceiling is ~1,400 bytes (1,472 for IPv4
  with 1,500-byte MTU minus IP and UDP headers).

## Wire Format

QWP-over-UDP reuses the ILP v4 binary wire format with the following
simplifications imposed by the self-contained datagram constraint:

### What Stays the Same

| Aspect                    | Notes                                               |
|---------------------------|-----------------------------------------------------|
| Message header            | 12 bytes: magic `QWP1`, version, flags, table count (always 1), payload length |
| Column type codes         | All 22 types (0x01 - 0x16), same wire encoding      |
| Null bitmaps              | 1-byte null flag + `ceil(rows/8)` bitmap bytes, LSB-first, bit=1 means NULL |
| Varint encoding           | Unsigned LEB128, identical                           |
| Fixed-width column layout | Contiguous arrays, little-endian (big-endian for DECIMALs only)           |
| String/VARCHAR encoding   | Offset array + concatenated UTF-8                    |
| Boolean bit-packing       | 8 values per byte, LSB-first                         |
| Array encoding            | nDims + dim lengths + flattened values                |
| Decimal scale prefix      | 1-byte scale before big-endian value array            |
| GeoHash encoding          | Varint precision + packed values                      |

### What Changes

| WebSocket QWP                          | UDP QWP                                  | Rationale                                           |
|----------------------------------------|------------------------------------------|-----------------------------------------------------|
| Schema reference mode (0x01)           | **Full schema only (0x00)**              | No session state to store schema definitions        |
| Delta symbol dictionary (FLAG 0x08)    | **Per-column dictionary only**           | No cross-datagram dictionary accumulation. Each SYMBOL column carries its own dictionary inline (dict_size + entries + indices), same as the existing non-delta wire format in `QwpSymbolColumnCursor`. |
| Gorilla timestamp encoding (FLAG 0x04) | **Disabled**                             | Marginal benefit at sub-1,400 byte datagram scale; simplifies encoder |
| Multi-table batches                    | **Single table per datagram**            | Simplicity and predictable sizing                   |
| Response/ACK channel                   | **None (fire-and-forget)**               | UDP has no built-in response channel; consistent with best-effort semantics |
| Pipelining / in-flight window          | **N/A**                                  | No acknowledgments to track                          |
| Max batch size: 16 MB                  | **~1,400 bytes effective payload**       | MTU constraint                                       |

### Header Flags Byte for UDP

Since Gorilla and delta symbol dictionary are disabled, the flags byte is
always `0x00` in the initial implementation. LZ4/Zstd compression flags are
reserved for future use but are not implemented initially (compression of
sub-1,400 byte payloads offers marginal benefit).

### Datagram Layout

```
┌──────────────────────────────────────────────────────────────┐
│ UDP payload (max ~1,400 bytes)                               │
│                                                              │
│  Message Header (12 bytes)                                   │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Magic:          4 bytes LE  "QWP1" (0x31505751)        │  │
│  │ Version:        1 byte      0x01                       │  │
│  │ Flags:          1 byte      0x00                       │  │
│  │ Table count:    2 bytes LE  0x0001 (always 1)          │  │
│  │ Payload length: 4 bytes LE  (byte count after header)  │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
│  Payload                                                     │
│  ┌────────────────────────────────────────────────────────┐  │
│  │ Table name length:  varint                             │  │
│  │ Table name:         UTF-8 bytes                        │  │
│  │ Row count:          varint                             │  │
│  │ Column count:       varint                             │  │
│  │                                                        │  │
│  │ Schema (always full, mode 0x00)                        │  │
│  │ ┌──────────────────────────────────────────────────┐   │  │
│  │ │ 0x00                                             │   │  │
│  │ │ For each column:                                 │   │  │
│  │ │   name_len: varint                               │   │  │
│  │ │   name:     UTF-8 bytes                          │   │  │
│  │ │   type:     uint8 (type code, 0x01-0x16)          │   │  │
│  │ └──────────────────────────────────────────────────┘   │  │
│  │                                                        │  │
│  │ Column data (repeated for each column)                 │  │
│  │ ┌──────────────────────────────────────────────────┐   │  │
│  │ │ null_flag: uint8 (0=no nulls, nonzero=bitmap)    │   │  │
│  │ │ [Null bitmap if flag != 0: ceil(rows/8) bytes]   │   │  │
│  │ │ [Type-specific encoded values]                   │   │  │
│  │ └──────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────┘  │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

This is a strict subset of the WebSocket QWP format. A server that understands
WebSocket QWP can decode a UDP datagram by checking `flags == 0x00` and
`table_count == 1`. Conversely, any standard QWP v1 decoder handles UDP
datagrams without modification.

## Client Side

### Public API

The UDP sender implements the existing `Sender` interface, preserving the
fluent API:

```java
try (Sender sender = Sender.fromConfig("udp::addr=host:port;")) {
    sender.table("cpu_metrics")
          .symbol("host", "server-1")
          .doubleColumn("usage", 73.2)
          .longColumn("cores", 8)
          .atNow();
}
```

Key behavioral differences from the WebSocket sender:

| Aspect         | WebSocket Sender              | UDP Sender                          |
|----------------|-------------------------------|-------------------------------------|
| `flush()`      | Waits for server ACK          | Sends immediately, returns at once  |
| `at()`/`atNow()` | Buffers row in microbatch  | Buffers row; auto-flushes if datagram would exceed MTU (see below) |
| Error handling | Throws on server error        | Throws `LineSenderException` only for programming errors (single row exceeds MTU, API misuse). Network `sendto()` failures are logged at WARN and silently swallowed -- consistent with fire-and-forget semantics. |
| `close()`      | Flushes, waits, disconnects   | Flushes remaining buffer, closes socket |

Note on `Sender` interface contract: The `Sender` Javadoc describes retry
semantics (call `flush()` again after error, or `reset()` to discard). For the
UDP sender, `flush()` never fails due to server-side errors (there is no
response channel). It can only fail due to local socket errors, in which case
retry semantics still apply. The `reset()` method discards the current
datagram buffer as expected.

### Auto-Flush Behavior

The sender accumulates rows for the current table and flushes a datagram when:

1. **Calling `table()` with a different table name** -- flushes the current
   table's datagram before switching.
2. **Buffer approaching MTU limit** -- the sender tracks encoded size and
   flushes before exceeding the configured max datagram size.
3. **Calling `flush()` explicitly** -- sends whatever is buffered.
4. **Calling `close()`** -- flushes before closing the socket.

There is no time-based auto-flush (unlike the WebSocket sender's 100 ms
interval), because UDP datagrams are cheap to send and there is no session
overhead.

### MTU Handling and Row Overflow

The sender tracks the estimated encoded size of the current datagram. The size
check happens **before** committing a row (i.e., inside `at()`/`atNow()`),
not after. The behavior depends on whether the datagram already contains
previous rows:

1. **Datagram has previous rows and adding this row would exceed MTU:**
   The sender flushes the datagram with the previously accumulated rows,
   then starts a fresh datagram with the current row. This is the common
   auto-split path.

2. **Datagram is empty and a single row exceeds MTU:** The sender throws
   `LineSenderException`. This is a programming error -- the caller is
   sending data that cannot physically fit in one datagram. No silent drop.

3. **Row fits:** The row is committed to the buffer normally.

This guarantees that `at()`/`atNow()` never silently loses a row. Either the
row is buffered for sending, or the caller gets an exception it can act on.

The caller can tune `maxDatagramSize` to match their network's MTU (default:
1,400 bytes).

### Size Estimation

To decide when to flush, the sender needs a cheap size estimate before
encoding. The approach:

- **Schema overhead**: computed once per table when `table()` is called
  (table name + column defs are known). Cached on the `QwpTableBuffer`.
- **Per-row overhead**: fixed-width columns contribute a known constant;
  variable-width columns (VARCHAR, SYMBOL, arrays) contribute their actual
  byte count tracked by `QwpTableBuffer.ColumnBuffer`.
- The sender checks `12 (header) + schemaOverhead + currentDataSize` against
  `maxDatagramSize` inside `at()`/`atNow()`, before committing the row.

The estimate must be conservative (overestimate rather than underestimate) to
avoid producing datagrams that exceed MTU after encoding. For SYMBOL columns,
the per-column dictionary size is variable and depends on distinct values; the
estimator accounts for the current dictionary state at estimation time.

### Reuse from WebSocket QWP

| Component              | Reuse Level  | Notes                                            |
|------------------------|-------------|--------------------------------------------------|
| `QwpTableBuffer`       | 100%        | Columnar accumulation, type-specific add methods  |
| `QwpConstants`         | 100%        | Type codes, flags, limits                         |
| `NativeBufferWriter`   | 100%        | Off-heap buffer with varint/string/block writes   |
| `QwpGorillaEncoder`    | Not used    | Disabled for UDP                                  |
| `GlobalSymbolDictionary` | Not used  | No cross-datagram state                          |
| `QwpColumnWriter`      | ~80%        | Column encoding logic reused; header/symbol dict code forked |
| `Sender` interface     | 100%        | UDP sender implements it                          |

The column encoding methods in `QwpColumnWriter` (`writeBooleanColumn`,
`writeStringColumn`, `writeDecimal*Column`, etc.) are transport-agnostic and
shared between WebSocket and UDP transports.

### New Client Classes

```
java-questdb-client/core/src/main/java/io/questdb/client/cutlass/qwp/
├── client/
│   ├── QwpUdpSender.java           Main sender, implements Sender
│   └── QwpUdpEncoder.java          Encodes single-table datagram
└── protocol/
    (no new files -- reuse existing QwpTableBuffer, QwpConstants, etc.)
```

### `QwpUdpSender`

Responsibilities:
- Manages a UDP socket (unicast or multicast).
- Holds one `QwpTableBuffer` for the current table.
- Tracks encoded size to decide when to flush.
- On flush: encodes with `QwpUdpEncoder`, sends via `sendto()`.

Constructor parameters (matching existing `LineUdpSender` pattern):
- `interfaceIPv4Address` -- network interface to bind to.
- `sendToIPv4Address` -- target address (unicast or multicast group).
- `port` -- target port.
- `maxDatagramSize` -- max UDP payload (default 1,400).
- `ttl` -- multicast TTL (default 2).

### `QwpUdpEncoder`

A simplified encoder that reuses `QwpColumnWriter` for column encoding:
- Removed: delta symbol dictionary, schema reference mode, Gorilla encoding
  tag.
- Added: always writes full schema, always uses per-column symbol dictionary
  (same non-delta wire format as `QwpColumnWriter.writeSymbolColumn()`).
- Same `encodeColumn()` dispatch and all type-specific write methods.

## Server Side

### Architecture

The server-side UDP receiver mirrors the existing `LineUdpReceiver`
architecture but replaces the text-based ILP lexer/parser with the binary
QWP decoder:

```
UDP socket
  │
  ▼
QwpUdpReceiver (reads datagrams)
  │
  ▼
QwpMessageHeader.parse() (validates magic, version, flags)
  │
  ▼
QwpMessageCursor (iterates table blocks -- always 1 for UDP)
  │
  ▼
QwpTableBlockCursor (iterates columns within the table)
  │
  ▼
QwpWalAppender (writes to WAL)
```

### Reuse from WebSocket QWP Server

| Component               | Reuse Level | Notes                                          |
|-------------------------|-------------|-------------------------------------------------|
| `QwpMessageHeader`      | 100%        | Same 12-byte header parsing                     |
| `QwpMessageCursor`      | 100%        | Table block iteration                            |
| `QwpTableBlockCursor`   | 100%        | Column iteration within table                    |
| `QwpSchema`             | 100%        | Full schema parsing (always mode 0x00)           |
| All column cursors      | 100%        | `QwpFixedWidthColumnCursor`, `QwpBooleanColumnCursor`, `QwpStringColumnCursor`, `QwpSymbolColumnCursor`, `QwpGeoHashColumnCursor`, etc. |
| `QwpWalAppender`        | 100%        | Column-type mapping, WAL writes, auto-create     |
| `QwpNullBitmap`         | 100%        | Null bitmap reading                              |
| `QwpVarint`             | 100%        | Varint decoding                                  |

The entire decoding pipeline is transport-agnostic. The WebSocket processor
(`QwpWebSocketUpgradeProcessor`) handles WebSocket frame parsing and response
writing -- none of that applies to UDP. The message payload parsing and WAL
appending are fully reusable.

### New Server Classes

```
core/src/main/java/io/questdb/cutlass/qwp/udp/
├── QwpUdpReceiver.java              Generic UDP receiver
├── QwpUdpReceiverConfiguration.java Configuration interface
├── DefaultQwpUdpReceiverConfiguration.java Defaults
└── LinuxMMQwpUdpReceiver.java       Linux recvmmsg optimization (optional)
```

### `QwpUdpReceiver`

Follows the pattern of the existing `LineUdpReceiver`:

- **Binding**: `nf.socketUdp()` + `nf.bindUdp()`. For multicast, additionally
  calls `nf.join()`.
- **Threading**: dedicated thread (default) or shared worker pool via
  `SynchronizedJob` interface. Same `AtomicBoolean running` /
  `SOCountDownLatch started/halted` pattern.
- **Receive loop**: `nf.recvRaw(fd, buf, bufLen)` returns one datagram. On
  Linux, `LinuxMMQwpUdpReceiver` uses `nf.recvmmsgRaw()` for multi-datagram
  batching.
- **Datagram validation**: before decoding, the receiver validates that
  `HEADER_SIZE + header.payloadLength <= receivedBytes`. This is critical
  because `QwpMessageCursor.of()` computes `payloadEnd` from the header's
  `payloadLength` field without checking it against the actual received
  datagram size (see `QwpMessageCursor.java:190`). For WebSocket this is
  safe because framing guarantees the full message is present, but for UDP
  a crafted or truncated datagram could claim a payload larger than what
  was actually received, causing out-of-bounds memory reads. The receiver
  must clamp or reject such datagrams before passing them to the cursor.
- **Decoding**: after validation, the receiver uses `QwpMessageCursor` /
  `QwpTableBlockCursor` to iterate columns, and `QwpWalAppender` to write
  to WAL.
- **Commit batching**: commits to WAL on a configurable timer
  (`qwp.udp.commit.interval`) or after a configurable number of uncommitted
  datagrams (`qwp.udp.max.uncommitted.datagrams`) to amortize commit overhead.

**No response channel.** The receiver silently drops malformed datagrams and
logs errors at DEBUG level to avoid log flooding.

### Configuration Properties

Following the existing `line.udp.*` naming convention:

```
qwp.udp.enabled                = true/false
qwp.udp.bind.to               = 0.0.0.0:9007
qwp.udp.unicast                = true/false
qwp.udp.join                   = 232.1.2.3   (multicast group)
qwp.udp.own.thread             = true
qwp.udp.own.thread.affinity    = -1
qwp.udp.commit.interval        = 2s
qwp.udp.max.uncommitted.datagrams = 1048576
qwp.udp.msg.buffer.size        = 2048
qwp.udp.msg.count              = 10000   (recvmmsg batch size, Linux only)
qwp.udp.receive.buffer.size    = 8388608 (8 MB OS socket buffer)
```

### Registration

In `Services.createQwpUdpReceiver()`, following the existing factory pattern:

```java
if (Os.isLinux()) {
    return new LinuxMMQwpUdpReceiver(config, engine, workerPool);
}
return new QwpUdpReceiver(config, engine, workerPool);
```

Called from `ServerMain` alongside the existing line UDP receiver setup.

## Size Budget Example

To validate that meaningful data fits within the MTU, here is a worked example
for a typical telemetry row:

**Table:** `cpu_metrics`, 3 columns: `host` (SYMBOL), `usage` (DOUBLE),
designated timestamp (TIMESTAMP).

```
Message header:                           12 bytes
Table name "cpu_metrics" (varint + UTF8): 1 + 11 = 12 bytes
Row count (varint, 1 row):                1 byte
Column count (varint, 3):                 1 byte
Schema mode byte:                         1 byte
Column "host" def (varint + UTF8 + type): 1 + 4 + 1 = 6 bytes
Column "usage" def:                       1 + 5 + 1 = 7 bytes
Column "" (designated ts) def:            1 + 0 + 1 = 2 bytes
Symbol null flag:                         1 byte
Symbol dictionary (1 entry "server-1"):   1 + (1 + 8) = 10 bytes
Symbol index (varint):                    1 byte
Double null flag:                         1 byte
Double value:                             8 bytes
Timestamp null flag:                      1 byte
Timestamp value:                          8 bytes
─────────────────────────────────────────────────
Total:                                    72 bytes
```

At 72 bytes per single row with full schema, there is ample room. With 10
rows (same schema, same symbol), the incremental cost per row is ~17 bytes
(symbol index + double + timestamp), bringing the total to roughly
72 + 9 * 17 = 225 bytes -- comfortably within 1,400 bytes.

For wider schemas (20 columns, long column names), the fixed schema overhead
grows but still typically fits. The sender's size estimator catches overflow
before encoding.

## Open Questions

1. **New magic or same `QWP1`?** Using the same magic means any QWP v1
   decoder works out of the box. A distinct magic (e.g., `QWPu`) would let
   the server distinguish UDP datagrams at the first 4 bytes without
   inspecting the transport layer. Current recommendation: **keep `QWP1`** for
   compatibility.

2. **Sequence numbers.** Adding an optional monotonic sequence number to each
   datagram (e.g., in unused flag bits or an extended header) would let the
   receiver detect gaps for monitoring purposes, without requiring
   retransmission. Not required for v1 but worth considering.

3. **Compression.** LZ4/Zstd compression is unlikely to help at datagram
   scale (< 1,400 bytes). Defer to a future version if telemetry shows
   columnar data in datagrams compresses well.

## Implementation Plan

1. **Client: `QwpUdpEncoder`** -- build on `QwpColumnWriter`, remove delta
   dict / schema ref / Gorilla paths. Add size estimation method.
2. **Client: `QwpUdpSender`** -- implement `Sender` interface. Manage UDP
   socket, `QwpTableBuffer`, auto-flush on MTU threshold.
3. **Client: Builder integration** -- add `Transport.UDP` and `udp::` URI
   scheme to `LineSenderBuilder`.
4. **Server: `QwpUdpReceiver`** -- follow `LineUdpReceiver` pattern, use
   QWP decoder pipeline + `QwpWalAppender`.
5. **Server: Configuration** -- add `qwp.udp.*` properties and
   `QwpUdpReceiverConfiguration`.
6. **Server: Registration** -- wire into `Services` and `ServerMain`.
7. **Tests** -- covering the following categories:

### Test Plan

**Encoder/Decoder round-trip:**
- Encode a table buffer with `QwpUdpEncoder`, decode with
  `QwpMessageCursor` + column cursors, assert values match.
- All 22 column types including nullable variants.
- Multi-row datagrams with per-column symbol dictionaries.

**Sender integration (loopback):**
- `QwpUdpSender` -> `QwpUdpReceiver` over localhost.
- Verify rows appear in WAL/table after commit.
- Multicast group join + receive on loopback.

**MTU boundary cases:**
- Row that fits exactly at the MTU limit (no overflow).
- Row that triggers auto-split (accumulated rows flushed, new row starts
  fresh datagram).
- Single row that exceeds MTU -> `LineSenderException`.
- Table switch (`table()` with different name) flushes previous datagram.

**Malformed datagram handling (server):**
- `payloadLength` in header exceeds actual received bytes -> drop.
- Truncated datagram (received bytes < 12) -> drop.
- Invalid magic bytes -> drop.
- Wrong version byte -> drop.
- Valid header but truncated payload (varint runs past end) -> drop.
- `payloadLength = 0` with `tableCount = 1` -> drop.
- Verify that malformed datagrams do not crash the receiver or corrupt
  state for subsequent valid datagrams.

**Duplicate and out-of-order delivery:**
- Same datagram sent twice -> both rows ingested (idempotency is not
  guaranteed; this is documented behavior).
- Datagrams for the same table arriving out of order -> both ingested,
  timestamp ordering handled by QuestDB storage layer.

**Size estimation accuracy:**
- Encode a datagram, compare actual encoded size against the sender's
  size estimate. Estimate must be >= actual for all type combinations.
