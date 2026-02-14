# Branch Research: `jh_experiment_new_ilp` - ILP v4 WebSocket Protocol

**Date**: 2026-02-14
**Branch**: `jh_experiment_new_ilp` (79 commits ahead of master)
**Scope**: 168 files changed, ~70k lines added, ~6k lines removed

---

## 1. Executive Summary

This branch introduces **ILP v4** (InfluxDB Line Protocol version 4), a new **binary, column-oriented
wire protocol** for high-performance time-series data ingestion over **WebSocket**. It represents a
fundamental shift from the existing text-based, row-oriented ILP protocol.

### Key Changes

| Area | What Changed | Scale |
|------|-------------|-------|
| **Protocol** | New binary column-first wire format with gorilla encoding, schema caching, symbol dictionaries | ~30 new protocol classes |
| **Client** | New `IlpV4WebSocketSender` with double-buffering, async I/O, in-flight window | ~10 new client classes |
| **WebSocket transport** | New zero-GC native WebSocket client with platform-specific implementations | ~7 new transport classes |
| **Server** | WebSocket upgrade processor, frame parsing, connection management, ILP v4 message processing | ~12 new server classes |
| **WAL** | New `ColumnarRowAppender` interface and `WalColumnarRowAppender` for column-first writes | 3 new/modified WAL classes |
| **Bridge** | `IlpV4WalAppender` connecting server decoder to WAL writer | 1 new bridge class |
| **Tests** | Comprehensive unit, integration, and E2E tests | ~55 new test files |

---

## 2. Protocol: ILP v4 Binary Wire Format

### 2.1 Overview

The protocol is fully specified in `ILP_V4_SPECIFICATION.md` (674 lines). Key properties:

- **Column-oriented**: All values for a column are stored contiguously (not row-by-row)
- **Binary**: Compact encoding with varint, null bitmaps, gorilla compression
- **Batched**: Multiple tables and many rows per message
- **Schema-cached**: First send includes full schema; subsequent sends reference by XXH64 hash
- **WebSocket transport**: RFC 6455 over HTTP upgrade on `/write/v4`

### 2.2 Message Structure

```
Message = Header (12 bytes fixed) + Payload (variable, optionally compressed)

Header:
  [0-3]  Magic: "ILP4" (0x34504C49, LE)
  [4]    Version: 0x01
  [5]    Flags byte (bitmask):
           bit 0 (0x01): FLAG_LZ4 compression
           bit 1 (0x02): FLAG_ZSTD compression (mutually exclusive with LZ4)
           bit 2 (0x04): FLAG_GORILLA timestamp encoding
           bit 3 (0x08): FLAG_DELTA_SYMBOL_DICT (delta symbol dictionary mode)
  [6-7]  Table count (uint16 LE)
  [8-11] Payload length (uint32 LE)

Payload (when FLAG_DELTA_SYMBOL_DICT is set):
  [DeltaSymbolDictionary] + TableBlock[0] + ... + TableBlock[N-1]
Payload (otherwise):
  TableBlock[0] + ... + TableBlock[N-1]
```

**Magic byte variants:**
- `ILP4` (0x34504C49) - Normal data message
- `ILP?` (0x3F504C49) - Capability request
- `ILP!` (0x21504C49) - Capability response
- `ILP0` (0x30504C49) - Fallback (server doesn't support v4)

**Delta Symbol Dictionary section** (when flag `0x08` is set):
```
[deltaStartId: varint]   - First ID in this delta batch
[deltaCount: varint]     - Number of new symbol entries
For each new symbol:
  [string length: varint][string bytes: UTF-8]
```
This is a connection-level dictionary shared across all tables/columns. Only newly added
symbols since the last ACK'd batch are sent (delta encoding). Symbol columns then
reference global IDs (varints) instead of embedding per-column dictionaries.

### 2.3 Table Block Structure

```
TableBlock:
  Table Header:
    - name_length (varint) + name (UTF-8)
    - row_count (varint)
    - column_count (varint)
  Schema Section:
    - mode_byte: 0x00 (full schema) or 0x01 (hash reference)
    - If full: column definitions (name + type_code per column)
    - If reference: 8-byte XXH64 schema hash
  Column Data:
    - For each column: [null bitmap if nullable] + encoded values
```

### 2.4 Column Types Supported

22 types total:

| Code | Type | Size | Endian | Notes |
|------|------|------|--------|-------|
| 0x01 | BOOLEAN | bit-packed | N/A | 1 bit per value, LSB first |
| 0x02 | BYTE | 1B | N/A | int8 |
| 0x03 | SHORT | 2B | LE | int16 |
| 0x04 | INT | 4B | LE | int32 |
| 0x05 | LONG | 8B | LE | int64 |
| 0x06 | FLOAT | 4B | LE | IEEE 754 |
| 0x07 | DOUBLE | 8B | LE | IEEE 754 |
| 0x08 | STRING | var | N/A | offset array + UTF-8 |
| 0x09 | SYMBOL | var | N/A | dictionary or global ID |
| 0x0A | TIMESTAMP | 8B/compressed | LE | microseconds, gorilla-capable |
| 0x0B | DATE | 8B | LE | milliseconds |
| 0x0C | UUID | 16B | LE | lo:int64 + hi:int64 |
| 0x0D | LONG256 | 32B | LE | 4x int64, least significant first |
| 0x0E | GEOHASH | var | N/A | varint precision + packed bits |
| 0x0F | VARCHAR | var | N/A | same as STRING on wire |
| 0x10 | TIMESTAMP_NANOS | 8B/compressed | LE | nanoseconds, gorilla-capable |
| 0x11 | DOUBLE_ARRAY | var | LE | n-dimensional |
| 0x12 | LONG_ARRAY | var | LE | n-dimensional |
| 0x13 | DECIMAL64 | 8B | **BE** | scale byte + big-endian unscaled |
| 0x14 | DECIMAL128 | 16B | **BE** | scale byte + big-endian unscaled |
| 0x15 | DECIMAL256 | 32B | **BE** | scale byte + big-endian unscaled |
| 0x16 | CHAR | 2B | LE | UTF-16 code unit |

Nullable flag is encoded in the high bit (`0x80`) of the type code.
So `0x85` = nullable LONG, `0x07` = non-nullable DOUBLE.

### 2.5 Key Encoding Schemes

- **Varint**: Unsigned LEB128 for variable-length integers
- **Null bitmap**: `ceil(rows/8)` bytes, LSB first, bit=1 means NULL. Only non-null values are stored.
- **Gorilla timestamps**: Delta-of-delta encoding (1/9/12/16/36 bits per timestamp)
- **Symbol dictionary**: Per-batch dictionary with varint indices; global dictionary with delta encoding across batches
- **Strings**: Offset array (uint32 LE) + concatenated UTF-8 data

### 2.6 Response Format

```
Response:
  status_code (uint8): 0=OK, 1=PARTIAL, 2=SCHEMA_REQUIRED, 3-7=various errors
  [error payload if status != 0]
```

Server uses **cumulative ACK** strategy (ACK every N batches) with sequence numbers.

### 2.7 Protocol Constants

Defined in `IlpV4Constants.java` (506 lines). Includes magic bytes, header offsets, flag bits,
type codes, null sentinels, and protocol limits.

### 2.8 Protocol Limits

| Limit | Value |
|-------|-------|
| Max batch size | 16 MB |
| Max tables/batch | 256 |
| Max rows/table | 1,000,000 |
| Max columns/table | 2,048 |
| Max in-flight batches | 4 |

---

## 3. Client Implementation

### 3.1 Architecture

```
User Thread                              I/O Thread
    |                                        |
table().symbol().at()                        |
    |                                        |
    v                                        |
IlpV4TableBuffer (per-table column buffers)  |
    |                                        |
    v                                        |
IlpV4WebSocketEncoder                        |
    |                                        |
    v                                        |
MicrobatchBuffer (double-buffered)           |
    |--- seal & enqueue --->  WebSocketSendQueue ---> InFlightWindow
                                             |
                                    WebSocketClient (native)
                                             |
                                    ResponseReader (ACK processing)
```

### 3.2 Key Client Classes

**Package**: `io.questdb.cutlass.ilpv4.client`

| Class | Lines | Responsibility |
|-------|-------|---------------|
| `IlpV4WebSocketSender` | 1418 | Main sender implementing `Sender` interface. Fluent API + fast-path API. |
| `IlpV4WebSocketEncoder` | 790 | Encodes table buffers into ILP v4 binary format with gorilla, schema caching, delta symbols. |
| `MicrobatchBuffer` | 492 | Double-buffered batch accumulator with state machine (FILLING/SEALED/SENDING/RECYCLED). |
| `WebSocketSendQueue` | 708 | Bounded queue with dedicated I/O thread for async sending + ACK receiving. |
| `InFlightWindow` | 474 | Tracks unacknowledged batches with `ReentrantLock` + conditions for blocking. |
| `WebSocketChannel` | 668 | Low-level WebSocket I/O wrapper (connect, send binary, read responses). |
| `GlobalSymbolDictionary` | 173 | Cross-batch symbol dictionary for delta encoding. |
| `NativeBufferWriter` | 289 | Direct memory buffer writer (native memory, no GC). |
| `ResponseReader` | 252 | Parses server ACK/error responses. |
| `IlpBufferWriter` | 177 | Buffer writer abstraction. |
| `WebSocketResponse` | 283 | Response parsing for ILP v4 protocol responses. |

### 3.3 Two Operation Modes

1. **Synchronous** (`connect()`): `inFlightWindowSize=1`. Sends each batch and waits for ACK before proceeding.
2. **Asynchronous** (`connectAsync()`): `inFlightWindowSize>1` (default 8). Double-buffered with dedicated I/O thread.

### 3.4 Fluent API

```java
sender.table("metrics")
      .symbol("host", "server-1")
      .doubleColumn("cpu", 98.5)
      .longColumn("requests", 42)
      .at(timestamp, ChronoUnit.MICROS);
```

**Additional ILP v4 column types beyond `Sender` interface:**
- `intColumn(name, value)` / `shortColumn(name, value)` / `charColumn(name, value)`
- `uuidColumn(name, lo, hi)` / `long256Column(name, l0, l1, l2, l3)`
- `decimalColumn(name, Decimal64/128/256/CharSequence)`
- `doubleArray(name, double[]/double[][]/DoubleArray)` / `longArray(name, long[]/long[][]/LongArray)`
- `timestampColumn(name, value, ChronoUnit.NANOS)` (nanosecond precision)

**Builder / Config string support:**
```java
// Builder pattern
Sender.builder(Sender.Transport.WEBSOCKET)
    .address("localhost:9000")
    .asyncMode(true)
    .autoFlushRows(500)
    .inFlightWindowSize(8)
    .build();

// Config string
Sender.fromConfig("ws::addr=host:port;");   // plain
Sender.fromConfig("wss::addr=host:port;");  // TLS
```

### 3.5 Fast-Path API (for high-throughput generators)

Bypasses per-row overhead (hashmap lookups, validation):

```java
IlpV4TableBuffer tableBuffer = sender.getTableBuffer("q");
IlpV4TableBuffer.ColumnBuffer colBid = tableBuffer.getOrCreateColumn("b", TYPE_DOUBLE, false);

// Hot path (per row) - no hashmap lookups, no validation
colBid.addDouble(bid);
tableBuffer.nextRow();
sender.incrementPendingRowCount();
```

### 3.6 Auto-Flush Triggers

| Trigger | Default | Description |
|---------|---------|-------------|
| `autoFlushRows` | 500 | Flush after N rows |
| `autoFlushBytes` | 1 MB | Flush when buffer reaches N bytes |
| `autoFlushInterval` | 100 ms | Flush if oldest row is older than N |

### 3.7 Flow Control

Backpressure chain: Server slow -> InFlightWindow fills -> I/O thread blocks -> SendQueue backs up -> User thread blocks on enqueue.

Maximum rows in flight (defaults): `(8 in-flight + 16 queued + 2 buffers) * 500 = 13,000 rows`

### 3.8 Schema Caching

- First time a table schema is sent: full inline schema
- Subsequent times: 8-byte XXH64 hash reference
- `sentSchemaHashes` (LongHashSet) tracks what the server has seen
- If server returns `SCHEMA_REQUIRED`, client resends full schema

### 3.9 Symbol Delta Encoding

- `GlobalSymbolDictionary`: Maps symbol strings to integer IDs across all batches
- Only new symbols (delta) are sent with each batch
- `maxSentSymbolId` (volatile) tracks what the server knows
- Server maintains `connectionSymbolDict` to reconstruct

---

## 4. WebSocket Transport Layer

### 4.1 Architecture

**Package**: `io.questdb.cutlass.http.client`

The transport layer is a **zero-GC native WebSocket client** built from scratch (no third-party deps).

| Class | Lines | Responsibility |
|-------|-------|---------------|
| `WebSocketClient` | 776 | Core WebSocket client: handshake, frame send/recv, masking, ping/pong |
| `WebSocketClientFactory` | 137 | Factory for platform-specific socket instances |
| `WebSocketClientOsx` | 75 | macOS socket implementation |
| `WebSocketClientLinux` | 71 | Linux socket implementation |
| `WebSocketClientWindows` | 74 | Windows socket implementation |
| `WebSocketFrameHandler` | 93 | Callback interface for received frames |
| `WebSocketSendBuffer` | 582 | Native memory send buffer with frame encoding |

### 4.2 Key Design Decisions

- **No third-party dependencies** (consistent with QuestDB's philosophy)
- **Native sockets** via JNI for zero-GC operation
- **Platform-specific** I/O multiplexing: Linux=epoll, macOS=kqueue, Windows=select (Template Method pattern)
- **RFC 6455 compliant**: Client-to-server masking, proper handshake
- **Non-blocking I/O**: Blocking connect, then switch to non-blocking for data path
- **Auto-pong**: Client automatically sends pong on received ping

### 4.2a WebSocketSendBuffer: Zero-Copy Frame Building

The `WebSocketSendBuffer` (582 lines) uses a "reserve-write-patch" strategy that eliminates
intermediate copies. It implements `IlpBufferWriter`, so the ILP encoder writes directly
into the WebSocket send buffer:

```
beginBinaryFrame():
  1. Record frameStartOffset
  2. Reserve MAX_HEADER_SIZE (14 bytes) for variable-length WS header
  3. Record payloadStartOffset

[ILP encoder writes binary payload directly via putByte/putInt/putLong/putVarint etc.]

endBinaryFrame():
  1. Calculate actual payload length
  2. Calculate actual header size (2-14 bytes depending on payload)
  3. Write real header right-aligned in reserved space
  4. Apply RFC 6455 masking (8-byte XOR chunks for speed)
  5. Return FrameInfo(offset, length) pointing to complete frame
```

This means: ILP v4 message -> WebSocket frame -> TCP send with **zero intermediate copies**.

### 4.3 Server-Side WebSocket

**Package**: `io.questdb.cutlass.ilpv4.websocket`

| Class | Lines | Responsibility |
|-------|-------|---------------|
| `WebSocketFrameParser` | 342 | Zero-allocation RFC 6455 frame parser |
| `WebSocketFrameWriter` | 281 | Frame header/payload writing |
| `WebSocketHandshake` | 421 | RFC 6455 handshake validation |
| `WebSocketOpcode` | 136 | WebSocket opcodes (TEXT, BINARY, CLOSE, PING, PONG) |
| `WebSocketCloseCode` | 178 | RFC 6455 close codes |
| `WebSocketProcessor` | 87 | Event callback interface |

---

## 5. Server Implementation

### 5.1 Architecture

```
HTTP Request (GET /write/v4)
    |
    v
IlpV4WebSocketUpgradeProcessor (HttpRequestProcessor)
    |-- validates WebSocket handshake
    |-- sends 101 Switching Protocols
    |-- calls context.switchProtocol()
    |
    v
resumeRecv() loop
    |-- reads from socket
    |-- WebSocketFrameParser: parses frames, unmasks
    |
    v
handleBinaryMessage()
    |-- IlpV4ProcessorState.addData() + processMessage()
    |-- IlpV4StreamingDecoder: decodes ILP v4 binary format
    |-- IlpV4WalAppender: appends decoded data to WAL
    |-- commit()
    |
    v
Cumulative ACK response (every N batches or on error)
```

### 5.2 Key Server Classes

**Package**: `io.questdb.cutlass.ilpv4.server`

| Class | Lines | Responsibility |
|-------|-------|---------------|
| `IlpV4WebSocketUpgradeProcessor` | 718 | HTTP processor handling upgrade + WebSocket frame loop |
| `IlpV4ProcessorState` | 382 | Per-connection state: buffer, decoder, appender, symbol dict, ACK tracking |
| `IlpV4StreamingDecoder` | 162 | Streaming decoder for ILP v4 messages |
| `IlpV4TudCache` | 373 | Table Update Details cache (maps table name -> writer) |
| `IlpV4WebSocketProcessor` | 126 | Adapter for binary message processing |
| `IlpV4WebSocketProcessorState` | 258 | WebSocket-level connection state |
| `IlpV4WebSocketHttpProcessor` | 156 | Handshake validation helper |
| `WebSocketBuffer` | 205 | Server-side WebSocket buffer |
| `WebSocketConnectionContext` | 563 | Connection lifecycle management |

### 5.3 Server-Side Symbol Handling

- `connectionSymbolDict` (ObjList<String>): Per-connection accumulated symbol dictionary
- `ConnectionSymbolCache`: Maps `clientSymbolId -> tableSymbolId` to avoid string lookups
- Delta decoding: Client sends only new symbols, server appends to its dictionary

### 5.4 ACK Strategy

- **Cumulative ACK**: Server ACKs the highest processed sequence number (not every batch)
- **ACK batch size**: 8 (sends ACK every 8 successfully processed messages)
- **Error handling**: On error, first ACK all successful messages, then send error for the failing one
- **Response format**: WebSocket binary frame with status byte (1) + sequence (8 LE) = 9 bytes

### 5.5 Server Send State Machine

The server has a send state machine for handling ACK backpressure:

```
SendState.READY  --[ACK blocked by full OS buffer]--> SendState.SENDING
SendState.SENDING --[resumeSend() completes flush]--> SendState.READY
```

When in SENDING state: pings are skipped, error responses are logged but not sent,
close responses may be skipped. The `resumeSend()` method handles transitioning back
to READY after a blocked send completes.

### 5.6 Integration with HTTP Server

- Registered as an `HttpRequestProcessor` for the `/write/v4` and `/api/v4/write` paths
- After handshake, calls `context.switchProtocol()` to bypass HTTP parsing
- `handleClientRecv()` detects `protocolSwitched` and calls `handleProtocolSwitchedRecv()`
- Subsequent data handled via `resumeRecv()` callback
- Uses `LocalValue<IlpV4ProcessorState>` for thread-safe per-connection state
- Single-threaded per connection (all processing on one HTTP worker thread)

### 5.7 Known Server Architecture Issues

- **Dual state classes**: `IlpV4WebSocketProcessorState` (simpler, buffer-only) and
  `IlpV4ProcessorState` (full state with WAL) coexist. The simpler one appears to be
  an earlier/alternative design that may be unused.
- **`IlpV4WebSocketProcessor`** (callback-based adapter): Appears to be an alternative/unused
  design. Actual frame dispatch is done directly in `IlpV4WebSocketUpgradeProcessor.handleWebSocketFrame()`.
- **Status code mismatch**: `IlpV4WebSocketUpgradeProcessor` uses its own status codes
  (0, 1, 3, 4, 255) while `IlpV4StatusCode` defines different codes (0-7). These don't fully align.
- **ACK_BATCH_SIZE hardcoded**: The batch size of 8 is a constant, not configurable.

---

## 6. Protocol Decoder Layer

### 6.1 Architecture

**Package**: `io.questdb.cutlass.ilpv4.protocol`

The decoder layer provides **flyweight cursor-based** access to ILP v4 binary data,
enabling zero-allocation iteration over columns and rows.

### 6.2 Key Decoder Classes

| Class | Lines | Responsibility |
|-------|-------|---------------|
| `IlpV4MessageHeader` | 356 | Parses 12-byte message header |
| `IlpV4MessageCursor` | 303 | Iterates over table blocks in a message |
| `IlpV4TableBlockCursor` | 562 | Iterates over columns/rows in a table block |
| `IlpV4TableHeader` | 273 | Parses table header (name, row count, column count) |
| `IlpV4Schema` | 511 | Schema parsing and validation |
| `IlpV4SchemaCache` | 104 | XXH64-based schema lookup cache |
| `IlpV4SchemaHash` | 574 | Schema hashing for reference mode |
| `IlpV4ColumnDef` | 167 | Column definition (name + type code) |

### 6.3 Column Cursors (Flyweight Pattern)

Each column type has a dedicated cursor for zero-allocation iteration:

| Cursor | Decodes |
|--------|---------|
| `IlpV4FixedWidthColumnCursor` | BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256 |
| `IlpV4TimestampColumnCursor` | TIMESTAMP, TIMESTAMP_NANOS (handles gorilla decoding) |
| `IlpV4BooleanColumnCursor` | BOOLEAN (bit-packed) |
| `IlpV4StringColumnCursor` | STRING, VARCHAR (offset array + data) |
| `IlpV4SymbolColumnCursor` | SYMBOL (dictionary + varint indices) |
| `IlpV4GeoHashColumnCursor` | GEOHASH (variable precision) |
| `IlpV4ArrayColumnCursor` | DOUBLE_ARRAY, LONG_ARRAY (n-dimensional) |
| `IlpV4DecimalColumnCursor` | DECIMAL64/128/256 (big-endian) |

### 6.4 Supporting Decoders

| Class | Responsibility |
|-------|---------------|
| `IlpV4GorillaDecoder` / `IlpV4GorillaEncoder` | Delta-of-delta timestamp compression |
| `IlpV4BitReader` / `IlpV4BitWriter` | Bit-level I/O for gorilla and booleans |
| `IlpV4NullBitmap` | Null bitmap read/write utilities |
| `IlpV4Varint` | LEB128 varint encoding/decoding |
| `IlpV4ZigZag` | ZigZag encoding for signed integers |
| `IlpV4ResponseEncoder` | Encodes server response messages |
| `IlpV4StatusCode` | Status code enum |
| `IlpV4ParseException` | Protocol parse error |

---

## 7. WAL Changes: Column-First Writes

### 7.1 Overview

The most impactful architectural change is adding a **columnar write path** to the WAL layer.
Previously, WAL only supported row-by-row writes via `TableWriter.Row`. Now it also supports
writing entire columns at once.

### 7.2 New Interface: `ColumnarRowAppender`

**File**: `core/src/main/java/io/questdb/cairo/wal/ColumnarRowAppender.java` (340 lines)

```java
public interface ColumnarRowAppender {
    void beginColumnarWrite(int rowCount);
    void putFixedColumn(int columnIndex, long valuesAddress, int valueCount,
                        int valueSize, long nullBitmapAddress, int rowCount);
    void putTimestampColumn(...);
    void putStringColumn(...);
    void putVarcharColumn(...);
    void putSymbolColumn(...);
    void putBooleanColumn(...);
    void putGeoHashColumn(...);
    void putArrayColumn(...);
    void putDecimalColumn(...);
    void putCharColumn(...);
    void putFixedColumnNarrowing(...);
    void endColumnarWrite(long minTimestamp, long maxTimestamp, boolean outOfOrder);
    void cancelColumnarWrite();
}
```

### 7.3 Implementation: `WalColumnarRowAppender`

**File**: `core/src/main/java/io/questdb/cairo/wal/WalColumnarRowAppender.java` (789 lines)

Key features:
- **Fast path for no-null fixed-width columns**: Direct `memcpy` from wire buffer to WAL memory
- **Null expansion**: Converts sparse non-null values + null bitmap to dense storage with null sentinels
- **Designated timestamp handling**: Writes 128-bit `(timestamp, rowId)` pairs
- **Symbol resolution**: Integrates with `WalWriter.resolveSymbol()` for symbol ID mapping
- **Type narrowing**: Handles ILP types wider than storage types (e.g., LONG -> INT)
- **Thread safety**: NOT thread-safe; each WalWriter has its own instance

### 7.4 WalWriter Changes

**File**: `core/src/main/java/io/questdb/cairo/wal/WalWriter.java` (+376 lines modified)

New methods added:
- `getColumnarRowAppender()` - Returns the columnar appender (lazy init)
- `getDataColumn(int)` / `getAuxColumn(int)` - Exposes column memory for direct writes
- `setRowValueNotNullColumnar(int, long)` - Marks columns as written
- `resolveSymbol(int, CharSequence, SymbolMapReader)` - Symbol resolution for columnar path
- `finishColumnarWrite(int, long, long, boolean)` - Finalizes: fills nulls for unwritten columns, updates timestamps
- `cancelColumnarWrite(long)` - Rollback via `setAppendPosition()`
- `getCachedSymbolKey(int, CharSequence)` - Symbol ID lookup from internal cache

Also modified:
- `getSegmentRowCount()` is no longer `@TestOnly`
- `configureSymbolTable()` was refactored to support the columnar path
- New `refreshSymbolWatermarks()` method: re-reads `_txn` and `_cv` files on each
  segment rollover to update symbol counts. Previously `configureSymbolTable()` was
  only called at construction.

### 7.4a "Seq Advise" Explained

The latest commit ("wal writer to use seq advise") changes `POSIX_MADV_RANDOM` to
`POSIX_MADV_SEQUENTIAL` for WAL column memory mappings. This is an `madvise()` hint
to the OS kernel, telling it that WAL column data files will be accessed sequentially
(append-only writes), enabling the kernel to optimize readahead and page cache behavior.
Applied to both data and aux column files when opening new segments.

### 7.5 Bridge: `IlpV4WalAppender`

**File**: `core/src/main/java/io/questdb/cutlass/line/tcp/IlpV4WalAppender.java` (640 lines)

Connects the protocol decoder to WAL:
1. **Phase 1**: Resolves column indices (ILP column index -> QuestDB column index), auto-creates missing columns
2. **Phase 2**: Two-pass timestamp processing:
   - First pass: Scan timestamps for min/max/outOfOrder
   - Second pass: Write columns via `ColumnarRowAppender`
3. **Type conversion**: Maps ILP v4 types to QuestDB column types, handles nano/micro timestamp conversion
4. **Symbol caching**: Uses `ConnectionSymbolCache` for `clientSymbolId -> tableSymbolId` optimization

### 7.6 Symbol Caching Infrastructure

**New classes**:
- `ClientSymbolCache` (`cutlass/line/tcp/`): Per-table client-side symbol ID tracking
- `ConnectionSymbolCache` (`cutlass/line/tcp/`): Per-connection symbol cache mapping client IDs to table IDs

---

## 8. Test Coverage

### 8.1 Test File Count by Area

| Area | Test Files | Notable Tests |
|------|-----------|---------------|
| Protocol decoders | 17 | Varint, ZigZag, Gorilla, NullBitmap, each column type decoder |
| WebSocket (server) | 15 | Frame parser, handshake, upgrade, connection, integration, TLS |
| WebSocket (client) | 8 | Channel, send queue, in-flight window, microbatch buffer |
| Sender E2E | 6 | Sender unit, encoder, ACK integration, sender-receiver, E2E |
| WAL | 2 | WalColumnarRowAppenderTest (3859 lines!), WalWriterTest |
| WAL Appender | 1 | IlpV4WalAppenderTest |
| Symbol | 4 | GlobalSymbolDictionary, DeltaSymbolDictionary, ClientSymbolCache, ConnectionSymbolCache |
| Builder | 1 | LineSenderBuilderWebSocketTest |
| Benchmarks | 2 | IlpV4AllocationTestClient, StacBenchmarkClient |

### 8.2 Notable Test Highlights

- `WalColumnarRowAppenderTest` (3859 lines) - Extremely thorough columnar write testing
- `IlpV4WebSocketSenderReceiverTest` (4102 lines) - Full client-server protocol testing
- `WebSocketIntegrationTest` (1362 lines) - WebSocket protocol integration
- `IlpV4WebSocketEncoderTest` (1262 lines) - Encoder unit tests
- `WebSocketChannelIntegrationTest` (1008 lines) - Channel integration

---

## 9. What's Done (Completeness Assessment)

### Fully Implemented

- ILP v4 binary wire protocol specification and all type encoders/decoders
- Gorilla delta-of-delta timestamp compression
- Schema caching with XXH64 hash references
- Symbol delta encoding (global dictionary)
- WebSocket frame parser/writer (server side, RFC 6455 compliant)
- WebSocket handshake validation and HTTP 101 upgrade
- Server-side frame processing and ILP v4 message decoding
- `IlpV4WebSocketSender` with fluent API and fast-path API
- Double-buffering with async I/O thread
- In-flight window flow control
- Cumulative ACK strategy
- `ColumnarRowAppender` interface and `WalColumnarRowAppender`
- `IlpV4WalAppender` bridge (streaming cursor to WAL)
- Connection-level symbol caching
- Zero-GC native WebSocket client with platform-specific implementations
- WAL writer extensions for columnar writes
- TLS support
- Comprehensive test suite

### Partially Implemented / TODOs

- **Authentication**: `// TODO: Store auth credentials for connection` - Auth token/basic auth
  storage not wired. `create()` accepts auth params but doesn't use them. Builder throws
  "not yet supported" for `token=`, `username=`, `password=`.
- **Byte-limit auto-flush**: Comment in `shouldAutoFlush()`: "Byte limit is harder to estimate
  without encoding, skip for now" -- only row count and time-based auto-flush are active.
- **LZ4/Zstd compression**: Protocol flags defined but actual compression/decompression logic
  for payloads was not found in the encoder/decoder code.
- **Capability negotiation**: `ILP?`/`ILP!` magic bytes defined but the actual negotiation
  protocol details beyond magic bytes are sparse.
- **Two WebSocket client implementations coexist**: `WebSocketChannel` (custom, in ilpv4/client/)
  and `WebSocketClient` (in cutlass/http/client/). Suggests in-progress migration.
- **Server status code alignment**: `IlpV4WebSocketUpgradeProcessor` uses codes (0,1,3,4,255)
  while `IlpV4StatusCode` defines codes (0-7). These need reconciliation.
- **Seq Advise**: Latest commit changes `madvise()` hint - now complete (see section 7.4a).

### Known Hot-Path Concerns

- `putStringColumn()` in `WalColumnarRowAppender` does `Utf8s.toString(utf8Value)` which
  allocates a Java String on the hot path for STRING (not VARCHAR) columns
- `putBooleanColumn()`, `putCharColumn()`, `putVarcharColumn()`, `putStringColumn()`,
  `putSymbolColumn()` wrap `IlpV4ParseException` in `RuntimeException` instead of propagating
- `shouldAutoFlush()` calls `System.nanoTime()` on every row (potential concern at very high throughput)
- No gorilla decompression fast path for timestamps when `!tsCursor.supportsDirectAccess()`
- WalWriter symbol paths use `Path.PATH2.get()` (thread-local) with `// todo: use own path` comment

### Not Implemented / Out of Scope

- Automatic reconnection with exponential backoff (mentioned in design doc Phase 7)
- Connection keepalive (ping/pong initiated by client)
- Metrics/logging instrumentation for production monitoring
- WebSocket fragmentation (by design - expects single unfragmented frames)
- `DELETE` support (QuestDB doesn't support DELETE)

---

## 10. Key Architectural Decisions

### 10.1 Column-First over Row-First

The most significant decision. Instead of sending data row-by-row (old ILP),
data is sent column-by-column. Benefits:
- Direct memcpy for fixed-width non-null columns (zero conversion cost)
- Better compression (gorilla works across column values)
- Better cache locality during ingestion
- Reduced per-row overhead

### 10.2 WebSocket over HTTP

Persistent connection avoids per-request overhead. Enables streaming with pipelined
ACKs (in-flight window), achieving much higher throughput than request-response HTTP.

### 10.3 No Third-Party Dependencies

Custom WebSocket implementation (both client and server) maintains QuestDB's
zero-dependency philosophy while enabling zero-GC operation on hot paths.

### 10.4 Double-Buffering

Two MicrobatchBuffers alternate between user thread (FILLING) and I/O thread (SENDING),
ensuring the user is never blocked waiting for I/O except under backpressure.

### 10.5 Schema Hash Caching

XXH64 hash references reduce per-batch overhead for repeated schemas from O(columns * name_lengths)
to O(1) constant 8 bytes.

---

## 11. Data Flow: End-to-End

```
1. USER CODE
   sender.table("t").symbol("s","v").doubleColumn("d",1.5).at(ts)

2. CLIENT: IlpV4TableBuffer
   Accumulates column data in per-column buffers

3. CLIENT: IlpV4WebSocketEncoder
   Encodes table buffers to ILP v4 binary:
   - Header (12 bytes)
   - Schema (full or hash reference)
   - Column data (gorilla timestamps, null bitmaps, symbol dictionaries)

4. CLIENT: MicrobatchBuffer
   Accumulates encoded bytes until flush trigger

5. CLIENT: WebSocketSendQueue (I/O Thread)
   Wraps in WebSocket binary frame, sends via native socket

6. NETWORK: WebSocket binary frame

7. SERVER: IlpV4WebSocketUpgradeProcessor.resumeRecv()
   Reads from socket, parses WebSocket frames, unmasks

8. SERVER: IlpV4ProcessorState.processMessage()
   IlpV4StreamingDecoder decodes binary to cursor API

9. SERVER: IlpV4WalAppender.appendToWalStreaming()
   Maps columns, creates missing columns, resolves symbols

10. WAL: WalColumnarRowAppender
    Writes columns directly to WAL memory (memcpy fast path)

11. WAL: WalWriter.finishColumnarWrite()
    Fills nulls, updates timestamps, increments row count

12. SERVER: IlpV4WebSocketUpgradeProcessor
    Sends cumulative ACK -> WebSocket binary frame

13. CLIENT: ResponseReader / InFlightWindow
    Processes ACK, slides window, recycles batch buffer
```

---

## 12. ILP v3 vs v4 Comparison

| Aspect | ILP v1-v3 (Line Protocol) | ILP v4 (Binary) |
|--------|--------------------------|-----------------|
| Format | Text (InfluxDB Line Protocol) | Binary |
| Layout | Row-oriented | Column-first |
| Transport | TCP or HTTP | WebSocket |
| Schema | Implicit (inferred from data) | Explicit (full or hash reference) |
| Compression | None | Gorilla timestamps, LZ4/Zstd (flags) |
| Symbols | Inline text every row | Dictionary-encoded (local or global delta) |
| Batching | One row per line | Multi-table batches with columnar data |
| Nullability | Limited | First-class (null bitmap + type flag) |
| Types | ~5 (string, int, float, bool, timestamp) | 22 types including arrays, decimals, UUID, geohash |
| Flow control | None (fire and forget on TCP) | ACK-based with in-flight window |
| Connection | New per request (HTTP) or persistent (TCP) | Persistent WebSocket with pipelining |

---

## 13. Files Changed (Non-ILP v4, from master merges)

The branch also includes changes unrelated to ILP v4 that were merged from master
or developed in parallel:

- `SqlOptimiser.java` (+738 lines changed) - SQL optimizer changes
- `IntervalUtils.java` (-1122 lines) - Interval handling simplification
- `CompiledTickExpression.java` (-427 lines removed)
- `DateVariableExpr.java` (-270 lines removed)
- `FilterPushdownIntoUnionTest.java` (-598 lines removed)
- Various view/window/union test changes

These appear to be from master merges (commits `d07c5dd750`, `6280e34003`, `7230eb02b6`).

---

## 14. Suggested Follow-Up Work

### High Priority
1. **Wire up authentication** - The TODO in `IlpV4WebSocketSender.create()` for storing auth credentials
2. **Integration testing with real QuestDB server** - Verify E2E with actual storage
3. **Compression testing** - Exercise LZ4/Zstd flags in integration tests
4. **Seq advise completion** - The latest commit suggests this is still in progress

### Medium Priority
5. **Reconnection logic** - Automatic recovery from transient failures (Phase 7 from design doc)
6. **Connection keepalive** - Periodic ping/pong to detect dead connections
7. **Error recovery** - Client-side retry with exponential backoff for retryable errors
8. **Production metrics** - Throughput, latency, error rates, buffer utilization

### Lower Priority
9. **Decimal type support** - Full encoder/decoder testing for DECIMAL64/128/256
10. **WebSocket fragmentation** - May be needed for very large batches exceeding frame limits
11. **Multi-table batch optimization** - Ensure batches with many small tables are efficient
12. **Documentation** - Public API documentation and migration guide from ILP v3

---

## 15. How to Pick Up Work From This Branch

### If you want to work on the **client**:
- Start with `IlpV4WebSocketSender.java` - it's the entry point
- Tests in `test/cutlass/line/websocket/` cover all sender functionality
- `IlpV4WebSocketEncoderTest.java` for encoding specifics

### If you want to work on the **server**:
- Start with `IlpV4WebSocketUpgradeProcessor.java` - HTTP -> WebSocket upgrade + frame processing
- `IlpV4ProcessorState.java` orchestrates message decoding and WAL writing
- Tests in `test/cutlass/http/websocket/` for server-side

### If you want to work on the **WAL/storage integration**:
- Start with `WalColumnarRowAppender.java` - the core columnar write implementation
- `IlpV4WalAppender.java` bridges protocol to WAL
- `WalColumnarRowAppenderTest.java` (3859 lines) is the most comprehensive test file

### If you want to work on the **protocol**:
- Start with `ILP_V4_SPECIFICATION.md` for the formal spec
- `IlpV4Constants.java` for all constants
- Tests in `test/cutlass/http/ilpv4/` for each type's encoder/decoder

### If you want to work on the **WebSocket transport**:
- Client: `cutlass/http/client/WebSocketClient.java`
- Server: `cutlass/ilpv4/websocket/WebSocketFrameParser.java` + `WebSocketFrameWriter.java`
- Tests in `test/cutlass/http/websocket/` for protocol-level WebSocket tests
