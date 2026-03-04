# QWP over UDP: Iterative Implementation Plan

Related documents:
- [QWP Protocol Specification](QWP_PROTOCOL.md) -- wire format reference
- [QWP over UDP Design](QWP_UDP_DESIGN.md) -- design rationale, constraints,
  reuse analysis, server architecture, and open questions

Each iteration produces working, integration-tested code. No mocking --
tests use real UDP sockets, real CairoEngine, real WAL writes, real SQL queries.

### Progress Legend

- `[ ]` Not started
- `[-]` In progress
- `[x]` Done

## Key Reuse Insight

`QwpWebSocketEncoder` with `setGorillaEnabled(false)` and
`encode(tableBuffer, false)` already produces a valid UDP datagram: flags=0x00,
tableCount=1, full schema (mode 0x00), per-column symbol dictionaries. No
encoder fork is needed for the first iterations.


## Iteration 1: Vertical slice -- sender, receiver, and integration test `[ ]`

**Goal:** A working sender-to-table pipeline over real UDP loopback. The
thinnest possible slice: explicit `flush()` only, no auto-flush, no size
estimation, no builder integration, no server config wiring.

### Client: `QwpUdpSender` `[ ]`

Minimal `Sender` implementation. Internally holds:
- `QwpWebSocketEncoder` (Gorilla disabled) -- reused as-is.
- `QwpTableBuffer` for the current table.
- `UdpLineChannel` (reused as-is) for socket I/O.

Behavior:
- `table(name)`: if a different table is already buffered, flush it first.
- `symbol()`, `doubleColumn()`, `longColumn()`, etc.: delegate to
  `QwpTableBuffer`.
- `at()`/`atNow()`: commit the row via `tableBuffer.nextRow()`.
- `flush()`: encode via `QwpWebSocketEncoder.encode()`, send via
  `UdpLineChannel.send()`. Reset the table buffer. If `sendToRaw()` fails,
  log at WARN and swallow (fire-and-forget).
- `close()`: flush remaining buffer, close socket.
- `cancelRow()`, `bufferView()`: throw `LineSenderException` (not supported).

No size estimation, no MTU checking, no auto-flush. Caller is responsible for
keeping datagrams small by calling `flush()` before exceeding MTU.

```
java-questdb-client/core/src/main/java/io/questdb/client/cutlass/qwp/client/
  QwpUdpSender.java    NEW
```

### Server: `QwpUdpReceiver` `[ ]`

Thin class modeled on `LineUdpReceiver`. Does NOT extend
`AbstractLineProtoUdpReceiver` (that class hardwires `LineUdpLexer` +
`LineUdpParserImpl`). Instead, directly implements `SynchronizedJob + Closeable`
with the same socket/threading patterns:

- Socket: `nf.socketUdp()`, `nf.bindUdp()`, optional `nf.join()` for
  multicast, optional `nf.setRcvBuf()`.
- Threading: dedicated thread (with `AtomicBoolean running` /
  `SOCountDownLatch` pattern) or shared worker pool.
- Receive loop: `nf.recvRaw(fd, buf, bufLen)`.

**Datagram validation** (before decoding):
1. `receivedBytes < HEADER_SIZE (12)` -> drop.
2. Parse header via `QwpMessageHeader.parse(buf, receivedBytes)`.
3. `HEADER_SIZE + header.getPayloadLength() > receivedBytes` -> drop. This is
   the critical safety check: clamp to actual received bytes before the cursor
   computes `payloadEnd`.
4. Catch `QwpParseException` from `QwpMessageHeader.parse()` (invalid magic,
   unsupported version) -> drop.

**Decoding**: `QwpMessageCursor.of(buf, validatedLength, null, null)`.
No schema cache, no connection symbol dict (both null -- not used for UDP).

**WAL append**: `QwpWalAppender.appendToWalStreaming()` for each table block.

**Commit**: after every `commitRate` datagrams and on idle.

The receiver constructor takes a simple configuration interface
(`QwpUdpReceiverConfiguration`) with the same shape as
`LineUdpReceiverConfiguration` but without text-ILP-specific fields
(`defaultColumnTypeForFloat`, `timestampUnit`, etc.).

```
core/src/main/java/io/questdb/cutlass/qwp/udp/
  QwpUdpReceiver.java                NEW
  QwpUdpReceiverConfiguration.java   NEW (interface)
```

### Tests `[ ]`

All tests in `core/src/test/` (server module), using `AbstractCairoTest` for
engine access or `AbstractBootstrapTest` for full-server tests.

- `[ ]` **Integration test -- sender to table:** Construct a `QwpUdpReceiver`
  with a test `CairoEngine` on a random loopback port. Construct a
  `QwpUdpSender` targeting that port. Send:
  `table("cpu").symbol("host", "srv-1").doubleColumn("usage", 73.2).at(ts, MICROS)`.
  Call `flush()`. Wait briefly for the receiver to process + commit. Query:
  `SELECT host, usage, timestamp FROM cpu`. Assert values match.
- `[ ]` **Multi-row integration:** Send 10 rows for the same table in one
  `flush()`. Query `SELECT count() FROM t`. Assert 10. Query
  `SELECT sum(value) FROM t`. Assert correct sum.
- `[ ]` **Multi-table integration:** Send rows for table "a", then
  `table("b")`, then `flush()`. Assert both tables exist with correct data.
- `[ ]` **SYMBOL column round-trip:** Send rows with 5 distinct symbol values
  (same column). Flush. Query. Verify all 5 distinct values present.
- `[ ]` **Nullable column round-trip:** Use the fast-path API
  (`getTableBuffer()` / `getOrCreateColumn()`) to send a nullable DOUBLE
  column with some rows null and some non-null. Query. Verify NULLs and values.
- `[ ]` **Close flushes:** Send rows without calling `flush()`. Call `close()`.
  Verify data arrived.
- `[ ]` **Multiple datagrams:** Send + flush 3 times (3 separate datagrams).
  Verify all data accumulated.


## Iteration 2: Malformed datagram resilience `[ ]`

**Goal:** Prove the receiver does not crash or corrupt state when receiving
garbage, and always recovers to process subsequent valid datagrams.

### What to build `[ ]`

Harden the receiver's validation path. Add counters for dropped datagrams
(by reason: too short, bad magic, bad version, truncated payload, parse error).

### Tests `[ ]`

Each test sends a malformed datagram, then a valid datagram, and asserts the
valid one was processed correctly. The "recovery" assertion is the crucial part.

- `[ ]` Datagram shorter than 12 bytes (e.g., 4 bytes of zeros).
- `[ ]` Invalid magic bytes (`"ILP3"`, all zeros).
- `[ ]` Wrong version byte (0x02).
- `[ ]` `payloadLength` exceeds actual received bytes (header claims 1000,
  datagram is 50 bytes). This is the `payloadEnd` vulnerability test.
- `[ ]` Valid header, `payloadLength = 0`, `tableCount = 1`.
- `[ ]` Valid header but payload truncated (table name varint runs past end).
- `[ ]` Valid header + table header, column data truncated mid-value.
- `[ ]` 256 random bytes.
- `[ ]` Duplicate datagram (same bytes sent twice) -> both ingested.
- `[ ]` Out-of-order timestamps across datagrams -> both ingested.

All tests use real UDP sockets and a real `CairoEngine`. No mocking.


## Iteration 3: Size estimation and auto-flush `[ ]`

**Goal:** The sender automatically splits rows across multiple datagrams when
approaching the MTU limit.

### What to build `[ ]`

**`QwpDatagramSizeEstimator`** (client module) -- computes a conservative byte
estimate from a `QwpTableBuffer`'s current state:

- Header: 12 bytes.
- Schema: table name (varint + UTF-8) + row count varint + column count varint
  \+ mode byte + per-column (name varint + name + type byte).
- Per-column data: type-specific (see Iteration 3 in the previous plan for the
  full breakdown per type).
- Invariant: `estimate >= actual` always holds.

**Auto-flush in `QwpUdpSender`**:

Inside `at()`/`atNow()`, after committing the row:
1. Estimate exceeds `maxDatagramSize` and `rowCount > 1`: cancel last row,
   flush previous rows, re-add the cancelled row to a fresh buffer.
2. Estimate exceeds `maxDatagramSize` and `rowCount == 1`: throw
   `LineSenderException`.
3. Otherwise: continue buffering.

### Tests

**Estimate accuracy (encode + compare):**

- `[ ]` For each of the 22 column types: single-row table, verify
  `estimate >= actual` and `estimate - actual < 32`.
- `[ ]` Multi-row tables (1, 5, 10, 50 rows) with DOUBLE + TIMESTAMP.
- `[ ]` SYMBOL with 1, 10, 100 distinct values.
- `[ ]` STRING with empty, short, and long values.
- `[ ]` Property test: 100 random schemas + random data, verify
  `estimate >= actual`.

**Auto-flush integration (sender + receiver):**

- `[ ]` Set `maxDatagramSize = 200`. Send 50 rows with DOUBLE + TIMESTAMP.
  Verify all 50 rows arrive in the table (receiver got multiple datagrams).
- `[ ]` Same with SYMBOL column (dictionary growth triggers splits).
- `[ ]` Same with STRING column (variable-length values).

**Boundary cases:**

- `[ ]` Single row exceeds MTU: set `maxDatagramSize = 100`, send a row with
  a 200-byte string. Verify `LineSenderException` is thrown.
- `[ ]` Table switch triggers flush: `table("a")` -> add rows -> `table("b")`
  (without explicit flush). Verify table "a" received its rows.


## Iteration 4: Builder integration and server registration `[ ]`

**Goal:** Users can create UDP senders via `Sender.fromConfig("udp::...")` and
the server starts the QWP UDP receiver from configuration properties.

### Client: `LineSenderBuilder` changes `[ ]`

In `Sender.java`:
- Add `UDP` to `enum Transport`.
- Recognize `"udp"` scheme in `fromConfig()`.
- In `build()`: construct `QwpUdpSender` for `PROTOCOL_UDP`.
- New builder methods: `maxDatagramSize(int)`, `multicastTtl(int)`.
- Default port: 9007.
- `enableTls()` with UDP -> throw.

### Server: configuration and registration `[ ]`

- `QwpUdpReceiverConfiguration` defaults via `PropServerConfiguration`.
- `PropertyKey` entries for `qwp.udp.*` properties.
- `Services.createQwpUdpReceiver()` factory (Linux -> recvmmsg variant).
- `ServerMain` registers with `freeOnExit()`, independently of TCP ILP.

### Tests

**Config parsing (client):**

- `[ ]` `"udp::addr=localhost:9007;"` -> builds `QwpUdpSender`, default MTU
  1400.
- `[ ]` `"udp::addr=10.0.0.1:5555;max_datagram_size=500;"` -> custom MTU.
- `[ ]` `"udp::"` (no addr) -> throws.
- `[ ]` `"udps::"` -> throws (no TLS for UDP).

**`TestServerMain` E2E:**

- `[ ]` Start `TestServerMain` with `qwp.udp.enabled=true`. Use
  `Sender.fromConfig("udp::addr=localhost:PORT;")` to send 100 rows. Call
  `serverMain.awaitTable(...)`. Assert row count and values via
  `serverMain.assertSql(...)`. Same pattern as
  `QwpWebSocketSenderReceiverTest`.
- `[ ]` Start with `qwp.udp.enabled=false`. Verify no socket opened.
- `[ ]` Graceful shutdown: send data, shut down server, no crash or resource
  leak.


## Iteration 5: Encoder extraction and remaining column types E2E `[ ]`

**Goal:** Clean up the encoder layering (prevent drift) and verify all column
types end-to-end.

### What to build `[ ]`

Extract transport-agnostic column encoding from `QwpWebSocketEncoder` into
`QwpColumnWriter`. Both `QwpWebSocketEncoder` and `QwpUdpSender` delegate to
it. This is a pure refactoring -- behavior is unchanged.

### Tests

**Regression:**

- `[ ]` All existing `QwpWebSocketEncoderTest` tests pass byte-for-byte.
- `[ ]` All existing `QwpWebSocketSenderReceiverTest` E2E tests pass.
- `[ ]` All iteration 1-4 UDP tests pass.

**All-types E2E:**

- `[ ]` Send one row per column type through the full UDP pipeline
  (sender -> loopback -> receiver -> WAL -> SQL query). Verify value and type
  for: BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, FLOAT, DOUBLE, DATE, STRING,
  VARCHAR, SYMBOL, UUID, LONG256, TIMESTAMP, TIMESTAMP_NANOS, GEOHASH,
  DOUBLE_ARRAY (1D/2D/3D), DECIMAL64, DECIMAL128, DECIMAL256.
- `[ ]` A single row containing all 21 server-supported types simultaneously.

**Stress:**

- `[ ]` Send 100,000 rows in a tight loop over loopback. Verify no crashes, no
  resource leaks (`assertMemoryLeak`). On loopback, assert close to 100%
  delivery.

**SYMBOL dictionary overflow:**

- `[ ]` SYMBOL column with distinct values accumulating until the dictionary +
  data would exceed MTU. Verify auto-flush produces self-contained datagrams,
  each with its own dictionary. Receiver ingests all rows correctly.


## Dependency Graph

```
Iteration 1 (vertical slice: sender + receiver + integration test)
    │
    ├──────────────────────┐
    ▼                      ▼
Iteration 2              Iteration 3
(malformed datagram      (size estimation +
 resilience)              auto-flush)
    │                      │
    └──────────┬───────────┘
               ▼
         Iteration 4
         (builder + server config + TestServerMain E2E)
               │
               ▼
         Iteration 5
         (encoder extraction + all-types E2E + stress)
```

Iterations 2 and 3 are independent and can proceed in parallel after 1.