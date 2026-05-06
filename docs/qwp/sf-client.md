# QWP Store-and-Forward Client Specification

This document specifies the client-side Store-and-Forward (SF) substrate that
sits beneath QWP ingest. It is a **companion** to [`wire-ingress.md`](wire-ingress.md): that
spec defines the bytes on the wire; this spec defines what every SF-capable
client must do around them — disk format, frame sequence numbering,
handshake, ACK-driven trim, durable-ack, keepalive, reconnect, replay, and
error policy.

The goal is interop. Clients written in any language must:

- accept the same connect-string options,
- write the same on-disk format (so a slot written by one client can be
  drained by another),
- observe the same wire-level invariants (so the server sees consistent
  behaviour from every client),
- surface the same error categories and policies (so users get a portable
  contract).

## Table of Contents

1. [Overview](#1-overview)
2. [Reference Implementation](#2-reference-implementation)
3. [Modes](#3-modes)
4. [Connect-String Options](#4-connect-string-options)
5. [Slot Directory Layout](#5-slot-directory-layout)
6. [Segment File Format](#6-segment-file-format)
7. [Frame-Sequence-Number (FSN) Model](#7-frame-sequence-number-fsn-model)
8. [WebSocket Handshake](#8-websocket-handshake)
9. [OK ACK and Trim](#9-ok-ack-and-trim)
10. [Durable-Ack ACK and Trim](#10-durable-ack-ack-and-trim)
11. [Keepalive PING](#11-keepalive-ping)
12. [Self-Sufficient Framing](#12-self-sufficient-framing)
13. [Reconnect and Replay](#13-reconnect-and-replay)
14. [Error Frame Handling](#14-error-frame-handling)
15. [WebSocket Close Codes](#15-websocket-close-codes)
16. [Backpressure](#16-backpressure)
17. [Close and Shutdown](#17-close-and-shutdown)
18. [Recovery and Orphan Adoption](#18-recovery-and-orphan-adoption)
19. [Protocol Invariants for Cross-Client Interop](#19-protocol-invariants-for-cross-client-interop)
20. [Observability](#20-observability)
21. [Deferred Items](#21-deferred-items)

## 1. Overview

The SF substrate sits between the user's `Sender` API and the QWP wire
encoder. Encoded QWP messages ("frames") are appended to an in-memory or
on-disk ring of fixed-size segments. A dedicated I/O thread drains the ring
over a WebSocket connection. The server's ACKs feed back into the ring as a
trim watermark, freeing storage. Disconnects, server restarts, and JVM
crashes are masked from the producer; the I/O thread reconnects and replays
whatever frames remain unacked.

Two modes share the same substrate:

- **Memory mode** — segments in malloc'd memory; data does not survive
  process exit but tolerates transient network blips.
- **SF mode** — segments are mmap'd files under a slot directory; data
  survives JVM crashes and process restarts. Recovery on next start replays
  unacked frames against a fresh server connection.

The producer never blocks on the wire. `flush()` returns once data is
published into the substrate (RAM in Memory mode, disk in SF mode); ACKs
arrive asynchronously.

## 2. Reference Implementation

The authoritative reference is the Java client at
[github.com/questdb/java-questdb-client](https://github.com/questdb/java-questdb-client),
under `core/src/main/java/io/questdb/client/cutlass/qwp/client/sf/cursor/`.
This spec was extracted from commit `1125b0b` (branch `vi_sf`,
2026-05-05). When the implementation drifts from this spec, the spec is
stale; raise it with the client maintainers and update this document.

Key files:

| Concern | File |
|---------|------|
| Segment file I/O | `MmapSegment.java` |
| Segment ring + rotation | `SegmentRing.java` |
| Manager thread + cap | `SegmentManager.java` |
| Slot lock | `SlotLock.java` |
| Orphan adoption | `OrphanScanner.java`, `BackgroundDrainerPool.java`, `BackgroundDrainer.java` |
| Engine (lifecycle, recovery) | `CursorSendEngine.java` |
| I/O thread (wire) | `CursorWebSocketSendLoop.java` |
| Wire response parser | `WebSocketResponse.java` (one level up) |
| Sender + connect-string parser | `Sender.java`, `QwpWebSocketSender.java` (one level up) |

## 3. Modes

| Aspect | Memory | SF |
|--------|--------|----|
| Storage | malloc'd ring | mmap'd files in `<sf_dir>/<sender_id>/` |
| Default total cap | 128 MiB | 10 GiB |
| Survives JVM exit | No | Yes |
| Survives JVM crash | No | Yes (recovery on next start) |
| Reconnect retries | Yes | Yes |
| Concurrent multi-sender slot collision | n/a | Detected at startup via advisory exclusive lock |
| Orphan adoption (drain another sender's stale slot) | n/a | Opt-in (`drain_orphans=on`) |

A client implementation may choose to support Memory mode only, SF mode
only, or both. Users select Memory mode by omitting `sf_dir` from the
connect string, and SF mode by including it.

## 4. Connect-String Options

All options pertain to the WebSocket transport (`ws::` / `wss::`). HTTP/TCP
ingest does not use the SF substrate. UDP has its own, separate model
(see [`wire-udp.md`](wire-udp.md)).

### 4.1 Storage (SF mode)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sf_dir` | path | unset (= Memory mode) | Group root. The slot lives at `<sf_dir>/<sender_id>/`. |
| `sender_id` | string | `default` | Slot subdirectory name. Two senders sharing a `sender_id` collide on the lock. |
| `sf_max_bytes` | size | `4M` | Per-segment file size; rotation threshold. |
| `sf_max_total_bytes` | size | `128M` (Memory) / `10G` (SF) | Cap on total buffered bytes; backpressures producer when full. |
| `sf_durability` | enum | `memory` | `memory` only is implemented; `flush` and `append` are reserved (see §21). |
| `sf_append_deadline_millis` | int (ms) | `30000` | How long the producer's `appendBlocking` waits for ACK-driven trim before throwing. |
| `drain_orphans` | bool | `off` | Scan `<sf_dir>/*` at startup and spawn drainers for sibling slots that contain unacked data. |
| `max_background_drainers` | int | `4` | Cap on concurrent orphan drainers. |

Size values accept integer bytes or unit suffixes (`K`, `M`, `G`, `T`,
binary multipliers).

### 4.2 Reconnect / connect

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `reconnect_max_duration_millis` | int (ms) | `300000` | Per-outage time budget. Resets on each successful reconnect. |
| `reconnect_initial_backoff_millis` | int (ms) | `100` | Initial backoff. |
| `reconnect_max_backoff_millis` | int (ms) | `5000` | Backoff ceiling. |
| `initial_connect_retry` | enum | `off` | `off`/`false` = terminal on first failure; `on`/`true`/`sync` = same retry loop as reconnect, blocking; `async` = same retry loop in the I/O thread, non-blocking. |
| `close_flush_timeout_millis` | int (ms) | `5000` | `close()` blocks up to this long waiting for `ackedFsn >= publishedFsn`. `0` or `-1` skips the drain entirely AND skips pre-drain `checkError()`. |

### 4.3 Durable-ack

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `request_durable_ack` | bool | `off` | Opt-in via the upgrade header. Trim is then driven by `STATUS_DURABLE_ACK` frames only; OK frames no longer advance the trim watermark. Connect fails loudly if the server does not echo `X-QWP-Durable-Ack: enabled`. |
| `durable_ack_keepalive_interval_millis` | int (ms) | `200` | Cadence of WebSocket PING the I/O loop sends while there are pending durable confirmations. `0` or negative disables. WebSocket-only. |

### 4.4 Error handling

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `error_inbox_capacity` | int (≥16) | `256` | Bounded SPSC capacity for async error notifications. Overflow drops oldest and bumps `droppedErrorNotifications`. |

The per-category override keys (`on_server_error`, `on_schema_error`,
`on_parse_error`, `on_internal_error`, `on_security_error`,
`on_write_error`) are reserved in the spec but are **not yet recognised** by
the Java parser; they are wired only via the fluent `LineSenderBuilder` API
today. New clients should accept them in connect strings — that's the
spec — and treat them with the precedence rules in §14.

### 4.5 Other (WebSocket-relevant)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `addr` | host[:port][,host[:port]…] | required | Server endpoint(s). Comma-separated for multi-host failover (parsed today; only the first is used; see §21). |
| `username` / `password` | string | unset | HTTP Basic auth on the upgrade request. |
| `token` | string | unset | Bearer token on the upgrade request. |
| `tls_verify` | enum | `on` | `on` or `unsafe_off`. Applies on `wss::` / TLS connections. |
| `tls_roots` | path | system trust | Path to a custom CA trust store. |
| `tls_roots_password` | string | unset | Trust store password. |
| `auto_flush` | bool | `on` | Master switch for auto-flush triggers. |
| `auto_flush_rows` | int / `off` | `1000` | Row-count flush trigger. |
| `auto_flush_bytes` | int / `off` | `0` (off) | Byte-size flush trigger. |
| `auto_flush_interval` | int (ms) / `off` | `100` | Time-since-first-row flush trigger. Triggered on the next `at()` / `flush()` call, not by a timer. |
| `init_buf_size` | size | `64K` | Initial encode buffer capacity. |
| `max_buf_size` | size | `100M` | Max encode buffer capacity. |
| `max_name_len` | int | `127` | Local validation cap for table/column names. |
| `in_flight_window` | int (>1) | `128` | Max concurrent unacked batches. |
| `max_schemas_per_connection` | int | `65535` | Per-connection schema-id ceiling. |

### 4.6 Validation

The parser MUST reject:

- Unknown keys (forward-compat is via the spec, not silent ignore).
- `sf_durability` values other than `memory`, `flush`, `append`. `flush` and
  `append` parse but are rejected at build-time today (deferred).
- `sender_id` containing path separators or empty.
- `request_durable_ack=on` on non-WebSocket transports.

## 5. Slot Directory Layout

In SF mode the substrate writes all files under one slot directory:

```
<sf_dir>/<sender_id>/
├── .lock              # advisory exclusive lock (kernel-released on process exit)
├── .lock.pid          # UTF-8 text: holder PID + '\n' (diagnostic only)
├── .failed            # OPTIONAL: drainer-failure sentinel (UTF-8 reason text)
├── sf-<gen>.sfa       # segment files (one or more)
└── sf-<gen>.sfa
```

### 5.1 `.lock` and `.lock.pid`

The `.lock` file is held under an advisory exclusive lock for the engine's
entire lifetime. POSIX clients use `flock`/`fcntl`; Windows clients use
`LockFileEx`. The lock is automatically released when the file descriptor
is closed, including on hard process exit (kernel cleanup).

A second engine pointing at the same slot dir MUST refuse to start. The
contract is: detect the collision at acquire time and fail loudly. The
error message MUST include the holder's PID by reading `.lock.pid`. The
PID is a separate file because Windows `LockFileEx` is mandatory and would
prevent reading the lock file's own bytes while it is held.

`.lock.pid` is overwritten on every successful acquisition with the
acquirer's PID followed by a newline, UTF-8 encoded. Absent or empty
`.lock.pid` is harmless; the failed acquire reports `holder=unknown`.

Neither `.lock` nor `.lock.pid` is removed on clean shutdown. Stale files
are harmless and are silently overwritten by the next acquirer.

### 5.2 `.failed`

Present iff a previous drainer attempt gave up on the slot (reconnect
budget exhausted, auth-terminal, irrecoverable corruption, etc.). The
contents are a UTF-8 text reason for human operators; the **presence**, not
the contents, is the signal. While `.failed` exists, the slot is excluded
from auto-drain by the orphan scanner. Operators clear it manually.

Drainers MUST write `.failed` before releasing the slot lock and exiting.

### 5.3 Segment files (`sf-<gen>.sfa`)

`<gen>` is a 16-character lowercase zero-padded hexadecimal generation
counter, e.g. `sf-0000000000000003.sfa`. The generation is allocated by the
segment manager monotonically across the JVM lifetime; on recovery it is
seeded to `max(existing-gen) + 1` to avoid collisions with recovered files.

The filename **does not** encode the segment's FSN range. It encodes only
the allocation order. The FSN range is read from the segment's header at
recovery time. `sf-initial.sfa` is a legacy name accepted by the recovery
scanner and skipped during max-gen computation.

Empty slot directories (`*.sfa`-less) are NOT auto-cleaned; the substrate
never deletes the slot directory itself.

## 6. Segment File Format

All multi-byte integers are **little-endian**. The reference implementation
relies on x86/ARM native byte order; clients MUST emit little-endian
explicitly to be portable.

A segment file consists of a 24-byte header followed by zero or more
frames. The file is pre-allocated at create time to its full size
(`sf_max_bytes`); the tail is zero-filled until written.

### 6.1 Header (24 bytes, fixed)

```
Offset  Size  Type    Field            Description
──────────────────────────────────────────────────────────────────
0       4     uint32  magic            0x31304653 ("SF01" little-endian)
4       1     uint8   version          0x01
5       1     uint8   flags            Reserved (must be 0)
6       2     uint16  reserved         Must be 0
8       8     uint64  baseSeq          FSN of the first frame in this segment
16      8     int64   createdMicros    Wall-clock microseconds at creation
```

`baseSeq` MUST be non-negative. The recovery scanner refuses segments with
`baseSeq < 0` to defend against bit-rot or malicious files.

`createdMicros` is informational. Implementations may use any monotonic or
wall-clock source; recovery does not validate it.

### 6.2 Frame envelope (8-byte header + payload)

Frames are tightly packed starting at offset 24. Each frame is:

```
Offset       Size           Type    Field        Description
──────────────────────────────────────────────────────────────────
0            4              uint32  crc32c       CRC32C of (payloadLen||payload), Castagnoli
4            4              int32   payloadLen   Payload size in bytes
8            payloadLen     bytes   payload      One QWP message (verbatim wire bytes)
```

`payloadLen` MUST be ≥ 0 and `≤ sizeBytes - offset - 8`. A negative or
overflowing length is treated as a torn-tail boundary by the recovery
scanner.

The CRC is **standard CRC-32C (Castagnoli)**: polynomial 0x1EDC6F41,
reflected in/out, init `0xFFFFFFFF`, final XOR `0xFFFFFFFF` (same
variant as iSCSI/SCTP/Btrfs). It covers `payloadLen` (4 bytes,
little-endian) followed by the payload bytes, in that order. Any
off-the-shelf CRC-32C library will produce identical checksums.

The payload is the complete QWP message including its 12-byte QWP1 header
(magic `QWP1`, version, flags, table count, payload length). See
[`wire-ingress.md`](wire-ingress.md).

### 6.3 Append protocol (single-writer)

The producer is the single writer to a segment. It appends a frame with
this sequence:

1. Compute `total = 8 + payloadLen` and check `appendCursor + total ≤ sizeBytes`.
   If not, return failure (caller rotates to the next segment).
2. Write `payloadLen` (little-endian uint32) at `mmap_addr + appendCursor + 4`.
3. Copy `payload` to `mmap_addr + appendCursor + 8`.
4. Compute CRC32C over the just-written `[payloadLen, payload]` bytes.
5. Write the CRC at `mmap_addr + appendCursor + 0`.
6. Bump `appendCursor` to `appendCursor + total` and `frameCount` by 1.
7. **Last**: publish via a release-store of `publishedCursor = appendCursor`.

Step 7 is the publication barrier. Until the volatile/release write of
`publishedCursor` retires, no consumer (I/O thread, drainer) is permitted
to read the new bytes. The CRC + zero-filled tail combination makes the
"interrupted at any point" outcome recoverable: the next recovery's CRC
verification will reject any partially-written frame.

### 6.4 Trim and rotation

A segment's last FSN is `baseSeq + frameCount - 1`. The segment becomes
trimmable once `ackedFsn >= lastSeq`. The ring's manager thread

1. closes the mmap and file descriptor,
2. unlinks the file,
3. decrements the substrate's `totalBytes` counter.

Trim is destructive — files are unlinked, not zeroed.

A new segment is rotated in when the active one fills. The manager
pre-allocates "hot spare" segments ahead of demand to keep the producer
non-blocking; rotation rebases the spare's `baseSeq` to the active's
`lastSeq + 1` at install time. This is implementation detail; cross-client
interop only requires that the on-disk layout described in §6.1–6.3 is
correct.

### 6.5 Recovery scan

For each `*.sfa` file in the slot:

1. Open the file, validate `length ≥ 24`, mmap.
2. Validate magic == `0x31304653`, version == `1`, `baseSeq ≥ 0`.
3. From offset 24, walk frames forward verifying each CRC. Stop at the
   first frame whose declared length overruns the file or whose CRC
   mismatches; the byte position becomes `lastGood`.
4. Set `frameCount = (count of verified frames)`, `appendCursor = lastGood`,
   `publishedCursor = lastGood`.
5. **Torn-tail detection**: if any byte in `[lastGood, lastGood + min(8, fileSize - lastGood))`
   is non-zero, the writer attempted-but-failed a frame write. Log a WARN
   with the count. Clean partial fills (writer never reached the tail)
   leave zeros and report 0.

After scanning all files, sort segments by `baseSeq` ascending and verify
`prev.baseSeq + prev.frameCount == curr.baseSeq` for each adjacent pair.
A gap is a fatal recovery error — refuse to start. The highest-`baseSeq`
segment becomes active; the rest are sealed and drained in order.

The substrate seeds `ackedFsn = lowest baseSeq − 1` so recovery does not
re-replay frames that were already trimmed before the previous shutdown.

## 7. Frame-Sequence-Number (FSN) Model

Two distinct counters operate together:

- **FSN** (frame-sequence-number): a monotonic counter assigned at
  append time, persisted in segment headers via `baseSeq`, never reset
  while the slot exists. FSN survives restarts and reconnects.
- **wireSeq**: the per-connection counter the server uses for
  deduplication, reset to 0 on every successful WebSocket upgrade.

The relationship is pinned at connect time:

```
fsn = fsnAtZero + wireSeq
```

On a fresh connection, `fsnAtZero = ackedFsn + 1` (i.e. `0` on a brand-new
slot, or wherever the trim watermark left off after a reconnect or
restart). The first frame the I/O loop sends has `wireSeq = 0`, regardless
of its FSN.

### 7.1 Server invariants the client relies on

- The server MUST dedup by `(connection, wireSeq)`. Replays after
  reconnect carry the same payload but at a fresh `wireSeq` window;
  dedup must use `messageSequence` inside the wire payload (which the
  encoder emits) to recognise duplicates across reconnects.
- The server's dedup window MUST be ≥ a sender's `sf_max_total_bytes`
  worth of FSNs. Otherwise a sustained outage with a full SF cap can
  produce double-writes on replay.
- The server's per-table seqTxn watermarks reported in OK and durable-ack
  frames are monotonically non-decreasing.

### 7.2 wireSeq is not in the wire frame

The QWP wire encoder does NOT serialize wireSeq into the message header.
It is implicit: the server assigns wireSeq to inbound messages in receive
order, starting at 0 per connection, and echoes it back in the `sequence`
field of OK and error frames. The client maps the echoed value back to
FSN via `fsn = fsnAtZero + sequence`.

A consequence: clients MUST send frames in strict order. The server
assumes "Nth frame in = wireSeq N", so any reordering breaks the FSN
mapping.

## 8. WebSocket Handshake

Open a WebSocket on the server's `/write/v4` (or `/api/v4/write`) path
with these request headers:

| Header | Required | Value |
|--------|----------|-------|
| `X-QWP-Max-Version` | recommended | Highest QWP version supported. Defaults to `1`. |
| `X-QWP-Client-Id` | optional | Free-form string (e.g. `cpp/0.1.0`). |
| `X-QWP-Accept-Encoding` | optional | Encoding preference; omit if not supported. |
| `X-QWP-Max-Batch-Rows` | optional | Per-batch row cap; omit for server default. |
| `X-QWP-Request-Durable-Ack` | iff `request_durable_ack=on` | Literal `true`. |
| `Authorization` etc. | as needed | Standard HTTP auth headers. |

On 101 the server responds with:

| Header | Meaning |
|--------|---------|
| `X-QWP-Version` | Negotiated version (defaults to 1 if absent). |
| `X-QWP-Durable-Ack` | Echoed `enabled` iff the client opted in AND the server can deliver durable-ack frames. |

### 8.1 Durable-ack handshake

If the client sent `X-QWP-Request-Durable-Ack: true`:

- A response with `X-QWP-Durable-Ack: enabled` confirms the server will
  emit `STATUS_DURABLE_ACK` frames. The client switches to durable-ack-
  driven trim (§10).
- **Absence** of the confirmation header means the server cannot deliver
  durable-acks (OSS build, primary not initialised, registry missing,
  replica). The client MUST fail the connection loudly with a
  `LineSenderException`-equivalent error; silently waiting for ack
  frames that will never arrive lets the SF grow until disk fills.

### 8.2 Authentication

Auth failures during upgrade (HTTP 401 / 403, non-101 status, invalid
upgrade response) are **terminal**. They neither retry the initial
connect nor trigger reconnect; they surface immediately as a
`SECURITY_ERROR` category error (HALT policy).

## 9. OK ACK and Trim

Every server response begins with a 1-byte status. OK is `0x00`.

### 9.1 OK frame layout

```
Offset    Size       Field          Notes
─────────────────────────────────────────────────────────
0         1          status         0x00
1         8          sequence       wireSeq (little-endian int64)
9         2          tableCount     uint16 LE; 0 for non-WAL or empty batches
11        variable   table entries  repeated tableCount times:
                                       2 bytes nameLen (LE)
                                       nameLen bytes name (UTF-8)
                                       8 bytes seqTxn (LE int64)
```

### 9.2 Default trim (no durable-ack)

On OK reception:

1. Compute `fsn = fsnAtZero + min(sequence, nextWireSeq - 1)`. The
   `min` defends against malicious or buggy servers reporting a wireSeq
   ahead of what the client sent.
2. `engine.acknowledge(fsn)`: advance the trim watermark, possibly
   freeing whole segments (§6.4).
3. Bump per-table seqTxn watermarks for any subsequent durability
   tracking (§10).

Auto-trim cadence is at the discretion of the I/O loop; the reference
implementation calls `acknowledge` immediately on every OK.

### 9.3 Durable-ack mode trim

When `request_durable_ack=on` was negotiated, OK frames **do not** advance
`ackedFsn`. They are stashed (with their per-table seqTxns) into a
`pendingDurable` queue keyed by wireSeq. Trim happens only on
`STATUS_DURABLE_ACK` (§10).

Empty OK frames (`tableCount=0`, e.g. materialized-view-only batches) are
trivially durable: their wireSeq advances `ackedFsn` as soon as all
preceding entries in `pendingDurable` have been confirmed.

## 10. Durable-Ack ACK and Trim

Durable-ack frames carry per-table watermarks for data already uploaded
from the server's WAL to the configured object store.

### 10.1 Durable-ack frame layout

```
Offset    Size       Field          Notes
─────────────────────────────────────────────────────────
0         1          status         0x02
1         2          tableCount     uint16 LE
3         variable   table entries  repeated tableCount times:
                                       2 bytes nameLen (LE)
                                       nameLen bytes name (UTF-8)
                                       8 bytes seqTxn (LE int64)
```

There is **no** `sequence` field. Durable-acks are cumulative per table:
each entry reports the highest seqTxn whose WAL segments are durable.

### 10.2 Drain loop

Maintain `durableTableWatermarks: Map<tableName, int64>` (monotonic, max-merge).

On each durable-ack frame:

1. For each entry, set `durableTableWatermarks[name] = max(current, seqTxn)`.
2. Walk the head of `pendingDurable` (FIFO of `(wireSeq, tableSeqTxnsForThisFrame)`).
3. An entry is "covered" iff for every `(name, seqTxn)` it carries,
   `durableTableWatermarks[name] >= seqTxn`.
4. Pop all consecutive covered head entries; call
   `engine.acknowledge(fsnAtZero + maxCoveredWireSeq)` once with the
   highest covered wireSeq.

The watermark advances strictly monotonically; reordered durable-ack
frames are tolerated by the max-merge.

### 10.3 Reconnect

On reconnect, `pendingDurable` is discarded. The new connection's OKs
re-stash entries; the server re-emits cumulative durable-ack watermarks
from scratch. Trim restarts against the new connection's wireSeq mapping.

## 11. Keepalive PING

The OSS server only flushes pending durable-ack frames during inbound
recv events (binary message, ping, close). An idle client that has
finished publishing would otherwise never see the durable-ack frame.

The SF I/O loop sends a WebSocket PING (any small payload; reference
sends 2 bytes) when **all** of these hold:

- `request_durable_ack=on` was negotiated.
- `pendingDurable` is non-empty.
- `durable_ack_keepalive_interval_millis > 0`.
- At least `durable_ack_keepalive_interval_millis` have elapsed since the
  last sent frame or PING.

In non-durable-ack mode the loop sends NO keepalive PINGs; ordinary frame
traffic keeps the connection alive on the server side.

PING is always a WebSocket-level PING (RFC 6455 opcode 0x9), not a QWP
message. Servers reply with PONG; clients ignore PONG content.

## 12. Self-Sufficient Framing

Every frame written to the SF substrate MUST be self-sufficient: it
carries the **full schema** for every table it touches and the **full
symbol-dictionary delta from id 0**. Schema-by-id refs are forbidden, and
incremental delta-dicts are forbidden.

Concretely, the encoder is invoked with:

- `confirmedMaxId = -1` (so the symbol delta starts at id 0 every time)
- `useSchemaRef = false` (so column blocks always carry full schema mode)

Rationale: SF frames may be replayed against a fresh server connection
weeks later — after process restart, after reconnect, or in a
background-drainer adopting an orphan slot. A frame that references
schema id N or symbol id M is unreplayable if the new server has never
seen those ids. Self-sufficiency makes every frame valid against any
server. The cost is a small per-batch overhead which is accepted.

This is mandatory for cross-client interop. A drainer written in C++
adopting a slot written by Java must be able to replay the bytes
unchanged.

## 13. Reconnect and Replay

### 13.1 Failure detection

Any wire error — send failure, recv failure, server close, ack timeout —
enters the reconnect loop. The producer thread is not notified; it keeps
publishing into the substrate (subject to the cap, §16).

### 13.2 Backoff

- Initial backoff = `reconnect_initial_backoff_millis` (default 100).
- After each attempt: `backoff = min(backoff * 2, reconnect_max_backoff_millis)`.
- Jitter: actual sleep is `backoff + uniform_rand(0, backoff)`, i.e. in
  `[backoff, 2 * backoff)`.
- Per-outage cap = `reconnect_max_duration_millis` (default 300000). Reset
  on each successful reconnect.
- If the next sleep would exceed the deadline, cap it to the remaining
  time.

### 13.3 Terminal conditions

- HTTP upgrade rejection (non-101 status, including 401/403/426) is
  terminal — does NOT consume the retry budget. Surfaces as
  `SECURITY_ERROR`.
- Reconnect-budget exhaustion is terminal. Surfaces as
  `PROTOCOL_VIOLATION` with FSN span = unacked window at giveup time.

### 13.4 Initial connect

By default (`initial_connect_retry=off`) any failure on the very first
connect is terminal — typically a misconfig, retrying for 5 minutes
just hides it. Setting `initial_connect_retry=sync` (or legacy
`on`/`true`) reuses the same retry loop, blocking the calling thread.
`async` runs the retry loop in the I/O thread and lets the constructor
return; the producer then experiences backpressure if it tries to
publish before the connection comes up.

### 13.5 Replay

On each successful (re)connect:

1. Set `fsnAtZero = engine.ackedFsn() + 1` (or `0` on a brand-new slot
   where `ackedFsn` is `-1`).
2. Reset `nextWireSeq = 0`.
3. Position the read cursor in the segment ring at `fsnAtZero`.
4. Stream frames from disk to the wire in FSN order, one frame per
   WebSocket binary message, incrementing `nextWireSeq`.
5. Frames originally appended after the disconnect (`fsn > replayTargetFsn`)
   are sent as new frames; the boundary affects only the
   `totalFramesReplayed` counter, not behaviour.

Producer threads MAY append concurrently during replay; the I/O loop
uses the volatile `publishedFsn()` cursor to discover newly-appended
frames.

## 14. Error Frame Handling

A non-OK status byte (other than 0x02) is an error frame.

### 14.1 Error frame layout

```
Offset    Size       Field          Notes
─────────────────────────────────────────────────────────
0         1          status         see table below
1         8          sequence       wireSeq the error applies to (LE int64)
9         2          msgLen         uint16 LE (≤ 1024)
11        msgLen     msg            UTF-8 error message
```

### 14.2 Status byte → category

| Status | Hex   | Category            |
|--------|-------|---------------------|
| 3      | 0x03  | `SCHEMA_MISMATCH`   |
| 5      | 0x05  | `PARSE_ERROR`       |
| 6      | 0x06  | `INTERNAL_ERROR`    |
| 8      | 0x08  | `SECURITY_ERROR`    |
| 9      | 0x09  | `WRITE_ERROR`       |
| any other (incl. 0x04, 0x07, 0xFF) | — | `UNKNOWN`            |
| (n/a — synthesized for WS close codes) | — | `PROTOCOL_VIOLATION` |

### 14.3 Default policy per category

| Category            | Default policy        | Rationale |
|---------------------|-----------------------|-----------|
| `SCHEMA_MISMATCH`   | `DROP_AND_CONTINUE`   | Replay reproduces the same rejection; halting blocks unrelated tables. |
| `PARSE_ERROR`       | `HALT`                | Almost certainly a client bug; preserve disk frames for postmortem. |
| `INTERNAL_ERROR`    | `HALT`                | Catch-all server fault; conservative without a retryable bit. |
| `SECURITY_ERROR`    | `HALT`                | Misconfig; loud failure wanted. |
| `WRITE_ERROR`       | `DROP_AND_CONTINUE`   | "Table not accepting writes" is per-batch in character. |
| `PROTOCOL_VIOLATION`| `HALT` (forced)       | Connection is gone — no choice. |
| `UNKNOWN`           | `HALT` (forced)       | Never silently drop something we don't understand. |

The two non-forced rows are user-overridable via the `on_*_error` connect
keys (when implemented; see §4.4).

### 14.4 DROP_AND_CONTINUE flow

1. Log a WARN with the wireSeq, category, status byte, and server message.
2. Build a `SenderError` and offer to the error inbox (non-blocking).
3. Advance the trim watermark past the rejected span:
   - Non-durable mode: `engine.acknowledge(fsnAtZero + sequence)`.
   - Durable mode: enqueue a trivially-durable empty entry into
     `pendingDurable`, then run the drain loop.
4. Keep draining the next frame.

DROP semantically discards the data — once the server has rejected, the
SF substrate is no longer the producer's safety net. Users who need a
dead-letter trail must register an error handler.

### 14.5 HALT flow

1. Build a `SenderError` and stash it as the loop's terminal error.
2. Offer to the error inbox.
3. Stop the loop. The producer's next API call observes the latched
   error and throws (typed exception carrying the `SenderError`).

### 14.6 Error inbox and dispatcher

- Bounded SPSC queue, capacity = `error_inbox_capacity` (default 256).
- Producer is the I/O thread. Consumer is a lazy-start daemon dispatcher
  thread that invokes the user's error handler.
- Overflow policy: drop the oldest entry, bump `droppedErrorNotifications`
  counter. Watermarks are monotonic, so the latest entry is always the
  most informative — drops compress information rather than lose it.
- The default error handler logs ERROR for HALT and WARN for DROP. Silence
  is forbidden; clients MUST install a non-silent default.

## 15. WebSocket Close Codes

WebSocket `Close` frames are inspected for the close code:

### 15.1 Terminal (PROTOCOL_VIOLATION)

Surface as `PROTOCOL_VIOLATION` / `HALT`. Do NOT enter the reconnect
loop.

| Code | Name |
|------|------|
| 1002 | PROTOCOL_ERROR |
| 1003 | UNSUPPORTED_DATA |
| 1007 | INVALID_PAYLOAD_DATA |
| 1008 | POLICY_VIOLATION |
| 1009 | MESSAGE_TOO_BIG |
| 1010 | MANDATORY_EXTENSION |

The `serverStatusByte` is `-1` and `messageSequence` is `-1` for
PROTOCOL_VIOLATION events. The error message is
`"ws-close[<code>]: <reason>"`. The FSN span is
`[engine.ackedFsn() + 1, engine.publishedFsn()]` — the unacked window at
close time.

### 15.2 Reconnect-eligible

Enter the reconnect loop (subject to per-outage cap):

| Code | Name |
|------|------|
| 1000 | NORMAL_CLOSURE |
| 1001 | GOING_AWAY |
| 1006 | ABNORMAL_CLOSURE |
| 1011 | INTERNAL_ERROR |
| any other code not listed in §15.1 | — |

## 16. Backpressure

The substrate enforces `sf_max_total_bytes` as a hard cap on resident
storage. When full, the producer's `appendBlocking` busy-spins (with
cooperative yield) for up to `sf_append_deadline_millis` waiting for ACK
arrival → trim → space free. If the deadline fires the call throws a
`LineSenderException`.

The error message MUST distinguish:

- *backpressure while wire is publishing*: server is acking but slowly.
- *backpressure while reconnecting*: I/O loop is in the retry loop. The
  message includes attempt count and outage start time.

The cap covers all sealed segments + the active segment. Trim immediately
returns segment bytes to the available pool when `ackedFsn` crosses
`baseSeq + frameCount - 1`.

## 17. Close and Shutdown

`close()` semantics depend on `close_flush_timeout_millis`:

- Default (`5000`): block waiting for `engine.ackedFsn() >= engine.publishedFsn()`
  for up to 5 seconds. If the wait succeeds, all data is acked. If the
  timeout fires, log a WARN and proceed; pending data remains on disk
  (SF mode) or is lost (Memory mode). Then run `checkError()` and
  rethrow any latched terminal error.
- `0` or `-1`: skip the drain entirely AND skip the pre-drain
  `checkError()`. Pending data is lost (Memory) or persists (SF). Users
  opting out observe outcomes only via the async `SenderProgressHandler`
  / `SenderErrorHandler` callbacks.
- Any other positive value: that timeout in milliseconds.

Implementations MUST always release the slot lock and unmap segments on
`close()`, regardless of timeout outcome. Implementations MUST rethrow
any latched terminal error from `close()` unless the user explicitly
opted out — otherwise a user who only ever calls `close()` could
silently lose data.

## 18. Recovery and Orphan Adoption

### 18.1 Foreground recovery

On engine open in SF mode:

1. Acquire `<sf_dir>/<sender_id>/.lock`. Refuse to start on contention.
2. Run §6.5 recovery scan over the slot's `*.sfa` files.
3. Seed `ackedFsn = lowestBaseSeq − 1`.
4. Seed segment manager `fileGeneration = max(existing-gen) + 1`.
5. Bump connection generation so the I/O loop, on first connect,
   replays from disk against a fresh wireSeq window.

A clean shutdown that drained all data is indistinguishable from a
fresh start — no segments, no replay, just `ackedFsn = -1`.

### 18.2 Orphan adoption (opt-in)

When `drain_orphans=on` the foreground sender, after acquiring its own
lock, scans `<sf_dir>/*` for sibling slots that:

- are NOT the foreground's own `sender_id`,
- contain at least one `*.sfa` file,
- do NOT contain `.failed`.

Note: lock state is intentionally NOT in the candidate filter; testing
flock state races with concurrent acquirers. The drainer pool tries the
locks in turn.

For each candidate, up to `max_background_drainers` at a time, spawn a
drainer that:

1. Tries to acquire the orphan slot's `.lock`. Skip on contention.
2. Opens its own engine + WebSocket connection (separate from the
   foreground's).
3. Runs §6.5 recovery and §13.5 replay over the orphan's data.
4. Releases the lock when `ackedFsn ≥ publishedFsn` and exits.

Drainer failure modes:
- Reconnect budget exhausted → drop `.failed` with a UTF-8 reason,
  release lock, exit.
- Auth-terminal upgrade error → same.
- Irrecoverable corruption (e.g. CRC failure with no torn-tail
  heuristic match) → same.

`.failed` slots are excluded from auto-drain on subsequent scans; the
operator must clear the sentinel manually.

## 19. Protocol Invariants for Cross-Client Interop

Mandatory for every conformant SF client:

- **On-disk format** (§6) is authoritative. Magic, header layout,
  CRC32C-Castagnoli, frame envelope, little-endian byte order. No
  per-client extensions in the segment header until a versioned format
  is agreed.
- **Filename pattern** (§5.3): `sf-<gen:016x>.sfa`. Other clients
  reading the slot need this to enumerate.
- **Slot lock semantics** (§5.1): advisory exclusive on `.lock`,
  PID in `.lock.pid`. Cross-client lock interop requires identical lock
  primitives — POSIX clients use `flock`/`fcntl`, Windows uses
  `LockFileEx`. A POSIX client and a Windows client MUST refuse to share
  a slot on a network filesystem.
- **`.failed` sentinel** (§5.2): presence (not contents) is the auto-drain
  exclusion signal.
- **FSN model** (§7): `fsn = fsnAtZero + wireSeq`. Strict in-order send
  on the wire.
- **Self-sufficient frames** (§12): mandatory. Schema refs and
  incremental delta-dicts are forbidden in SF frames.
- **Durable-ack handshake** (§8.1): if the client opts in, it MUST
  validate the response header and fail loudly on absence.
- **Trim driver**: in non-durable mode, OK frames advance trim. In
  durable mode, OK frames are tracked but do NOT advance trim; only
  durable-ack frames do.
- **Status byte ↔ category mapping** (§14.2) is normative. Clients MAY
  add forward-compat mapping for new server status bytes (treated as
  `UNKNOWN`).
- **WS close-code routing** (§15) is normative.
- **Connect-string keys** (§4) are normative names. Clients MUST accept
  spec-defined keys; they MAY accept implementation-specific extensions
  but should namespace them (e.g. `cpp_*`).

Permitted variations:

- Internal threading model.
- Manager / drainer thread allocation strategy.
- Hot-spare provisioning policy.
- Default error-handler formatting.

## 20. Observability

A conformant client SHOULD expose, at minimum:

| Counter | Meaning |
|---------|---------|
| `getTotalReconnectAttempts()` | Reconnect attempt count across the lifetime of the sender. |
| `getTotalReconnectsSucceeded()` | Successful reconnect count. |
| `getTotalFramesReplayed()` | Frames re-sent after a reconnect (i.e. `wireSeq` < `replayTargetFsn`). |
| `getTotalServerErrors()` | Count of error frames received (any category). |
| `getDroppedErrorNotifications()` | Count of error notifications dropped due to inbox overflow. |
| `getTotalErrorNotificationsDelivered()` | Count delivered to the user handler. |
| `getTotalBackpressureStalls()` | Count of producer threads that hit the cap. |
| `getLastTerminalError()` | The latched `SenderError` (or null). |
| `getBackgroundDrainers()` | Snapshot of `{slotDir, framesPending, framesAcked, lastError, isFailed}` per drainer. |

The default error handler MUST log every received `SenderError`. Silence is
forbidden by the contract: a buggy or no-op handler hides data loss
indistinguishably from a healthy connection.

## 21. Deferred Items

Items intentionally out of scope today, tracked for future revisions:

1. **`sf_durability=flush` and `=append`**: per-batch and per-frame fsync.
   Parser accepts the values; build-time rejects them with
   "not yet supported". The wire-level behaviour is unchanged; only the
   producer's flush path (and the `sf_append_deadline_millis` semantics)
   need new code.
2. **Multi-host failover**: see [`failover.md`](failover.md) for the
   normative spec covering host-health states, selection priority,
   backoff math, and the three context-specific loops (ingress non-SF
   initial connect, ingress SF reconnect loop, egress per-Execute
   loop).
3. **Retryable errors**: the wire format has no retryable bit; transient
   server faults and permanent ones both surface as `INTERNAL_ERROR`. A
   future server change could split status bytes or add a 1-byte field;
   when that lands, the spec gains a `RETRY_TRANSIENT` policy.
4. **Per-table attribution in multi-table batch errors**: today
   `tableName` is only populated when the rejected batch had exactly one
   table. The wire format would need an optional table index field for
   the multi-table case.
5. **`on_*_error` connect-string keys**: normatively defined in §4.4 but
   not yet recognised by the Java parser; only the fluent
   `LineSenderBuilder` API exposes them today.
6. **Per-segment partial-ack tracking**: would let trim drop the acked
   prefix of a partially-acked sealed segment, instead of waiting until
   the entire segment is acked. Improves disk efficiency under tail-heavy
   ack patterns; not implemented.
7. **`resumeAfterHalt()` API**: clears the latched terminal error and
   restarts the I/O loop without rebuilding the sender. The reference
   loop is one-shot today; users work around with `close()` + rebuild.

## Document History

| Date | Commit | Change |
|------|--------|--------|
| 2026-05-05 | client `1125b0b` / oss `dc108e66ff` | Initial spec extracted from Java reference. |
