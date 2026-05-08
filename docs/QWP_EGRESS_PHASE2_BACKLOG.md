# QWP Egress — Phase 2 Backlog

Open work items for the QWP egress feature. Phase 1 is feature-complete and
tested (see `docs/QWP_EGRESS_EXTENSION.md` for the spec and the
`QwpEgress*Test` suites for the covered scenarios).

## Carried over from Phase 1

### `QwpResultCursor` row-iterator wrapper

A blocking row-by-row API on top of the column-batch consumer. Purely
convenience — the column-batch API already covers all functional use cases.
Low priority polish.

### Mid-stream CANCEL against an actively WRITE-parked stream

The CANCEL frame, its plumbing through the server (cancel flag, `streamResults`
observation, `STATUS_CANCELLED` mapping), and the client-side `cancel()` API
are all complete. But the IO dispatcher registers each fd for a single
operation (READ xor WRITE). While the server is parked on WRITE during an
unbounded-credit streaming query, kernel-buffered CANCEL frames are only
dispatched after the write unblocks — by which point the query has often
completed. Under credit-based flow control this is a non-issue: the server
parks cooperatively between batches and the fd is re-registered for READ, so
CANCEL is observed before the next batch. Fixing the unbounded-credit path
requires dispatcher-level dual-registration (READ + WRITE during streaming).

## Phase 2 candidates, by ascending effort

| Rank | Item | Effort | Rationale |
|---|---|---|---|
| 1 | Allocation tracker in the benchmark (`ThreadMXBean.getAllocatedBytes`) to quantify the delta the Phase 1 zero-alloc refactors bought | Tiny | Closes the loop on the "how much did we save?" question without standing up a profiler. |
| 2 | Tighten SYMBOL bind handling — server is currently lenient and accepts SYMBOL wire type by routing it through STRING. Either reject SYMBOL bind type code at the decoder, or formalise the leniency in the spec. | Tiny | Cleanup; no functional issue. |
| 3 | Lazy per-column decode on the client — skip decoding columns the user never reads | Small | Helps `COUNT(*)` / projection-heavy queries. Requires per-column dispatch on first access. |
| 4 | Factor the duplicated codec into a shared module between parent and submodule | Small but cross-repo | Removes the "change wire format -> update 4 places" risk. |
| 5 | Mid-stream CANCEL for unbounded-credit streams (see above) | Medium | Requires dispatcher dual-registration. Only credit-flow streams observe CANCEL today. |
| ~~6~~ | ~~Client-side bind encoder~~ — done. `QwpBindValues` + new `execute(sql, binds, handler)` overload cover every scalar type the server decoder accepts (BOOLEAN, BYTE, SHORT, CHAR, INT, LONG, FLOAT, DOUBLE, TIMESTAMP, TIMESTAMP_NANOS, DATE, VARCHAR, UUID, LONG256, GEOHASH, DECIMAL64/128/256). Tests: `QwpBindEncoderTest` (submodule unit, 39), `QwpEgressBindRoundTripTest` (parent E2E, 31), `QwpEgressBindFuzzTest` (parent fuzz, 4). | Small | Server-side bind decoder already supports every scalar type; the `QwpQueryClient.execute` API now exposes a typed builder. |
| 7 | Multiple in-flight queries per connection with a fair scheduler | Medium | Relevant for BI dashboard clients that want to multiplex several queries on one socket. Phase 1 hard-limits to one in-flight query. |
| 8 | `wss://` (TLS) support in the client | Medium | Server already supports TLS; the client does not yet expose it. |

## Not-yet-scoped, but likely Phase 3

- **HTTP/2 or HTTP/3** transport for connection multiplexing. QWP wire format would sit inside a stream instead of a WebSocket frame.
- **Prepared statements** — send a `PREPARE` + handle once, then `EXECUTE` with binds. Saves server-side compile cost for repeated-shape queries.
- **Multi-statement script support** (today Phase 1 rejects everything except `SELECT` / `PSEUDO_SELECT` / the supported non-SELECT set that returns `EXEC_DONE`).
- **TLS path tuning** — align plaintext buffer to OpenSSL/JSSE record size.
- **Backpressure feedback to the planner** — reject queries whose first-row latency exceeds a threshold rather than exhaust the credit window before the first batch.

## Phase 1 done-state (as reference)

All items in this list are net-new work; everything below is already landed on
`vi_egress`:

- Wire protocol spec: `docs/QWP_EGRESS_EXTENSION.md`.
- Server: `/read/v1` endpoint, handshake, `QUERY_REQUEST` decoder with binds
  for all scalar types, cursor -> `RESULT_BATCH` -> `RESULT_END` loop,
  per-connection schema registry, native column scratches + reusable Decimal
  sinks + cached UTF-8 column names.
- Non-SELECT execution (`EXEC_DONE`, 0x16) for DDL, INSERT, UPDATE, ALTER,
  DROP, TRUNCATE, CREATE TABLE, CREATE MAT VIEW, and parse-time-executed
  statements.
- Connection-scoped delta SYMBOL dictionary (`FLAG_DELTA_SYMBOL_DICT`) with a
  per-column native-keyed dedup map.
- Gorilla delta-of-delta encoding (`FLAG_GORILLA`) for TIMESTAMP /
  TIMESTAMP_NANOS / DATE columns, with per-column raw-vs-encoded fallback.
- Whole-batch-body zstd compression (`FLAG_ZSTD`) negotiated via
  `X-QWP-Accept-Encoding` / `X-QWP-Content-Encoding`, with a reused
  `ZSTD_CCtx` per connection and per-batch raw fallback when the compressed
  form is not smaller.
- CANCEL frame plumbing (flag, `streamResults` observation, status mapping,
  client API) and CREDIT flow control with cooperative park/resume.
- HTTP upgrade split across `onHeadersReady` / `onRequestComplete` /
  `resumeSend` to tolerate small send-buffer fragmentation.
- Client (submodule): `QwpQueryClient` with dedicated I/O thread + SPSC batch
  queue, zero-alloc column batch with direct UTF-8 views, per-type raw
  accessors, raw-column-address API, lazy `ZSTD_DCtx` with a 64 MiB
  decompress scratch cap, early zstd probe at `connect()`.
- 16 end-to-end test classes (165 tests) plus the submodule
  `QwpResultBatchDecoderHardeningTest`.
- User-facing examples under `java-questdb-client/examples/` covering basic
  query, typed result, error handling, credit flow, `EXEC_DONE`, compression,
  and large-result streaming.
- Read-side JMH-style benchmarks comparing QWP / PGWire / HTTP+JSON (narrow
  and wide schemas).
