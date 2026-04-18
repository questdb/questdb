# QWP Egress — Phase 2 Backlog

Open work items for the QWP egress feature, carried over from Phase 1. Phase 1
is feature-complete and tested (see `docs/QWP_EGRESS_EXTENSION.md` for the spec
and the `QwpEgressBootstrapTest` suite for the covered scenarios).

## Carried over from Phase 1

### #10 — `QwpResultCursor` row-iterator wrapper

A blocking row-by-row API on top of the column-batch consumer. Purely
convenience — the column-batch API already covers all functional use cases.
Low priority polish.

## Phase 2 candidates, by ascending effort

| Rank | Item | Effort | Rationale |
|---|---|---|---|
| 1 | Allocation tracker in the benchmark (`ThreadMXBean.getAllocatedBytes`) to quantify the delta the Phase 1 zero-alloc refactors bought | Tiny | Closes the loop on the "how much did we save?" question without standing up a profiler. |
| 2 | **CANCEL** wiring — server currently parses and logs; should translate into a circuit-breaker trip on the active cursor | Small | Needed for long-running queries that the user wants to abort. |
| 3 | Lazy per-column decode on the client — skip decoding columns the user never reads | Small | Helps `COUNT(*)` / projection-heavy queries. Requires per-column dispatch on first access. |
| 4 | Factor the duplicated codec into a shared module between parent and submodule | Small but cross-repo | Removes the "change wire format → update 4 places" risk. |
| 5 | **CREDIT** flow control wiring — server already parses the frame | Medium | Enables memory-bounded consumers and proper slow-client back-pressure. |
| 6 | Server yield/resume state machine for slow consumers — mirrors the ingress `RESUME_*` state machine so the HTTP worker doesn't stall on `rawSocket.send` | Medium | Required for production robustness; complements #5. |
| 7 | LZ4 / Zstd batch compression — flag bit is already reserved in the spec, negotiated on handshake | Medium | 2–3× bandwidth savings on text-heavy results. |
| 8 | Multiple in-flight queries per connection with a fair scheduler | Medium | Relevant for BI dashboard clients that want to multiplex several queries on one socket. Phase 1 hard-limits to one in-flight query. |
| 9 | Native byte-keyed symbol dict to eliminate the last allocation source (HashMap entry + `String.toString()` per unique SYMBOL value per batch) | Medium | Small absolute gain unless symbol cardinality is very high. |
| 10 | Connection-scoped delta SYMBOL dictionary (`FLAG_DELTA_SYMBOL_DICT`) — save bandwidth on dashboards that re-query the same set of symbols. Spec already describes the future shape; Phase 1 ships the simpler per-batch inline dict. | Medium | Bandwidth win for repeated-query workloads (BI dashboards). |
| 11 | Tighten SYMBOL bind handling — server is currently lenient and accepts SYMBOL wire type by routing it through STRING. Either reject SYMBOL bind type code at the decoder, or formalise the leniency in the spec. | Tiny | Cleanup; no functional issue. |

## Not-yet-scoped, but likely Phase 3

- **HTTP/2 or HTTP/3** transport for connection multiplexing. QWP wire format would sit inside a stream instead of a WebSocket frame.
- **Prepared statements** — send a `PREPARE` + handle once, then `EXECUTE` with binds. Saves server-side compile cost for repeated-shape queries.
- **Multi-statement script support** (today Phase 1 rejects everything except `SELECT` / `PSEUDO_SELECT`).
- **TLS path tuning** — align plaintext buffer to OpenSSL/JSSE record size.
- **Backpressure feedback to the planner** — reject queries whose first-row latency exceeds a threshold rather than exhaust the credit window before the first batch.

## Phase 1 done-state (as reference)

All items in this list are net-new work; everything below is already landed on
`vi_egress`:

- Wire protocol spec: `docs/QWP_EGRESS_EXTENSION.md`.
- Server: `/read/v1` endpoint, handshake, QUERY_REQUEST decoder with binds for
  all scalar types, cursor → RESULT_BATCH → RESULT_END loop, per-connection
  schema registry, native column scratches + reusable Decimal sinks +
  cached UTF-8 column names.
- Client (submodule): `QwpQueryClient` with dedicated I/O thread + SPSC batch
  queue, zero-alloc column batch with direct UTF-8 views, per-type raw
  accessors, raw-column-address API.
- 11 E2E tests covering all supported wire types, NULL handling, large result
  streaming, SQL errors, and non-SELECT rejection.
- 4 user-facing examples under `java-questdb-client/examples/`.
- Read-side JMH-style benchmark comparing QWP / PGWire / HTTP+JSON.
