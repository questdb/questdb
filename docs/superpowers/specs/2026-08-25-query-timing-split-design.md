# Accurate Query Timing: Execute/Wait Split and Time-to-First-Row

Date: 2026-08-25
Status: draft, pending review
Scope: query timing accuracy only. No retention config, no principal
filtering, no min-duration threshold, no non-SELECT trace coverage.

## Problem

`_query_trace.execution_micros` and the `fin` log line's `time=` report
cursor-open to cursor-close wall time. `QueryProgress` takes `beginNanos` at
`getCursor()` (`QueryProgress.java:291`) and computes the duration when the
cursor closes (`logEnd`, `QueryProgress.java:120`). Execution is pull-based:
the protocol layer drives `hasNext()`, and for streaming results the engine's
work interleaves with network writes paced by the client. A query that
executes in 20 ms but feeds a slow dashboard for 10 s records 10 s.

Any slow-query detection built on `_query_trace` therefore cannot
distinguish a slow query from a slow client. The HTTP `/exec` `timings`
field only escapes this because console queries are small enough that the
whole response fits in the send buffer; once serialization suspends on
socket backpressure, its `execute` number absorbs network wait too
(`JsonQueryProcessorState.java:1286` reads the clock when the trailing
`timings` JSON fragment is serialized).

The protocol layers, however, know exactly when they are blocked on the
client: suspension and resumption are explicit, discrete events. This spec
routes that knowledge into the trace.

## Timing model

For one query execution (cursor open to cursor close):

- `wall` -- open-to-close wall time. Existing semantics, unchanged.
- `wait` -- accumulated time during which the consumer had suspended the
  query for client/network reasons: socket backpressure, or a PGWire portal
  sitting between `Execute` messages. Measured by explicit pause/resume
  calls from the protocol layer.
- `active` -- `wall - wait`. Derived, not stored.
- `time_to_first_row` -- elapsed time from cursor open to the first
  successful `hasNext()` (or first non-null page frame). NULL when the
  query produced no rows.

Invariants: `0 <= wait <= wall`; `time_to_first_row` is NULL or in
`[0, wall]`.

Compile time is outside `wall` (the timer starts at cursor open, after the
factory is built). That is existing behavior and stays as is.

## Design

### 1. Cursor interface: pause/resume notifications

Add two default no-op methods to `io.questdb.cairo.sql.RecordCursor` and
`io.questdb.cairo.sql.PageFrameCursor`:

```java
default void suspendTimer() {
}

default void resumeTimer() {
}
```

Default methods keep every existing implementation source-compatible.
Only `QueryProgress.RegisteredRecordCursor` and
`QueryProgress.RegisteredPageFrameCursor` override them, delegating to the
owning `QueryProgress`. A cursor that is not progress-wrapped silently
ignores the calls.

### 2. Accounting in QueryProgress

New instance state on `QueryProgress`, reset on every cursor open (the
factory is cached and reused across executions):

- `long waitAccumNanos` -- accumulated wait.
- `long waitStartNanos` -- start of the in-flight wait; -1 when not
  suspended (doubles as the isSuspended flag).
- `long firstRowNanos` -- elapsed nanos to first row, or -1 (sentinel for
  "no row seen yet"; distinct from an actual measurement of 0).

Behavior:

- `suspendTimer()`: if not already suspended, record `waitStartNanos`.
  Idempotent -- a second call before `resumeTimer()` is a no-op.
- `resumeTimer()`: if suspended, add `now - waitStartNanos` to
  `waitAccumNanos` and clear the flag. Idempotent when not suspended.
- Cursor close while suspended (client disconnect, portal closed without
  re-execute): the close path performs an implicit `resumeTimer()` before
  computing the trace record, so the terminal wait interval is counted.
- First row: `RegisteredRecordCursor.hasNext()` sets `firstRowNanos` on
  the first `true` return (one predictable branch per call, one clock read
  per query). `RegisteredPageFrameCursor.next()` does the same on the
  first non-null frame.

`unregisterAndCleanup()` copies `waitAccumNanos` and `firstRowNanos` into
the `QueryTrace` before calling `logEnd`, alongside the existing fields.

The clock is the configuration's existing `NanosecondClock`, same as
`beginNanos`. Suspension can resume on a different worker thread;
`System.nanoTime()` is cross-thread monotonic on supported platforms and
`beginNanos`/`logEnd` already rely on that.

### 3. Protocol hook points

The rule at both layers: whenever the processor parks the connection while
a query cursor is open, call `cursor.suspendTimer()`; on the matching
resume, call `cursor.resumeTimer()` before pulling or serializing again.

**HTTP (`JsonQueryProcessor` / `JsonQueryProcessorState`)**

- Pause: where `PeerIsSlowToReadException` escapes the serialization loop
  with an open cursor (the processor's send path around
  `onRequestComplete`/`doResume`), before rethrowing to the dispatcher.
- Resume: at the top of `resumeSend()`, before serialization continues.

**PGWire (`PGConnectionContext` / `PGPipelineEntry`)**

Two distinct suspension kinds:

- Socket backpressure: same pattern as HTTP -- pause where
  `PeerIsSlowToReadException` propagates with an open cursor, resume in
  the send-resume path.
- Portal suspension: a portal that hits `maxRows` sends `PortalSuspended`
  and holds its cursor until the client's next `Execute` (or `Close`).
  Pause when `PortalSuspended` is emitted; resume when the next `Execute`
  message for that portal starts pulling rows. A portal closed without
  re-execution ends its wait via the implicit resume on cursor close.
  Time between portal fetches is client think-time and counts as wait --
  that is the point: today it counts as execution.

**QWP egress (`QwpEgressUpgradeProcessor` / `QwpEgressProcessorState`)**

- The state forwards timer calls to its retained `RecordCursor` or
  `PageFrameCursor`; one streaming query owns exactly one of them.
- Credit exhaustion suspends before `streamResults()` returns. A matching
  CREDIT resumes before re-entering `streamResults()`; a closed stream
  relies on the cursor-close implicit resume.
- Every streaming `PeerIsSlowToReadException` suspends before it parks.
  `resumeSend()` resumes only after `resumeResponseSend()` has completed,
  so a deferred flush that re-parks leaves the timer suspended. A resumed
  loop that re-parks suspends again before propagating the exception.

The streaming parquet-export path drives `PageFrameCursor` directly; its
suspend/resume sites get the same two calls on the page frame cursor.

### 4. QueryTrace and \_query\_trace schema

`QueryTrace` gains `long waitNanos` and `long firstRowNanos` (propagated
through `clear()` and `copyTo()`).

`_query_trace` gains two columns:

```
wait_micros LONG        -- accumulated client/network wait
first_row_micros LONG   -- elapsed to first row; NULL when no rows
```

`execution_micros` keeps its current meaning (wall). Active time is
`execution_micros - wait_micros` in SQL. Storing wall+wait rather than
wall+active preserves the existing column's semantics for anything already
reading it.

NULL semantics (per project convention, sentinel vs. real value):

- `wait_micros` = 0 is a genuine measurement ("never suspended"), so 0 is
  stored, never NULL.
- `first_row_micros` uses `Numbers.LONG_NULL` when the cursor never
  produced a row -- a zero-row query has no time-to-first-row; writing 0
  would fabricate one.

Migration: `QueryTracingJob.acquireTableWriter()` currently only issues
`CREATE TABLE IF NOT EXISTS`, so existing deployments have the 4-column
table. After resolving the token, the job checks writer metadata for the
new columns and issues `ALTER TABLE ... ADD COLUMN` for each missing one.
Column indices used by `runSerially()` switch from hard-coded positions to
indices resolved once from writer metadata at startup.

### 5. Log line

`logEnd` appends `, wait=<nanos>, ttfr=<nanos>` to the `fin` line when a
`QueryTrace` is present (the 6-arg overload; the 4-arg non-SELECT paths are
unchanged). `ttfr=-1` when no row was produced. ASCII only, per logging
convention.

### 6. HTTP /exec timings (additive, small)

The `timings` JSON object gains one key: `"wait"` (nanos, same units as
the existing keys). The existing `execute` key keeps its current
definition -- changing its meaning could break existing consumers,
including the web console's subtraction. Consumers that know about `wait`
can now compute honest execute/network numbers even for streamed
responses. Console UI changes are out of scope (separate repo).

## Overhead

- Suspend/resume accounting: two `nanoTime()` reads (~20-25 ns each) per
  suspension event. Suspensions involve epoll re-arm and a dispatcher
  round-trip (microseconds) and occur only when the query is already
  blocked. Queries that never suspend pay nothing.
- Time-to-first-row: one clock read per query plus one predictable branch
  per `hasNext()` call, adjacent to an existing virtual dispatch and
  try/catch.
- Memory: three longs on `QueryProgress`, two on `QueryTrace` (pooled).
  No allocation; zero-GC preserved.
- Baseline for comparison: a traced query already pays two clock reads,
  registry register/unregister, `fin` line formatting of the full SQL
  text, a `principal.toString()` allocation, and a queue enqueue.

No config gate: the accounting is always on. It also feeds the `fin` line,
which is on by default, and the cost is negligible against the existing
per-query bookkeeping.

## Edge cases

- Cached factory reuse: accounting fields reset at cursor open;
  the HTTP select cache polls a factory out while in use, and PGWire
  pipeline entries own their factories, so a `QueryProgress` instance is
  not driven concurrently.
- Double pause / double resume: idempotent by the `waitStartNanos` flag.
- Client disconnect mid-suspension: cursor close performs the implicit
  resume; the record shows large wait, small active -- correct.
- `toTop()` / count second pass (HTTP `count=true`): inside the same
  open-close window; no special handling, counts as active time.
- Queries not wrapped by `QueryProgress` (progress logger disabled paths):
  default no-op methods; nothing recorded, same as today.

## Testing

Per project convention: `assertMemoryLeak()` everywhere, resource cleanup
asserted on error paths, `.returns(...)` for query assertions. Timing
tests assert invariants, not absolute durations, to stay flake-free:
`wait_micros <= execution_micros`, `first_row_micros <= execution_micros`,
`wait_micros = 0` for fully-buffered responses, NULL `first_row_micros`
for zero-row queries.

- Unit: `QueryProgress` accounting against a controllable
  `NanosecondClock` (test configuration override) -- pause/resume pairing,
  idempotency, implicit resume on close, first-row sentinel.
- HTTP backpressure: small send buffer forcing
  `PeerIsSlowToReadException` mid-stream with a deliberately slow reader;
  assert `wait_micros > 0` and active time well below wall. Transport
  fault injection with a mock socket is the sanctioned use of fakes
  (partial sends), per testing rules.
- PGWire portal: JDBC client with `setFetchSize()` and autocommit off to
  force portal suspension; sleep between fetches; assert wait reflects a
  conservative lower bound of the sleep total.
- Zero-row and error paths: no first-row stamp; trace row (success) has
  NULL `first_row_micros`; error path leaks nothing.
- Migration: start engine against a pre-existing 4-column
  `_query_trace`; assert columns are added and rows land correctly.
- `/exec` timings: `timings=true` response contains `wait`; existing keys
  unchanged.
- QWP egress: the pinned `QwpQueryClient` with one-byte initial credit
  holds batch callbacks long enough to prove a nonzero traced wait;
  transport-fault coverage proves a deferred socket flush re-park does not
  resume early, and state forwarding covers retained record and page-frame
  cursors.

No wire constants or wire formats change, so no pinned-client submodule
test is required.

## Out of scope (deliberately)

Retention/TTL config, principal exclusion, `query.tracing.min.duration`,
non-SELECT trace coverage, per-batch active-time accumulation beyond the
suspend/resume model, console UI changes, and the external
collector/alerting system. Each builds on this but ships separately.
