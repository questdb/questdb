# Query Timing Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record accurate per-query timing in `_query_trace`: accumulated client/network wait (`wait_micros`) and time-to-first-row (`first_row_micros`) alongside the existing open-to-close wall time (`execution_micros`).

**Architecture:** `RecordCursor`/`PageFrameCursor` gain default no-op `suspendTimer()`/`resumeTimer()` methods. Only `QueryProgress`'s wrapper cursors override them, accumulating wait time and stamping time-to-first-row. The HTTP `/exec`, HTTP `/exp` export, and PGWire layers call the two methods at their existing suspend/resume seams (socket backpressure everywhere, plus PGWire portal suspension). Results flow into two new `_query_trace` columns, the `fin` log line, and a new `wait` key in the `/exec` `timings` JSON.

**Tech Stack:** Java 17 (core module), Maven, JUnit 4 with QuestDB's `AbstractCairoTest`/`BasePGTest`/`HttpQueryTestBuilder`/`AbstractBootstrapTest` infrastructure.

**Spec:** `docs/superpowers/specs/2026-08-25-query-timing-split-design.md`

## Global Constraints

- Zero-GC on data paths: no allocations in per-row or per-suspension code. New state is `long` fields on existing pooled/reused objects.
- Log messages: strictly ASCII.
- Boolean names use `is`/`has` prefixes.
- All timing tests assert invariants (`wait <= wall`, `ttfr <= wall`, `> 0`, `= 0`, NULL-ness), never absolute durations.
- Tests use `assertMemoryLeak()`; query assertions use the `assertQuery(sql).returns(...)` builder; DDL via `execute()`.
- `_query_trace` sentinel convention: `QueryTrace.firstRowNanos == -1` means "no row produced" and is stored as SQL NULL (`Numbers.LONG_NULL`); `waitNanos == 0` is a genuine measurement, stored as 0.
- `QueryProgress.waitStartNanos == -1` means "not suspended" (doubles as the isSuspended flag).
- Commit titles: short plain English, no Conventional Commits prefix, active-voice body wrapped at 72 chars.
- Line numbers below refer to the branch base, origin/master commit `2d9244fec3`. Verify with the quoted signatures before editing; nearby code may have shifted.
- Run `mvn` from the repo root of this worktree. Do not run multiple `mvn test` commands in parallel.

## File Structure

| File | Change |
|---|---|
| `core/src/main/java/io/questdb/cairo/sql/RecordCursor.java` | Add default `suspendTimer()`/`resumeTimer()` |
| `core/src/main/java/io/questdb/cairo/sql/PageFrameCursor.java` | Add default `suspendTimer()`/`resumeTimer()` |
| `core/src/main/java/io/questdb/metrics/QueryTrace.java` | Add `waitNanos`, `firstRowNanos` |
| `core/src/main/java/io/questdb/griffin/engine/QueryProgress.java` | Wait accounting, first-row stamp, fin-line fields |
| `core/src/main/java/io/questdb/metrics/QueryTracingJob.java` | Two new columns, startup migration, metadata-resolved write indices |
| `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessorState.java` | State-level wait accounting, cursor forwarding, `wait` timings key |
| `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessor.java` | Wire park/resume to state accounting |
| `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java` | `suspendCursorTimer()`/`resumeCursorTimer()`, portal resume hook |
| `core/src/main/java/io/questdb/cutlass/pgwire/PGConnectionContext.java` | Backpressure + portal-retention hooks |
| `core/src/main/java/io/questdb/cutlass/http/processors/ExportQueryProcessor.java` (+ its state class) | Park/resume forwarding for `/exp` cursors |
| `core/src/test/java/io/questdb/test/griffin/engine/QueryProgressTimingTest.java` | New: accounting unit tests |
| `core/src/test/java/io/questdb/test/metrics/QueryTracingTest.java` | Extend: columns, NULL semantics, migration |
| `core/src/test/java/io/questdb/test/cutlass/pgwire/PGQueryTimingTest.java` | New: portal-suspension wait |
| `core/src/test/java/io/questdb/test/cutlass/http/IODispatcherTest.java` (or the file holding the existing timings assertions) | Extend: `wait` key in timings JSON |
| `core/src/test/java/io/questdb/test/cutlass/http/QueryTimingHttpTest.java` | New: end-to-end slow-client test for `/exec` and `/exp` |

---

### Task 1: Cursor timer interface, QueryTrace fields, QueryProgress accounting

**Files:**
- Modify: `core/src/main/java/io/questdb/cairo/sql/RecordCursor.java`
- Modify: `core/src/main/java/io/questdb/cairo/sql/PageFrameCursor.java`
- Modify: `core/src/main/java/io/questdb/metrics/QueryTrace.java`
- Modify: `core/src/main/java/io/questdb/griffin/engine/QueryProgress.java`
- Create: `core/src/test/java/io/questdb/test/griffin/engine/QueryProgressTimingTest.java`

**Interfaces:**
- Consumes: existing `QueryProgress` structure — `beginNanos` set at cursor open (`getCursor()` line 291, `getPageFrameCursor()` line 341), `unregisterAndCleanup(Throwable)` line 455 calling the 6-arg `logEnd` at line 463, the pooled `queryTrace` field.
- Produces: `RecordCursor.suspendTimer()` / `RecordCursor.resumeTimer()` and identical methods on `PageFrameCursor` (default no-ops, overridden by `QueryProgress`'s inner cursors); `QueryTrace.waitNanos` (long, 0 = never suspended) and `QueryTrace.firstRowNanos` (long, -1 = no row produced), populated by the time `logEnd` enqueues the trace. Tasks 3–5 call the cursor methods; Task 2 reads the two `QueryTrace` fields.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/griffin/engine/QueryProgressTimingTest.java`. The test drives the clock via `AbstractCairoTest.currentMicros` (the static test nano clock returns `currentMicros * 1000`), and asserts on the `QueryTrace` object dequeued straight from the message-bus queue — no `QueryTracingJob` runs, so traces stay in the queue.

```java
package io.questdb.test.griffin.engine;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryProgressTimingTest extends AbstractCairoTest {

    @Before
    public void setup() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testNoRowsLeavesFirstRowUnset() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_empty (x LONG)");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            try (
                    RecordCursorFactory factory = select("tab_empty");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertFalse(cursor.hasNext());
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(-1, trace.firstRowNanos);
            Assert.assertEquals(0, trace.waitNanos);
        });
    }

    @Test
    public void testSuspendIsIdempotent() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_idem AS (SELECT x FROM long_sequence(1))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab_idem");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                cursor.resumeTimer(); // resume with no suspend: no-op
                currentMicros = 1_100;
                cursor.suspendTimer();
                currentMicros = 1_200;
                cursor.suspendTimer(); // second suspend: no-op, interval keeps running
                currentMicros = 1_400;
                cursor.resumeTimer();  // wait = 1400 - 1100 = 300us
                cursor.resumeTimer();  // second resume: no-op
                while (cursor.hasNext()) {
                }
                currentMicros = 2_000;
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(300_000L, trace.waitNanos);
        });
    }

    @Test
    public void testTerminalSuspensionIsCountedOnClose() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab_term AS (SELECT x FROM long_sequence(2))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab_term");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                Assert.assertTrue(cursor.hasNext());
                currentMicros = 1_500;
                cursor.suspendTimer();
                currentMicros = 2_000;
                // close while suspended: implicit resume must count 500us
            }
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(500_000L, trace.waitNanos);
            Assert.assertEquals(1_000_000L, trace.executionNanos);
        });
    }

    @Test
    public void testWaitAndFirstRowAccounting() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(3))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            currentMicros = 1_000;
            try (
                    RecordCursorFactory factory = select("tab");
                    RecordCursor cursor = factory.getCursor(sqlExecutionContext)
            ) {
                currentMicros = 1_500;
                Assert.assertTrue(cursor.hasNext()); // first row at 1500 -> ttfr 500us
                currentMicros = 1_600;
                cursor.suspendTimer();
                currentMicros = 1_900;
                cursor.resumeTimer();                // wait 300us
                while (cursor.hasNext()) {
                }
                currentMicros = 2_000;
            }                                        // wall = 1000us
            final QueryTrace trace = new QueryTrace();
            Assert.assertTrue(queue.tryDequeue(trace));
            Assert.assertEquals(1_000_000L, trace.executionNanos);
            Assert.assertEquals(300_000L, trace.waitNanos);
            Assert.assertEquals(500_000L, trace.firstRowNanos);
        });
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        final QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
    }
}
```

Notes for the implementer:
- `select(CharSequence)` on `AbstractCairoTest` compiles a SELECT; with the progress logger on (the default for one-shot selects) the returned factory is a `QueryProgress`, so `cursor.suspendTimer()` reaches the accounting.
- `currentMicros` is reset by the base class between tests; if a test fails with wall-clock-sized numbers, the override did not take — check that `currentMicros` is set before `getCursor()`.
- The trace is enqueued at cursor close (that is why assertions sit after the try block).

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl core test -Dtest=QueryProgressTimingTest -DfailIfNoTests=false`
Expected: COMPILE ERROR — `QueryTrace` has no `firstRowNanos`/`waitNanos`, `RecordCursor` has no `suspendTimer()`. That compile failure is this step's "red".

- [ ] **Step 3: Add the default interface methods**

In `core/src/main/java/io/questdb/cairo/sql/RecordCursor.java`, add (near the other default methods):

```java
/**
 * Notifies the cursor that the consumer stops pulling rows for reasons
 * unrelated to query execution, such as network backpressure or a
 * suspended PGWire portal. Timing-aware cursors exclude the interval
 * until {@link #resumeTimer()} from active execution time. No-op by
 * default. Both methods are idempotent.
 */
default void suspendTimer() {
}

/**
 * Ends the wait interval started by {@link #suspendTimer()}. No-op by
 * default and when the cursor is not suspended.
 */
default void resumeTimer() {
}
```

Add the same two methods with the same javadoc to `core/src/main/java/io/questdb/cairo/sql/PageFrameCursor.java`.

- [ ] **Step 4: Extend QueryTrace**

In `core/src/main/java/io/questdb/metrics/QueryTrace.java` add two public fields, keeping the field list alphabetical like the existing ones:

```java
public long executionNanos;
// Elapsed nanos from cursor open to the first row; -1 when the query
// produced no rows. -1 (not 0) is the sentinel because 0 is a legitimate
// measurement under a coarse test clock.
public long firstRowNanos = -1;
public boolean isJit;
public String principal;
public String queryText;
public long timestamp;
public long waitNanos;
```

Update `clear()` (add `firstRowNanos = -1; waitNanos = 0;`) and `copyTo()` (copy both).

- [ ] **Step 5: Implement accounting in QueryProgress**

In `core/src/main/java/io/questdb/griffin/engine/QueryProgress.java`:

a. New instance fields next to `beginNanos`:

```java
private long beginNanos;
// Nanosecond clock cached at cursor open; used by the timer methods,
// which may run after executionContext is nulled.
private NanosecondClock clock;
private long firstRowNanos;
private long waitAccumNanos;
// -1 when the timer is running (not suspended); doubles as the flag.
private long waitStartNanos;
```

(`io.questdb.std.datetime.nanotime.NanosecondClock` — match the type `CairoConfiguration.getNanosecondClock()` returns; adjust the import to whatever the getter declares.)

b. In both open blocks — `getCursor()` (after the `beginNanos = ...` assignment at line 291) and `getPageFrameCursor()` (same pattern at line 341):

```java
clock = executionContext.getCairoEngine().getConfiguration().getNanosecondClock();
beginNanos = clock.getTicks();
waitAccumNanos = 0;
waitStartNanos = -1;
firstRowNanos = -1;
```

(Replace the existing `beginNanos = executionContext...getTicks();` line with the cached-clock form.)

c. Private accounting methods on `QueryProgress`:

```java
private void onConsumerResume() {
    if (waitStartNanos != -1) {
        waitAccumNanos += clock.getTicks() - waitStartNanos;
        waitStartNanos = -1;
    }
}

private void onConsumerSuspend() {
    if (waitStartNanos == -1 && executionContext != null) {
        waitStartNanos = clock.getTicks();
    }
}
```

(`executionContext != null` guards a suspend arriving after close; `unregisterAndCleanup` nulls it.)

d. Overrides in `RegisteredRecordCursor` and `RegisteredPageFrameCursor` (both inner classes):

```java
@Override
public void resumeTimer() {
    onConsumerResume();
}

@Override
public void suspendTimer() {
    onConsumerSuspend();
}
```

e. First-row stamp. In `RegisteredRecordCursor.hasNext()`:

```java
@Override
public boolean hasNext() {
    try {
        final boolean hasNext = base.hasNext();
        if (hasNext && firstRowNanos == -1) {
            firstRowNanos = clock.getTicks() - beginNanos;
        }
        return hasNext;
    } catch (Throwable th) {
        close0(th);
        throw th;
    }
}
```

In `RegisteredPageFrameCursor.next(long skipTarget)`:

```java
@Override
public @Nullable PageFrame next(long skipTarget) {
    final PageFrame frame = baseCursor.next(skipTarget);
    if (frame != null && firstRowNanos == -1) {
        firstRowNanos = clock.getTicks() - beginNanos;
    }
    return frame;
}
```

f. In `unregisterAndCleanup(Throwable th)` (line 455), before the `logEnd`/`logError` calls:

```java
// A close during suspension (client disconnect, abandoned portal) ends
// the terminal wait interval here so it is counted.
onConsumerResume();
queryTrace.waitNanos = waitAccumNanos;
queryTrace.firstRowNanos = firstRowNanos;
```

g. Fin-line fields. In the 6-arg `logEnd`, after `.$(", time=").$(durationNanos)`:

```java
if (queryTrace != null) {
    log.$(", wait=").$(queryTrace.waitNanos)
            .$(", ttfr=").$(queryTrace.firstRowNanos);
}
```

(ASCII only. The 4-arg overload passes `queryTrace = null` and its line is unchanged.)

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -pl core test -Dtest=QueryProgressTimingTest -DfailIfNoTests=false`
Expected: PASS (4 tests).

- [ ] **Step 7: Guard against regressions in the surrounding suites**

Run: `mvn -pl core test -Dtest='QueryTracingTest,QueryRegistryTest' -DfailIfNoTests=false`
Expected: PASS. (`QueryTracingTest` exercises the unchanged 4-column write path — it must still pass untouched in this task.)

- [ ] **Step 8: Commit**

```bash
git add core/src/main/java/io/questdb/cairo/sql/RecordCursor.java \
        core/src/main/java/io/questdb/cairo/sql/PageFrameCursor.java \
        core/src/main/java/io/questdb/metrics/QueryTrace.java \
        core/src/main/java/io/questdb/griffin/engine/QueryProgress.java \
        core/src/test/java/io/questdb/test/griffin/engine/QueryProgressTimingTest.java
git commit -m "Track query wait time and time-to-first-row

QueryProgress accumulates consumer wait time between new
RecordCursor/PageFrameCursor suspendTimer()/resumeTimer() default
methods and stamps the elapsed time to the first row. The wrapper
copies both values into QueryTrace before logEnd, and the fin log
line prints them as wait= and ttfr=. Protocol layers do not call
the new methods yet, so behavior is unchanged outside tests."
```

---

### Task 2: \_query\_trace columns and startup migration

**Files:**
- Modify: `core/src/main/java/io/questdb/metrics/QueryTracingJob.java`
- Modify: `core/src/test/java/io/questdb/test/metrics/QueryTracingTest.java`

**Interfaces:**
- Consumes: `QueryTrace.waitNanos` (0 default) and `QueryTrace.firstRowNanos` (-1 sentinel) from Task 1.
- Produces: `_query_trace` columns `wait_micros LONG` and `first_row_micros LONG` (appended after `principal`); constants `QueryTracingJob.COLUMN_WAIT_MICROS = "wait_micros"` and `QueryTracingJob.COLUMN_FIRST_ROW_MICROS = "first_row_micros"`. `first_row_micros` is SQL NULL when `firstRowNanos == -1`.

- [ ] **Step 1: Write the failing tests**

Extend `core/src/test/java/io/questdb/test/metrics/QueryTracingTest.java` (keep its existing structure: `setup()` enables `PropertyKey.QUERY_TRACING_ENABLED` and drops `_query_trace`; tests build a one-worker `WorkerPool`, assign a `new QueryTracingJob(engine)`, and poll with exponential backoff). Add:

```java
@Test
public void testMigrationAddsTimingColumns() throws Exception {
    assertMemoryLeak(() -> {
        // Simulate a pre-upgrade deployment: 4-column table already on disk.
        engine.execute(
                "CREATE TABLE '_query_trace' (" +
                        "ts TIMESTAMP, query_text VARCHAR, execution_micros LONG, principal VARCHAR" +
                        ") TIMESTAMP(ts) PARTITION BY HOUR TTL 1 DAY BYPASS WAL"
        );
        try (QueryTracingJob ignore = new QueryTracingJob(engine)) {
            assertQuery("SELECT column_name FROM (SHOW COLUMNS FROM '_query_trace') WHERE column_name IN ('wait_micros', 'first_row_micros')")
                    .returns("column_name\nwait_micros\nfirst_row_micros\n");
        }
    });
}

@Test
public void testTraceRowCarriesTimingColumns() throws Exception {
    assertMemoryLeak(() -> {
        // Same worker pool + job scaffold as testQueryTracing.
        final String query = "SELECT table_name FROM tables() WHERE table_name = 'no_such'";
        // ... run the query via assertQuery/execute as testQueryTracing does ...
        // then poll (same backoff loop) until this returns count=1:
        // SELECT count() FROM _query_trace
        // WHERE query_text = '<query>'
        //   AND wait_micros = 0
        //   AND first_row_micros >= 0
        //   AND first_row_micros <= execution_micros
    });
}

@Test
public void testZeroRowQueryHasNullFirstRow() throws Exception {
    // Same scaffold; the traced query returns no rows, then poll until:
    // SELECT count() FROM _query_trace
    // WHERE query_text = '<query>' AND first_row_micros IS NULL AND wait_micros = 0
    // returns 1.
}
```

The `// ...` scaffold lines mean: copy the worker-pool/job/polling structure from the existing `testQueryTracing()` in the same file verbatim (pool of 1, `workerPool.assign(job)`, `start(LOG)`, `halt()` in finally, exponential-backoff polling with `.noLeakCheck()` on intermediate assertions). It is boilerplate shared with the test directly above the new ones — mirror it, do not redesign it.

Careful with `testTraceRowCarriesTimingColumns`: the polling SELECTs against `_query_trace` are themselves traced. Filter on the exact traced query text (as `testQueryTracing` does), and note a query that scans `tables()` with zero result rows belongs in `testZeroRowQueryHasNullFirstRow`, while the carries-columns test needs a query that returns at least one row (e.g. `SELECT 1`), so `first_row_micros >= 0`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl core test -Dtest=QueryTracingTest -DfailIfNoTests=false`
Expected: the three new tests FAIL (no such columns); `testQueryTracing` still PASSES.

- [ ] **Step 3: Implement columns, migration, and write path**

In `core/src/main/java/io/questdb/metrics/QueryTracingJob.java`:

a. Constants (alongside the existing `COLUMN_*`):

```java
public static final String COLUMN_FIRST_ROW_MICROS = "first_row_micros";
public static final String COLUMN_WAIT_MICROS = "wait_micros";
```

b. Extend the CREATE DDL in `acquireTableWriter()` (line 88-96) so a fresh table has six columns:

```java
.$(COLUMN_PRINCIPAL).$(" VARCHAR, ")
.$(COLUMN_WAIT_MICROS).$(" LONG, ")
.$(COLUMN_FIRST_ROW_MICROS).$(" LONG")
```

c. Migrate pre-existing tables. After `engine.getWriter(tableToken, WRITER_LOCK_REASON)` returns (line 100), while holding the writer:

```java
final TableWriter writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
try {
    if (writer.getMetadata().getColumnIndexQuiet(COLUMN_WAIT_MICROS) < 0) {
        writer.addColumn(COLUMN_WAIT_MICROS, ColumnType.LONG);
    }
    if (writer.getMetadata().getColumnIndexQuiet(COLUMN_FIRST_ROW_MICROS) < 0) {
        writer.addColumn(COLUMN_FIRST_ROW_MICROS, ColumnType.LONG);
    }
} catch (Throwable th) {
    writer.close();
    throw th;
}
return writer;
```

(Doing this via `TableWriter.addColumn` while holding the writer avoids a second SQL round-trip and cannot race the permanent writer lock. If `addColumn`'s signature demands more arguments in this checkout, mirror the simplest existing caller.)

d. Resolve write indices from metadata once, instead of the hard-coded `1/2/3` in `runSerially()` (lines 121-125). New final fields set in the constructor after `acquireTableWriter()`:

```java
private final int executionMicrosColumnIndex;
private final int firstRowMicrosColumnIndex;
private final int principalColumnIndex;
private final int queryTextColumnIndex;
private final int waitMicrosColumnIndex;
```

```java
final TableRecordMetadata metadata = tableWriter.getMetadata();
queryTextColumnIndex = metadata.getColumnIndex(COLUMN_QUERY_TEXT);
executionMicrosColumnIndex = metadata.getColumnIndex(COLUMN_EXECUTION_MICROS);
principalColumnIndex = metadata.getColumnIndex(COLUMN_PRINCIPAL);
waitMicrosColumnIndex = metadata.getColumnIndex(COLUMN_WAIT_MICROS);
firstRowMicrosColumnIndex = metadata.getColumnIndex(COLUMN_FIRST_ROW_MICROS);
```

e. Write the new values in `runSerially()`:

```java
final TableWriter.Row row = tableWriter.newRow(trace.timestamp);
putVarchar(row, queryTextColumnIndex, trace.queryText);
row.putLong(executionMicrosColumnIndex, trace.executionNanos / Micros.MICRO_NANOS);
putVarchar(row, principalColumnIndex, trace.principal);
row.putLong(waitMicrosColumnIndex, trace.waitNanos / Micros.MICRO_NANOS);
row.putLong(
        firstRowMicrosColumnIndex,
        trace.firstRowNanos < 0 ? Numbers.LONG_NULL : trace.firstRowNanos / Micros.MICRO_NANOS
);
row.append();
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl core test -Dtest=QueryTracingTest -DfailIfNoTests=false`
Expected: PASS, including the pre-existing `testQueryTracing`.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/io/questdb/metrics/QueryTracingJob.java \
        core/src/test/java/io/questdb/test/metrics/QueryTracingTest.java
git commit -m "Add wait_micros and first_row_micros to _query_trace

QueryTracingJob creates the trace table with two extra LONG columns
and, on startup against a pre-upgrade 4-column table, adds them via
TableWriter.addColumn while holding the permanent writer. The write
path resolves column indices from writer metadata instead of
hard-coded positions. first_row_micros stores SQL NULL when the
query produced no rows; wait_micros stores 0 when the query never
suspended, which is a genuine measurement rather than a sentinel."
```

---

### Task 3: HTTP /exec hooks and timings wait key

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessorState.java`
- Modify: `core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessor.java`
- Modify: whichever test file holds the existing `timings=true` JSON assertions (search `grep -rn '"timings"' core/src/test/java/io/questdb/test/cutlass/http/`) — expected `IODispatcherTest.java`

**Interfaces:**
- Consumes: `RecordCursor.suspendTimer()`/`resumeTimer()` from Task 1; existing state fields `cursor` (line 121), `nanosecondClock` (line 107), `executeStartNanos` (line 124); seams `JsonQueryProcessor.parkRequest(HttpConnectionContext, boolean)` (line 307), `resumeSend(HttpConnectionContext)` (line 328), `onRequestRetry(HttpConnectionContext)` (line 301), `startExecutionTimer()` (state line 400).
- Produces: `JsonQueryProcessorState.suspendExecutionTimer()` and `resumeExecutionTimer()` (public, idempotent); a `"wait"` key (nanos) appended to the `timings` JSON object.

- [ ] **Step 1: Update the existing timings-JSON expectations (the failing test)**

The HTTP test builder's default nano clock is `StationaryNanosClock.INSTANCE`, so existing timings tests exact-match a fully deterministic JSON. Find them:

```bash
grep -rn '"timings"' core/src/test/java/io/questdb/test/cutlass/http/
```

In each expected-response string, extend the timings object with `,"wait":0` as its last member, e.g.

```
"timings":{"authentication":0,"compiler":0,"execute":0,"count":0}
```
becomes
```
"timings":{"authentication":0,"compiler":0,"execute":0,"count":0,"wait":0}
```

(Whatever the actual key set/order is in those fixtures — append `wait` last and mirror it exactly in Step 3. If any fixture pins a `Content-Length`, recompute it or prefer the builder variant that ignores it.) If no existing test asserts the timings JSON, add one to `IODispatcherTest` using the nearest `testJsonQuery*` as the template with `&timings=true` in the URL and the full expected response body.

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -pl core test -Dtest=IODispatcherTest#<the timings test methods> -DfailIfNoTests=false`
Expected: FAIL — response lacks the `wait` key.

- [ ] **Step 3: Implement state accounting and wire the seams**

a. `JsonQueryProcessorState` — new fields next to `executeStartNanos`:

```java
private long waitAccumNanos;
// -1 when not parked; doubles as the isParked flag.
private long waitStartNanos = -1;
```

b. New public methods (grouped with the other publics):

```java
public void resumeExecutionTimer() {
    if (waitStartNanos != -1) {
        waitAccumNanos += nanosecondClock.getTicks() - waitStartNanos;
        waitStartNanos = -1;
    }
    if (cursor != null) {
        cursor.resumeTimer();
    }
}

public void suspendExecutionTimer() {
    if (waitStartNanos == -1) {
        waitStartNanos = nanosecondClock.getTicks();
    }
    if (cursor != null) {
        cursor.suspendTimer();
    }
}
```

c. Reset in `startExecutionTimer()` (line 400) and in the request-scoped `clear()`:

```java
waitAccumNanos = 0;
waitStartNanos = -1;
```

d. Timings JSON (lines 1281-1289): append the `wait` member last, matching Step 1's fixtures:

```java
.putAscii(',').putAsciiQuoted("wait").putAscii(':').put(waitAccumNanos)
```

(If a suspension is in flight when the suffix serializes, `waitAccumNanos` holds the wait so far; the remainder is unobservable in that response and lands only in `_query_trace`. This is best-effort by design.)

e. `JsonQueryProcessor` seams:
- `parkRequest(...)` (line 307): first statement inside the existing state null-check: `state.suspendExecutionTimer();`
- `resumeSend(...)` (line 328): after the circuit-breaker/rnd restore, before `doResumeSend(...)` (line 341): `state.resumeExecutionTimer();`
- `onRequestRetry(...)` (line 301): before delegating to `execute0`: `state.resumeExecutionTimer();`

(`parkRequest` fires for both socket backpressure and async-operation retries; the retry path has no open cursor so the cursor calls no-op, and DDL responses render no timings block, so counting the retry park in state wait is harmless and keeps the seams symmetric.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl core test -Dtest=IODispatcherTest -DfailIfNoTests=false`
Expected: PASS — timings fixtures updated in Step 1 now match; every other `/exec` fixture without `timings=true` is untouched.

- [ ] **Step 5: Commit**

```bash
git add core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessorState.java \
        core/src/main/java/io/questdb/cutlass/http/processors/JsonQueryProcessor.java \
        core/src/test/java/io/questdb/test/cutlass/http/IODispatcherTest.java
git commit -m "Report wait time in /exec timings and HTTP query traces

JsonQueryProcessor forwards its park/resume seams into the open
query cursor, so _query_trace excludes socket backpressure from a
query's active time on the /exec path. The state keeps its own
wait accumulator over the same seams and the timings JSON gains a
wait key carrying it; existing timings keys keep their meaning."
```

---

### Task 4: PGWire hooks (backpressure and portal suspension)

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java`
- Modify: `core/src/main/java/io/questdb/cutlass/pgwire/PGConnectionContext.java`
- Create: `core/src/test/java/io/questdb/test/cutlass/pgwire/PGQueryTimingTest.java`

**Interfaces:**
- Consumes: `RecordCursor.suspendTimer()`/`resumeTimer()` from Task 1; `QueryTrace.waitNanos`/`firstRowNanos` from Task 1; PGWire seams — backpressure throw in `doSendWithRetries` (`PGConnectionContext.java:542`), resume via `resumeCallback` (`handleClientOperation`, lines 412-414), portal retention in `syncPipeline()` (lines 1470-1477), retained-cursor re-execute in `PGPipelineEntry.msgExecuteSelect` (guard `if (cursor == null)` at line 1688), cursor field `PGPipelineEntry.cursor` (line 182).
- Produces: `PGPipelineEntry.suspendCursorTimer()` and `resumeCursorTimer()` (public, null-safe).

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cutlass/pgwire/PGQueryTimingTest.java`:

```java
package io.questdb.test.cutlass.pgwire;

import io.questdb.PropertyKey;
import io.questdb.metrics.QueryTrace;
import io.questdb.mp.ConcurrentQueue;
import io.questdb.std.Os;
import io.questdb.test.cutlass.pgwire.BasePGTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGQueryTimingTest extends BasePGTest {

    @Before
    public void setupTracing() {
        node1.getConfigurationOverrides().setProperty(PropertyKey.QUERY_TRACING_ENABLED, true);
    }

    @Test
    public void testPortalSuspensionCountsAsWait() throws Exception {
        assertWithPgServer(CONN_AWARE_EXTENDED, (connection, binary, mode, port) -> {
            execute("CREATE TABLE tab AS (SELECT x FROM long_sequence(100))");
            final ConcurrentQueue<QueryTrace> queue = engine.getMessageBus().getQueryTraceQueue();
            drain(queue);
            connection.setAutoCommit(false); // required for portal suspension via fetch size
            final String query = "SELECT x FROM tab";
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setFetchSize(10);
                try (ResultSet resultSet = statement.executeQuery()) {
                    int rows = 0;
                    while (resultSet.next()) {
                        if (++rows % 10 == 0) {
                            // client think-time while the portal is suspended
                            Os.sleep(20);
                        }
                    }
                    Assert.assertEquals(100, rows);
                }
            }
            connection.commit();
            final QueryTrace trace = pollTraceFor(queue, query);
            Assert.assertTrue("expected wait > 0, got " + trace.waitNanos, trace.waitNanos > 0);
            Assert.assertTrue(trace.waitNanos <= trace.executionNanos);
            Assert.assertTrue(trace.firstRowNanos >= 0);
            Assert.assertTrue(trace.firstRowNanos <= trace.executionNanos);
        });
    }

    private static void drain(ConcurrentQueue<QueryTrace> queue) {
        final QueryTrace trace = new QueryTrace();
        while (queue.tryDequeue(trace)) {
        }
    }

    private static QueryTrace pollTraceFor(ConcurrentQueue<QueryTrace> queue, String query) {
        final QueryTrace trace = new QueryTrace();
        final long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            while (queue.tryDequeue(trace)) {
                if (query.equals(trace.queryText)) {
                    return trace;
                }
            }
            Os.sleep(50);
        }
        Assert.fail("no trace for query: " + query);
        return null;
    }
}
```

Notes for the implementer:
- `BasePGTest.assertWithPgServer(CONN_AWARE_EXTENDED, ...)` — mirror the exact functional-interface shape used by neighboring tests in `PGJobContextTest` (the lambda arity varies between checkouts). `CONN_AWARE_EXTENDED` restricts to the extended protocol, which is what portal suspension requires; if the constant set differs, pick the extended-protocol-only mode used by the `testBasicFetch` tests.
- The trace enqueues when the portal's cursor closes (data exhausted at the 10th fetch), inside the server, possibly after the client's last `next()` returns — hence the polling.
- `System.currentTimeMillis()` in test polling is fine; production code uses the injected clocks.
- No frozen clock here: `BasePGTest` randomizes buffer sizes per run, so assert only invariants. The 20 ms sleeps make `waitNanos > 0` robust — each sits between two portal fetches while the cursor is suspended.

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl core test -Dtest=PGQueryTimingTest -DfailIfNoTests=false`
Expected: FAIL on `waitNanos > 0` (PGWire never calls the timer hooks yet; wait stays 0).

- [ ] **Step 3: Implement the hooks**

a. `PGPipelineEntry` — public null-safe forwarding methods (near `closeSuspendedCursor()`, line 380):

```java
public void resumeCursorTimer() {
    if (cursor != null) {
        cursor.resumeTimer();
    }
}

public void suspendCursorTimer() {
    if (cursor != null) {
        cursor.suspendTimer();
    }
}
```

b. Socket backpressure suspend — `PGConnectionContext.doSendWithRetries` (line 517), immediately before `throw PeerIsSlowToReadException.INSTANCE;` (line 542):

```java
if (pipelineCurrentEntry != null) {
    pipelineCurrentEntry.suspendCursorTimer();
}
```

c. Socket backpressure resume — `PGConnectionContext.handleClientOperation` (line 382), inside the `resumeCallback != null` branch (lines 412-414), before invoking `resumeCallback.resume()`:

```java
if (pipelineCurrentEntry != null) {
    pipelineCurrentEntry.resumeCursorTimer();
}
```

d. Portal suspension — `PGConnectionContext.syncPipeline()`, in the retention branch (lines 1470-1477) that `break`s while keeping a suspended entry, before the `break`:

```java
pipelineCurrentEntry.suspendCursorTimer();
```

This branch — not the `PortalSuspended`-message send in `PGPipelineEntry.msgSync` — is the correct hook: it runs after the sync response is fully flushed, so a backpressure resume (hook c) that fires mid-sync gets re-suspended here, and the drain-to-next-Execute gap is charged to wait, not active time.

e. Portal resume — `PGPipelineEntry.msgExecuteSelect` (line 1681): the `if (cursor == null)` guard (line 1688) opens a fresh cursor; add the retained-cursor branch:

```java
} else {
    // Execute resumes a suspended portal; its retained cursor was
    // suspended when the pipeline parked it in syncPipeline().
    resumeCursorTimer();
}
```

No hook is needed on `closeSuspendedCursor()` (line 380) or the `SYNC_DATA_EXHAUSTED` cursor free: closing the cursor triggers `QueryProgress`'s implicit terminal resume from Task 1.

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl core test -Dtest=PGQueryTimingTest -DfailIfNoTests=false`
Expected: PASS.

- [ ] **Step 5: Guard the PGWire suite**

Run: `mvn -pl core test -Dtest='PGJobContextTest' -DfailIfNoTests=false`
Expected: PASS. Every PG test randomizes send-buffer fragmentation in `setUp()`, so this run exercises the backpressure hooks (b)+(c) across the whole protocol surface.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cutlass/pgwire/PGPipelineEntry.java \
        core/src/main/java/io/questdb/cutlass/pgwire/PGConnectionContext.java \
        core/src/test/java/io/questdb/test/cutlass/pgwire/PGQueryTimingTest.java
git commit -m "Exclude PGWire client wait from traced query time

PGConnectionContext suspends the current pipeline entry's cursor
timer when a send hits socket backpressure and resumes it when the
socket drains. syncPipeline suspends a portal retained across
Execute messages after its sync response flushes, and
msgExecuteSelect resumes the retained cursor on the next Execute,
so client think-time between portal fetches counts as wait rather
than execution. Cursor close covers abandoned portals through the
terminal resume in QueryProgress."
```

---

### Task 5: /exp export hooks and end-to-end slow-client test

**Files:**
- Modify: `core/src/main/java/io/questdb/cutlass/http/processors/ExportQueryProcessor.java` (and its state class holding `cursor`/`pageFrameCursor` — same file or sibling `ExportQueryProcessorState.java`)
- Create: `core/src/test/java/io/questdb/test/cutlass/http/QueryTimingHttpTest.java`

**Interfaces:**
- Consumes: `suspendTimer()`/`resumeTimer()` on both cursor types (Task 1); `_query_trace` columns (Task 2); `/exec` seams (Task 3); export seams — `parkRequest(HttpConnectionContext, boolean)` (`ExportQueryProcessor.java:321`), `resumeSend(HttpConnectionContext)` (line 331), state fields `cursor` and `pageFrameCursor` (set at lines 206-223).
- Produces: park/resume timer forwarding on the `/exp` path; the feature's end-to-end HTTP test.

- [ ] **Step 1: Write the failing test**

Create `core/src/test/java/io/questdb/test/cutlass/http/QueryTimingHttpTest.java`, extending `AbstractBootstrapTest` (full `ServerMain`, so the real `QueryTracingJob` drains traces into `_query_trace`):

```java
package io.questdb.test.cutlass.http;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.cutlass.http.SendAndReceiveRequestBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryTimingHttpTest extends AbstractBootstrapTest {

    @Before
    public void setUp() {
        super.setUp();
        // Tiny send buffer forces mid-stream suspension on any non-trivial
        // result; tracing must be on for _query_trace rows to appear.
        // Property names: PropertyKey.QUERY_TRACING_ENABLED.getPropertyPath(),
        // PropertyKey.HTTP_SEND_BUFFER_SIZE.getPropertyPath().
    }

    @Test
    public void testSlowExecClientCountsAsWait() throws Exception {
        try (TestServerMain serverMain = startWithEnvVariables(
                PropertyKey.QUERY_TRACING_ENABLED.getEnvVarName(), "true",
                PropertyKey.HTTP_SEND_BUFFER_SIZE.getEnvVarName(), "1024"
        )) {
            serverMain.start();
            serverMain.execute("CREATE TABLE tab AS (SELECT x, rnd_str(64, 64, 0) s FROM long_sequence(10_000))");

            // Slow reader: SendAndReceiveRequestBuilder with a pause between
            // send and receive, so the server fills its 1 KiB buffer and
            // suspends while the client is not reading.
            new SendAndReceiveRequestBuilder()
                    .withPort(HTTP_PORT)
                    .withPauseBetweenSendAndReceive(500)
                    .executeUntilDisconnect(
                            "GET /exec?query=" + urlEncode("SELECT * FROM tab") + " HTTP/1.1\r\n"
                                    + SendAndReceiveRequestBuilder.RequestHeaders,
                            /* read the full response, discarding content */
                    );

            // The trace lands asynchronously via QueryTracingJob; poll.
            assertEventually(() -> serverMain.assertSql(
                    "SELECT count() FROM _query_trace"
                            + " WHERE query_text = 'SELECT * FROM tab'"
                            + " AND wait_micros > 0"
                            + " AND wait_micros <= execution_micros"
                            + " AND first_row_micros IS NOT NULL",
                    "count\n1\n"
            ));
        }
    }

    @Test
    public void testSlowExpClientCountsAsWait() throws Exception {
        // Same scaffold, but against GET /exp?query=... (CSV export path),
        // same _query_trace assertion for that query text.
    }
}
```

Implementer notes (this scaffold intentionally names the intent; bind it to the real helper APIs found in neighboring `AbstractBootstrapTest` subclasses rather than inventing new infrastructure):
- `startWithEnvVariables(...)` / `TestServerMain` usage: copy from any sibling bootstrap test (e.g. the ones asserting via `serverMain.assertSql`). If properties are passed via `server.conf` writing instead, use that mechanism — search `HTTP_SEND_BUFFER_SIZE` in `core/src/test` for precedent.
- The slow-read mechanics: `SendAndReceiveRequestBuilder` field `pauseBetweenSendAndReceive` (line 74) delays the first read; for a sustained slow drain use `executeExplicit(...)` (line 125) with an `HttpClientStateListener` that sleeps in `onReceived`. The requirement is only: the server must block on the socket at least once mid-result. 10 000 rows of 64-char strings against a 1 KiB send buffer guarantees that many times over.
- `assertEventually` — if `AbstractBootstrapTest` lacks such a helper, write the same exponential-backoff loop used by `QueryTracingTest` (100 ms doubling to 6.4 s), treating `AssertionError` as retry.
- The `/exp` test drives `ExportQueryProcessor`'s CSV path (`state.cursor`); it fails before Step 3 because the export processor never forwards park/resume. The parquet-export `pageFrameCursor` branch is covered by the same forwarding code but not separately end-to-end tested here (the CSV and parquet paths share `parkRequest`/`resumeSend`).

- [ ] **Step 2: Run tests to verify the expected failure mode**

Run: `mvn -pl core test -Dtest=QueryTimingHttpTest -DfailIfNoTests=false`
Expected: `testSlowExecClientCountsAsWait` PASSES already (Task 3 wired `/exec`) — it validates the whole pipeline end to end. `testSlowExpClientCountsAsWait` FAILS on `wait_micros > 0`. If the `/exec` test does not pass here, stop and debug Tasks 1-3 before touching the export processor.

- [ ] **Step 3: Implement export forwarding**

In `ExportQueryProcessor` (state class): add null-safe forwarding that covers both cursor kinds:

```java
public void resumeCursorTimers() {
    if (cursor != null) {
        cursor.resumeTimer();
    }
    if (pageFrameCursor != null) {
        pageFrameCursor.resumeTimer();
    }
}

public void suspendCursorTimers() {
    if (cursor != null) {
        cursor.suspendTimer();
    }
    if (pageFrameCursor != null) {
        pageFrameCursor.suspendTimer();
    }
}
```

Wire them:
- `parkRequest(...)` (line 321): after the state is resolved, `state.suspendCursorTimers();`
- `resumeSend(...)` (line 331): before `doResumeSend(context)` (line 335), `state.resumeCursorTimers();`

(For the parquet export, the page-frame cursor may be handed off to the export task and nulled on the state at line 714; after that hand-off the state-level calls no-op, which is correct — the exporter path re-throws `PeerIsSlowToReadException` at `copyQueryToParquetFile` line 731-733 and its accounting can be refined later if parquet exports need the split. This limitation is deliberate scope control; note it in the PR body.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -pl core test -Dtest=QueryTimingHttpTest -DfailIfNoTests=false`
Expected: PASS (both tests).

- [ ] **Step 5: Run the surrounding export and HTTP suites**

Run: `mvn -pl core test -Dtest='QueryExportTest,IODispatcherTest' -DfailIfNoTests=false`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add core/src/main/java/io/questdb/cutlass/http/processors/ExportQueryProcessor.java \
        core/src/test/java/io/questdb/test/cutlass/http/QueryTimingHttpTest.java
git commit -m "Count slow export readers as wait time in query traces

ExportQueryProcessor forwards its park and resume seams to the open
record or page-frame cursor, so CSV /exp downloads paced by a slow
client no longer inflate a query's active execution time in
_query_trace. The new bootstrap test drives a real server with a
1 KiB send buffer and a stalling reader over both /exec and /exp
and asserts wait_micros lands between zero and execution_micros.
The parquet export task's handed-off page-frame cursor is not yet
timed after hand-off; the CSV path and all /exec queries are."
```

---

### Task 6: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Run the broader affected suites sequentially**

```bash
mvn -pl core test -Dtest='QueryProgressTimingTest,QueryTracingTest,PGQueryTimingTest,QueryTimingHttpTest,IODispatcherTest,PGJobContextTest,QueryExportTest' -DfailIfNoTests=false
```

Expected: PASS. Any failure here is a live bug in this branch until proven otherwise — do not dismiss as flaky without reproducing on the base commit.

- [ ] **Step 2: Confirm no unintended diff**

```bash
git status --short
git diff --stat master...HEAD
```

Expected: only the files listed in this plan's File Structure table (plus the spec and this plan).

- [ ] **Step 3: Verification-before-completion**

Use the superpowers:verification-before-completion skill before claiming the branch done. The PR (title suggestion: `feat(sql): report accurate query execution time excluding client wait`; labels: SQL, Core, Performance) bundles all commits; PR body must present the parquet-export hand-off limitation and the best-effort in-flight-wait timings caveat with the same weight as the improvements.
