/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByFillValueRecordCursorFactory;
import io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware keyed SAMPLE BY FILL operators:
 * <ul>
 *     <li>{@link io.questdb.griffin.engine.groupby.SampleByFillValueRecordCursorFactory} /
 *     {@link io.questdb.griffin.engine.groupby.SampleByFillNullRecordCursorFactory} ->
 *     {@code SampleByFillValueRecordCursor} (OrderedMap + GROUP BY allocator),</li>
 *     <li>{@link io.questdb.griffin.engine.groupby.SampleByFillPrevRecordCursorFactory} ->
 *     {@code SampleByFillPrevRecordCursor},</li>
 *     <li>{@link io.questdb.griffin.engine.groupby.SampleByFillNoneRecordCursorFactory} ->
 *     {@code SampleByFillNoneRecordCursor},</li>
 *     <li>{@link io.questdb.griffin.engine.groupby.SampleByInterpolateRecordCursorFactory}
 *     (recordKeyMap + dataMap + allocator) for {@code FILL(LINEAR)}.</li>
 * </ul>
 * The keyed FILL cursors construct their map(s) and GROUP BY allocator in lazy
 * mode and bind the active workload's MemoryTracker before the first cursor
 * open, so a runaway SAMPLE BY whose key set (or per-group function state) grows
 * past the limit crosses it at the offending allocation site. The non-keyed FILL
 * variants are out of scope (single SimpleMapValue, one group) and are not
 * exercised here. The GROUP BY fast-path {@code SampleByFillRecordCursorFactory}
 * (the default / {@code ALIGN TO CALENDAR} target) builds its key-discovery
 * {@code keysMap} the same lazy way, binding the tracker in the cursor's
 * {@code of()} before {@code reopen()} and freeing at cursor close; the
 * {@code testFastPath*} cases below exercise it under mixed full and partial
 * fetch. On the fast path the pre-aggregated base GROUP BY (already
 * tracker-aware) still charges the dominant key-by-bucket map, so a breach is usually caught
 * there first; wiring {@code keysMap} closes the remaining accounting gap and
 * keeps the per-query counter balanced across the reused cursor's
 * close/reopen cycle.
 * <p>
 * The per-query limit and the GROUP BY allocator's default chunk size are
 * applied per test in {@link #setUp()} via {@code setProperty} so they survive
 * the per-test override reset; the provider reads the limit live on each tracker
 * acquisition. The shrunk chunk size keeps small workloads comfortably under the
 * limit.
 * <p>
 * Parallel GROUP BY is disabled per test via
 * {@code sqlExecutionContext.setParallelGroupByEnabled(false)} so the base scan
 * feeding SAMPLE BY stays on the synchronous path.
 */
public class SampleByFillMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 512 KiB: large enough for the small initial map/allocator allocations
        // to fit comfortably, small enough that a high-cardinality SAMPLE BY FILL
        // breaches after a few map doublings.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
        // Shrink the GROUP BY allocator's default chunk size so allocator-backed
        // function state sits well under the 512 KiB per-query limit on small
        // workloads. Re-read on every factory construction, so @Before suffices.
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
    }

    @Test
    public void testFastPathFillNullFailsOnHighCardinality() throws Exception {
        // Default alignment (ALIGN TO CALENDAR) routes to the GROUP BY fast-path
        // SampleByFillRecordCursorFactory. A high-cardinality key trips the per-query
        // limit while the keyed maps grow during the build pass -- usually at the base
        // GROUP BY's key-by-bucket map, but the fill cursor's keysMap is now
        // charged too. Either way the breach carries the per-query message.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR", SampleByFillRecordCursorFactory.class);
    }

    @Test
    public void testFastPathFillNullSucceedsOnSmall() throws Exception {
        // A single key with a one-bucket gap fits the limit and routes to the fast
        // path; FILL(NULL) synthesizes the missing 01:00 bucket as a null sum.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            // Rows at 00:00 and 02:00 leave bucket 01:00 empty.
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT 7::long AS k, x::double AS v, ((x - 1) * 7_200_000_000L)::timestamp AS ts " +
                            "FROM long_sequence(2)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            final String query = "SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR";
            // Confirm the default-alignment query routes to the GROUP BY fast-path
            // (otherwise this would silently exercise a legacy cursor instead).
            try (RecordCursorFactory factory = select(query)) {
                assertHitsFastPath(factory);
            }
            assertQuery(query)
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("k\tsum\n" +
                            "7\t1.0\n" +
                            "7\tnull\n" +
                            "7\t2.0\n");
        });
    }

    @Test
    public void testKeyedFillLinearFailsOnHighCardinality() throws Exception {
        // FILL(LINEAR) routes to SampleByInterpolateRecordCursorFactory, whose
        // recordKeyMap (distinct keys) and dataMap (key x bucket) grow with the
        // key set. A high-cardinality key trips the per-query limit.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(LINEAR)", SampleByInterpolateRecordCursorFactory.class);
    }

    @Test
    public void testKeyedFillLinearSucceedsOnSmall() throws Exception {
        // A small key set keeps the interpolate maps and allocator within the
        // limit; the query completes and returns one row per (bucket, key).
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            createSmallTable();
            // All rows fall in a single 1h bucket, so FILL(LINEAR) adds no
            // interpolated rows; each key's sum passes through unchanged.
            assertQuery("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(LINEAR) ORDER BY k, sum")
                    .noLeakCheck()
                    .expectSize()
                    .returns("k\tsum\n" +
                            "0\t275.0\n" +
                            "1\t235.0\n" +
                            "2\t245.0\n" +
                            "3\t255.0\n" +
                            "4\t265.0\n");
        });
    }

    @Test
    public void testKeyedFillNoneFailsOnHighCardinality() throws Exception {
        // FILL(NONE) routes to SampleByFillNoneRecordCursor; its OrderedMap grows
        // with the distinct key set discovered during the build pass.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NONE) ALIGN TO FIRST OBSERVATION", SampleByFillNoneRecordCursorFactory.class);
    }

    @Test
    public void testKeyedFillNoneOpenBreachDoesNotLeakReader() throws Exception {
        // of()-time breach (not the hasNext() drain the other tests use): a tiny
        // limit makes of() breach in super.of() after it took the base cursor but
        // before isOpen flips true. close() must still free that base cursor;
        // tests use freeLeakedReaders=false, so a leak shows as a busy reader.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            // Build the table under the @Before limit so DDL and WAL apply don't breach.
            createSmallTable();
            final String query = "SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NONE) ALIGN TO FIRST OBSERVATION";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                assertUsesFactory(factory, SampleByFillNoneRecordCursorFactory.class);
                // Shrink the limit now (read live per open) so of()'s allocator.reopen() breaches.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64L);
                for (int i = 0; i < 5; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected per-query memory breach during cursor open, got cursor: " + cursor);
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
            // Every failed open must have returned its base cursor's reader.
            Assert.assertEquals("busy reader count", 0, engine.getBusyReaderCount());
        });
    }

    @Test
    public void testKeyedFillNullFailsOnHighCardinality() throws Exception {
        // FILL(NULL) routes through SampleByFillValueRecordCursor; the keyed map
        // it builds during the key-discovery pass grows with the key set and
        // trips the per-query limit.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION", SampleByFillNullRecordCursorFactory.class);
    }

    @Test
    public void testKeyedFillNullSucceedsOnSmall() throws Exception {
        // A single key across three consecutive buckets fits the limit and
        // returns the expected per-bucket sums with no fill rows added.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT 7::long AS k, x::double AS v, (x * 3_600_000_000L)::timestamp AS ts " +
                            "FROM long_sequence(3)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            assertQuery("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("k\tsum\n" +
                            "7\t1.0\n" +
                            "7\t2.0\n" +
                            "7\t3.0\n");
        });
    }

    @Test
    public void testKeyedFillPrevFailsOnHighCardinality() throws Exception {
        // FILL(PREV) routes to SampleByFillPrevRecordCursor; its OrderedMap grows
        // with the distinct key set.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION", SampleByFillPrevRecordCursorFactory.class);
    }

    @Test
    public void testKeyedFillValueFailsOnHighCardinality() throws Exception {
        // FILL(constant) routes to SampleByFillValueRecordCursor; same keyed map
        // growth as FILL(NULL).
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(0) ALIGN TO FIRST OBSERVATION", SampleByFillValueRecordCursorFactory.class);
    }

    @Test
    public void testRepeatedFastPathFillRunsReleaseAllocations() throws Exception {
        // The GROUP BY fast-path (default / ALIGN TO CALENDAR) reuses a single fill
        // cursor across getCursor(); its lazy keysMap reopens on open and frees on
        // close. Mixing full and partial fetches is the exact case the per-query
        // wiring had to get right -- a malloc/free asymmetry on the reused-cursor
        // streaming lifecycle would surface here as a residual native allocation.
        // The routing guard inside the helper asserts we are actually on the fast
        // path rather than silently testing a legacy cursor.
        assertRepeatedRunsReleaseAllocations("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO CALENDAR", true);
    }

    @Test
    public void testRepeatedFillCursorRunsReleaseAllocations() throws Exception {
        // Repeat the same keyed FILL(NULL) SAMPLE BY many times to verify that
        // close/reopen cycles release every byte the map and allocator allocate.
        // assertMemoryLeak around the loop is the load-bearing check; a
        // malloc/free asymmetry from the lazy-open wiring would show up as a
        // residual native allocation count at the end of the test.
        assertRepeatedRunsReleaseAllocations("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testRepeatedFillNoneCursorRunsReleaseAllocations() throws Exception {
        // None FILL keeps its own OrderedMap; verify close/reopen balance across
        // mixed full and partial fetches.
        assertRepeatedRunsReleaseAllocations("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NONE) ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testRepeatedFillPrevCursorRunsReleaseAllocations() throws Exception {
        // Prev FILL shares the abstract base's lazy OrderedMap; verify the
        // close/reopen cycle releases it under mixed full and partial fetches.
        assertRepeatedRunsReleaseAllocations("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testRepeatedInterpolateCursorRunsReleaseAllocations() throws Exception {
        // Same close/reopen balance check for the interpolate path (two maps plus
        // the allocator), which reopens all three on each cursor open.
        assertRepeatedRunsReleaseAllocations("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(LINEAR)");
    }

    private void assertBreach(String query, Class<?> expectedFactory) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            // 60_000 distinct keys: the keyed map grows past the 512 KiB limit
            // during the key-discovery build pass.
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT x::long AS k, x::double AS v, (x * 100_000L)::timestamp AS ts " +
                            "FROM long_sequence(60_000)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(query, sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                    // Routing guard: confirm the query exercises the intended FILL
                    // factory rather than silently testing a different cursor.
                    assertUsesFactory(factory, expectedFactory);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                            // drain until breach
                        }
                        Assert.fail("expected per-query memory breach");
                    } catch (CairoException e) {
                        Assert.assertTrue("expected isOutOfMemory(), got: " + e.getFlyweightMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        TestUtils.assertContains(e.getFlyweightMessage(), "workload=QUERY");
                    }
                }
            }
        });
    }

    private void assertHitsFastPath(RecordCursorFactory factory) {
        assertUsesFactory(factory, SampleByFillRecordCursorFactory.class);
    }

    private void assertRepeatedRunsReleaseAllocations(String query) throws Exception {
        assertRepeatedRunsReleaseAllocations(query, false);
    }

    private void assertRepeatedRunsReleaseAllocations(String query, boolean assertFastPath) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            // A handful of keys over a few buckets: comfortably under the limit so
            // every run completes, exercising the open/build/close cycle.
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT (x % 50)::long AS k, x::double AS v, (x * 10_000_000L)::timestamp AS ts " +
                            "FROM long_sequence(2000)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                if (assertFastPath) {
                    assertHitsFastPath(factory);
                }
                for (int i = 0; i < 21; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        switch (i % 3) {
                            case 0:
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                    // full drain
                                }
                                break;
                            case 1:
                                // Partial fetch: open, fetch one row, then close. Exercises the
                                // close-after-partial-build path that a full drain does not.
                                cursor.hasNext();
                                break;
                            default:
                                // Zero fetch: open then close with no hasNext(). Exercises the
                                // reopen-then-close path where the map's initial backing is
                                // allocated by of() but never built up, so the free at close
                                // must still balance the open.
                                break;
                        }
                    }
                }
            }
        });
    }

    private void assertUsesFactory(RecordCursorFactory factory, Class<?> target) {
        // The SAMPLE BY factory sits below an outer SelectedRecordCursorFactory /
        // sort wrap, so walk the base-factory chain rather than checking the top
        // factory's class directly.
        RecordCursorFactory cur = factory;
        while (cur != null) {
            if (target.isInstance(cur)) {
                return;
            }
            RecordCursorFactory next = cur.getBaseFactory();
            if (next == cur) {
                break;
            }
            cur = next;
        }
        Assert.fail("expected " + target.getSimpleName() + " in base chain of " + factory.getClass().getSimpleName());
    }

    private void createSmallTable() throws Exception {
        // 5 keys, 50 rows over ~5 seconds -> a single 1h bucket.
        execute(
                "CREATE TABLE tab AS (" +
                        "SELECT (x % 5)::long AS k, x::double AS v, (x * 100_000L)::timestamp AS ts " +
                        "FROM long_sequence(50)" +
                        ") TIMESTAMP(ts)"
        );
        drainWalQueue();
    }
}
