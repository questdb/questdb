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
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SQL-level tests that exercise the per-query memory limit through the
 * tracker-aware keyed SAMPLE BY FILL operators wired by Phase 3:
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
 * also stays global-only: its cursor instance is reused across getCursor() with a
 * lifecycle that does not fit the lazy bind-on-open pattern, and its pre-aggregated
 * base GROUP BY (wired by PR 2.5) already charges the dominant key-by-bucket map.
 * <p>
 * The per-query limit is set in {@link #beforeClass()} because
 * {@code CairoEngine#getMemoryTrackerProvider} caches the
 * {@code PerQueryMemoryTrackerProvider} on first access with the limit then in
 * effect; per-test overrides via {@code setProperty} land too late on a shared
 * engine. The GROUP BY allocator's default chunk size is shrunk in
 * {@link #setUp()} so small workloads fit comfortably under the limit; it is
 * re-read on every factory construction, so {@code @Before} is sufficient.
 * <p>
 * Parallel GROUP BY is disabled per test via
 * {@code sqlExecutionContext.setParallelGroupByEnabled(false)} so the base scan
 * feeding SAMPLE BY stays on the synchronous path.
 */
public class SampleByFillMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void beforeClass() {
        // 512 KiB: large enough for the small initial map/allocator allocations
        // to fit comfortably, small enough that a high-cardinality SAMPLE BY FILL
        // breaches after a few map doublings. The limit is read lazily on the
        // PerQueryMemoryTrackerProvider's first access and cached for the
        // engine's lifetime, so @BeforeClass is the right hook for it.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
    }

    @Before
    public void setUp() {
        super.setUp();
        // Shrink the GROUP BY allocator's default chunk size so allocator-backed
        // function state sits well under the 512 KiB per-query limit on small
        // workloads. Re-read on every factory construction, so @Before suffices.
        setProperty(PropertyKey.CAIRO_SQL_GROUPBY_ALLOCATOR_DEFAULT_CHUNK_SIZE, 4 * 1024L);
    }

    @Test
    public void testKeyedFillLinearFailsOnHighCardinality() throws Exception {
        // FILL(LINEAR) routes to SampleByInterpolateRecordCursorFactory, whose
        // recordKeyMap (distinct keys) and dataMap (key x bucket) grow with the
        // key set. A high-cardinality key trips the per-query limit.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(LINEAR)");
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
            assertSql(
                    "k\tsum\n" +
                            "0\t275.0\n" +
                            "1\t235.0\n" +
                            "2\t245.0\n" +
                            "3\t255.0\n" +
                            "4\t265.0\n",
                    "SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(LINEAR) ORDER BY k, sum"
            );
        });
    }

    @Test
    public void testKeyedFillNoneFailsOnHighCardinality() throws Exception {
        // FILL(NONE) routes to SampleByFillNoneRecordCursor; its OrderedMap grows
        // with the distinct key set discovered during the build pass.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NONE) ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testKeyedFillNullFailsOnHighCardinality() throws Exception {
        // FILL(NULL) routes through SampleByFillValueRecordCursor; the keyed map
        // it builds during the key-discovery pass grows with the key set and
        // trips the per-query limit.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testKeyedFillNullSucceedsOnSmall() throws Exception {
        // A single key across three consecutive buckets fits the limit and
        // returns the expected per-bucket sums with no fill rows added.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT 7::long AS k, x::double AS v, (x * 3600000000L)::timestamp AS ts " +
                            "FROM long_sequence(3)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            assertSql(
                    "k\tsum\n" +
                            "7\t1.0\n" +
                            "7\t2.0\n" +
                            "7\t3.0\n",
                    "SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(NULL) ALIGN TO FIRST OBSERVATION"
            );
        });
    }

    @Test
    public void testKeyedFillPrevFailsOnHighCardinality() throws Exception {
        // FILL(PREV) routes to SampleByFillPrevRecordCursor; its OrderedMap grows
        // with the distinct key set.
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(PREV) ALIGN TO FIRST OBSERVATION");
    }

    @Test
    public void testKeyedFillValueFailsOnHighCardinality() throws Exception {
        // FILL(constant) routes to SampleByFillValueRecordCursor; same keyed map
        // growth as FILL(NULL).
        assertBreach("SELECT k, sum(v) FROM tab SAMPLE BY 1h FILL(0) ALIGN TO FIRST OBSERVATION");
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

    private void assertBreach(String query) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            // 60_000 distinct keys: the keyed map grows past the 512 KiB limit
            // during the key-discovery build pass.
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT x::long AS k, x::double AS v, (x * 100000L)::timestamp AS ts " +
                            "FROM long_sequence(60_000)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(query, sqlExecutionContext);
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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
        });
    }

    private void assertRepeatedRunsReleaseAllocations(String query) throws Exception {
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelGroupByEnabled(false);
            // A handful of keys over a few buckets: comfortably under the limit so
            // every run completes, exercising the open/build/close cycle.
            execute(
                    "CREATE TABLE tab AS (" +
                            "SELECT (x % 50)::long AS k, x::double AS v, (x * 10000000L)::timestamp AS ts " +
                            "FROM long_sequence(2000)" +
                            ") TIMESTAMP(ts)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory()) {
                for (int i = 0; i < 20; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        if ((i & 1) == 0) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                                // full drain
                            }
                        } else {
                            // Partial fetch: open, fetch one row, then close. Exercises the
                            // close-after-partial-build path that a full drain does not.
                            cursor.hasNext();
                        }
                    }
                }
            }
        });
    }

    private void createSmallTable() throws Exception {
        // 5 keys, 50 rows over ~5 seconds -> a single 1h bucket.
        execute(
                "CREATE TABLE tab AS (" +
                        "SELECT (x % 5)::long AS k, x::double AS v, (x * 100000L)::timestamp AS ts " +
                        "FROM long_sequence(50)" +
                        ") TIMESTAMP(ts)"
        );
        drainWalQueue();
    }
}
