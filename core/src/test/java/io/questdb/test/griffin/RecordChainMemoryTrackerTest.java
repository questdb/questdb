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
 * tracker-aware {@code MemoryCARW} family wired by PR 2.4:
 * {@link io.questdb.cairo.vm.MemoryCARWImpl},
 * {@link io.questdb.cairo.vm.MemoryPARWImpl},
 * {@link io.questdb.cairo.RecordChain},
 * {@link io.questdb.cairo.SingleRecordSink},
 * {@link io.questdb.std.MemoryPages}, and
 * {@link io.questdb.griffin.engine.orderby.RecordTreeChain}.
 * <p>
 * The owning factories bind the active workload's MemoryTracker before the
 * first cursor open. The malloc/free pairs that follow are charged
 * symmetrically to the per-query counter; a runaway operator crosses the
 * limit and fails at the offending allocation site.
 * <p>
 * The per-query limit and the various per-operator page sizes are set in
 * {@link #beforeClass()} because {@code CairoEngine#getMemoryTrackerProvider}
 * caches the {@code PerQueryMemoryTrackerProvider} on first access with the
 * limit then in effect. Tuning the page sizes down keeps initial allocations
 * comfortably under the 512 KiB limit; small workloads then fit easily and
 * runaway workloads breach after a few heap doublings.
 */
public class RecordChainMemoryTrackerTest extends AbstractCairoTest {

    @BeforeClass
    public static void beforeClass() {
        // 512 KiB: large enough for the small initial heaps of each operator
        // wired by PR 2.4 to fit comfortably, small enough that a runaway
        // operator breaches after a few heap doublings. The limit is read
        // lazily on the {@code PerQueryMemoryTrackerProvider}'s first access
        // and cached for the engine's lifetime, so {@code @BeforeClass} is
        // the right hook for it.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 512 * 1024L);
    }

    @Before
    public void setUp() {
        super.setUp();
        // Shrink the per-operator page sizes so the initial allocations from
        // RecordChain / RecordTreeChain / SingleRecordSink / MemoryPages /
        // MemoryCARW (window store) sit well under the 512 KiB per-query
        // limit and the breach tests can grow the heap by doubling. The
        // defaults are large (16 MiB sort/hash-join value, 128 KiB sort key,
        // 1 MiB window store) and would either consume the budget on the
        // first allocation or require unrealistic workload sizes to trigger
        // growth.
        // <p>
        // The page sizes are re-read on every {@code overrides.getConfiguration}
        // call (the wrapper rebuilds the underlying {@code PropServerConfiguration}
        // when properties change), so setting them in {@code @Before} is
        // sufficient. {@code AbstractCairoTest.tearDown} calls
        // {@code overrides.reset()} which clears properties between tests, so
        // a static initializer would only land for the first test.
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_HASH_JOIN_VALUE_PAGE_SIZE, 4 * 1024L);
        setProperty(PropertyKey.CAIRO_SQL_WINDOW_STORE_PAGE_SIZE, 4 * 1024L);
    }

    @Test
    public void testAsOfJoinFailsOnLargeKey() throws Exception {
        // AsOfJoinFastRecordCursorFactory owns master/slave SingleRecordSink
        // pairs. The sinks grow to fit the LARGEST key seen during the join
        // (clear() resets the append pointer but does not shrink the heap).
        // A single varchar key wider than half the per-query limit forces the
        // pair of sinks past the limit on the final doubling realloc.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (k varchar, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (k varchar, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            // 400_000 byte varchar key per row. Each SingleRecordSink doubles
            // to ~512 KiB to fit; with both master and slave sinks growing in
            // lockstep, the pair crosses the per-query limit before the join
            // finishes.
            execute(
                    "INSERT INTO master SELECT rpad('k', 400_000, 'x'), cast(x * 1000 AS TIMESTAMP) FROM long_sequence(4)"
            );
            execute(
                    "INSERT INTO slave SELECT rpad('k', 400_000, 'x'), x, cast(x * 1000 AS TIMESTAMP) FROM long_sequence(4)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT master.k, slave.v FROM master ASOF JOIN slave ON k",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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

    @Test
    public void testAsOfJoinSucceedsOnSmallKey() throws Exception {
        // Same shape with short keys fits the per-query limit; the
        // SingleRecordSink malloc/free pairs hold the counter in balance and
        // the join returns the expected matches.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (k varchar, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (k varchar, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute(
                    "INSERT INTO master SELECT cast(x AS varchar), cast(x * 1000 AS TIMESTAMP) FROM long_sequence(3)"
            );
            execute(
                    "INSERT INTO slave SELECT cast(x AS varchar), x, cast(x * 1000 AS TIMESTAMP) FROM long_sequence(3)"
            );
            drainWalQueue();
            assertSql(
                    "k\tv\n" +
                            "1\t1\n" +
                            "2\t2\n" +
                            "3\t3\n",
                    "SELECT master.k, slave.v FROM master ASOF JOIN slave ON k"
            );
        });
    }

    @Test
    public void testCumeDistFailsOnLargeInput() throws Exception {
        // cume_dist() over (order by k) with a single distinct value buffers
        // every row offset into the function's deferredOffsets MemoryARW
        // (one peer group spans the whole result set). The buffer grows
        // monotonically and breaches the per-query limit at the offending
        // append-page allocation.
        assertMemoryLeak(() -> {
            // Single distinct value for k so all rows form one peer group.
            execute("CREATE TABLE tab AS (SELECT 1::long AS k, x AS v FROM long_sequence(200_000))");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT v, cume_dist() OVER (ORDER BY k) AS cd FROM tab",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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

    @Test
    public void testCumeDistSucceedsOnSmallInput() throws Exception {
        // Small input fits the limit; cume_dist returns the expected
        // cumulative distribution values.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab AS (SELECT x AS k FROM long_sequence(4))");
            drainWalQueue();
            assertSql(
                    "k\tcd\n" +
                            "1\t0.25\n" +
                            "2\t0.5\n" +
                            "3\t0.75\n" +
                            "4\t1.0\n",
                    "SELECT k, cume_dist() OVER (ORDER BY k) AS cd FROM tab ORDER BY k"
            );
        });
    }

    @Test
    public void testFullHashJoinFailsOnLargeBuildSide() throws Exception {
        // Force the full HashJoinRecordCursorFactory (not the Light variant
        // selected by default for random-access slaves) via setFullFatJoins.
        // The slave RecordChain stores entire rows so a moderately sized
        // slave with wide varchar columns breaches the per-query limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (k LONG, v LONG)");
            execute("CREATE TABLE slave (k LONG, payload varchar)");
            execute("INSERT INTO master SELECT x, x FROM long_sequence(1_000)");
            execute("INSERT INTO slave SELECT x, rpad(cast(x AS varchar), 1024, 'p') FROM long_sequence(2_000)");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                final CompiledQuery cq = compiler.compile(
                        "SELECT master.k, slave.payload FROM master JOIN slave ON k",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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

    @Test
    public void testHashOuterJoinFailsOnLargeBuildSide() throws Exception {
        // Force HashOuterJoinRecordCursorFactory (full-fat) for a LEFT JOIN.
        // The slave RecordChain stores entire rows; a moderately sized slave
        // with wide varchar columns breaches the per-query limit.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (k LONG)");
            execute("CREATE TABLE slave (k LONG, payload varchar)");
            execute("INSERT INTO master SELECT x FROM long_sequence(500)");
            execute("INSERT INTO slave SELECT x, rpad(cast(x AS varchar), 1024, 'p') FROM long_sequence(2_000)");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                final CompiledQuery cq = compiler.compile(
                        "SELECT master.k, slave.payload FROM master LEFT JOIN slave ON k",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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

    @Test
    public void testRepeatedHashJoinCursorRunsReleaseAllocations() throws Exception {
        // Many close/reopen cycles through HashJoinRecordCursorFactory must
        // release every byte they allocated -- both the join map (PR 2.2
        // wiring) and the slave RecordChain (PR 2.4 wiring). The load-bearing
        // check is assertMemoryLeak: any malloc/free asymmetry would show up
        // as a residual allocation count.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (k LONG, v LONG)");
            execute("CREATE TABLE slave (k LONG, w LONG)");
            execute("INSERT INTO master SELECT x, x FROM long_sequence(50)");
            execute("INSERT INTO slave SELECT x, x * 10 FROM long_sequence(50)");
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                compiler.setFullFatJoins(true);
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT master.k, slave.w FROM master JOIN slave ON k",
                        sqlExecutionContext
                ).getRecordCursorFactory()) {
                    for (int i = 0; i < 20; i++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            long rows = 0;
                            while (cursor.hasNext()) {
                                rows++;
                            }
                            Assert.assertEquals(50, rows);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testSampleByFullSortFailsOnLargeInput() throws Exception {
        // SAMPLE BY ... FILL(PREV) with the full_recordchain strategy
        // forces codegen to construct a SortedRecordCursorFactory backed by
        // a RecordTreeChain (MemoryPages key heap + RecordChain value chain).
        // A large input grows both children past the per-query limit; the
        // breach fires at either the MemoryPages allocate0 (key heap) or the
        // RecordChain inner MemoryCARW realloc (value chain).
        setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "full_recordchain");
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (ts TIMESTAMP, k SYMBOL, x DOUBLE) " +
                            "TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO t " +
                            "SELECT cast(x * 1_000_000 AS TIMESTAMP), rnd_symbol(2000, 6, 12, 0), x::double " +
                            "FROM long_sequence(80_000)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(
                        "SELECT ts, k, sum(x) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                        sqlExecutionContext
                );
                try (RecordCursorFactory factory = cq.getRecordCursorFactory();
                     RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
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

    @Test
    public void testSampleByFullSortSucceedsOnSmallInput() throws Exception {
        // Same SAMPLE BY full-sort path on a small input fits the per-query
        // limit; the RecordTreeChain's children stay well below it. We
        // don't assert exact contents -- the goal is malloc/free symmetry
        // through the lazy MemoryPages / RecordChain pair, which
        // assertMemoryLeak verifies.
        setProperty(PropertyKey.CAIRO_SQL_SAMPLEBY_FILL_SORT_STRATEGY, "full_recordchain");
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE t (ts TIMESTAMP, k SYMBOL, x DOUBLE) " +
                            "TIMESTAMP(ts) PARTITION BY DAY"
            );
            execute(
                    "INSERT INTO t VALUES " +
                            "('2024-01-01T00:00:00.000000Z', 'A', 1.0), " +
                            "('2024-01-01T01:00:00.000000Z', 'A', 2.0), " +
                            "('2024-01-01T02:00:00.000000Z', 'B', 3.0)"
            );
            drainWalQueue();
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory factory = compiler.compile(
                         "SELECT ts, k, sum(x) FROM t SAMPLE BY 1h FILL(PREV) ALIGN TO CALENDAR",
                         sqlExecutionContext
                 ).getRecordCursorFactory()) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    long rows = 0;
                    while (cursor.hasNext()) {
                        rows++;
                    }
                    Assert.assertTrue("expected some result rows, got " + rows, rows > 0);
                }
            }
        });
    }
}
