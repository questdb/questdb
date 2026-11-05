/*******************************************************************************
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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.AdaptiveSymbolPatternRecordCursorFactory;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolPatternIndexRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Proves that native memory allocated by the parallel covered-index decode path is
 * charged to (and therefore capped by) the per-query memory tracker, exactly like the
 * parquet decode path, and released as the scan moves on, as the parquet path releases
 * a decoded row group: the limit counts the covered frames a query holds, not every
 * frame it has decoded.
 * <p>
 * A low-selectivity {@code sym = 'A'} materializes a wide covered VARCHAR for every
 * matched row. Decoded in one frame, those rows breach a per-query limit that comfortably
 * fits the group-by map, address cache and filter scratch; decoded in many small frames,
 * whose sum is well above the same limit, they do not, as each frame is freed once its
 * reducer is done with it. A narrow {@code sym = 'RARE'} control proves the covered path
 * runs fine under the same limit when its decode footprint is small - i.e. the breach is
 * caused specifically by the covered decode growing past the ceiling, not by unrelated
 * machinery.
 */
public class CoveringIndexMemoryLimitTest extends AbstractCairoTest {

    private static final long QUERY_MEMORY_LIMIT = 8 * 1024 * 1024L;
    private static final String RARE_QUERY = "SELECT grp, sum(payload) FROM t WHERE sym = 'RARE' GROUP BY grp ORDER BY grp";
    // plain Java over the inserted rows: the RARE rows are x = 10,000, 20,000, ... 100,000, in group
    // x % 7, with a payload of x
    private static final String RARE_RESULT = rareResult();

    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        // Many small page frames so the covered scan fans out across the worker pool.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000);
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, QUERY_MEMORY_LIMIT);
        super.setUp();
    }

    @Test
    public void testCoveredDecodeIsCappedByPerQueryLimit() throws Exception {
        // Frames as large as the covering scan cuts them, 65,536 rows, which one partition fills:
        // a single frame's covered VARCHAR, 65,536 x 200 bytes, exceeds the 8 MiB limit.
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1_000_000);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTable(engine, sqlExecutionContext, "t", true);
                        // 100k rows, 100 seconds apart in time, so all of them in one partition.
                        // Nearly all are sym='A' with a wide (200-byte) covered VARCHAR; a
                        // sprinkling of sym='RARE' rows gives a tiny covered set for the control.
                        insertRows(engine, sqlExecutionContext, "t", 1_000L);

                        final StringSink sink = new StringSink();
                        // Control: the covered decode path runs fine under the same limit when the
                        // matched (covered) set is tiny. Proves the query is otherwise valid and the
                        // limit is not tripped by unrelated machinery.
                        TestUtils.printSql(engine, sqlExecutionContext, RARE_QUERY, sink);
                        TestUtils.assertEquals(RARE_RESULT, sink);

                        // Breach: decoding one covered frame must push the per-query tracker past
                        // its ceiling. Were covered buffers not charged to the tracker, this query
                        // would complete on global-only accounting and no breach would be raised.
                        try {
                            TestUtils.printSql(engine, sqlExecutionContext, "SELECT grp, sum(payload) FROM t WHERE sym = 'A' GROUP BY grp ORDER BY grp", sink);
                            Assert.fail("expected a per-query memory breach from covered decode");
                        } catch (CairoException e) {
                            Assert.assertTrue(
                                    "expected isOutOfMemory(), got: " + e.getFlyweightMessage(),
                                    e.isOutOfMemory()
                            );
                            TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        }

                        // the failed decode left nothing charged and nothing behind: the control
                        // query runs again under the same limit
                        TestUtils.printSql(engine, sqlExecutionContext, RARE_QUERY, sink);
                        TestUtils.assertEquals(RARE_RESULT, sink);
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testCoveredScanLargerThanPerQueryLimitCompletes() throws Exception {
        // 1,000-row page frames, of which a covered frame holds about 230 KB; the 100k matched rows
        // decode about 23 MB in all, well above the 8 MiB limit. Each frame is freed once its
        // reducer is done with it, so the query holds a few frames at a time and completes. A pool
        // that kept every frame it decoded until the query ended would breach the limit.
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        createTable(engine, sqlExecutionContext, "t", true);
                        createTable(engine, sqlExecutionContext, "ref", false);
                        // ~100k rows over many day-partitions, 10 minutes apart in time
                        insertRows(engine, sqlExecutionContext, "t", 600_000_000L);
                        insertRows(engine, sqlExecutionContext, "ref", 600_000_000L);

                        // Async GROUP BY: the workers' pools free each frame after aggregating it,
                        // and first() and last() copy the covered VARCHAR, which is not stable.
                        TestUtils.assertSqlCursors(
                                compiler,
                                sqlExecutionContext,
                                "SELECT grp, sum(payload), first(grp), last(grp), count() FROM ref WHERE sym = 'A' GROUP BY grp ORDER BY grp",
                                "SELECT grp, sum(payload), first(grp), last(grp), count() FROM t WHERE sym = 'A' GROUP BY grp ORDER BY grp",
                                LOG
                        );
                        // Async Filter: a reduce-queue slot frees its frame once the cursor has
                        // collected it.
                        TestUtils.assertSqlCursors(
                                compiler,
                                sqlExecutionContext,
                                "SELECT ts, grp, payload FROM ref WHERE sym = 'A' AND payload > 1",
                                "SELECT ts, grp, payload FROM t WHERE sym = 'A' AND payload > 1",
                                LOG
                        );
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testEagerPatternCoveringFramesAreCappedByPerQueryLimit() throws Exception {
        assertMemoryLeak(() -> {
            final WorkerPool pool = new WorkerPool(() -> 4);
            TestUtils.execute(
                    pool,
                    (engine, compiler, sqlExecutionContext) -> {
                        engine.execute(
                                "CREATE TABLE pattern_t (" +
                                        "  ts TIMESTAMP," +
                                        "  sym SYMBOL INDEX TYPE POSTING INCLUDE (payload)," +
                                        "  payload VARCHAR" +
                                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                                sqlExecutionContext
                        );
                        // Four matching symbols cover exactly 2% of the rows, keeping the adaptive
                        // route inside its 1/50 admission boundary. Only matching rows carry a wide
                        // value, so fixture creation stays small while eager merged frames retain
                        // more than the 8 MiB query limit.
                        engine.execute(
                                "INSERT INTO pattern_t SELECT" +
                                        "  (x * 1000L)::timestamp," +
                                        "  CASE WHEN x % 50 = 0" +
                                        "    THEN 'match' || ((x / 50) % 4)" +
                                        "    ELSE 'other'" +
                                        "  END," +
                                        "  CASE WHEN x % 50 = 0" +
                                        "    THEN rpad('x'::varchar, 1000, 'x')" +
                                        "    ELSE 'x'::varchar" +
                                        "  END" +
                                        " FROM long_sequence(500_000)",
                                sqlExecutionContext
                        );
                        engine.releaseAllWriters();

                        AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                        CoveringIndexRecordCursorFactory.resetCoveredRowsWrittenForTesting();
                        SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled = true;
                        try {
                            final StringSink sink = new StringSink();
                            CairoException failure = null;
                            try {
                                TestUtils.printSql(
                                        engine,
                                        sqlExecutionContext,
                                        "SELECT sym, sum(length(payload))" +
                                                " FROM pattern_t" +
                                                " WHERE sym LIKE 'match%'" +
                                                " GROUP BY sym ORDER BY sym",
                                        sink
                                );
                            } catch (CairoException e) {
                                failure = e;
                            }
                            Assert.assertEquals(1, AdaptiveSymbolPatternRecordCursorFactory.testCoveringInvocations.get());
                            Assert.assertEquals(0, AdaptiveSymbolPatternRecordCursorFactory.testScanInvocations.get());
                            Assert.assertTrue(
                                    "the eager multi-key covering merge must materialize rows before the breach",
                                    CoveringIndexRecordCursorFactory.getCoveredRowsWrittenForTesting() > 0
                            );
                            if (failure == null) {
                                Assert.fail("expected eager pattern covering frames to breach the per-query memory limit");
                            }
                            Assert.assertTrue(
                                    "expected isOutOfMemory(), got: " + failure.getFlyweightMessage(),
                                    failure.isOutOfMemory()
                            );
                            TestUtils.assertContains(failure.getFlyweightMessage(), "query memory limit exceeded");
                            TestUtils.assertContains(failure.getFlyweightMessage(), "workload=QUERY");
                        } finally {
                            SymbolPatternIndexRecordCursorFactory.isRouteCounterEnabled = false;
                            AdaptiveSymbolPatternRecordCursorFactory.resetTestCounters();
                            CoveringIndexRecordCursorFactory.resetCoveredRowsWrittenForTesting();
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    private static String rareResult() {
        final long[] sums = new long[7];
        for (long x = 10_000; x <= 100_000; x += 10_000) {
            sums[(int) (x % 7)] += x;
        }
        final StringBuilder sb = new StringBuilder("grp\tsum\n");
        for (int g = 0; g < 7; g++) {
            sb.append('g').append(g).append("x".repeat(198)).append('\t').append(sums[g]).append('\n');
        }
        return sb.toString();
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String name, boolean isCovering) throws SqlException {
        engine.execute(
                "CREATE TABLE " + name + " (" +
                        "  ts TIMESTAMP," +
                        (isCovering ? "  sym SYMBOL INDEX TYPE POSTING INCLUDE (grp, payload)," : "  sym SYMBOL,") +
                        "  grp VARCHAR," +
                        "  payload LONG" +
                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL",
                sqlExecutionContext
        );
    }

    private static void insertRows(CairoEngine engine, SqlExecutionContext sqlExecutionContext, String name, long timestampStepMicros) throws SqlException {
        engine.execute(
                "INSERT INTO " + name + " SELECT" +
                        "  (x * " + timestampStepMicros + "L)::timestamp," +
                        "  CASE WHEN x % 10000 = 0 THEN 'RARE' ELSE 'A' END," +
                        "  rpad(('g' || (x % 7))::varchar, 200, 'x')," +
                        "  x" +
                        " FROM long_sequence(100_000)",
                sqlExecutionContext
        );
        engine.releaseAllWriters();
    }
}
