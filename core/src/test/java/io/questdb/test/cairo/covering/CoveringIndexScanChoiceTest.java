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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.str.StringSink;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The choice, when a query compiles, between a covering-index scan and a scan of the table, from
 * the posting index's per-key counts: a key that selects more than
 * {@code cairo.sql.index.scan.choice.max.selectivity} of the scanned rows takes the scan the
 * {@code no_index} hint gives, and a rare key keeps the covering scan. A covering scan's residual
 * filter and GROUP BY are parallel plans, which reduce the frames on the query's thread when, at
 * open, they hold fewer than {@code cairo.sql.parallel.covering.local.max.rows} rows. Hints that
 * pick the access path are respected, a bind-variable key keeps the covering plan, and every arm
 * returns the rows of the {@code no_index} plan.
 * <p>
 * The table holds 1.2M rows (above one full-scan page frame, under which nothing changes): key F
 * about 20% of them, R about 0.4%.
 */
public class CoveringIndexScanChoiceTest extends AbstractCoveringIndexQueryTest {

    @Override
    public void setUp() {
        super.setUp();
        UnorderedPageFrameSequence.setLocalReduceOpenCountingForTesting(true);
    }

    @Override
    public void tearDown() throws Exception {
        UnorderedPageFrameSequence.setLocalReduceOpenCountingForTesting(false);
        UnorderedPageFrameSequence.setLocalReduceMaxFramesForTesting(-1);
        super.tearDown();
    }

    @Test
    public void testBindVariableKeyKeepsCovering() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            bindVariableService.clear();
            bindVariableService.setStr(0, "F");
            assertPlanContains("select ts, v from t where sym = $1", "CoveringIndex");
            assertSameResult("select ts, v from t where sym = $1", "select /*+ no_index */ ts, v from t where sym = $1");
        });
    }

    @Test
    public void testChoiceDisabledKeepsCovering() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "false");
        assertMemoryLeak(() -> {
            createTable();
            assertPlanContains("select ts, v from t where sym = 'F'", "CoveringIndex");
            assertPlanContains("select count(), sum(v) from t where sym = 'R'", "Async");
        });
    }

    @Test
    public void testFrequentKeyTakesScan() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select ts, v from t where sym = 'F'",
                    "select ts, (v + w) / 2 mid from t where sym = 'F'",
                    "select ts, v from t where sym = 'F' and w > 10",
                    "select avg(v) from t where sym = 'F'",
                    "select timestamp_floor('1h', ts) h, s2, avg(v) from t where sym = 'F' order by h, s2",
                    "select ts, v from t where sym in ('F', 'G')",
            };
            for (String q : queries) {
                assertPlanLacks(q, "CoveringIndex");
                assertSameResult(q, q.replace("select ", "select /*+ no_index */ "));
            }
            // the interval narrows the scan below one full-scan frame (400K rows): the covering plan stays
            final String slice = "select ts, v from t where sym = 'F' and ts in '1970-01-02'";
            assertPlanContains(slice, "CoveringIndex");
            assertSameResult(slice, slice.replace("select ", "select /*+ no_index */ "));
        });
    }

    @Test
    public void testFramesHoldAtMostTheConfiguredRows() throws Exception {
        // a key's rows of a partition spread over frames of cairo.sql.parallel.covering.frame.rows
        // rows, which the workers decode, rather than one frame of up to a page frame's rows
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 1000);
        assertMemoryLeak(() -> {
            createTable();
            final String sql = "select /*+ use_index */ ts, v from t where sym = 'F'";
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertTrue(factory.supportsPageFrameCursor());
                long rows = 0;
                int frames = 0;
                try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                    PageFrame frame;
                    while ((frame = cursor.next()) != null) {
                        final long frameRows = frame.getPartitionHi() - frame.getPartitionLo();
                        Assert.assertTrue(frameRows + " rows", frameRows > 0 && frameRows <= 1000);
                        rows += frameRows;
                        frames++;
                    }
                }
                final StringSink count = new StringSink();
                printSql("select /*+ no_index */ count() from t where sym = 'F'", count);
                TestUtils.assertEquals("count\n" + rows + "\n", count);
                Assert.assertTrue(frames + " frames", frames >= rows / 1000);
            }
            assertSameResult("select /*+ use_index */ count(), sum(v), min(ts), max(ts) from t where sym = 'F'",
                    "select /*+ no_index */ count(), sum(v), min(ts), max(ts) from t where sym = 'F'");
        });
    }

    @Test
    public void testFrequentKeyScanKeepsSubQueryPredicates() throws Exception {
        // A frequent key generates the table query a second time, as a scan. The sub-queries of
        // the residual filter are models whose where clauses the first generation consumed: the
        // scan must still read their keys and intervals.
        assertMemoryLeak(() -> {
            createTable();
            execute("create table t2 (ts timestamp, sym symbol index type posting, v long, w double) timestamp(ts) partition by day");
            execute("insert into t2 select ts, sym, v, w from t");
            execute("create table r (ts timestamp, k symbol, x double) timestamp(ts) partition by day");
            execute("insert into r select (x * 1_000_000L)::timestamp, case when x % 2 = 0 then 'a' else 'b' end, x * 3.0 from long_sequence(100_000)");
            final String[] queries = {
                    "select ts, v from t where sym = 'F' and v > (select avg(v) from t where sym = 'R')",
                    "select count(), sum(v) from t where sym = 'F' and w * 2 < (select max(x) from r where k = 'a' and ts in '1970-01-01')",
                    "select count() from t where sym = 'F' and w > (select max(w) from t2 where sym = 'R' and ts in '1970-01-01')",
                    "select ts, v from t where sym = 'F' and w > (select max(w) from t where sym = 'R' and ts in '1970-01-02') limit -5",
                    "select count() from t where sym = 'F' and v < (select max(v) from t2 where sym = 'G' and v < (select avg(v) from t where sym = 'R'))",
            };
            for (String q : queries) {
                assertPlanLacks(q.substring(0, q.indexOf(" and ")), "CoveringIndex");
                assertSameResult(q, q.replaceFirst("select ", "select /*+ no_index */ "));
            }
        });
    }

    @Test
    public void testFrequentKeyScanKeepsSubQueryLatestOn() throws Exception {
        // A frequent key generates the table query a second time, as a scan. A LATEST ON
        // sub-query of the residual filter is a model whose LATEST BY the first generation
        // consumed: the scan must still read one row per partition of the sub-query, not all of
        // its rows. The expected results are literal: no_index shares the plans that generate a
        // model twice, so it is no reference here.
        assertMemoryLeak(() -> {
            createTable();
            execute("create table t3 as (select * from t) timestamp(ts) partition by day");
            // the latest R is row 29,751 of the rows written after the index; 100 symbols
            final String latestR = "(select v from t3 where sym = 'R' latest on ts partition by sym)";
            final String[][] cases = {
                    {
                            "select count(), sum(v) from t where sym = 'F' and v < " + latestR,
                            "count\tsum\n11900\t177042250\n"
                    },
                    {
                            "select count(), sum(v) from t where sym = 'F' and v < (select count() from (select * from t3 latest on ts partition by sym))",
                            "count\tsum\n38\t1900\n"
                    },
                    {
                            "select ts, v from t where sym = 'F' and v < " + latestR + " limit -3",
                            """
                                    ts\tv
                                    1970-01-04T01:47:03.840000Z\t29740
                                    1970-01-04T01:47:04.920000Z\t29745
                                    1970-01-04T01:47:06.000000Z\t29750
                                    """
                    },
                    {
                            "select count(), sum(v) from t where sym = 'F' and s2 in (select s2 from t3 where sym = 'R' latest on ts partition by sym)",
                            "count\tsum\n82000\t48030615000\n"
                    },
                    {
                            "select count(), sum(v) from t where sym = 'F' and v < (select v from t where sym = 'R' and ts in '1970-01-01' latest on ts partition by sym)",
                            "count\tsum\n85950\t16070221125\n"
                    },
                    // the hour() GROUP BY generates its base again when it cannot vectorise it
                    {
                            "select hour(ts) h, count() c from t3 where v < (select max(v) from (select v from t3 where ts in '1970-01-01' latest on ts partition by sym)) order by h",
                            """
                                    h\tc
                                    0\t33332
                                    1\t30001
                                    2\t16666
                                    3\t16667
                                    4\t16667
                                    5\t16666
                                    6\t16667
                                    7\t16667
                                    8\t16666
                                    9\t16667
                                    10\t16667
                                    11\t16666
                                    12\t16667
                                    13\t16667
                                    14\t16666
                                    15\t16667
                                    16\t16667
                                    17\t16666
                                    18\t16667
                                    19\t16667
                                    20\t16666
                                    21\t16667
                                    22\t16667
                                    23\t16665
                                    """
                    },
                    {
                            "select hour(ts) h, count() c from t3 where v < (select v from t where sym = 'R' and ts in '1970-01-01' order by ts desc limit 1) order by h",
                            """
                                    h\tc
                                    0\t33332
                                    1\t30001
                                    2\t16666
                                    3\t16667
                                    4\t16667
                                    5\t16666
                                    6\t16667
                                    7\t16667
                                    8\t16666
                                    9\t16667
                                    10\t16667
                                    11\t16666
                                    12\t16667
                                    13\t16667
                                    14\t16666
                                    15\t16667
                                    16\t16667
                                    17\t16666
                                    18\t16667
                                    19\t16667
                                    20\t16666
                                    21\t16667
                                    22\t16667
                                    23\t16417
                                    """
                    },
            };
            assertPlanLacks("select count(), sum(v) from t where sym = 'F'", "CoveringIndex");
            for (String[] c : cases) {
                assertResult(c[1], c[0]);
            }
            // and with the choice off, the covering plan
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "false");
            assertPlanContains("select count(), sum(v) from t where sym = 'F'", "CoveringIndex");
            for (String[] c : cases) {
                assertResult(c[1], c[0]);
            }
        });
    }

    @Test
    public void testFrequentKeyScanKeepsLateralOuterSharedFactory() throws Exception {
        // The LATERAL body is t CROSS JOIN the outer GROUP BY, through a wrapper whose delegate
        // the main tree generated, and owns, before the body. The body's frequent key generates t
        // a second time, as a scan: that must not drop the outer factory's shared-factory cache
        // entry, or the wrapper builds a second primary that nothing owns (a native leak at
        // compile) or opens (a NullPointerException in its shared cursor at execution).
        assertMemoryLeak(() -> {
            createTable();
            execute("create table m (ts timestamp, sym symbol, v long) timestamp(ts) partition by day");
            execute("insert into m select (x * 1_000_000L)::timestamp, 'k' || (x % 7), x * 300 from long_sequence(1000)");
            // three outer rows, x = 300, 30300 and 60000
            final String outer = "(SELECT ts, max(v) AS x FROM m WHERE v IN (300, 30300, 60000) GROUP BY ts) o";
            final String[][] cases = {
                    {
                            "SELECT o.ts, o.x, l.c FROM " + outer
                                    + " JOIN LATERAL (SELECT count() c FROM t WHERE t.sym = 'F' AND t.v <= o.x) l ON true ORDER BY o.ts",
                            """
                                    ts\tx\tc
                                    1970-01-01T00:00:01.000000Z\t300\t120
                                    1970-01-01T00:01:41.000000Z\t30300\t12060
                                    1970-01-01T00:03:20.000000Z\t60000\t18000
                                    """
                    },
                    {
                            "SELECT o.ts, o.x, l.c, l.s FROM " + outer
                                    + " JOIN LATERAL (SELECT count() c, sum(v) s FROM t WHERE t.sym = 'F' AND t.v < o.x * 10) l ON true ORDER BY o.ts",
                            """
                                    ts\tx\tc\ts
                                    1970-01-01T00:00:01.000000Z\t300\t1198\t1797000
                                    1970-01-01T00:01:41.000000Z\t30300\t66599\t9270763500
                                    1970-01-01T00:03:20.000000Z\t60000\t125999\t36089715000
                                    """
                    },
                    {
                            "SELECT o.ts, o.x, l.v FROM " + outer
                                    + " JOIN LATERAL (SELECT v FROM t WHERE t.sym = 'F' AND t.v <= o.x ORDER BY ts DESC LIMIT 2) l ON true ORDER BY o.ts, l.v",
                            """
                                    ts\tx\tv
                                    1970-01-01T00:00:01.000000Z\t300\t295
                                    1970-01-01T00:00:01.000000Z\t300\t300
                                    1970-01-01T00:01:41.000000Z\t30300\t29995
                                    1970-01-01T00:01:41.000000Z\t30300\t30000
                                    1970-01-01T00:03:20.000000Z\t60000\t29995
                                    1970-01-01T00:03:20.000000Z\t60000\t30000
                                    """
                    },
            };
            for (String[] c : cases) {
                assertQuery(c[0]).withPlanContaining("(Shared)").inferRandomAccess().inferTimestamp().returns(c[1]);
            }
            // and with the choice off, the covering plan
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "false");
            for (String[] c : cases) {
                assertQuery(c[0]).withPlanContaining("(Shared)", "CoveringIndex").inferRandomAccess().inferTimestamp().returns(c[1]);
            }
        });
    }

    @Test
    public void testFrequentKeyScanKeepsSubQueryWindowJoins() throws Exception {
        // An intermediate WINDOW JOIN rewrites its aggregate model's columns: each aggregate it
        // computes becomes a reference to its alias. The scan's generation of the sub-query must
        // find the aggregates again, not "Invalid column: s1".
        assertMemoryLeak(() -> {
            createTable();
            createWindowJoinTables();
            // max(s1 + s2) is 1,064,000 (the windows include the prevailing row)
            final String sub = "(select max(s1 + s2) from (select m.ts, sum(a.v) s1, sum(b.v) s2 from m"
                    + " window join n a on (m.sym = a.sym) range between 10 seconds preceding and current row"
                    + " window join n b on (m.sym = b.sym) range between 20 seconds preceding and current row))";
            final String[][] cases = {
                    {
                            "select count(), sum(v) from t where sym = 'F' and v < " + sub,
                            "count\tsum\n218799\t113299083000\n"
                    },
                    {
                            "select ts, v from t where sym = 'F' and v < " + sub + " limit -3",
                            """
                                    ts\tv
                                    1970-01-04T01:47:57.840000Z\t29990
                                    1970-01-04T01:47:58.920000Z\t29995
                                    1970-01-04T01:48:00.000000Z\t30000
                                    """
                    },
            };
            for (String[] c : cases) {
                assertQuery(c[0]).withPlanNotContaining("CoveringIndex").inferRandomAccess().inferTimestamp().expectSize().sizeMayVary().returns(c[1]);
            }
            setProperty(PropertyKey.CAIRO_SQL_INDEX_SCAN_CHOICE_ENABLED, "false");
            for (String[] c : cases) {
                assertQuery(c[0]).withPlanContaining("CoveringIndex").inferRandomAccess().inferTimestamp().expectSize().sizeMayVary().returns(c[1]);
            }
        });
    }

    @Test
    public void testExplainCoveringScanInsideScalarSubQuery() throws Exception {
        // a covering scan in a scalar sub-query of a filter: EXPLAIN threw a NullPointerException
        assertMemoryLeak(() -> {
            execute("create table s (ts timestamp, sym symbol index type posting include (v, w), v long, w double) timestamp(ts) partition by day");
            execute("insert into s select (x * 1_000_000L)::timestamp, case when x % 2 = 0 then 'F' else 'G' end, x, x / 7.0 from long_sequence(1_000)");
            // the outer filter prints against its base's metadata, which the sub-query's covering
            // scan must not inherit
            final String sql = "select count(), sum(v) from s where sym = 'F' and w < (select min(w) + 10 from s where sym = 'G')";
            final StringSink plan = new StringSink();
            printSql("explain " + sql, plan);
            TestUtils.assertContains(plan, "CoveringIndex on: sym with: w\n");
            TestUtils.assertContains(plan, "filter: sym='G'");
            assertSameResult(sql, sql.replace("select count()", "select /*+ no_index */ count()"));
        });
    }

    @Test
    public void testHintsPickThePath() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            // use_index and force_use_covering keep the covering scan of a frequent key
            assertPlanContains("select /*+ use_index */ ts, v from t where sym = 'F'", "CoveringIndex");
            assertPlanContains("select /*+ force_use_covering */ ts, v from t where sym = 'F'", "CoveringIndex");
            assertSameResult("select /*+ use_index */ ts, v from t where sym = 'F'", "select /*+ no_index */ ts, v from t where sym = 'F'");
            // no_covering and no_index keep their plans for a rare key
            assertPlanLacks("select /*+ no_covering */ ts, v from t where sym = 'R'", "CoveringIndex");
            assertPlanLacks("select /*+ no_index */ ts, v from t where sym = 'R'", "CoveringIndex");
        });
    }

    @Test
    public void testParallelArmsOnWorkerPool() throws Exception {
        // AbstractCairoTest has no workers, so there "dispatched" means the query thread steals
        // its own tasks. Here four workers reduce the dispatched arm, and the local arm runs
        // with the same pool idle: both return the rows of the no_index plan, and each open
        // takes the arm the threshold names.
        final String[] queries = {
                "select count(), sum(v), max(w), min(ts), max(ts) from t where sym = 'R'",
                "select s2, count(), max(v % 7) m, min(w) from t where sym = 'R' order by s2",
                "select count(), sum(v) from t where sym = 'R' and w > 100",
                "select timestamp_floor('1h', ts) h, s2, avg(v) a from t where sym = 'R' order by h, s2",
                "select ts, v, w from t where sym = 'R' and w > 100",
                "select ts, v from t where sym = 'R' and v % 3 = 0 limit -7",
                "select count(), sum(v) from t where sym = 'R' and ts in '1970-01-02'",
        };
        // a threshold above every selection reduces locally; 1 dispatches every non-empty open
        // (0 would turn the per-open decision off, and with it the counting); the frame cap the
        // switch adds is lifted, see testManyCoveredFramesAreDispatchedDespiteTheRowThreshold()
        UnorderedPageFrameSequence.setLocalReduceMaxFramesForTesting(Integer.MAX_VALUE);
        for (long localMaxRows : new long[]{Long.MAX_VALUE, 1}) {
            final boolean local = localMaxRows > 1;
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_LOCAL_MAX_ROWS, localMaxRows);
            // frames smaller than the selection, so the dispatched arm has tasks for every worker
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 500);
            assertMemoryLeak(() -> {
                final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
                TestUtils.execute(pool, (engine, compiler, ctx) -> {
                    if (engine.getTableTokenIfExists("t") == null) {
                        createTable(engine, ctx);
                    }
                    for (String q : queries) {
                        final StringSink plan = new StringSink();
                        TestUtils.printSql(compiler, ctx, "explain " + q, plan);
                        Assert.assertTrue(q + '\n' + plan, plan.toString().contains("CoveringIndex"));
                        Assert.assertTrue(q + '\n' + plan, plan.toString().contains("Async"));
                        final StringSink expected = new StringSink();
                        TestUtils.printSql(compiler, ctx, q.replaceFirst("select ", "select /*+ no_index */ "), expected);
                        try (RecordCursorFactory factory = compiler.compile(q, ctx).getRecordCursorFactory()) {
                            // one factory, opened three times
                            for (int i = 0; i < 3; i++) {
                                final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                                final StringSink actual = new StringSink();
                                try (RecordCursor cursor = factory.getCursor(ctx)) {
                                    println(factory.getMetadata(), cursor, actual);
                                }
                                final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                                TestUtils.assertEquals(q, expected, actual);
                                Assert.assertTrue(q + (local ? " ran local" : " was dispatched"), after[local ? 0 : 1] > before[local ? 0 : 1]);
                                Assert.assertEquals(q, before[local ? 1 : 0], after[local ? 1 : 0]);
                            }
                        }
                    }
                }, configuration, LOG);
            });
        }
    }

    @Test
    public void testManyCoveredFramesAreDispatchedDespiteTheRowThreshold() throws Exception {
        // A rare key over 2,000 partitions: 6,000 rows, under the local threshold, in 2,000 frames.
        // With the frame dispatch switch on the frames are produced lean (no covering cursor, no
        // boundary postings) and, past the frame cap, dispatched to the workers, which decode them;
        // off, they are produced as before and reduced on the query's thread. The rows are the
        // no_covering plan's either way.
        final String[] queries = {
                "select count(), sum(v), max(d) from m where sym = 'R'",
                "select count(), sum(v) from m where sym = 'R' and d > 1",
                "select ts, v, d from m where sym = 'R' and d > 1",
                "select ts, v from m where sym = 'R' and v % 3 = 0 limit -7",
                "select ts, v from m where sym = 'R' limit 1000, 1010",
                "select count(), sum(v) from m where sym = 'k7'",
                "select count(), sum(v) from m where sym = 'R' and ts in '2020-03'",
        };
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(pool, (engine, compiler, ctx) -> {
                engine.execute("create table m (ts timestamp, sym symbol index type posting include (v, d), v long, d double) timestamp(ts) partition by day bypass wal", ctx);
                engine.execute("insert into m select ('2020-01-01'::timestamp + x * 216_000_000L)::timestamp,"
                        + " case when x % 133 = 0 then 'R' else 'k' || (x % 50) end, x, x / 3.0 from long_sequence(800_000)", ctx);
                // frames of 500 rows, so a frequent key's partition spans chunks
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 500);
                for (String q : queries) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, "false");
                    final StringSink expected = new StringSink();
                    TestUtils.printSql(compiler, ctx, q.replaceFirst("select ", "select /*+ no_covering */ "), expected);
                    for (boolean dispatchOn : new boolean[]{false, true}) {
                        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, String.valueOf(dispatchOn));
                        final StringSink plan = new StringSink();
                        TestUtils.printSql(compiler, ctx, "explain " + q, plan);
                        Assert.assertTrue(q + '\n' + plan, plan.toString().contains("CoveringIndex"));
                        try (RecordCursorFactory factory = compiler.compile(q, ctx).getRecordCursorFactory()) {
                            for (int i = 0; i < 2; i++) {
                                CoveringIndexRecordCursorFactory.resetLeanCoveredFramesForTesting();
                                final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                                final StringSink actual = new StringSink();
                                try (RecordCursor cursor = factory.getCursor(ctx)) {
                                    println(factory.getMetadata(), cursor, actual);
                                }
                                final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                                TestUtils.assertEquals(q + " [dispatch=" + dispatchOn + ']', expected, actual);
                                final long[] lean = CoveringIndexRecordCursorFactory.getLeanCoveredFramesForTesting();
                                if (!plan.toString().contains("Async")) {
                                    // the covering record cursor, which has no page frames
                                    continue;
                                }
                                if (q.contains("limit -")) {
                                    // descending: produced by the traverse, as before
                                    Assert.assertEquals(q, 0, lean[0]);
                                } else {
                                    Assert.assertEquals(q, dispatchOn, lean[0] > 0);
                                    Assert.assertEquals(q, 0, lean[1]);
                                }
                                if (q.contains("sym = 'R'") && !q.contains("ts in")) {
                                    // 2,000 frames: dispatched with the switch on, local with it off
                                    Assert.assertEquals(q + " [dispatch=" + dispatchOn + ']', dispatchOn, after[1] > before[1]);
                                }
                            }
                        }
                    }
                }
            }, configuration, LOG);
        });
    }

    @Test
    public void testParallelArmsWithFrameDispatchOnWorkerPool() throws Exception {
        // Covered Async Filter and Async GROUP BY plans with frame dispatch on (deferred frame
        // addresses, frame groups), reduced locally and dispatched to four workers, return the rows
        // of the no_covering plan with dispatch off. Covered frames are never grouped, though at
        // 500 rows they are small enough to be: a covering page-frame cursor does not defer, and a
        // group takes only uncovered native frames. A no_index scan of the same table, which does
        // defer and group, is the positive control.
        final String[] queries = {
                "select ts, v, w from t where sym = 'R' and w > 100",
                "select ts, v from t where sym = 'R' and v % 3 = 0 limit -7",
                "select count(), sum(v) from t where sym = 'R' and ts in '1970-01-02'",
                "select count(), sum(v), max(w), min(ts), max(ts) from t where sym = 'R'",
                "select s2, count(), max(v % 7) m, min(w) from t where sym = 'R' order by s2",
        };
        final String scan = "select /*+ no_index */ ts, v, w from t where sym = 'R' and w > 100";
        // small scan frames, so that the no_index control forms groups
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 1000);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_FRAME_ROWS, 500);
        final String[] expected = new String[queries.length];
        // {dispatch, local threshold}: the first arm is the reference; a threshold above every
        // selection reduces locally, 1 dispatches every non-empty open (0 would turn the per-open
        // decision off, and with it the counting)
        final Object[][] arms = {{false, Long.MAX_VALUE}, {true, Long.MAX_VALUE}, {true, 1L}, {false, 1L}};
        // the frame cap the switch adds is lifted, see testManyCoveredFramesAreDispatchedDespiteTheRowThreshold()
        UnorderedPageFrameSequence.setLocalReduceMaxFramesForTesting(Integer.MAX_VALUE);
        for (Object[] arm : arms) {
            final boolean dispatchOn = (Boolean) arm[0];
            final long localMaxRows = (Long) arm[1];
            final boolean local = localMaxRows > 1;
            final boolean reference = expected[0] == null;
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FRAME_DISPATCH_ENABLED, String.valueOf(dispatchOn));
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_COVERING_LOCAL_MAX_ROWS, localMaxRows);
            assertMemoryLeak(() -> {
                final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
                TestUtils.execute(pool, (engine, compiler, ctx) -> {
                    if (engine.getTableTokenIfExists("t") == null) {
                        createTable(engine, ctx);
                    }
                    if (reference) {
                        for (int i = 0; i < queries.length; i++) {
                            final String q = queries[i].replaceFirst("select ", "select /*+ no_covering */ ");
                            final StringSink plan = new StringSink();
                            TestUtils.printSql(compiler, ctx, "explain " + q, plan);
                            Assert.assertFalse(q + '\n' + plan, plan.toString().contains("CoveringIndex"));
                            final StringSink sink = new StringSink();
                            TestUtils.printSql(compiler, ctx, q, sink);
                            expected[i] = sink.toString();
                        }
                    }
                    for (int i = 0; i < queries.length; i++) {
                        final String q = queries[i];
                        final StringSink plan = new StringSink();
                        TestUtils.printSql(compiler, ctx, "explain " + q, plan);
                        Assert.assertTrue(q + '\n' + plan, plan.toString().contains("CoveringIndex"));
                        Assert.assertTrue(q + '\n' + plan, plan.toString().contains("Async"));
                        try (RecordCursorFactory factory = compiler.compile(q, ctx).getRecordCursorFactory()) {
                            // one factory, opened three times
                            for (int j = 0; j < 3; j++) {
                                final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                                final long groupsBefore = PageFrameSequence.getGroupsDispatched();
                                final StringSink actual = new StringSink();
                                try (RecordCursor cursor = factory.getCursor(ctx)) {
                                    println(factory.getMetadata(), cursor, actual);
                                }
                                final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                                final String what = q + " [dispatch=" + dispatchOn + ", local=" + local + ']';
                                TestUtils.assertEquals(what, expected[i], actual);
                                Assert.assertTrue(what, after[local ? 0 : 1] > before[local ? 0 : 1]);
                                Assert.assertEquals(what, before[local ? 1 : 0], after[local ? 1 : 0]);
                                Assert.assertEquals(what + ": covered frames grouped", groupsBefore, PageFrameSequence.getGroupsDispatched());
                            }
                        }
                    }
                    // the positive control: an uncovered scan defers its frames and, with dispatch
                    // on, groups them
                    final long groupsBefore = PageFrameSequence.getGroupsDispatched();
                    final StringSink actual = new StringSink();
                    TestUtils.printSql(compiler, ctx, scan, actual);
                    TestUtils.assertEquals(scan, expected[0], actual);
                    Assert.assertEquals(scan + " grouped", dispatchOn, PageFrameSequence.getGroupsDispatched() > groupsBefore);
                }, configuration, LOG);
            });
        }
    }

    @Test
    public void testRareKeyKeepsCoveringAndSmallWorkRunsLocally() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select ts, v from t where sym = 'R'",
                    "select ts, (v + w) / 2 mid from t where sym = 'R'",
                    "select ts, v from t where sym = 'absent'",
            };
            for (String q : queries) {
                assertPlanContains(q, "CoveringIndex");
                assertSameResult(q, q.replace("select ", "select /*+ no_index */ "));
            }
            // the residual filter and the GROUP BY are parallel plans; R's ~4.9K rows are fewer than
            // the local reduce threshold, so each open reduces the frames on the query's thread
            final String[] parallel = {
                    "select ts, v from t where sym = 'R' and w > 10",
                    "select timestamp_floor('1h', ts) h, s2, avg(v) a from t where sym = 'R' order by h, s2",
                    "select count(), sum(v) from t where sym = 'R'",
            };
            for (String q : parallel) {
                assertPlanContains(q, "CoveringIndex");
                assertPlanContains(q, "Async");
                final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                assertSameResult(q, q.replace("select ", "select /*+ no_index */ "));
                final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                Assert.assertTrue(q, after[0] > before[0]);
                Assert.assertEquals(q, before[1], after[1]);
            }
            // a frequent key's covering scan, which use_index keeps, dispatches its frames
            final String frequent = "select /*+ use_index */ count(), sum(v) from t where sym = 'F'";
            assertPlanContains(frequent, "CoveringIndex");
            final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
            assertSameResult(frequent, frequent.replace("use_index", "no_index"));
            Assert.assertTrue(UnorderedPageFrameSequence.getLocalReduceOpensForTesting()[1] > before[1]);
        });
    }

    @Test
    public void testLocalReduceIsDecidedPerOpen() throws Exception {
        // a cached plan compiled while a key has few rows dispatches its frames once the key has
        // many, and the other way round
        assertMemoryLeak(() -> {
            createTable();
            final String[] queries = {
                    "select count(), sum(v), min(ts), max(ts) from t where sym = 'NEW'",
                    "select ts, v from t where sym = 'NEW' and w > 0",
            };
            execute("insert into t select (259_200_000_000L + 30_000 * 216_000L + x * 1_000L)::timestamp, 'NEW', 's1', x, x / 3.0 from long_sequence(1_000)");
            try (RecordCursorFactory grouped = select(queries[0]); RecordCursorFactory filtered = select(queries[1])) {
                final RecordCursorFactory[] factories = {grouped, filtered};
                for (int i = 0; i < factories.length; i++) {
                    final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    assertFactoryResult(factories[i], queries[i]);
                    final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    Assert.assertTrue(queries[i], after[0] > before[0]);
                    Assert.assertEquals(queries[i], before[1], after[1]);
                }
                execute("insert into t select (259_200_000_000L + 31_000 * 216_000L + x * 1_000L)::timestamp, 'NEW', 's1', x, x / 3.0 from long_sequence(300_000)");
                for (int i = 0; i < factories.length; i++) {
                    final long[] before = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    assertFactoryResult(factories[i], queries[i]);
                    final long[] after = UnorderedPageFrameSequence.getLocalReduceOpensForTesting();
                    Assert.assertTrue(queries[i], after[1] > before[1]);
                    Assert.assertEquals(queries[i], before[0], after[0]);
                }
            }
        });
    }

    @Test
    public void testSmallTableKeepsPlans() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table s (ts timestamp, sym symbol index type posting include (v), v long) timestamp(ts) partition by day");
            execute("insert into s select (x * 1_000_000L)::timestamp, case when x % 2 = 0 then 'F' else 'R' end, x from long_sequence(50_000)");
            assertPlanContains("select ts, v from s where sym = 'F'", "CoveringIndex");
            assertPlanContains("select count(), sum(v) from s where sym = 'R'", "Async");
        });
    }

    private void assertFactoryResult(RecordCursorFactory factory, String query) throws Exception {
        final StringSink actual = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            println(factory.getMetadata(), cursor, actual);
        }
        final StringSink expected = new StringSink();
        printSql(query.replaceFirst("select ", "select /*+ no_index */ "), expected);
        TestUtils.assertEquals(query, expected, actual);
    }

    private void assertResult(String expected, String query) throws Exception {
        final StringSink actual = new StringSink();
        printSql(query, actual);
        TestUtils.assertEquals(query, expected, actual);
    }

    private void assertPlanContains(String query, String fragment) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        Assert.assertTrue(query + '\n' + plan, plan.toString().contains(fragment));
    }

    private void assertPlanLacks(String query, String fragment) throws Exception {
        final StringSink plan = new StringSink();
        printSql("explain " + query, plan);
        Assert.assertFalse(query + '\n' + plan, plan.toString().contains(fragment));
    }

    private static void createWindowJoinTables() throws SqlException {
        execute("create table m (ts timestamp, sym symbol, v long) timestamp(ts) partition by day");
        execute("insert into m select (x * 1_000_000L)::timestamp, 'k' || (x % 7), x * 300 from long_sequence(1000)");
        execute("create table n (ts timestamp, sym symbol, v long) timestamp(ts) partition by day");
        execute("insert into n select (x * 1_300_000L)::timestamp, 'k' || (x % 7), x * 200 from long_sequence(1000)");
    }

    private static void createTable(CairoEngine engine, SqlExecutionContext ctx) throws SqlException {
        engine.execute("create table t (ts timestamp, sym symbol, s2 symbol, v long, w double) timestamp(ts) partition by day", ctx);
        // 1.2M rows over 3 days (one 1M-row frame is the floor of the choice)
        engine.execute("insert into t select (x * 216_000L)::timestamp,"
                + " case when x % 5 = 0 then 'F' when x % 250 = 1 then 'R' when x % 5 = 1 then 'G' else 'k' || (x % 97) end,"
                + " 's' || (x % 3), x, x / 7.0 from long_sequence(1_200_000)", ctx);
        engine.execute("alter table t alter column sym add index type posting include (s2, v, w)", ctx);
        // and rows after the index was built
        engine.execute("insert into t select (259_200_000_000L + x * 216_000L)::timestamp,"
                + " case when x % 5 = 0 then 'F' when x % 250 = 1 then 'R' else 'k' || (x % 97) end,"
                + " 's' || (x % 3), x, x / 7.0 from long_sequence(30_000)", ctx);
    }

    private void createTable() throws Exception {
        createTable(engine, sqlExecutionContext);
        final StringSink plan = new StringSink();
        printSql("explain select /*+ use_index */ ts, v from t where sym = 'R'", plan);
        TestUtils.assertContains(plan, "CoveringIndex");
    }
}
