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
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.constants.BooleanConstant;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_DESC;
import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The runtime input swap of the parallel hash join's first join. The serial hash join hashes its
 * left input and streams its right one when no parent fixed its order and both inputs report a
 * size, the left one the smaller; the rows then come in the right input's order, each with its left
 * matches newest first. The parallel join swaps by the same rule, in every execution, from the sizes
 * that the inputs' record cursors would report, so every query here compares with the ordinary
 * plan, in order, and checks which orientation the execution took. The tables hold duplicate keys
 * on both sides and NULL keys, so that the two orientations order their rows differently.
 */
public class AsyncHashJoinSwapTest extends AbstractCairoTest {
    private static final String INPUT_SWAP = "inputSwap: when the left input is smaller";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        // read_parquet() reads files under the input root.
        inputRoot = root;
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBackwardLeftScanWithUndeterminedOrder() throws Exception {
        assertMemoryLeak(() -> {
            // l's keys alternate, so a frame of three rows holds a key twice, in an order that the
            // build's row direction decides.
            createTables(9, 30, 2);
            // ORDER BY the left input's timestamp descending runs it backward and fixes the join's
            // order through the parent's question, so no SQL streams a backward left input whose
            // order is still open. The test plans the join itself, over a left input that scans l
            // backward, and nothing asks it about its order. Swapped, it hashes l in that order,
            // which chains each key's rows from the oldest, as the ordinary join over the sorted
            // sub-query does; l's timestamps are unique, so the sort yields the backward scan's order.
            final String expectedSwapped = rows(ordinary(
                    "SELECT l.k, l.v, l.s, l.str, l.vc, l.ts, r.w, r.rvc, r.k, r.s, r.rts FROM r JOIN (SELECT * FROM l ORDER BY ts DESC) l ON r.k = l.k",
                    sqlExecutionContext
            ));
            // Once a parent fixed the order, the join streams l backward.
            final String expectedStreamed = rows(ordinary(
                    "SELECT * FROM (SELECT * FROM l ORDER BY ts DESC) l JOIN r ON l.k = r.k",
                    sqlExecutionContext
            ));
            Assert.assertNotEquals(expectedSwapped, expectedStreamed);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (boolean hasPostFilter : new boolean[]{false, true}) {
                    try (
                            RecordCursorFactory left = select("SELECT * FROM l ORDER BY ts DESC");
                            RecordCursorFactory right = select("SELECT * FROM r");
                            AsyncHashJoinRecordCursorFactory factory = newSwapCandidate(left, right, hasPostFilter ? BooleanConstant.TRUE : null)
                    ) {
                        final String plan = plan(factory, sqlExecutionContext);
                        TestUtils.assertContains(plan, "Frame backward scan on: l");
                        TestUtils.assertContains(plan, INPUT_SWAP);
                        Assert.assertEquals(expectedSwapped, rows(result(factory, sqlExecutionContext)));
                        final HashJoinBuildStage stage = factory.getAtom().getStage(0);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertTrue(factory.getAtom().isSwapped());
                            Assert.assertSame(stage, factory.getAtom().getStage(0));
                            Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                            Assert.assertEquals(minRows == 0, stage.getPartitionCount() > 1);
                        }
                        // The left input followed the advice to scan backward, so a parent that asks
                        // fixes the order and the next execution streams l, unless a post-join filter
                        // answers for the join, as the serial plan's filter does, without asking it.
                        Assert.assertEquals(!hasPostFilter, factory.followedOrderByAdvice());
                        Assert.assertEquals(hasPostFilter, plan(factory, sqlExecutionContext).contains(INPUT_SWAP));
                        Assert.assertEquals(hasPostFilter ? expectedSwapped : expectedStreamed, rows(result(factory, sqlExecutionContext)));
                        Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_BACKWARD, factory.getScanDirection());
                        Assert.assertFalse(plan(factory, sqlExecutionContext).contains(INPUT_SWAP));
                        Assert.assertEquals(expectedStreamed, rows(result(factory, sqlExecutionContext)));
                    }
                }
            }
        });
    }

    @Test
    public void testCachedFactoryFlipsOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 12, 3);
            final String sql = "SELECT * FROM l JOIN r ON l.k = r.k";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Each execution compares the sizes of the scans it opens.
                assertExecution(factory, join, sql, true);
                execute("INSERT INTO l SELECT (x % 3)::INT, 100 + x, ('s' || (x % 3))::SYMBOL, 'str' || x, 'vc' || x, timestamp_sequence('2024-01-01T01', 60_000_000) FROM long_sequence(10)");
                assertExecution(factory, join, sql, false);
                execute("INSERT INTO r SELECT 200 + x, 'rv' || x, (x % 4)::INT, ('s' || (x % 4))::SYMBOL, timestamp_sequence('2024-01-01T02', 60_000_000) FROM long_sequence(10)");
                assertExecution(factory, join, sql, true);
                // An empty left input reports a size of zero, which the rule does not swap by.
                execute("TRUNCATE TABLE l");
                assertExecution(factory, join, sql, false);
                Assert.assertEquals("", rows(result(factory, sqlExecutionContext)));
            }
        });
    }

    @Test
    public void testCancellationAndMemoryLimitInSwappedBuild() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 12, 3);
            final String sql = "SELECT * FROM l JOIN r ON l.k = r.k";
            final String expected = ordinary(sql, sqlExecutionContext);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // A limit of four bytes admits the open cursor and fails the build of the left input.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 4L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(join.getAtom().isSwapped());
                    cursor.hasNext();
                    Assert.fail("expected the build to breach the memory limit");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isOutOfMemory());
                    TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                }
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                // Cancelled before the build of the left input.
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(join.getAtom().isSwapped());
                    breaker.cancel();
                    cursor.hasNext();
                    Assert.fail("expected cancellation");
                } catch (CairoException e) {
                    Assert.assertTrue(e.isCancellation());
                }
                breaker.reset();
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
            // Either input of a swapped join fails to open once its table is gone, and the scan that
            // opened first closes with the cursor.
            for (String table : new String[]{"r", "l"}) {
                try (RecordCursorFactory factory = select(sql)) {
                    execute("DROP TABLE " + table);
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected the scan of " + table + " to fail");
                    } catch (CairoException | SqlException e) {
                        TestUtils.assertContains(e.getMessage(), table);
                    }
                    Assert.assertEquals(0, engine.getBusyReaderCount());
                }
                execute("DROP TABLE IF EXISTS l");
                execute("DROP TABLE IF EXISTS r");
                createTables(5, 12, 3);
                assertExecution(sql, true);
            }
        });
    }

    @Test
    public void testFilteredIntervalAndPushdownInputsNeverSwap() throws Exception {
        assertMemoryLeak(() -> {
            // l spans three hourly partitions, and r too.
            createTables(13, 30, 3);
            sqlExecutionContext.changePageFrameSizes(2, 3);
            // l is the smaller, but a filter's cursor and an interval scan's report no size, so the
            // ordinary plan streams l, and so does the parallel join, which plans no swap.
            for (String sql : new String[]{
                    "SELECT * FROM l JOIN r ON l.k = r.k WHERE l.v > 0",
                    "SELECT * FROM l JOIN (SELECT * FROM r WHERE w > 0) r ON l.k = r.k",
                    "SELECT * FROM l JOIN r ON l.k = r.k WHERE l.ts IN '2024-01-01'",
                    "SELECT * FROM l JOIN (SELECT * FROM r WHERE rts IN '2023-12-31') r ON l.k = r.k",
            }) {
                Assert.assertFalse(assertExecution(sql, false).contains(INPUT_SWAP));
            }
            // A scan of Parquet partitions that prunes row groups by the filter's conditions reports no
            // size either, and the filter's cursor none in any case.
            execute("ALTER TABLE l CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-01T02'");
            execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE rts < '2023-12-31T02'");
            for (String sql : new String[]{
                    "SELECT * FROM l JOIN r ON l.k = r.k WHERE l.v > 0",
                    "SELECT * FROM l JOIN (SELECT * FROM r WHERE w > 0) r ON l.k = r.k",
            }) {
                Assert.assertFalse(assertExecution(sql, false).contains(INPUT_SWAP));
            }
            // Over unfiltered Parquet partitions, the join swaps as over native ones.
            TestUtils.assertContains(assertExecution("SELECT * FROM l JOIN r ON l.k = r.k", true), INPUT_SWAP);
        });
    }

    @Test
    public void testInputSizes() throws Exception {
        assertMemoryLeak(() -> {
            // The left input smaller, larger, as large, empty, and the right input empty; with
            // duplicate left keys, which chain in the swapped build, and unique ones, whose build
            // positions the payload on the lookup itself.
            final int[][] shapes = {
                    // left rows, right rows, left key modulus, swapped
                    {5, 30, 3, 1},
                    {6, 30, 1_000, 1},
                    {30, 5, 3, 0},
                    {12, 12, 3, 0},
                    {0, 12, 3, 0},
                    {12, 0, 3, 0},
            };
            for (int[] shape : shapes) {
                createTables(shape[0], shape[1], shape[2]);
                for (int frameRows : new int[]{2, 1_000_000}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    for (String sql : new String[]{
                            "SELECT * FROM l JOIN r ON l.k = r.k",
                            // Projections that reorder, drop and repeat the inputs' columns.
                            "SELECT l.vc, r.rvc, l.v, r.w, l.s, r.s, l.k FROM (SELECT ts, vc, k, s, v, v v2 FROM l) l JOIN (SELECT rvc, s, k, w FROM r) r ON l.k = r.k",
                            // The keys stage through key sinks, as composite keys do.
                            "SELECT * FROM l JOIN r ON l.k = r.k AND l.s = r.s",
                            "SELECT * FROM l JOIN r ON l.v = r.w",
                            // A LIMIT closes the cursor with frames in flight, and a negative LIMIT
                            // counts the rows, then reads them again from the top.
                            "SELECT * FROM l JOIN r ON l.k = r.k LIMIT 4",
                            "SELECT * FROM l JOIN r ON l.k = r.k LIMIT -4",
                    }) {
                        TestUtils.assertContains(assertExecution(sql, shape[3] == 1), INPUT_SWAP);
                    }
                }
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
                if (shape[3] == 1) {
                    // The rows come in another order than when the left input streams, which the left
                    // input does when it offers no random access.
                    Assert.assertNotEquals(
                            ordinary("SELECT * FROM l JOIN r ON l.k = r.k", sqlExecutionContext),
                            ordinary("SELECT * FROM '*!*l' l JOIN r ON l.k = r.k", sqlExecutionContext)
                    );
                    Assert.assertFalse(assertExecution("SELECT * FROM '*!*l' l JOIN r ON l.k = r.k", false).contains(INPUT_SWAP));
                }
                execute("DROP TABLE l");
                execute("DROP TABLE r");
            }
        });
    }

    @Test
    public void testHandPlannedJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 30, 3);
            final String swapped = rows(ordinary("SELECT * FROM l JOIN r ON l.k = r.k", sqlExecutionContext));
            final String streamed = rows(ordinary("SELECT * FROM '*!*l' l JOIN r ON l.k = r.k", sqlExecutionContext));
            Assert.assertNotEquals(swapped, streamed);
            try (
                    RecordCursorFactory left = select("SELECT * FROM l");
                    RecordCursorFactory right = select("SELECT * FROM r");
                    AsyncHashJoinRecordCursorFactory factory = newInnerJoin(left, right, null, false, RecordCursorFactory.SCAN_DIRECTION_FORWARD)
            ) {
                // A join without the keys of the swapped orientation never swaps, although l is the
                // smaller.
                Assert.assertFalse(plan(factory, sqlExecutionContext).contains(INPUT_SWAP));
                Assert.assertEquals(streamed, rows(result(factory, sqlExecutionContext)));
                Assert.assertFalse(factory.getAtom().isSwapped());
            }
            try (
                    RecordCursorFactory left = select("SELECT * FROM l");
                    RecordCursorFactory right = select("SELECT * FROM r");
                    AsyncHashJoinRecordCursorFactory factory = newInnerJoin(left, right, null, true, RecordCursorFactory.SCAN_DIRECTION_OTHER)
            ) {
                // A left input that reports no scan direction leaves the order open when a parent
                // asks, as it leaves the serial join's.
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_OTHER, factory.getScanDirection());
                TestUtils.assertContains(plan(factory, sqlExecutionContext), INPUT_SWAP);
                Assert.assertEquals(swapped, rows(result(factory, sqlExecutionContext)));
                Assert.assertTrue(factory.getAtom().isSwapped());
            }
        });
    }

    @Test
    public void testKeysThatDoNotCompileKeepOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lb AS (SELECT x::INT k, rnd_bin(1, 4, 1) b, ARRAY[x::DOUBLE] a, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(5)) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE rb AS (SELECT rnd_bin(1, 4, 1) b, ARRAY[x::DOUBLE] a, (x % 4)::INT k FROM long_sequence(12))");
            // An INNER join that may swap its inputs, over keys that the operator does not stage.
            assertOrdinaryPlan("SELECT lb.k, rb.k FROM lb JOIN rb ON lb.b = rb.b", sqlExecutionContext);
            assertOrdinaryPlan("SELECT lb.k, rb.k FROM lb JOIN rb ON lb.k = rb.k AND lb.a = rb.a", sqlExecutionContext);
            TestUtils.assertContains(assertExecution("SELECT lb.k, rb.k FROM lb JOIN rb ON lb.k = rb.k", true), INPUT_SWAP);
        });
    }

    @Test
    public void testOrderDeterminedByAsOfJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 30, 3);
            execute("CREATE TABLE q AS (SELECT ('s' || (x % 3))::SYMBOL s, x qv, timestamp_sequence('2024-01-01', 450_000_000) qts FROM long_sequence(20)) TIMESTAMP(qts) PARTITION BY DAY");
            TestUtils.assertContains(assertExecution("SELECT l.ts, l.s, l.v, r.w FROM l JOIN r ON l.k = r.k", true), INPUT_SWAP);
            // The ASOF join asks its master for its scan direction, which fixes the INNER join's
            // order, so no execution swaps the INNER join's inputs, although l is the smaller: the
            // plan, which prints once every parent asked, names no swap, and the rows come in l's
            // order, as the ordinary plan's.
            for (String sql : new String[]{
                    "SELECT l.ts, l.v, r.w, q.qv FROM l JOIN r ON l.k = r.k ASOF JOIN q ON (s)",
                    "SELECT j.ts, j.v, j.w, q.qv FROM (SELECT l.ts, l.s, l.v, r.w FROM l JOIN r ON l.k = r.k) j ASOF JOIN q ON (s)",
            }) {
                final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
                TestUtils.assertContains(plan, "AsOf Join");
                Assert.assertFalse(plan, plan.contains(INPUT_SWAP));
            }
        });
    }

    @Test
    public void testOrderDeterminedByOrderBy() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 30, 3);
            // ORDER BY the left input's timestamp asks the join for its scan direction, which fixes
            // its order, ascending or descending, so the join streams l and sorts nothing.
            for (String sql : new String[]{
                    "SELECT * FROM l JOIN r ON l.k = r.k ORDER BY l.ts",
                    "SELECT * FROM l JOIN r ON l.k = r.k ORDER BY l.ts DESC",
                    "SELECT * FROM l JOIN r ON l.k = r.k ORDER BY l.ts, r.w",
            }) {
                final String plan = assertExecution(sql, false);
                Assert.assertFalse(plan, plan.contains(INPUT_SWAP));
            }
            // ORDER BY another column sorts the join's rows, whose order the join keeps open: the left
            // input's forward scan follows no advice, so the join swaps.
            TestUtils.assertContains(assertExecution("SELECT * FROM l JOIN r ON l.k = r.k ORDER BY r.w, l.v", true), INPUT_SWAP);
            // A parent's questions, asked of the join itself.
            try (RecordCursorFactory factory = select("SELECT * FROM l JOIN r ON l.k = r.k")) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                Assert.assertFalse(join.followedOrderByAdvice());
                assertExecution(factory, join, "SELECT * FROM l JOIN r ON l.k = r.k", true);
                Assert.assertEquals(RecordCursorFactory.SCAN_DIRECTION_FORWARD, join.getScanDirection());
                Assert.assertFalse(plan(factory, sqlExecutionContext).contains(INPUT_SWAP));
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertFalse(join.getAtom().isSwapped());
                }
                Assert.assertEquals(ordinary("SELECT * FROM l JOIN r ON l.k = r.k ORDER BY l.ts", sqlExecutionContext), result(factory, sqlExecutionContext));
            }
        });
    }

    @Test
    public void testOtherFramedInputsKeepOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 30, 3);
            // Page frame inputs other than table scans answer size() from cursors of their own, which
            // the parallel join does not reproduce: an INNER first join over one, whose ordinary plan
            // could swap, keeps that plan.
            sqlExecutionContext.setParallelReadParquetEnabled(true);
            writeParquet("r", "r.parquet");
            writeParquet("l", "l.parquet");
            execute("CREATE TABLE flag AS (SELECT true b FROM long_sequence(1))");
            execute("CREATE TABLE ix (s SYMBOL INDEX, k INT, w LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("INSERT INTO ix SELECT ('s' || (x % 3))::SYMBOL, (x % 4)::INT, x, timestamp_sequence('2024-01-01', 60_000_000) FROM long_sequence(30)");
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, k INT, y DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 100ms START FROM BEGINNING AS SELECT ts, sym, k, y, sum(y) OVER (PARTITION BY sym ORDER BY ts ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) sy FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                execute("INSERT INTO base SELECT timestamp_sequence('2024-01-01', 30_000_000), ('s' || (x % 2))::SYMBOL, (x % 4)::INT, x::DOUBLE FROM long_sequence(40)");
                drainWalQueue();
                //noinspection StatementWithEmptyBody
                for (int i = 0; i < 64 && job.run(); i++) {
                }
                drainWalQueue();
            }
            for (String sql : new String[]{
                    // A live view's dictionary may hold a text under two keys, so no SYMBOL key
                    // translates into it, whichever join hashes it.
                    "SELECT * FROM l LEFT JOIN lv ON l.s = lv.sym",
                    // read_parquet()
                    "SELECT * FROM l JOIN read_parquet('r.parquet') p ON l.k = p.k",
                    "SELECT * FROM read_parquet('l.parquet') p JOIN r ON p.k = r.k",
                    // a live view
                    "SELECT * FROM l JOIN lv ON l.k = lv.k",
                    "SELECT * FROM lv JOIN r ON lv.k = r.k",
                    // a filter whose value is a runtime constant, which gates its scan
                    "SELECT * FROM l JOIN (SELECT * FROM r WHERE (SELECT b FROM flag LIMIT 1)) r ON l.k = r.k",
                    "SELECT * FROM (SELECT * FROM l WHERE (SELECT b FROM flag LIMIT 1)) l JOIN r ON l.k = r.k",
                    // a single-symbol lookup on an indexed column
                    "SELECT * FROM l JOIN (SELECT * FROM ix WHERE s = 's1') i ON l.k = i.k",
            }) {
                assertOrdinaryPlan(sql, sqlExecutionContext);
            }
            TestUtils.assertContains(ordinaryPlan("SELECT * FROM l JOIN (SELECT * FROM r WHERE (SELECT b FROM flag LIMIT 1)) r ON l.k = r.k"), "Filter filter: ");
            TestUtils.assertContains(ordinaryPlan("SELECT * FROM l JOIN (SELECT * FROM ix WHERE s = 's1') i ON l.k = i.k"), "DeferredSingleSymbolFilterPageFrame");
            // Over such an input, a join whose ordinary plan never swaps takes the operator: a LEFT
            // join, or an INNER one whose other input scans an interval.
            for (String sql : new String[]{
                    "SELECT * FROM l LEFT JOIN read_parquet('r.parquet') p ON l.k = p.k",
                    "SELECT * FROM l JOIN read_parquet('r.parquet') p ON l.k = p.k WHERE l.ts IN '2024-01-01'",
                    "SELECT * FROM lv LEFT JOIN r ON lv.k = r.k",
                    "SELECT * FROM l LEFT JOIN (SELECT * FROM r WHERE (SELECT b FROM flag LIMIT 1)) r ON l.k = r.k",
                    "SELECT * FROM l JOIN (SELECT * FROM r WHERE (SELECT b FROM flag LIMIT 1)) r ON l.k = r.k WHERE l.ts IN '2024-01-01'",
            }) {
                Assert.assertFalse(assertParallelHashJoin(sql, sqlExecutionContext).contains(INPUT_SWAP));
            }
        });
    }

    @Test
    public void testPartitionedBuildOfLeftInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables(40, 120, 7);
            sqlExecutionContext.changePageFrameSizes(4, 5);
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 2);
            // The payload copy reads the swapped build's payload, the left input's columns, from a
            // copy, whenever its columns allow one.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (String sql : new String[]{
                        "SELECT * FROM l JOIN r ON l.k = r.k",
                        "SELECT l.k, l.v, l.s, r.w, r.s FROM l JOIN r ON l.s = r.s",
                        "SELECT l.k, l.v, l.ts, r.w FROM l JOIN r ON l.k = r.k AND l.s = r.s",
                }) {
                    assertExecution(sql, true);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            final HashJoinBuildStage stage = join.getAtom().getStage(0);
                            Assert.assertTrue(join.getAtom().isSwapped());
                            Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                            Assert.assertEquals(minRows == 0, stage.getPartitionCount() > 1);
                            // The left input's 40 rows are the build's.
                            Assert.assertEquals(40, stage.getFrozenBuild().getRowCount());
                            // A payload of fixed-size columns copies; one with a STRING or VARCHAR
                            // column does not.
                            Assert.assertEquals(!sql.startsWith("SELECT *"), stage.isPayloadCopied());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 30, 3);
            // The plan shows the inputs in the query's roles, and the first join the rule it swaps by.
            assertQuery("SELECT l.k, l.v, r.w FROM l JOIN r ON l.k = r.k")
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 1
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: l
                                    Build
                                      joinType: inner
                                      condition: r.k=l.k
                                      inputSwap: when the left input is smaller
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: r
                            """);
            // A LEFT join, and a left input without random access, never swap.
            Assert.assertFalse(assertExecution("SELECT * FROM l LEFT JOIN r ON l.k = r.k", false).contains("inputSwap"));
            Assert.assertFalse(assertExecution("SELECT * FROM '*!*l' l JOIN r ON l.k = r.k", false).contains("inputSwap"));
        });
    }

    @Test
    public void testPostJoinFilterKeepsSwapPossible() throws Exception {
        assertMemoryLeak(() -> {
            // A post-join filter runs on the workers in either orientation, over the joined rows'
            // SYMBOL columns of both inputs too. A parent that asks whether the order followed its
            // advice gets the serial filter's answer, no, which leaves the order open: the join swaps
            // under ORDER BY a column that is not the timestamp, and under window functions.
            for (int[] shape : new int[][]{{5, 30, 1}, {30, 5, 0}}) {
                createTables(shape[0], shape[1], 3);
                sqlExecutionContext.changePageFrameSizes(2, 3);
                for (String sql : new String[]{
                        "SELECT * FROM l JOIN r ON l.k = r.k WHERE l.v <> r.w ORDER BY r.w, l.v",
                        "SELECT * FROM l JOIN r ON l.k = r.k WHERE l.s <> r.s OR r.s = 's1' ORDER BY r.rvc",
                        "SELECT l.k, l.v, r.w, row_number() OVER (PARTITION BY l.k ORDER BY r.w) rn FROM l JOIN r ON l.k = r.k WHERE l.v <> r.w",
                        "SELECT l.k, l.s, r.s, sum(r.w) OVER (PARTITION BY l.s) sw FROM l JOIN r ON l.k = r.k WHERE l.s <> r.s",
                }) {
                    final String plan = assertExecution(sql, shape[2] == 1);
                    TestUtils.assertContains(plan, "postJoinFilter: ");
                    TestUtils.assertContains(plan, INPUT_SWAP);
                }
                // ORDER BY the left input's timestamp still asks for the scan direction, which fixes
                // the order, as it does in the serial plan.
                Assert.assertFalse(assertExecution("SELECT * FROM l JOIN r ON l.k = r.k WHERE l.v <> r.w ORDER BY l.ts DESC", false).contains(INPUT_SWAP));
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
                execute("DROP TABLE l");
                execute("DROP TABLE r");
            }
        });
    }

    @Test
    public void testSerialSizeMatchesRecordCursorSize() throws Exception {
        assertMemoryLeak(() -> {
            createTables(5, 30, 3);
            execute("CREATE TABLE e (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR");
            execute("CREATE TABLE n AS (SELECT (x % 3)::INT k, x v FROM long_sequence(7))");
            execute("CREATE TABLE pq AS (SELECT (x % 3)::INT k, x v, timestamp_sequence('2024-01-01', 3_600_000_000) ts FROM long_sequence(48)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE pq CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-02'");
            // Every input kind the parallel join takes, in both roles: the size it computes from the
            // input's page frame cursor is the size the input's record cursor reports.
            for (String input : new String[]{
                    "l",
                    "(SELECT v, k, ts, v v2 FROM l)",
                    "e",
                    "n",
                    "pq",
                    "(SELECT * FROM l WHERE ts IN '2024-01-01T00')",
                    "(SELECT * FROM l WHERE v > 2)",
                    "(SELECT k, v FROM l WHERE v > 2)",
                    "(SELECT * FROM pq WHERE v > 2)",
            }) {
                final long expected;
                try (RecordCursorFactory factory = select("SELECT * FROM " + input); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    expected = cursor.size();
                }
                assertSerialSize(input, expected, "SELECT * FROM " + input + " a LEFT JOIN n ON a.k = n.k", true);
                assertSerialSize(input, expected, "SELECT * FROM n LEFT JOIN " + input + " b ON n.k = b.k", false);
            }
            // Scans of their own: a backward scan, and the scan under a filter whose Parquet row group
            // pruning skips rows that its frame cursor counts, which its record cursor does not report.
            for (String sql : new String[]{"SELECT * FROM l ORDER BY ts DESC", "SELECT * FROM pq ORDER BY ts DESC", "SELECT * FROM pq WHERE v > 2"}) {
                try (RecordCursorFactory query = select(sql)) {
                    RecordCursorFactory scan = query.getBaseFactory();
                    if (!scan.supportsPageFrameCursor()) {
                        scan = scan.getBaseFactory();
                    }
                    final int order = scan.getScanDirection() == RecordCursorFactory.SCAN_DIRECTION_BACKWARD ? ORDER_DESC : ORDER_ASC;
                    final long expected;
                    try (RecordCursor cursor = scan.getCursor(sqlExecutionContext)) {
                        expected = cursor.size();
                    }
                    try (PageFrameCursor frameCursor = scan.getPageFrameCursor(sqlExecutionContext, order)) {
                        Assert.assertEquals(sql.contains("WHERE"), frameCursor.hasActivePushdownFilter());
                        Assert.assertEquals(sql, expected, HashJoinInput.getSerialSize(frameCursor, false));
                    }
                    Assert.assertEquals(sql, sql.contains("WHERE") ? -1 : sql.contains("pq") ? 48 : 5, expected);
                }
            }
        });
    }

    @Test
    public void testSymbolKeysTranslatedBothDirections() throws Exception {
        assertMemoryLeak(() -> {
            // The inputs' dictionaries differ, so a probe's SYMBOL key translates into the build's
            // dictionary: r's into l's when the join swaps, l's into r's when it does not.
            for (int[] shape : new int[][]{{5, 30, 1}, {30, 5, 0}}) {
                createTables(shape[0], shape[1], 3);
                execute("ALTER TABLE l ADD COLUMN s2 SYMBOL");
                execute("UPDATE l SET s2 = 's' || (v % 2)");
                for (String sql : new String[]{
                        "SELECT * FROM l JOIN r ON l.s = r.s",
                        "SELECT l.k, l.s, r.s, r.w FROM l JOIN r ON l.s = r.s AND l.k = r.k",
                        // A SYMBOL key that compares with a VARCHAR as text.
                        "SELECT l.s, l.v, r.rvc FROM l JOIN r ON l.s = r.rvc",
                        // Two SYMBOL keys of l with two columns of r's projection that read one
                        // column of r: l's keys translate into r's dictionary, and swapped, each of
                        // r's columns into the dictionary of its l column.
                        "SELECT l.s, l.s2, r.s, r.w FROM l JOIN (SELECT s, s s3, w, k FROM r) r ON l.s = r.s AND l.s2 = r.s3",
                }) {
                    final String plan = assertExecution(sql, shape[2] == 1);
                    TestUtils.assertContains(plan, INPUT_SWAP);
                    Assert.assertEquals(!sql.contains("rvc"), plan.contains("symbolKeyJoin: true"));
                }
                // Two keys on one column become one key and a filter on the other side's columns.
                final String filtered = assertExecution("SELECT l.s, l.s2, r.s, r.w FROM l JOIN r ON l.s = r.s AND l.s2 = r.s", false);
                TestUtils.assertContains(filtered, "filter: s=s2");
                Assert.assertFalse(filtered, filtered.contains(INPUT_SWAP));
                // The optimiser matches the names as written, so two keys on one column of r that
                // name it in another case stay two keys. Swapped, r's column would probe both and
                // translate into two dictionaries of l, which the operator does not stage, so a
                // join that may swap keeps the ordinary plan, while one over a filtered l, which
                // never swaps, translates l's two columns into r's dictionary.
                assertOrdinaryPlan("SELECT l.s, l.s2, r.s, r.w FROM l JOIN r ON l.s = r.s AND l.s2 = r.S", sqlExecutionContext);
                assertOrdinaryPlan("SELECT l.s, l.s2, r.s, r.w FROM l JOIN r ON l.s = r.\"S\" AND l.s2 = r.s", sqlExecutionContext);
                final String unswapped = assertParallelHashJoin("SELECT l.s, l.s2, r.s, r.w FROM l JOIN r ON l.s = r.s AND l.s2 = r.S WHERE l.v > 1", sqlExecutionContext);
                TestUtils.assertContains(unswapped, "condition: r.S=l.s2 and r.s=l.s");
                Assert.assertFalse(unswapped, unswapped.contains(INPUT_SWAP));
                try (RecordCursorFactory factory = select("SELECT * FROM l JOIN r ON l.s = r.s")) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        final HashJoinBuildStage stage = join.getAtom().getStage(0);
                        Assert.assertEquals(shape[2] == 1, join.getAtom().isSwapped());
                        Assert.assertTrue(stage.isSymbolKey());
                        // The build holds every row of the input it hashes, the left one when swapped.
                        Assert.assertEquals(shape[2] == 1 ? shape[0] : shape[1], stage.getFrozenBuild().getRowCount());
                    }
                }
                execute("DROP TABLE l");
                execute("DROP TABLE r");
            }
        });
    }

    @Test
    public void testToTopMidStreamInBothOrientations() throws Exception {
        assertMemoryLeak(() -> {
            for (int[] shape : new int[][]{{5, 30, 1}, {30, 5, 0}}) {
                createTables(shape[0], shape[1], 3);
                sqlExecutionContext.changePageFrameSizes(2, 3);
                final String sql = "SELECT * FROM l JOIN r ON l.k = r.k";
                final String expected;
                sqlExecutionContext.setParallelHashJoinEnabled(false);
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertNull(find(factory));
                    expected = printRows(factory);
                }
                sqlExecutionContext.setParallelHashJoinEnabled(true);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final RecordMetadata metadata = factory.getMetadata();
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(shape[2] == 1, join.getAtom().isSwapped());
                        // toTop() before the first read, after a few rows, inside a chain of matches
                        // and after the last row each read the whole result again, in the same
                        // orientation.
                        for (int stop : new int[]{0, 1, 4, 7, Integer.MAX_VALUE}) {
                            final StringSink sink = new StringSink();
                            int rows = 0;
                            while (rows++ < stop && cursor.hasNext()) {
                                CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                            }
                            Assert.assertTrue(expected.startsWith(sink.toString()));
                            cursor.toTop();
                            Assert.assertEquals(shape[2] == 1, join.getAtom().isSwapped());
                        }
                        final StringSink sink = new StringSink();
                        while (cursor.hasNext()) {
                            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                        }
                        Assert.assertEquals(expected, sink.toString());
                    }
                }
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
                execute("DROP TABLE l");
                execute("DROP TABLE r");
            }
        });
    }

    // Compares an execution of the factory with the ordinary plan and checks its orientation.
    private static void assertExecution(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, String sql, boolean isSwapped) throws SqlException {
        Assert.assertEquals(sql, ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
            }
        }
    }

    // Asserts that the query runs the operator, with the ordinary plan's result, and that an
    // execution took the given orientation; returns the operator's plan.
    private static String assertExecution(String sql, boolean isSwapped) throws SqlException {
        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            assertExecution(factory, join, sql, isSwapped);
        }
        return plan;
    }

    private static void assertSerialSize(String input, long expected, String sql, boolean isLeft) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            final AsyncHashJoinSpec spec = join.getSpec();
            final HashJoinInput hashJoinInput = isLeft ? spec.getLeftInput() : spec.getStage(0).getBuildInput();
            try (PageFrameCursor cursor = hashJoinInput.getFactory().getPageFrameCursor(sqlExecutionContext, hashJoinInput.getOrder())) {
                Assert.assertEquals(input, expected, hashJoinInput.getSerialSize(cursor));
            }
        }
    }

    // l, the left input, and r, the right one, with duplicate and NULL keys on both sides, SYMBOL
    // columns over dictionaries that differ, and var-size columns. The key columns sit at other
    // positions in the two tables, so that a key read from the wrong input's column misreads.
    private static void createTables(int leftRows, int rightRows, int leftKeyModulus) throws Exception {
        execute("CREATE TABLE l AS (" +
                "SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % " + leftKeyModulus + ")::INT END k, x v, ('s' || (x % 3))::SYMBOL s," +
                " 'str' || x str, ('vc' || x)::VARCHAR vc, timestamp_sequence('2024-01-01', 600_000_000) ts" +
                " FROM long_sequence(" + leftRows + ")) TIMESTAMP(ts) PARTITION BY HOUR");
        execute("CREATE TABLE r AS (" +
                "SELECT x w, ('s' || (x % 5))::VARCHAR rvc, CASE WHEN x % 5 = 0 THEN NULL ELSE (x % 4)::INT END k," +
                " ('s' || (x % 4))::SYMBOL s, timestamp_sequence('2023-12-31', 300_000_000) rts" +
                " FROM long_sequence(" + rightRows + ")) TIMESTAMP(rts) PARTITION BY HOUR");
    }

    private static IntList identity(int columnCount) {
        final IntList columns = new IntList();
        for (int i = 0; i < columnCount; i++) {
            columns.add(i);
        }
        return columns;
    }

    // An INNER join of two compiled scans on their columns k that may swap its inputs, planned by
    // hand, with the given post-join filter, if any.
    private static AsyncHashJoinRecordCursorFactory newSwapCandidate(
            RecordCursorFactory leftQuery,
            RecordCursorFactory rightQuery,
            @Nullable Function postFilter
    ) throws SqlException {
        return newInnerJoin(leftQuery, rightQuery, postFilter, true, leftQuery.getBaseFactory().getScanDirection());
    }

    // An INNER join of two compiled scans on their columns k, planned by hand: one that may swap its
    // inputs, with the keys of the swapped orientation, or one that never swaps, without them; with
    // the given post-join filter, if any, and the scan direction that the left input reported when
    // the join was planned. The factory takes the scans the two QueryProgress factories wrap, and the
    // wrappers close them again, which is a no-op.
    private static AsyncHashJoinRecordCursorFactory newInnerJoin(
            RecordCursorFactory leftQuery,
            RecordCursorFactory rightQuery,
            @Nullable Function postFilter,
            boolean isSwapCandidate,
            int leftScanDirection
    ) throws SqlException {
        final RecordCursorFactory left = leftQuery.getBaseFactory();
        final RecordCursorFactory right = rightQuery.getBaseFactory();
        final RecordMetadata leftMetadata = left.getMetadata();
        final RecordMetadata rightMetadata = right.getMetadata();
        final int leftKey = leftMetadata.getColumnIndex("k");
        final int rightKey = rightMetadata.getColumnIndex("k");
        final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        Assert.assertTrue(keys.add(leftKey, leftMetadata.getColumnType(leftKey), rightKey, rightMetadata.getColumnType(rightKey)));
        final HashJoinGroupByKeys swapKeys = new HashJoinGroupByKeys();
        Assert.assertTrue(swapKeys.add(rightKey, rightMetadata.getColumnType(rightKey), leftKey, leftMetadata.getColumnType(leftKey)));
        final IntList leftColumns = identity(leftMetadata.getColumnCount());
        final IntList rightColumns = identity(rightMetadata.getColumnCount());
        final BytecodeAssembler asm = new BytecodeAssembler();
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, asm, keys, "l", leftMetadata, leftColumns, "r", rightMetadata, rightColumns);
        final HashJoinKeyLayout swapKeyLayout = new HashJoinKeyLayout(configuration, asm, swapKeys, "r", rightMetadata, rightColumns, "l", leftMetadata, leftColumns);
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, leftColumns.size() + rightColumns.size());
        metadata.copyColumnMetadataFrom("l", leftMetadata);
        metadata.copyColumnMetadataFrom("r", rightMetadata);
        metadata.setTimestampIndex(leftMetadata.getTimestampIndex());
        final HashJoinInput rightInput = new HashJoinInput(right, rightColumns, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0));
        final HashJoinStageSpec stage = isSwapCandidate
                ? new HashJoinStageSpec(rightInput, keyLayout, swapKeyLayout, false, "r.k=l.k", metadata, null, null)
                : new HashJoinStageSpec(rightInput, keyLayout, false, "r.k=l.k", metadata, null, null);
        if (postFilter != null) {
            stage.ofPostFilter(postFilter, null);
        }
        final AsyncHashJoinSpec spec = new AsyncHashJoinSpec(
                new HashJoinInput(left, leftColumns, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0)),
                stage,
                metadata,
                left.followedOrderByAdvice(),
                leftScanDirection
        );
        Assert.assertEquals(isSwapCandidate, spec.isSwapCandidate());
        return new AsyncHashJoinRecordCursorFactory(
                engine,
                configuration,
                engine.getMessageBus(),
                spec,
                () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                1
        );
    }

    private static String ordinaryPlan(String sql) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            return plan(factory, sqlExecutionContext);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    private static String printRows(RecordCursorFactory factory) throws SqlException {
        final StringSink sink = new StringSink();
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                CursorPrinter.println(cursor.getRecord(), factory.getMetadata(), sink, true);
            }
        }
        return sink.toString();
    }

    // A result's rows, without its metadata lines.
    private static String rows(String result) {
        return result.substring(result.indexOf('\n', result.indexOf('\n') + 1) + 1);
    }

    // Writes the table's first partition to a Parquet file under the input root, for read_parquet().
    private static void writeParquet(String table, String file) {
        try (
                Path path = new Path();
                PartitionDescriptor descriptor = new PartitionDescriptor();
                TableReader reader = engine.getReader(table)
        ) {
            path.of(root).concat(file);
            engine.getConfiguration().getFilesFacade().remove(path.$());
            PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
            PartitionEncoder.encode(descriptor, path);
        }
    }
}
