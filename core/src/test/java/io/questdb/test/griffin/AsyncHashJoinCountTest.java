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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.test.TestLatchedCounterFunctionFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * The parallel hash join's count of its rows, {@code RecordCursor.calculateSize()}, which a negative
 * {@code LIMIT} and {@code count(*)} over the join call: count-only tasks count each frame's joined
 * rows on the workers, the chains that the tuples leave to the consumer count by their links, which
 * stay as they are, and a count that follows some rows first counts what the frames already joined
 * into tuples hold, the rest of a frame left to a continuation included. Every count compares with
 * the rows of the ordinary plan, and the cursor reads the ordinary plan's rows again, in order, after
 * {@code toTop()}.
 */
public class AsyncHashJoinCountTest extends AbstractCairoTest {
    private static final String ATOM = AsyncHashJoinAtom.class.getName();
    private static final String CURSOR = "io.questdb.griffin.engine.table.AsyncHashJoinRecordCursor";
    // The error of a count whose rows do not fit in a LONG.
    private static final String OVERFLOW = "hash join row count overflow";
    // Single stages and stacks, each with its rows' shape: unique matches and LEFT misses, which the
    // tuples carry as locators; duplicate keys, NULL keys among them, whose chains the tuples leave
    // to the consumer; residuals and post-join filters, which the workers run per pair, and whose
    // fan-out overflows a task's cap; a JIT compiled probe filter, also over a backward scan of a
    // build whose keys hold one row or several, and under a residual; a backward probe scan; the swapped
    // orientation, which hashes d1 and probes t; two deferred stages, which the consumer walks
    // odometer-style; an enumerated stage ahead of deferred ones; a swapped stack, whose later stage
    // reads its key off the first stage's matches; and an INNER join over an empty build.
    private static final String[] SHAPES = {
            "SELECT t.v, u1.w FROM t LEFT JOIN u1 ON t.k = u1.k",
            "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k",
            "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w > t.v % 9",
            "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k WHERE d1.w + t.v % 5 > 6",
            "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k WHERE t.v > 7",
            "SELECT t.v, m.w FROM t LEFT JOIN m ON t.k = m.k WHERE t.v > 5 ORDER BY t.ts DESC",
            "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w > t.v % 9 WHERE t.v > 3",
            "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k ORDER BY t.ts DESC",
            "SELECT d1.w, t.v FROM d1 JOIN t ON d1.k = t.k",
            "SELECT t.v, d1.w, d2.w2 FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk",
            "SELECT t.v, d1.w, d2.w2, d3.w3 FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w > t.v % 9 JOIN d2 ON t.lk = d2.lk LEFT JOIN d3 ON t.sym = d3.s",
            "SELECT t.v, d1.w, d2.w2 FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.lk = d2.lk WHERE d2.w2 > t.v",
            "SELECT d1.w, t.v, d3.w3 FROM d1 JOIN t ON d1.k = t.k LEFT JOIN d3 ON d1.s = d3.s",
            "SELECT t.v, e.w FROM t JOIN (SELECT * FROM d1 WHERE w < 0) e ON t.k = e.k",
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownCount() {
        TestLatchedCounterFunctionFactory.reset(null);
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
        sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
    }

    @Test
    public void testCalculateSizeAfterEveryRow() throws Exception {
        // A window of three frames, so that the frames past it count on count-only tasks, and a cap
        // of one tuple per probe row, so that a frame whose pairs outnumber its rows goes on in a
        // continuation.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            for (String sql : SHAPES) {
                final int[] counts = assertCountAfterEveryRow(sql);
                if (sql.contains("w < 0")) {
                    // An INNER join over an empty build dispatches no task.
                    Assert.assertArrayEquals(sql, new int[]{0, 0, 0}, counts);
                    continue;
                }
                // Count-only tasks counted the frames past the window, and the count met frames whose
                // tasks joined them into tuples before it began.
                Assert.assertTrue(sql, counts[0] > 0);
                Assert.assertTrue(sql, counts[1] > 0);
                // Every shape that walks its pairs on the workers, with more pairs than rows, leaves
                // continuations, which some counts met before they were joined.
                final boolean isEnumerated = sql.contains("%") || sql.contains("d2.w2 > t.v") || sql.contains("d3 ON d1.s");
                Assert.assertEquals(sql, isEnumerated, counts[2] > 0);
            }
        });
    }

    @Test
    public void testCalculateSizeAfterEveryRowInOneWindow() throws Exception {
        // The default window takes every frame in the first dispatch, so a count that follows a row
        // meets tuple-carrying tasks alone.
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            for (String sql : SHAPES) {
                assertCountAfterEveryRow(sql);
            }
        });
    }

    @Test
    public void testCalculateSizeOnSharedStepperTemplate() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 3);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            // A JVM that cannot define hidden classes hands every stage the shared stepper template,
            // whose count runs here.
            AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(false);
            try {
                createTables();
                sqlExecutionContext.changePageFrameSizes(4, 4);
                for (String sql : SHAPES) {
                    try (RecordCursorFactory factory = select(sql)) {
                        Assert.assertFalse(sql, find(factory).getStepperClass(0).isHidden());
                    }
                    assertCountAfterEveryRow(sql);
                }
                // A count-only task that walks a deferred chain to count it checks the breaker every
                // so many links, and unwinds once the execution is cancelled, over every row of its
                // frame or over the rows that its probe filter kept.
                for (String sql : new String[]{
                        "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k",
                        "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k WHERE t.v > 2",
                }) {
                    final String expected = ordinary(sql, sqlExecutionContext);
                    final ChainWalkBreaker breaker = new ChainWalkBreaker();
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), new RecordCursor.Counter());
                            Assert.fail("expected cancellation: " + sql);
                        } catch (CairoException e) {
                            Assert.assertTrue(e.getMessage(), e.isCancellation());
                        } finally {
                            ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                        }
                        Assert.assertTrue(sql, breaker.isCancelledInChainWalk);
                        assertReleased(join);
                        Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                    }
                }
            } finally {
                AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
            }
        });
    }

    @Test
    public void testCancellationInsideConsumerChainCount() throws Exception {
        // After the first row, the count walks the chains that the tuples left to the consumer to
        // count them, and checks the breaker before each walk: a query that a hot key fans out to
        // billions of rows stops at the next chain.
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            for (String sql : new String[]{
                    // The tuples after the first one carry chains of m's duplicate keys.
                    "SELECT t.v, m.w FROM t LEFT JOIN m ON t.k = m.k",
                    // The first row's tuple carries two chains, whose later combinations the count
                    // counts first.
                    "SELECT t.v, m.w, d2.w2 FROM t JOIN m ON t.k = m.k JOIN d2 ON t.lk = d2.lk",
            }) {
                final String expected = ordinary(sql, sqlExecutionContext);
                final ConsumerChainCountBreaker breaker = new ConsumerChainCountBreaker();
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), new RecordCursor.Counter());
                        Assert.fail("expected cancellation: " + sql);
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isCancellation());
                    } finally {
                        ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                    }
                    // The first chain walk met the cancellation, and the count walked no other chain.
                    Assert.assertEquals(sql, 1, breaker.chainWalkCheckCount);
                    assertReleased(join);
                    Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
                }
            }
        });
    }

    @Test
    public void testCancellationInsideCount() throws Exception {
        // Frames of eight rows, whose tasks check the breaker every eight pairs, and a cap of one
        // tuple per probe row, which leaves the rest of a frame with more pairs than rows to a
        // continuation.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 1);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(8, 8);
            // The residual, and the post-join filter under a probe filter, read both inputs, so the
            // operator runs them for each pair on the workers, and their first operand counts the pairs.
            for (String sql : new String[]{
                    "SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k AND (test_latched_counter() OR d1.w = t.v)",
                    "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k WHERE t.v > 0 AND (test_latched_counter() OR d1.w = t.v)",
            }) {
                assertCancellationInsideCount(sql);
            }
        });
    }

    @Test
    public void testCountOnlyTasksUnderJitProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Frames of 1_000 rows, of which the probe filter keeps all but a few: each count-only task
            // keeps far more rows than the 32 that a task's row list starts with, so a compiled filter
            // that wrote into a list it did not size would overrun it.
            execute("""
                    CREATE TABLE tj AS (
                        SELECT CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 7)::INT END k, (x % 6)::LONG lk, x v,
                            timestamp_sequence('2024-01-01', 10_000_000) ts
                        FROM long_sequence(3_000)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            sqlExecutionContext.changePageFrameSizes(1_000, 1_000);
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            final String[] queries = {
                    "SELECT tj.v, d1.w FROM tj LEFT JOIN d1 ON tj.k = d1.k WHERE tj.v > 5",
                    "SELECT tj.v, d1.w, d2.w2 FROM tj JOIN d1 ON tj.k = d1.k LEFT JOIN d2 ON tj.lk = d2.lk WHERE tj.v > 5",
            };
            for (int jitMode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(jitMode);
                final boolean isCompiled = jitMode == SqlJitMode.JIT_MODE_ENABLED && JitUtil.isJitSupported();
                for (String sql : queries) {
                    final String plan = assertCountStar(sql);
                    Assert.assertEquals(plan, isCompiled, plan.contains("Async JIT Hash Join"));
                    TestUtils.assertContains(plan, "filter: 5<v");
                    // A count from the first row and from the middle of the second frame.
                    final long rowCount = ordinaryRowCount(sql);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        for (long prefix : new long[]{0, rowCount / 2}) {
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                for (long i = 0; i < prefix; i++) {
                                    Assert.assertTrue(cursor.hasNext());
                                }
                                final RecordCursor.Counter counter = new RecordCursor.Counter();
                                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                                Assert.assertEquals(sql, rowCount - prefix, counter.get());
                                Assert.assertEquals(3, join.getProbeFrameCount());
                                if (prefix == 0) {
                                    Assert.assertEquals(sql, 3, join.getCountOnlyFrameCount());
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCountStarAndNegativeLimitOverStagesAndStacks() throws Exception {
        // A window of two frames, and a cap that continues the frames whose pairs outnumber their rows.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 1);
        assertMemoryLeak(() -> {
            createTables();
            sqlExecutionContext.changePageFrameSizes(4, 4);
            for (String sql : SHAPES) {
                // count(*) over a single join takes the fused GROUP BY while it is on, unless the join
                // has a post-join filter.
                for (boolean isFusedGroupBy : new boolean[]{false, true}) {
                    sqlExecutionContext.setParallelHashJoinGroupByEnabled(isFusedGroupBy);
                    if (sql.contains("ORDER BY")) {
                        continue;
                    }
                    final String count = "SELECT count(*) FROM (" + sql + ")";
                    final boolean isFused;
                    try (RecordCursorFactory factory = select(count)) {
                        isFused = plan(factory, sqlExecutionContext).contains("Hash Join Group By");
                        if (isFused) {
                            // The fused GROUP BY builds through the same stage, which prepares its own
                            // build.
                            Assert.assertEquals(count, ordinary(count, sqlExecutionContext), result(factory, sqlExecutionContext));
                        }
                    }
                    // The fused GROUP BY takes count(*) over one join without a residual or a post-join
                    // filter, while it is on.
                    Assert.assertEquals(count, isFusedGroupBy && isSingleJoin(sql) && !sql.contains("%"), isFused);
                    if (!isFused) {
                        assertCountStar(sql);
                    }
                }
                if (sql.contains("WHERE t.v >")) {
                    // The planner gives a negative LIMIT over a filtered probe input to the input's
                    // filter, which the operator then does not take.
                    continue;
                }
                // A negative LIMIT counts the rows, then reads them again from the top.
                for (String limit : new String[]{" LIMIT -7", " LIMIT -9, -2", " LIMIT 2, -3", " LIMIT -100_000"}) {
                    final String query = sql + limit;
                    final String expected = ordinary(query, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(query)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(query, join);
                        Assert.assertEquals(query, expected, result(factory, sqlExecutionContext));
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            // The count ran on count-only tasks alone, before any row was read.
                            Assert.assertEquals(query, join.getProbeFrameCount(), join.getCountOnlyFrameCount());
                            Assert.assertEquals(query, 0, join.getCountedTupleFrameCount());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCountOverflowAtTwoToThe63FailsTheCount() throws Exception {
        // A probe row of key 1 meets one row of u, 65_536 rows of key 1 in each of h1 to h3, 32_768 in
        // q4 and two in h5: 2^16 * 2^16 * 2^16 * 2^15 * 2 = 2^64 joined rows. The product of the
        // chains passes what a LONG holds at q4, 2^63, whose 128-bit high half is 0 and whose low half
        // reads as a negative LONG, and h5 multiplies the overflow on: every count fails. With one row
        // in h5, the 2^63 rows still fail the count from the top, and so they do after the LEFT join's
        // first row, key 2's, which leaves all of them; after the INNER join's first row, one of key
        // 1's, the 2^63 - 1 rows left fit exactly.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            // fo holds key 2, which only a LEFT join keeps, then key 1.
            execute("CREATE TABLE fo AS (SELECT (3 - x)::INT k, x v FROM long_sequence(2))");
            execute("CREATE TABLE u AS (SELECT 1 k, 7L w FROM long_sequence(1))");
            for (int i = 1; i <= 3; i++) {
                execute("CREATE TABLE h" + i + " AS (SELECT 1 k, x w FROM long_sequence(65_536))");
            }
            execute("CREATE TABLE q4 AS (SELECT 1 k, x w FROM long_sequence(32_768))");
            execute("CREATE TABLE h5 AS (SELECT 1 k, x w FROM long_sequence(2))");
            final String inner = "SELECT fo.v, u.w, h1.w w1, h2.w w2, h3.w w3, q4.w w4, h5.w w5 FROM fo JOIN u ON fo.k = u.k"
                    + " JOIN h1 ON fo.k = h1.k JOIN h2 ON fo.k = h2.k JOIN h3 ON fo.k = h3.k JOIN q4 ON fo.k = q4.k"
                    + " JOIN h5 ON fo.k = h5.k";
            final String left = inner.replace(" JOIN ", " LEFT JOIN ");
            final String[] queries = {inner, left};
            final RecordCursorFactory[] factories = new RecordCursorFactory[queries.length];
            try {
                for (int i = 0; i < queries.length; i++) {
                    factories[i] = select(queries[i]);
                }
                for (int h5Rows = 2; h5Rows > 0; h5Rows--) {
                    execute("TRUNCATE TABLE h5");
                    execute("INSERT INTO h5 SELECT 1, x FROM long_sequence(" + h5Rows + ")");
                    for (int i = 0; i < queries.length; i++) {
                        final String sql = queries[i];
                        final AsyncHashJoinRecordCursorFactory join = find(factories[i]);
                        Assert.assertNotNull(sql, join);
                        for (String query : new String[]{"SELECT count(*) FROM (" + sql + ")", sql + " LIMIT -1"}) {
                            try (
                                    RecordCursorFactory queryFactory = select(query);
                                    RecordCursor cursor = queryFactory.getCursor(sqlExecutionContext)
                            ) {
                                Assert.assertNotNull(query, find(queryFactory));
                                final String rows = printRows(cursor, queryFactory.getMetadata(), Integer.MAX_VALUE);
                                Assert.fail(query + ": expected the count to overflow, read: " + rows);
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), OVERFLOW);
                            }
                        }
                        for (int prefix = 0; prefix < 2; prefix++) {
                            final boolean isFitting = h5Rows == 1 && prefix == 1 && sql.equals(inner);
                            try (RecordCursor cursor = factories[i].getCursor(sqlExecutionContext)) {
                                if (prefix > 0) {
                                    Assert.assertTrue(sql, cursor.hasNext());
                                }
                                final RecordCursor.Counter counter = new RecordCursor.Counter();
                                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                                Assert.assertTrue(sql + ": expected the count to overflow with " + h5Rows + " rows in h5 after "
                                        + prefix + " rows, counted " + counter.get(), isFitting);
                                Assert.assertEquals(sql, Long.MAX_VALUE, counter.get());
                            } catch (CairoException e) {
                                Assert.assertFalse(sql + ": " + e.getFlyweightMessage(), isFitting);
                                TestUtils.assertContains(e.getFlyweightMessage(), OVERFLOW);
                            }
                            Assert.assertFalse(sql, join.getAtom().isSwapped());
                            assertReleased(join);
                        }
                    }
                }
                // With 32_767 rows in q4 and one in h5, 2^48 * 32_767 = 2^63 - 2^48 rows of key 1 fit, and
                // for the LEFT join one more row, key 2's, without a match. The factories that failed
                // count them.
                execute("TRUNCATE TABLE q4");
                execute("INSERT INTO q4 SELECT 1, x FROM long_sequence(32_767)");
                final long[] expectedCounts = {9_223_090_561_878_065_152L, 9_223_090_561_878_065_153L};
                for (int i = 0; i < queries.length; i++) {
                    try (RecordCursor cursor = factories[i].getCursor(sqlExecutionContext)) {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(queries[i], expectedCounts[i], counter.get());
                    }
                    assertQuery("SELECT count(*) FROM (" + queries[i] + ")")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Hash Join workers:")
                            .returns("count\n" + expectedCounts[i] + "\n");
                }
            } finally {
                Misc.free(factories);
            }
        });
    }

    @Test
    public void testCountOverflowFailsTheCount() throws Exception {
        // A probe row of key 1 meets one row of u and 65_536 rows of key 1 in each of h1 to h4:
        // 2^64 joined rows, more than a LONG count holds, which the ordinary plan would never finish
        // reading. The count fails rather than return a wrapped number, on the count-only tasks of a
        // count from the top, and on the consumer after the first row, whether the tuple it reads
        // carries the chains or a later tuple of the frame does.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            // fo holds key 2, which only a LEFT join keeps, then key 1.
            execute("CREATE TABLE fo AS (SELECT (3 - x)::INT k, x v FROM long_sequence(2))");
            execute("CREATE TABLE u AS (SELECT 1 k, 7L w FROM long_sequence(1))");
            for (int i = 1; i <= 4; i++) {
                execute("CREATE TABLE h" + i + " AS (SELECT 1 k, x w FROM long_sequence(65_536))");
            }
            final String inner = "SELECT fo.v, u.w, h1.w w1, h2.w w2, h3.w w3, h4.w w4 FROM fo JOIN u ON fo.k = u.k"
                    + " JOIN h1 ON fo.k = h1.k JOIN h2 ON fo.k = h2.k JOIN h3 ON fo.k = h3.k JOIN h4 ON fo.k = h4.k";
            final String left = inner.replace(" JOIN ", " LEFT JOIN ");
            // u is smaller than fo, so the operator hashes u and probes fo; the later stages read their
            // key off u's matches.
            final String swapped = "SELECT u.w, fo.v, h1.w w1, h2.w w2, h3.w w3, h4.w w4 FROM u JOIN fo ON u.k = fo.k"
                    + " JOIN h1 ON u.k = h1.k JOIN h2 ON u.k = h2.k JOIN h3 ON u.k = h3.k JOIN h4 ON u.k = h4.k";
            final String[] queries = {inner, left, swapped};
            final RecordCursorFactory[] factories = new RecordCursorFactory[queries.length];
            try {
                for (int i = 0; i < queries.length; i++) {
                    factories[i] = select(queries[i]);
                    assertCountOverflow(queries[i], factories[i], queries[i] == swapped);
                }
                // With one row of key 1 in h4, the counts fit: 2^48 rows of key 1, and for the LEFT
                // join one more row, key 2's, without a match. The factories that failed count them.
                execute("TRUNCATE TABLE h4");
                execute("INSERT INTO h4 VALUES (1, 1)");
                final long[] expectedCounts = {281_474_976_710_656L, 281_474_976_710_657L, 281_474_976_710_656L};
                for (int i = 0; i < queries.length; i++) {
                    try (RecordCursor cursor = factories[i].getCursor(sqlExecutionContext)) {
                        final RecordCursor.Counter counter = new RecordCursor.Counter();
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                        Assert.assertEquals(queries[i], expectedCounts[i], counter.get());
                    }
                    assertQuery("SELECT count(*) FROM (" + queries[i] + ")")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Hash Join workers:")
                            .returns("count\n" + expectedCounts[i] + "\n");
                }
            } finally {
                Misc.free(factories);
            }
        });
    }

    @Test
    public void testCountOverflowOfTheSumFailsTheCount() throws Exception {
        // Each probe row of key 1 stands for 2^16 * 2^16 * 2^15 * 2^15 = 2^62 joined rows, which a
        // LONG holds, and two of them for 2^63, which it does not. The count fails whether the two
        // rows' counts add up in one frame's task or across frames.
        assertMemoryLeak(() -> {
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            execute("CREATE TABLE fs AS (SELECT 1 k, x v FROM long_sequence(2))");
            execute("CREATE TABLE u AS (SELECT 1 k, 7L w FROM long_sequence(1))");
            execute("CREATE TABLE h1 AS (SELECT 1 k, x w FROM long_sequence(65_536))");
            execute("CREATE TABLE h2 AS (SELECT 1 k, x w FROM long_sequence(65_536))");
            execute("CREATE TABLE q3 AS (SELECT 1 k, x w FROM long_sequence(32_768))");
            execute("CREATE TABLE q4 AS (SELECT 1 k, x w FROM long_sequence(32_768))");
            final String inner = "SELECT fs.v, u.w, h1.w w1, h2.w w2, q3.w w3, q4.w w4 FROM fs JOIN u ON fs.k = u.k"
                    + " JOIN h1 ON fs.k = h1.k JOIN h2 ON fs.k = h2.k JOIN q3 ON fs.k = q3.k JOIN q4 ON fs.k = q4.k";
            for (String sql : new String[]{inner, inner.replace(" JOIN ", " LEFT JOIN ")}) {
                for (int frameRows : new int[]{2, 1}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            final RecordCursor.Counter counter = new RecordCursor.Counter();
                            try {
                                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                                Assert.fail(sql + ": expected the count to overflow, counted " + counter.get());
                            } catch (CairoException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), OVERFLOW);
                            }
                            // The two probe rows were in one frame or in two.
                            Assert.assertEquals(sql, frameRows == 2 ? 1 : 2, join.getProbeFrameCount());
                        }
                        assertReleased(join);
                    }
                    // One probe row's 2^62 rows fit.
                    assertQuery("SELECT count(*) FROM (" + sql + " WHERE fs.v = 1)")
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Hash Join workers:")
                            .returns("count\n4611686018427387904\n");
                }
            }
        });
    }

    @Test
    public void testThrowingFilterInsideCount() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // p * 1000 does not fit DECIMAL(4,0) for the one row with p = 10, whose key d1 holds, so a
            // filter that casts it fails on that row's pairs, in a frame between frames that pass.
            execute("CREATE TABLE tp AS (SELECT (x % 7)::INT k, CASE WHEN x = 46 THEN 10 ELSE x % 10 END p, timestamp_sequence('2024-01-01', 600_000_000) ts FROM long_sequence(60)) TIMESTAMP(ts) PARTITION BY HOUR");
            final String poison = "(tp.p * 1000)::DECIMAL(4,0) > d1.w::DECIMAL(4,0)";
            sqlExecutionContext.changePageFrameSizes(3, 3);
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            for (String sql : new String[]{
                    "SELECT tp.k, tp.p, d1.w FROM tp LEFT JOIN d1 ON tp.k = d1.k AND " + poison,
                    "SELECT tp.k, tp.p, d1.w FROM tp JOIN d1 ON tp.k = d1.k WHERE " + poison,
            }) {
                for (String query : new String[]{"SELECT count(*) FROM (" + sql + ")", sql + " LIMIT -3"}) {
                    final String expected = error(query, false);
                    TestUtils.assertContains(expected, "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                    Assert.assertEquals(query, expected, error(query, true));
                }
                // The count of the rows after the first one fails as well, and the factory reads its rows
                // when the filter passes them.
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), new RecordCursor.Counter());
                        Assert.fail("expected the filter to fail: " + sql);
                    } catch (CairoException | ImplicitCastException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 10000 [LONG -> DECIMAL(4,0)]");
                    }
                    assertReleased(join);
                }
            }
            assertCountStar("SELECT tp.k, tp.p, d1.w FROM tp LEFT JOIN d1 ON tp.k = d1.k AND (tp.p * 100)::DECIMAL(4,0) > d1.w::DECIMAL(4,0)");
        });
    }

    // count(*), a negative LIMIT, and the factory's count from the top and after the first row, each
    // fail with the overflow, and release what the execution held.
    private static void assertCountOverflow(String sql, RecordCursorFactory factory, boolean isSwapped) throws Exception {
        final AsyncHashJoinRecordCursorFactory join = find(factory);
        Assert.assertNotNull(sql, join);
        for (String query : new String[]{"SELECT count(*) FROM (" + sql + ")", sql + " LIMIT -1"}) {
            try (RecordCursorFactory queryFactory = select(query)) {
                Assert.assertNotNull(query, find(queryFactory));
                try (RecordCursor cursor = queryFactory.getCursor(sqlExecutionContext)) {
                    final String rows = printRows(cursor, queryFactory.getMetadata(), Integer.MAX_VALUE);
                    Assert.fail(query + ": expected the count to overflow, read: " + rows);
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), OVERFLOW);
                }
            }
        }
        for (int prefix = 0; prefix < 2; prefix++) {
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                if (prefix > 0) {
                    Assert.assertTrue(sql, cursor.hasNext());
                }
                final RecordCursor.Counter counter = new RecordCursor.Counter();
                cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                Assert.fail(sql + ": expected the count to overflow after " + prefix + " rows, counted " + counter.get());
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), OVERFLOW);
            }
            Assert.assertEquals(sql, isSwapped, join.getAtom().isSwapped());
            assertReleased(join);
        }
    }

    // Cancels a count after the first row: inside a count-only task, inside the owner's count of the
    // rest of the first frame, which its task left to a continuation, or before the count begins.
    private static void assertCancellationInsideCount(String sql) throws SqlException {
        final String expected = ordinary(sql, sqlExecutionContext);
        for (String method : new String[]{"countRows", "countFrameRest", "before"}) {
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine);
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicInteger cancelledAt = new AtomicInteger(-1);
            final TestLatchedCounterFunctionFactory.Callback callback = new TestLatchedCounterFunctionFactory.Callback() {
                @Override
                public boolean onGet(Record rec, int count) {
                    if (isArmed.get() && cancelledAt.get() < 0 && isOnStack(method)
                            && (method.equals("countFrameRest") || !isOnStack("countFrameRest"))) {
                        cancelledAt.set(count);
                        breaker.cancel();
                    }
                    return true;
                }
            };
            TestLatchedCounterFunctionFactory.reset(callback);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestLatchedCounterFunctionFactory.reset(callback);
                final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    // The first frame's task stopped at its cap.
                    Assert.assertTrue(cursor.hasNext());
                    isArmed.set(true);
                    if (method.equals("before")) {
                        cancelledAt.set(TestLatchedCounterFunctionFactory.getCount());
                        breaker.cancel();
                    }
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), new RecordCursor.Counter());
                    Assert.fail("expected cancellation: " + method);
                } catch (CairoException e) {
                    Assert.assertTrue(method + ": " + e.getMessage(), e.isCancellation());
                    TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
                } finally {
                    isArmed.set(false);
                    ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
                }
                Assert.assertTrue(method, cancelledAt.get() > 0);
                if (method.equals("before")) {
                    // No pair ran the residual after the cancellation.
                    Assert.assertEquals(cancelledAt.get(), TestLatchedCounterFunctionFactory.getCount());
                } else {
                    // The stepper observes the cancellation within the eight pairs that follow.
                    Assert.assertTrue(method + ": " + TestLatchedCounterFunctionFactory.getCount(), TestLatchedCounterFunctionFactory.getCount() < cancelledAt.get() + 8);
                }
                // The owner counted the rest of the first frame before any count-only task ran.
                Assert.assertEquals(method, 1, join.getCountedContinuationCount());
                assertReleased(join);
                Assert.assertEquals(sql, expected, result(factory, sqlExecutionContext));
            }
        }
    }

    // Counts the rows after every number of rows read, from none to all, and reads all of them again
    // after toTop(), in the ordinary plan's order. Returns, summed over the counts: the frames that
    // count-only tasks counted, the frames of tuple-carrying tasks that the counts met, and the
    // continuations that the counts counted on the owner.
    private static int[] assertCountAfterEveryRow(String sql) throws SqlException {
        final String expected = printRows(sql, false);
        final int rowCount = lineCount(expected);
        final int[] counts = new int[3];
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            final RecordMetadata metadata = factory.getMetadata();
            for (int prefix = 0; prefix <= rowCount; prefix++) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    final String head = printRows(cursor, metadata, prefix);
                    Assert.assertTrue(sql, expected.startsWith(head));
                    final RecordCursor.Counter counter = new RecordCursor.Counter();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(sql + ", after " + prefix + " rows", rowCount - prefix, counter.get());
                    // The count leaves the cursor at its end, and a second count adds nothing.
                    Assert.assertFalse(cursor.hasNext());
                    counter.clear();
                    cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
                    Assert.assertEquals(0, counter.get());
                    counts[0] += join.getCountOnlyFrameCount();
                    counts[1] += join.getCountedTupleFrameCount();
                    counts[2] += join.getCountedContinuationCount();
                    cursor.toTop();
                    Assert.assertEquals(sql + ", after " + prefix + " rows", expected, printRows(cursor, metadata, Integer.MAX_VALUE));
                }
            }
        }
        return counts;
    }

    // count(*) over the query equals the ordinary plan's count, and the operator counts every frame on
    // count-only tasks. Returns the operator's plan.
    private static String assertCountStar(String sql) throws SqlException {
        final String count = "SELECT count(*) FROM (" + sql + ")";
        final String plan = assertParallelHashJoin(count, sqlExecutionContext);
        TestUtils.assertContains(plan, "Count");
        Assert.assertEquals(count, "timestamp: -1\ncount\n" + ordinaryRowCount(sql) + ":LONG\n\n", ordinary(count, sqlExecutionContext));
        try (RecordCursorFactory factory = select(count)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(count, join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(count, ordinaryRowCount(sql), cursor.getRecord().getLong(0));
                Assert.assertEquals(count, join.getProbeFrameCount(), join.getCountOnlyFrameCount());
                Assert.assertEquals(count, 0, join.getCountedTupleFrameCount());
                Assert.assertEquals(count, 0, join.getCountedContinuationCount());
            }
        }
        return plan;
    }

    // The failure released every slot, reader, build and tuple.
    private static void assertReleased(AsyncHashJoinRecordCursorFactory join) {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
            Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    private static void createTables() throws Exception {
        // t, the fact table: a NULL INT key every eleventh row, and keys that some dimensions lack.
        execute("""
                CREATE TABLE t AS (
                    SELECT
                        CASE WHEN x % 11 = 0 THEN NULL ELSE (x % 7)::INT END k,
                        (x % 6)::LONG lk,
                        ('s' || (x % 4))::SYMBOL sym,
                        x v,
                        timestamp_sequence('2024-01-01', 600_000_000) ts
                    FROM long_sequence(60)
                ) TIMESTAMP(ts) PARTITION BY HOUR
                """);
        // d1 repeats its INT keys, NULL among them, and lacks keys 5 and 6; its symbols are t's texts,
        // and one more.
        execute("""
                CREATE TABLE d1 AS (
                    SELECT CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END k, x w, ('s' || (5 - x % 5))::SYMBOL s
                    FROM long_sequence(20)
                )
                """);
        // d2 repeats its LONG keys, which stage into a map, and lacks key 0.
        execute("CREATE TABLE d2 AS (SELECT 1 + x % 5 lk, x * 10 w2 FROM long_sequence(15))");
        // d3 holds one row per SYMBOL, in another order than t's dictionary, and lacks s0.
        execute("CREATE TABLE d3 AS (SELECT ('s' || (5 - x))::SYMBOL s, x * 100 w3 FROM long_sequence(4))");
        // u1 holds one row per key, and lacks keys 5 and 6.
        execute("CREATE TABLE u1 AS (SELECT (x - 1)::INT k, x * 3 w FROM long_sequence(5))");
        // m holds three rows of key 1, two of key 3 and one of keys 2, 4 and 5.
        execute("CREATE TABLE m AS (SELECT CASE WHEN x <= 3 THEN 1 WHEN x = 4 THEN 2 WHEN x <= 6 THEN 3 ELSE x - 3 END::INT k, x w FROM long_sequence(8))");
    }

    // The error that counting the query's rows ends with, under the operator or under the ordinary plan.
    private static String error(String sql, boolean isParallel) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(sql, isParallel, find(factory) != null);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.fail("expected the filter to fail: " + sql);
            } catch (CairoException | ImplicitCastException e) {
                return e.getPosition() + ": " + e.getFlyweightMessage();
            }
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
        return null;
    }

    private static boolean isOnStack(String methodName) {
        return StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(ATOM) && frame.getMethodName().equals(methodName)));
    }

    // True for a query of one join, whose count(*) the fused GROUP BY takes while it is on.
    private static boolean isSingleJoin(String sql) {
        return sql.indexOf(" JOIN ") == sql.lastIndexOf(" JOIN ");
    }

    private static int lineCount(String text) {
        int count = 0;
        for (int i = 0, n = text.length(); i < n; i++) {
            if (text.charAt(i) == '\n') {
                count++;
            }
        }
        return count;
    }

    // The rows of the query's ordinary plan.
    private static long ordinaryRowCount(String sql) throws SqlException {
        return lineCount(printRows(sql, false));
    }

    // The query's rows, as CursorPrinter prints them, under the operator or under the ordinary plan.
    private static String printRows(String sql, boolean isParallel) throws SqlException {
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        try (RecordCursorFactory factory = select(sql); RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(sql, isParallel, find(factory) != null);
            return printRows(cursor, factory.getMetadata(), Integer.MAX_VALUE);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(true);
        }
    }

    // Up to the given number of the cursor's next rows, as CursorPrinter prints them.
    private static String printRows(RecordCursor cursor, RecordMetadata metadata, long maxRows) {
        final StringSink sink = new StringSink();
        for (long rows = 0; rows < maxRows && cursor.hasNext(); rows++) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    /**
     * Cancels the query at the first check of its state that the consumer makes before it walks a
     * chain to count it, and counts those checks. A breaker of a class of its own is thread safe, so
     * the cursor consults it directly rather than through a copy.
     */
    private static class ConsumerChainCountBreaker extends AtomicBooleanCircuitBreaker {
        private int chainWalkCheckCount;

        private ConsumerChainCountBreaker() {
            super(engine);
        }

        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
            if (StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getClassName().equals(CURSOR) && frame.getMethodName().equals("countChain")))) {
                chainWalkCheckCount++;
                cancel();
            }
            super.statefulThrowExceptionIfTrippedTimeThrottled();
        }
    }

    /**
     * Cancels the query at the first check of its state that a stepper makes while it walks a deferred
     * chain to count it. A breaker of a class of its own is thread safe, so the tasks consult it
     * directly rather than through a copy.
     */
    private static class ChainWalkBreaker extends AtomicBooleanCircuitBreaker {
        private volatile boolean isCancelledInChainWalk;

        private ChainWalkBreaker() {
            super(engine);
        }

        @Override
        public int getState() {
            if (!isCancelledInChainWalk && StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("emitChain")))) {
                isCancelledInChainWalk = true;
                cancel();
            }
            return super.getState();
        }

        // A time-throttled check between two of its periods checks nothing, as a network breaker's
        // does, so the count meets the cancellation only through the frames it collects.
        @Override
        public void statefulThrowExceptionIfTrippedTimeThrottled() {
        }
    }
}
