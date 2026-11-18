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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.std.Chars;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * A parallel hash join under a LIMIT that the query states: the join takes the LIMIT, of its own
 * query or of a query over it whose operators read at most one row of the join per row they
 * return, through projections, sub-queries, CTEs, the master side of ASOF and LT joins, UNION ALL
 * branches and runtime-constant filters, and an execution under a positive one first dispatches
 * only the leading probe frames whose rows reach it, the last of them joining only the rows the
 * LIMIT still needs; the consumer continues that
 * frame itself, in chunks that double, if it reads further, and only then opens the full window. The
 * rows always equal the ordinary plan's, in order: the LIMIT stays with its own factory. A range
 * {@code LIMIT lo, hi} of no negative bound reads at most the larger bound's rows, and narrows the
 * first window to them as a positive LIMIT of as many rows does. A NULL, zero or negative LIMIT, or a
 * range with a negative bound, leaves the execution as it was. The join reads the
 * LIMIT once the SELECT list compiled, so an untyped bind variable that both use takes the type that
 * the SELECT list gives it, as on the ordinary plan.
 * <p>
 * The probe table t holds 64 rows in one partition, cut into frames of four rows: sixteen frames,
 * which a full window dispatches at once.
 */
public class AsyncHashJoinLimitTest extends AbstractCairoTest {
    private static final int FRAME_COUNT = 16;
    private static final int FRAME_ROWS = 4;
    // A join of t whose every probe row meets one row of d1, so that n rows of it need the first
    // ceil(n / 4) frames; and the same join with t's designated timestamp.
    private static final String JOIN_D1 = "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k";
    private static final String JOIN_D1_TS = "SELECT t.v, d1.w, t.ts FROM t JOIN d1 ON t.k = d1.k";
    // A join of t whose rows are t's of key 3, every fourth one from the third on: 16 rows.
    private static final String JOIN_M = "SELECT t.v, m.w FROM t JOIN m ON t.k = m.k";
    // The operator's EXPLAIN node, with or without a compiled probe filter; the fused GROUP BY's
    // node does not end so.
    private static final String OPERATOR = "Hash Join workers:";
    // The joins of the stars: LEFT joins, deferred chains, residuals, and an INNER join that drops
    // the probe rows of key 0.
    private static final String[] STAR_JOINS = {
            " FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k",
            " FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 % 2 = 0 LEFT JOIN d3 ON t.k = d3.k",
            " FROM t JOIN d2 ON t.k = d2.k JOIN d1 ON t.k = d1.k",
            " FROM t JOIN d3 ON t.k = d3.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 > t.v",
    };
    // The stars, each projecting some of its columns.
    private static final String[] STARS = {
            "SELECT t.v, d1.w, d2.w2" + STAR_JOINS[0],
            "SELECT t.v, d1.w, d2.w2, d3.w3" + STAR_JOINS[1],
            "SELECT t.v, d2.w2, d1.w" + STAR_JOINS[2],
            "SELECT t.v, d3.w3, d2.w2" + STAR_JOINS[3],
    };

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
        // The default dispatch limit follows the host's query workers, two on a two-core host, so
        // the tests pin one that lets a full window hold every frame.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, FRAME_COUNT);
    }

    @After
    public void restorePageFrameSizes() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBindVariableLimitPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = STARS[0] + " LIMIT :lim";
            bindVariableService.setLong("lim", 1);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // One factory, executed under each value: the first window follows each execution's.
                final long[] limits = {2, 100, -2, Numbers.LONG_NULL, 5, 0};
                // The frames a first row needs: one for two rows, two for five; a LIMIT of more rows than
                // a full window holds, or none, dispatches a full window.
                final int[] firstWindows = {1, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, 2, FRAME_COUNT};
                for (int i = 0; i < limits.length; i++) {
                    bindVariableService.setLong("lim", limits[i]);
                    final String label = "LIMIT " + limits[i];
                    final String expected = ordinaryRows(sql, sqlExecutionContext);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                        cursor.toTop();
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                    }
                    // The join's own cursor, under the same LIMIT, returns its first row from the frames
                    // that the LIMIT needs, and continues the capped frame, if any, to read every row.
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(label, firstWindows[i], join.getDispatchedFrameCount());
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertEquals(label, firstWindows[i] < FRAME_COUNT, join.getAtom().getContinuationCount() > 0);
                    }
                }
            }
        });
    }

    @Test
    public void testBindVariableLimitPerExecutionThroughSubQueryAsofAndUnionAll() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // One factory of each shape, executed under each value of an INT and of a LONG $1: the
            // first join's first row comes with the frames that the execution's LIMIT needs, one for
            // one row, two for five, five for seventeen; LIMIT 0 reads no row, and a negative or NULL
            // LIMIT, or one of more rows than a full window holds, dispatches a full window.
            final String[] shapes = {
                    "SELECT * FROM (" + JOIN_D1 + ") LIMIT $1",
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) LIMIT $1",
                    JOIN_D1 + " UNION ALL " + JOIN_D1 + " LIMIT $1",
            };
            final int[] firstWindows = {1, 2, 5, 0, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT};
            for (String sql : shapes) {
                for (int bindType : new int[]{ColumnType.INT, ColumnType.LONG}) {
                    final boolean isInt = bindType == ColumnType.INT;
                    final long[] limits = {1, 5, 17, 0, -1, isInt ? Numbers.INT_NULL : Numbers.LONG_NULL, isInt ? Integer.MAX_VALUE : Long.MAX_VALUE};
                    bindVariableService.clear();
                    try {
                        setBindLimit(bindType, 1);
                        try (RecordCursorFactory factory = select(sql)) {
                            final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, sqlExecutionContext);
                            Assert.assertTrue(sql, joins.size() > 0);
                            for (int i = 0; i < limits.length; i++) {
                                setBindLimit(bindType, limits[i]);
                                final String label = sql + ", " + ColumnType.nameOf(bindType) + " $1 = " + limits[i];
                                final String expected = ordinaryRows(sql, sqlExecutionContext);
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                                    cursor.toTop();
                                    Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                                }
                                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                    Assert.assertEquals(label, limits[i] != 0, cursor.hasNext());
                                    Assert.assertEquals(label, firstWindows[i], joins.getQuick(0).getDispatchedFrameCount());
                                }
                            }
                        }
                    } finally {
                        bindVariableService.clear();
                    }
                }
            }
        });
    }

    @Test
    public void testCachedFactoryAcrossInsertTruncateAndAlterUnderAnOuterLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A cached factory of each shape, executed while t holds rows, once it holds more, once
            // TRUNCATE emptied it and once it holds rows again: the first join's first row comes with
            // the first frame alone, and an empty t dispatches none. A new column makes the factory
            // stale, and the new compile takes the LIMIT again.
            for (String sql : new String[]{
                    "SELECT * FROM (" + JOIN_D1 + ") LIMIT 3",
                    JOIN_D1 + " UNION ALL " + JOIN_D1 + " LIMIT 3",
            }) {
                RecordCursorFactory factory = select(sql);
                try {
                    assertFirstJoinDispatched(factory, sql, 1);
                    execute("INSERT INTO t SELECT (x % 4)::INT, x + 64, timestamp_sequence('2024-01-02', 60_000_000) FROM long_sequence(8)");
                    assertFirstJoinDispatched(factory, sql, 1);
                    execute("TRUNCATE TABLE t");
                    assertFirstJoinDispatched(factory, sql, 0);
                    execute("INSERT INTO t SELECT (x % 4)::INT, x, timestamp_sequence('2024-01-01', 60_000_000) FROM long_sequence(64)");
                    assertFirstJoinDispatched(factory, sql, 1);
                    execute("ALTER TABLE t ADD COLUMN extra INT");
                    try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected a stale factory: " + sql);
                    } catch (TableReferenceOutOfDateException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "cached query plan cannot be used because table schema has changed");
                    }
                    factory = Misc.free(factory);
                    factory = select(sql);
                    assertFirstJoinDispatched(factory, sql, 1);
                    execute("ALTER TABLE t DROP COLUMN extra");
                } finally {
                    Misc.free(factory);
                }
            }
        });
    }

    @Test
    public void testCalculateSizeAndToTopInsideTheCappedFrame() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                final String sql = star + " LIMIT 3";
                // The join's own rows, which the ordinary plan's outermost join returns.
                final String all = ordinaryJoinRows(sql);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    // The join's own cursor returns every row: the LIMIT stays above it, and narrows only
                    // the first window, whose capped frame a count after the first row counts to its end.
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(star, 1, join.getDispatchedFrameCount());
                        Assert.assertTrue(star, join.getAtom().getRing().getResumePosition(0) > 0);
                        Assert.assertEquals(star, countLines(all) - 1, size(cursor));
                        Assert.assertTrue(star, join.getCountedContinuationCount() > 0);
                        cursor.toTop();
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(star, 1, join.getDispatchedFrameCount());
                        cursor.toTop();
                        Assert.assertEquals(star, all, printRows(cursor, join.getMetadata()));
                    }
                }
            }
        });
    }

    @Test
    public void testCancelledInsideTheCappedFrameAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // The LIMIT of the query, of a sub-query over the join, and of an ASOF join of which the
            // join is the master, each over the same star of LEFT joins.
            for (String sql : new String[]{
                    STARS[0] + " LIMIT 3",
                    "SELECT * FROM (" + STARS[0] + ") LIMIT 3",
                    "SELECT t.v, d1.w, d2.w2, q.p FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k ASOF JOIN q ON (k) LIMIT 3",
            }) {
                assertCancelledInsideTheCappedFrameAndReuse(sql);
            }
        });
    }

    @Test
    public void testExplainWithUntypedBindVariableSharedWithTheLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String select = "SELECT left(t.v::VARCHAR, $1) y, d1.w FROM t JOIN d1 ON t.k = d1.k";
            // The LIMIT of the join's own query, of a sub-query over the join and of a UNION ALL of
            // joins, whose SELECT lists all read $1.
            for (String sql : new String[]{
                    "EXPLAIN " + select + " LIMIT $1",
                    "EXPLAIN SELECT * FROM (" + select + ") LIMIT $1",
                    "EXPLAIN " + select + " UNION ALL " + select + " LIMIT $1",
            }) {
                final String ordinary = explainUntyped(sql, false);
                final String parallel = explainUntyped(sql, true);
                TestUtils.assertContains(sql, parallel, "Async Hash Join workers:");
                // The LIMIT reads $1 as the INT that left(VARCHAR, INT) made it, as on the ordinary plan.
                final String limit = parallel.substring(0, parallel.indexOf('\n'));
                Assert.assertEquals(sql, ordinary.substring(0, ordinary.indexOf('\n')), limit);
                TestUtils.assertContains(sql, limit, "$0::int");
            }
        });
    }

    @Test
    public void testFailedCompileAfterTheHandOverFreesTheJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The LIMIT of a sub-query reaches the joins of its UNION ALL, and then the join of the
            // enclosing query's other branch fails to construct: the failed compile frees the joins
            // that took the LIMIT, with their parses of it.
            final String sql = "SELECT * FROM (" + JOIN_D1 + " UNION ALL " + JOIN_D1 + " LIMIT $1) UNION ALL " + JOIN_D1;
            bindVariableService.clear();
            try {
                bindVariableService.setInt(0, 5);
                AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"), 2);
                try {
                    assertExceptionNoLeakCheck(sql, 0, "injected construction failure");
                } finally {
                    AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
                }
                // Compiled again, the first join takes the sub-query's LIMIT, whose five rows need its
                // first two frames; the join of the enclosing query's other branch holds no LIMIT, and
                // reads every frame.
                assertDispatched(sql, new int[]{2, 0, 0}, new int[]{2, 0, FRAME_COUNT});
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testInvalidLimitFailsAsTheOrdinaryPlanDoes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String limit : new String[]{" LIMIT 'abc'", " LIMIT true", " LIMIT 1.5", " LIMIT now()", " LIMIT k"}) {
                final String sql = STARS[0] + limit;
                // The ordinary plan's error is the oracle: the LIMIT's own factory reads the LIMIT
                // first and fails as the ordinary plan's does, before the join reads it, leaking
                // nothing.
                sqlExecutionContext.setParallelHashJoinEnabled(false);
                final SqlException expected = Assert.assertThrows(sql, SqlException.class, () -> select(sql).close());
                sqlExecutionContext.setParallelHashJoinEnabled(true);
                final SqlException actual = Assert.assertThrows(sql, SqlException.class, () -> select(sql).close());
                Assert.assertEquals(sql, expected.getPosition(), actual.getPosition());
                Assert.assertEquals(sql, expected.getFlyweightMessage().toString(), actual.getFlyweightMessage().toString());
            }
            // The LIMIT of a sub-query over the join and of a UNION ALL of joins, which reaches the
            // joins, fails as on the ordinary plan, and so does a LIMIT that holds a sub-query.
            createLimitShapeTables();
            final String[] sqls = {
                    "SELECT * FROM (" + JOIN_D1 + ") LIMIT 1.5",
                    JOIN_D1 + " UNION ALL " + JOIN_D1 + " LIMIT 'abc'",
                    "SELECT * FROM (" + JOIN_D1 + ") LIMIT (SELECT 3)",
            };
            final String[] errors = {
                    "[" + sqls[0].indexOf("1.5") + "] invalid type: DOUBLE",
                    "[" + sqls[1].indexOf("'abc'") + "] invalid type: STRING",
                    "[" + sqls[2].indexOf("SELECT 3") + "] LIMIT expressions must be convertible to INT",
            };
            for (int i = 0; i < sqls.length; i++) {
                final String sql = sqls[i];
                for (boolean isParallel : new boolean[]{false, true}) {
                    sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
                    final SqlException e = Assert.assertThrows(sql, SqlException.class, () -> select(sql).close());
                    Assert.assertEquals(sql, errors[i], "[" + e.getPosition() + "] " + e.getFlyweightMessage());
                }
            }
            // $1, which the SELECT list makes a DOUBLE, fails the LIMIT of an ASOF join of which the
            // join is the master, at the LIMIT's $1, on every plan.
            final String asOf = "SELECT t.v::DOUBLE + $1 y, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q LIMIT $1";
            assertUntypedBindVariable(asOf, true, "[" + asOf.lastIndexOf("$1") + "] invalid type: DOUBLE");
            // The tables still serve the join.
            assertParallelHashJoin(STARS[0] + " LIMIT 3", sqlExecutionContext);
            assertParallelHashJoin("SELECT * FROM (" + JOIN_D1 + ") LIMIT 3", sqlExecutionContext);
        });
    }

    @Test
    public void testLimitDispatchesTheFirstWindowOnly() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                // Without a LIMIT the first row comes with a full window of frames in flight.
                try (RecordCursorFactory factory = select(star)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(star, FRAME_COUNT, join.getDispatchedFrameCount());
                        Assert.assertEquals(star, FRAME_COUNT * FRAME_ROWS, join.getAtom().getJoinedProbeRowCount());
                    }
                }
                // LIMIT 3 dispatches the first frame alone, whose task joins three of its rows.
                for (int limit : new int[]{1, 3, FRAME_ROWS}) {
                    final String sql = star + " LIMIT " + limit;
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                            Assert.assertEquals(sql, limit, join.getAtom().getJoinedProbeRowCount());
                            //noinspection StatementWithEmptyBody
                            while (cursor.hasNext()) {
                            }
                            // The rows the LIMIT returns came from the first frame's capped task.
                            Assert.assertEquals(sql, 1, join.getDispatchedFrameCount());
                            Assert.assertEquals(sql, limit, join.getAtom().getJoinedProbeRowCount());
                            Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLimitHandOverStopsAtTheInnerLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The join takes the sub-query's LIMIT 9, which the optimiser also advises the join's
            // model, and the outer LIMIT 1 stops at the LIMIT 9's own factory: no other LIMIT
            // reaches the join.
            final String nested = "SELECT * FROM (SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT 9) LIMIT 1";
            Assert.assertEquals(nested, "9", joinModelLimitAdvice(nested));
            try (RecordCursorFactory factory = select(nested)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(nested, join);
                // The join holds LIMIT 9: its first row comes with the first three frames, those
                // that nine rows need.
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(nested, cursor.hasNext());
                    Assert.assertEquals(nested, 3, join.getDispatchedFrameCount());
                }
            }
        });
    }

    @Test
    public void testLimitOverSeveralFrames() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                // Ten rows take the first two frames whole and two rows of the third.
                final String sql = star + " LIMIT 10";
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(sql, 3, join.getDispatchedFrameCount());
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.assertEquals(sql, 3, join.getDispatchedFrameCount());
                        // The owner joins a frame when it reads it, so the frames it did not need to
                        // read joined none of their rows.
                        Assert.assertTrue(sql, join.getAtom().getJoinedProbeRowCount() <= 10);
                    }
                }
                // A first window wider than the ring's leaves the execution as it was: no task stops
                // short of its frame's end, which the join's own cursor, reading every row, shows.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(sql, 2, join.getDispatchedFrameCount());
                    }
                    Assert.assertEquals(sql, ordinaryRows(sql, sqlExecutionContext), printRows(factory));
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(sql, ordinaryJoinRows(sql), printRows(cursor, join.getMetadata()));
                    }
                    Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                } finally {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, FRAME_COUNT);
                }
            }
        });
    }

    @Test
    public void testLimitOverAFanOutJoinStaysWithinTheWindowOfTheTaskCap() throws Exception {
        // Each probe row meets d2's two rows of its key, which the post-join filter keeps, so a cap
        // of eight tuples holds a frame's four rows, and a ring budget of 256 bytes two such tasks'
        // tuples of two longs, 128 bytes each, where the frames' rows, 64 bytes, would let four
        // frames in.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, 8);
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TUPLE_RING_MAX_SIZE, 256);
        assertMemoryLeak(() -> {
            createTables();
            final String join = "SELECT t.v, d2.w2 FROM t JOIN d2 ON t.k = d2.k WHERE d2.w2 > t.v";
            // Without a LIMIT, and under one that the rows of the window's two frames do not reach,
            // the first row comes with the window's two frames, whose tasks join every row; LIMIT 3
            // needs three rows of frame 0, whose task joins them alone.
            final Object[][] queriesDispatchedAndRows = {
                    {join, 2, 128},
                    {join + " LIMIT 10", 2, 10},
                    {join + " LIMIT 3", 1, 3},
            };
            for (Object[] query : queriesDispatchedAndRows) {
                final String sql = (String) query[0];
                final int dispatched = (int) query[1];
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory fanOutJoin = find(factory);
                    Assert.assertNotNull(sql, fanOutJoin);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 2, fanOutJoin.getAtom().getRing().getDispatchLimit());
                        Assert.assertEquals(sql, dispatched, fanOutJoin.getDispatchedFrameCount());
                        if (dispatched == 1) {
                            Assert.assertEquals(sql, 3, fanOutJoin.getAtom().getJoinedProbeRowCount());
                        }
                        int rows = 1;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(sql, (int) query[2], rows);
                        Assert.assertEquals(sql, 0, fanOutJoin.getAtom().getContinuationCount());
                    }
                }
            }
        });
    }

    @Test
    public void testLimitOverAProbeFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE m AS (SELECT 3 k, 33 w FROM long_sequence(1))");
            // A filter of the left input in a sub-query, which the join steals, keeps two rows of each
            // frame; the LIMIT caps the rows that the filter keeps.
            final String even = "(SELECT * FROM t WHERE v % 2 = 0) t";
            final String odd = "(SELECT * FROM t WHERE v % 2 = 1) t";
            for (boolean isCopy : new boolean[]{true, false}) {
                // The stages' hidden copies from the first task, and the shared template, as on a JVM
                // without hidden classes.
                useSteppers(isCopy);
                try {
                    // One kept row of the first frame yields the two rows of its chain.
                    final String sql = "SELECT t.v, d1.w, d2.w2 FROM " + even + " LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k LIMIT 1";
                    TestUtils.assertContains(assertParallelHashJoin(sql, sqlExecutionContext), "filter: v%2=0");
                    assertFirstRow(sql, 1, 1, 0, 1);
                    assertStepperRuns(sql, sqlExecutionContext, isCopy);
                    // The first kept row, of key 1, misses m; the chunk of one more, of key 3, meets it.
                    final String inner = "SELECT t.v, m.w FROM " + odd + " JOIN m ON t.k = m.k LIMIT 1";
                    assertParallelHashJoin(inner, sqlExecutionContext);
                    assertFirstRow(inner, 1, 2, 1, 1);
                    assertStepperRuns(inner, sqlExecutionContext, isCopy);
                    for (int limit : new int[]{1, 2, 3, 5, 9, 31, 32, 33}) {
                        final String residual = "SELECT t.v, d1.w, d2.w2 FROM " + odd + " LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k AND d2.w2 > t.v LIMIT " + limit;
                        assertParallelHashJoin(residual, sqlExecutionContext);
                        assertStepperRuns(residual, sqlExecutionContext, isCopy);
                        assertParallelHashJoin("SELECT t.v, m.w FROM " + even + " JOIN m ON t.k = m.k LIMIT " + limit, sqlExecutionContext);
                    }
                } finally {
                    restoreSteppers();
                }
            }
        });
    }

    @Test
    public void testLimitReachesOnlyTheJoinItApplies() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A window function may read every joined row, so the optimiser gives the join model no
            // LIMIT, and the LIMIT stops at the window's factory: the join's first row comes with
            // the full window.
            final String window = "SELECT t.v, row_number() OVER () rn FROM t JOIN d1 ON t.k = d1.k LIMIT 1";
            Assert.assertNull(window, joinModelLimitAdvice(window));
            assertFirstWindow(window, FRAME_COUNT);
            assertParallelHashJoin(window, sqlExecutionContext);
            // The join takes the LIMIT of the sub-query that states it, LIMIT 9, which the optimiser
            // gives the join model too, and not the outer LIMIT 1, which counts the sub-query's rows:
            // the join's first row comes with the first three frames, those that nine rows need.
            final String nested = "SELECT * FROM (SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT 9) LIMIT 1";
            Assert.assertEquals(nested, "9", joinModelLimitAdvice(nested));
            assertFirstWindow(nested, 3);
            assertParallelHashJoin(nested, sqlExecutionContext);
            // A LIMIT with an upper bound alone reads the first five rows: the join's first row comes
            // with the first two frames, those that five rows need.
            final String upperBound = "SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT , 5";
            Assert.assertEquals(upperBound, "null, 5", joinModelLimitAdvice(upperBound));
            assertFirstWindow(upperBound, 2);
            assertParallelHashJoin(upperBound, sqlExecutionContext);
            // A range in a sub-query reaches the join; the outer LIMIT does not, whether it states
            // a lower bound or only an upper one.
            for (String outer : new String[]{" LIMIT 1", " LIMIT , 1"}) {
                final String nestedRange = "SELECT * FROM (SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT 2, 9)" + outer;
                Assert.assertEquals(nestedRange, "2, 9", joinModelLimitAdvice(nestedRange));
                assertFirstWindow(nestedRange, 3);
                assertParallelHashJoin(nestedRange, sqlExecutionContext);
                final String nestedUpperBound = "SELECT * FROM (SELECT t.v, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT , 9)" + outer;
                Assert.assertEquals(nestedUpperBound, "null, 9", joinModelLimitAdvice(nestedUpperBound));
                assertFirstWindow(nestedUpperBound, 3);
                assertParallelHashJoin(nestedUpperBound, sqlExecutionContext);
            }
        });
    }

    @Test
    public void testLimitReachesEachUnionAllBranch() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            final String empty = JOIN_D1 + " WHERE t.v < 0";
            // [query, the frames that each join had dispatched at the query's first row, null for no
            // row, and at its end]. Each branch reads at most the LIMIT's rows, and its join takes the
            // LIMIT: a branch whose rows the branches before it left unread never builds, an empty
            // branch reads every frame, and the branch after it needs one frame for three rows, or,
            // after a branch of 16 rows, five frames for eighteen, of which it reads two rows.
            final Object[][] cases = {
                    {JOIN_D1 + " UNION ALL " + JOIN_D1 + " LIMIT 3", new int[]{1, 0}, new int[]{1, 0}},
                    {empty + " UNION ALL " + JOIN_D1 + " LIMIT 3", new int[]{FRAME_COUNT, 1}, new int[]{FRAME_COUNT, 1}},
                    {JOIN_M + " UNION ALL " + JOIN_D1 + " LIMIT 18", new int[]{5, 0}, new int[]{FRAME_COUNT, 5}},
                    // A range reads at most its larger bound's rows: the second branch's two rows
                    // after the eighteen that it skips.
                    {JOIN_M + " UNION ALL " + JOIN_D1 + " LIMIT 18, 20", new int[]{FRAME_COUNT, 5}, new int[]{FRAME_COUNT, 5}},
                    // Three branches, the first two of which a nested UNION ALL holds.
                    {JOIN_D1 + " UNION ALL " + JOIN_D1 + " UNION ALL " + JOIN_D1 + " LIMIT 3", new int[]{1, 0, 0}, new int[]{1, 0, 0}},
                    {empty + " UNION ALL " + JOIN_M + " UNION ALL " + JOIN_D1 + " LIMIT 18", new int[]{FRAME_COUNT, 5, 0}, new int[]{FRAME_COUNT, FRAME_COUNT, 5}},
                    // SYMBOL columns, which the union casts back to SYMBOL over its branches.
                    {"SELECT t.v, ds.s FROM t JOIN ds ON t.k = ds.k UNION ALL SELECT t.v, ds.s FROM t JOIN ds ON t.k = ds.k LIMIT 3", new int[]{1, 0}, new int[]{1, 0}},
                    {"SELECT t.v, ds.s FROM t JOIN ds ON t.k = ds.k WHERE t.v < 0 UNION ALL SELECT t.v, ds.s FROM t JOIN ds ON t.k = ds.k LIMIT 3", new int[]{FRAME_COUNT, 1}, new int[]{FRAME_COUNT, 1}},
                    // A branch keeps its own LIMIT 4, and the outer LIMIT 5 reaches the other one.
                    {"SELECT * FROM (" + JOIN_D1 + " LIMIT 4) UNION ALL " + JOIN_D1 + " LIMIT 5", new int[]{1, 0}, new int[]{1, 2}},
                    // The UNION ALL in a sub-query, and of a CTE, which each branch reads.
                    {"SELECT * FROM (" + JOIN_D1 + " UNION ALL " + JOIN_D1 + ") LIMIT 3", new int[]{1, 0}, new int[]{1, 0}},
                    {"SELECT * FROM (" + empty + " UNION ALL " + JOIN_D1 + ") LIMIT 3", new int[]{FRAME_COUNT, 1}, new int[]{FRAME_COUNT, 1}},
                    {"WITH j AS (" + JOIN_D1 + ") SELECT * FROM j UNION ALL SELECT * FROM j LIMIT 3", new int[]{1, 0}, new int[]{1, 0}},
            };
            for (Object[] c : cases) {
                assertDispatched((String) c[0], (int[]) c[1], (int[]) c[2]);
            }
        });
    }

    @Test
    public void testLimitReachesTheJoinThroughARuntimeConstantFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A WHERE that holds no column reads once per execution, before the join: true hands the
            // join's own cursor over, whose first row the first frame holds, and false returns no row
            // without opening the join.
            assertDispatched(JOIN_D1 + " WHERE now() > '2020-01-01'::TIMESTAMP LIMIT 3", new int[]{1}, new int[]{1});
            assertDispatched(JOIN_D1 + " WHERE now() < '2020-01-01'::TIMESTAMP LIMIT 3", null, new int[]{0});
            // A boolean $1, which each execution of one factory reads.
            final String sql = JOIN_D1 + " WHERE $1 LIMIT 3";
            bindVariableService.clear();
            try {
                bindVariableService.setBoolean(0, true);
                try (RecordCursorFactory factory = select(sql)) {
                    for (boolean isTrue : new boolean[]{true, false, true}) {
                        bindVariableService.setBoolean(0, isTrue);
                        assertFirstJoinDispatched(factory, sql, isTrue ? 1 : 0);
                    }
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testLimitReachesTheJoinThroughSubQueriesAndCtes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // Each of t's rows meets one row of d1, so n rows need the first ceil(n / 4) frames, up to
            // a full window, the last of them joining the rows still needed: the query's first row, and
            // its last, come with those frames dispatched, and a LIMIT of fewer than four rows joins
            // as many probe rows. The join's own cursor reads every row, and continues the capped
            // frame that a LIMIT ends inside: four rows end at a frame's end, five one row into the
            // second frame, and 64 at the last frame's end.
            final int[] limits = {1, 3, 4, 5, 10, 17, 64, 65};
            final int[] frames = {1, 1, 1, 2, 3, 5, FRAME_COUNT, FRAME_COUNT};
            for (int i = 0; i < limits.length; i++) {
                final int limit = limits[i];
                final String sql = "SELECT * FROM (" + JOIN_D1 + ") LIMIT " + limit;
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, frames[i], join.getDispatchedFrameCount());
                        if (limit < FRAME_ROWS) {
                            Assert.assertEquals(sql, limit, join.getAtom().getJoinedProbeRowCount());
                        }
                        int rows = 1;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(sql, Math.min(limit, FRAME_COUNT * FRAME_ROWS), rows);
                        Assert.assertEquals(sql, frames[i], join.getDispatchedFrameCount());
                    }
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        int rows = 0;
                        while (cursor.hasNext()) {
                            rows++;
                        }
                        Assert.assertEquals(sql, FRAME_COUNT * FRAME_ROWS, rows);
                        final boolean isCapped = limit % FRAME_ROWS != 0 && limit < FRAME_COUNT * FRAME_ROWS;
                        Assert.assertEquals(sql, isCapped, join.getAtom().getContinuationCount() > 0);
                    }
                }
            }
            // A range reads at most its larger bound's rows, the bounds swapped when the lower one is
            // the larger; a negative bound counts rows from the end, which reads every row.
            final String[] ranges = {" LIMIT 2, 5", " LIMIT 5, 2", " LIMIT , 5", " LIMIT -5, -2", " LIMIT 2, -1"};
            final int[] rangeWindows = {2, 2, 2, FRAME_COUNT, FRAME_COUNT};
            for (int i = 0; i < ranges.length; i++) {
                final String sql = "SELECT * FROM (" + JOIN_D1 + ")" + ranges[i];
                assertParallelHashJoin(sql, sqlExecutionContext);
                assertFirstWindow(sql, rangeWindows[i]);
            }
            // Projections, at any depth, a column that the projection computes, a CTE, a view, a
            // WHERE that the join runs, as a build filter, a probe filter or a post-join filter, a
            // star and a LEFT join: the LIMIT reaches the join, whose first three rows the first
            // frame holds, but under the probe filter, whose frame keeps two of them: the third row
            // reads on into the full window.
            execute("CREATE VIEW jv AS (" + JOIN_D1 + ")");
            final Object[][] cases = {
                    {"SELECT * FROM (SELECT * FROM (" + JOIN_D1 + ")) LIMIT 3", 1},
                    {"SELECT * FROM (SELECT * FROM (SELECT * FROM (" + JOIN_D1 + "))) LIMIT 3", 1},
                    {"SELECT v + 1 x, w FROM (" + JOIN_D1 + ") LIMIT 3", 1},
                    {"SELECT * FROM (SELECT v + 1 x, w FROM (" + JOIN_D1 + ")) LIMIT 3", 1},
                    {"SELECT w, v FROM (" + JOIN_D1 + ") LIMIT 3", 1},
                    {"SELECT j.v FROM (" + JOIN_D1 + ") j LIMIT 3", 1},
                    {"WITH j AS (" + JOIN_D1 + ") SELECT * FROM j LIMIT 3", 1},
                    {"WITH j AS (" + JOIN_D1 + ") SELECT v, w FROM j LIMIT 3", 1},
                    {"SELECT * FROM jv LIMIT 3", 1},
                    {"SELECT * FROM (" + JOIN_D1 + ") WHERE w > 15 LIMIT 3", 1},
                    {"SELECT * FROM (" + JOIN_D1 + ") WHERE v > 2 LIMIT 3", FRAME_COUNT},
                    {"SELECT * FROM (" + JOIN_D1 + " WHERE d1.w > t.v) LIMIT 3", 1},
                    {"SELECT * FROM (" + STARS[2] + ") LIMIT 3", 1},
                    {"SELECT * FROM (SELECT t.v, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k) LIMIT 3", 1},
            };
            for (Object[] c : cases) {
                assertDispatched((String) c[0], new int[]{1}, new int[]{(int) c[1]});
            }
            // A sub-query's own LIMIT 9 stands between the outer LIMIT and the join, which takes the
            // LIMIT 9 alone: its first row needs the first three frames.
            for (String outer : new String[]{" LIMIT 1", " LIMIT 30"}) {
                final String sql = "SELECT * FROM (" + JOIN_D1 + " LIMIT 9)" + outer;
                assertParallelHashJoin(sql, sqlExecutionContext);
                assertFirstWindow(sql, 3);
            }
        });
    }

    @Test
    public void testLimitReachesTheJoinUnderAnOrderByOfTheDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // The join scans t in the order of t's designated timestamp, however the query names it,
            // forward or backward, so no sort stands between the LIMIT and the join, whose first three
            // rows, in either direction, the first frame holds.
            for (String sql : new String[]{
                    JOIN_D1 + " ORDER BY t.ts DESC LIMIT 3",
                    JOIN_D1 + " ORDER BY t.ts LIMIT 3",
                    "SELECT x.v, d1.w FROM t x JOIN d1 ON x.k = d1.k ORDER BY x.ts DESC LIMIT 3",
                    "SELECT t.v, d1.w, t.ts tts FROM t JOIN d1 ON t.k = d1.k ORDER BY tts DESC LIMIT 3",
                    "SELECT * FROM (" + JOIN_D1_TS + ") ORDER BY ts LIMIT 3",
                    "SELECT * FROM (" + JOIN_D1_TS + " ORDER BY t.ts DESC) LIMIT 3",
            }) {
                assertDispatched(sql, new int[]{1}, new int[]{1});
            }
        });
    }

    @Test
    public void testLimitReachesTheMasterOfAsofAndLtJoins() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // An ASOF or LT join reads one row of its master per row it returns, so the LIMIT reaches
            // the join that is its master, whose first three rows the first frame holds: keyed and
            // without a key, under a TOLERANCE, over a filtered slave, LT after ASOF, a LEFT join and
            // a star as the master, a sub-query over the ASOF join, and a sub-query as the master.
            for (String sql : new String[]{
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) LIMIT 3",
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q LIMIT 3",
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) TOLERANCE 1m LIMIT 3",
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN (SELECT * FROM q WHERE p > 10) q ON (k) LIMIT 3",
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k LT JOIN q ON (k) LIMIT 3",
                    "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k LT JOIN q LIMIT 3",
                    "SELECT t.v, d1.w, q.p, q2.p p2 FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) LT JOIN q q2 ON (k) LIMIT 3",
                    "SELECT t.v, d1.w, q.p FROM t LEFT JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) LIMIT 3",
                    "SELECT t.v, d1.w, d2.w2, q.p FROM t JOIN d1 ON t.k = d1.k JOIN d2 ON t.k = d2.k ASOF JOIN q ON (k) LIMIT 3",
                    "SELECT * FROM (SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k)) LIMIT 3",
                    "SELECT j.v, j.w, q.p FROM ((" + JOIN_D1_TS + ") TIMESTAMP(ts)) j ASOF JOIN q LIMIT 3",
            }) {
                assertDispatched(sql, new int[]{1}, new int[]{1});
            }
            // A slave without random access, a join of q, makes the ASOF and LT joins the full-fat
            // ones, which read their master as the others do; the slave join keeps a full window.
            final String slave = "((SELECT q.k, q.p, q.ts FROM q JOIN d1 ON q.k = d1.k) TIMESTAMP(ts)) q2 ON (k)";
            for (String sql : new String[]{
                    "SELECT t.v, d1.w, q2.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN " + slave + " LIMIT 3",
                    "SELECT t.v, d1.w, q2.p FROM t JOIN d1 ON t.k = d1.k LT JOIN " + slave + " LIMIT 3",
            }) {
                assertParallelHashJoin(sql, sqlExecutionContext);
                try (RecordCursorFactory factory = select(sql)) {
                    final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, sqlExecutionContext);
                    Assert.assertEquals(sql, 2, joins.size());
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, 1, joins.getQuick(0).getDispatchedFrameCount());
                    }
                    try (RecordCursor cursor = joins.getQuick(1).getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(sql, cursor.hasNext());
                        Assert.assertEquals(sql, FRAME_COUNT, joins.getQuick(1).getDispatchedFrameCount());
                    }
                }
            }
            // A range skips its lower bound's rows through the ASOF join into its master and reads at
            // most its larger bound's: nine rows need the first two frames and one row of the third.
            final String range = "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) LIMIT 5, 9";
            assertDispatched(range, new int[]{3}, new int[]{3});
            try (RecordCursorFactory factory = select(range)) {
                final AsyncHashJoinRecordCursorFactory join = findAll(factory, sqlExecutionContext).getQuick(0);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    //noinspection StatementWithEmptyBody
                    while (cursor.hasNext()) {
                    }
                    Assert.assertEquals(range, 2 * FRAME_ROWS + 1, join.getAtom().getJoinedProbeRowCount());
                    Assert.assertEquals(range, 0, join.getAtom().getContinuationCount());
                }
            }
        });
    }

    @Test
    public void testLimitStopsAtOtherOperators() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // An operator that may read more rows of the join than it returns keeps the LIMIT from
            // the join, which then holds no LIMIT: its own first row comes with a full window. A
            // window function, a CROSS JOIN, a serial join over the join, a filter that the join
            // cannot run, a sort, DISTINCT, GROUP BY, SAMPLE BY and LATEST ON over it, UNION, EXCEPT
            // and INTERSECT, a sort over a UNION ALL, the join as the slave of an ASOF join, and a
            // filter over an ASOF join of which the join is the master.
            sqlExecutionContext.setParallelHashJoinGroupByEnabled(false);
            try {
                for (String sql : new String[]{
                        "SELECT t.v, row_number() OVER () rn FROM t JOIN d1 ON t.k = d1.k LIMIT 3",
                        "SELECT * FROM (" + JOIN_D1 + ") j CROSS JOIN long_sequence(2) c LIMIT 3",
                        "SELECT t.v, d1.w, d4.z FROM t JOIN d1 ON t.k = d1.k LEFT JOIN d4 ON d1.w = d4.w LIMIT 3",
                        "SELECT * FROM (" + JOIN_D1 + ") WHERE rnd_int(0, 1, 0) >= 0 LIMIT 3",
                        "SELECT * FROM (" + JOIN_D1_TS + ") ORDER BY ts DESC LIMIT 3",
                        "SELECT * FROM (SELECT DISTINCT t.v FROM t JOIN d1 ON t.k = d1.k) LIMIT 3",
                        "SELECT t.k, count() c FROM t JOIN d1 ON t.k = d1.k GROUP BY t.k LIMIT 3",
                        "SELECT ts, count() c FROM (" + JOIN_D1_TS + ") SAMPLE BY 1h LIMIT 3",
                        "SELECT * FROM (SELECT t.v, d1.w, t.ts, t.k FROM t JOIN d1 ON t.k = d1.k) LATEST ON ts PARTITION BY k LIMIT 3",
                        JOIN_D1 + " UNION " + JOIN_D1 + " LIMIT 3",
                        JOIN_D1 + " EXCEPT " + JOIN_D1 + " WHERE t.v > 60 LIMIT 3",
                        JOIN_D1 + " INTERSECT " + JOIN_D1 + " LIMIT 3",
                        "SELECT * FROM (" + JOIN_D1_TS + " UNION ALL " + JOIN_D1_TS + ") ORDER BY ts LIMIT 3",
                        "SELECT q.p, j.w FROM q ASOF JOIN ((" + JOIN_D1_TS + ") TIMESTAMP(ts)) j LIMIT 3",
                        "SELECT t.v, d1.w, q.p FROM t JOIN d1 ON t.k = d1.k ASOF JOIN q ON (k) WHERE q.p > t.v LIMIT 3",
                }) {
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    assertFirstWindow(sql, FRAME_COUNT);
                }
            } finally {
                sqlExecutionContext.setParallelHashJoinGroupByEnabled(configuration.isSqlParallelHashJoinGroupByEnabled());
            }
        });
    }

    @Test
    public void testLimitThroughASubQueryAtDefaultFrameSizes() throws Exception {
        assertMemoryLeak(() -> {
            // 300,000 rows over three days, whose partitions the frames never span, at the default
            // frame sizes: the LIMIT over a sub-query needs the first frame alone.
            execute("CREATE TABLE big AS (SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 864_000) ts FROM long_sequence(300_000)) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE d1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))");
            final String sql = "SELECT * FROM (SELECT big.v, d1.w FROM big JOIN d1 ON big.k = d1.k) LIMIT 10";
            assertParallelHashJoin(sql, sqlExecutionContext);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue("probe frames: " + join.getProbeFrameCount(), join.getProbeFrameCount() >= 3);
                    Assert.assertEquals(1, join.getDispatchedFrameCount());
                    Assert.assertEquals(10, join.getAtom().getJoinedProbeRowCount());
                }
            }
        });
    }

    @Test
    public void testLimitThroughASubQueryInEachExecutionMode() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // A probe filter that the join steals, compiled and interpreted: LIMIT 1 caps the first
            // frame's task at one row that the filter keeps.
            final String even = "SELECT * FROM (SELECT t.v, d1.w FROM (SELECT * FROM t WHERE v % 2 = 0) t JOIN d1 ON t.k = d1.k) LIMIT 1";
            for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                sqlExecutionContext.setJitMode(mode);
                try {
                    assertParallelHashJoin(even, sqlExecutionContext);
                    assertFirstRow(even, 1, 1, 0, 1);
                } finally {
                    sqlExecutionContext.setJitMode(configuration.getSqlJitMode());
                }
            }
            // d1 is smaller than t: the INNER join swaps its inputs, as the ordinary plan does, and
            // probes t's frames, the first of which holds the three rows.
            final String swapped = "SELECT * FROM (SELECT d1.w, t.v FROM d1 JOIN t ON d1.k = t.k) LIMIT 3";
            assertDispatched(swapped, new int[]{1}, new int[]{1});
            try (RecordCursorFactory factory = select(swapped)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertTrue(join.getAtom().isSwapped());
                }
            }
            // The stages' hidden stepper copies from the first task, and the shared template, over a
            // sub-query and as the master of an ASOF join.
            for (boolean isCopy : new boolean[]{true, false}) {
                useSteppers(isCopy);
                try {
                    for (String sql : new String[]{
                            "SELECT * FROM (" + STARS[1] + ") LIMIT 5",
                            "SELECT t.v, d1.w, d2.w2, q.p FROM t LEFT JOIN d1 ON t.k = d1.k LEFT JOIN d2 ON t.k = d2.k ASOF JOIN q ON (k) LIMIT 5",
                    }) {
                        assertDispatched(sql, new int[]{2}, new int[]{2});
                        try (RecordCursorFactory factory = select(sql)) {
                            final AsyncHashJoinRecordCursorFactory join = findAll(factory, sqlExecutionContext).getQuick(0);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                //noinspection StatementWithEmptyBody
                                while (cursor.hasNext()) {
                                }
                                assertStepperRuns(join, isCopy, sql);
                            }
                        }
                    }
                } finally {
                    restoreSteppers();
                }
            }
        });
    }

    @Test
    public void testLimitThroughASubQueryOverAParquetProbe() throws Exception {
        // Row groups of four rows: t's first partition converts into sixteen Parquet frames.
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, FRAME_ROWS);
        assertMemoryLeak(() -> {
            createTables();
            // A later partition, which stays native, lets the first one convert.
            execute("INSERT INTO t VALUES (1, 65, '2024-01-02')");
            execute("ALTER TABLE t CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
            assertQuery("SELECT count() c, sum(isParquet::INT) p FROM table_partitions('t')")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            c\tp
                            2\t1
                            """);
            final String sql = "SELECT * FROM (" + JOIN_D1 + ") LIMIT 3";
            assertDispatched(sql, new int[]{1}, new int[]{1});
            // The probe's frames: the sixteen row groups and the native partition's one frame.
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(FRAME_COUNT + 1, join.getProbeFrameCount());
                }
            }
        });
    }

    @Test
    public void testLimitThroughASubQueryOverDataShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            createLimitShapeTables();
            // An empty probe has no frame to dispatch, under a sub-query and under a UNION ALL.
            execute("CREATE TABLE e (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String emptyJoin = "SELECT e.v, d1.w FROM e JOIN d1 ON e.k = d1.k";
            assertDispatched("SELECT * FROM (" + emptyJoin + ") LIMIT 3", null, new int[]{0});
            assertDispatched(emptyJoin + " UNION ALL " + emptyJoin + " LIMIT 3", null, new int[]{0, 0});
            // A probe of one row: its one frame, which the join's first window, cut for the LIMIT's
            // three rows, holds.
            execute("CREATE TABLE t1 AS (SELECT 1 k, 1L v, '2024-01-01'::TIMESTAMP ts FROM long_sequence(1)) TIMESTAMP(ts) PARTITION BY DAY");
            final String one = "SELECT * FROM (SELECT t1.v, d1.w FROM t1 JOIN d1 ON t1.k = d1.k) LIMIT 3";
            AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting();
            assertDispatched(one, new int[]{1}, new int[]{1});
            Assert.assertEquals(3, AsyncHashJoinRecordCursorFactory.takeLastFirstWindowRowsForTesting());
            // An INNER join that drops rows reads on: LIMIT 1 caps the first frame's task at one row,
            // which misses m, and the consumer goes on with one row, then two, the first of which
            // meets it: four rows joined, by the task and two chunks.
            final String inner = "SELECT * FROM (" + JOIN_M + ") LIMIT 1";
            assertParallelHashJoin(inner, sqlExecutionContext);
            assertFirstRow(inner, 1, 4, 2, 1);
            // A first window wider than the ring's leaves the execution as it was: no task stops
            // short of its frame's end.
            setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, 2);
            try {
                final String wide = "SELECT * FROM (" + JOIN_D1 + ") LIMIT 10";
                assertParallelHashJoin(wide, sqlExecutionContext);
                assertFirstWindow(wide, 2);
                try (RecordCursorFactory factory = select(wide)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(ordinaryJoinRows(wide), printRows(cursor, join.getMetadata()));
                    }
                    Assert.assertEquals(0, join.getAtom().getContinuationCount());
                }
            } finally {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_DISPATCH_LIMIT, FRAME_COUNT);
            }
        });
    }

    @Test
    public void testLimitReadsOnThroughTheContinuation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // Only t's rows of key 3, every fourth one from the third on, meet m's one row.
            execute("CREATE TABLE m AS (SELECT 3 k, 33 w FROM long_sequence(1))");
            execute("CREATE TABLE n AS (SELECT 3L v, 33 w FROM long_sequence(1))");
            final String inner = "SELECT t.v, m.w FROM t JOIN m ON t.k = m.k";
            // The optimiser gives the join the LIMIT under an ORDER BY of the designated timestamp that
            // names it as the table does.
            for (String order : new String[]{"", " ORDER BY ts DESC"}) {
                // LIMIT 1 caps the first frame's task at one row, which misses. The consumer goes on
                // with one row, then two: forward, rows 2 and 3, the third of which meets the row it
                // needs, four rows joined in all, by the task and two chunks; backward, the frame's
                // second row from its end does, two rows joined, by the task and one chunk.
                final String sql = inner + order + " LIMIT 1";
                assertParallelHashJoin(sql, sqlExecutionContext);
                if (order.isEmpty()) {
                    assertFirstRow(sql, 1, 4, 2, 1);
                } else {
                    assertFirstRow(sql, 1, 2, 1, 1);
                }
                // LIMIT 6 takes the first frame whole and caps the second at two rows; it reads on
                // through the second frame's continuation and then the full window.
                final String sql6 = inner + order + " LIMIT 6";
                assertParallelHashJoin(sql6, sqlExecutionContext);
                assertFirstRow(sql6, 2, FRAME_ROWS + 2, 0, 6);
                // Only t's third row meets n's. Forward, the capped task's two rows miss, and the
                // chunk of two more meets it; the LIMIT then reads on through the frames after the
                // first, none of which yields a row, to the end. Backward, the first frame yields
                // none, and the row comes from the last frame, of the full window.
                final String sparse = "SELECT t.v, n.w FROM t JOIN n ON t.v = n.v" + order + " LIMIT 2";
                assertParallelHashJoin(sparse, sqlExecutionContext);
                if (order.isEmpty()) {
                    assertFirstRow(sparse, 1, 4, 1, 1);
                } else {
                    assertFirstRow(sparse, FRAME_COUNT, FRAME_COUNT * FRAME_ROWS, 1, 1);
                }
            }
        });
    }

    @Test
    public void testNegativeAndZeroLimitsUnchanged() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String star : STARS) {
                // A negative bound counts rows from the end, which takes every row; a range whose
                // bounds are both zero, or equal, returns none.
                for (String limit : new String[]{
                        " LIMIT -1", " LIMIT -3", " LIMIT -100", " LIMIT 0",
                        " LIMIT -5, -2", " LIMIT -3, 0", " LIMIT 2, -1", " LIMIT 0, -60", " LIMIT 0, 0", " LIMIT , 0"
                }) {
                    final String sql = star + limit;
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(join);
                        Assert.assertEquals(sql, ordinaryRows(sql, sqlExecutionContext), printRows(factory));
                        // No task stopped short of its frame's end, which a first window under a LIMIT of
                        // one or three rows would have made it do.
                        Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
                        try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, FRAME_COUNT, join.getDispatchedFrameCount());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testParityUnderEachLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (boolean isCopy : new boolean[]{true, false}) {
                // The stages' hidden copies from the first task, and the shared template, as on a JVM
                // without hidden classes.
                useSteppers(isCopy);
                try {
                    assertParityUnderEachLimit();
                    for (String star : STARS) {
                        assertStepperRuns(star + " LIMIT 5", sqlExecutionContext, isCopy);
                        assertStepperRuns(star + " ORDER BY ts DESC LIMIT 2, 5", sqlExecutionContext, isCopy);
                    }
                } finally {
                    restoreSteppers();
                }
            }
        });
    }

    @Test
    public void testRangeLimitDispatchesTheFirstWindowOnly() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // A range reads at most its larger bound's rows, the bounds swapped when the lower one is
            // the larger, so its first window holds the frames of four rows that those rows need,
            // the last of them capped at the rows still needed, and a bound past a full window's
            // rows dispatches the full window. Each range and the positive LIMIT of its larger bound
            // dispatch the same frames.
            final String[] ranges = {" LIMIT 2, 5", " LIMIT 5, 2", " LIMIT , 5", " LIMIT 0, 3", " LIMIT 3, 3", " LIMIT 8, 12", " LIMIT 60, 70", " LIMIT 100, 10"};
            final String[] limits = {" LIMIT 5", " LIMIT 5", " LIMIT 5", " LIMIT 3", " LIMIT 3", " LIMIT 12", " LIMIT 70", " LIMIT 100"};
            final int[] firstWindows = {2, 2, 2, 1, 1, 3, FRAME_COUNT, FRAME_COUNT};
            // Whether the first window's last frame stops short of its end: 5 and 3 rows end inside a
            // frame, 12 rows at the end of the third.
            final boolean[] isCapped = {true, true, true, true, true, false, false, false};
            for (String star : STARS) {
                for (int i = 0; i < ranges.length; i++) {
                    final String sql = star + ranges[i];
                    assertParallelHashJoin(sql, sqlExecutionContext);
                    for (String query : new String[]{sql, star + limits[i]}) {
                        try (RecordCursorFactory factory = select(query)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(query, join);
                            // The join's own cursor returns its first row with the first window in
                            // flight, and goes on to read every row, through the capped frame's
                            // continuation, if any.
                            try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                                Assert.assertTrue(query, cursor.hasNext());
                                Assert.assertEquals(query, firstWindows[i], join.getDispatchedFrameCount());
                                if (firstWindows[i] == 1) {
                                    // The first frame's task joined the rows that the LIMIT needs.
                                    Assert.assertEquals(query, 3, join.getAtom().getJoinedProbeRowCount());
                                }
                                int rows = 1;
                                while (cursor.hasNext()) {
                                    rows++;
                                }
                                Assert.assertEquals(query, countLines(ordinaryJoinRows(query)), rows);
                                Assert.assertEquals(query, isCapped[i], join.getAtom().getContinuationCount() > 0);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRangeLimitOfZeroUpperBoundReadsTheFirstRows() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // LIMIT 5, 0 returns the first five rows, as LIMIT 5 does: LimitRecordCursorFactory swaps
            // the bounds of a range whose lower one is the larger, zero counting as positive. The
            // join's first window holds the frames of four rows that five rows need, the second
            // capped at one row, whose continuation the join's own cursor goes on to read, through
            // every row: each of t's rows meets one row of d1 and two of d2. Literal bounds, and
            // bound ones.
            bindVariableService.setLong("lo", 5);
            bindVariableService.setLong("hi", 0);
            try {
                for (String limit : new String[]{" LIMIT 5, 0", " LIMIT :lo, :hi"}) {
                    final String sql = STARS[0] + limit;
                    final String expected = ordinaryRows(sql, sqlExecutionContext);
                    Assert.assertEquals(sql, ordinaryRows(STARS[0] + " LIMIT 5", sqlExecutionContext), expected);
                    try (RecordCursorFactory factory = select(sql)) {
                        final AsyncHashJoinRecordCursorFactory join = find(factory);
                        Assert.assertNotNull(sql, join);
                        Assert.assertEquals(sql, expected, printRows(factory));
                        try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(sql, cursor.hasNext());
                            Assert.assertEquals(sql, 2, join.getDispatchedFrameCount());
                            int rows = 1;
                            while (cursor.hasNext()) {
                                rows++;
                            }
                            Assert.assertEquals(sql, 2 * FRAME_COUNT * FRAME_ROWS, rows);
                            Assert.assertTrue(sql, join.getAtom().getContinuationCount() > 0);
                        }
                    }
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testRangeLimitPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String sql = STARS[0] + " LIMIT :lo, :hi";
            bindVariableService.setLong("lo", 0);
            bindVariableService.setLong("hi", 1);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // One factory, executed under each pair of bounds: the first window follows each
                // execution's larger bound, unless one bound is negative or NULL, or both are zero.
                final long[][] bounds = {
                        {2, 5}, {-5, -2}, {5, 2}, {0, 0}, {2, -1}, {0, 9}, {2, Numbers.LONG_NULL}, {Numbers.LONG_NULL, Numbers.LONG_NULL},
                        {4, 4}, {0, Long.MAX_VALUE}, {Numbers.LONG_NULL, -1}, {1, 2}
                };
                final int[] firstWindows = {2, FRAME_COUNT, 2, FRAME_COUNT, FRAME_COUNT, 3, FRAME_COUNT, FRAME_COUNT, 1, FRAME_COUNT, FRAME_COUNT, 1};
                for (int i = 0; i < bounds.length; i++) {
                    bindVariableService.setLong("lo", bounds[i][0]);
                    bindVariableService.setLong("hi", bounds[i][1]);
                    final String label = "LIMIT " + bounds[i][0] + ", " + bounds[i][1];
                    final String expected = ordinaryRows(sql, sqlExecutionContext);
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                        cursor.toTop();
                        Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                    }
                    try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(label, firstWindows[i], join.getDispatchedFrameCount());
                    }
                }
            } finally {
                bindVariableService.clear();
            }
            // An untyped $1 that the SELECT list makes an INT reads as one in the range's upper bound,
            // on both plans, and the join takes the range.
            final String untyped = "SELECT t.k + $1 y, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT 0, $1";
            assertUntypedBindVariable(untyped, true, "$1 INT: y INT, w LONG");
            assertUntypedLimitPerExecution(
                    untyped,
                    ColumnType.INT,
                    new long[]{1, FRAME_ROWS, 5, 0, -2, Numbers.INT_NULL},
                    new int[]{1, 1, 2, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT},
                    new boolean[]{true, false, true, false, false, false}
            );
        });
    }

    @Test
    public void testUntypedBindVariableLimitPerExecution() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // t.k + $1 makes $1 an INT. A LIMIT of one or four rows needs the first frame, of five the
            // first two; one of more rows than a full window holds, or none, dispatches a full
            // window. A LIMIT that ends inside a frame leaves the rest of that frame to its
            // continuation.
            assertUntypedLimitPerExecution(
                    "SELECT t.k + $1 y, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT $1",
                    ColumnType.INT,
                    new long[]{1, FRAME_ROWS, 5, 0, -2, Numbers.INT_NULL, Integer.MAX_VALUE, -Integer.MAX_VALUE},
                    new int[]{1, 1, 2, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT},
                    new boolean[]{true, false, true, false, false, false, false, false}
            );
            // t.v + $1 makes $1 a LONG.
            assertUntypedLimitPerExecution(
                    "SELECT t.v + $1 y, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT $1",
                    ColumnType.LONG,
                    new long[]{1, 5, Long.MAX_VALUE, Numbers.LONG_NULL, -1},
                    new int[]{1, 2, FRAME_COUNT, FRAME_COUNT, FRAME_COUNT},
                    new boolean[]{true, true, false, false, false}
            );
            // A negative LIMIT of an INT $1 returns the last rows and leaves the execution as it is.
            final String last = "SELECT t.k + $1 y, d1.w FROM t JOIN d1 ON t.k = d1.k LIMIT -$1";
            bindVariableService.clear();
            try (RecordCursorFactory factory = select(last)) {
                Assert.assertEquals("INT", ColumnType.nameOf(bindVariableService.getFunction(0).getType()));
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                bindVariableService.setInt(0, 3);
                // t's last rows, v = 62, 63 and 64, of keys 2, 3 and 0, meet d1's rows of w = 20, 30
                // and 40; y = k + 3.
                final String expected = """
                        5:INT\t20:LONG
                        6:INT\t30:LONG
                        3:INT\t40:LONG
                        """;
                Assert.assertEquals(expected, printRows(factory));
                Assert.assertEquals(expected, ordinaryRows(last, sqlExecutionContext));
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(cursor.hasNext());
                    Assert.assertEquals(FRAME_COUNT, join.getDispatchedFrameCount());
                }
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testUntypedBindVariableOverAnEmptyProbe() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("CREATE TABLE e (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            final String sql = "SELECT e.k + $1 y FROM e JOIN d1 ON e.k = d1.k LIMIT $1";
            bindVariableService.clear();
            try (RecordCursorFactory factory = select(sql)) {
                Assert.assertNotNull(find(factory));
                Assert.assertEquals("INT", ColumnType.nameOf(bindVariableService.getFunction(0).getType()));
                bindVariableService.setInt(0, 3);
                Assert.assertEquals("", printRows(factory));
                Assert.assertEquals("", ordinaryRows(sql, sqlExecutionContext));
            } finally {
                bindVariableService.clear();
            }
        });
    }

    @Test
    public void testUntypedBindVariableSharedWithTheLimit() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String join = " FROM t JOIN d1 ON t.k = d1.k";
            // The SELECT list types $1 first, as its functions' signatures take it, and the LIMIT
            // reads it as that type, as PostgreSQL's first use of a parameter fixes its type:
            // left(VARCHAR, INT), lpad(STRING, INT, STRING), which takes the VARCHAR and the CHAR
            // fill, substring(VARCHAR, INT, INT), which returns a STRING, dateadd(CHAR, INT,
            // TIMESTAMP), INT + INT and LONG + LONG.
            final String trigger = "SELECT left(t.v::VARCHAR, $1) y, d1.w" + join + " LIMIT $1";
            assertUntypedBindVariable(trigger, true, "$1 INT: y VARCHAR, w LONG");
            assertUntypedBindVariable("SELECT lpad(t.v::VARCHAR, $1, '*') y, d1.w" + join + " LIMIT $1", true, "$1 INT: y STRING, w LONG");
            assertUntypedBindVariable("SELECT substring(t.v::VARCHAR, 1, $1) y, d1.w" + join + " LIMIT $1", true, "$1 INT: y STRING, w LONG");
            assertUntypedBindVariable("SELECT dateadd('d', $1, t.ts) y, d1.w" + join + " LIMIT $1", true, "$1 INT: y TIMESTAMP, w LONG");
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w" + join + " LIMIT $1", true, "$1 INT: y INT, w LONG");
            assertUntypedBindVariable("SELECT t.v + $1 y, d1.w" + join + " LIMIT $1", true, "$1 LONG: y LONG, w LONG");
            // A negative LIMIT and an expression of $1.
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w" + join + " LIMIT -$1", true, "$1 INT: y INT, w LONG");
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w" + join + " LIMIT $1 + 1", true, "$1 INT: y INT, w LONG");
            // A type that QuestDB's LIMIT rejects fails on every plan, at the LIMIT's $1, and a named
            // variable that nothing defines fails at its first use, in the SELECT list.
            final String doubleLimit = "SELECT t.v::DOUBLE + $1 y, d1.w" + join + " LIMIT $1";
            assertUntypedBindVariable(doubleLimit, true, "[" + doubleLimit.lastIndexOf("$1") + "] invalid type: DOUBLE");
            final String stringLimit = "SELECT t.k, $1 y" + join + " LIMIT $1";
            assertUntypedBindVariable(stringLimit, true, "[" + stringLimit.lastIndexOf("$1") + "] invalid type: STRING");
            final String named = "SELECT t.k + :lim y, d1.w" + join + " LIMIT :lim";
            assertUntypedBindVariable(named, true, "[" + named.indexOf(":lim") + "] undefined bind variable: :lim");

            // The LIMIT applies to the join's rows, and the join takes it, over a LEFT join, a star,
            // a post-join filter that the join absorbs, a probe filter that it steals, a scan in the
            // designated timestamp's order, in a sub-query, a CTE and the branches of a UNION ALL. A
            // WHERE on t alone is left out: the parallel filter over t parses the LIMIT before the
            // SELECT list, on the ordinary plan too.
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k LIMIT $1", true, "$1 INT: y INT, w LONG");
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w, d2.w2" + join + " JOIN d2 ON t.k = d2.k LIMIT $1", true, "$1 INT: y INT, w LONG, w2 LONG");
            final String postFilter = "SELECT t.k + $1 y, d1.w" + join + " WHERE d1.w > t.v LIMIT $1";
            assertUntypedBindVariable(postFilter, true, "$1 INT: y INT, w LONG");
            final int jitMode = sqlExecutionContext.getJitMode();
            try {
                for (int mode : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    sqlExecutionContext.setJitMode(mode);
                    assertUntypedBindVariable(
                            "SELECT t.k + $1 y, d1.w FROM (SELECT * FROM t WHERE v % 2 = 0) t JOIN d1 ON t.k = d1.k LIMIT $1",
                            true,
                            "$1 INT: y INT, w LONG"
                    );
                }
            } finally {
                sqlExecutionContext.setJitMode(jitMode);
            }
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w" + join + " ORDER BY ts DESC LIMIT $1", true, "$1 INT: y INT, w LONG");
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w" + join + " ORDER BY ts LIMIT $1", true, "$1 INT: y INT, w LONG");
            final String subQuery = "SELECT * FROM (SELECT t.k + $1 y, d1.w" + join + " LIMIT $1)";
            assertUntypedBindVariable(subQuery, true, "$1 INT: y INT, w LONG");
            final String cte = "WITH q AS (SELECT t.k + $1 y, d1.w" + join + " LIMIT $1) SELECT * FROM q";
            assertUntypedBindVariable(cte, true, "$1 INT: y INT, w LONG");
            assertUntypedBindVariable(
                    "SELECT * FROM (SELECT t.k + $1 y, d1.w" + join + " LIMIT $1) UNION ALL SELECT * FROM (SELECT t.k y, d2.w2 w FROM t JOIN d2 ON t.k = d2.k LIMIT $1)",
                    true,
                    "$1 INT: y INT, w LONG"
            );
            // An ON condition that reads $1 types it before the SELECT list does, on every plan:
            // d1.w > $1 compares with a LONG.
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w FROM t JOIN d1 ON t.k = d1.k AND d1.w > $1 LIMIT $1", true, "$1 LONG: y LONG, w LONG");
            assertUntypedBindVariable("SELECT t.k + $1 y, d1.w FROM t LEFT JOIN d1 ON t.k = d1.k AND d1.w > $1 LIMIT $1", true, "$1 LONG: y LONG, w LONG");
            // The LIMIT of a query over a sub-query that states no LIMIT, of a UNION ALL whose SELECT
            // lists both read $1, and of an ASOF join of which the join is the master.
            final String outerLimit = "SELECT y, w FROM (SELECT t.k + $1 y, d1.w" + join + ") LIMIT $1";
            assertUntypedBindVariable(outerLimit, true, "$1 INT: y INT, w LONG");
            final String unionAll = "SELECT t.k + $1 y, d1.w" + join + " UNION ALL SELECT t.k + $1 y, d1.w" + join + " LIMIT $1";
            assertUntypedBindVariable(unionAll, true, "$1 INT: y INT, w LONG");
            execute("""
                    CREATE TABLE q AS (
                        SELECT (x % 4)::INT k, x * 7 p, timestamp_sequence('2024-01-01', 30_000_000) ts
                        FROM long_sequence(128)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            final String asOf = "SELECT t.k + $1 y, d1.w, q.p" + join + " ASOF JOIN q ON (k) LIMIT $1";
            assertUntypedBindVariable(asOf, true, "$1 INT: y INT, w LONG, p LONG");
            // The LIMIT counts rows other than the join's: of DISTINCT, of GROUP BY and of a window
            // function.
            assertUntypedBindVariable("SELECT DISTINCT t.k + $1 y" + join + " LIMIT $1", true, "$1 INT: y INT");
            assertUntypedBindVariable("SELECT t.k + $1 y, count(*) c" + join + " GROUP BY 1 LIMIT $1", true, "$1 INT: y INT, c LONG");
            assertUntypedBindVariable("SELECT t.k + $1 y, row_number() OVER () rn" + join + " LIMIT $1", true, "$1 INT: y INT, rn LONG");

            // The join takes the LIMIT through the sub-query, the CTE, the absorbed post-join filter,
            // the outer query, each branch of the UNION ALL and the ASOF join: its first row, under
            // $1 = 1 as under LIMIT 1, needs the first frame alone.
            assertFirstWindowOfOneFrame(subQuery);
            assertFirstWindowOfOneFrame(cte);
            assertFirstWindowOfOneFrame(postFilter);
            assertFirstWindowOfOneFrame(outerLimit);
            assertFirstWindowOfOneFrame(unionAll);
            assertFirstWindowOfOneFrame(asOf);

            // The tables still serve the query.
            try {
                bindVariableService.setInt(0, 3);
                assertParallelHashJoin(trigger, sqlExecutionContext);
            } finally {
                bindVariableService.clear();
            }
        });
    }

    // The join's own cursor reads on past the LIMIT's rows, into the capped frame's continuation,
    // which the cancellation keeps from running; the join's own cursor then reads every row of the
    // join, which the outermost serial join of the ordinary plan returns, and the factory every row
    // of the query.
    private static void assertCancelledInsideTheCappedFrameAndReuse(String sql) throws SqlException {
        final String all = ordinaryOutermostOuterJoinRows(sql);
        final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
        final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = findAll(factory, sqlExecutionContext).getQuick(0);
            ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
            try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue(sql, cursor.hasNext());
                }
                breaker.cancel();
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                Assert.fail("expected cancellation: " + sql);
            } catch (CairoException e) {
                Assert.assertTrue(sql, e.isCancellation());
                TestUtils.assertContains(e.getFlyweightMessage(), "cancelled by user");
            }
            Assert.assertEquals(sql, 0, join.getAtom().getContinuationCount());
            Assert.assertEquals(sql, 0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
            Assert.assertEquals(sql, 0, engine.getBusyReaderCount());
            breaker.reset();
            try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                Assert.assertEquals(sql, all, printRows(cursor, join.getMetadata()));
            }
            Assert.assertTrue(sql, join.getAtom().getContinuationCount() > 0);
            Assert.assertEquals(sql, ordinaryRows(sql, sqlExecutionContext), printRows(factory));
        } finally {
            ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
        }
    }

    // Reads the query's rows, which equal the ordinary plan's, and asserts the frames that each of its
    // parallel hash joins, in the order of the plan, had dispatched once the query returned its first
    // row, or that it returned none for null, and once it returned its last. Also compares the
    // operator's plan and two executions with the ordinary plan's.
    private static void assertDispatched(String sql, int[] atFirstRow, int[] atEnd) throws SqlException {
        assertParallelHashJoin(sql, sqlExecutionContext);
        final String expected = ordinaryRows(sql, sqlExecutionContext);
        try (RecordCursorFactory factory = select(sql)) {
            final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, sqlExecutionContext);
            Assert.assertEquals(sql, atEnd.length, joins.size());
            final StringSink rows = new StringSink();
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final RecordMetadata metadata = factory.getMetadata();
                Assert.assertEquals(sql, atFirstRow != null, cursor.hasNext());
                if (atFirstRow != null) {
                    Assert.assertEquals(sql, Arrays.toString(atFirstRow), dispatched(joins));
                    do {
                        CursorPrinter.println(cursor.getRecord(), metadata, rows, true);
                    } while (cursor.hasNext());
                }
                Assert.assertEquals(sql, Arrays.toString(atEnd), dispatched(joins));
            }
            Assert.assertEquals(sql, expected, rows.toString());
        }
    }

    // Reads the factory's rows, which equal the ordinary plan's, and asserts that the query's first
    // parallel hash join returns its first row with the given frames dispatched, or, for none, that
    // the query returns no row and the join dispatched none.
    private static void assertFirstJoinDispatched(RecordCursorFactory factory, String sql, int frames) throws SqlException {
        Assert.assertEquals(sql, ordinaryRows(sql, sqlExecutionContext), printRows(factory));
        final AsyncHashJoinRecordCursorFactory join = findAll(factory, sqlExecutionContext).getQuick(0);
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(sql, frames > 0, cursor.hasNext());
            Assert.assertEquals(sql, frames, join.getDispatchedFrameCount());
        }
    }

    private static void assertFirstRow(String sql, int dispatched, long joinedRows, int continuations, int rows) throws SqlException {
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(join);
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(sql, dispatched, join.getDispatchedFrameCount());
                Assert.assertEquals(sql, continuations, join.getAtom().getContinuationCount());
                if (rows == 1) {
                    Assert.assertEquals(sql, joinedRows, join.getAtom().getJoinedProbeRowCount());
                }
                int count = 1;
                while (cursor.hasNext()) {
                    count++;
                }
                Assert.assertEquals(sql, rows, count);
            }
        }
    }

    // Each parallel hash join of the query returns its first row, through its own cursor, with the
    // given frames dispatched; $1, if the query has one, reads 1.
    private static void assertFirstWindow(String sql, int dispatched) throws SqlException {
        bindVariableService.clear();
        try (RecordCursorFactory factory = select(sql)) {
            if (sql.contains("$1")) {
                bindVariableService.setInt(0, 1);
            }
            final ObjList<AsyncHashJoinRecordCursorFactory> joins = findAll(factory, sqlExecutionContext);
            Assert.assertTrue(sql, joins.size() > 0);
            for (int i = 0, n = joins.size(); i < n; i++) {
                final AsyncHashJoinRecordCursorFactory join = joins.getQuick(i);
                try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                    Assert.assertTrue(sql, cursor.hasNext());
                    Assert.assertEquals(sql, dispatched, join.getDispatchedFrameCount());
                }
            }
        } finally {
            bindVariableService.clear();
        }
    }

    // The query, compiled with $1 undefined and executed with $1 = 1, and the same query with a
    // literal LIMIT 1 both return the join's first row from the first frame alone.
    private static void assertFirstWindowOfOneFrame(String sql) throws SqlException {
        assertFirstWindow(sql, 1);
        final String literal = sql.replace("LIMIT $1", "LIMIT 1");
        Assert.assertNotEquals(sql, literal);
        assertFirstWindow(literal, 1);
    }

    private static void assertParityUnderEachLimit() throws SqlException {
        for (String star : STARS) {
            for (String suffix : new String[]{
                    " LIMIT 1", " LIMIT 2", " LIMIT 5", " LIMIT 17", " LIMIT 63", " LIMIT 64", " LIMIT 65", " LIMIT 1000",
                    " ORDER BY t.ts DESC LIMIT 3", " ORDER BY ts DESC LIMIT 3", " ORDER BY ts DESC LIMIT 9", " ORDER BY ts LIMIT 7",
                    " LIMIT 2, 5", " LIMIT 5, 2", " LIMIT , 5", " LIMIT 0, 1", " LIMIT 3, 3", " LIMIT 16, 17", " LIMIT 63, 64",
                    " LIMIT 64, 65", " LIMIT 60, 1000", " ORDER BY ts DESC LIMIT 2, 5", " ORDER BY ts LIMIT 4, 9",
            }) {
                final String sql = star + suffix;
                assertParallelHashJoin(sql, sqlExecutionContext);
                // In a sub-query and in a CTE.
                assertParallelHashJoin("SELECT * FROM (" + sql + ")", sqlExecutionContext);
                assertParallelHashJoin("WITH q AS (" + sql + ") SELECT * FROM q", sqlExecutionContext);
            }
        }
    }

    // Compiles the query with its bind variables undefined, as a PostgreSQL Parse without parameter
    // types leaves them, under the ordinary plan, the oracle, and under the operator, and asserts
    // that both give $1 and the columns the expected types, or both fail with the expected error.
    private static void assertUntypedBindVariable(String sql, boolean isOperatorExpected, String expected) throws SqlException {
        Assert.assertEquals(sql, expected, compileUntyped(sql, false, false));
        Assert.assertEquals(sql, expected, compileUntyped(sql, true, isOperatorExpected));
    }

    // Compiles the query once with $1 undefined under each plan, and executes both factories under
    // each value of $1: the operator's rows equal the ordinary plan's, also after toTop(), and the
    // join's own cursor returns its first row with the given frames dispatched, then every row of t,
    // each of which meets one of d1's, with a continuation of the capped frame where expected.
    private static void assertUntypedLimitPerExecution(
            String sql,
            int bindType,
            long[] values,
            int[] firstWindows,
            boolean[] continuations
    ) throws SqlException {
        final boolean isEnabledBefore = sqlExecutionContext.isParallelHashJoinEnabled();
        try {
            sqlExecutionContext.setParallelHashJoinEnabled(false);
            bindVariableService.clear();
            try (RecordCursorFactory ordinary = select(sql)) {
                Assert.assertEquals(sql, ColumnType.nameOf(bindType), ColumnType.nameOf(bindVariableService.getFunction(0).getType()));
                sqlExecutionContext.setParallelHashJoinEnabled(true);
                bindVariableService.clear();
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertEquals(sql, ColumnType.nameOf(bindType), ColumnType.nameOf(bindVariableService.getFunction(0).getType()));
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(sql, join);
                    for (int i = 0; i < values.length; i++) {
                        if (bindType == ColumnType.INT) {
                            bindVariableService.setInt(0, (int) values[i]);
                        } else {
                            bindVariableService.setLong(0, values[i]);
                        }
                        final String label = sql + ", $1 = " + values[i];
                        final String expected = printRows(ordinary);
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                            cursor.toTop();
                            Assert.assertEquals(label, expected, printRows(cursor, factory.getMetadata()));
                        }
                        try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(label, cursor.hasNext());
                            Assert.assertEquals(label, firstWindows[i], join.getDispatchedFrameCount());
                            int rows = 1;
                            while (cursor.hasNext()) {
                                rows++;
                            }
                            Assert.assertEquals(label, FRAME_COUNT * FRAME_ROWS, rows);
                            Assert.assertEquals(label, continuations[i], join.getAtom().getContinuationCount() > 0);
                        }
                    }
                }
            }
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(isEnabledBefore);
            bindVariableService.clear();
        }
    }

    // The type that the compile gives $1, left undefined, and the query's columns, or the compile's
    // error. Asserts whether the plan runs the operator.
    private static String compileUntyped(String sql, boolean isParallel, boolean isOperatorExpected) throws SqlException {
        final boolean isEnabledBefore = sqlExecutionContext.isParallelHashJoinEnabled();
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        bindVariableService.clear();
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(sql, isOperatorExpected, plan(factory, sqlExecutionContext).contains(OPERATOR));
            final StringSink sink = new StringSink();
            sink.put("$1 ").put(ColumnType.nameOf(bindVariableService.getFunction(0).getType())).put(':');
            final RecordMetadata metadata = factory.getMetadata();
            for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
                sink.put(i == 0 ? " " : ", ").put(metadata.getColumnName(i)).put(' ').put(ColumnType.nameOf(metadata.getColumnType(i)));
            }
            return sink.toString();
        } catch (SqlException e) {
            return "[" + e.getPosition() + "] " + e.getFlyweightMessage();
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(isEnabledBefore);
            bindVariableService.clear();
        }
    }

    private static int countLines(String rows) {
        int count = 0;
        for (int i = 0, n = rows.length(); i < n; i++) {
            if (rows.charAt(i) == '\n') {
                count++;
            }
        }
        return count;
    }

    // q holds 128 rows, two a minute from t's first one, of t's four keys, which ASOF and LT joins look
    // up; m one row of key 3; d4 one row per value of d1.w, which a serial join over the join looks
    // up; ds a SYMBOL per key.
    private static void createLimitShapeTables() throws SqlException {
        execute("""
                CREATE TABLE q AS (
                    SELECT (x % 4)::INT k, x * 7 p, timestamp_sequence('2024-01-01', 30_000_000) ts
                    FROM long_sequence(128)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        execute("CREATE TABLE m AS (SELECT 3 k, 33 w FROM long_sequence(1))");
        execute("CREATE TABLE d4 AS (SELECT x * 10 w, x * 1000 z FROM long_sequence(4))");
        execute("CREATE TABLE ds AS (SELECT (x % 4)::INT k, ('s' || (x % 4))::SYMBOL s FROM long_sequence(4))");
    }

    private static void createTables() throws Exception {
        // 64 rows in one partition, cut into sixteen frames of four rows.
        execute("""
                CREATE TABLE t AS (
                    SELECT (x % 4)::INT k, x v, timestamp_sequence('2024-01-01', 60_000_000) ts
                    FROM long_sequence(64)
                ) TIMESTAMP(ts) PARTITION BY DAY
                """);
        // d1 holds one row per key, d2 two and d3 three, but none for key 0.
        execute("CREATE TABLE d1 AS (SELECT (x % 4)::INT k, x * 10 w FROM long_sequence(4))");
        execute("CREATE TABLE d2 AS (SELECT (x % 4)::INT k, x * 100 w2 FROM long_sequence(8))");
        execute("CREATE TABLE d3 AS (SELECT (x % 3 + 1)::INT k, x * 1000 w3 FROM long_sequence(9))");
        sqlExecutionContext.changePageFrameSizes(FRAME_ROWS, FRAME_ROWS);
    }

    // The frames that each of the joins dispatched since its cursor opened, as "[a, b, ...]".
    private static String dispatched(ObjList<AsyncHashJoinRecordCursorFactory> joins) {
        final int[] frames = new int[joins.size()];
        for (int i = 0; i < frames.length; i++) {
            frames[i] = joins.getQuick(i).getDispatchedFrameCount();
        }
        return Arrays.toString(frames);
    }

    // The plan of an EXPLAIN compiled with its bind variables undefined.
    private static String explainUntyped(String sql, boolean isParallel) throws SqlException {
        final boolean isEnabledBefore = sqlExecutionContext.isParallelHashJoinEnabled();
        sqlExecutionContext.setParallelHashJoinEnabled(isParallel);
        bindVariableService.clear();
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final StringSink sink = new StringSink();
            while (cursor.hasNext()) {
                sink.put(cursor.getRecord().getStrA(0)).put('\n');
            }
            return sink.toString();
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(isEnabledBefore);
            bindVariableService.clear();
        }
    }

    // The LIMIT advice that the optimiser gives the query's join model: its one bound, "lo, hi" for a
    // range, or null.
    private static String joinModelLimitAdvice(String sql) throws SqlException {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
            while (model.getJoinModels().size() < 2) {
                model = model.getNestedModel();
                Assert.assertNotNull(sql, model);
            }
            final ExpressionNode lo = model.getLimitAdviceLo();
            final ExpressionNode hi = model.getLimitAdviceHi();
            if (hi == null) {
                return lo != null ? Chars.toString(lo.token) : null;
            }
            return (lo != null ? Chars.toString(lo.token) : "null") + ", " + Chars.toString(hi.token);
        }
    }

    // The rows of the ordinary plan's outermost join, which the LIMIT and the projections over it
    // leave out, as the parallel join's own cursor returns them.
    private static String ordinaryJoinRows(String sql) throws SqlException {
        final boolean isEnabledBefore = sqlExecutionContext.isParallelHashJoinEnabled();
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            RecordCursorFactory join = factory;
            while (join.getBaseFactory() != null) {
                join = join.getBaseFactory();
            }
            Assert.assertTrue(join.getClass().getName(), join.getClass().getSimpleName().contains("Join"));
            return printRows(join);
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(isEnabledBefore);
        }
    }

    // The rows of the ordinary plan's outermost LEFT hash join, the first that its plan lists, which
    // may sit under an ASOF join, where no base factory names it.
    private static String ordinaryOutermostOuterJoinRows(String sql) throws SqlException {
        final boolean isEnabledBefore = sqlExecutionContext.isParallelHashJoinEnabled();
        sqlExecutionContext.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = select(sql)) {
            final ObjList<HashOuterJoinLightRecordCursorFactory> joins = findAll(factory, sqlExecutionContext, HashOuterJoinLightRecordCursorFactory.class);
            Assert.assertTrue(sql, joins.size() > 0);
            return printRows(joins.getQuick(0));
        } finally {
            sqlExecutionContext.setParallelHashJoinEnabled(isEnabledBefore);
        }
    }

    private static String printRows(RecordCursorFactory factory) throws SqlException {
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            return printRows(cursor, factory.getMetadata());
        }
    }

    private static String printRows(RecordCursor cursor, RecordMetadata metadata) {
        final StringSink sink = new StringSink();
        while (cursor.hasNext()) {
            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
        }
        return sink.toString();
    }

    private static void setBindLimit(int bindType, long value) throws SqlException {
        if (bindType == ColumnType.INT) {
            bindVariableService.setInt(0, (int) value);
        } else {
            bindVariableService.setLong(0, value);
        }
    }

    private static long size(RecordCursor cursor) {
        final RecordCursor.Counter counter = new RecordCursor.Counter();
        cursor.calculateSize(sqlExecutionContext.getCircuitBreaker(), counter);
        return counter.get();
    }
}
