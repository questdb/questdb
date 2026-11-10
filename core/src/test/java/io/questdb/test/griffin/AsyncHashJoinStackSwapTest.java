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
import io.questdb.cairo.sql.AtomicBooleanCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.async.PageFrameReduceTask;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinKeyLayout;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.table.AsyncFilterContext;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncHashJoinSpec;
import io.questdb.griffin.engine.table.HashJoinBuildStage;
import io.questdb.griffin.engine.table.HashJoinInput;
import io.questdb.griffin.engine.table.HashJoinStageSpec;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.test.griffin.ParallelHashJoinTestUtils.*;

/**
 * Stacks of the parallel hash join whose first join may swap its inputs at execution, as the serial
 * hash join swaps them when its left input is the smaller and no parent fixed its order. A swapped
 * execution hashes the left input in the first stage and streams the first join's right input, so
 * the later stages, whose keys read the left input's columns, read them from the first stage's
 * payload, which the workers position on each of the first stage's matches. Every result compares
 * with the ordinary plan of the same SQL, in order, which swaps its first join by the same rule and
 * joins the later joins over its rows, and checks the orientation that the execution took. The
 * key columns sit at other positions in every table, so that a key read from another column
 * misreads, and a stack planned by hand over a left input whose compiled columns sit at other
 * positions than its frame columns shows that each orientation reads the later keys through a key
 * layout of its own.
 */
public class AsyncHashJoinStackSwapTest extends AbstractCairoTest {
    private static final String INPUT_SWAP = "inputSwap: when the left input is smaller";
    // The first join on l's INT key k, then seven later joins on l's other columns: an INT key, a
    // staged LONG key, a translated SYMBOL key, a unique INT build, a staged VARCHAR key, a staged
    // STRING key and a composite key with a translated SYMBOL column; INNER and LEFT joins, an ON
    // residual on the fifth later stage, so that the workers walk the first six stages and the
    // consumer expands the chains of the last two, and duplicates in most builds.
    private static final String SEVEN_LATER = "SELECT l.v, r.w, a.w1, b.w2, c.w3, e.uw, f.w2, g.w3, h.w2 FROM l"
            + " JOIN r ON l.k = r.k"
            + " LEFT JOIN d1 a ON l.ik = a.ik"
            + " JOIN d2 b ON l.lk = b.lk"
            + " LEFT JOIN d3 c ON l.s = c.s"
            + " LEFT JOIN u1 e ON l.ik = e.ik"
            + " LEFT JOIN d2 f ON l.vc = f.vc AND f.w2 > l.v"
            + " LEFT JOIN d3 g ON l.str = g.str"
            + " LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q";

    @Override
    @Before
    public void setUp() {
        super.setUp();
        sqlExecutionContext.setParallelHashJoinEnabled(true);
    }

    @After
    public void tearDownStackSwap() {
        sqlExecutionContext.restoreToDefaultPageFrameSizes();
    }

    @Test
    public void testBuildsInRoundsAndPayloadCopies() throws Exception {
        // Every build that can copies its payload, from a probe of any size, and builds of more than
        // two rows build in rounds of hash partitions of a row or two each.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 2);
        assertMemoryLeak(() -> {
            createTables(12, 40);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            // A left input of fixed-size columns, whose swapped build copies its payload, which the
            // later stages' keys then read: an INT key, a staged LONG key and a staged key with a
            // translated SYMBOL column, whose builds in rounds hand every slot a partitioned probe of
            // the orientation's key layout. The projections of d2 leave out its VARCHAR column,
            // which no copy holds.
            final String sql = "SELECT l.v, l.k, r.w, a.w1, b.w2, h.w2 FROM (SELECT ts, s, lk, ik, k, v FROM l) l"
                    + " JOIN r ON l.k = r.k"
                    + " LEFT JOIN d1 a ON l.ik = a.ik"
                    + " LEFT JOIN (SELECT w2, q, lk FROM d2) b ON l.lk = b.lk"
                    + " LEFT JOIN (SELECT w2, q, lk FROM d2) h ON l.lk = h.lk AND l.s = h.q";
            for (long minRows : new long[]{0, Long.MAX_VALUE}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                assertSwappedStack(sql, 4, true);
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    // Two executions: the second reopens the probes that the first created.
                    for (int run = 0; run < 2; run++) {
                        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertTrue(join.getAtom().isSwapped());
                            for (int k = 0; k < 4; k++) {
                                final HashJoinBuildStage stage = join.getAtom().getStage(k);
                                Assert.assertEquals(minRows == 0, stage.isBuiltInRounds());
                                Assert.assertEquals(minRows == 0, stage.isBuildPartitioned());
                                Assert.assertTrue(stage.isPayloadCopied());
                            }
                            // The first stage hashes l's twelve rows.
                            Assert.assertEquals(12, join.getAtom().getStage(0).getFrozenBuild().getRowCount());
                            Assert.assertTrue(join.getAtom().getStage(3).isKeyStaged());
                        }
                        Assert.assertEquals(ordinary(sql, sqlExecutionContext), result(factory, sqlExecutionContext));
                    }
                }
            }
        });
    }

    @Test
    public void testCachedFactoryFlipsOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            sqlExecutionContext.changePageFrameSizes(4, 4);
            // INT, staged and translated SYMBOL keys in the later stages, and a post-join filter on
            // the last one, which the workers run in either orientation.
            final String sql = "SELECT l.v, l.s, r.w, a.w1, b.w2, c.w3, h.w2 FROM l"
                    + " JOIN r ON l.k = r.k"
                    + " LEFT JOIN d1 a ON l.ik = a.ik"
                    + " JOIN d2 b ON l.lk = b.lk"
                    + " LEFT JOIN d3 c ON l.s = c.s"
                    + " LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q"
                    + " WHERE h.w2 IS NULL OR h.w2 <> b.w2";
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                TestUtils.assertContains(plan(factory, sqlExecutionContext), "stages: 5");
                // Each execution compares the sizes of the scans it opens, and the later stages read
                // their keys from wherever the execution's orientation holds the left input's rows.
                // They presize their key tables by l's table in either orientation: d1's 15 rows
                // exceed l's 12 rows, but not r's 40, nor l's 52 once l grows.
                assertExecution(factory, join, sql, true);
                assertLaterStagePresized(factory, join, false);
                execute("INSERT INTO l SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % 3)::INT END, 100 + x, (x % 5)::LONG, ('s' || (x % 4))::SYMBOL,"
                        + " CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 6)::INT END, 'str' || (x % 3), ('vc' || (x % 4))::VARCHAR,"
                        + " timestamp_sequence('2024-01-01T03', 60_000_000) FROM long_sequence(40)");
                assertExecution(factory, join, sql, false);
                assertLaterStagePresized(factory, join, true);
                execute("INSERT INTO r SELECT 200 + x, CASE WHEN x % 5 = 0 THEN NULL ELSE (x % 4)::INT END, ('s' || (x % 4))::SYMBOL,"
                        + " timestamp_sequence('2024-01-02', 60_000_000) FROM long_sequence(40)");
                assertExecution(factory, join, sql, true);
                assertLaterStagePresized(factory, join, true);
                // An empty left input reports a size of zero, which the rule does not swap by.
                execute("TRUNCATE TABLE l");
                assertExecution(factory, join, sql, false);
                assertLaterStagePresized(factory, join, false);
                Assert.assertEquals("", rows(result(factory, sqlExecutionContext)));
            }
        });
    }

    @Test
    public void testCancellationAndMemoryLimitInSwappedStack() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            sqlExecutionContext.changePageFrameSizes(3, 3);
            final String sql = "SELECT l.v, r.w, a.w1, b.w2, h.w2 FROM l JOIN r ON l.k = r.k"
                    + " LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q";
            final String expected = ordinary(sql, sqlExecutionContext);
            final SqlExecutionCircuitBreaker previous = sqlExecutionContext.getCircuitBreaker();
            final AtomicBooleanCircuitBreaker breaker = new AtomicBooleanCircuitBreaker(engine, 0);
            try (RecordCursorFactory factory = select(sql)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                // Limits from a few bytes up fail the swapped stack at its allocations in turn: the
                // builds of l and of the later stages, their probes and translations, and the tuples.
                // Each failure releases every build and slot, and the first limit that admits the
                // query reads the rows.
                int failures = 0;
                for (long limit = 4; ; limit *= 2) {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, limit);
                    try {
                        Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                        break;
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                        failures++;
                    }
                    Assert.assertTrue(join.getAtom().isSwapped());
                    assertReleased(join);
                }
                Assert.assertTrue(failures > 1);
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0L);
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
                // Cancelled before the builds, then while the workers join the frames.
                ((SqlExecutionContextImpl) sqlExecutionContext).with(breaker);
                for (int rowsBeforeCancel = 0; rowsBeforeCancel < 2; rowsBeforeCancel++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(join.getAtom().isSwapped());
                        for (int i = 0; i < rowsBeforeCancel; i++) {
                            Assert.assertTrue(cursor.hasNext());
                        }
                        breaker.cancel();
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("expected cancellation");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.isCancellation());
                    }
                    breaker.reset();
                    assertReleased(join);
                }
                Assert.assertEquals(expected, result(factory, sqlExecutionContext));
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).with(previous);
            }
            // The tables still work.
            assertSwappedStack(sql, 4, true);
        });
    }

    @Test
    public void testConstructionFailureWhileStackingOverCandidate() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            // The first join, which may swap, the second, the third and the post-join filter make
            // four constructions; each fails in turn, after the joins before it gave up their specs,
            // and releases everything.
            final String sql = "SELECT l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k"
                    + " LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk AND l.s = b.q"
                    + " WHERE b.w2 IS NULL OR b.w2 > a.w1";
            final String plan = assertSwappedStack(sql, 3, true);
            TestUtils.assertContains(plan, "postJoinFilter: ");
            for (int constructions = 0; constructions < 4; constructions++) {
                AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(CairoException.nonCritical().put("injected construction failure"), constructions);
                try {
                    assertExceptionNoLeakCheck(sql, 0, "injected construction failure");
                } finally {
                    AsyncHashJoinRecordCursorFactory.setConstructorFailureForTesting(null);
                }
            }
            // A native memory limit just above what the process holds fails the compile at each of
            // its allocations in turn, the swapped key layouts' among them; every failure releases
            // what the compile took, and the first limit that admits the compile plans the stack.
            int failures = 0;
            for (long extra = 0; ; extra += 64) {
                RecordCursorFactory factory = null;
                try {
                    Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + extra);
                    factory = select(sql);
                } catch (CairoException e) {
                    Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                    failures++;
                    continue;
                } finally {
                    Unsafe.setRssMemLimit(0);
                }
                try {
                    TestUtils.assertContains(plan(factory, sqlExecutionContext), "stages: 3");
                } finally {
                    factory.close();
                }
                break;
            }
            Assert.assertTrue(failures > 0);
            // The tables still work.
            assertSwappedStack(sql, 3, true);
        });
    }

    @Test
    public void testHandPlannedStackOverRemappedLeftInput() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            // The planner hands a join that may swap its inputs an unfiltered scan, whose frames hold
            // the left input's columns as compiled, so the first stage's payload and the probe frames
            // lay them out alike. A stack planned by hand over a left input that maps its columns
            // onto other frame columns lays them out apart: the later stages' keys then read other
            // columns of their key source in the two orientations, an INT key, a translated SYMBOL key
            // and a staged key with a translated SYMBOL column.
            final String sql = "SELECT l.ik, l.k, l.s, l.lk, l.v, l.ts, r.w, r.k, r.rs, r.rts, a.q, a.w1, a.ik, c.w3, c.str, c.s, h.w2, h.vc, h.q, h.lk"
                    + " FROM (SELECT ik, k, s, lk, v, ts FROM l) l JOIN r ON l.k = r.k JOIN d1 a ON l.ik = a.ik"
                    + " LEFT JOIN d3 c ON l.s = c.s LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q";
            for (int frameRows : new int[]{1, 3, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                try (
                        RecordCursorFactory leftQuery = select("SELECT * FROM l");
                        RecordCursorFactory leftProjection = select("SELECT ik, k, s, lk, v, ts FROM l");
                        RecordCursorFactory rightQuery = select("SELECT * FROM r");
                        RecordCursorFactory d1Query = select("SELECT * FROM d1");
                        RecordCursorFactory d3Query = select("SELECT * FROM d3");
                        RecordCursorFactory d2Query = select("SELECT * FROM d2");
                        AsyncHashJoinRecordCursorFactory join = newRemappedStack(leftQuery, leftProjection, rightQuery, d1Query, d3Query, d2Query)
                ) {
                    Assert.assertTrue(join.getSpec().isSwapCandidate());
                    Assert.assertEquals(1, countMatches(plan(join, sqlExecutionContext), "inputSwap"));
                    // l, the smaller, swaps, until it grows past r.
                    Assert.assertEquals(rows(ordinary(sql, sqlExecutionContext)), rows(result(join, sqlExecutionContext)));
                    assertHandPlannedExecution(join, true);
                    execute("INSERT INTO l SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % 3)::INT END, 100 + x, (x % 5)::LONG, ('s' || (x % 4))::SYMBOL,"
                            + " CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 6)::INT END, 'str' || (x % 3), ('vc' || (x % 4))::VARCHAR,"
                            + " timestamp_sequence('2024-01-01T03', 60_000_000) FROM long_sequence(40)");
                    Assert.assertEquals(rows(ordinary(sql, sqlExecutionContext)), rows(result(join, sqlExecutionContext)));
                    assertHandPlannedExecution(join, false);
                }
                dropTables();
                createTables(12, 40);
            }
        });
    }

    @Test
    public void testHandCheckedNestingOrder() throws Exception {
        assertMemoryLeak(() -> {
            // l holds two rows of key 1, r three rows, so l is the smaller: the swapped first join
            // streams r and lists each r row's l matches newest first, and the later join lists each
            // l match's a matches newest first, as the serial joins' nested loops read them.
            execute("CREATE TABLE l (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO l VALUES (1, 10, '2024-01-01T00:00:00.000000Z'), (1, 11, '2024-01-01T01:00:00.000000Z')");
            execute("CREATE TABLE r (k INT, w STRING)");
            execute("INSERT INTO r VALUES (1, 'r1'), (2, 'r2'), (1, 'r3')");
            execute("CREATE TABLE a (v LONG, a STRING)");
            execute("INSERT INTO a VALUES (10, 'a1'), (11, 'a2'), (10, 'a3')");
            final String sql = "SELECT l.v, r.w, a.a FROM l JOIN r ON l.k = r.k LEFT JOIN a ON l.v = a.v";
            for (int frameRows : new int[]{1, 2, 1_000_000}) {
                sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                assertQuery(sql)
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                v\tw\ta
                                11\tr1\ta2
                                10\tr1\ta3
                                10\tr1\ta1
                                11\tr3\ta2
                                10\tr3\ta3
                                10\tr3\ta1
                                """);
                assertSwappedStack(sql, 2, true);
                // Once ORDER BY l's timestamp fixes the order, the join streams l, and each l row
                // lists its r matches newest first.
                assertQuery(sql + " ORDER BY l.ts")
                        .noLeakCheck()
                        .noRandomAccess()
                        .returns("""
                                v\tw\ta
                                10\tr3\ta3
                                10\tr3\ta1
                                10\tr1\ta3
                                10\tr1\ta1
                                11\tr3\ta2
                                11\tr1\ta2
                                """);
                Assert.assertFalse(assertSwappedStack(sql + " ORDER BY l.ts", 2, false).contains(INPUT_SWAP));
            }
        });
    }

    @Test
    public void testLaterStageKeysInBothOrientations() throws Exception {
        assertMemoryLeak(() -> {
            // l smaller than r swaps; l larger than r does not. Both orientations read the later
            // stages' keys off the left input's columns: the probe record when l streams, the first
            // stage's payload when l is hashed.
            for (int[] shape : new int[][]{{12, 40, 1}, {40, 12, 0}}) {
                createTables(shape[0], shape[1]);
                for (int frameRows : new int[]{1, 3, 1_000_000}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    final boolean isSwapped = shape[2] == 1;
                    for (String sql : new String[]{
                            // An INT key, a staged LONG key, a translated SYMBOL key, a SYMBOL key that
                            // compares with a STRING as text, a staged VARCHAR key and a TIMESTAMP key.
                            "SELECT l.v, r.w, a.w1, a.q FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik",
                            "SELECT l.v, r.w, b.w2, b.vc FROM l JOIN r ON l.k = r.k JOIN d2 b ON l.lk = b.lk",
                            "SELECT l.v, l.s, r.w, c.w3, c.s FROM l JOIN r ON l.k = r.k LEFT JOIN d3 c ON l.s = c.s",
                            "SELECT l.v, l.s, r.w, c.w3 FROM l JOIN r ON l.k = r.k LEFT JOIN d3 c ON l.s = c.str",
                            "SELECT l.v, l.vc, r.w, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d2 b ON l.vc = b.vc",
                            "SELECT l.v, r.w, x.xv FROM l JOIN r ON l.k = r.k LEFT JOIN x ON l.ts = x.xts",
                            // A staged key of two columns, one of them a translated SYMBOL, over a
                            // projection of l that reorders and repeats its columns, and over one that
                            // drops the first join's key from the joined rows.
                            "SELECT l.v, l.s, r.w, h.w2, h.q FROM l JOIN r ON l.k = r.k LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q",
                            "SELECT l.v2, l.s, r.w, h.w2 FROM (SELECT ts, s, v, lk, v v2, k FROM l) l JOIN r ON l.k = r.k LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q",
                            "SELECT l.v, r.w, a.w1, c.w3 FROM (SELECT ik, s, k, v, ts FROM l) l JOIN r ON l.k = r.k JOIN d1 a ON l.ik = a.ik LEFT JOIN d3 c ON l.s = c.s",
                            // A projection that repeats a column ahead of the keys: an INT key and a
                            // translated SYMBOL key of the INT layout, and staged keys.
                            "SELECT l.v, l.v2, r.w, a.w1, c.w3 FROM (SELECT v, v v2, ik, s, k, ts FROM l) l JOIN r ON l.k = r.k JOIN d1 a ON l.ik = a.ik LEFT JOIN d3 c ON l.s = c.s",
                            "SELECT l.v2, r.w, b.w2, g.w3 FROM (SELECT v, v v2, vc, lk, str, k, ts FROM l) l JOIN r ON l.k = r.k LEFT JOIN d2 b ON l.lk = b.lk LEFT JOIN d3 g ON l.str = g.str",
                            // The first join on a SYMBOL key too, which translates r's keys into l's
                            // dictionary when it swaps, and l's into r's when it does not.
                            "SELECT l.v, l.s, r.w, r.rs, c.w3 FROM l JOIN r ON l.s = r.rs LEFT JOIN d3 c ON l.s = c.s",
                            "SELECT l.v, l.s, r.w, a.w1, h.w2 FROM l JOIN r ON l.s = r.rs JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q",
                    }) {
                        final String plan = assertSwappedStack(sql, countMatches(sql, " JOIN "), isSwapped);
                        TestUtils.assertContains(plan, INPUT_SWAP);
                        Assert.assertEquals(plan, 1, countMatches(plan, "inputSwap"));
                    }
                }
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
                dropTables();
            }
        });
    }

    @Test
    public void testOrderDeterminedThroughSwapCandidateStack() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            // ORDER BY l's timestamp asks the stack for its scan direction, which fixes its order, so
            // no execution swaps the first join's inputs, and nothing sorts, as in the ordinary plan.
            for (String sql : new String[]{
                    "SELECT l.ts, l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk ORDER BY l.ts",
                    "SELECT l.ts, l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk ORDER BY l.ts DESC",
            }) {
                final String plan = assertSwappedStack(sql, 3, false);
                Assert.assertFalse(plan, plan.contains(INPUT_SWAP));
                Assert.assertFalse(plan, isSorted(plan));
                Assert.assertFalse(isSorted(ordinaryPlan(sql)));
                try (RecordCursorFactory factory = select(sql)) {
                    final AsyncHashJoinRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    Assert.assertTrue(join.getSpec().isSwapCandidate());
                    Assert.assertTrue(join.getSpec().isOrderDetermined());
                }
            }
            // ORDER BY another column sorts the stack's rows, whose order stays open, so the first
            // join swaps; so does one under a post-join filter on a later stage, whose serial filter
            // answers the parent's advice without asking the joins.
            Assert.assertTrue(isSorted(assertSwappedStack("SELECT l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk ORDER BY r.w, l.v, a.w1, b.w2", 3, true)));
            final String filtered = assertSwappedStack("SELECT l.ts, l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk"
                    + " WHERE b.w2 IS NULL OR b.w2 > a.w1 ORDER BY r.w, l.v, a.w1, b.w2", 3, true);
            TestUtils.assertContains(filtered, "postJoinFilter: ");
            Assert.assertTrue(isSorted(filtered));
        });
    }

    @Test
    public void testParquetLeftInputSwapped() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            // l's first hour lives in a Parquet partition, its second in a native one, and so do r's.
            execute("ALTER TABLE l CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-01T01'");
            execute("ALTER TABLE r CONVERT PARTITION TO PARQUET WHERE rts < '2023-12-31T01'");
            for (String copyRatio : new String[]{"1000000", "0"}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, copyRatio);
                for (int frameRows : new int[]{2, 1_000_000}) {
                    sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                    // The swapped first stage hashes l's Parquet and native frames, and the later
                    // stages' keys read l's columns through its payload readers, which decode the
                    // Parquet row group of each match, or through its copy.
                    for (String sql : new String[]{
                            "SELECT l.v, l.s, r.w, a.w1, b.w2, c.w3 FROM (SELECT ts, s, lk, ik, k, v FROM l) l JOIN r ON l.k = r.k"
                                    + " LEFT JOIN d1 a ON l.ik = a.ik JOIN d2 b ON l.lk = b.lk LEFT JOIN d3 c ON l.s = c.s",
                            "SELECT l.v, l.str, r.w, h.w2, g.w3 FROM l JOIN r ON l.k = r.k"
                                    + " LEFT JOIN d2 h ON l.lk = h.lk AND l.s = h.q LEFT JOIN d3 g ON l.str = g.str",
                    }) {
                        assertSwappedStack(sql, countMatches(sql, " JOIN "), true);
                        try (RecordCursorFactory factory = select(sql)) {
                            final AsyncHashJoinRecordCursorFactory join = find(factory);
                            Assert.assertNotNull(join);
                            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertTrue(join.getAtom().isSwapped());
                                // Only a payload of fixed-size columns copies.
                                Assert.assertEquals(copyRatio.equals("0") && !sql.contains("l.str"), join.getAtom().getStage(0).isPayloadCopied());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            // The plan shows the inputs in the query's roles, and only the first join names the rule
            // it swaps by: the later joins read the left input's columns in either orientation.
            assertQuery("SELECT l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik JOIN d2 b ON l.lk = b.lk")
                    .noLeakCheck()
                    .assertsPlan("""
                            SelectedRecord
                                Async Hash Join workers: 1
                                  stages: 3
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
                                    Build
                                      joinType: left outer
                                      condition: a.ik=l.ik
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d1
                                    Build
                                      joinType: inner
                                      condition: b.lk=l.lk
                                      buildPayload: copied when the probe is large enough
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: d2
                            """);
            // A later join whose key reads a column of the first join's right input, a snowflake
            // join, wraps the stack as an ordinary join's master, and a star join over that joins
            // the ordinary join's rows.
            // The ordinary joins ask the stack nothing about its order, so its first join swaps, as the
            // ordinary plan's does: the rows, which compare in order, would differ otherwise.
            final String snowflake = assertMixedPlan("SELECT l.v, r.w, a.w1, x.w3 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik"
                    + " LEFT JOIN d3 x ON r.rs = x.s LEFT JOIN d2 b ON l.lk = b.lk", 2);
            Assert.assertEquals(snowflake, 2, countMatches(snowflake, "Hash Left Outer Join Light"));
            // A later join over a build that exposes no page frames does too.
            final String grouped = assertMixedPlan("SELECT l.v, r.w, g.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN (SELECT lk, max(w2) w2 FROM d2) g ON l.lk = g.lk", 1);
            Assert.assertEquals(grouped, 1, countMatches(grouped, "Hash Left Outer Join Light"));
            // Eight stages.
            final String eight = assertSwappedStack(SEVEN_LATER, 8, true);
            Assert.assertEquals(eight, 8, countMatches(eight, " Build "));
            Assert.assertEquals(eight, 1, countMatches(eight, "inputSwap"));
        });
    }

    @Test
    public void testSwappedFirstJoinUnderLaterStages() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            for (long minRows : new long[]{Long.MAX_VALUE, 0}) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, minRows);
                for (int cap : new int[]{1_000_000, 1}) {
                    // A cap of one tuple leaves each frame's rest to the consumer's continuations, which
                    // run the stages on the owner's slot.
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_TASK_OUTPUT_MAX_ROWS, cap);
                    for (int frameRows : new int[]{1, 3, 1_000_000}) {
                        sqlExecutionContext.changePageFrameSizes(frameRows, frameRows);
                        for (String sql : new String[]{
                                // One later stage, INNER and LEFT.
                                "SELECT l.v, r.w, a.w1 FROM l JOIN r ON l.k = r.k JOIN d1 a ON l.ik = a.ik",
                                "SELECT * FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik",
                                // Two later stages, INNER then LEFT and LEFT then INNER, one over a
                                // unique build, and one with a post-join filter that reads both
                                // inputs of the first join.
                                "SELECT l.v, r.w, b.w2, c.w3 FROM l JOIN r ON l.k = r.k JOIN d2 b ON l.lk = b.lk LEFT JOIN d3 c ON l.s = c.s",
                                "SELECT l.v, r.w, e.uw, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN u1 e ON l.ik = e.ik JOIN d2 b ON l.lk = b.lk",
                                "SELECT l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik LEFT JOIN d2 b ON l.lk = b.lk"
                                        + " WHERE b.w2 IS NULL OR b.w2 % 3 <> r.w % 3",
                                // An ON residual on the first later stage, which the workers walk.
                                "SELECT l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik AND a.w1 > r.w % 9 LEFT JOIN d2 b ON l.lk = b.lk",
                                // A projection of l that repeats a column ahead of the later keys.
                                "SELECT l.v2, r.w, a.w1, c.w3 FROM (SELECT v, v v2, ik, s, k, ts FROM l) l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik JOIN d3 c ON l.s = c.s",
                                // Seven later stages, and a LIMIT and a negative LIMIT over them.
                                SEVEN_LATER,
                                SEVEN_LATER + " LIMIT 9",
                                SEVEN_LATER + " LIMIT -9",
                        }) {
                            assertSwappedStack(sql, countMatches(sql, " JOIN "), true);
                        }
                    }
                }
            }
            // The continuations ran.
            try (RecordCursorFactory factory = select(SEVEN_LATER)) {
                final AsyncHashJoinRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                result(factory, sqlExecutionContext);
                Assert.assertTrue(join.getAtom().isSwapped());
                Assert.assertTrue(join.getAtom().getContinuationCount() > 0);
            }
        });
    }

    @Test
    public void testToTopMidStreamSwapped() throws Exception {
        assertMemoryLeak(() -> {
            createTables(12, 40);
            sqlExecutionContext.changePageFrameSizes(2, 3);
            // The first stage walks its matches on the workers, and the later stages hand their
            // chains to the consumer.
            final String sql = "SELECT l.v, r.w, a.w1, b.w2 FROM l JOIN r ON l.k = r.k LEFT JOIN d1 a ON l.ik = a.ik JOIN d2 b ON l.lk = b.lk";
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
                    Assert.assertTrue(join.getAtom().isSwapped());
                    // toTop() before the first read, after a few rows, inside the later stages' chains
                    // and after the last row each read the whole result again, in the same orientation.
                    for (int stop : new int[]{0, 1, 4, 7, 23, Integer.MAX_VALUE}) {
                        final StringSink sink = new StringSink();
                        int rows = 0;
                        while (rows++ < stop && cursor.hasNext()) {
                            CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                        }
                        Assert.assertTrue(expected.startsWith(sink.toString()));
                        cursor.toTop();
                        Assert.assertTrue(join.getAtom().isSwapped());
                    }
                    final StringSink sink = new StringSink();
                    while (cursor.hasNext()) {
                        CursorPrinter.println(cursor.getRecord(), metadata, sink, true);
                    }
                    Assert.assertEquals(expected, sink.toString());
                }
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

    private static void assertHandPlannedExecution(AsyncHashJoinRecordCursorFactory join, boolean isSwapped) throws SqlException {
        try (RecordCursor cursor = join.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(isSwapped, join.getAtom().isSwapped());
        }
    }

    // An execution of the factory presizes the key table of its second stage, a join of d1 on an INT
    // key, by the left input's table, or does not.
    private static void assertLaterStagePresized(RecordCursorFactory factory, AsyncHashJoinRecordCursorFactory join, boolean isPresized) throws SqlException {
        try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
            Assert.assertEquals(isPresized, join.getAtom().getStage(1).isKeyCapacityPresized());
        }
    }

    // The query runs an operator of the given stages, which may swap its first join's inputs, as the
    // master of ordinary joins, and its rows equal the ordinary plan's, in order; returns the plan.
    private static String assertMixedPlan(String sql, int stageCount) throws SqlException {
        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
        Assert.assertEquals(sql + '\n' + plan, stageCount, getStageCount(plan));
        Assert.assertEquals(sql + '\n' + plan, 1, countMatches(plan, "inputSwap"));
        TestUtils.assertContains(plan, INPUT_SWAP);
        Assert.assertTrue(plan, plan.indexOf("Hash Left Outer Join Light") < plan.indexOf("Async Hash Join"));
        return plan;
    }

    // The failure released every slot, reader and build of every stage, and the tuples.
    private static void assertReleased(AsyncHashJoinRecordCursorFactory join) {
        Assert.assertEquals(0, join.getBuildRoundAtom().getPerWorkerLocks().getAcquiredSlotCount());
        Assert.assertEquals(0, join.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
        for (int k = 0, n = join.getSpec().getStageCount(); k < n; k++) {
            Assert.assertNull(join.getAtom().getStage(k).getFrozenBuild());
        }
        Assert.assertEquals(0, join.getAtom().getRing().getAllocatedBytes());
        Assert.assertEquals(0, engine.getBusyReaderCount());
    }

    // The query runs one operator of the given stages, whose rows equal the ordinary plan's, in
    // order, and whose execution took the given orientation; returns the operator's plan.
    private static String assertSwappedStack(String sql, int stageCount, boolean isSwapped) throws SqlException {
        final String plan = assertParallelHashJoin(sql, sqlExecutionContext);
        Assert.assertEquals(sql + '\n' + plan, stageCount, getStageCount(plan));
        Assert.assertEquals(sql + '\n' + plan, 1, countMatches(plan, "Hash Join workers:"));
        try (RecordCursorFactory factory = select(sql)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            assertExecution(factory, join, sql, isSwapped);
        }
        return plan;
    }

    private static int countMatches(String text, String sub) {
        int count = 0;
        for (int i = text.indexOf(sub); i > -1; i = text.indexOf(sub, i + sub.length())) {
            count++;
        }
        return count;
    }

    // l, the left input of every first join, and r, its right input, with duplicate and NULL keys on
    // both sides and SYMBOL columns over dictionaries that differ; and the dimensions of the later
    // joins: d1 over an INT key, d2 over a staged LONG key, a VARCHAR and a SYMBOL, d3 over a SYMBOL
    // key and a STRING, u1 over unique INT keys and x over l's timestamps. The key columns sit at
    // other positions in every table, so that a key read from another column misreads.
    private static void createTables(int leftRows, int rightRows) throws Exception {
        execute("CREATE TABLE l AS (" +
                "SELECT CASE WHEN x % 7 = 0 THEN NULL ELSE (x % 3)::INT END k, x v, (x % 5)::LONG lk, ('s' || (x % 4))::SYMBOL s," +
                " CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 6)::INT END ik, 'str' || (x % 3) str, ('vc' || (x % 4))::VARCHAR vc," +
                " timestamp_sequence('2024-01-01', 600_000_000) ts" +
                " FROM long_sequence(" + leftRows + ")) TIMESTAMP(ts) PARTITION BY HOUR");
        execute("CREATE TABLE r AS (" +
                "SELECT x w, CASE WHEN x % 5 = 0 THEN NULL ELSE (x % 4)::INT END k, ('s' || (x % 5))::SYMBOL rs," +
                " timestamp_sequence('2023-12-31', 300_000_000) rts" +
                " FROM long_sequence(" + rightRows + ")) TIMESTAMP(rts) PARTITION BY HOUR");
        execute("CREATE TABLE d1 AS (SELECT ('q' || (x % 3))::SYMBOL q, x w1, CASE WHEN x % 6 = 0 THEN NULL ELSE (x % 5)::INT END ik FROM long_sequence(15))");
        execute("CREATE TABLE d2 AS (SELECT x * 10 w2, ('vc' || (x % 3))::VARCHAR vc, ('s' || (3 - x % 4))::SYMBOL q, 1 + x % 4 lk FROM long_sequence(10))");
        execute("CREATE TABLE d3 AS (SELECT x * 100 w3, 'str' || (x % 2) str, ('s' || (4 - x))::SYMBOL s FROM long_sequence(3))");
        execute("CREATE TABLE u1 AS (SELECT x * 3 uw, (x - 1)::INT ik FROM long_sequence(5))");
        execute("CREATE TABLE x AS (SELECT x xv, timestamp_sequence('2024-01-01', 1_200_000_000) xts FROM long_sequence(4))");
    }

    private static IntList identity(int columnCount) {
        final IntList columns = new IntList();
        for (int i = 0; i < columnCount; i++) {
            columns.add(i);
        }
        return columns;
    }

    // An input over the scan that a compiled query wraps, unfiltered, whose compiled columns are the
    // given frame columns; the factory takes the scan, and the wrapper closes it again, a no-op.
    private static HashJoinInput newInput(RecordCursorFactory query, IntList columnMap) {
        return new HashJoinInput(query.getBaseFactory(), columnMap, new AsyncFilterContext(configuration, null, null, null, null, null, null, 1, 0, 0, 0));
    }

    // Keys of one frame column of the probe and one of the build, each read with its frame type.
    private static HashJoinGroupByKeys newKeys(RecordMetadata probeMetadata, int[] probeColumns, RecordMetadata buildMetadata, int[] buildColumns) {
        final HashJoinGroupByKeys keys = new HashJoinGroupByKeys();
        for (int i = 0; i < probeColumns.length; i++) {
            Assert.assertTrue(keys.add(probeColumns[i], probeMetadata.getColumnType(probeColumns[i]), buildColumns[i], buildMetadata.getColumnType(buildColumns[i])));
        }
        return keys;
    }

    // A later stage of the remapped stack: its keys over the left input's frame columns, and, as a
    // swapped execution reads them, over the first stage's payload, whose columns lead the master's
    // metadata; the stack takes it with the joined rows' metadata.
    private static void addRemappedStage(
            AsyncHashJoinSpec spec,
            BytecodeAssembler asm,
            RecordCursorFactory buildQuery,
            String alias,
            boolean isOuter,
            int[] frameColumns,
            int[] payloadColumns,
            int[] buildColumns
    ) throws SqlException {
        final RecordMetadata leftFrameMetadata = spec.getLeftInput().getMetadata();
        final RecordMetadata masterMetadata = spec.getMetadata();
        final RecordMetadata buildMetadata = buildQuery.getMetadata();
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, asm, newKeys(leftFrameMetadata, frameColumns, buildMetadata, buildColumns),
                null, leftFrameMetadata, identity(leftFrameMetadata.getColumnCount()), alias, buildMetadata, identity(buildMetadata.getColumnCount()));
        final HashJoinKeyLayout swapKeySourceLayout = new HashJoinKeyLayout(configuration, asm, newKeys(masterMetadata, payloadColumns, buildMetadata, buildColumns),
                null, masterMetadata, identity(masterMetadata.getColumnCount()), alias, buildMetadata, identity(buildMetadata.getColumnCount()));
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, masterMetadata.getColumnCount() + buildMetadata.getColumnCount());
        metadata.copyColumnMetadataFrom(null, masterMetadata);
        metadata.copyColumnMetadataFrom(alias, buildMetadata);
        metadata.setTimestampIndex(masterMetadata.getTimestampIndex());
        spec.addStage(
                new HashJoinStageSpec(newInput(buildQuery, identity(buildMetadata.getColumnCount())), keyLayout, isOuter, alias, null, null, null, swapKeySourceLayout),
                metadata
        );
    }

    // A stack of l JOIN r ON k, then d1 ON ik, d3 ON s and d2 ON lk and s, planned by hand over a left
    // input that reads l's scan, whose frames hold k, v, lk, s, ik, str, vc and ts, as the columns
    // ik, k, s, lk, v and ts, in that order. The first join may swap its inputs.
    private static AsyncHashJoinRecordCursorFactory newRemappedStack(
            RecordCursorFactory leftQuery,
            RecordCursorFactory leftProjection,
            RecordCursorFactory rightQuery,
            RecordCursorFactory d1Query,
            RecordCursorFactory d3Query,
            RecordCursorFactory d2Query
    ) throws SqlException {
        final BytecodeAssembler asm = new BytecodeAssembler();
        final RecordMetadata leftMetadata = leftQuery.getMetadata();
        final RecordMetadata rightMetadata = rightQuery.getMetadata();
        final IntList leftColumnMap = new IntList();
        for (String column : new String[]{"ik", "k", "s", "lk", "v", "ts"}) {
            leftColumnMap.add(leftMetadata.getColumnIndex(column));
        }
        Assert.assertEquals("[4,0,3,2,1,7]", leftColumnMap.toString());
        final int leftKey = leftMetadata.getColumnIndex("k");
        final int rightKey = rightMetadata.getColumnIndex("k");
        final IntList leftFrameColumns = identity(leftMetadata.getColumnCount());
        final IntList rightColumns = identity(rightMetadata.getColumnCount());
        final HashJoinKeyLayout keyLayout = new HashJoinKeyLayout(configuration, asm, newKeys(leftMetadata, new int[]{leftKey}, rightMetadata, new int[]{rightKey}),
                "l", leftMetadata, leftFrameColumns, "r", rightMetadata, rightColumns);
        final HashJoinKeyLayout swapKeyLayout = new HashJoinKeyLayout(configuration, asm, newKeys(rightMetadata, new int[]{rightKey}, leftMetadata, new int[]{leftKey}),
                "r", rightMetadata, rightColumns, "l", leftMetadata, leftFrameColumns);
        // The joined rows lead with the left input's columns as compiled, which the projection lists.
        final RecordMetadata projectionMetadata = leftProjection.getMetadata();
        final JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, projectionMetadata.getColumnCount() + rightMetadata.getColumnCount());
        metadata.copyColumnMetadataFrom("l", projectionMetadata);
        metadata.copyColumnMetadataFrom("r", rightMetadata);
        metadata.setTimestampIndex(projectionMetadata.getTimestampIndex());
        final RecordCursorFactory leftScan = leftQuery.getBaseFactory();
        final AsyncHashJoinSpec spec = new AsyncHashJoinSpec(
                newInput(leftQuery, leftColumnMap),
                new HashJoinStageSpec(newInput(rightQuery, rightColumns), keyLayout, swapKeyLayout, false, "r.k=l.k", metadata, null, null),
                metadata,
                leftScan.followedOrderByAdvice(),
                leftScan.getScanDirection()
        );
        try {
            final RecordMetadata d1 = d1Query.getMetadata();
            final RecordMetadata d3 = d3Query.getMetadata();
            final RecordMetadata d2 = d2Query.getMetadata();
            // ik is frame column 4 and payload column 0, s frame column 3 and payload column 2, and lk
            // frame column 2 and payload column 3.
            addRemappedStage(spec, asm, d1Query, "a", false, new int[]{4}, new int[]{0}, new int[]{d1.getColumnIndex("ik")});
            addRemappedStage(spec, asm, d3Query, "c", true, new int[]{3}, new int[]{2}, new int[]{d3.getColumnIndex("s")});
            addRemappedStage(spec, asm, d2Query, "h", true, new int[]{2, 3}, new int[]{3, 2}, new int[]{d2.getColumnIndex("lk"), d2.getColumnIndex("q")});
            return new AsyncHashJoinRecordCursorFactory(
                    engine,
                    configuration,
                    engine.getMessageBus(),
                    spec,
                    () -> new PageFrameReduceTask(configuration, MemoryTag.NATIVE_OFFLOAD),
                    1
            );
        } catch (Throwable th) {
            spec.close();
            throw th;
        }
    }

    private static void dropTables() throws Exception {
        for (String table : new String[]{"l", "r", "d1", "d2", "d3", "u1", "x"}) {
            execute("DROP TABLE " + table);
        }
    }

    // The stages that the operator's plan names.
    private static int getStageCount(String plan) {
        final int start = plan.indexOf("stages: ") + "stages: ".length();
        int end = start;
        while (end < plan.length() && Character.isDigit(plan.charAt(end))) {
            end++;
        }
        return Integer.parseInt(plan.substring(start, end));
    }

    // True when the plan sorts the rows: by a sort of its own, or by encoded sort keys.
    private static boolean isSorted(String plan) {
        return plan.contains("Sort") || plan.contains("sort");
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
}
