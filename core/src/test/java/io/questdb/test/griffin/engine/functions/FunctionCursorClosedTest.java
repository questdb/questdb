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

package io.questdb.test.griffin.engine.functions;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.InsertOperation;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.bool.InSymbolCursorFunctionFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.sql.async.SlotGatedWorkStealingStrategy;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * A cached factory (SELECT) or operation (INSERT ... SELECT) outlives its cursor. A function that
 * builds state which grows with the data it reads - a cast-to-symbol dictionary, the value set of
 * {@code sym IN (SELECT ...)} - must drop that state when the cursor closes, instead of keeping it
 * until the next execution re-initializes the function.
 */
public class FunctionCursorClosedTest extends AbstractCairoTest {
    private static final String ISSUE_CTES = """
            WITH ref AS (
                SELECT cast(k AS SYMBOL) k, v1
                FROM ref_table
                LATEST ON ts PARTITION BY k
            ),
            a AS (
                (SELECT t1.c1, cast(t1.k AS SYMBOL) k, t1.ts, ref.v1
                 FROM t1 INNER JOIN ref ON t1.k = ref.k) timestamp(ts)
            ),
            b AS (
                (SELECT t2.c1, cast(t2.k AS SYMBOL) k, t2.ts
                 FROM t2 INNER JOIN ref ON t2.k = ref.k) timestamp(ts)
            )
            """;
    private static final String ISSUE_PROJECTION = """
            SELECT a.c1, a.k, a.ts, b.c1 c2
            FROM a ASOF JOIN b ON (a.k = b.k)""";

    @Test
    public void testAsyncFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelFilterEnabled(true);
            assertOwnerReleasesCastDictionaries("select v from t where cast(v as symbol) <> 'k7'", "Async Filter");
        });
    }

    @Test
    public void testAsyncGroupByFilterReleasesCastDictionary() throws Exception {
        // A cast to symbol is not parallel-safe, so parallel GROUP BY keeps it out of its keys and
        // aggregates; it reaches the parallel atoms through the filter they take over.
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertOwnerReleasesCastDictionaries(
                    "select s, count() from t where cast(v as symbol) <> 'k7' order by s",
                    "Async Group By"
            );
        });
    }

    @Test
    public void testAsyncGroupByNotKeyedFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertOwnerReleasesCastDictionaries("select max(l) from t where cast(v as symbol) <> 'k7'", "Async Group By");
        });
    }

    @Test
    public void testAsyncGroupingSetsFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertOwnerReleasesCastDictionaries(
                    "select s, max(l) m from t where cast(v as symbol) <> 'k7' group by rollup(s) order by s",
                    "groupingSets: true"
            );
        });
    }

    @Test
    public void testAsyncTopKFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            assertOwnerReleasesCastDictionaries("select l from t where cast(v as symbol) <> 'k7' order by l desc limit 5", "Async Top K");
        });
    }

    @Test
    public void testAsyncWindowJoinFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelWindowJoinEnabled(true);
            assertOwnerReleasesCastDictionaries(
                    "select a.l, sum(b.l) m from t a window join t b on cast(a.v as symbol) = cast(b.v as symbol) range between 1 second preceding and 2 second following",
                    "Async Window Join"
            );
        });
    }

    @Test
    public void testCastToSymbolInsertAsSelectReleasesDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            createIssueTables();
            // the fixed symbol capacity keeps the writer's metadata version stable, so that the
            // compiled operation stays valid for a second execution
            execute("create table target (c1 long, k symbol capacity 2048, ts timestamp, c2 long) timestamp(ts) partition by day");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cq = compiler.compile(ISSUE_CTES + "INSERT INTO target " + ISSUE_PROJECTION, sqlExecutionContext);
                Assert.assertEquals(CompiledQuery.INSERT_AS_SELECT, cq.getType());
                try (InsertOperation op = cq.popInsertOperation()) {
                    // testCastToSymbolSelectReleasesDictionaries shows that this SELECT builds native
                    // dictionaries while its cursor is open
                    for (int run = 0; run < 2; run++) {
                        final long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
                        op.execute(sqlExecutionContext).await();
                        Assert.assertEquals(
                                "the executed operation must free the native cast-to-symbol dictionaries",
                                nativeBaseline,
                                Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS)
                        );
                    }
                }
            }
            // both executions of the cached operation inserted every row of the ASOF JOIN
            assertQuery("select count() from target").noRandomAccess().expectSize().returns("""
                    count
                    8000
                    """);
        });
    }

    @Test
    public void testCastToSymbolSelectReleasesDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            createIssueTables();
            assertSelectReleasesCastDictionaries(ISSUE_CTES + ISSUE_PROJECTION);
        });
    }

    @Test
    public void testCastToSymbolUnderMultiArgFunctionReleasesDictionary() throws Exception {
        // concat() is a MultiArgFunction, so cursorClosed() only reaches the cast if it forwards
        assertMemoryLeak(() -> {
            execute("create table t as (select ('k' || x)::varchar v from long_sequence(1000))");
            assertSelectReleasesCastDictionaries("select concat(cast(v as symbol), '-') c from t");
        });
    }

    @Test
    public void testCoveringIndexFilterInitFailureLeavesFreedFilterUntouched() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table tc (
                        ts timestamp,
                        s symbol index type posting include (s2, k),
                        s2 symbol,
                        k symbol
                    ) timestamp(ts) partition by day bypass wal""");
            execute("""
                    insert into tc select (x * 1000000)::timestamp, 's' || (x % 3), 's' || (x % 2), 's' || (x % 5)
                    from long_sequence(100)""");
            assertFilterInitFailureLeavesFreedFilterUntouched(
                    "select s, s2, k from tc where s = 's1' and s = s2 and k in (select k from r) latest on ts partition by s",
                    "CoveringIndex"
            );
        });
    }

    @Test
    public void testFilterOnExcludedValuesInitFailureLeavesFreedFilterUntouched() throws Exception {
        assertMemoryLeak(() -> {
            createTwoSymbolTable();
            assertFilterInitFailureLeavesFreedFilterUntouched(
                    "select * from ti2 where s not in ('s1') and s = s2 and s2 in (select k from r)",
                    "FilterOnExcludedValues"
            );
        });
    }

    @Test
    public void testFilterOnExcludedValuesReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTable();
            sqlExecutionContext.setParallelFilterEnabled(false);
            assertOwnerReleasesCastDictionaries("select * from ti where s not in ('s1') and cast(v as symbol) <> 'k7'", "FilterOnExcludedValues");
        });
    }

    @Test
    public void testFilterOnSubQueryInitFailureLeavesFreedFilterUntouched() throws Exception {
        assertMemoryLeak(() -> {
            createTwoSymbolTable();
            execute("create table r2 as (select ('s' || x)::symbol k from long_sequence(2))");
            assertFilterInitFailureLeavesFreedFilterUntouched(
                    "select * from ti2 where s in (select k from r2) and s = s2 and s2 in (select k from r)",
                    "FilterOnSubQuery"
            );
        });
    }

    @Test
    public void testFilterOnValuesInitFailureLeavesFreedFilterUntouched() throws Exception {
        assertMemoryLeak(() -> {
            createTwoSymbolTable();
            assertFilterInitFailureLeavesFreedFilterUntouched(
                    "select * from ti2 where s in ('s1', 's2') and s = s2 and s2 in (select k from r)",
                    "FilterOnValues"
            );
        });
    }

    @Test
    public void testFilterOnValuesReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTable();
            sqlExecutionContext.setParallelFilterEnabled(false);
            assertOwnerReleasesCastDictionaries("select * from ti where s in ('s1', 's2') and cast(v as symbol) <> 'k7'", "FilterOnValues");
        });
    }

    @Test
    public void testFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelFilterEnabled(false);
            assertOwnerReleasesCastDictionaries("select v from t where cast(v as symbol) <> 'k7'", "Filter filter");
        });
    }

    @Test
    public void testGroupByKeyReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(false);
            assertOwnerReleasesCastDictionaries("select cast(v as symbol) k, count() from t order by k", "GroupBy vectorized: false");
        });
    }

    @Test
    public void testGroupByNotKeyedReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(false);
            assertOwnerReleasesCastDictionaries("select max(length(cast(v as symbol))) from t", "GroupBy vectorized: false");
        });
    }

    @Test
    public void testGroupingSetsKeyReleasesCastDictionary() throws Exception {
        // ROLLUP, CUBE and GROUPING SETS evaluate a prefix key themselves. A cast to symbol is not
        // parallel-safe, so they run serially even with parallel GROUP BY enabled.
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertOwnerReleasesCastDictionaries("select cast(v as symbol) k, s, count() c from t group by k, rollup(s)", "groupingSets: true");
            assertOwnerReleasesCastDictionaries("select cast(v as symbol) k, s, count() c from t group by k, cube(s)", "groupingSets: true");
            assertOwnerReleasesCastDictionaries("select cast(v as symbol) k, s, count() c from t group by grouping sets ((k, s), (k))", "groupingSets: true");
            assertOwnerReleasesCastDictionaries(
                    "select ts, cast(v as symbol) k, s, count() c from t sample by 1h grouping sets ((k, s), (k)) fill(null)",
                    "groupingSets: true"
            );
            // the next statement takes a query tracker from the pool the queries' trackers went back to
            assertQuery("select count() from t").noRandomAccess().expectSize().returns("""
                    count
                    1000
                    """);
        });
    }

    @Test
    public void testHashOuterJoinFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            for (String join : new String[]{"left", "right", "full"}) {
                assertOwnerReleasesCastDictionaries(
                        "select a.l, b.l from t a " + join + " join t b on a.l = b.l and cast(a.v as symbol) <> 'k1'",
                        "Outer Join Light"
                );
                // a varchar join key takes the non-light hash join
                assertOwnerReleasesCastDictionaries(
                        "select a.l, b.l from t a " + join + " join t b on a.v = b.v and a.l = b.l and cast(a.v as symbol) <> 'k1'",
                        "Outer Join"
                );
            }
        });
    }

    @Test
    public void testHorizonJoinReleasesCastDictionary() throws Exception {
        // a cast to symbol is not parallel-safe, so these run on the single-threaded horizon factories
        assertMemoryLeak(() -> {
            createVarcharTable();
            final String keyed = "select cast(a.v as symbol) k, count() c from t a horizon join t b on (s) list (0s, 1s) as h order by k";
            assertQuery(keyed).noLeakCheck().assertsPlanNotContaining("Async");
            assertOwnerReleasesCastDictionaries(keyed, "Horizon Join");
            final String notKeyed = "select max(length(cast(b.v as symbol))) m from t a horizon join t b on (s) list (0s, 1s) as h";
            assertQuery(notKeyed).noLeakCheck().assertsPlanNotContaining("Async");
            assertOwnerReleasesCastDictionaries(notKeyed, "Horizon Join");
        });
    }

    @Test
    public void testHorizonJoinWithoutAggregatesFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            final String keyed = "select h.offset o, a.ts, b.l from t a horizon join t b on (s) list (0s, 1s) as h" +
                    " where a.v::symbol = 'k1' order by a.ts, o";
            assertOwnerReleasesCastDictionaries(keyed, "Async Horizon Join Projection");
            final String notKeyed = "select h.offset o, a.ts, b.l from t a horizon join t b list (0s, 1s) as h" +
                    " where a.v::symbol = 'k1' order by a.ts, o";
            assertOwnerReleasesCastDictionaries(notKeyed, "Async Horizon Join Projection");
        });
    }

    @Test
    public void testInSymbolCursorReleasesValueSet() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select ('s' || (x % 100))::symbol s, ('s' || x)::varchar v from long_sequence(1000))");
            execute("create table r as (select ('s' || x)::varchar s from long_sequence(50))");
            // a static symbol column takes the key-set variant, a cast to symbol the value-set one
            final String sql = "select s in (select s from r) a, cast(v as symbol) in (select s from r) b from t";
            try (RecordCursorFactory factory = select(sql)) {
                final ObjList<Function> functions = findVirtualFactory(factory).getFunctions();
                Assert.assertEquals(2, functions.size());
                String firstRun = null;
                for (int run = 0; run < 2; run++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        println(factory, cursor);
                        for (int i = 0; i < 2; i++) {
                            // every one of the subquery's 50 values is in the set
                            Assert.assertEquals(50, InSymbolCursorFunctionFactory.getValueCount(functions.getQuick(i)));
                        }
                    }
                    for (int i = 0; i < 2; i++) {
                        Assert.assertEquals(
                                "cursor close must drop the subquery's values",
                                0,
                                InSymbolCursorFunctionFactory.getValueCount(functions.getQuick(i))
                        );
                    }
                    // the cached factory re-executes after the release and returns the same rows
                    if (firstRun == null) {
                        firstRun = sink.toString();
                    } else {
                        Assert.assertEquals(firstRun, sink.toString());
                    }
                }
            }
        });
    }

    @Test
    public void testIndexedFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTable();
            sqlExecutionContext.setParallelFilterEnabled(false);
            assertOwnerReleasesCastDictionaries("select * from ti where s = 's1' and cast(v as symbol) <> 'k7'", "Index forward scan on: s");
        });
    }

    @Test
    public void testLatestByIndexedFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createIndexedTable();
            assertOwnerReleasesCastDictionaries(
                    "select * from ti where s = 's1' and cast(v as symbol) <> 'k7' latest on ts partition by s",
                    "Index backward scan on: s"
            );
        });
    }

    @Test
    public void testMultiHorizonJoinReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            final String keyed = "select cast(a.v as symbol) k, count() c from t a horizon join t b on (s) horizon join t d on (s) list (0s, 1s) as h order by k";
            assertQuery(keyed).noLeakCheck().assertsPlanNotContaining("Async");
            assertOwnerReleasesCastDictionaries(keyed, "Multi Horizon Join");
            final String notKeyed = "select max(length(cast(b.v as symbol))) m from t a horizon join t b on (s) horizon join t d on (s) list (0s, 1s) as h";
            assertQuery(notKeyed).noLeakCheck().assertsPlanNotContaining("Async");
            assertOwnerReleasesCastDictionaries(notKeyed, "Multi Horizon Join");
        });
    }

    @Test
    public void testNestedLoopJoinFilterReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            for (String join : new String[]{"left", "right", "full"}) {
                assertOwnerReleasesCastDictionaries(
                        "select a.l, b.l from t a " + join + " join (select * from t where l < 10) b on cast(a.v as symbol) <> cast(b.v as symbol)",
                        "Nested Loop"
                );
            }
        });
    }

    @Test
    public void testNumericCastsToSymbolReleaseDictionaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    create table t as (
                        select x l, x::int i, x::short s, x::byte b, x::double d, x::float f,
                               cast(x as char) c, x::date dt, x::timestamp ts
                        from long_sequence(100)
                    )""");
            // one cast per query, so that each cast's native dictionary is checked on its own
            for (String column : new String[]{"l", "i", "s", "b", "d", "f", "c", "dt", "ts"}) {
                assertSelectReleasesCastDictionaries("select cast(" + column + " as symbol) " + column + " from t");
            }
        });
    }

    @Test
    public void testParallelWorkersReleaseCastDictionaries() throws Exception {
        // Each worker evaluates the filter with a clone of its own, and the clone builds a dictionary
        // of its own. The latch holds the query owner back until a worker has taken a slot, so that
        // a worker reduces at least one frame. The frame sequence reads the factory provider when the
        // query compiles; the overrides reset it after the test.
        factoryProvider = SlotGatedWorkStealingStrategy.newFactoryProvider();
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MIN_ROWS, 100);
        setProperty(PropertyKey.CAIRO_SMALL_SQL_PAGE_FRAME_MAX_ROWS, 100);
        assertMemoryLeak(() -> {
            final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
            TestUtils.execute(
                    pool,
                    (engine, compiler, context) -> {
                        engine.execute("""
                                create table t as (
                                    select ('k' || (x % 300))::varchar v, ('s' || (x % 3))::symbol s, x l, (x * 1_000_000)::timestamp ts
                                    from long_sequence(10_000)
                                ) timestamp(ts) partition by hour""", context);
                        final String[] queries = {
                                "select v from t where cast(v as symbol) <> 'k7'",
                                "select s, count() c from t where cast(v as symbol) <> 'k7' order by s",
                                "select max(l) m from t where cast(v as symbol) <> 'k7'",
                                "select l from t where cast(v as symbol) <> 'k7' order by l desc limit 5",
                                "select h.offset o, a.l from t a horizon join t b on (s) list (0s, 1s) as h where cast(a.v as symbol) <> 'k7'"
                        };
                        for (String sql : queries) {
                            try (RecordCursorFactory factory = compiler.compile(sql, context).getRecordCursorFactory()) {
                                final PerWorkerLocks workerLocks = TestUtils.findPerWorkerLocks(factory, sql);
                                for (int run = 0; run < 2; run++) {
                                    final long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
                                    // a fresh latch per open, so that the acquisition it records belongs to this open
                                    final CountDownLatch acquired = new CountDownLatch(1);
                                    workerLocks.setTestAcquireLatch(acquired);
                                    final MemoryTracker tracker;
                                    try (RecordCursor cursor = factory.getCursor(context)) {
                                        tracker = context.getMemoryTracker();
                                        Assert.assertNotNull(tracker);
                                        TestUtils.drainCursor(cursor);
                                    } finally {
                                        workerLocks.setTestAcquireLatch(null);
                                    }
                                    Assert.assertEquals("no worker reduced a frame: " + sql, 0, acquired.getCount());
                                    assertCastDictionariesReleased(nativeBaseline, tracker);
                                }
                            }
                        }
                    },
                    configuration,
                    LOG
            );
        });
    }

    @Test
    public void testSampleByReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            // ALIGN TO FIRST OBSERVATION keeps the sample-by cursors, which evaluate the aggregates
            assertOwnerReleasesCastDictionaries(
                    "select ts, max(length(cast(v as symbol))) m from t sample by 1m align to first observation",
                    "Sample By"
            );
            assertOwnerReleasesCastDictionaries(
                    "select ts, s, max(length(cast(v as symbol))) m from t sample by 1m fill(5) align to first observation",
                    "Sample By"
            );
            // aligned to calendar, SAMPLE BY runs as GROUP BY with the cast as a key
            sqlExecutionContext.setParallelGroupByEnabled(false);
            assertOwnerReleasesCastDictionaries("select ts, cast(v as symbol) k, count() c from t sample by 1m", "GroupBy");
        });
    }

    @Test
    public void testUnionReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            assertOwnerReleasesCastDictionaries(
                    "select cast(v as symbol) k from t union select cast(v as symbol) k from t where l < 10",
                    "Union"
            );
            assertOwnerReleasesCastDictionaries(
                    "select cast(v as symbol) k from t union all select cast(v as symbol) k from t where l < 10",
                    "Union All"
            );
        });
    }

    @Test
    public void testVarcharCastToSymbolReleasesDictionary() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table t as (select ('k' || (x % 300))::varchar v from long_sequence(1000))");
            assertSelectReleasesCastDictionaries("select cast(v as symbol) s from t");
        });
    }

    @Test
    public void testWindowJoinReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelWindowJoinEnabled(false);
            assertOwnerReleasesCastDictionaries(
                    "select a.l, sum(length(cast(b.v as symbol))) m from t a window join t b range between 1 second preceding and 2 second following",
                    "Window Join"
            );
            assertOwnerReleasesCastDictionaries(
                    "select a.l, sum(b.l) m from t a window join t b on cast(a.v as symbol) = cast(b.v as symbol) range between 1 second preceding and 2 second following",
                    "Window Join"
            );
        });
    }

    @Test
    public void testWindowReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            assertOwnerReleasesCastDictionaries(
                    "select l, row_number() over (partition by cast(v as symbol) order by ts) rn from t",
                    "Window"
            );
        });
    }

    // An owner opens its cursor and then initializes the filter, so a filter whose init fails leaves
    // that cursor open. The factory may free the filter before the cursor, and the cursor must not
    // notify the freed filter then: EqSym's cursorClosed() would reallocate the lookup cache its
    // close() just freed, which assertMemoryLeak reports.
    private void assertFilterInitFailureLeavesFreedFilterUntouched(String sql, String expectedPlanFragment) throws Exception {
        execute("create table r as (select ('s' || x)::symbol k from long_sequence(2))");
        assertQuery(sql).noLeakCheck().assertsPlanContaining(expectedPlanFragment);
        try (RecordCursorFactory factory = select(sql)) {
            // a recreated table fails the init of the IN (SELECT ...) conjunct
            execute("drop table r");
            execute("create table r as (select ('s' || x)::symbol k from long_sequence(2))");
            try {
                factory.getCursor(sqlExecutionContext).close();
                Assert.fail("the filter's init must fail");
            } catch (TableReferenceOutOfDateException expected) {
                // the factory closes next, with the cursor still open
            }
        }
    }

    // An owner (filter, group by, join, ...) initializes the functions it evaluates, so it must also
    // notify them when its cursor closes.
    private void assertOwnerReleasesCastDictionaries(String sql, String expectedPlanFragment) throws Exception {
        assertQuery(sql).noLeakCheck().assertsPlanContaining(expectedPlanFragment);
        assertSelectReleasesCastDictionaries(sql);
    }

    // A cast to symbol keeps its dictionary in native memory, charged to the query's memory tracker.
    // The cursor close must free it before the tracker goes back to its pool.
    private static void assertCastDictionariesReleased(long nativeBaseline, MemoryTracker tracker) {
        Assert.assertEquals(
                "cursor close must free the native cast-to-symbol dictionaries",
                nativeBaseline,
                Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS)
        );
        Assert.assertEquals("the query's tracker went back to its pool with memory charged to it", 0, tracker.getUsed());
    }

    private static void assertSelectReleasesCastDictionaries(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            String firstRun = null;
            for (int run = 0; run < 2; run++) {
                final long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
                final MemoryTracker tracker;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertNotNull(tracker);
                    println(factory, cursor);
                    Assert.assertTrue(
                            "an open cursor must hold its cast-to-symbol dictionaries in native memory",
                            Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS) > nativeBaseline
                    );
                }
                assertCastDictionariesReleased(nativeBaseline, tracker);
                // the cached factory re-executes after the release and returns the same rows
                if (firstRun == null) {
                    firstRun = sink.toString();
                } else {
                    Assert.assertEquals(firstRun, sink.toString());
                }
            }
        }
    }

    private static void createIndexedTable() throws Exception {
        execute("""
                create table ti as (
                    select ('s' || (x % 3))::symbol s, ('k' || (x % 300))::varchar v, (x * 1000000)::timestamp ts
                    from long_sequence(1000)
                ), index(s) timestamp(ts) partition by hour""");
    }

    private static void createIssueTables() throws Exception {
        execute("create table ref_table (k varchar, v1 long, ts timestamp) timestamp(ts) partition by day");
        execute("insert into ref_table select ('k' || x)::varchar, x, (x * 1000000)::timestamp from long_sequence(1000)");
        execute("insert into ref_table select ('k' || x)::varchar, x + 1, (x * 1000000 + 2000000000)::timestamp from long_sequence(1000)");
        execute("create table t1 (c1 long, k varchar, ts timestamp) timestamp(ts) partition by day");
        execute("insert into t1 select x, ('k' || (1 + x % 1000))::varchar, (x * 1000000)::timestamp from long_sequence(4000)");
        execute("create table t2 (c1 long, k varchar, ts timestamp) timestamp(ts) partition by day");
        execute("insert into t2 select x, ('k' || (1 + x % 1000))::varchar, (x * 1000000)::timestamp from long_sequence(4000)");
    }

    private static void createTwoSymbolTable() throws Exception {
        execute("""
                create table ti2 as (
                    select ('s' || (x % 3))::symbol s, ('s' || (x % 2))::symbol s2, (x * 1000000)::timestamp ts
                    from long_sequence(1000)
                ), index(s) timestamp(ts) partition by hour""");
    }

    private static void createVarcharTable() throws Exception {
        execute("""
                create table t as (
                    select ('k' || (x % 300))::varchar v, ('s' || (x % 3))::symbol s, x l, (x * 1000000)::timestamp ts
                    from long_sequence(1000)
                ) timestamp(ts) partition by hour""");
    }

    private static VirtualRecordCursorFactory findVirtualFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof VirtualRecordCursorFactory virtualFactory) {
                return virtualFactory;
            }
        }
        throw new AssertionError("no virtual factory in the tree, top was " + factory.getClass().getSimpleName());
    }
}
