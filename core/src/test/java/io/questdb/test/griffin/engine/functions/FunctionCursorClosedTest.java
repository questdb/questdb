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
import io.questdb.cairo.SqlJitMode;
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
    public void testAsyncGroupByProjectionReleasesCastDictionary() throws Exception {
        // abs() keeps the constant out of constant folding, so the cast to symbol runs above the
        // parallel GROUP BY, as a projection function that the GROUP BY cursor initializes. SAMPLE BY
        // FILL and an outer GROUP BY read the symbol by key, which fills the cast's dictionary.
        assertMemoryLeak(() -> {
            createVarcharTable();
            sqlExecutionContext.setParallelGroupByEnabled(true);
            final String sampleBy = "select (abs(0.5))::symbol k, count() c, ts from t sample by 1m fill(null)";
            assertOwnerReleasesCastDictionaries(sampleBy, "Async Group By");
            // rows sit 1s apart from 1s to 1000s, so the 1m buckets run from 00:00 to 16:00
            assertQuery("select k, count() n, sum(c) c from (" + sampleBy + ")").noLeakCheck().expectSize().returns("""
                    k\tn\tc
                    0.5\t17\t1000
                    """);
            final String outerGroupBy = "select k, count() n, sum(c) c from (select (abs(0.5))::symbol k, count() c from t)";
            assertOwnerReleasesCastDictionaries(outerGroupBy, "Async Group By");
            assertQuery(outerGroupBy).noLeakCheck().expectSize().returns("""
                    k\tn\tc
                    0.5\t1\t1000
                    """);
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
    public void testAsyncHorizonJoinProjectionReleasesCastDictionary() throws Exception {
        // the cast to symbol runs above the parallel horizon join, as a projection function that the
        // join cursor initializes; the outer GROUP BY reads the symbol by key
        assertMemoryLeak(() -> {
            createVarcharTable();
            final String sql = "select k, count() n, sum(c) c from (" +
                    "select (abs(0.5))::symbol k, count() c from t a horizon join t b on (s) list (0s, 1s) as h)";
            assertOwnerReleasesCastDictionaries(sql, "Async Horizon Join");
            // each of the 1000 rows of a joins once per offset
            assertQuery(sql).noLeakCheck().expectSize().returns("""
                    k\tn\tc
                    0.5\t1\t2000
                    """);
        });
    }

    @Test
    public void testAsyncHorizonJoinKeysAndAggregatesReleaseCastDictionary() throws Exception {
        // A bare cast to symbol keeps a horizon join off the parallel path, but a wrapper that
        // reports parallel support lets it into the keys and aggregates of the parallel atoms.
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            final String[] joins = {
                    "t a horizon join t b on (s) list (0s, 1s) as h",
                    "t a horizon join t b on (s) horizon join t d on (s) list (0s, 1s) as h"
            };
            final String[] plans = {"Async Horizon Join", "Async Multi Horizon Join"};
            for (int i = 0; i < joins.length; i++) {
                final String join = joins[i];
                final String plan = plans[i];
                final String castKey = "select json_extract(((a.l % 3)::symbol)::varchar, '$') j, count() c from " + join + " order by j";
                assertOwnerReleasesCastDictionaries(castKey, plan);
                // each of the 1000 rows of a joins once per offset
                assertQuery(castKey).noLeakCheck().expectSize().returns("""
                        j\tc
                        0\t666
                        1\t668
                        2\t666
                        """);
                final String castAggregate = "select a.s, sum(case when a.s in ($1::symbol) then 1 else 0 end) c from " + join + " order by a.s";
                assertOwnerReleasesCastDictionaries(castAggregate, plan);
                // $1 casts to the symbol '1', which matches no s value
                assertQuery(castAggregate).noLeakCheck().expectSize().returns("""
                        s\tc
                        s0\t0
                        s1\t0
                        s2\t0
                        """);
                final String castNotKeyed = "select percentile_disc(b.l::double, ($1::symbol)::double) p from " + join;
                assertOwnerReleasesCastDictionaries(castNotKeyed, plan);
                // $1 casts to the 1.0 percentile, which is the largest matched b.l: the last row, l = 1000
                assertQuery(castNotKeyed).noLeakCheck().noRandomAccess().expectSize().returns("""
                        p
                        1000.0
                        """);
            }
        });
    }

    @Test
    public void testAsyncMultiHorizonJoinProjectionReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            final String sql = "select k, count() n, sum(c) c from (" +
                    "select (abs(0.5))::symbol k, count() c from t a horizon join t b on (s) horizon join t d on (s) list (0s, 1s) as h)";
            assertOwnerReleasesCastDictionaries(sql, "Async Multi Horizon Join");
            // each of the 1000 rows of a joins once per offset
            assertQuery(sql).noLeakCheck().expectSize().returns("""
                    k\tn\tc
                    0.5\t1\t2000
                    """);
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
    public void testCastToSymbolInsertValuesReleasesDictionaries() throws Exception {
        // An INSERT ... VALUES initializes its value functions on each execution, so it must also
        // free what they built once the rows are in the writer. A single-choice rnd_varchar() keeps
        // the cast from folding to a constant and still yields a known value. The designated
        // timestamp has a cast of its own, which InsertRowImpl evaluates apart from the other values.
        assertMemoryLeak(() -> {
            execute("create table target (k symbol capacity 2048, v long, ts timestamp) timestamp(ts) partition by day");
            execute("create table target_no_ts (k symbol capacity 2048, v long)");
            final String[] inserts = {
                    "insert into target values (rnd_varchar('a', 'a')::symbol, 1, rnd_varchar('2024-01-01', '2024-01-01')::symbol::timestamp)",
                    "insert into target_no_ts values (rnd_varchar('a', 'a')::symbol, 1), (rnd_varchar('b', 'b')::symbol, 2)"
            };
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String insert : inserts) {
                    final CompiledQuery cq = compiler.compile(insert, sqlExecutionContext);
                    Assert.assertEquals(CompiledQuery.INSERT, cq.getType());
                    try (InsertOperation op = cq.popInsertOperation()) {
                        for (int run = 0; run < 2; run++) {
                            final long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
                            op.execute(sqlExecutionContext).await();
                            Assert.assertEquals(
                                    "the executed insert must free the native cast-to-symbol dictionaries",
                                    nativeBaseline,
                                    Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS)
                            );
                        }
                    }
                }
            }
            // both executions of each cached insert added their rows
            assertQuery("select k, v, ts from target").noLeakCheck().timestamp("ts").expectSize().returns("""
                    k\tv\tts
                    a\t1\t2024-01-01T00:00:00.000000Z
                    a\t1\t2024-01-01T00:00:00.000000Z
                    """);
            assertQuery("select k, v from target_no_ts order by v, k").noLeakCheck().expectSize().returns("""
                    k\tv
                    a\t1
                    a\t1
                    b\t2
                    b\t2
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
    public void testDynamicIntervalReleasesCastDictionary() throws Exception {
        // the interval model evaluates the bound once per execution and keeps only its value
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            assertExecutionReleasesCastDictionaries("select * from t where ts > ($2::symbol)::timestamp", "Interval forward scan");
            assertExecutionReleasesCastDictionaries("select * from t where ts in ($2::symbol)", "Interval forward scan");
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
    public void testInSymbolDeferredValuesReleaseCastDictionary() throws Exception {
        // IN keeps a bind-variable value apart from its constant values and initializes it itself
        assertMemoryLeak(() -> {
            createVarcharTable();
            createIndexedTable();
            bindCastValues();
            for (int parallel = 0; parallel < 2; parallel++) {
                sqlExecutionContext.setParallelFilterEnabled(parallel == 1);
                assertExecutionReleasesCastDictionaries("select * from t where s in ($1::symbol, 's2')", "s in [s2] or s in [$0::long::symbol]");
                assertExecutionReleasesCastDictionaries("select * from t where s not in ($1::symbol)", "not (s in [$0::long::symbol])");
                assertExecutionReleasesCastDictionaries(
                        "select * from ti where s in ($1::symbol, 's2') latest on ts partition by s",
                        "LatestByDeferredListValuesFiltered"
                );
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
    public void testJsonExtractReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            assertOwnerReleasesCastDictionaries("select json_extract(((l % 3)::symbol)::varchar, '$') j from t", "json_extract");
            assertOwnerReleasesCastDictionaries("select json_extract('{\"a\":1}', ($1::symbol)::varchar) j from t", "json_extract");
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
    public void testLimitReleasesCastDictionary() throws Exception {
        // every LIMIT reads its bounds once per execution and keeps only their values
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            assertExecutionReleasesCastDictionaries("select * from t limit ($1::symbol)::int", "Limit value");
            assertExecutionReleasesCastDictionaries("select * from t limit ($1::symbol)::int, ($1::symbol)::int + 3", "Limit left");
            assertExecutionReleasesCastDictionaries("select s, count() c from t group by s order by s limit ($1::symbol)::int", "Encode sort light lo");
            sqlExecutionContext.setParallelTopKEnabled(false);
            assertExecutionReleasesCastDictionaries("select * from t order by v desc limit ($1::symbol)::int", "Encode sort light lo");
            assertExecutionReleasesCastDictionaries("select * from t order by v desc limit ($1::symbol)::int, ($1::symbol)::int + 3", "Encode sort light lo");
            setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, "false");
            assertExecutionReleasesCastDictionaries("select * from t order by v desc limit ($1::symbol)::int", "Sort light lo");
            assertExecutionReleasesCastDictionaries("select * from t order by v desc limit ($1::symbol)::int, ($1::symbol)::int + 3", "Sort light lo");
            sqlExecutionContext.setParallelFilterEnabled(true);
            for (int jit = 0; jit < 2; jit++) {
                sqlExecutionContext.setJitMode(jit == 0 ? SqlJitMode.JIT_MODE_DISABLED : SqlJitMode.JIT_MODE_ENABLED);
                assertExecutionReleasesCastDictionaries(
                        "select * from t where l > 3 limit ($1::symbol)::int",
                        jit == 0 ? "Async Filter" : "Async JIT Filter"
                );
            }
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
    public void testOwnersKeepResultsAcrossExecutions() throws Exception {
        // These owners now notify the functions they initialized when the cursor closes, or right
        // after they read a value once. The functions here hold no cast dictionary, since a cast to
        // symbol does not reach these plans, so the queries check that each execution of the cached
        // factory still returns the same rows after that notification.
        assertMemoryLeak(() -> {
            createVarcharTable();
            execute("""
                    create table tl as (
                        select ('k' || (x % 300))::varchar v, ('s' || (x % 3))::symbol s, x l, (x * 1000000)::timestamp ts
                        from long_sequence(1000)
                    ), index(s) timestamp(ts) partition by hour""");
            execute("create table tc (v varchar, s symbol index type posting include (v), l long, ts timestamp) timestamp(ts) partition by hour");
            execute("insert into tc select ('k' || (x % 300))::varchar, ('s' || (x % 3))::symbol, x, (x * 1000000)::timestamp from long_sequence(1000)");
            bindCastValues();
            // tl holds x = 1..1000 with s = 's' || (x % 3)
            assertQuery("select l from tl where s = 's2' and l < 10").noLeakCheck().withPlanContaining("deferred: true").returns("""
                    l
                    2
                    5
                    8
                    """);
            assertQuery("select l from tl where s = $4 and l < 6").noLeakCheck().withPlanContaining("deferred: true").returns("""
                    l
                    1
                    4
                    """);
            assertQuery("select l from tl where s in ('s1', 's2') and l < 6").noLeakCheck().withPlanContaining("FilterOnValues").returns("""
                    l
                    1
                    2
                    4
                    5
                    """);
            assertQuery("select l from tl where s in ('s1', 's2') and l < 6 order by ts desc").noLeakCheck()
                    .withPlanContaining("Cursor-order scan").returns("""
                            l
                            5
                            4
                            2
                            1
                            """);
            assertQuery("select l from tl where s != 's0' and l < 6").noLeakCheck().withPlanContaining("FilterOnExcludedValues").returns("""
                    l
                    1
                    2
                    4
                    5
                    """);
            assertQuery("select l from tl where s = 's2' latest on ts partition by s").noLeakCheck().withPlanContaining("deferred: true").returns("""
                    l
                    998
                    """);
            assertQuery("select l from tl where s = 'zz' latest on ts partition by s").noLeakCheck().withPlanContaining("deferred: true").returns("""
                    l
                    """);
            assertQuery("select v from tc where s in ('s1', 's2') and l < 6").noLeakCheck().returns("""
                    v
                    k1
                    k2
                    k4
                    k5
                    """);
            assertQuery("select l from tl where s in ($4, 's2') latest on ts partition by s order by l").noLeakCheck().expectSize().returns("""
                    l
                    998
                    1000
                    """);
            assertQuery("select l from t where s in ($4, 's2') latest on ts partition by s order by l").noLeakCheck().expectSize().returns("""
                    l
                    998
                    1000
                    """);
            assertQuery("select l from t where s = 'zz' latest on ts partition by s").noLeakCheck().returns("""
                    l
                    """);
            assertQuery("select l from t where s = $4 latest on ts partition by s").noLeakCheck().returns("""
                    l
                    1000
                    """);
            assertQuery("select v from tc where s in ($4, 's2') and l < 6").noLeakCheck().returns("""
                    v
                    k1
                    k2
                    k4
                    k5
                    """);
            assertQuery("select l from tl where s = $4 limit 2").noLeakCheck().returns("""
                    l
                    1
                    4
                    """);
            // ts + 1m >= 10m keeps the rows from 9m on: x = 540..1000
            assertQuery("select count() from t where dateadd('m', 1, ts) >= (select (($1::symbol)::long * 600_000_000)::timestamp)").noLeakCheck().expectSize().noRandomAccess().returns("""
                    count
                    461
                    """);
            // the gate hands page frames to the parallel GROUP BY
            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertQuery("select s, count() c from t where $1::symbol = '1' order by s").noLeakCheck().expectSize().returns("""
                    s\tc
                    s0\t333
                    s1\t334
                    s2\t333
                    """);
            assertQuery("select l from tl where l > 1 latest on ts partition by s order by l").noLeakCheck().expectSize().returns("""
                    l
                    998
                    999
                    1000
                    """);
            assertQuery("select ts, first(l) f, last(l) la from tl where s = 's1' sample by 5m align to first observation").noLeakCheck().noRandomAccess().timestamp("ts").returns("""
                    ts\tf\tla
                    1970-01-01T00:00:01.000000Z\t1\t298
                    1970-01-01T00:05:01.000000Z\t301\t598
                    1970-01-01T00:10:01.000000Z\t601\t898
                    1970-01-01T00:15:01.000000Z\t901\t1000
                    """);
            assertQuery("select v from t except select v from t where l > 3").noLeakCheck().returns("""
                    v
                    """);
            assertQuery("select ts, v from ((select ts, v from t order by ts) union all (select ts, s from t order by ts) order by ts) limit 4").noLeakCheck().expectSize().noRandomAccess().timestamp("ts").returns("""
                    ts\tv
                    1970-01-01T00:00:01.000000Z\tk1
                    1970-01-01T00:00:01.000000Z\ts1
                    1970-01-01T00:00:02.000000Z\tk2
                    1970-01-01T00:00:02.000000Z\ts2
                    """);
            // ts + 1m between 10m and 11m40s keeps x = 540..640
            assertQuery("select count() from t where dateadd('m', 1, ts) between (select (($1::symbol)::long * 600_000_000)::timestamp) and (select (($1::symbol)::long * 700_000_000)::timestamp)")
                    .noLeakCheck().expectSize().noRandomAccess().returns("""
                            count
                            101
                            """);
            assertQuery("select count() from t where $1::symbol = '2'").noLeakCheck().expectSize().noRandomAccess().returns("""
                    count
                    0
                    """);
            assertQuery("select s, count() c from t where $1::symbol = '2' order by s").noLeakCheck().returns("""
                    s\tc
                    """);
            // v = 'k1' sorts first; x = 1, 301, 601 and 901 carry it, and all of them have s = 's1'
            assertQuery("select s, v from tc where s in ($4, 's2') order by v limit 3").noLeakCheck().returns("""
                    s\tv
                    s1\tk1
                    s1\tk1
                    s1\tk1
                    """);
            assertQuery("select count() from ((select ts, v from t order by ts) union all (select ts, v from t order by ts) order by ts)").noLeakCheck().expectSize().noRandomAccess().returns("""
                    count
                    2000
                    """);
            assertExecutionReleasesCastDictionaries("select sparkline(l::double, 0.0, 1000.0, 10) sp from t", "sparkline");
            assertExecutionReleasesCastDictionaries(
                    "select ts, count() c from t sample by 1m from '1970-01-01T00:01:00' fill(null) align to calendar time zone 'Europe/Berlin'",
                    "Sample By Fill"
            );
            // JIT filters that read a bind variable
            assertQuery("select l from tl where l > $1 latest on ts partition by s order by l").noLeakCheck().expectSize().withPlanContaining("jit: true").returns("""
                    l
                    998
                    999
                    1000
                    """);
            assertQuery("select count() from t where s = $4 and l > 0").noLeakCheck().expectSize().noRandomAccess().withPlanContaining("Async JIT Filter").returns("""
                    count
                    334
                    """);
            assertQuery("select count() from t where l > $1").noLeakCheck().expectSize().noRandomAccess().withPlanContaining("Async JIT Filter").returns("""
                    count
                    999
                    """);
            assertQuery("select s, count() c from t where l > $1 order by s").noLeakCheck().expectSize().withPlanContaining("Async JIT Group By").returns("""
                    s\tc
                    s0\t333
                    s1\t333
                    s2\t333
                    """);
            // each of the 999 rows of a that pass the filter joins once per offset
            assertQuery("select count() from (select h.offset o, a.ts, b.l from t a horizon join t b on (s) list (0s, 1s) as h where a.l > $1)")
                    .noLeakCheck().expectSize().noRandomAccess().withPlanContaining("Horizon Join Projection").returns("""
                            count
                            1998
                            """);
            sqlExecutionContext.setParallelWindowJoinEnabled(true);
            assertQuery("select count() from (select a.l, sum(b.l) m from t a window join t b on (s) range between ($1::symbol)::int seconds preceding and 1 second following where a.l > $1)")
                    .noLeakCheck().expectSize().noRandomAccess().withPlanContaining("Async Window Join").returns("""
                            count
                            999
                            """);
            // a CASE over a symbol column with a static symbol table, and a scalar sub-query bound
            assertQuery("select sum(case s when 's1' then 1 else 0 end) c from t").noLeakCheck().expectSize().noRandomAccess().withPlanContaining("switch(s,'s1',1,0)").returns("""
                    c
                    334
                    """);
            assertQuery("select count() from t where ts > (select min(ts) from t)").noLeakCheck().expectSize().noRandomAccess().returns("""
                    count
                    999
                    """);
            // set operations and an ordered UNION ALL, with and without cast functions
            assertQuery("select s from t union select s from t order by s").noLeakCheck().returns("""
                    s
                    s0
                    s1
                    s2
                    """);
            assertQuery("select count() c, count_distinct(v) d from ((select ts, v from t order by ts) union all (select ts, s from t order by ts) order by ts)")
                    .noLeakCheck().expectSize().noRandomAccess().returns("""
                            c\td
                            2000\t303
                            """);
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
                                "select h.offset o, a.l from t a horizon join t b on (s) list (0s, 1s) as h where cast(a.v as symbol) <> 'k7'",
                                "select json_extract(((a.l % 3)::symbol)::varchar, '$') j, count() c from t a horizon join t b on (s) list (0s, 1s) as h",
                                "select max(length(json_extract(((b.l % 3)::symbol)::varchar, '$'))) m from t a horizon join t b on (s) list (0s, 1s) as h",
                                "select json_extract(((a.l % 3)::symbol)::varchar, '$') j, count() c from t a horizon join t b on (s) horizon join t d on (s) list (0s, 1s) as h",
                                "select max(length(json_extract(((b.l % 3)::symbol)::varchar, '$'))) m from t a horizon join t b on (s) horizon join t d on (s) list (0s, 1s) as h"
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
    public void testPercentileReleasesCastDictionary() throws Exception {
        // the percentile argument sits apart from the aggregated one
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            for (int parallel = 0; parallel < 2; parallel++) {
                sqlExecutionContext.setParallelGroupByEnabled(parallel == 1);
                assertOwnerReleasesCastDictionaries("select s, percentile_disc(l, ($1::symbol)::double) p from t order by s", "percentile_disc");
                assertOwnerReleasesCastDictionaries("select percentile_disc(l::double, ($1::symbol)::double) p from t", "percentile_disc");
                assertOwnerReleasesCastDictionaries(
                        "select percentile_disc(l::double, array[($1::symbol)::double]) p from t",
                        "percentile_disc"
                );
                assertOwnerReleasesCastDictionaries(
                        "select approx_percentile(l, array[($1::symbol)::double]) p from t",
                        "approx_percentile"
                );
                assertOwnerReleasesCastDictionaries(
                        "select approx_percentile(l, array[($1::symbol)::double], 3) p from t",
                        "approx_percentile"
                );
                assertOwnerReleasesCastDictionaries(
                        "select approx_percentile(l::double, array[($1::symbol)::double]) p from t",
                        "approx_percentile"
                );
                assertOwnerReleasesCastDictionaries(
                        "select approx_percentile(l::double, array[($1::symbol)::double], 3) p from t",
                        "approx_percentile"
                );
            }
        });
    }

    @Test
    public void testRegexReleasesCastDictionary() throws Exception {
        // a pattern that is known only at execution time sits apart from the matched argument
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            assertOwnerReleasesCastDictionaries("select * from t where v::string ~ ($1::symbol)::string", "~");
            assertOwnerReleasesCastDictionaries("select * from t where s ~ ($1::symbol)::string", "~");
            assertOwnerReleasesCastDictionaries(
                    "select regexp_replace(v::string, ($1::symbol)::string, ($1::symbol)::string) r from t",
                    "regexp_replace"
            );
        });
    }

    @Test
    public void testRuntimeConstFilterReleasesCastDictionary() throws Exception {
        // a filter that does not read the row is evaluated once per execution
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            assertExecutionReleasesCastDictionaries("select * from t where $1::symbol = '1'", "Filter filter: $0::long::symbol='1'");
        });
    }

    @Test
    public void testSampleByBoundsReleaseCastDictionary() throws Exception {
        // the FROM and TO bounds are read once per execution
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            assertExecutionReleasesCastDictionaries("select ts, count() c from t sample by 1m from ($2::symbol)::timestamp", "Sample By");
            assertExecutionReleasesCastDictionaries("select ts, count() c from t sample by 1m to ($3::symbol)::timestamp", "Interval forward scan");
            assertExecutionReleasesCastDictionaries(
                    "select ts, count() c from t sample by 1m from ($2::symbol)::timestamp fill(null)",
                    "fill: null"
            );
            assertExecutionReleasesCastDictionaries(
                    "select ts, count() c from t sample by 1m from ($2::symbol)::timestamp fill(prev)",
                    "fill: prev"
            );
            assertExecutionReleasesCastDictionaries(
                    "select ts, count() c from t sample by 1m from ($2::symbol)::timestamp to ($3::symbol)::timestamp fill(linear)",
                    "fill: linear"
            );
            assertExecutionReleasesCastDictionaries(
                    "select ts, s, sum(l) m from t sample by 1m from ($2::symbol)::timestamp fill(linear)",
                    "fill: linear"
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
    public void testTimestampSequenceReleasesCastDictionary() throws Exception {
        assertMemoryLeak(() -> {
            bindCastValues();
            assertOwnerReleasesCastDictionaries("select timestamp_sequence(0, ($1::symbol)::long) x from long_sequence(3)", "timestamp_sequence");
            assertOwnerReleasesCastDictionaries(
                    "select timestamp_sequence(($1::symbol)::long, ($1::symbol)::long) x from long_sequence(3)",
                    "timestamp_sequence"
            );
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
    public void testWindowJoinBoundsReleaseCastDictionary() throws Exception {
        // a window bound that is known only at execution time
        assertMemoryLeak(() -> {
            createVarcharTable();
            bindCastValues();
            for (int parallel = 0; parallel < 2; parallel++) {
                sqlExecutionContext.setParallelWindowJoinEnabled(parallel == 1);
                final String plan = parallel == 1 ? "Async Window Join" : "Window Join";
                assertOwnerReleasesCastDictionaries(
                        "select a.l, sum(b.l) m from t a window join t b on (s) range between ($1::symbol)::int seconds preceding and 1 second following",
                        plan
                );
                assertOwnerReleasesCastDictionaries(
                        "select a.l, sum(b.l) m from t a window join t b range between 1 second preceding and ($1::symbol)::int seconds following",
                        plan
                );
            }
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

    // A read-once owner, such as a LIMIT, a FROM-TO bound or a dynamic interval, frees the dictionary
    // as soon as it has the value, so the open cursor may hold none; its close must leave none behind.
    private void assertExecutionReleasesCastDictionaries(String sql, String expectedPlanFragment) throws Exception {
        assertQuery(sql).noLeakCheck().assertsPlanContaining(expectedPlanFragment);
        try (RecordCursorFactory factory = select(sql)) {
            String firstRun = null;
            for (int run = 0; run < 2; run++) {
                final long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_FUNC_RSS);
                final MemoryTracker tracker;
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    tracker = sqlExecutionContext.getMemoryTracker();
                    Assert.assertNotNull(tracker);
                    println(factory, cursor);
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

    // $1 is a number whose cast to symbol builds a dictionary on every read, $2 and $3 are timestamps
    // a minute and ten minutes into the tables' data, and $4 is the text of a symbol value
    private static void bindCastValues() throws Exception {
        bindVariableService.clear();
        bindVariableService.setLong(0, 1);
        bindVariableService.setTimestamp(1, 60_000_000L);
        bindVariableService.setTimestamp(2, 600_000_000L);
        bindVariableService.setStr(3, "s1");
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
