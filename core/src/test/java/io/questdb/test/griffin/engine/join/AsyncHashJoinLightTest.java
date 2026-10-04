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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.join.AsyncHashJoinLightRecordCursorFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * An inner hash join whose build side is unique on the join key probes its master on the shared
 * workers ({@code Async Hash Join Light}). Its output must be the light hash join's, row for row:
 * every test runs each query with the parallel probe switched off and on and compares the two,
 * through a fresh compile, a rewind and a second execution of the same factory.
 */
public class AsyncHashJoinLightTest extends AbstractCairoTest {
    private static final String C = "ts, ex, sym, v, size, price, x";

    @Override
    public void setUp() {
        super.setUp();
        sqlExecutionContext.changePageFrameSizes(1, 64);
    }

    @Override
    public void tearDown() throws Exception {
        AsyncHashJoinLightRecordCursorFactory.DEBUG_ASSUME_UNIQUE_BUILD = false;
        super.tearDown();
    }

    @Test
    public void testCancelBeforeTheProbe() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createT(engine, ctx, 20_000);
            final String query = mo59();
            final String expected = serial(engine, ctx, query);
            final NetworkSqlExecutionCircuitBreaker circuitBreaker = new NetworkSqlExecutionCircuitBreaker(
                    engine,
                    new DefaultSqlExecutionCircuitBreakerConfiguration()
            );
            try {
                ctx.with(ctx.getSecurityContext(), ctx.getBindVariableService(), ctx.getRandom(), ctx.getRequestFd(), circuitBreaker);
                ctx.setParallelHashJoinProbeEnabled(true);
                try (RecordCursorFactory factory = engine.select(query, ctx)) {
                    final AsyncHashJoinLightRecordCursorFactory join = find(factory);
                    Assert.assertNotNull(join);
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        // the build has run; no frame of the probe has been dispatched
                        circuitBreaker.cancel();
                        //noinspection StatementWithEmptyBody
                        while (cursor.hasNext()) {
                        }
                        Assert.fail("cancelled query ran to completion");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isCancellation());
                    }
                    circuitBreaker.clearCancelSentinel();
                    circuitBreaker.resetTimer();
                    try (RecordCursor cursor = factory.getCursor(ctx)) {
                        TestUtils.assertEquals(expected, print(cursor, factory));
                    }
                    Assert.assertEquals(0, join.getAcquiredSlotCount());
                }
            } finally {
                Misc.free(circuitBreaker);
            }
        }));
    }

    @Test
    public void testDuplicateBuildKeysWalkTheChain() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 2_000);
            execute("create table d as (select rnd_symbol('A', 'B', 'C', null) dex, (x % 9)::double dsize, x dx from long_sequence(50))");
            // the planner cannot prove this build unique; pretend it did, so that the cursor meets
            // repeated keys at run time
            AsyncHashJoinLightRecordCursorFactory.DEBUG_ASSUME_UNIQUE_BUILD = true;
            final String query = "select t.ts, t.x, t.ex, t.size, d.dx, d.dsize from t join d on t.ex = d.dex and t.size = d.dsize";
            assertSameAsSerial(query, true);
            assertSameAsSerial("select count(*) from (" + query + ")", false);
            assertSameAsSerial(query + " limit 7", true);
            try (RecordCursorFactory factory = select(query)) {
                final AsyncHashJoinLightRecordCursorFactory join = find(factory);
                Assert.assertNotNull(join);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    print(cursor, factory);
                }
                Assert.assertFalse(join.isLastBuildUnique());
            }
        });
    }

    @Test
    public void testEmptySides() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 0);
            assertSameAsSerial(mo59(), true, true);
            assertSameAsSerial(mo70() + " limit -3", true, true);
            execute("create table t2 as (select * from t)");
            execute("drop table t");
            createT(engine, sqlExecutionContext, 500);
            // master full, build empty
            assertSameAsSerial("select " + q("t", C) + " from t join (select ex mex, min(size) mn from t2) m on t.ex = m.mex", true, true);
        });
    }

    @Test
    public void testExplain() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 10);
            final StringSink plan = new StringSink();
            printSql("explain " + mo59(), plan);
            TestUtils.assertEquals(
                    """
                            QUERY PLAN
                            SelectedRecord
                                Async Hash Join Light workers: 1
                                  condition: m.min_size=t.size and m.msym=t.sym and m.mex=t.ex
                                  symbolKeyJoin: true
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: t
                                    Hash
                                        Async Group By workers: 1
                                          keys: [msym,mex]
                                          values: [min(size)]
                                          filter: null
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t
                            """,
                    plan
            );
            sqlExecutionContext.setParallelHashJoinProbeEnabled(false);
            printSql("explain " + mo59(), plan);
            TestUtils.assertContains(plan, "Hash Join Light");
            TestUtils.assertNotContains(plan, "Async Hash Join Light");
        });
    }

    @Test
    public void testIneligibleJoinsStaySerial() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 2_000);
            // the build is a table: keys repeat
            assertSameAsSerial("select " + q("t", C) + " from t join (select ex mex, size msize from t where x < 50) m on t.ex = m.mex and t.size = m.msize", false);
            // a GROUP BY key that is not a join key
            assertSameAsSerial("select " + q("t", C) + " from t join (select ex mex, sym msym, min(size) mn from t) m on t.ex = m.mex and t.size = m.mn", false);
            // a join key computed from the GROUP BY key
            assertSameAsSerial("select " + q("t", C) + " from t join (select ex || 'x' mex, min(size) mn from t) m on t.ex || 'x' = m.mex", false, true, true);
            // no designated timestamp in the output: the light hash join may swap its sides
            assertSameAsSerial("select t.x, t.ex from t join (select ex mex, min(size) mn from t) m on t.ex = m.mex and t.size = m.mn", false);
            // an explicit GROUP BY of a column it does not select
            assertSameAsSerial("select " + q("t", C) + " from t join (select ex mex, min(size) mn from t group by ex, sym) m on t.ex = m.mex and t.size = m.mn", false);
            // a master without page frames
            assertSameAsSerial("select " + q("t", C) + " from (select * from t where price > 1) t join (select ex mex, min(size) mn from t) m on t.ex = m.mex and t.size = m.mn", false);
            // switched off
            sqlExecutionContext.setParallelHashJoinProbeEnabled(false);
            assertSameAsSerial(mo59(), false, false, false);
        });
    }

    @Test
    public void testKeysAndProjections() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            // slave columns projected: the slave record is positioned on demand
            assertSameAsSerial("select t.ts, t.x, t.sym, m.msym, m.mn, m.mx, m.c from t join (select sym msym, min(price) mn, max(price) mx, count() c from t) m on t.sym = m.msym where t.price = m.mx", true);
            assertSameAsSerial("select * from t join (select sym msym, v mv, max(x) mx from t) m on t.sym = m.msym and t.v = m.mv and t.x = m.mx", true);
            // VARCHAR keys with NULL; NULL matches NULL as in the light hash join
            assertSameAsSerial("select " + q("t", C) + ", m.mv from t join (select v mv, max(size) mx from t) m on t.v = m.mv and t.size = m.mx", true);
            // a symbol the master does not have
            execute("create table s as (select rnd_symbol('A', 'B', 'ZZ', null) sk, x sx from long_sequence(40))");
            assertSameAsSerial("select t.ts, t.x, t.ex, m.sk, m.c from t join (select sk, count() c from s) m on t.ex = m.sk", true);
            // SELECT DISTINCT
            assertSameAsSerial("select t.ts, t.x, t.ex, t.v from t join (select distinct ex dex, v dv from t where x % 3 = 0) d on t.ex = d.dex and t.v = d.dv", true);
            // a renaming sub-query over the GROUP BY
            assertSameAsSerial("select t.ts, t.x, t.ex, m.k from t join (select mex k, mn from (select ex mex, min(size) mn from t)) m on t.ex = m.k and t.size = m.mn", true);
        });
    }

    @Test
    public void testLimitAndAggregates() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            assertSameAsSerial(mo70() + " limit 5", true);
            assertSameAsSerial(mo70() + " limit -3", true);
            assertSameAsSerial(mo70() + " limit 3, 7", true);
            // the GROUP BY fuses with the join instead, see AsyncHashJoinGroupByRecordCursorFactory
            assertSameAsSerial("select count(*), sum(price) from (" + mo59() + ")", false);
            assertSameAsSerial(mo60(), true);
        });
    }

    @Test
    public void testOnWorkerPool() throws Exception {
        assertMemoryLeak(() -> inPool((engine, ctx) -> {
            createT(engine, ctx, 30_000);
            for (String query : new String[]{mo59(), mo60(), mo70(), mo70() + " limit -7",
                    "select t.ts, t.x, t.sym, m.mx from t join (select sym msym, max(price) mx from t) m on t.sym = m.msym"}) {
                final String expected = serial(engine, ctx, query);
                ctx.setParallelHashJoinProbeEnabled(true);
                for (int run = 0; run < 3; run++) {
                    try (RecordCursorFactory factory = engine.select(query, ctx)) {
                        Assert.assertNotNull(query, find(factory));
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            TestUtils.assertEquals(query, expected, print(cursor, factory));
                        }
                    }
                }
            }
        }));
    }

    @Test
    public void testParquetMaster() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 3_000);
            execute("alter table t convert partition to parquet list '1970-01-01', '1970-01-03'");
            assertSameAsSerial(mo59(), true);
            assertSameAsSerial(mo60(), true);
            assertSameAsSerial(mo70(), true);
        });
    }

    @Test
    public void testTaqShapes() throws Exception {
        assertMemoryLeak(() -> {
            createT(engine, sqlExecutionContext, 5_000);
            assertSameAsSerial(mo59(), true);
            assertSameAsSerial(mo60(), true);
            assertSameAsSerial(mo70(), true);
            // 56's master is a filtered sub-query, without page frames
            assertSameAsSerial("select " + C + " from (select " + C + " from t where size >= (select min(max_size) from (select ex, max(size) max_size from t))) t " +
                    "join (select ex mex, max(size) max_size from t) m on m.mex = t.ex where t.size = m.max_size", false);
        });
    }

    private static void createT(CairoEngine engine, SqlExecutionContext ctx, int rows) throws Exception {
        engine.execute(
                "create table t as (select" +
                        " timestamp_sequence(0, 100000000) ts," +
                        " rnd_symbol('A', 'B', 'C', null) ex," +
                        " rnd_symbol(40, 1, 3, 5) sym," +
                        " rnd_varchar('p', 'q', 'r', null) v," +
                        " case when x % 37 = 0 then null else ((x * 7919) % 13)::float / 4 end size," +
                        " case when x % 41 = 0 then null else ((x * 31) % 17) / 3.0 end price," +
                        " x" +
                        " from long_sequence(" + rows + ")) timestamp(ts) partition by day",
                ctx
        );
    }

    private static AsyncHashJoinLightRecordCursorFactory find(RecordCursorFactory factory) {
        while (factory != null) {
            if (factory instanceof AsyncHashJoinLightRecordCursorFactory join) {
                return join;
            }
            factory = factory.getBaseFactory();
        }
        return null;
    }

    private static String mo59() {
        return "select " + q("t", C) + " from t t join (select ex mex, sym msym, min(size) min_size from t) m on t.ex = m.mex and t.sym = m.msym and t.size = m.min_size";
    }

    private static String mo60() {
        return "select " + C + " from (select " + C + " from t join (select ex mex, sym msym, min(size) mn from t) m on ex = m.mex and sym = m.msym where size = m.mn) order by sym";
    }

    private static String mo70() {
        return "select " + C + " from t join (select sym msym, min(price) min_price from t) m on t.sym = m.msym where price = min_price";
    }

    private static String print(RecordCursor cursor, RecordCursorFactory factory) {
        final StringSink sink = new StringSink();
        CursorPrinter.println(cursor, factory.getMetadata(), sink, true, false);
        return sink.toString();
    }

    private static String q(String alias, String columns) {
        final StringSink sink = new StringSink();
        final String[] names = columns.split(", ");
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                sink.put(", ");
            }
            sink.put(alias).put('.').put(names[i]);
        }
        return sink.toString();
    }

    private static String run(CairoEngine engine, SqlExecutionContext ctx, String query, boolean expectAsync) throws Exception {
        try (RecordCursorFactory factory = engine.select(query, ctx)) {
            Assert.assertEquals(query, expectAsync, find(factory) != null);
            final String first;
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                first = print(cursor, factory);
                cursor.toTop();
                TestUtils.assertEquals(query, first, print(cursor, factory));
            }
            try (RecordCursor cursor = factory.getCursor(ctx)) {
                TestUtils.assertEquals(query, first, print(cursor, factory));
            }
            return first;
        }
    }

    private static String serial(CairoEngine engine, SqlExecutionContext ctx, String query) throws Exception {
        ctx.setParallelHashJoinProbeEnabled(false);
        return run(engine, ctx, query, false);
    }

    private void assertSameAsSerial(String query, boolean expectAsync) throws Exception {
        assertSameAsSerial(query, expectAsync, true, false);
    }

    private void assertSameAsSerial(String query, boolean expectAsync, boolean allowEmpty) throws Exception {
        assertSameAsSerial(query, expectAsync, true, allowEmpty);
    }

    private void assertSameAsSerial(String query, boolean expectAsync, boolean switchOn, boolean allowEmpty) throws Exception {
        final String expected = serial(engine, sqlExecutionContext, query);
        sqlExecutionContext.setParallelHashJoinProbeEnabled(switchOn);
        final String actual = run(engine, sqlExecutionContext, query, expectAsync);
        TestUtils.assertEquals(query, expected, actual);
        if (!allowEmpty) {
            Assert.assertTrue(query + " returned no rows", expected.indexOf('\n') < expected.length() - 1);
        }
    }

    private void inPool(PoolTest test) throws Exception {
        final WorkerPool pool = new TestWorkerPool(4, TestUtils.getWorkerPoolMode(TestUtils.generateRandom(LOG)));
        TestUtils.execute(pool, (engine, compiler, context) -> {
            final SqlExecutionContextImpl ctx = (SqlExecutionContextImpl) context;
            ctx.changePageFrameSizes(1, 64);
            test.run(engine, ctx);
        }, configuration, LOG);
    }

    @FunctionalInterface
    private interface PoolTest {
        void run(CairoEngine engine, SqlExecutionContextImpl ctx) throws Exception;
    }
}
