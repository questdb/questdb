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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableReader;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.parquet.PartitionDescriptor;
import io.questdb.griffin.engine.table.parquet.PartitionEncoder;
import io.questdb.jit.JitUtil;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class NoParallelHintTest extends AbstractCairoTest {
    @Override
    @Before
    public void setUp() {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_TOP_K_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HORIZON_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_WINDOW_JOIN_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_READ_PARQUET_ENABLED, "true");
        setProperty(PropertyKey.CAIRO_SQL_PAGE_FRAME_MAX_ROWS, 20);
        setProperty(PropertyKey.QUERY_WITHIN_LATEST_BY_OPTIMISATION_ENABLED, "true");
        super.setUp();
    }

    @Test
    public void testAsOfJoinKeyedFilter() throws Exception {
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_DISABLED, true, false);
    }

    @Test
    public void testAsOfJoinKeyedFilterWithParallelFiltersDisabled() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "false");
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_DISABLED, true, false);
    }

    @Test
    public void testAsOfJoinKeyedProjectedFilter() throws Exception {
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_DISABLED, true, true);
    }

    @Test
    public void testAsOfJoinKeyedProjectedJitFilter() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_ENABLED, true, true);
    }

    @Test
    public void testAsOfJoinNonKeyedFilter() throws Exception {
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_DISABLED, false, false);
    }

    @Test
    public void testAsOfJoinNonKeyedJitFilter() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_ENABLED, false, false);
    }

    @Test
    public void testAsOfJoinNonKeyedProjectedFilter() throws Exception {
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_DISABLED, false, true);
    }

    @Test
    public void testAsOfJoinNonKeyedProjectedFilterWithParallelFiltersDisabled() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_FILTER_ENABLED, "false");
        assertFilteredAsOfJoin(SqlJitMode.JIT_MODE_DISABLED, false, true);
    }

    @Test
    public void testCompilationFailure() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                assertQuery("select /*+ no_parallel */ k, sum(v) from tab group by k limit 'invalid'")
                        .withCompiler(compiler)
                        .withContext(context)
                        .noLeakCheck()
                        .fails(62, "invalid type: STRING");
                assertParallelPlan(compiler, context, "select v from tab where v > 40", "Async");
            }
        });
    }

    @Test
    public void testCte() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery(
                    "with c as (select k, sum(v) s from tab group by k) select * from c order by k",
                    "with c as (select k, sum(v) s from tab group by k) select /*+ no_parallel */ * from c order by k",
                    "vectorized: true"
            );
        });
    }

    @Test
    public void testFilter() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select v from tab where v > 40", "Async");
            assertSerialQuery("select v from tab where v > 40", "select /*+ NO_PARALLEL */ v from tab where v > 40", "Async");
        });
    }

    @Test
    public void testGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select sum(v) from tab", "Async");
            assertSerialQuery("select k, sum(v) from tab group by k order by k", "vectorized: true");
            assertSerialQuery("select k, sum(v) from tab where v > 40 group by k order by k", "Async");
            assertSerialQuery("select sum(v) from tab where v > 40", "Async");
        });
    }

    @Test
    public void testHorizonJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select t.ts, sum(p.v) from tab t horizon join tab p on (t.k = p.k) range from -2s to 2s step 1s as h", "Async");
        });
    }

    @Test
    public void testJitFilter() throws Exception {
        Assume.assumeTrue(JitUtil.isJitSupported());
        assertMemoryLeak(() -> {
            createTable();
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                assertSerialQuery(compiler, context, "select v from tab where v > 40", "select /*+ no_parallel */ v from tab where v > 40", "Async JIT Filter");
            }
        });
    }

    @Test
    public void testLatestBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("alter table tab alter column k add index");
            assertSerialQuery("select * from tab latest on ts partition by k", "Async index backward scan");
            assertSerialQuery("select * from tab where ts >= 0 latest on ts partition by k", "Async index backward scan");
        });
    }

    @Test
    public void testLatestByWithin() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select (x % 4)::symbol k, " +
                    "cast(case when x % 3 = 0 then 'gk1gj8' when x % 3 = 1 then 'mbx5c0' else 'u4pruy' end as geohash(6c)) geo, " +
                    "timestamp_sequence(0, 1000000) ts from long_sequence(100)), index(k) timestamp(ts) partition by day");
            assertSerialQuery("select * from tab where geo within (#gk1gj8, #mbx5c0) latest on ts partition by k", "Async index backward scan");
        });
    }

    @Test
    public void testMultiHorizonJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select t.ts, sum(p.v), sum(p2.v) from tab t " +
                    "horizon join tab p on (t.k = p.k) horizon join tab p2 on (t.k = p2.k) " +
                    "range from -2s to 2s step 1s as h", "Async Multi Horizon Join");
        });
    }

    @Test
    public void testNestedQuery() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select * from (select k, sum(v) s from tab group by k) order by k", "vectorized: true");
        });
    }

    @Test
    public void testNestedSerialFilterGroupBy() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                assertParallelPlan(compiler, context, "SELECT sum(v) FROM (SELECT v FROM tab WHERE v > 40)", "Async");
                assertQuery("SELECT sum(v) FROM (SELECT /*+ no_parallel */ v FROM tab WHERE v > 40)")
                        .withContext(context)
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Filter")
                        .withPlanNotContaining("Async", "GroupBy vectorized: true")
                        .noLeakCheck()
                        .returns("sum\n4230\n");
            }
        });
    }

    @Test
    public void testParquet() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab AS (
                        SELECT (x % 4)::SYMBOL k, x v, timestamp_sequence(0, 1_000_000_000) ts
                        FROM long_sequence(100)
                    ) TIMESTAMP(ts) PARTITION BY DAY
                    """);
            execute("alter table tab convert partition to parquet list '1970-01-01'");
            assertQuery("SELECT name, isParquet FROM table_partitions('tab') WHERE name = '1970-01-01'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("name\tisParquet\n1970-01-01\ttrue\n");
            assertSerialQuery("select k, sum(v) from tab where v > 40 group by k order by k", "Async");
        });
    }

    @Test
    public void testReadParquet() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            inputRoot = root;
            try (
                    TableReader reader = engine.getReader("tab");
                    PartitionDescriptor descriptor = new PartitionDescriptor();
                    Path path = new Path()
            ) {
                PartitionEncoder.populateFromTableReader(reader, descriptor, 0);
                PartitionEncoder.encode(descriptor, path.of(root).concat("tab.parquet"));
            }
            assertSerialQuery("select sum(v) from read_parquet('tab.parquet') where v > 40", "Async");
        });
    }

    @Test
    public void testScalarSubquery() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select v from tab where v > (select avg(v) from tab)", "Async");
        });
    }

    @Test
    public void testSharedCte() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery(
                    "with c as (select k, sum(v) s from tab group by k) select * from (select * from c union all select * from c) order by k",
                    "with c as (select k, sum(v) s from tab group by k) select /*+ no_parallel */ * from (select * from c union all select * from c) order by k",
                    "vectorized: true"
            );
        });
    }

    @Test
    public void testSharedCteWithDifferentHintScopes() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                assertQuery("with c as (select k, sum(v) s from tab group by k) select * from c union all select /*+ no_parallel */ * from c")
                        .withCompiler(compiler)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlanContaining("vectorized: true", "vectorized: false");
            }
        });
    }

    @Test
    public void testTopK() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select v from tab order by v desc limit 5", "Async");
        });
    }

    @Test
    public void testUnion() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select sum(v) from tab union all select sum(v) from tab where v > 40", "Async");
        });
    }

    @Test
    public void testWindowJoin() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertSerialQuery("select t.ts, sum(p.v) from tab t window join tab p on t.k = p.k range between 2 seconds preceding and 2 seconds following", "Async");
        });
    }

    private void assertFilteredAsOfJoin(int jitMode, boolean isKeyed, boolean isProjected) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE master (ts TIMESTAMP, k SYMBOL) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE slave (ts TIMESTAMP, k SYMBOL, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO master VALUES
                        (0, 'x'), (1_000_000, 'y'), (2_000_000, 'x'), (3_000_000, null), (4_000_000, 'z')
                    """);
            execute("""
                    INSERT INTO slave VALUES
                        (0, 'y', 10), (1_000_000, 'x', 20), (2_000_000, 'x', 30), (3_000_000, null, 40)
                    """);
            String queryBody = (isProjected ? "m.ts, m.k, s.k slave_k, s.v" : "*")
                    + " FROM master m ASOF JOIN (SELECT "
                    + (isProjected ? "v, ts, k" : "*")
                    + " FROM slave WHERE v != 20) s" + (isKeyed ? " ON (k)" : "");
            String expected;
            if (isProjected) {
                expected = isKeyed ? """
                        ts\tk\tslave_k\tv
                        1970-01-01T00:00:00.000000Z\tx\t\tnull
                        1970-01-01T00:00:01.000000Z\ty\ty\t10
                        1970-01-01T00:00:02.000000Z\tx\tx\t30
                        1970-01-01T00:00:03.000000Z\t\t\t40
                        1970-01-01T00:00:04.000000Z\tz\t\tnull
                        """ : """
                        ts\tk\tslave_k\tv
                        1970-01-01T00:00:00.000000Z\tx\ty\t10
                        1970-01-01T00:00:01.000000Z\ty\ty\t10
                        1970-01-01T00:00:02.000000Z\tx\tx\t30
                        1970-01-01T00:00:03.000000Z\t\t\t40
                        1970-01-01T00:00:04.000000Z\tz\t\t40
                        """;
            } else {
                expected = isKeyed ? """
                        ts\tk\tts1\tk1\tv
                        1970-01-01T00:00:00.000000Z\tx\t\t\tnull
                        1970-01-01T00:00:01.000000Z\ty\t1970-01-01T00:00:00.000000Z\ty\t10
                        1970-01-01T00:00:02.000000Z\tx\t1970-01-01T00:00:02.000000Z\tx\t30
                        1970-01-01T00:00:03.000000Z\t\t1970-01-01T00:00:03.000000Z\t\t40
                        1970-01-01T00:00:04.000000Z\tz\t\t\tnull
                        """ : """
                        ts\tk\tts1\tk1\tv
                        1970-01-01T00:00:00.000000Z\tx\t1970-01-01T00:00:00.000000Z\ty\t10
                        1970-01-01T00:00:01.000000Z\ty\t1970-01-01T00:00:00.000000Z\ty\t10
                        1970-01-01T00:00:02.000000Z\tx\t1970-01-01T00:00:02.000000Z\tx\t30
                        1970-01-01T00:00:03.000000Z\t\t1970-01-01T00:00:03.000000Z\t\t40
                        1970-01-01T00:00:04.000000Z\tz\t1970-01-01T00:00:03.000000Z\t\t40
                        """;
            }
            try (
                    SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                context.setJitMode(jitMode);
                if (jitMode == SqlJitMode.JIT_MODE_ENABLED) {
                    assertParallelPlan(compiler, context, "SELECT * FROM slave WHERE v != 20", "Async JIT Filter");
                }
                for (int i = 0; i < 2; i++) {
                    assertQuery("SELECT " + (i == 0 ? "" : "/*+ no_parallel */ ") + queryBody)
                            .withContext(context)
                            .timestamp("ts")
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("Filtered AsOf Join Fast")
                            .withPlanNotContaining("Async")
                            .noLeakCheck()
                            .returns(expected);
                }
                assertQuery("SELECT /*+ no_parallel asof_linear(m s) */ " + queryBody)
                        .withContext(context)
                        .timestamp("ts")
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining(isKeyed ? "AsOf Join Light" : "AsOf Join\n")
                        .withPlanNotContaining("Filtered AsOf Join Fast", "Async")
                        .noLeakCheck()
                        .returns(expected);
            }
        });
    }

    private void assertParallelPlan(SqlCompiler compiler, SqlExecutionContextImpl context, String query, String parallelOperator) throws Exception {
        assertQuery(query)
                .withCompiler(compiler)
                .withContext(context)
                .noLeakCheck()
                .assertsPlanContaining(parallelOperator);
    }

    private void assertSerialQuery(SqlCompiler compiler, SqlExecutionContextImpl context, String query, String hintedQuery, String parallelOperator) throws Exception {
        assertParallelPlan(compiler, context, query, parallelOperator);
        StringSink result = new StringSink();
        TestUtils.printSql(compiler, context, query, result);
        String expected = result.toString();
        var bus = engine.getMessageBus();
        long latestBySequence = bus.getLatestByPubSeq().current();
        assertQuery(hintedQuery)
                .withContext(context)
                .inferTimestamp()
                .inferRandomAccess()
                .sizeMayVary()
                .withPlanNotContaining("Async", "GroupBy vectorized: true")
                .noLeakCheck()
                .returns(expected);
        Assert.assertEquals(latestBySequence, bus.getLatestByPubSeq().current());
        assertParallelPlan(compiler, context, query, parallelOperator);
    }

    private void assertSerialQuery(String query, String parallelOperator) throws Exception {
        assertSerialQuery(query, query.replaceFirst("select ", "select /*+ no_parallel */ "), parallelOperator);
    }

    private void assertSerialQuery(String query, String hintedQuery, String parallelOperator) throws Exception {
        try (
                SqlExecutionContextImpl context = TestUtils.createSqlExecutionCtx(engine, 4);
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            assertSerialQuery(compiler, context, query, hintedQuery, parallelOperator);
        }
    }

    private void createTable() throws Exception {
        execute("create table tab as (select (x % 4)::symbol k, x v, timestamp_sequence(0, 1000000) ts from long_sequence(100)) timestamp(ts) partition by day");
    }
}
