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
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByMetrics;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class HashJoinGroupByPlannerTest extends AbstractCairoTest {
    private static final String SELECT = "select p.country, year(r.reading_ts) yr, month(r.reading_ts) mo, "
            + "sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, "
            + "sum(r.energy_kwh)/nullif(sum(p.installed_kwp),0) yield";
    private static final String SCALAR_SELECT = "select count(*) pairs, count(p.plant_id) plants, "
            + "sum(r.energy_kwh) energy, avg(r.irradiance_wm2) irradiance, "
            + "sum(r.energy_kwh)/nullif(sum(p.installed_kwp),0) yield";
    private static final String[] JOINS = {
            " from r join p on r.plant_id=p.plant_id",
            " from r left join p on r.plant_id=p.plant_id",
            " from p right join r on r.plant_id=p.plant_id"
    };

    @Test
    public void testConfigurationMatrix() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            Assert.assertFalse(new DefaultCairoConfiguration(root).isSqlParallelHashJoinGroupByEnabled());
            Assert.assertFalse(configuration.isSqlParallelHashJoinGroupByEnabled());
            for (boolean global : new boolean[]{false, true}) {
                for (boolean experimental : new boolean[]{false, true}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(global));
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_ENABLED, Boolean.toString(experimental));
                    Assert.assertEquals(experimental, configuration.isSqlParallelHashJoinGroupByEnabled());
                    for (int workers : new int[]{0, 1, 4}) {
                        try (SqlExecutionContextImpl context = context(workers)) {
                            boolean enabled = global && experimental && workers > 0;
                            Assert.assertEquals(enabled, context.isParallelHashJoinGroupByEnabled());
                            for (String join : JOINS) {
                                assertDifferential(SELECT + join + " order by country, yr, mo", context, enabled);
                                assertDifferential(SCALAR_SELECT + join, context, enabled);
                                assertDifferential("select count(*)" + join, context, enabled);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testFiltersAndOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int jit : new int[]{0, 2}) {
                    context.setJitMode(jit);
                    for (int j = 0; j < JOINS.length; j++) {
                        String sql = SELECT + JOINS[j] + " where r.reading_ts >= '2020-01-01'"
                                + " and r.reading_ts < '2021-01-01' and r.energy_kwh > 10"
                                + " and p.country in ('ES','IT') order by country, yr, mo limit 3";
                        assertDifferential(sql, context, true);
                        assertDifferential(sql.replace(SELECT, SCALAR_SELECT).replace(" order by country, yr, mo", " order by energy"), context, true);
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            String plan = plan(factory, context);
                            Assert.assertTrue(plan, plan.contains("Interval forward scan on: r"));
                            Assert.assertTrue(plan, plan.contains("buildStrategy: shared"));
                            Assert.assertTrue(plan, plan.contains("physicalJoinType: " + (j == 0 ? "inner" : "left outer")));
                            Assert.assertTrue(plan, plan.contains("inputSwapped: " + (j == 2)));
                            Assert.assertTrue(plan, plan.contains("probeFilter:"));
                            if (j > 0) {
                                Assert.assertTrue(plan, plan.contains("postJoinFilter:"));
                            }
                        }
                    }
                }
                for (int j = 1; j < JOINS.length; j++) {
                    for (String on : new String[]{"", " and p.installed_kwp > 5"}) {
                        for (String where : new String[]{"", " where p.installed_kwp is null", " where p.installed_kwp > 5"}) {
                            assertDifferential(SELECT + JOINS[j] + on + where + " order by country, yr, mo", context, true);
                            assertDifferential(SCALAR_SELECT + JOINS[j] + on + where, context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testProjectionsAndAliases() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String where : new String[]{"", " where energy_kwh > 10"}) {
                    String probe = "(select energy_kwh e, reading_ts ts, plant_id id, irradiance_wm2 irr from r" + where + ") a";
                    String build = "(select installed_kwp cap, plant_id id, country c from p where installed_kwp > 0) b";
                    for (String join : new String[]{probe + " join " + build, probe + " left join " + build, build + " right join " + probe}) {
                        assertDifferential("select b.c, year(a.ts) yr, sum(a.e) energy, avg(a.irr) irradiance, sum(b.cap) capacity from "
                                + join + " on a.id=b.id group by b.c, year(a.ts) order by b.c, yr", context, true);
                        assertDifferential("select sum(a.e) energy, avg(a.irr) irradiance, count(b.c) countries, sum(b.cap) capacity from "
                                + join + " on a.id=b.id", context, true);
                    }
                }
            }
        });
    }

    @Test
    public void testUnsupportedShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String sql : new String[]{
                        "select first(r.energy_kwh), last(r.energy_kwh)" + JOINS[0],
                        "select sum(r.plant_id), avg(r.plant_id)" + JOINS[0],
                        "select count(distinct p.country)" + JOINS[1],
                        "select min(r.energy_kwh), max(r.energy_kwh)" + JOINS[2],
                        "select p.country, first(r.energy_kwh), last(r.energy_kwh)" + JOINS[0] + " order by country",
                        "select p.country, sum(r.plant_id)" + JOINS[0] + " order by country",
                        SELECT + " from r join p on r.plant_id::long=p.plant_id::long order by country,yr,mo",
                        SELECT + JOINS[0] + " and r.energy_kwh=p.installed_kwp order by country,yr,mo",
                        SELECT + JOINS[1] + " and r.energy_kwh > 0 order by country,yr,mo",
                        SELECT + JOINS[1] + " where r.energy_kwh > 0 or p.installed_kwp > 0 order by country,yr,mo",
                        SELECT + " from (r limit 2) r join p on r.plant_id=p.plant_id order by country,yr,mo",
                        SELECT + JOINS[0] + " join p p2 on r.plant_id=p2.plant_id order by country,yr,mo"
                }) {
                    assertDifferential(sql, context, false);
                }
            }
        });
    }

    @Test
    public void testMetricsExplainAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String sql = "select r.plant_id, count(*) pairs, sum(r.energy_kwh) energy" + JOINS[1]
                        + " where p.installed_kwp is null order by r.plant_id";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                    HashJoinGroupByMetrics metrics = fused.getMetrics();
                    plan(factory, context);
                    Assert.assertEquals(0, metrics.getBuildRows());
                    Assert.assertEquals(0, metrics.getBuildNanos());
                    String expected = result(factory, context);
                    Assert.assertEquals(5, metrics.getBuildRows());
                    Assert.assertEquals(3, metrics.getBuildKeys());
                    Assert.assertTrue(metrics.getBuildBytes() > 0);
                    Assert.assertEquals(5, metrics.getScannedRows());
                    Assert.assertEquals(8, metrics.getMatchedPairs());
                    Assert.assertEquals(1, metrics.getNullExtendedRows());
                    Assert.assertEquals(4, metrics.getSurvivingRows());
                    Assert.assertEquals(3, metrics.getMergeCardinality());
                    Assert.assertTrue(metrics.getBuildNanos() > 0);
                    Assert.assertTrue(metrics.getProbeNanos() > 0);
                    Assert.assertTrue(metrics.getMergeNanos() > 0);
                    Assert.assertEquals(expected, result(factory, context));
                    Assert.assertEquals(5, metrics.getScannedRows());
                    execute("truncate table p");
                    result(factory, context);
                    Assert.assertEquals(0, metrics.getBuildRows());
                    Assert.assertEquals(0, metrics.getMatchedPairs());
                    Assert.assertEquals(5, metrics.getNullExtendedRows());
                    Assert.assertEquals(5, metrics.getSurvivingRows());
                }
            }
        });
    }

    @Test
    public void testSerialProbeFilterKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.setParallelFilterEnabled(false);
                for (String join : JOINS) {
                    assertDifferential(SELECT + join + " where r.reading_ts >= '2020-02-01' and r.energy_kwh > 10"
                            + " order by country,yr,mo", context, false);
                }
                assertDifferential(SELECT + JOINS[0] + " where p.installed_kwp > 5 order by country,yr,mo", context, true);
            }
        });
    }

    @Test
    public void testStorageAndBindRebinding() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("alter table r add column tag symbol");
            execute("insert into r values (1, '2022-01-01', 15, 150, 'left')");
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                bindVariableService.setDouble(0, 2);
                bindVariableService.setStr(1, "ES");
                bindVariableService.setDouble(2, 5);
                String sql = "select r.tag, p.country, sum(r.energy_kwh * $1) energy" + JOINS[1]
                        + " where p.country = $2 and r.energy_kwh > $3 order by r.tag, p.country";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    Assert.assertNotNull(fused(factory));
                    String expected = result(factory, context);
                    execute("alter table r convert partition to parquet where reading_ts < '2020-02-01'");
                    try {
                        result(factory, context);
                        Assert.fail("the child filter's native-format guard must request recompilation");
                    } catch (TableReferenceOutOfDateException expectedStalePlan) {
                        Assert.assertNull(context.getMemoryTracker());
                        Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    }
                    assertDifferential(sql, context, true);
                    execute("alter table r convert partition to parquet where reading_ts >= '2020-02-01'");
                    assertDifferential(sql, context, true);
                    execute("alter table r convert partition to native where reading_ts >= '2020-01-01'");
                    Assert.assertEquals(expected, result(factory, context));
                    execute("truncate table p");
                    execute("insert into p values (1, 'IT', 17), (1, 'IT', null)");
                    bindVariableService.setDouble(0, 7);
                    bindVariableService.setStr(1, "IT");
                    bindVariableService.setDouble(2, 12);
                    assertDifferential(sql, context, true);
                    context.setParallelHashJoinGroupByEnabled(false);
                    try (RecordCursorFactory baseline = engine.select(sql, context)) {
                        Assert.assertEquals(result(baseline, context), result(factory, context));
                    }
                }
                context.setParallelHashJoinGroupByEnabled(true);
                execute("alter table r convert partition to parquet where reading_ts < '2020-02-01'");
                execute("alter table r alter column energy_kwh type float");
                assertDifferential(SELECT.replace("sum(r.energy_kwh)", "sum(r.energy_kwh::double)")
                        + JOINS[1] + " where r.energy_kwh > 10 order by country,yr,mo", context, true);
            }
        });
    }

    @Test
    public void testSharedLateralInputKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertDifferential("select g.country, g.energy, x.n from (select p.country, sum(r.energy_kwh) energy"
                        + JOINS[0] + ") g join lateral (select count(*) n from r where energy_kwh > g.energy) x"
                        + " order by g.country", context, false);
            }
        });
    }

    @Test
    public void testQueryMemoryFailureAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String sql = SELECT + JOINS[0] + " where r.energy_kwh > 10 order by country,yr,mo";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                    String expected = result(factory, context);
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64);
                    try {
                        result(factory, context);
                        Assert.fail("expected the query memory limit error");
                    } catch (CairoException ex) {
                        Assert.assertTrue(ex.getMessage(), ex.isOutOfMemory());
                    }
                    Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertNull(context.getMemoryTracker());
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    Assert.assertEquals(expected, result(factory, context));
                }
            }
        });
    }

    @Test
    public void testLargerBuildStillSelectsShared() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into p select x::int, 'ES', 2.0 from long_sequence(100003)");
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String join : JOINS) {
                    assertDifferential(SELECT + join + " order by country, yr, mo", context, true);
                }
            }
        });
    }

    @Test
    public void testAllAggregateTypesKeyedAndUnkeyed() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table a (id int, i int, l long, d double, s symbol)");
            execute("create table b (id int, i int, l long, d double, s symbol)");
            String aggregates = "count(*) n, count() n2, count(a.i) ai, count(a.l) al, count(a.d) ad, count(a.s) asym, "
                    + "count(b.i) bi, count(b.l) bl, count(b.d) bd, count(b.s) bsym, "
                    + "sum(a.d) asum, avg(a.d) aavg, sum(b.d) bsum, avg(b.d) bavg";
            String[] joins = {" from a join b on a.id=b.id", " from a left join b on a.id=b.id", " from b right join a on a.id=b.id"};
            for (int scenario = 0; scenario < 8; scenario++) {
                execute("truncate table a");
                execute("truncate table b");
                if (scenario != 4 && scenario != 7) {
                    execute("insert into a values (1,1,100,0.5,'a'), (1,null,null,null,null), (2,2,200,2.0,'b'), "
                            + "(3,3,300,8.0,'c'), (null,null,null,null,null)");
                }
                if (scenario != 2 && scenario != 7) {
                    execute("insert into b values (1,1,10,1.0,'x'), (1,null,null,null,null), (1,2,20,4.0,'y'), "
                            + "(2,3,30,8.0,'z'), (null,null,null,null,null)");
                }
                if (scenario == 1) {
                    execute("update a set i=null, l=null, d=null, s=null");
                    execute("update b set i=null, l=null, d=null, s=null");
                } else if (scenario == 3) {
                    execute("update b set id=999");
                }
                String where = scenario == 5 ? " where b.i=999" : scenario == 6 ? " where a.i=999" : "";
                for (int workers : new int[]{1, 4}) {
                    try (SqlExecutionContextImpl context = context(workers)) {
                        context.setParallelGroupByEnabled(true);
                        context.setParallelHashJoinGroupByEnabled(true);
                        context.changePageFrameSizes(1, 1);
                        for (String join : joins) {
                            assertDifferential("select " + aggregates + join + where, context, true);
                            assertDifferential("select a.id, " + aggregates + join + where + " order by a.id", context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testScalarEmptyInputMetricsAndCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int j = 0; j < JOINS.length; j++) {
                    String sql = "select count(*) n, count(p.plant_id) c, sum(p.installed_kwp) s, avg(p.installed_kwp) a" + JOINS[j];
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                        Assert.assertFalse(fused.recordCursorSupportsRandomAccess());
                        Assert.assertTrue(plan(factory, context).contains("aggregation: scalar"));
                        Assert.assertEquals(0, fused.getMetrics().getBuildRows());
                        // Close before dispatch, and then consume and reread the same factory.
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertEquals(1, cursor.size());
                            Assert.assertEquals(0, cursor.preComputedStateSize());
                        }
                        String expected = result(factory, context);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            RecordCursor.Counter remaining = new RecordCursor.Counter();
                            cursor.calculateSize(context.getCircuitBreaker(), remaining);
                            Assert.assertEquals(0, remaining.get());
                            Assert.assertFalse(cursor.hasNext());
                            cursor.toTop();
                            StringSink sink = new StringSink();
                            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
                            Assert.assertEquals(expected, sink.toString());
                            cursor.toTop();
                            cursor.calculateSize(context.getCircuitBreaker(), remaining);
                            Assert.assertEquals(1, remaining.get());
                            Assert.assertFalse(cursor.hasNext());
                            Assert.assertEquals(1, cursor.preComputedStateSize());
                        }
                        execute("truncate table p");
                        Assert.assertEquals("n\tc\ts\ta\n" + (j == 0 ? "0" : "5") + ":LONG\t0:LONG\tnull:DOUBLE\tnull:DOUBLE\n", result(factory, context));
                        HashJoinGroupByMetrics metrics = fused.getMetrics();
                        Assert.assertEquals(0, metrics.getBuildRows());
                        Assert.assertEquals(0, metrics.getMatchedPairs());
                        Assert.assertEquals(j == 0 ? 0 : 5, metrics.getScannedRows());
                        Assert.assertEquals(j == 0 ? 0 : 5, metrics.getNullExtendedRows());
                        Assert.assertEquals(1, metrics.getMergeCardinality());
                        Assert.assertFalse(fused.getAtom().isSharded());
                        execute("insert into p values (1,'ES',5), (1,'ES',7), (1,'IT',null), (2,null,null), (null,'ES',11)");
                        Assert.assertEquals(expected, result(factory, context));
                    }
                }
            }
        });
    }

    private void assertDifferential(String sql, SqlExecutionContextImpl context, boolean enabled) throws Exception {
        String expected;
        String baselinePlan;
        int[] types;
        try (SqlExecutionContextImpl baselineContext = context(context.getSharedQueryWorkerCount())) {
            baselineContext.setParallelHashJoinGroupByEnabled(false);
            baselineContext.setParallelGroupByEnabled(context.isParallelGroupByEnabled());
            baselineContext.setParallelFilterEnabled(context.isParallelFilterEnabled());
            baselineContext.setJitMode(context.getJitMode());
            baselineContext.with(AllowAllSecurityContext.INSTANCE, context.getBindVariableService(), null, -1, null);
            try (RecordCursorFactory baseline = engine.select(sql, baselineContext)) {
                baselinePlan = plan(baseline, baselineContext);
                Assert.assertFalse(baselinePlan.contains("Async Hash Join Group By"));
                expected = result(baseline, baselineContext);
                types = new int[baseline.getMetadata().getColumnCount()];
                for (int i = 0; i < types.length; i++) {
                    types[i] = baseline.getMetadata().getColumnType(i);
                }
            }
        }
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            String plan = plan(factory, context);
            Assert.assertEquals(sql + "\n" + plan, enabled, plan.contains("Async Hash Join Group By"));
            if (!enabled) {
                Assert.assertEquals(sql, baselinePlan, plan);
            }
            Assert.assertEquals(types.length, factory.getMetadata().getColumnCount());
            for (int i = 0; i < types.length; i++) {
                Assert.assertEquals(sql, types[i], factory.getMetadata().getColumnType(i));
            }
            Assert.assertEquals(sql, expected, result(factory, context));
            Assert.assertEquals(sql, expected, result(factory, context));
        }
    }

    private SqlExecutionContextImpl context(int workers) {
        return new SqlExecutionContextImpl(engine, workers).with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
    }

    private void createTables() throws Exception {
        execute("create table r (plant_id int, reading_ts timestamp, energy_kwh double, irradiance_wm2 double) timestamp(reading_ts) partition by month");
        execute("create table p (plant_id int, country symbol, installed_kwp double)");
        execute("insert into r values (1, '2020-01-01', 10, 100), (3, '2020-01-02', 30, 300), "
                + "(1, '2020-01-03', 20, 200), (2, '2020-02-01', 40, null), (null, '2021-01-01', 50, 500)");
        execute("insert into p values (1, 'ES', 5), (1, 'ES', 7), (1, 'IT', null), (2, null, null), (null, 'ES', 11)");
    }

    private SqlExecutionContextImpl enabledContext() {
        SqlExecutionContextImpl context = context(4);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    private static AsyncHashJoinGroupByRecordCursorFactory fused(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncHashJoinGroupByRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (AsyncHashJoinGroupByRecordCursorFactory) factory;
    }

    private static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    private static String result(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            return sink.toString();
        }
    }
}
