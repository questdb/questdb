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
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.std.Numbers;
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
            Assert.assertTrue(new DefaultCairoConfiguration(root).isSqlParallelHashJoinGroupByEnabled());
            Assert.assertTrue(configuration.isSqlParallelHashJoinGroupByEnabled());
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
    public void testInnerJoinBuildsSmallerTable() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Equal sizes keep the join order: the second table builds.
                assertBuild("r join p", "p", false, 5, context);
                assertBuild("p join r", "r", false, 5, context);
                execute("INSERT INTO p VALUES (3, 'FR', 13), (4, 'DE', 17)");
                // p has 7 rows and r has 5. INNER builds r in either order, and the outer joins
                // keep building the table that does not preserve its rows, whatever its size.
                assertBuild("r join p", "r", true, 5, context);
                assertBuild("p join r", "r", false, 5, context);
                assertBuild("r left join p", "p", false, 7, context);
                assertBuild("p right join r", "p", true, 7, context);
                assertBuild("p left join r", "r", false, 5, context);
                assertBuild("r right join p", "r", true, 5, context);
                execute("INSERT INTO r VALUES (4, '2021-02-01', 60, 600), (5, '2021-03-01', 70, 700), (NULL, '2021-04-01', 80, NULL)");
                // r has 8 rows and p has 7. INNER builds p in either order.
                assertBuild("r join p", "p", false, 7, context);
                assertBuild("p join r", "p", true, 7, context);
                assertBuild("p left join r", "r", false, 8, context);
                assertBuild("r right join p", "r", true, 8, context);
                assertQuery("SELECT count(*) pairs, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity"
                        + " FROM p JOIN r ON r.plant_id = p.plant_id")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan("""
                                Async Hash Join Group By workers: 4
                                  logicalJoinType: inner
                                  physicalJoinType: inner
                                  inputSwapped: true
                                  condition: r.plant_id=p.plant_id
                                  buildStrategy: shared
                                  buildPayload: copied when the probe is larger
                                  aggregation: scalar
                                  values: [count(*),sum(r.energy_kwh),sum(p.installed_kwp)]
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: r
                                    Build
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: p
                                """)
                        .returns("""
                                pairs\tenergy\tcapacity
                                11\t350.0\t76.0
                                """);
            }
        });
    }

    @Test
    public void testInnerJoinOrientationDecidesEligibility() throws Exception {
        assertMemoryLeak(() -> {
            // A predicate on the indexed r.s turns the scan of r into an index scan, which
            // exposes no page frames. The build keeps row ids that probes read back through the
            // build's frames, so r qualifies as neither input.
            execute("CREATE TABLE r (id INT, d DOUBLE, s SYMBOL INDEX, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
            execute("CREATE TABLE p (id INT, d DOUBLE, s SYMBOL, t TIMESTAMP) TIMESTAMP(t) PARTITION BY DAY");
            execute("INSERT INTO r VALUES (1, 1, 'a', '2020-01-01'), (2, 2, 'b', '2020-01-02')");
            execute("INSERT INTO p VALUES (1, 4, 'a', '2020-01-01'), (2, 8, 'b', '2020-01-02'), (1, 16, 'a', '2020-01-03')");
            String select = "SELECT sum(r.d) rd, sum(p.d) pd, count(*) n";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // r is the smaller table, so INNER builds it in either order, and the index scan
                // keeps the ordinary plan.
                assertDifferential(select + " FROM r JOIN p ON r.id = p.id WHERE r.s = 'a'", context, false);
                assertDifferential(select + " FROM p JOIN r ON r.id = p.id WHERE r.s = 'a'", context, false);
                // LEFT preserves r, so r is the probe and the query keeps the ordinary plan.
                assertDifferential(select + " FROM r LEFT JOIN p ON r.id = p.id WHERE r.s = 'a'", context, false);
                execute("INSERT INTO r VALUES (1, 32, 'a', '2020-01-04'), (3, 64, 'c', '2020-01-05')");
                // r is the larger table now, so INNER probes it in either order, and the order
                // that used to build r keeps the ordinary plan as well.
                assertDifferential(select + " FROM r JOIN p ON r.id = p.id WHERE r.s = 'a'", context, false);
                assertDifferential(select + " FROM p JOIN r ON r.id = p.id WHERE r.s = 'a'", context, false);
                // RIGHT builds r whatever the sizes.
                assertDifferential(select + " FROM r RIGHT JOIN p ON r.id = p.id", context, true);
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
    public void testSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Equal key text has different symbol keys in the two tables, '3' exists only in r,
            // '9' only in p, and both sides have null keys.
            execute("create table r (plant_id symbol, reading_ts timestamp, energy_kwh double, irradiance_wm2 double, "
                    + "plant_str string, plant_vc varchar) timestamp(reading_ts) partition by month");
            execute("create table p (plant_id symbol, country symbol, installed_kwp double, plant_str string, plant_vc varchar)");
            execute("""
                    insert into p values
                        ('9', 'FR', 3, '9', '9'), ('2', null, null, '2', '2'), ('1', 'ES', 5, '1', '1'),
                        ('1', 'ES', 7, '1', '1'), ('1', 'IT', null, '1', '1'), (null, 'ES', 11, null, null)
                    """);
            execute("""
                    insert into r values
                        ('1', '2020-01-01', 10, 100, '1', '1'), ('3', '2020-01-02', 30, 300, '3', '3'),
                        ('1', '2020-01-03', 20, 200, '1', '1'), ('2', '2020-02-01', 40, null, '2', '2'),
                        (null, '2021-01-01', 50, 500, null, null)
                    """);
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int j = 0; j < JOINS.length; j++) {
                    String join = JOINS[j];
                    String sql = SELECT + join + " order by country, yr, mo";
                    assertDifferential(sql, context, true);
                    assertDifferential(SCALAR_SELECT + join, context, true);
                    assertDifferential("select p.plant_id, r.plant_id rp, count(*) n, sum(r.energy_kwh) energy"
                            + join + " order by p.plant_id, rp", context, true);
                    assertDifferential(SELECT + join + " where p.country = 'ES' order by country, yr, mo", context, true);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        String plan = plan(factory, context);
                        Assert.assertTrue(plan, plan.contains("symbolKeyJoin: true"));
                        // p has one row more than r, so the INNER join builds r, as RIGHT does.
                        Assert.assertTrue(plan, plan.contains("inputSwapped: " + (j != 1)));
                    }
                    // Mixed text keys reconcile to STRING or VARCHAR and stage that text into the
                    // build's map, which selects the same rows the symbol translation selects.
                    for (String column : new String[]{"plant_str", "plant_vc"}) {
                        assertDifferential(SELECT + join.replace("p.plant_id", "p." + column) + " order by country, yr, mo", context, true);
                        assertDifferential(SELECT + join.replace("r.plant_id", "r." + column) + " order by country, yr, mo", context, true);
                    }
                }
                context.setParallelHashJoinGroupByEnabled(false);
                assertDifferential(SELECT + JOINS[0] + " order by country, yr, mo", context, false);
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
                        "select arg_min(r.energy_kwh, r.plant_id), arg_max(r.energy_kwh, r.plant_id)" + JOINS[0],
                        "select count(distinct p.country)" + JOINS[1],
                        "select mode(r.energy_kwh), approx_percentile(r.energy_kwh, 0.5)" + JOINS[2],
                        "select p.country, first(r.energy_kwh), last(r.energy_kwh)" + JOINS[0] + " order by country",
                        "select p.country, sum(r.plant_id), mode(r.energy_kwh)" + JOINS[0] + " order by country",
                        SELECT + " from r join p on r.plant_id::long=p.plant_id::long order by country,yr,mo",
                        SELECT + JOINS[1] + " and r.energy_kwh > 0 order by country,yr,mo",
                        SELECT + JOINS[1] + " where r.energy_kwh > 0 or p.installed_kwp > 0 order by country,yr,mo",
                        SELECT + " from (r limit 2) r join p on r.plant_id=p.plant_id order by country,yr,mo",
                        // A sub-query predicate is a token-less QUERY node in the post-join filter.
                        SELECT + JOINS[0] + " where (select true from long_sequence(1)) order by country,yr,mo",
                        SCALAR_SELECT + JOINS[1] + " where (select false from long_sequence(1))",
                        SELECT + JOINS[0] + " join p p2 on r.plant_id=p2.plant_id order by country,yr,mo"
                }) {
                    assertDifferential(sql, context, false);
                }
                // A second equality is a key, not a residual predicate, so the pair stages a
                // composite key instead of keeping the ordinary plan.
                assertDifferential(SELECT + JOINS[0] + " and r.energy_kwh=p.installed_kwp order by country,yr,mo", context, true);
            }
        });
    }

    @Test
    public void testClausesOutsideTheJoinKeepOrdinaryPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertQuery("SELECT count(*) pairs, sum(r.energy_kwh) energy" + JOINS[0] + " WHERE 1 = 0")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("Empty table")
                        .withPlanNotContaining("Hash Join Group By")
                        .returns("""
                                pairs\tenergy
                                0\tnull
                                """);
                assertQuery("SELECT reading_ts, count(*) pairs, sum(r.energy_kwh) energy" + JOINS[0]
                        + " WHERE r.reading_ts < '2020-01-04' SAMPLE BY 1d FILL(NULL) ALIGN TO CALENDAR")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .timestamp("reading_ts")
                        .withPlanContaining("Sample By Fill")
                        .withPlanNotContaining("Hash Join Group By")
                        .returns("""
                                reading_ts\tpairs\tenergy
                                2020-01-01T00:00:00.000000Z\t3\t30.0
                                2020-01-02T00:00:00.000000Z\tnull\tnull
                                2020-01-03T00:00:00.000000Z\t3\t60.0
                                """);
                assertQuery("SELECT count(*) pairs, sum(energy) energy FROM ((SELECT r.reading_ts ts, p.country, r.energy_kwh energy"
                        + JOINS[0] + ") LATEST ON ts PARTITION BY country)")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlanContaining("LatestBy")
                        .withPlanNotContaining("Hash Join Group By")
                        .returns("""
                                pairs\tenergy
                                3\t110.0
                                """);
                // Without fill values generateFill() returns the GROUP BY unchanged, so SAMPLE BY stays fused.
                for (String fill : new String[]{"", " FILL(NONE)"}) {
                    assertDifferential("SELECT reading_ts, count(*) pairs, sum(r.energy_kwh) energy" + JOINS[0]
                            + " SAMPLE BY 1d" + fill + " ALIGN TO CALENDAR", context, true);
                }
            }
        });
    }

    @Test
    public void testBuildExplainAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                String sql = "select r.plant_id, count(*) pairs, sum(r.energy_kwh) energy" + JOINS[1]
                        + " where p.installed_kwp is null order by r.plant_id";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                    plan(factory, context);
                    Assert.assertNull(fused.getAtom().getFrozenBuild());
                    String expected = result(factory, context);
                    // Cursor close releases the build.
                    Assert.assertNull(fused.getAtom().getFrozenBuild());
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        FrozenHashJoinBuild build = fused.getAtom().getFrozenBuild();
                        Assert.assertEquals(5, build.getRowCount());
                        Assert.assertEquals(3, build.getKeyCount());
                        Assert.assertTrue(build.getSizeInBytes() > 0);
                        Assert.assertTrue(cursor.hasNext());
                    }
                    Assert.assertEquals(expected, result(factory, context));
                    execute("truncate table p");
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertEquals(0, fused.getAtom().getFrozenBuild().getRowCount());
                        Assert.assertTrue(cursor.hasNext());
                    }
                    Assert.assertNull(fused.getAtom().getFrozenBuild());
                }
            }
        });
    }

    @Test
    public void testBuildFiltersRunInTheOperator() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    // The build walks the scan's page frames and runs the scan's stolen filter
                    // itself, so the Build child is the bare scan and the filter an attribute. The
                    // attribute's name tells a compiled filter from an interpreted one.
                    final String buildFilter = jit == SqlJitMode.JIT_MODE_ENABLED ? "buildJitFilter" : "buildFilter";
                    assertQuery("SELECT count(*) pairs, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity"
                            + " FROM r JOIN p ON r.plant_id = p.plant_id WHERE p.installed_kwp > 5")
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlan("""
                                    Async Hash Join Group By workers: 4
                                      logicalJoinType: inner
                                      physicalJoinType: inner
                                      inputSwapped: false
                                      condition: r.plant_id=p.plant_id
                                      buildStrategy: shared
                                      buildPayload: copied when the probe is larger
                                      aggregation: scalar
                                      values: [count(*),sum(r.energy_kwh),sum(p.installed_kwp)]
                                      %s: 5<installed_kwp
                                        Probe
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: r
                                        Build
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: p
                                    """.formatted(buildFilter))
                            .returns("""
                                    pairs\tenergy\tcapacity
                                    3\t80.0\t25.0
                                    """);
                    // An outer join's ON conditions on the build alone drop build rows, next to
                    // whatever filter the scan carries.
                    assertQuery("SELECT count(*) pairs, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity"
                            + " FROM r LEFT JOIN p ON r.plant_id = p.plant_id AND p.installed_kwp > 5")
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlan("""
                                    Async Hash Join Group By workers: 4
                                      logicalJoinType: left outer
                                      physicalJoinType: left outer
                                      inputSwapped: false
                                      condition: r.plant_id=p.plant_id
                                      buildStrategy: shared
                                      buildPayload: copied when the probe is larger
                                      aggregation: scalar
                                      values: [count(*),sum(r.energy_kwh),sum(p.installed_kwp)]
                                      buildOnFilter: 5<installed_kwp
                                        Probe
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: r
                                        Build
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: p
                                    """)
                            .returns("""
                                    pairs\tenergy\tcapacity
                                    5\t150.0\t25.0
                                    """);
                    // Both filters at once, and a projection above the filtered scan.
                    assertDifferential("SELECT count(*) pairs, sum(r.energy_kwh) energy, sum(p.cap) capacity FROM r"
                            + " LEFT JOIN (SELECT installed_kwp cap, plant_id FROM p WHERE country = 'ES') p"
                            + " ON r.plant_id = p.plant_id AND p.cap > 5", context, true);
                }
            }
        });
    }

    @Test
    public void testSerialInputFilterKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.setParallelFilterEnabled(false);
                for (String join : JOINS) {
                    assertDifferential(SELECT + join + " where r.reading_ts >= '2020-02-01' and r.energy_kwh > 10"
                            + " order by country,yr,mo", context, false);
                }
                // The build walks page frames too, so a serial build filter keeps the ordinary plan as well.
                assertDifferential(SELECT + JOINS[0] + " where p.installed_kwp > 5 order by country,yr,mo", context, false);
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
                for (String join : JOINS) {
                    for (String projection : new String[]{SELECT, SCALAR_SELECT}) {
                        String sql = projection + join + " where r.energy_kwh > 10"
                                + (projection.equals(SELECT) ? " order by country,yr,mo" : "");
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
                }
            }
        });
    }

    @Test
    public void testLargerBuildStillSelectsShared() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            // INNER builds the smaller r; LEFT and RIGHT keep building the larger p.
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
    public void testRightJoinBuildSizeBound() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(32 * Numbers.SIZE_1MB, new DefaultCairoConfiguration(root).getSqlParallelHashJoinGroupByRightJoinMaxBuildSize());
            Assert.assertEquals(32 * Numbers.SIZE_1MB, configuration.getSqlParallelHashJoinGroupByRightJoinMaxBuildSize());
            createTables();
            // r and p have 5 rows each. A build row is an 8-byte link plus, when the build has payload
            // columns, the 8-byte id of its row: 16 bytes whatever the number and width of the payload
            // columns, 8 without any. The key column is not a payload column.
            final String narrow = "SELECT count(*) n, sum(r.energy_kwh) energy, sum(p.installed_kwp) capacity FROM ";
            final String wide = "SELECT count(*) n, sum(r.energy_kwh) energy, sum(r.irradiance_wm2) irradiance,"
                    + " sum(p.installed_kwp) capacity FROM ";
            final String keysOnly = "SELECT count(*) n, sum(p.installed_kwp) capacity FROM ";
            final String on = " ON r.plant_id = p.plant_id";
            final String[] joins = {"r RIGHT JOIN p", "p RIGHT JOIN r", "r LEFT JOIN p", "p LEFT JOIN r"};
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Every build with payload columns takes 80 bytes, narrow or wide, so a bound of 80
                // fuses every spelling.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 80);
                for (String join : joins) {
                    assertDifferential(narrow + join + on, context, true);
                    assertDifferential(wide + join + on, context, true);
                }
                // A build of r without payload columns stores links alone: 40 bytes. One byte less
                // keeps the RIGHT join that builds r on the ordinary plan, and the LEFT joins, which
                // take no bound, still fuse.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 39);
                assertDifferential(keysOnly + joins[0] + on, context, false);
                assertDifferential(keysOnly + joins[3] + on, context, true);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 40);
                assertDifferential(keysOnly + joins[0] + on, context, true);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 80);
                // A factory compiled under the bound keeps the fused plan after the table outgrows it.
                try (RecordCursorFactory factory = engine.select(narrow + joins[0] + on, context)) {
                    Assert.assertTrue(plan(factory, context).contains("Async Hash Join Group By"));
                    execute("INSERT INTO r VALUES (1, '2021-02-01', 60, 600)");
                    Assert.assertEquals("""
                            n\tenergy\tcapacity
                            11:LONG\t360.0:DOUBLE\t47.0:DOUBLE
                            """, result(factory, context));
                    try (RecordCursor ignored = factory.getCursor(context)) {
                        Assert.assertEquals(6, fused(factory).getAtom().getFrozenBuild().getRowCount());
                    }
                }
                // A new compile reads the new size: 6 rows take 96 bytes.
                assertDifferential(narrow + joins[0] + on, context, false);
                // The bound counts the table's rows, not the rows that a filter leaves in the build.
                assertDifferential(narrow + joins[0] + on + " AND r.energy_kwh > 100", context, false);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 96);
                assertDifferential(narrow + joins[0] + on + " AND r.energy_kwh > 100", context, true);
                // One byte short of the 5-row build of p: the RIGHT join keeps the ordinary plan
                // and the LEFT join that builds p still fuses.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 79);
                assertDifferential(narrow + joins[1] + on, context, false);
                assertDifferential(narrow + joins[2] + on, context, true);
                assertDifferential(narrow + joins[3] + on, context, true);
                // A bound of zero fuses a RIGHT join only over an empty build table.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_RIGHT_JOIN_MAX_BUILD_SIZE, 0);
                execute("CREATE TABLE e (plant_id INT, installed_kwp DOUBLE)");
                assertDifferential("SELECT count(*) n, sum(r.energy_kwh) energy, sum(e.installed_kwp) capacity"
                        + " FROM e RIGHT JOIN r ON r.plant_id = e.plant_id", context, true);
                assertDifferential(narrow + joins[1] + on, context, false);
            }
        });
    }

    @Test
    public void testBuildPresizesOnlyFromExactCounts() throws Exception {
        assertMemoryLeak(() -> {
            // pb has twice the rows of pa and the same ten keys in each key column. pa spreads its
            // 1_000 rows over four days, 250 a day.
            execute("CREATE TABLE pa (k INT, l LONG, s SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, l LONG, s SYMBOL, v DOUBLE)");
            execute("""
                    INSERT INTO pa SELECT (x % 10)::INT, x % 10, 'S' || (x % 10), x,
                        timestamp_sequence('2020-01-01', 345_600_000L)
                    FROM long_sequence(1_000)
                    """);
            execute("INSERT INTO pb SELECT (x % 10)::INT, x % 10, 'S' || (x % 10), x FROM long_sequence(2_000)");
            // A build row is an eight-byte link plus, when the build has payload columns, the eight-byte
            // id of its row: 8 bytes with no payload, 16 with any. The INT layout's key table takes eight bytes a slot and starts at 64
            // slots; a table presized for N keys takes the next power of two of 2N slots. The row
            // heap grows by doubling from 64 bytes unless an exact row count presizes it.
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (boolean isParallelFilter : new boolean[]{true, false}) {
                    context.setParallelFilterEnabled(isParallelFilter);
                    // An unfiltered build with no more rows than its probe sizes both by its rows:
                    // 2_048 slots for 1_000 rows, although they hold ten keys.
                    assertBuildSize("SELECT count(*) n, sum(pb.v) v FROM pa JOIN pb ON pa.k = pb.k",
                            1_000, 10, 2_048 * 8 + 1_000 * 8, context);
                    assertBuildSize("SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k",
                            1_000, 10, 2_048 * 8 + 1_000 * 16, context);
                    // A LEFT join builds the table after it, here the larger one: its key table
                    // grows to its ten keys, while its heap still takes the exact row count.
                    assertBuildSize("SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k",
                            2_000, 10, 64 * 8 + 2_000 * 16, context);
                    // An interval scan counts its rows from its frames: 250 rows on one day.
                    assertBuildSize("SELECT count(*) n, sum(pb.v) v FROM pa JOIN pb ON pa.k = pb.k"
                            + " WHERE pa.ts IN '2020-01-02'", 250, 10, 512 * 8 + 250 * 8, context);
                    // A filter hides the build's row count: the heap doubles to 8_192 bytes for
                    // 8_000, and the key table grows to the ten keys. The build walks page frames
                    // and runs the filter it steals, so a serial filter keeps the ordinary plan.
                    final String filtered = "SELECT count(*) n, sum(pb.v) v FROM pa JOIN pb ON pa.k = pb.k WHERE pa.v > 0";
                    final String filteredInterval = "SELECT count(*) n, sum(pb.v) v FROM pa JOIN pb ON pa.k = pb.k"
                            + " WHERE pa.ts IN '2020-01-02' AND pa.v > 0";
                    if (isParallelFilter) {
                        assertBuildSize(filtered, 1_000, 10, 64 * 8 + 8_192, context);
                        assertBuildSize(filteredInterval, 250, 10, 64 * 8 + 2_048, context);
                    } else {
                        assertDifferential(filtered, context, false);
                        assertDifferential(filteredInterval, context, false);
                    }
                    // A SYMBOL key holds at most the build dictionary's ten keys and the null key:
                    // 22 slots round up to 32, below the initial 64.
                    assertBuildSize("SELECT count(*) n, sum(pb.v) v FROM pa JOIN pb ON pa.s = pb.s",
                            1_000, 10, 64 * 8 + 1_000 * 8, context);
                    // A LONG key takes an Unordered8Map, 16 bytes an entry plus one for the zero
                    // key, at a 0.7 load factor: 128 entries initially, 2_048 for 1_000 keys.
                    assertBuildSize("SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.l = pb.l",
                            1_000, 10, 2_049 * 16 + 1_000 * 16, context);
                    assertBuildSize("SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.l = pb.l",
                            2_000, 10, 129 * 16 + 2_000 * 16, context);
                }
            }
        });
    }

    @Test
    public void testPayloadCopyRule() throws Exception {
        assertMemoryLeak(() -> {
            // pb has twice the rows of pa. pa spreads its 1_000 rows over four days, 250 a day.
            execute("CREATE TABLE pa (k INT, l LONG, s SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("""
                    INSERT INTO pa SELECT (x % 10)::INT, x % 10, 'S' || (x % 10), x,
                        timestamp_sequence('2020-01-01', 345_600_000L)
                    FROM long_sequence(1_000)
                    """);
            execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(2_000)");
            // Each LEFT join builds pa's 1_000 rows and probes pb's 2_000. A copied DOUBLE takes 8
            // bytes a row; a DOUBLE, a LONG and a SYMBOL take 20, aligned to 24.
            final String narrow = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            final String wide = "SELECT count(*) n, sum(pa.v) v, sum(pa.l) l, count(pa.s) s FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // The defaults copy a build whose probe has at least twice its rows.
                assertPayloadCopy(narrow, true, context);
                assertPayloadCopy(wide, true, context);
                // The probe's count is its frame rows, before the probe filter drops 1_900 of them.
                assertPayloadCopy(narrow + " WHERE pb.v > 1_900", true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "2.001");
                assertPayloadCopy(narrow, false, context);
                // An ON filter on the build's columns leaves 500 rows, which the ratio compares.
                final String onFiltered = narrow + " AND pa.v <= 500";
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "4");
                assertPayloadCopy(onFiltered, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "4.001");
                assertPayloadCopy(onFiltered, false, context);

                // The byte bound takes the build's rows times the copied row size.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 8_000);
                assertPayloadCopy(narrow, true, context);
                assertPayloadCopy(wide, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 7_999);
                assertPayloadCopy(narrow, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 24_000);
                assertPayloadCopy(wide, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 23_999);
                assertPayloadCopy(wide, false, context);
                // EXPLAIN names the rule while a copied row can fit the bound, and only for a build
                // with payload columns.
                try (RecordCursorFactory factory = engine.select(wide, context)) {
                    Assert.assertTrue(plan(factory, context).contains("buildPayload: copied when the probe is larger"));
                }
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 23);
                try (RecordCursorFactory factory = engine.select(wide, context)) {
                    Assert.assertFalse(plan(factory, context).contains("buildPayload"));
                }
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, Long.MAX_VALUE);
                final String keysOnly = "SELECT count(*) n FROM pb LEFT JOIN pa ON pa.k = pb.k";
                assertPayloadCopy(keysOnly, false, context);
                try (RecordCursorFactory factory = engine.select(keysOnly, context)) {
                    Assert.assertFalse(plan(factory, context).contains("buildPayload"));
                }

                // An interval probe counts the rows of its interval: 250 of pa's rows against the
                // 2_000 rows of pb that the LEFT join builds, not pa's 1_000.
                final String interval = "SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k"
                        + " WHERE pa.ts IN '2020-01-02'";
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0.125");
                assertPayloadCopy(interval, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0.126");
                assertPayloadCopy(interval, false, context);

                // The rule runs per execution: a factory whose probe grows past the ratio copies on
                // its next execution, and releases the copy with each cursor.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "2.001");
                try (RecordCursorFactory factory = engine.select(narrow, context)) {
                    assertCopied(factory, false, context);
                    execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(1)");
                    assertCopied(factory, true, context);
                }
                assertDifferential(narrow, context, true);
            }
        });
    }

    @Test
    public void testScalarEmptyInputAndCursorReuse() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int j = 0; j < JOINS.length; j++) {
                    String sql = "select count(*) n, count(p.plant_id) c, sum(p.installed_kwp) s, avg(p.installed_kwp) a" + JOINS[j];
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                        Assert.assertFalse(fused.recordCursorSupportsRandomAccess());
                        Assert.assertTrue(plan(factory, context).contains("aggregation: scalar"));
                        Assert.assertNull(fused.getAtom().getFrozenBuild());
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
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertEquals(0, fused.getAtom().getFrozenBuild().getRowCount());
                            Assert.assertTrue(cursor.hasNext());
                        }
                        Assert.assertFalse(fused.getAtom().isSharded());
                        execute("insert into p values (1,'ES',5), (1,'ES',7), (1,'IT',null), (2,null,null), (null,'ES',11)");
                        Assert.assertEquals(expected, result(factory, context));
                    }
                }
            }
        });
    }

    // Checks the result against the ordinary plan, and the rows, keys and allocated bytes of the build.
    private void assertBuildSize(String sql, long rows, long keys, long sizeInBytes, SqlExecutionContextImpl context) throws Exception {
        assertDifferential(sql, context, true);
        try (
                RecordCursorFactory factory = engine.select(sql, context);
                RecordCursor ignored = factory.getCursor(context)
        ) {
            FrozenHashJoinBuild build = fused(factory).getAtom().getFrozenBuild();
            Assert.assertEquals(sql, rows, build.getRowCount());
            Assert.assertEquals(sql, keys, build.getKeyCount());
            Assert.assertEquals(sql, sizeInBytes, build.getSizeInBytes());
        }
    }

    // Checks whether an execution of the factory copies its build's payload, and that closing the cursor drops the copy.
    private static void assertCopied(RecordCursorFactory factory, boolean isCopied, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            // The owner copies once the probe's frames are known, on the first read.
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(isCopied, fused(factory).getAtom().isPayloadCopied());
        }
        Assert.assertFalse(fused(factory).getAtom().isPayloadCopied());
    }

    // Checks the results against the ordinary plan, and whether the fused plan copies the build's payload.
    private void assertPayloadCopy(String sql, boolean isCopied, SqlExecutionContextImpl context) throws Exception {
        assertDifferential(sql, context, true);
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            assertCopied(factory, isCopied, context);
        }
    }

    // Checks keyed and scalar results against the ordinary plan, the build input in EXPLAIN, and the build's row count.
    private void assertBuild(String join, String buildTable, boolean isInputSwapped, long buildRows, SqlExecutionContextImpl context) throws Exception {
        String from = " from " + join + " on r.plant_id=p.plant_id";
        assertDifferential(SELECT + from + " order by country, yr, mo", context, true);
        assertDifferential(SCALAR_SELECT + from, context, true);
        try (RecordCursorFactory factory = engine.select(SCALAR_SELECT + from, context)) {
            TextPlanSink sink = new TextPlanSink();
            sink.of(factory, context);
            StringSink lines = new StringSink();
            // Lines are numbered from 1.
            for (int i = 1; i <= sink.getLineCount(); i++) {
                lines.put(sink.getLine(i)).put('\n');
            }
            String plan = lines.toString();
            // The Probe child precedes the Build child, and each ends with the scan of its table.
            int probe = plan.indexOf("Probe\n");
            int build = plan.indexOf("Build\n");
            Assert.assertTrue(plan, plan.contains("inputSwapped: " + isInputSwapped + '\n'));
            Assert.assertTrue(plan, probe > 0 && build > probe);
            String probeTable = buildTable.equals("r") ? "p" : "r";
            Assert.assertTrue(plan, plan.substring(probe, build).contains(" on: " + probeTable + '\n'));
            Assert.assertTrue(plan, plan.substring(build).contains(" on: " + buildTable + '\n'));
            try (RecordCursor ignored = factory.getCursor(context)) {
                Assert.assertEquals(plan, buildRows, fused(factory).getAtom().getFrozenBuild().getRowCount());
            }
        }
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
                Assert.assertFalse(baselinePlan.contains("Hash Join Group By"));
                expected = result(baseline, baselineContext);
                types = new int[baseline.getMetadata().getColumnCount()];
                for (int i = 0; i < types.length; i++) {
                    types[i] = baseline.getMetadata().getColumnType(i);
                }
            }
        }
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            String plan = plan(factory, context);
            Assert.assertEquals(sql + "\n" + plan, enabled, plan.contains("Hash Join Group By"));
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
