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
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.NetworkSqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.DefaultSqlExecutionCircuitBreakerConfiguration;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.jit.JitUtil;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.CountingSqlExecutionCircuitBreaker;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
            for (boolean isGlobal : new boolean[]{false, true}) {
                for (boolean isExperimental : new boolean[]{false, true}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, Boolean.toString(isGlobal));
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_ENABLED, Boolean.toString(isExperimental));
                    Assert.assertEquals(isExperimental, configuration.isSqlParallelHashJoinGroupByEnabled());
                    for (int workers : new int[]{0, 1, 4}) {
                        try (SqlExecutionContextImpl context = context(workers)) {
                            boolean isEnabled = isGlobal && isExperimental && workers > 0;
                            Assert.assertEquals(isEnabled, context.isParallelHashJoinGroupByEnabled());
                            for (String join : JOINS) {
                                assertDifferential(SELECT + join + " order by country, yr, mo", context, isEnabled);
                                assertDifferential(SCALAR_SELECT + join, context, isEnabled);
                                assertDifferential("select count(*)" + join, context, isEnabled);
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
                                  buildPayload: copied when the probe is large enough
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
    public void testCachedInnerJoinRecompilesOnceItsBuildTableOutgrowsTheProbe() throws Exception {
        assertMemoryLeak(() -> {
            // d has 1_000 rows, one per key; f starts with 10, so INNER builds f.
            execute("CREATE TABLE d (k INT, s SYMBOL, g INT)");
            execute("INSERT INTO d SELECT x::INT, 'S' || x, (x % 10)::INT FROM long_sequence(1_000)");
            for (String key : new String[]{"k", "s"}) {
                for (String where : new String[]{"", " WHERE f.ts >= '2020-01-01'"}) {
                    for (int workers : new int[]{1, 4}) {
                        execute("DROP TABLE IF EXISTS f");
                        // The capacity holds every key of d, so that no insert rebuilds the symbol map,
                        // which would change the table's metadata version.
                        execute("CREATE TABLE f (k INT, s SYMBOL CAPACITY 2048, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
                        insertFactRows(10, "2020-01-01");
                        final String sql = "SELECT d.g, count(*) n, sum(f.v) v FROM f JOIN d ON f." + key + " = d." + key
                                + where + " ORDER BY d.g";
                        // An interval compiles both orientations, unless the key is a SYMBOL pair.
                        final boolean isBuildChoice = key.equals("k") && !where.isEmpty();
                        try (SqlExecutionContextImpl context = context(workers)) {
                            context.setParallelGroupByEnabled(true);
                            context.setParallelHashJoinGroupByEnabled(true);
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                final String plan = plan(factory, context);
                                Assert.assertEquals(sql + '\n' + plan, isBuildChoice, plan.contains("Hash Join Group By Build Choice"));
                                Assert.assertTrue(sql + '\n' + plan, plan.contains("inputSwapped: true"));
                                Assert.assertEquals(sql, 10, builtRows(factory, context));
                                // At twice the rows of d, a reused factory that builds f keeps building it,
                                // and the build choice builds d, which has fewer rows in its intervals.
                                insertFactRows(1_990, "2020-01-02");
                                Assert.assertEquals(sql, isBuildChoice ? 1_000 : 2_000, builtRows(factory, context));
                                insertFactRows(1, "2020-01-03");
                                if (isBuildChoice) {
                                    // The orientation that builds d does not presize, and asks for nothing.
                                    Assert.assertEquals(sql, 1_000, builtRows(factory, context));
                                } else {
                                    // f has more than twice the rows of d: the factory asks the caches
                                    // to compile the query again rather than build f.
                                    try {
                                        builtRows(factory, context);
                                        Assert.fail(sql + ": a factory that would build the larger table must ask for a recompile");
                                    } catch (TableReferenceOutOfDateException e) {
                                        TestUtils.assertContains(e.getFlyweightMessage(), "[table=f]");
                                        Assert.assertNull(context.getMemoryTracker());
                                        Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                    }
                                }
                            }
                            // A new compile builds d.
                            assertDifferential(sql, context, true);
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                final String plan = plan(factory, context);
                                Assert.assertTrue(sql + '\n' + plan, isBuildChoice || plan.contains("inputSwapped: false"));
                                Assert.assertEquals(sql, 1_000, builtRows(factory, context));
                                Assert.assertEquals(sql, 1_000, builtRows(factory, context));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCachedInnerJoinRecompileKeepsTheBuildUnderTheMemoryLimit() throws Exception {
        assertMemoryLeak(() -> {
            // f starts with 1_000 rows and d has 5_000, so INNER builds f. Once f has 201_000 rows, a
            // build of f takes about 7 MiB and a build of d about 0.2 MiB.
            execute("CREATE TABLE f (k INT, v DOUBLE)");
            execute("CREATE TABLE d (k INT, g INT)");
            execute("INSERT INTO d SELECT x::INT, (x % 10)::INT FROM long_sequence(5_000)");
            execute("INSERT INTO f SELECT (x % 5_000 + 1)::INT, x FROM long_sequence(1_000)");
            final String sql = "SELECT d.g, sum(f.v) v FROM f JOIN d ON f.k = d.k ORDER BY d.g";
            try (SqlExecutionContextImpl context = enabledContext()) {
                RecordCursorFactory factory = engine.select(sql, context);
                try {
                    Assert.assertTrue(plan(factory, context).contains("inputSwapped: true"));
                    Assert.assertEquals(1_000, builtRows(factory, context));
                    execute("INSERT INTO f SELECT (x % 5_000 + 1)::INT, x FROM long_sequence(200_000)");
                    final String expected;
                    context.setParallelHashJoinGroupByEnabled(false);
                    try (RecordCursorFactory baseline = engine.select(sql, context)) {
                        expected = result(baseline, context);
                    } finally {
                        context.setParallelHashJoinGroupByEnabled(true);
                    }
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 2 * Numbers.SIZE_1MB);
                    // The HTTP and PG query caches run a cached factory this way: when the factory
                    // throws TableReferenceOutOfDateException, the cache closes it and compiles the
                    // query again.
                    int recompileCount = 0;
                    String actual;
                    while (true) {
                        try {
                            actual = result(factory, context);
                            break;
                        } catch (TableReferenceOutOfDateException e) {
                            Assert.assertEquals(0, recompileCount++);
                            factory = Misc.free(factory);
                            factory = engine.select(sql, context);
                        }
                    }
                    Assert.assertEquals(1, recompileCount);
                    Assert.assertEquals(expected, actual);
                    Assert.assertTrue(plan(factory, context).contains("inputSwapped: false"));
                    Assert.assertEquals(5_000, builtRows(factory, context));
                } finally {
                    setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
                    Misc.free(factory);
                }
            }
        });
    }

    @Test
    public void testCachedOuterJoinStopsPresizingOnceItsBuildTableOutgrowsTheProbe() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE pa (k INT, v DOUBLE)");
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, x FROM long_sequence(1_000)");
            for (String sql : new String[]{
                    "SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k",
                    "SELECT count(*) n, sum(pb.v) v FROM pb RIGHT JOIN pa ON pa.k = pb.k"
            }) {
                execute("TRUNCATE TABLE pb");
                execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(500)");
                try (SqlExecutionContextImpl context = enabledContext()) {
                    // Both joins build pb. It has fewer rows than pa, so the build sizes its key table by
                    // its rows: 1_024 slots for 500 rows over ten keys.
                    assertBuildSize(sql, 500, 10, 1_024 * 8 + 500 * 16, context);
                    try (RecordCursorFactory factory = engine.select(sql, context)) {
                        Assert.assertEquals(sql, 500, builtRows(factory, context));
                        // Twice the rows of pa keep the presized key table of a reused factory.
                        execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(1_500)");
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(sql, 4_096 * 8 + 2_000 * 16, fused(factory).getAtom().getFrozenBuild().getSizeInBytes());
                        }
                        execute("INSERT INTO pb VALUES (1, 1)");
                        try {
                            builtRows(factory, context);
                            Assert.fail(sql + ": a factory that presizes for the larger table must ask for a recompile");
                        } catch (TableReferenceOutOfDateException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "[table=pb]");
                            Assert.assertNull(context.getMemoryTracker());
                            Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                        }
                    }
                    // A new compile lets the key table grow to the ten keys.
                    assertBuildSize(sql, 2_001, 10, 64 * 8 + 2_001 * 16, context);
                }
            }
        });
    }

    @Test
    public void testCachedVariableSizeBuildOnFilterRecompilesOnceItsBuildTableOutgrowsTheProbe() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (id INT, k INT)");
            execute("CREATE TABLE b (id INT, v DOUBLE, vc VARCHAR)");
            execute("INSERT INTO p SELECT x::INT, (x % 2)::INT FROM long_sequence(10)");
            // b's ON condition holds for the rows whose text contains a 1.
            final String[] queries = {
                    "SELECT p.k, count(*) c, count(b.id) m, sum(b.v) s FROM p LEFT JOIN b ON p.id = b.id AND b.vc ~ '1' ORDER BY k",
                    "SELECT p.k, count(*) c, count(b.id) m, sum(b.v) s FROM b RIGHT JOIN p ON p.id = b.id AND b.vc ~ '1' ORDER BY k"
            };
            for (String sql : queries) {
                execute("TRUNCATE TABLE b");
                execute("INSERT INTO b SELECT x::INT, x, 'x' || x FROM long_sequence(10)");
                try (SqlExecutionContextImpl context = enabledContext()) {
                    RecordCursorFactory factory = engine.select(sql, context);
                    try {
                        // Both joins build b, which holds as many rows as p, so the query takes the
                        // fused plan.
                        final String fusedPlan = plan(factory, context);
                        Assert.assertTrue(sql + '\n' + fusedPlan, fusedPlan.contains("buildOnFilter: vc ~ 1"));
                        assertFactory(factory).withContext(context).expectSize().returns("""
                                k\tc\tm\ts
                                0\t5\t1\t10.0
                                1\t5\t1\t1.0
                                """);
                        // At twice the rows of p, a reused factory keeps the fused plan, while a new
                        // compile keeps the ordinary plan.
                        execute("INSERT INTO b SELECT (x % 10 + 1)::INT, 100 + x, 'y' || x FROM long_sequence(10)");
                        final String expected = """
                                k\tc\tm\ts
                                0\t5\t2\t111.0
                                1\t6\t2\t111.0
                                """;
                        assertFactory(factory).withContext(context).expectSize().returns(expected);
                        assertQuery(sql)
                                .withContext(context)
                                .noLeakCheck()
                                .withPlanNotContaining("Hash Join Group By")
                                .expectSize()
                                .returns(expected);
                        // Past twice the rows of p, the factory asks the caches to compile the query
                        // again, as the HTTP and PG query caches run a cached factory, and the new
                        // compile keeps the ordinary plan.
                        execute("INSERT INTO b VALUES (5, 1_000, 'z1')");
                        int recompileCount = 0;
                        while (true) {
                            try {
                                result(factory, context);
                                break;
                            } catch (TableReferenceOutOfDateException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "[table=b]");
                                Assert.assertNull(context.getMemoryTracker());
                                Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                Assert.assertEquals(sql, 0, recompileCount++);
                                factory = Misc.free(factory);
                                factory = engine.select(sql, context);
                            }
                        }
                        Assert.assertEquals(sql, 1, recompileCount);
                        final String ordinaryPlan = plan(factory, context);
                        Assert.assertFalse(sql + '\n' + ordinaryPlan, ordinaryPlan.contains("Hash Join Group By"));
                        assertFactory(factory).withContext(context).expectSize().returns("""
                                k\tc\tm\ts
                                0\t5\t2\t111.0
                                1\t6\t3\t1111.0
                                """);
                    } finally {
                        Misc.free(factory);
                    }
                }
            }
        });
    }

    @Test
    public void testCheapVariableSizeBuildOnFilterTakesFusedPlanWithLargerBuildTable() throws Exception {
        assertMemoryLeak(() -> {
            createVariableSizeOnFilterTables();
            final String select = "SELECT p.k, count(*) c, count(b.id) m, sum(b.v) s";
            // Both joins build b, which holds 6 rows against p's 4.
            final String[] froms = {
                    " FROM p LEFT JOIN b ON p.id = b.id AND ",
                    " FROM b RIGHT JOIN p ON p.id = b.id AND "
            };
            // Each ON condition only compares columns and constants, so it costs about as much on a
            // build row as the hash insert it spares, and keeps the fused plan whatever the sizes.
            // IS NULL and IS NOT NULL parse as a comparison with a NULL constant.
            final String[][] cases = {
                    {"b.vc = 'xa'", "buildOnFilter: vc='xa'", """
                            k\tc\tm\ts
                            1\t2\t1\t10.0
                            2\t2\t0\tnull
                            """},
                    {"b.vc IS NULL", "buildOnFilter: vc is null", """
                            k\tc\tm\ts
                            1\t2\t0\tnull
                            2\t2\t1\t40.0
                            """},
                    {"b.str = 'sa'", "buildOnFilter: str='sa'", """
                            k\tc\tm\ts
                            1\t2\t1\t10.0
                            2\t2\t1\t40.0
                            """},
                    {"b.vc <> 'xa' AND b.v > 15", "buildOnFilter: (vc!='xa' and 15<v)", """
                            k\tc\tm\ts
                            1\t2\t2\t50.0
                            2\t2\t1\t40.0
                            """},
                    {"b.vc < 'y' AND NOT b.v > 25", "buildOnFilter: (vc<'y' and not (25<v))", """
                            k\tc\tm\ts
                            1\t2\t1\t10.0
                            2\t2\t0\tnull
                            """}
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String from : froms) {
                    for (String[] c : cases) {
                        assertQuery(select + from + c[0] + " ORDER BY k")
                                .withContext(context)
                                .noLeakCheck()
                                .withPlanContaining("Async Hash Join Group By", c[1])
                                .expectSize()
                                .returns(c[2]);
                    }
                }
            }
        });
    }

    @Test
    public void testExpensiveVariableSizeBuildOnFilterNeedsProbeTableAsLargeAsBuildTable() throws Exception {
        assertMemoryLeak(() -> {
            createVariableSizeOnFilterTables();
            final String select = "SELECT p.k, count(*) c, count(b.id) m, sum(b.v) s";
            // Both joins build b, which holds 6 rows against p's 4: a LEFT join builds the input after
            // it, and a RIGHT join the input before it.
            final String[] froms = {
                    " FROM p LEFT JOIN b ON p.id = b.id AND ",
                    " FROM b RIGHT JOIN p ON p.id = b.id AND "
            };
            final String regexRows = """
                    k\tc\tm\ts
                    1\t2\t2\t40.0
                    2\t2\t0\tnull
                    """;
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String from : froms) {
                    // The fused plan would run an ON condition over a variable-size build column on
                    // each of b's rows, and the ordinary plan runs it only on the pairs whose keys
                    // match, so a build table larger than the probe table keeps the ordinary plan
                    // for a condition that does more than compare columns and constants: a regular
                    // expression, LIKE, IN, a function or a cast, alone or next to a comparison.
                    for (String on : new String[]{"b.vc ~ 'x'", "b.vc LIKE 'x%'", "b.vc IN ('xa', 'xc')", "upper(b.vc) LIKE 'X%'"}) {
                        assertQuery(select + from + on + " ORDER BY k")
                                .withContext(context)
                                .noLeakCheck()
                                .withPlanNotContaining("Hash Join Group By")
                                .expectSize()
                                .returns(regexRows);
                    }
                    assertQuery(select + from + "b.str::VARCHAR = 'sa' ORDER BY k")
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanNotContaining("Hash Join Group By")
                            .expectSize()
                            .returns("""
                                    k\tc\tm\ts
                                    1\t2\t1\t10.0
                                    2\t2\t1\t40.0
                                    """);
                    assertQuery(select + from + "b.vc = 'xa' AND b.vc ~ 'x' ORDER BY k")
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanNotContaining("Hash Join Group By")
                            .expectSize()
                            .returns("""
                                    k\tc\tm\ts
                                    1\t2\t1\t10.0
                                    2\t2\t0\tnull
                                    """);
                    assertQuery(select + from + "array_sum(b.arr) > 2.5 ORDER BY k")
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanNotContaining("Hash Join Group By")
                            .expectSize()
                            .returns("""
                                    k\tc\tm\ts
                                    1\t2\t1\t30.0
                                    2\t2\t0\tnull
                                    """);
                    // An ON condition over fixed-size columns costs about as much as the hash insert
                    // it saves, so it keeps the fused plan.
                    assertQuery(select + from + "b.v > 15 ORDER BY k")
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanContaining("Async Hash Join Group By", "buildOnFilter: 15<v")
                            .expectSize()
                            .returns("""
                                    k\tc\tm\ts
                                    1\t2\t2\t50.0
                                    2\t2\t1\t40.0
                                    """);
                }
                // b's own WHERE clause runs on each of its rows in both plans, so it keeps the fused
                // plan, and the rows are those of the ON condition that it spells.
                final String filteredB = "(SELECT * FROM b WHERE vc ~ 'x') b";
                for (String from : new String[]{
                        " FROM p LEFT JOIN " + filteredB + " ON p.id = b.id",
                        " FROM " + filteredB + " RIGHT JOIN p ON p.id = b.id"
                }) {
                    assertQuery(select + from + " ORDER BY k")
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanContaining("Async Hash Join Group By", "buildFilter: vc ~ x")
                            .expectSize()
                            .returns(regexRows);
                }
                // p holds as many rows as b now, so the fused plan runs the ON condition on no more
                // rows than the probe holds, and the query takes it.
                execute("INSERT INTO p VALUES (7, 3), (8, 3)");
                for (String from : froms) {
                    assertQuery(select + from + "b.vc ~ 'x' ORDER BY k")
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanContaining("Async Hash Join Group By", "buildOnFilter: vc ~ x")
                            .expectSize()
                            .returns("""
                                    k\tc\tm\ts
                                    1\t2\t2\t40.0
                                    2\t2\t0\tnull
                                    3\t2\t0\tnull
                                    """);
                }
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
    public void testCountOverRepeatedBuildKeysAddsKeyRowCounts() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // cb is the smaller table, so it builds: 175 rows over the seven keys 5 to 11 and 25
                // rows of a NULL key. Each of ca's 100 rows of a key from 5 to 9 joins the key's 25
                // build rows, and each of its 100 NULL rows joins the 25 NULL build rows, as NULL
                // keys match in the ordinary hash join.
                assertQuery("SELECT count() FROM ca JOIN cb ON ca.k = cb.k")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan("""
                                Async Hash Join Group By workers: 4
                                  logicalJoinType: inner
                                  physicalJoinType: inner
                                  inputSwapped: false
                                  condition: ca.k=cb.k
                                  buildStrategy: shared
                                  aggregation: scalar
                                  countStrategy: per key
                                  values: [count(*)]
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: ca
                                    Build
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cb
                                """)
                        .returns("""
                                count
                                15000
                                """);
                // Both inputs' filters run in the operator: the probe's over each probe frame, ahead
                // of the count, and the build's over the build's frames, ahead of the key table.
                context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                assertQuery("SELECT count() FROM ca JOIN cb ON ca.k = cb.k WHERE ca.v > 500 AND cb.w > 100")
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan("""
                                Async Hash Join Group By workers: 4
                                  logicalJoinType: inner
                                  physicalJoinType: inner
                                  inputSwapped: false
                                  condition: ca.k=cb.k
                                  buildStrategy: shared
                                  aggregation: scalar
                                  countStrategy: per key
                                  values: [count(*)]
                                  probeFilter: 500<v
                                  buildFilter: 100<w
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: ca
                                    Build
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cb
                                """)
                        .returns("""
                                count
                                3700
                                """);
                context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    // An INT key and a lone SYMBOL pair take the INT layout; a LONG key, a VARCHAR
                    // key and both composite keys stage into the build's map.
                    for (String on : new String[]{
                            "ca.k = cb.k",
                            "ca.s = cb.s",
                            "ca.l = cb.l",
                            "ca.vc = cb.vc",
                            "ca.k = cb.k AND ca.vc = cb.vc",
                            "ca.s = cb.s AND ca.l = cb.l"
                    }) {
                        for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                            context.setJitMode(jit);
                            // Every spelling of the lone count, in both join orders.
                            assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on, "count", 15_000, true, buildMode, context);
                            assertCountPerKey("SELECT count(*) FROM ca JOIN cb ON " + on, "count", 15_000, true, buildMode, context);
                            assertCountPerKey("SELECT count(1) FROM ca JOIN cb ON " + on, "count", 15_000, true, buildMode, context);
                            assertCountPerKey("SELECT count() c FROM cb JOIN ca ON " + on, "c", 15_000, true, buildMode, context);
                            // A filter on either input runs before the join: on the probe's frames,
                            // and on the build's rows before they reach the key table.
                            assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE ca.v > 500", "count", 7_500, true, buildMode, context);
                            assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE cb.w > 100", "count", 7_400, true, buildMode, context);
                            assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE ca.v > 500 AND cb.w > 100", "count", 3_700, true, buildMode, context);
                            // No probe row passes, and then no build row: an empty build repeats no key.
                            assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE ca.v > 1_000", "count", 0, true, buildMode, context);
                            assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE cb.w > 200", "count", 0, false, buildMode, context);
                        }
                        context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                    }
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysCancelsInTheKeyRowCountAndReuses() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            final AtomicBoolean isArmed = new AtomicBoolean();
            final AtomicInteger countChecks = new AtomicInteger();
            // The checks of the count that ran in a task of the frame sequence, rather than in the
            // owner's own call: in the round that counts a build of more than one partition.
            final AtomicInteger roundCountChecks = new AtomicInteger();
            final StackWalker stackWalker = StackWalker.getInstance();
            // Cancels the query on the first check inside the pass that counts the build's key rows.
            final CountingSqlExecutionCircuitBreaker breaker = new CountingSqlExecutionCircuitBreaker(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER) {
                @Override
                public void statefulThrowExceptionIfTrippedTimeThrottled() {
                    super.statefulThrowExceptionIfTrippedTimeThrottled();
                    if (stackWalker.walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("countChains")))) {
                        countChecks.incrementAndGet();
                        if (stackWalker.walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals("reduce")
                                && frame.getClassName().equals("io.questdb.cairo.sql.async.UnorderedPageFrameReduceJob")))) {
                            roundCountChecks.incrementAndGet();
                        }
                        if (isArmed.get()) {
                            throw CairoException.queryCancelled(1);
                        }
                    }
                }
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, breaker);
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    final boolean isParallel = buildMode == HashJoinBuildMode.PARALLEL;
                    for (String on : new String[]{"ca.k = cb.k", "ca.s = cb.s", "ca.l = cb.l", "ca.k = cb.k AND ca.vc = cb.vc"}) {
                        // cb's day partitions of 100 rows are its frames. A parallel build copies a
                        // VARCHAR key's rows into a region per partition, which the workers count; an
                        // INT, SYMBOL or LONG key's rows run through every frame's region, in runs too
                        // short for the count's tasks, so the owner counts them.
                        final boolean isCountedInRounds = isParallel && on.contains("vc");
                        try (RecordCursorFactory factory = engine.select("SELECT count() FROM ca JOIN cb ON " + on, context)) {
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            isArmed.set(true);
                            countChecks.set(0);
                            roundCountChecks.set(0);
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                // Opening the cursor neither builds nor counts; the first read does.
                                Assert.assertEquals(on, 0, countChecks.get());
                                Assert.assertNull(on, atom.getFrozenBuild());
                                cursor.hasNext();
                                Assert.fail("expected cancellation in the key row count");
                            } catch (CairoException e) {
                                Assert.assertTrue(e.isCancellation());
                            }
                            // The first check cancels the count: the owner's pass, or the first task of
                            // the round, after which the cancelled round runs no other task.
                            Assert.assertEquals(on, 1, countChecks.get());
                            Assert.assertEquals(on, isCountedInRounds ? 1 : 0, roundCountChecks.get());
                            // The cancelled execution leaves no build, no counts and no acquired slot behind.
                            Assert.assertNull(on, atom.getFrozenBuild());
                            Assert.assertFalse(on, atom.isCountedByKey());
                            Assert.assertEquals(on, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertNull(on, context.getMemoryTracker());
                            isArmed.set(false);
                            countChecks.set(0);
                            roundCountChecks.set(0);
                            // The same factory builds and counts again.
                            assertCountExecution(factory, 15_000, true, context);
                            if (isCountedInRounds) {
                                // Each task checks once, as it starts its partition's rows, and a task
                                // of a partition without rows does not: the build's eight keys leave
                                // at most eight of its partitions holding rows.
                                Assert.assertTrue(on + ", checks: " + countChecks.get(), countChecks.get() > 1 && countChecks.get() <= 8);
                                Assert.assertEquals(on, countChecks.get(), roundCountChecks.get());
                            } else {
                                // The owner's pass checks once, as it starts.
                                Assert.assertEquals(on, 1, countChecks.get());
                                Assert.assertEquals(on, 0, roundCountChecks.get());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysCountsInRoundsOverLongEnoughFrameRuns() throws Exception {
        assertMemoryLeak(() -> {
            // gb, the smaller table and so the build, holds 40_920 rows of 4_096 keys, nine or ten of
            // each; ga probes with 81_920 rows of the same keys, twenty of each: 818_400 joined pairs.
            execute("CREATE TABLE ga (k INT, l LONG, vc VARCHAR, v INT)");
            execute("CREATE TABLE gb (k INT, l LONG, vc VARCHAR, w INT)");
            execute("INSERT INTO ga SELECT (x % 4_096)::INT, x % 4_096, 'v' || (x % 4_096), x::INT FROM long_sequence(81_920)");
            execute("INSERT INTO gb SELECT (x % 4_096)::INT, x % 4_096, 'v' || (x % 4_096), x::INT FROM long_sequence(40_920)");
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Every build runs in rounds, with 256 partitions.
                HashJoinBuildMode.PARALLEL.applyProperties(node1.getConfigurationOverrides());
                // gb in 39 frames of 1_049 rows on average, and then in 40 frames of 1_023 rows.
                for (int frameRows : new int[]{1_024, 1_023}) {
                    context.changePageFrameSizes(frameRows, frameRows);
                    for (String on : new String[]{"ga.k = gb.k", "ga.l = gb.l", "ga.vc = gb.vc"}) {
                        for (String where : new String[]{"", " WHERE gb.w > 0"}) {
                            final String sql = "SELECT count() FROM ga JOIN gb ON " + on + where;
                            // Without a build filter, an INT or a LONG key's rows run through every
                            // frame's region of the heap; a build filter or a VARCHAR key copies each
                            // partition's rows into a region of its own.
                            final boolean hasFrameRegions = where.isEmpty() && !on.contains("vc");
                            // A task of the count walks a partition's own region in one run, so every
                            // partition takes a task. Over frame regions, a task walks a run per frame:
                            // frames of 1_049 rows leave four tasks runs of 262 rows on average, which
                            // the count takes, and frames of 1_023 rows leave them runs under the 256
                            // it needs, so the owner counts instead.
                            final boolean isCountedInRounds = !hasFrameRegions || frameRows == 1_024;
                            assertQuery(sql)
                                    .withContext(context)
                                    .noLeakCheck()
                                    .noRandomAccess()
                                    .expectSize()
                                    .withPlanContaining("countStrategy: per key")
                                    .returns("count\n818400\n");
                            try (RecordCursorFactory factory = engine.select(sql, context);
                                 RecordCursor cursor = factory.getCursor(context)) {
                                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                Assert.assertTrue(sql, cursor.hasNext());
                                Assert.assertEquals(sql, 818_400, cursor.getRecord().getLong(0));
                                Assert.assertEquals(sql, frameRows == 1_024 ? 39 : 40, atom.getBuildFrameCount());
                                Assert.assertTrue(sql, atom.isBuiltInRounds());
                                Assert.assertEquals(sql, 256, atom.getBuildPartitionCount());
                                Assert.assertTrue(sql, atom.isCountedByKey());
                                Assert.assertEquals(sql, hasFrameRegions, atom.hasBuildFrameRegions());
                                Assert.assertEquals(sql, isCountedInRounds, atom.isCountedInRounds());
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysCountsPartitionsThatSpanFrames() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Frames of two or three rows leave the build in rounds a few partitions, each of
                // whose rows run through dozens of frames: through every frame's region of the heap
                // for an unfiltered INT, SYMBOL or LONG key, and copied frame by frame into the
                // partition's own region for a build filter, a VARCHAR key or a composite one. The
                // workers count the key rows of a partition's own region, over its every frame. Runs
                // of two or three rows per frame are too short for the count's tasks, so the owner
                // counts the rows that run through the frames' regions.
                HashJoinBuildMode.PARALLEL.apply(node1.getConfigurationOverrides(), context);
                for (String on : new String[]{
                        "ca.k = cb.k",
                        "ca.s = cb.s",
                        "ca.l = cb.l",
                        "ca.vc = cb.vc",
                        "ca.k = cb.k AND ca.vc = cb.vc",
                        "ca.s = cb.s AND ca.l = cb.l"
                }) {
                    assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on, "count", 15_000, true, HashJoinBuildMode.PARALLEL, context);
                    assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE ca.v > 500", "count", 7_500, true, HashJoinBuildMode.PARALLEL, context);
                    assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE cb.w > 100", "count", 7_400, true, HashJoinBuildMode.PARALLEL, context);
                    assertCountPerKey("SELECT count() FROM ca JOIN cb ON " + on + " WHERE ca.v > 500 AND cb.w > 100", "count", 3_700, true,
                            HashJoinBuildMode.PARALLEL, context);
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysExceedsIntRange() throws Exception {
        assertMemoryLeak(() -> {
            // One key, 50_000 rows of it in either table: 2_500_000_000 joined pairs, past the INT range.
            execute("CREATE TABLE wa (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE wb (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO wa SELECT 7, x, timestamp_sequence('2020-01-01', 1_000_000L) FROM long_sequence(50_000)");
            execute("INSERT INTO wb SELECT 7, x, timestamp_sequence('2020-01-01', 1_000_000L) FROM long_sequence(50_000)");
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    // One probe frame, whose own sum passes the INT range, and then five frames of
                    // 500_000_000 pairs each, whose sums pass it only once a slot adds them up.
                    for (boolean isOneFrame : new boolean[]{true, false}) {
                        if (isOneFrame) {
                            context.restoreToDefaultPageFrameSizes();
                        } else {
                            context.changePageFrameSizes(10_000, 10_000);
                        }
                        assertCountPerKey("SELECT count() FROM wa JOIN wb ON wa.k = wb.k", "count", 2_500_000_000L, true, buildMode, context);
                        // The probe filter keeps 45_000 rows of every 50_000.
                        assertCountPerKey("SELECT count() FROM wa JOIN wb ON wa.k = wb.k WHERE wa.v <= 45_000",
                                "count", 2_250_000_000L, true, buildMode, context);
                    }
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysIsZeroOverAnEmptyProbeInterval() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A SYMBOL key compiles no build choice, so cb, the smaller table, builds its 25 rows of
                // each key, while the probe scans the interval of ca, which selects none of its partitions.
                final String sql = "SELECT count() FROM ca JOIN cb ON ca.s = cb.s WHERE ca.ts IN '2021'";
                assertQuery(sql)
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .withPlan("""
                                Async Hash Join Group By workers: 4
                                  logicalJoinType: inner
                                  physicalJoinType: inner
                                  inputSwapped: false
                                  condition: ca.s=cb.s
                                  symbolKeyJoin: true
                                  buildStrategy: shared
                                  aggregation: scalar
                                  countStrategy: per key
                                  values: [count(*)]
                                    Probe
                                        PageFrame
                                            Row forward scan
                                            Interval forward scan on: ca
                                              intervals: [("2021-01-01T00:00:00.000000Z","2021-12-31T23:59:59.999999Z")]
                                    Build
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: cb
                                """)
                        .returns("""
                                count
                                0
                                """);
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    // The build counts the rows of each key, and no probe frame adds any of them.
                    assertCountPerKey(sql, "count", 0, true, buildMode, context);
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysReusesAcrossBuildShapes() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    for (String on : new String[]{"ca.k = cb.k", "ca.s = cb.s", "ca.l = cb.l", "ca.k = cb.k AND ca.vc = cb.vc"}) {
                        execute("TRUNCATE TABLE cb");
                        insertCountBuildRows();
                        // One factory, whose executions find a build that repeats its keys, an empty
                        // build, a build of one row per key, and a build that repeats one key again.
                        try (RecordCursorFactory factory = engine.select("SELECT count() FROM ca JOIN cb ON " + on, context)) {
                            assertCountExecution(factory, 15_000, true, context);
                            assertCountExecution(factory, 15_000, true, context);
                            execute("TRUNCATE TABLE cb");
                            assertCountExecution(factory, 0, false, context);
                            // Keys 5 to 9 and a NULL key, one row each: every probe row of them joins once.
                            execute("""
                                    INSERT INTO cb VALUES
                                        (5, 5, 'S5', 'V5', 1, '2020-01-01'), (6, 6, 'S6', 'V6', 2, '2020-01-01'),
                                        (7, 7, 'S7', 'V7', 3, '2020-01-01'), (8, 8, 'S8', 'V8', 4, '2020-01-01'),
                                        (9, 9, 'S9', 'V9', 5, '2020-01-01'), (NULL, NULL, NULL, NULL, 6, '2020-01-01')
                                    """);
                            assertCountExecution(factory, 600, false, context);
                            // A second row of key 5 and of the NULL key.
                            execute("INSERT INTO cb VALUES (5, 5, 'S5', 'V5', 7, '2020-01-02'), (NULL, NULL, NULL, NULL, 8, '2020-01-02')");
                            assertCountExecution(factory, 800, true, context);
                            // The probe goes empty under a build that still repeats its keys. The build
                            // table then holds more than twice the rows of the probe table, so the factory
                            // asks for a recompile rather than build the larger table.
                            execute("TRUNCATE TABLE ca");
                            try {
                                assertCountExecution(factory, 0, true, context);
                                Assert.fail(on + ": a factory that would build the larger table must ask for a recompile");
                            } catch (TableReferenceOutOfDateException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "[table=cb]");
                                Assert.assertNull(context.getMemoryTracker());
                                Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            }
                        }
                        // A new compile builds the empty probe table, whose lone count adds nothing.
                        try (RecordCursorFactory factory = engine.select("SELECT count() FROM ca JOIN cb ON " + on, context)) {
                            assertCountExecution(factory, 0, false, context);
                        }
                        insertCountProbeRows();
                    }
                }
            }
        });
    }

    @Test
    public void testCountOverRepeatedBuildKeysVisitsNoPairs() throws Exception {
        final AtomicInteger pairBudgetChecks = new AtomicInteger();
        final StackWalker stackWalker = StackWalker.getInstance();
        circuitBreakerConfiguration = new DefaultSqlExecutionCircuitBreakerConfiguration() {
            @Override
            public MillisecondClock getClock() {
                return () -> {
                    // A reducer reads the clock only where a loop over joined pairs has spent its
                    // pair budget, so these reads count the budgets of pairs the reducers visited.
                    if (stackWalker.walk(frames -> frames.anyMatch(frame ->
                            frame.getMethodName().equals("isInterrupted")
                                    && frame.getClassName().equals(AsyncHashJoinGroupByRecordCursorFactory.class.getName())))) {
                        pairBudgetChecks.incrementAndGet();
                    }
                    return 1_000;
                };
            }
        };
        assertMemoryLeak(() -> {
            createCountTables();
            try (
                    SqlExecutionContextImpl context = enabledContext();
                    NetworkSqlExecutionCircuitBreaker breaker = new NetworkSqlExecutionCircuitBreaker(engine, circuitBreakerConfiguration)
            ) {
                context.with(AllowAllSecurityContext.INSTANCE, null, null, -1, breaker);
                breaker.setTimeout(Long.MAX_VALUE);
                // The pair budget is one page frame of joined pairs: 64 of the 15_000 this join has.
                context.changePageFrameSizes(64, 64);
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    for (String on : new String[]{"ca.k = cb.k", "ca.s = cb.s", "ca.l = cb.l", "ca.k = cb.k AND ca.vc = cb.vc"}) {
                        for (String where : new String[]{"", " WHERE ca.v > 0"}) {
                            final String from = " FROM ca JOIN cb ON " + on + where;
                            // A second aggregate reads every joined pair, so its reducers spend pair
                            // budgets: the clock does observe them.
                            Assert.assertEquals("n\tv\n15000:LONG\t7537500.0:DOUBLE\n",
                                    resultOf("SELECT count() n, sum(ca.v) v" + from, context, breaker, pairBudgetChecks));
                            Assert.assertTrue(from + ", checks: " + pairBudgetChecks.get(), pairBudgetChecks.get() > 0);
                            // The lone count adds each key's row count once per probe row instead.
                            Assert.assertEquals("count\n15000:LONG\n",
                                    resultOf("SELECT count()" + from, context, breaker, pairBudgetChecks));
                            Assert.assertEquals(from, 0, pairBudgetChecks.get());
                        }
                    }
                }
            } finally {
                circuitBreakerConfiguration = null;
            }
        });
    }

    @Test
    public void testCountOverUniqueBuildKeysAddsOnePerMatch() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            // cu holds one row per key over two days: keys 5 to 9, a NULL key, and keys 12 and 13,
            // which no probe row holds. Each of ca's 100 rows of a key from 5 to 9 joins one build
            // row, and so does each of its 100 NULL rows, as NULL keys match in the ordinary hash
            // join. ca's keys 1 to 4 are missing from the build, and their SYMBOL texts from its
            // dictionary.
            execute("CREATE TABLE cu (k INT, l LONG, s SYMBOL, vc VARCHAR, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO cu VALUES
                        (5, 5, 'S5', 'V5', 1, '2020-01-01'), (6, 6, 'S6', 'V6', 2, '2020-01-01'),
                        (7, 7, 'S7', 'V7', 3, '2020-01-01'), (8, 8, 'S8', 'V8', 4, '2020-01-01'),
                        (9, 9, 'S9', 'V9', 5, '2020-01-02'), (NULL, NULL, NULL, NULL, 6, '2020-01-02'),
                        (12, 12, 'S12', 'V12', 7, '2020-01-02'), (13, 13, 'S13', 'V13', 8, '2020-01-02')
                    """);
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    // An INT key and a lone SYMBOL pair take the INT layout; a LONG key, a VARCHAR
                    // key and both composite keys stage into the build's map.
                    for (String on : new String[]{
                            "ca.k = cu.k",
                            "ca.s = cu.s",
                            "ca.l = cu.l",
                            "ca.vc = cu.vc",
                            "ca.k = cu.k AND ca.vc = cu.vc",
                            "ca.s = cu.s AND ca.l = cu.l"
                    }) {
                        for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                            context.setJitMode(jit);
                            // Every spelling of the lone count, in both join orders.
                            assertCountPerKey("SELECT count() FROM ca JOIN cu ON " + on, "count", 600, false, buildMode, context);
                            assertCountPerKey("SELECT count(*) FROM ca JOIN cu ON " + on, "count", 600, false, buildMode, context);
                            assertCountPerKey("SELECT count(1) FROM ca JOIN cu ON " + on, "count", 600, false, buildMode, context);
                            assertCountPerKey("SELECT count() c FROM cu JOIN ca ON " + on, "c", 600, false, buildMode, context);
                            // The probe filter runs as JIT code under the JIT mode, and interpreted otherwise.
                            final String probeFiltered = "SELECT count() FROM ca JOIN cu ON " + on + " WHERE ca.v > 500";
                            try (RecordCursorFactory factory = engine.select(probeFiltered, context)) {
                                Assert.assertEquals(probeFiltered, JitUtil.isJitSupported() && jit == SqlJitMode.JIT_MODE_ENABLED,
                                        fused(factory).usesCompiledFilter());
                            }
                            assertCountPerKey(probeFiltered, "count", 300, false, buildMode, context);
                            // The build filter keeps the keys 8, 9, NULL, 12 and 13.
                            assertCountPerKey("SELECT count() FROM ca JOIN cu ON " + on + " WHERE cu.w > 3", "count", 300, false, buildMode, context);
                            assertCountPerKey("SELECT count() FROM ca JOIN cu ON " + on + " WHERE ca.v > 500 AND cu.w > 3", "count", 150, false, buildMode, context);
                            // No probe row passes, and then no build row.
                            assertCountPerKey("SELECT count() FROM ca JOIN cu ON " + on + " WHERE ca.v > 1_000", "count", 0, false, buildMode, context);
                            assertCountPerKey("SELECT count() FROM ca JOIN cu ON " + on + " WHERE cu.w > 200", "count", 0, false, buildMode, context);
                        }
                        context.setJitMode(SqlJitMode.JIT_MODE_ENABLED);
                    }
                }
            }
        });
    }

    @Test
    public void testCountStrategyCoversTheLoneInnerCountOnly() throws Exception {
        assertMemoryLeak(() -> {
            createCountTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (HashJoinBuildMode buildMode : HashJoinBuildMode.values()) {
                    buildMode.applyProperties(node1.getConfigurationOverrides());
                    // Each of these reads something of a joined pair, or counts a probe row without a
                    // match, so its reducers keep visiting the pairs of a build that repeats its keys.
                    for (String sql : new String[]{
                            "SELECT count() n, sum(ca.v) v FROM ca JOIN cb ON ca.k = cb.k",
                            "SELECT count(cb.w) FROM ca JOIN cb ON ca.k = cb.k",
                            "SELECT count(ca.v) FROM ca JOIN cb ON ca.k = cb.k",
                            "SELECT ca.k, count() FROM ca JOIN cb ON ca.k = cb.k ORDER BY ca.k",
                            "SELECT count() FROM ca LEFT JOIN cb ON ca.k = cb.k",
                            // A post-join filter reads the build's columns of every pair.
                            "SELECT count() FROM ca LEFT JOIN cb ON ca.k = cb.k WHERE cb.w > 100",
                            "SELECT count() FROM cb RIGHT JOIN ca ON ca.k = cb.k",
                            "SELECT count() FROM ca LEFT JOIN cb ON ca.k = cb.k AND cb.w > 100"
                    }) {
                        assertDifferential(sql, context, true);
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            final String plan = plan(factory, context);
                            Assert.assertFalse(sql + "\n" + plan, plan.contains("countStrategy"));
                            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                // The first read builds.
                                Assert.assertTrue(cursor.hasNext());
                                Assert.assertFalse(sql, atom.isCountedByKey());
                                Assert.assertTrue(sql, atom.getFrozenBuild().getRowCount() > atom.getFrozenBuild().getKeyCount());
                            }
                        }
                    }
                    // The optimiser folds a repeated count into one aggregate, which two output
                    // columns then read, and an expression over the count reads that aggregate too.
                    assertQuery("SELECT count() n, count() m, count() + 1 o FROM ca JOIN cb ON ca.k = cb.k")
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .withPlanContaining("countStrategy: per key", "values: [count(*)]")
                            .returns("""
                                    n\tm\to
                                    15000\t15000\t15001
                                    """);
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
                        // An open cursor has no build; the first read makes it.
                        Assert.assertNull(fused.getAtom().getFrozenBuild());
                        Assert.assertTrue(cursor.hasNext());
                        FrozenHashJoinBuild build = fused.getAtom().getFrozenBuild();
                        Assert.assertEquals(5, build.getRowCount());
                        Assert.assertEquals(3, build.getKeyCount());
                        Assert.assertTrue(build.getSizeInBytes() > 0);
                    }
                    Assert.assertEquals(expected, result(factory, context));
                    execute("truncate table p");
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(0, fused.getAtom().getFrozenBuild().getRowCount());
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
                                      buildPayload: copied when the probe is large enough
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
                                      buildPayload: copied when the probe is large enough
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
            Assert.assertEquals(256 * Numbers.SIZE_1MB, new DefaultCairoConfiguration(root).getSqlParallelHashJoinGroupByRightJoinMaxBuildSize());
            Assert.assertEquals(256 * Numbers.SIZE_1MB, configuration.getSqlParallelHashJoinGroupByRightJoinMaxBuildSize());
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
                // A factory compiled under the bound asks the caches to compile the query again once
                // the table outgrows it, rather than build the larger table.
                try (RecordCursorFactory factory = engine.select(narrow + joins[0] + on, context)) {
                    Assert.assertTrue(plan(factory, context).contains("Async Hash Join Group By"));
                    Assert.assertEquals(5, builtRows(factory, context));
                    execute("INSERT INTO r VALUES (1, '2021-02-01', 60, 600)");
                    try {
                        builtRows(factory, context);
                        Assert.fail("a factory whose RIGHT join build outgrew the bound must ask for a recompile");
                    } catch (TableReferenceOutOfDateException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "[table=r]");
                        Assert.assertNull(context.getMemoryTracker());
                        Assert.assertEquals(0, fused(factory).getAtom().getPerWorkerLocks().getAcquiredSlotCount());
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
    public void testBuildRunsOnWorkersFromDefaultMinRows() throws Exception {
        assertMemoryLeak(() -> {
            // pa holds one row short of the default bound of 300_000 rows, one per key; pb holds
            // 400_000 rows, one or two for each key of pa, so that the INNER join builds pa.
            execute("CREATE TABLE pa (k INT, v DOUBLE)");
            execute("CREATE TABLE pb (k INT, w DOUBLE)");
            execute("INSERT INTO pa SELECT x::INT, x FROM long_sequence(299_999)");
            execute("INSERT INTO pb SELECT (x % 300_000 + 1)::INT, x FROM long_sequence(400_000)");
            final String sql = "SELECT count(*) n, sum(pa.v) v, sum(pb.w) w FROM pa JOIN pb ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Below the default bound, the owner builds on its own thread.
                assertBuildRounds(sql, 299_999, 0, context);
                // From the bound on, the build runs in rounds, over 16 partitions: the power of two
                // at or above 300_000 rows / 32_768 rows per partition.
                execute("INSERT INTO pa VALUES (300_000, 300_000)");
                assertBuildRounds(sql, 300_000, 16, context);
            }
        });
    }

    @Test
    public void testBuildRunsOnWorkersFromMinRows() throws Exception {
        assertMemoryLeak(() -> {
            // pa spreads 1_000 rows over four days, 250 a day, so that its build has four frames;
            // pb has 2_000 rows. Both hold the same ten keys in every key column.
            execute("CREATE TABLE pa (k INT, l LONG, s SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, l LONG, s SYMBOL, v DOUBLE)");
            execute("""
                    INSERT INTO pa SELECT (x % 10)::INT, x % 10, 'S' || (x % 10), x,
                        timestamp_sequence('2020-01-01', 345_600_000L)
                    FROM long_sequence(1_000)
                    """);
            execute("INSERT INTO pb SELECT (x % 10)::INT, x % 10, 'S' || (x % 10), x FROM long_sequence(2_000)");
            // The INNER join builds pa, the smaller table; the LEFT joins build pa, the table after them.
            final String inner = "SELECT count(*) n, sum(pa.v) v FROM pa JOIN pb ON pa.k = pb.k";
            final String left = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 100);
                // The build input's 1_000 frame rows reach the bound exactly: the build runs in rounds,
                // over 16 partitions, the power of two at or above 1_000 / 100.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000);
                assertBuildRounds(inner, 1_000, 16, context);
                // One row past the frames keeps the build on the owner.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_001);
                assertBuildRounds(inner, 1_000, 0, context);

                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    // A filtered build counts the rows it keeps before it sizes anything: 900 rows
                    // take 16 partitions, 500 take 8 and 300 take 4. The frames run the scan's filter,
                    // a copy of it per worker when it is not thread safe, as the regex is.
                    assertBuildRounds(inner + " WHERE pa.v > 100", 900, 16, context);
                    assertBuildRounds(inner + " WHERE pa.s::STRING ~ 'S[1-3]'", 300, 4, context);
                    // So do an outer join's ON conditions on the build's columns alone.
                    assertBuildRounds(left + " AND pa.v <= 500", 500, 8, context);
                    assertBuildRounds(left + " AND pa.s::STRING ~ 'S[1-3]'", 300, 4, context);
                    // A WHERE clause on a LEFT join's build side filters the joined rows, not the build's.
                    assertBuildRounds(left + " AND pa.s::STRING ~ 'S[1-3]' WHERE pa.v > 100 OR pa.v IS NULL", 300, 4, context);
                }
                // The lone SYMBOL pair builds its build's own symbol keys in rounds too.
                assertBuildRounds("SELECT count(*) n, sum(pa.v) v FROM pa JOIN pb ON pa.s = pb.s", 1_000, 16, context);
                // So does a staged key, into a map per partition: a LONG key's Unordered8Map, and the
                // OrderedMap of a composite key, one with a SYMBOL pair among them, which stages the
                // build's own symbol key. A filter narrows its partitions as it narrows an INT key's.
                final String staged = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.l = pb.l";
                assertBuildRounds(staged, 1_000, 16, context);
                assertBuildRounds(staged + " AND pa.s::STRING ~ 'S[1-3]'", 300, 4, context);
                assertBuildRounds(staged + " AND pa.v <= 500", 500, 8, context);
                assertBuildRounds("SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k AND pa.l = pb.l", 1_000, 16, context);
                assertBuildRounds("SELECT count(*) n, sum(pa.v) v FROM pa JOIN pb ON pa.s = pb.s AND pa.l = pb.l", 1_000, 16, context);
                // A build of one partition's worth of rows runs in rounds into a single table or map.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1_000);
                assertBuildRounds(inner, 1_000, 1, context);
                assertBuildRounds(staged, 1_000, 1, context);

                // Each frame of a build in rounds copies the payload rows it kept.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 100);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
                assertPayloadCopy(inner, true, context);
                assertPayloadCopy(left + " AND pa.s::STRING ~ 'S[1-3]'", true, context);
                assertPayloadCopy(staged, true, context);
                assertPayloadCopy(staged + " AND pa.s::STRING ~ 'S[1-3]'", true, context);
                assertBuildRounds(inner, 1_000, 16, context);
                assertBuildRounds(staged, 1_000, 16, context);
            }
        });
    }

    @Test
    public void testParallelBuildModeReachesTheOperator() throws Exception {
        // The differential suites force the parallel build through HashJoinBuildMode. Its properties
        // must build even their small tables in rounds, over more than one frame and partition, on
        // the tests' own execution context.
        HashJoinBuildMode.PARALLEL.apply(node1.getConfigurationOverrides(), sqlExecutionContext);
        assertMemoryLeak(() -> {
            createTables();
            try (RecordCursorFactory factory = select(SCALAR_SELECT + " from r join p on r.plant_id=p.plant_id");
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                // The first read builds.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertTrue(atom.isBuiltInRounds());
                Assert.assertTrue("frames: " + atom.getBuildFrameCount(), atom.getBuildFrameCount() > 1);
                Assert.assertTrue("partitions: " + atom.getBuildPartitionCount(), atom.getBuildPartitionCount() > 1);
            } finally {
                sqlExecutionContext.restoreToDefaultPageFrameSizes();
            }
        });
    }

    @Test
    public void testPayloadCopyProbeReach() {
        // Each of a build's keys holds at least one of its K rows, so no key holds more than
        // K - keys + 1 of them, and a probe of R rows, each of which matches one key's rows, reaches
        // the F Parquet frames that a copy decodes once R x (K - keys + 1) >= F. A build without
        // Parquet frames is in reach of any probe, and one with Parquet frames out of reach of an
        // empty one. A build without rows has no keys.
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(0, 0, 0, 0));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(5, 0, 0, 0));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(0, 2_001, 2_001, 0));
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(0, 2_001, 2_001, 1));
        // Unique keys hold one row each.
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(19, 2_001, 2_001, 20));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(20, 2_001, 2_001, 20));
        // 1_999 keys over 2_001 rows: one key may hold 3 rows, although they hold 1.001 on average.
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(6, 2_001, 1_999, 20));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(7, 2_001, 1_999, 20));
        // A single probe row reaches 20 frames once one key may hold 20 rows.
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, 2_001, 1_982, 20));
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, 2_001, 1_983, 20));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, 2_001, 1_100, 20));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, 2_001, 1, 2_001));
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, 2_001, 1, 2_002));
        // Products past Long.MAX_VALUE compare exactly. R x (K - keys + 1) of 2^64 would wrap to zero,
        // of 2^63 and 2^64 - 2 to negative values, and of (2^63 - 1)^2 to one.
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1L << 32, 1L << 32, 1, Long.MAX_VALUE));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(2, 1L << 62, 1, Long.MAX_VALUE));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(Long.MAX_VALUE, Long.MAX_VALUE, 1, Long.MAX_VALUE));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, Long.MAX_VALUE, 1, Long.MAX_VALUE));
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(1, Long.MAX_VALUE, 2, Long.MAX_VALUE));
        Assert.assertTrue(AsyncHashJoinGroupByAtom.isParquetInProbeReach(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
        Assert.assertFalse(AsyncHashJoinGroupByAtom.isParquetInProbeReach(Long.MAX_VALUE - 1, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @Test
    public void testPayloadCopyRule() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(0.4, new DefaultCairoConfiguration(root).getSqlParallelHashJoinGroupByPayloadCopyMinProbeRatio(), 0);
            Assert.assertEquals(0.125, new DefaultCairoConfiguration(root).getSqlParallelHashJoinGroupByPayloadCopyParallelMinProbeRatio(), 0);
            Assert.assertEquals(300_000, new DefaultCairoConfiguration(root).getSqlParallelHashJoinGroupByBuildParallelMinRows());
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
            // The reversed LEFT join builds pb's 2_000 rows and probes pa's 1_000. With an interval it
            // probes the 250 rows of one of pa's days instead, or 249 of them, or the first 800 or 799
            // of pa's rows: the ratio counts the interval's rows.
            final String reversed = "SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k";
            final String interval = reversed + " WHERE pa.ts IN '2020-01-02'";
            final String interval249 = reversed + " WHERE pa.ts >= '2020-01-02' AND pa.ts < '2020-01-02T23:54'";
            final String interval800 = reversed + " WHERE pa.ts < '2020-01-04T04:48'";
            final String interval799 = reversed + " WHERE pa.ts < '2020-01-04T04:42'";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // These builds run on the owner, and its copy takes the ratio of 0.4 by default: a probe
                // of 800 rows copies pb's 2_000, and one of 799 keeps row ids, as the day's 250 do.
                assertPayloadCopy(narrow, true, context);
                assertPayloadCopy(wide, true, context);
                assertPayloadCopy(reversed, true, context);
                assertPayloadCopy(interval800, true, context);
                assertPayloadCopy(interval799, false, context);
                assertPayloadCopy(interval, false, context);
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
                    Assert.assertTrue(plan(factory, context).contains("buildPayload: copied when the probe is large enough"));
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
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0.125");
                assertPayloadCopy(interval, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0.126");
                assertPayloadCopy(interval, false, context);

                // A build in rounds copies on the workers, and takes the parallel ratio instead, 0.125
                // by default: the day's 250 probe rows copy pb's 2_000, and 249 rows keep row ids,
                // whatever the owner's ratio. The parallel ratio in turn leaves the owner's copy alone.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000");
                assertPayloadCopyInRounds(interval, true, context);
                assertPayloadCopyInRounds(interval249, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "0");
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0.126");
                assertPayloadCopyInRounds(interval, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(interval249, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0");
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "1000");
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
    public void testPayloadCopyRuleForParquetBuild() throws Exception {
        assertMemoryLeak(() -> {
            // pa spreads 1_000 rows over four days, 250 a day, and pb 2_000 rows over the same days.
            execute("CREATE TABLE pa (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO pa SELECT (x % 10)::INT, x, timestamp_sequence('2020-01-01', 345_600_000L) FROM long_sequence(1_000)");
            execute("INSERT INTO pb SELECT (x % 10)::INT, x, timestamp_sequence('2020-01-01', 172_800_000L) FROM long_sequence(2_000)");
            // The LEFT join builds pa's 1_000 rows and probes pb's 2_000, or none of them in an interval
            // without rows; the reversed one builds pb and probes pa. A copied DOUBLE takes 8 bytes a row.
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            // ON filters on the build's columns: the first keeps pa's first three days, 750 rows, and
            // the second its last day, 250 rows.
            final String firstDays = sql + " AND pa.v * 2 <= 1_500";
            final String lastDay = sql + " AND pa.v * 2 > 1_500";
            final String emptyProbe = " WHERE pb.ts IN '2019-01-01'";
            final String reversed = "SELECT count(*) n, sum(pb.v) v FROM pa LEFT JOIN pb ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // At a ratio of 8.001, a probe of 2_000 rows repays no copy of a native build of 1_000.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "8.001");
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "8.001");
                assertPayloadCopy(sql, false, context);
                // Once pa's first three days are Parquet, the ratios weigh the build's 250 native rows
                // alone: 2_000 probe rows repay their copy at a ratio of 8, and not at 8.001, on the
                // owner and on the workers alike.
                execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-04'");
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "8");
                assertPayloadCopy(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "8");
                assertPayloadCopyInRounds(sql, true, context);
                // The ratios weigh the native rows the build keeps: a build that keeps Parquet rows
                // alone copies for a probe whose matches land on as many row groups as the copy decodes,
                // as the 2_000 probe rows' do on pa's three, and one that keeps the native day's rows
                // alone weighs all of them.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "8.001");
                assertPayloadCopyInRounds(firstDays, true, context);
                assertPayloadCopyInRounds(lastDay, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "8.001");
                assertPayloadCopy(firstDays, true, context);
                assertPayloadCopy(lastDay, false, context);
                // A probe without rows lands on no row group, so the ratio weighs all the build's rows,
                // and even a build of Parquet rows alone keeps row ids.
                assertPayloadCopy(firstDays + emptyProbe, false, context);
                // The byte bound still holds.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MIN_PROBE_RATIO, "8");
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 7_999);
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 8_000);
                assertPayloadCopy(sql, true, context);
                // Only the build's frames count: the native build of a Parquet probe weighs all its rows.
                assertPayloadCopy(reversed, false, context);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    Assert.assertTrue(plan(factory, context).contains("buildPayload: copied when the probe is large enough"));
                }
                // Without a decode cache, the readers decode a row group again whenever a match moves
                // to another one, and the results still match the ordinary plan's.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 0);
                assertPayloadCopy(sql, false, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetBuildOutOfProbeReach() throws Exception {
        assertMemoryLeak(() -> {
            createParquetRowGroupBuild("k INT", "x::INT", 1);
            // pb's rows find one build row each, a different one, all of them Parquet rows.
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pb SELECT (x * 101)::INT, x FROM long_sequence(19)");
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertBuildFrameCount(sql, 21, context);
                // Without the copy, the 19 probe rows' matches decode at most 19 row groups, fewer than
                // the 20 that the copy would decode. The build then weighs all its 2_001 rows, as a
                // native build does, and the tiny probe keeps row ids, on the owner and on the workers.
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                // At a ratio of 0.0094, 19 probe rows repay the copy of 2_001 rows, and at 0.0095 they
                // do not, although they would repay the copy of the build's one native row.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0.0094");
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_PARALLEL_MIN_PROBE_RATIO, "0.0095");
                assertPayloadCopyInRounds(sql, false, context);
                // A 20th probe row reaches as many row groups as the copy decodes, so the ratio weighs
                // the build's one native row alone, and the build copies, on the workers and on the
                // owner alike.
                execute("INSERT INTO pb VALUES (2_000, 20)");
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, true, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetBuildWithRepeatedKeys() throws Exception {
        assertMemoryLeak(() -> {
            // Over its 2_001 rows, pa's k repeats 1_100 keys, 0 to 1_099, its f repeats 20 keys, and its
            // u is unique but for the key 1, which rows 1, 1_000 and 1_500 hold, in three row groups.
            createParquetRowGroupBuild(
                    "k INT, f INT, u INT",
                    "(x % 1_100)::INT, (x % 20)::INT, (CASE WHEN x IN (1_000, 1_500) THEN 1 ELSE x END)::INT",
                    1
            );
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pb SELECT x::INT, x FROM long_sequence(6)");
            execute("CREATE TABLE pc (k INT, v DOUBLE)");
            execute("INSERT INTO pc VALUES (7, 1)");
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.u = pb.k";
            final String fewKeys = "SELECT count(*) n, sum(pa.v) v FROM pc LEFT JOIN pa ON pa.f = pc.k";
            final String manyKeys = "SELECT count(*) n, sum(pa.v) v FROM pc LEFT JOIN pa ON pa.k = pc.k";
            final String manyKeysProbe = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertBuildFrameCount(sql, 21, context);
                // u holds 1_999 keys over 2_001 rows, so no key holds more than 2_001 - 1_998 = 3 rows,
                // and 6 probe rows match at most 18 build rows: they decode fewer row groups than the 20
                // that the copy decodes, and keep row ids. A 7th probe row may match 21, so the ratio
                // weighs the build's one native row alone, and the build copies, on the owner and on the
                // workers alike. The average, 1.001 rows a key, would ask for 20 probe rows.
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                execute("INSERT INTO pb VALUES (7, 7)");
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, true, context);
                // The rule cannot tell which keys a probe asks for, so it takes the most rows that one
                // key may hold. k's 1_100 keys may leave 902 rows to one of them, so a single probe row
                // reaches the 20 row groups, and copies, although each of k's keys holds 1.8 rows, as
                // the key 7 does. A key that the build repeats 100 times does the same.
                assertPayloadCopy(manyKeys, true, context);
                assertPayloadCopy(manyKeysProbe, true, context);
                assertPayloadCopy(fewKeys, true, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForMostlyNativeBuild() throws Exception {
        assertMemoryLeak(() -> {
            createMostlyNativeBuild();
            // The LEFT join builds pa's 10_101 rows and probes pb's 10, far fewer than the default
            // ratios ask to repay the copy of a native build: 0.4 of its rows for a copy on the owner,
            // and 0.125 for one on the workers.
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertPayloadCopy(sql, false, context);
                // pa's first day turns Parquet, 100 of the build's rows. A copy would copy the 10_001
                // native rows as well, and the ratios weigh those as before, so the tiny probe keeps
                // row ids, on the owner and on the workers alike.
                execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-02'");
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                // Once the second day is Parquet too, one native row is left, which the probe's 10 rows
                // repay, so the build copies, on the workers and on the owner alike.
                execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, true, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForFilteredParquetBuild() throws Exception {
        assertMemoryLeak(() -> {
            createMostlyNativeBuild();
            execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-02'");
            // ON filters on the build's columns: the first keeps the 10_001 rows of pa's native days and
            // none of its Parquet day, the second the Parquet day's 100 rows alone.
            final String nativeRows = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k AND pa.v * 2 > 1_000";
            final String parquetRows = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k AND pa.v * 2 < 1_000";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // The ratios weigh the native rows that the build keeps, whatever frames it reads: the
                // build that reads the Parquet day but keeps none of its rows weighs all of its rows,
                // as a native build does, and the build that keeps Parquet rows alone copies for the
                // probe's 10 rows, on the owner and on the workers alike.
                assertPayloadCopy(nativeRows, false, context);
                assertPayloadCopy(parquetRows, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(nativeRows, false, context);
                assertPayloadCopyInRounds(parquetRows, true, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetPartOverReaderShare() throws Exception {
        assertMemoryLeak(() -> {
            // pa holds 100 rows on each of its first four days, valued 1 to 400, 10_000 rows on its
            // fifth, valued 1_001 to 11_000, and one row on its sixth, the last partition, which stays
            // native. pb holds 10 rows. Both key their rows from 0 to 9.
            execute("CREATE TABLE pa (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, x, timestamp_sequence('2020-01-01', 864_000_000L) FROM long_sequence(400)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, 1_000 + x, timestamp_sequence('2020-01-05', 1_000_000) FROM long_sequence(10_000)");
            execute("INSERT INTO pa VALUES (0, 1_000_000, '2020-01-06')");
            execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(10)");
            execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-05'");
            // The LEFT join builds pa's 10_401 rows and probes pb's 10, far fewer than the default
            // ratios ask to repay the copy of its 10_001 native rows. The second keeps one row of each
            // Parquet day and every native row.
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            final String sparse = sql + " AND (pa.v IN (1, 101, 201, 301) OR pa.v > 1_000)";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A probe reader decodes the DOUBLE payload alone, 8 bytes a row, and not the INT key,
                // so the four Parquet days decode to 3_200 bytes, which a reader's share of the default
                // cache holds many times over: the tiny probe keeps row ids.
                assertPayloadCopy(sql, false, context);
                assertPayloadCopy(sparse, false, context);
                // Each of the five slots' readers takes a quarter of the cache, so at 12_800 bytes a
                // share holds the decoded Parquet days exactly, and the build keeps row ids. At 12_799
                // bytes it falls short, and every slot whose matches cycle through the days would decode
                // them again, so the build copies, whatever the ratios.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_800);
                assertPayloadCopy(sql, false, context);
                assertPayloadCopy(sparse, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_799);
                assertPayloadCopy(sql, true, context);
                // A reader decodes the payload of every row of a Parquet frame, so the sparse build, which
                // keeps four Parquet rows, one in each day, weighs the 400 rows of those days and copies as well.
                assertPayloadCopy(sparse, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, true, context);
                assertPayloadCopyInRounds(sparse, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_800);
                assertPayloadCopyInRounds(sql, false, context);
                assertPayloadCopyInRounds(sparse, false, context);
                // The byte bound still holds: a copied DOUBLE takes 8 bytes a row.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_799);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 10_401 * 8 - 1);
                assertPayloadCopyInRounds(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 10_401 * 8);
                assertPayloadCopyInRounds(sql, true, context);
                // Without a decode cache, any Parquet frame would decode again whenever a match moves
                // to another frame, so the build copies. A native build weighs all its rows whatever
                // the cache, as before: once the days are native again, the tiny probe keeps row ids.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 0);
                assertPayloadCopyInRounds(sql, true, context);
                execute("ALTER TABLE pa CONVERT PARTITION TO NATIVE WHERE ts < '2020-01-05'");
                assertPayloadCopyInRounds(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, false, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetPartOverReaderShareOutOfProbeReach() throws Exception {
        assertMemoryLeak(() -> {
            createParquetRowGroupBuild("k INT", "x::INT", 10_001);
            // pb's rows find one build row each, a different one, all of them Parquet rows.
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pb SELECT (x * 101)::INT, x FROM long_sequence(19)");
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A probe reader decodes the DOUBLE payload alone, 8 bytes a row, so the Parquet days
                // decode to 16_000 bytes. Each of the five slots' readers takes a quarter of the cache,
                // which at 63_999 bytes falls short of them.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 63_999);
                // Without the copy, the 19 probe rows' matches decode at most 19 row groups, however
                // often they evict each other, fewer than the 20 that the copy would decode. The build
                // then weighs all its 12_001 rows, as a native build does, and the tiny probe keeps row
                // ids, on the owner and on the workers.
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                // A 20th probe row may decode as many row groups as the copy, so the build copies,
                // however many native rows it holds, on the workers and on the owner alike.
                execute("INSERT INTO pb VALUES (2_000, 20)");
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, true, context);
                // A share that holds the Parquet days leaves the ratio to weigh the 10_001 native rows,
                // which the 20 probe rows do not repay.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 64_000);
                assertPayloadCopy(sql, false, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetPartOverReaderShareWithHotNullKey() throws Exception {
        assertMemoryLeak(() -> {
            // pa's keys are unique but for a NULL key on the last row of each of its 20 Parquet row
            // groups, rows 100, 200 and so on to 2_000.
            createParquetRowGroupBuild("k INT", "(CASE WHEN x <= 2_000 AND x % 100 = 0 THEN NULL ELSE x END)::INT", 10_001);
            // pb's one row has a NULL key, which matches the 20 NULL build rows, as NULL keys match in
            // the ordinary hash join.
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pb VALUES (NULL, 1)");
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A reader's share of the default cache holds the 16_000 bytes that the Parquet days
                // decode to, so the ratio weighs the 10_001 native rows, which one probe row does not repay.
                assertPayloadCopy(sql, false, context);
                // Each of the five slots' readers takes a quarter of the cache, which at 63_999 bytes falls
                // short of the Parquet days.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 63_999);
                // pa's 12_001 rows hold 11_982 keys, the NULL key among them, so no key holds more than
                // 12_001 - 11_981 = 20 rows, and the probe row may decode as many row groups as the copy
                // does. This one would: its NULL key walks all 20 row groups, which a share that falls
                // short of them would decode again for every such probe row. The build copies, on the
                // owner and on the workers alike, although its keys hold 1.002 rows on average.
                assertPayloadCopy(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, true, context);
                assertQuery(sql)
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                n\tv
                                20\t21000.0
                                """);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetPartOverReaderShareWithHotSentinelKey() throws Exception {
        assertMemoryLeak(() -> {
            // pa's keys are unique but for the key 0 on every 400th of its Parquet rows, rows 400, 800
            // and so on to 2_000, one in each of five of its 20 Parquet row groups.
            createParquetRowGroupBuild("k INT", "(CASE WHEN x <= 2_000 AND x % 400 = 0 THEN 0 ELSE x END)::INT", 10_001);
            // pb's rows all ask for the key 0.
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pb SELECT 0, x FROM long_sequence(3)");
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // Each of the five slots' readers takes a quarter of the cache, which at 63_999 bytes falls
                // short of the 16_000 bytes that the Parquet days decode to.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 63_999);
                // pa's 12_001 rows hold 11_997 keys, so no key holds more than 12_001 - 11_996 = 5 rows,
                // and 3 probe rows match at most 15 build rows: they decode fewer row groups than the 20
                // that the copy decodes, however often they evict each other. The build then weighs all
                // its 12_001 rows, as a native build does, and the tiny probe keeps row ids, on the owner
                // and on the workers.
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                // A 4th probe row may match 20 build rows, as these do, so the build copies, however many
                // native rows it holds, on the workers and on the owner alike, although its keys hold
                // 1.0003 rows on average.
                execute("INSERT INTO pb VALUES (0, 4)");
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, true, context);
                assertQuery(sql)
                        .withContext(context)
                        .noLeakCheck()
                        .noRandomAccess()
                        .expectSize()
                        .returns("""
                                n\tv
                                20\t24000.0
                                """);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetPartOverReaderShareWithMixedWidthPayload() throws Exception {
        assertMemoryLeak(() -> {
            // As testPayloadCopyRuleForParquetPartOverReaderShare, but the payload holds two columns of
            // different widths, the DOUBLE v and the INT w.
            execute("CREATE TABLE pa (k INT, v DOUBLE, w INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, x, x::INT, timestamp_sequence('2020-01-01', 864_000_000L) FROM long_sequence(400)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, 1_000 + x, x::INT, timestamp_sequence('2020-01-05', 1_000_000) FROM long_sequence(10_000)");
            execute("INSERT INTO pa VALUES (0, 1_000_000, 7, '2020-01-06')");
            execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(10)");
            execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-05'");
            final String sql = "SELECT count(*) n, sum(pa.v) v, sum(pa.w) w FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A probe reader decodes v and w, 12 bytes a row, so the four Parquet days decode to
                // 4_800 bytes. Each of the five slots' readers takes a quarter of the cache, so at 19_200
                // bytes a share holds them exactly, and the build keeps row ids. At 19_199 bytes it falls
                // short, and the build copies. A row size that counted one of the two columns alone, 8 or
                // 4 bytes, would keep row ids at 19_199 bytes, and one of 16 bytes would copy at 19_200.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 19_200);
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 19_199);
                assertPayloadCopy(sql, true, context);
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetPartWithFilterOnlyVarcharOverReaderShare() throws Exception {
        assertMemoryLeak(() -> {
            // As testPayloadCopyRuleForParquetPartOverReaderShare, but pa also holds msg, 300 bytes of
            // text a row that compress well, which only the build scan's WHERE filter reads.
            execute("CREATE TABLE pa (k INT, v DOUBLE, msg VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, x, rpad(x::VARCHAR, 300, ' log line'), timestamp_sequence('2020-01-01', 864_000_000L) FROM long_sequence(400)");
            execute("INSERT INTO pa SELECT (x % 10)::INT, 1_000 + x, rpad(x::VARCHAR, 300, ' log line'), timestamp_sequence('2020-01-05', 1_000_000) FROM long_sequence(10_000)");
            execute("INSERT INTO pa VALUES (0, 1_000_000, 'last', '2020-01-06')");
            execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(10)");
            execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-05'");
            // The LEFT join builds the 10_401 rows that the filter keeps, all of them, and probes pb's 10.
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN (SELECT k, v FROM pa WHERE msg NOT LIKE '%qqqq%') pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    TestUtils.assertContains(plan(factory, context), "buildFilter: not (msg like %qqqq%)");
                }
                // A probe reader decodes the DOUBLE payload alone, 8 bytes a row, and neither the INT key
                // nor msg, so the four Parquet days decode to 3_200 bytes. Each of the five slots' readers
                // takes a quarter of the cache, so at 12_800 bytes a share holds them exactly, and the build
                // keeps row ids. At 12_799 bytes it falls short, and the build copies, whatever the ratios.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_800);
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_799);
                assertPayloadCopy(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 12_800);
                assertPayloadCopyInRounds(sql, false, context);
            }
        });
    }

    @Test
    public void testParquetBuildReadersDecodeNoVarcharOfOnCondition() throws Exception {
        assertMemoryLeak(() -> {
            // pa's first four days, 1_000 rows each, turn into a Parquet row group each, and its last
            // day, one row, stays native. msg holds 300 bytes of text a row that compress well, which
            // only the ON condition on pa alone reads.
            execute("CREATE TABLE pa (k INT, v DOUBLE, msg VARCHAR, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k INT, v DOUBLE)");
            execute("INSERT INTO pa SELECT x::INT, x, rpad(x::VARCHAR, 300, ' log line'), timestamp_sequence('2020-01-01', 86_400_000L) FROM long_sequence(4_000)");
            execute("INSERT INTO pa VALUES (4_001, 4_001, 'last', '2020-01-05')");
            execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-05'");
            // pb holds as many rows as pa, so the LEFT join takes the fused plan with an ON condition
            // on a VARCHAR of pa alone.
            execute("INSERT INTO pb SELECT x::INT, x FROM long_sequence(4_001)");
            final String sql = "SELECT count(*) n, count(pa.k) m, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k AND pa.msg NOT LIKE '%7%'";
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A probe this large repays the copy.
                assertPayloadCopy(sql, true, context);
                // Without the copy, every slot that probes reads the Parquet days through its reader.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_PAYLOAD_COPY_MAX_SIZE, 0);
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                // pb's 4_001 rows make one probe frame, so one slot probes, and its reader decodes each
                // Parquet day once.
                context.changePageFrameSizes(8_192, 8_192);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    TestUtils.assertContains(plan(factory, context), "buildOnFilter: not (msg like %7%)");
                    final long decodedBefore = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertFalse(fused(factory).getAtom().isPayloadCopied());
                        // The open cursor still holds what the reader decoded: the payload of the four
                        // Parquet days, pa.k, which count(pa.k) reads, and pa.v, 12 bytes a row, and up
                        // to an eighth more for the decoder's bookkeeping. msg's 16-byte slice entries
                        // would add 64_000 bytes.
                        final long decoded = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER) - decodedBefore;
                        final long bound = 4_000 * (Integer.BYTES + Double.BYTES);
                        Assert.assertTrue("decoded " + decoded + ", bound " + bound, decoded >= bound && decoded <= bound + bound / 8);
                    }
                    Assert.assertEquals(decodedBefore, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER));
                }
            }
        });
    }

    @Test
    public void testPayloadCopyRuleForParquetStringKeyOverReaderShare() throws Exception {
        // A STRING key of 34 chars would decode to an 8-byte offset a row and 72 bytes of length and
        // UTF-16 chars, but a reader never decodes it.
        assertParquetVarSizeKeyCopy("STRING", "STRING", 34);
    }

    @Test
    public void testPayloadCopyRuleForParquetStringKeyStagedAsVarcharOverReaderShare() throws Exception {
        // A VARCHAR probe key makes the build's STRING key stage as UTF-8, which leaves what a reader
        // decodes unchanged: the payload alone.
        assertParquetVarSizeKeyCopy("STRING", "VARCHAR", 36);
    }

    @Test
    public void testPayloadCopyRuleForParquetVarcharKeyOverReaderShare() throws Exception {
        // A VARCHAR key of 36 chars would decode to a 16-byte slice entry a row and the page bytes
        // that the entry points into, but a reader never decodes it.
        assertParquetVarSizeKeyCopy("VARCHAR", "VARCHAR", 36);
    }

    @Test
    public void testPayloadCopyRuleFollowsPartitionConversion() throws Exception {
        assertMemoryLeak(() -> {
            createMostlyNativeBuild();
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            try (SqlExecutionContextImpl context = enabledContext()) {
                assertDifferential(sql, context, true);
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final String expected = result(factory, context);
                    // Each execution weighs the frames that it reads: one factory keeps row ids for
                    // the native build, copies once pa's first two days are Parquet and a single
                    // native row is left, and keeps row ids again once the days are native again.
                    assertCopied(factory, false, context);
                    execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-03'");
                    assertCopied(factory, true, context);
                    Assert.assertEquals(expected, result(factory, context));
                    execute("ALTER TABLE pa CONVERT PARTITION TO NATIVE WHERE ts < '2020-01-03'");
                    assertCopied(factory, false, context);
                    Assert.assertEquals(expected, result(factory, context));
                }
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
                            // Neither call asks for a row, so neither builds.
                            Assert.assertNull(fused.getAtom().getFrozenBuild());
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
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertEquals(0, fused.getAtom().getFrozenBuild().getRowCount());
                        }
                        Assert.assertFalse(fused.getAtom().isSharded());
                        execute("insert into p values (1,'ES',5), (1,'ES',7), (1,'IT',null), (2,null,null), (null,'ES',11)");
                        Assert.assertEquals(expected, result(factory, context));
                    }
                }
            }
        });
    }

    @Test
    public void testThrowingWhereConjunctMatchesOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k SYMBOL, v LONG)");
            execute("CREATE TABLE b (k SYMBOL, c0 SHORT, c1 SHORT)");
            execute("INSERT INTO a VALUES ('x', 1), ('y', 2)");
            execute("INSERT INTO b VALUES ('x', 7288, 1), ('y', 1, 1), ('z', 2, 1)");
            // DECIMAL(4, 2) holds at most 99.99, so the cast throws on the row where c0 is 7288.
            final String cast = "0.62::DECIMAL(2, 2) >= b.c0::DECIMAL(4, 2)";
            final String castFilter = "0.62>=c0::DECIMAL(4,2)";
            // Only FunctionParser folds these conjuncts, so the optimizer leaves them to the filter
            // after the join instead of emptying the query.
            final String constFalse = "0.366490 >= 29_062::SHORT";
            final String constTrue = "0.366490 <= 29_062::SHORT";
            // Each spelling evaluates b's conjuncts in b's scan, in written order, and stops at the
            // first false one; the fused plan's probe filter is that scan's filter. Each row holds a
            // WHERE clause, optionally followed by GROUP BY, and the fused plan's probe filter, or
            // null where the fused analysis must keep the ordinary plan.
            final String[][] wheres = {
                    {"b.c1 > 5 AND " + cast, "(5<c1 and " + castFilter + ")"},
                    {cast + " AND b.c1 > 5", "(" + castFilter + " and 5<c1)"},
                    // The ordinary plan folds a filter above the join with a constant-false conjunct
                    // to false and evaluates none of its other conjuncts, so the analysis keeps the
                    // ordinary plan.
                    {cast + " AND " + constFalse, null},
                    {constFalse + " AND " + cast, null},
                    {cast + " AND " + constFalse + " GROUP BY b.k", null},
                    {cast + " AND NULL::BOOLEAN", null},
                    {cast + " AND ((a.v > 0 AND " + constFalse + ") OR 1 = 2)", null},
                    {"b.c1 > 0 AND " + constFalse, null},
                    // The optimizer empties the query for a conjunct of literals alone.
                    {cast + " AND false", null},
                    {cast + " AND 1 = 2", null},
                    // A constant-true conjunct keeps the fused plan.
                    {cast, castFilter},
                    {cast + " AND " + constTrue, castFilter}
            };
            // A runtime constant keeps the fused plan too, since one factory serves every execution
            // and every bind value. The analysis runs before init(): now() reads 0 there, so
            // now() > '1970-01-02' is false, and $1, bound to 2, has no linked value yet. A rule that
            // evaluated runtime constants would pick the ordinary plan or fail. a.v > 0 keeps a
            // conjunct that reads a column in the filter after an outer join, so that filter is not
            // a runtime constant alone, which the ordinary plan turns into a gate over the join. An
            // INNER join pushes a.v > 0 into a's scan, so these rows skip the INNER spelling.
            final String[][] runtimeConstantWheres = {
                    {"a.v > 0 AND now() > '1970-01-02' AND " + cast, castFilter},
                    {"a.v > 0 AND $1 = 1 AND b.c1 > 0", "0<c1"}
            };
            final String[] froms = {
                    " FROM a RIGHT JOIN b ON a.k = b.k",
                    " FROM b LEFT JOIN a ON a.k = b.k",
                    " FROM b JOIN a ON a.k = b.k"
            };
            // The ordinary plan compiles each WHERE clause that it keeps above the join into one
            // filter, and evaluates a filter that is a runtime constant once per execution: a false
            // value returns no row without opening the join, so b's scan never runs the cast. The
            // fused plan evaluates such a clause per joined row, after its probe filter, so the
            // analysis keeps the ordinary plan. $1 is bound to 2 and now() reads the current time,
            // so each of these runtime constants is false until assertOutcome() binds $1 to 1.
            final String select = "SELECT count(*), count(a.v)";
            final String[][] gatedQueries = {
                    // Each spelling runs the cast in b's scan and leaves the runtime constant alone
                    // above the join, after a constant-true conjunct folds away.
                    {select + froms[0] + " WHERE " + cast + " AND $1 = 1", null},
                    {select + froms[1] + " WHERE " + cast + " AND $1 = 1", null},
                    {select + froms[2] + " WHERE " + cast + " AND $1 = 1", null},
                    {select + froms[0] + " WHERE $1 = 1 AND " + cast, null},
                    {select + froms[0] + " WHERE " + cast + " AND now() < '1970-01-02'", null},
                    {select + froms[1] + " WHERE " + cast + " AND now() < '1970-01-02'", null},
                    {select + froms[2] + " WHERE " + cast + " AND now() < '1970-01-02'", null},
                    {select + froms[0] + " WHERE " + cast + " AND $1 = 1 AND " + constTrue, null},
                    {select + froms[1] + " WHERE " + cast + " AND $1 = 1 AND " + constTrue, null},
                    {select + froms[0] + " WHERE b.c1 > 0 AND $1 = 1", null},
                    // An INNER join pushes a.v > 0 into a's scan as well.
                    {select + froms[2] + " WHERE a.v > 0 AND $1 = 1", null},
                    {select + froms[0] + " WHERE $1 = 1", null},
                    {select + froms[0] + " WHERE now() < '1970-01-02'", null},
                    // The outer query's WHERE clause is a filter of its own, above the join's.
                    {"SELECT count(*), count(v) FROM (SELECT b.k, a.v" + froms[1] + " WHERE a.v > 0 AND " + cast + ") WHERE $1 = 1", null},
                    {"SELECT count(*), count(v) FROM (SELECT b.k, b.c0, a.v" + froms[0] + " WHERE a.v > 0)"
                            + " WHERE $1 = 1 AND 0.62::DECIMAL(2, 2) >= c0::DECIMAL(4, 2)", null},
                    // A filter that also reads a column is not a runtime constant, so it keeps the
                    // fused plan: a.v > 0 reads the NULL-extended input, which an outer join keeps
                    // above the join.
                    {select + froms[0] + " WHERE a.v > 0 AND $1 = 1 AND " + cast, castFilter},
                    {select + froms[1] + " WHERE a.v > 0 AND $1 = 1 AND " + cast, castFilter},
                    {"SELECT count(*), count(v) FROM (SELECT b.k, a.v" + froms[1] + " WHERE " + cast + " AND $1 = 1) WHERE v > 0", castFilter}
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                bindVariableService.setInt(0, 2);
                // Like a server entry point, the test freezes now() for the statements, so
                // now() > '1970-01-02' holds at run time.
                context.initNow();
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    for (String[] where : wheres) {
                        for (String from : froms) {
                            assertOutcome("SELECT count(*), count(a.v)" + from + " WHERE " + where[0], where[1], context);
                        }
                    }
                    for (String[] where : runtimeConstantWheres) {
                        // The RIGHT and LEFT spellings.
                        for (int i = 0; i < 2; i++) {
                            assertOutcome("SELECT count(*), count(a.v)" + froms[i] + " WHERE " + where[0], where[1], context);
                        }
                    }
                    for (String[] query : gatedQueries) {
                        assertOutcome(query[0], query[1], context);
                    }
                }
                // The plans below name the filter factory that runs without JIT.
                context.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
                // Each spelling keeps its ordinary plan, with the folded filter above the join and
                // the cast in b's scan, below it. The RIGHT spelling hashes b.
                final String constFalseWhere = " WHERE " + cast + " AND " + constFalse;
                assertQuery("SELECT count(*), count(a.v)" + froms[0] + constFalseWhere)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlan("""
                                GroupBy vectorized: false
                                  values: [count(*),count(v)]
                                    SelectedRecord
                                        Filter filter: false
                                            Hash Right Outer Join Light
                                              condition: b.k=a.k
                                              symbolKeyJoin: true
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: a
                                                Hash
                                                    Async Filter workers: 4
                                                      filter: 0.62>=c0::DECIMAL(4,2)
                                                        PageFrame
                                                            Row forward scan
                                                            Frame forward scan on: b
                                """);
                final String pushedPlan = """
                        GroupBy vectorized: false
                          values: [count(*),count(v)]
                            SelectedRecord
                                Filter filter: false
                                    %s
                                      condition: a.k=b.k
                                      symbolKeyJoin: true
                                        Async Filter workers: 4
                                          filter: 0.62>=c0::DECIMAL(4,2)
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: b
                                        Hash
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: a
                        """;
                assertQuery("SELECT count(*), count(a.v)" + froms[1] + constFalseWhere)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlan(pushedPlan.formatted("Hash Left Outer Join Light"));
                assertQuery("SELECT count(*), count(a.v)" + froms[2] + constFalseWhere)
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlan(pushedPlan.formatted("Hash Join Light"));
                // A runtime-constant filter gates the ordinary join, over b's scan with the cast.
                assertQuery(select + froms[1] + " WHERE " + cast + " AND $1 = 1")
                        .withContext(context)
                        .noLeakCheck()
                        .assertsPlan("""
                                GroupBy vectorized: false
                                  values: [count(*),count(v)]
                                    SelectedRecord
                                        Filter filter: $0::int=1
                                            Hash Left Outer Join Light
                                              condition: a.k=b.k
                                              symbolKeyJoin: true
                                                Async Filter workers: 4
                                                  filter: 0.62>=c0::DECIMAL(4,2)
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: b
                                                Hash
                                                    PageFrame
                                                        Row forward scan
                                                        Frame forward scan on: a
                                """);
                // One factory serves every bind value, so a gated shape keeps the ordinary plan
                // when $1 = 1 holds too, and then returns the joined rows.
                try (RecordCursorFactory factory = engine.select(select + froms[2] + " WHERE a.v > 0 AND $1 = 1", context)) {
                    Assert.assertFalse(plan(factory, context).contains("Hash Join Group By"));
                    assertFactory(factory).withContext(context).expectSize().noRandomAccess().returns("""
                            count\tcount1
                            0\t0
                            """);
                    bindVariableService.setInt(0, 1);
                    assertFactory(factory).withContext(context).expectSize().noRandomAccess().returns("""
                            count\tcount1
                            2\t2
                            """);
                } finally {
                    bindVariableService.setInt(0, 2);
                }
            }
        });
    }

    @Test
    public void testConstantFalseOnClauseKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, v LONG)");
            execute("CREATE TABLE b (k INT, c0 SHORT)");
            execute("INSERT INTO a VALUES (1, 1), (2, 2)");
            execute("INSERT INTO b VALUES (1, 1), (3, 7288)");
            // DECIMAL(4, 2) holds at most 99.99, so b's filter throws on the row where c0 is 7288.
            final String b = "(SELECT k, c0 FROM b WHERE 0.62::DECIMAL(2, 2) >= c0::DECIMAL(4, 2)) b";
            // Only FunctionParser folds this conjunct, so the optimizer leaves it in the ON clause.
            final String constFalse = "0.366490 >= 29_062::SHORT";
            // For a constant-false ON clause, generateJoins() replaces the build input with an empty
            // table, so the ordinary plan never runs b's scan or its filter. The analysis keeps the
            // ordinary plan for each spelling.
            final String[] queries = {
                    "SELECT count(*), sum(a.v) FROM a LEFT JOIN " + b + " ON a.k = b.k AND " + constFalse,
                    "SELECT count(*), sum(a.v) FROM " + b + " RIGHT JOIN a ON b.k = a.k AND " + constFalse,
                    "SELECT count(*), sum(a.v) FROM a LEFT JOIN " + b + " ON a.k = b.k AND b.c0 > 0 AND " + constFalse,
                    "SELECT count(*), sum(a.v) FROM a LEFT JOIN " + b + " ON a.k = b.k AND 1 = 2",
                    "SELECT count(*), sum(a.v) FROM a LEFT JOIN " + b + " ON a.k = b.k AND NULL::BOOLEAN",
                    "SELECT a.k, count(*), sum(a.v) FROM a LEFT JOIN " + b + " ON a.k = b.k AND " + constFalse + " ORDER BY 1"
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    for (String query : queries) {
                        assertOutcome(query, null, context);
                    }
                }
                // An ON filter on the build alone that is not a constant keeps the fused plan.
                final String[] fusedFroms = {
                        " FROM a LEFT JOIN b ON a.k = b.k AND b.c0 > 1",
                        " FROM b RIGHT JOIN a ON b.k = a.k AND b.c0 > 1"
                };
                for (String from : fusedFroms) {
                    assertQuery("SELECT count(*), count(b.k), sum(a.v)" + from)
                            .withContext(context)
                            .noLeakCheck()
                            .withPlanContaining("Async Hash Join Group By", "buildOnFilter: 1<c0")
                            .expectSize()
                            .noRandomAccess()
                            .returns("""
                                    count\tcount1\tsum
                                    2\t0\t3
                                    """);
                }
            }
        });
    }

    @Test
    public void testThrowingBuildOnFilterFailsOnlyInFusedPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k INT, v LONG)");
            execute("CREATE TABLE b (k INT, c0 SHORT)");
            execute("INSERT INTO a VALUES (1, 1), (2, 2)");
            execute("INSERT INTO b VALUES (1, 1), (3, 7288)");
            // Accepted divergence: the fused plan runs an ON filter on the build alone for every build row,
            // the ordinary plan only for key-matched pairs, so one that throws on the unmatched row where c0 is
            // 7288 fails only the fused plan. The analysis cannot tell which filters throw.
            final String cast = "0.62::DECIMAL(2, 2) >= b.c0::DECIMAL(4, 2)";
            final String[] queries = {
                    "SELECT count(*), sum(a.v) FROM a LEFT JOIN b ON a.k = b.k AND " + cast,
                    "SELECT count(*), sum(a.v) FROM b RIGHT JOIN a ON b.k = a.k AND " + cast
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (String query : queries) {
                    try (RecordCursorFactory factory = engine.select(query, context)) {
                        final String plan = plan(factory, context);
                        Assert.assertTrue(query + "\n" + plan, plan.contains("buildOnFilter: 0.62>=c0::DECIMAL(4,2)"));
                        Assert.assertEquals(query, "error: inconvertible value: 7288 [SHORT -> DECIMAL(4,2)]", outcome(factory, context));
                    }
                    context.setParallelHashJoinGroupByEnabled(false);
                    try {
                        assertQuery(query)
                                .withContext(context)
                                .noLeakCheck()
                                .withPlanNotContaining("Hash Join Group By")
                                .expectSize()
                                .noRandomAccess()
                                .returns("""
                                        count\tsum
                                        2\t3
                                        """);
                    } finally {
                        context.setParallelHashJoinGroupByEnabled(true);
                    }
                }
            }
        });
    }

    @Test
    public void testFilterOnShadowingComputedColumnKeepsOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k SYMBOL, v LONG)");
            execute("CREATE TABLE b (k SYMBOL, c0 SHORT, c1 SHORT, abs SHORT)");
            execute("INSERT INTO a VALUES ('x', 1), ('y', 2)");
            execute("INSERT INTO b VALUES ('x', 7288, 1, 7288), ('y', 1, 1, 1), ('z', 2, 1, 2)");
            final String[] froms = {
                    " FROM a RIGHT JOIN b ON a.k = b.k",
                    " FROM b LEFT JOIN a ON a.k = b.k",
                    " FROM b JOIN a ON a.k = b.k"
            };
            // The optimizer keeps a WHERE conjunct that names a computed column above the projection
            // that computes it, and the ordinary plan filters the output of that projection. Here c0
            // names b.c0 + 1000 above the projection and b.c0 below it, so an analysis that resolved
            // the conjunct below the projection would filter on b.c0 > 1000 instead. Each query keeps
            // the ordinary plan.
            final String[] shadowed = {
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 1000 c0%s) WHERE c0 > 1000",
                    "SELECT k, count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 1000 c0%s) WHERE c0 > 1000 ORDER BY k",
                    // A conjunct on a computed build column filters after the join.
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v + 1000 v%s) WHERE v > 1001",
                    // The optimizer moves c1 > 0 below the projection and keeps c0 > 1000 above it.
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 1000 c0, b.c1%s) WHERE c1 > 0 AND c0 > 1000",
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 - a.v * 7288 c0%s) WHERE c0 > 1000",
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, CASE WHEN b.c0 > 1000 THEN 0 ELSE 1 END c0%s) WHERE c0 = 1",
                    "SELECT count(*), count(v) FROM (SELECT concat(b.k, 'q') k, a.v%s) WHERE k = 'x'",
                    // A computed column named after its own function.
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, abs(b.abs - 5000) abs%s) WHERE abs > 1000",
                    // A projection above the one that computes c0, and a computed c0 over another one.
                    "SELECT count(*), count(v) FROM (SELECT * FROM (SELECT b.k, a.v, b.c0 + 1000 c0%s) WHERE c0 > 1000)",
                    "SELECT count(*), count(v) FROM (SELECT k, v, c0 * 2 c0 FROM (SELECT b.k, a.v, b.c0 + 1000 c0%s)) WHERE c0 > 2002",
                    // A computed column whose name names nothing below the projection.
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 1000 d%s) WHERE d > 1000"
            };
            // The optimizer moves a conjunct that names only plain columns below the projection, so
            // it stays fused, with the probe filter that follows each query.
            final String[][] fusedQueries = {
                    {"SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 x%s) WHERE x > 1000", "1000<c0"},
                    {"SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 1000 c0, b.c1%s) WHERE c1 > 0", "0<c1"}
            };
            // A GROUP BY key or an aggregate reads the projection's output, so a computed column
            // that no WHERE conjunct names stays fused.
            final String[] unfiltered = {
                    "SELECT c0, count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 1000 c0%s) ORDER BY c0",
                    "SELECT sum(c0), sum(v), count(*) FROM (SELECT b.k, a.v + 1000 v, b.c0 + 1000 c0%s)"
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    for (String from : froms) {
                        for (String query : shadowed) {
                            assertOutcome(query.formatted(from), null, context);
                        }
                        for (String[] query : fusedQueries) {
                            assertOutcome(query[0].formatted(from), query[1], context);
                        }
                        for (String query : unfiltered) {
                            assertDifferential(query.formatted(from), context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testShadowingColumnFiltersOnTwoLevelsKeepOrdinaryPlan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k SYMBOL, v LONG)");
            execute("CREATE TABLE b (k SYMBOL, c0 SHORT, c1 SHORT)");
            execute("INSERT INTO a VALUES ('x', 1), ('y', 2)");
            execute("INSERT INTO b VALUES ('x', 7288, 1), ('y', 1, 1), ('z', 2, 1)");
            final String[] froms = {
                    " FROM a RIGHT JOIN b ON a.k = b.k",
                    " FROM b LEFT JOIN a ON a.k = b.k",
                    " FROM b JOIN a ON a.k = b.k"
            };
            // DECIMAL(4, 2) holds at most 99.99, so the cast throws on the row where c0 is 7288.
            final String cast = "0.62::DECIMAL(2, 2) >= ";
            // The ordinary plan evaluates the sub-query's WHERE clause below the projection and the
            // outer one above it, so b.c1 > 5 rejects every row before the cast runs. The outer
            // conjunct names c0, a computed column whose name also names b.c0 below the projection,
            // so each query keeps the ordinary plan, and the fused probe filter never holds
            // conjuncts from two WHERE clauses.
            final String[] queries = {
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 0 c0%s WHERE b.c1 > 5) WHERE " + cast + "c0::DECIMAL(4, 2)",
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, abs(b.c0) c0%s WHERE b.c1 > 5) WHERE " + cast + "c0::DECIMAL(4, 2)",
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c0::SHORT c0%s WHERE b.c1 > 5) WHERE " + cast + "c0::DECIMAL(4, 2)",
                    "SELECT k, count(*), count(v) FROM (SELECT b.k, a.v, b.c0 + 0 c0%s WHERE b.c1 > 5) WHERE "
                            + cast + "c0::DECIMAL(4, 2) ORDER BY k",
                    // The roles reversed: the ordinary plan runs the cast first and fails.
                    "SELECT count(*), count(v) FROM (SELECT b.k, a.v, b.c1 + 0 c1%s WHERE " + cast + "b.c0::DECIMAL(4, 2)) WHERE c1 > 5"
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    for (String from : froms) {
                        for (String query : queries) {
                            assertOutcome(query.formatted(from), null, context);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testRightJoinWhereMatchesLeftJoinSpelling() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (k SYMBOL, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (k SYMBOL, c0 SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO a VALUES
                        ('x', 1, '2024-01-01T00:00:00.000000Z'),
                        ('y', 2, '2024-01-02T00:00:00.000000Z')
                    """);
            execute("""
                    INSERT INTO b VALUES
                        ('x', 7288, '2024-01-01T00:00:00.000000Z'),
                        ('y', 1, '2024-01-02T00:00:00.000000Z'),
                        ('y', 0, '2024-01-02T12:00:00.000000Z'),
                        ('z', 2, '2024-01-03T00:00:00.000000Z')
                    """);
            // DECIMAL(4, 2) holds at most 99.99, so the cast throws on the row where c0 is 7288, and
            // the predicate holds only where c0 is 0.
            final String cast = "0.62::DECIMAL(2, 2) >= b.c0::DECIMAL(4, 2)";
            final String castFilter = "0.62>=c0::DECIMAL(4,2)";
            // The interval leaves out the row where c0 is 7288.
            final String interval = "b.ts >= '2024-01-02T00:00:00.000000Z' AND b.ts < '2024-01-04T00:00:00.000000Z'";
            final String right = " FROM a RIGHT JOIN b ON a.k = b.k";
            final String left = " FROM b LEFT JOIN a ON a.k = b.k";
            // The optimizer filters b, the preserved input, in its own scan for both spellings, with
            // interval extraction, and keeps the conjuncts on a above the join. Both plans of both
            // spellings then evaluate each conjunct on the same rows in the same order: a.v > 100 does
            // not spare a row of b the cast, and the interval does. Each row holds a query, with %s
            // for the FROM clause, and the fused plan's probe filter, or null for the ordinary plan.
            final String[][] queries = {
                    {"SELECT count(*), count(a.v)%s WHERE a.v > 100 AND " + cast, castFilter},
                    {"SELECT count(*), count(a.v)%s WHERE " + cast + " AND a.v > 100", castFilter},
                    {"SELECT count(*), count(a.v)%s WHERE (0.366490 >= 29_062::SHORT OR a.v > 100) AND " + cast, castFilter},
                    {"SELECT count(*), count(a.v)%s WHERE " + cast + " AND " + interval, castFilter},
                    {"SELECT count(*), count(a.v)%s WHERE a.v > 0 AND " + cast + " AND " + interval, castFilter},
                    // The runtime constant is alone in the sub-query's WHERE clause, above the join,
                    // and the optimizer moves the outer cast into b's scan, so the ordinary plan gates
                    // the join on it and the fused analysis keeps the ordinary plan.
                    {"SELECT count(*), count(v) FROM (SELECT b.k, b.c0, a.v%s WHERE now() < '1970-01-02')"
                            + " WHERE 0.62::DECIMAL(2, 2) >= c0::DECIMAL(4, 2)", null}
            };
            try (SqlExecutionContextImpl context = enabledContext()) {
                context.initNow();
                for (int jit : new int[]{SqlJitMode.JIT_MODE_ENABLED, SqlJitMode.JIT_MODE_DISABLED}) {
                    context.setJitMode(jit);
                    for (String[] query : queries) {
                        final String rightSql = query[0].formatted(right);
                        final String leftSql = query[0].formatted(left);
                        assertOutcome(rightSql, query[1], context);
                        assertOutcome(leftSql, query[1], context);
                        context.setParallelHashJoinGroupByEnabled(false);
                        try (
                                RecordCursorFactory rightFactory = engine.select(rightSql, context);
                                RecordCursorFactory leftFactory = engine.select(leftSql, context)
                        ) {
                            Assert.assertEquals(rightSql, outcome(leftFactory, context), outcome(rightFactory, context));
                        } finally {
                            context.setParallelHashJoinGroupByEnabled(true);
                        }
                    }
                }
                // The fused plan of the RIGHT spelling scans b's interval with the cast as its probe
                // filter, as the ordinary plan of either spelling does.
                assertQuery("SELECT count(*), count(a.v)" + right + " WHERE " + cast + " AND " + interval)
                        .withContext(context)
                        .noLeakCheck()
                        .withPlanContaining("probeFilter: " + castFilter)
                        .withPlanContaining("Interval forward scan on: b")
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                count\tcount1
                                1\t1
                                """);
            }
        });
    }

    // Checks the result against the ordinary plan, and the rows, keys and allocated bytes of the build.
    private void assertBuildSize(String sql, long rows, long keys, long sizeInBytes, SqlExecutionContextImpl context) throws Exception {
        assertDifferential(sql, context, true);
        try (
                RecordCursorFactory factory = engine.select(sql, context);
                RecordCursor cursor = factory.getCursor(context)
        ) {
            // The first read builds, whether or not it returns a row.
            cursor.hasNext();
            FrozenHashJoinBuild build = fused(factory).getAtom().getFrozenBuild();
            Assert.assertEquals(sql, rows, build.getRowCount());
            Assert.assertEquals(sql, keys, build.getKeyCount());
            Assert.assertEquals(sql, sizeInBytes, build.getSizeInBytes());
        }
    }

    // Checks the page frames that an execution's build reads.
    private void assertBuildFrameCount(String sql, int frameCount, SqlExecutionContextImpl context) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, context);
             RecordCursor cursor = factory.getCursor(context)) {
            // The first read builds.
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(sql, frameCount, fused(factory).getAtom().getBuildFrameCount());
        }
    }

    /**
     * Checks the results against the ordinary plan, and where the build ran: in rounds over this many
     * hash partitions, or on the owner for zero.
     */
    private void assertBuildRounds(String sql, long buildRows, int partitionCount, SqlExecutionContextImpl context) throws Exception {
        assertDifferential(sql, context, true);
        try (RecordCursorFactory factory = engine.select(sql, context);
             RecordCursor cursor = factory.getCursor(context)) {
            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
            // The first read builds, whether or not it returns a row.
            cursor.hasNext();
            Assert.assertEquals(sql, partitionCount > 0, atom.isBuiltInRounds());
            Assert.assertEquals(sql, Math.max(1, partitionCount), atom.getBuildPartitionCount());
            Assert.assertEquals(sql, buildRows, atom.getFrozenBuild().getRowCount());
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

    // As assertPayloadCopy(), and checks that the build ran in rounds, so that the workers copied it.
    private void assertPayloadCopyInRounds(String sql, boolean isCopied, SqlExecutionContextImpl context) throws Exception {
        assertDifferential(sql, context, true);
        try (RecordCursorFactory factory = engine.select(sql, context);
             RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(cursor.hasNext());
            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
            Assert.assertTrue(atom.isBuiltInRounds());
            Assert.assertEquals(isCopied, atom.isPayloadCopied());
        }
    }

    // pa holds 100 rows on each of its first four days, which turn Parquet, 10_000 rows on its fifth
    // and one row on its sixth, which stay native; pb holds 10 rows. Both key their rows by ten strings
    // of keyLength chars, pa's of the build key type and pb's of the probe key type. The LEFT join
    // builds pa's 10_401 rows and probes pb's 10, far fewer than the default ratios ask to repay the
    // copy of its 10_001 native rows. A probe reader decodes the DOUBLE payload of the four Parquet
    // days alone, 8 bytes a row, and never the key, which the key table holds, so the build copies once
    // 3_200 bytes exceed a reader's share of the decode cache, however long the key.
    private void assertParquetVarSizeKeyCopy(String buildKeyType, String probeKeyType, int keyLength) throws Exception {
        assertMemoryLeak(() -> {
            final String key = "lpad((x % 10)::VARCHAR, " + keyLength + ", '0')";
            execute("CREATE TABLE pa (k " + buildKeyType + ", v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE pb (k " + probeKeyType + ", v DOUBLE)");
            execute("INSERT INTO pa SELECT " + key + "::" + buildKeyType + ", x, timestamp_sequence('2020-01-01', 864_000_000L) FROM long_sequence(400)");
            execute("INSERT INTO pa SELECT " + key + "::" + buildKeyType + ", 1_000 + x, timestamp_sequence('2020-01-05', 1_000_000) FROM long_sequence(10_000)");
            execute("INSERT INTO pa SELECT " + key + "::" + buildKeyType + ", 1_000_000, '2020-01-06' FROM long_sequence(1)");
            execute("INSERT INTO pb SELECT " + key + "::" + probeKeyType + ", x FROM long_sequence(10)");
            execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-05'");
            final String sql = "SELECT count(*) n, sum(pa.v) v FROM pb LEFT JOIN pa ON pa.k = pb.k";
            final long parquetDecodedSize = 400L * Double.BYTES;
            try (SqlExecutionContextImpl context = enabledContext()) {
                // A pool that decodes the payload alone, as a reader's pool does, counts 8 bytes a row
                // for the four Parquet days, where one that decodes the key as well counts more than 50.
                final IntHashSet payload = new IntHashSet();
                payload.add(1);
                Assert.assertEquals(parquetDecodedSize, decodedParquetSize("SELECT k, v FROM pa", payload, context));
                final long decodedWithKey = decodedParquetSize("SELECT k, v FROM pa", null, context);
                Assert.assertTrue("decoded " + decodedWithKey, decodedWithKey > 400L * 50);
                // A reader's share of the default cache holds the Parquet days many times over, so the
                // tiny probe keeps row ids, on the owner and on the workers.
                assertPayloadCopy(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                assertPayloadCopyInRounds(sql, false, context);
                // Each of the five slots' readers takes a quarter of the cache. A share that holds the
                // payload of the Parquet days exactly keeps row ids, however long the key, and one a byte
                // short copies, on the workers and on the owner alike.
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 4 * parquetDecodedSize);
                assertPayloadCopyInRounds(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 4 * parquetDecodedSize - 1);
                assertPayloadCopyInRounds(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 1_000_000);
                assertPayloadCopy(sql, true, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 4 * parquetDecodedSize);
                assertPayloadCopy(sql, false, context);
                // A build in rounds of many partitions keeps the same boundary.
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_PARALLEL_MIN_ROWS, 0);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_HASH_JOIN_GROUPBY_BUILD_ROWS_PER_PARTITION, 1_000);
                assertPayloadCopyInRounds(sql, false, context);
                setProperty(PropertyKey.CAIRO_SQL_PARQUET_CACHE_MEMORY_SIZE, 4 * parquetDecodedSize - 1);
                assertPayloadCopyInRounds(sql, true, context);
                try (RecordCursorFactory factory = engine.select(sql, context);
                     RecordCursor cursor = factory.getCursor(context)) {
                    Assert.assertTrue(cursor.hasNext());
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    Assert.assertTrue("partitions: " + atom.getBuildPartitionCount(), atom.getBuildPartitionCount() > 1);
                }
            }
        });
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
            try (RecordCursor cursor = factory.getCursor(context)) {
                // The first read builds; a scalar aggregate always returns its row.
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(plan, buildRows, fused(factory).getAtom().getFrozenBuild().getRowCount());
            }
        }
    }

    // Whether the open cursor's build counts its key rows on the workers. A build of more than one
    // partition does when each partition's rows fill a region of their own. When the rows run through
    // every frame's region instead, a task of the count walks a run of them per frame, and the count
    // takes the round only with at least four tasks whose runs average 256 rows or more: at least
    // four partitions, and frames of 1_024 rows or more on average; see
    // IntHashJoinBuild.getKeyRowCountTaskCount().
    private static boolean isKeyRowCountInRounds(AsyncHashJoinGroupByAtom atom) {
        if (!atom.isCountedByKey() || atom.getBuildPartitionCount() < 2) {
            return false;
        }
        return !atom.hasBuildFrameRegions()
                || atom.getBuildPartitionCount() >= 4 && atom.getFrozenBuild().getRowCount() >= 1_024L * atom.getBuildFrameCount();
    }

    // Runs the lone count's factory once, and checks the count and where the execution took it from.
    private static void assertCountExecution(
            RecordCursorFactory factory,
            long expected,
            boolean isCountedByKey,
            SqlExecutionContextImpl context
    ) throws Exception {
        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
        try (RecordCursor cursor = factory.getCursor(context)) {
            // The first read builds and counts; an open cursor has done neither.
            Assert.assertFalse(atom.isCountedByKey());
            Assert.assertFalse(atom.isCountedByMatch());
            Assert.assertTrue(cursor.hasNext());
            Assert.assertEquals(isCountedByKey, atom.isCountedByKey());
            // A build of one row per key, an empty one included, counts by match instead.
            Assert.assertEquals(!isCountedByKey, atom.isCountedByMatch());
            // A build of more than one partition counts its key rows on the workers, unless its rows
            // run through frames too small for the count's tasks.
            Assert.assertEquals(isKeyRowCountInRounds(atom), atom.isCountedInRounds());
            Assert.assertEquals(expected, cursor.getRecord().getLong(0));
            Assert.assertFalse(cursor.hasNext());
        }
        Assert.assertFalse(atom.isCountedByKey());
        Assert.assertFalse(atom.isCountedByMatch());
        Assert.assertFalse(atom.isCountedInRounds());
    }

    /**
     * Checks the lone count against the ordinary plan and against the given total, that EXPLAIN names
     * its strategy, and that an execution over a build of the given mode takes the count from the
     * build's key row counts when it should: when the build repeats a key. Any other build counts
     * one pair per probe row that finds its key.
     */
    private void assertCountPerKey(
            String sql,
            String column,
            long expected,
            boolean isCountedByKey,
            HashJoinBuildMode buildMode,
            SqlExecutionContextImpl context
    ) throws Exception {
        assertDifferential(sql, context, true);
        assertQuery(sql)
                .withContext(context)
                .noLeakCheck()
                .noRandomAccess()
                .expectSize()
                .withPlanContaining("countStrategy: per key", "values: [count(*)]")
                .returns(column + '\n' + expected + '\n');
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
            try (RecordCursor cursor = factory.getCursor(context)) {
                // The first read builds and counts; an open cursor has done neither.
                Assert.assertFalse(sql, atom.isCountedByKey());
                Assert.assertFalse(sql, atom.isCountedByMatch());
                Assert.assertNull(sql, atom.getFrozenBuild());
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(sql, isCountedByKey, atom.isCountedByKey());
                Assert.assertEquals(sql, !isCountedByKey, atom.isCountedByMatch());
                Assert.assertEquals(sql, buildMode == HashJoinBuildMode.PARALLEL, atom.isBuiltInRounds());
                if (buildMode == HashJoinBuildMode.PARALLEL && atom.getFrozenBuild().getRowCount() > 1) {
                    // More than one hash partition, so the reducers run the loops over the partitioned probes.
                    Assert.assertTrue(sql + ", partitions: " + atom.getBuildPartitionCount(), atom.getBuildPartitionCount() > 1);
                }
                // The workers count the key rows of a build of more than one partition, unless its
                // rows run through frames too small for the count's tasks, and the owner those of
                // any other build.
                Assert.assertEquals(sql, isKeyRowCountInRounds(atom), atom.isCountedInRounds());
                Assert.assertEquals(sql, expected, cursor.getRecord().getLong(0));
                // Rereading the count neither rebuilds nor recounts.
                cursor.toTop();
                Assert.assertTrue(cursor.hasNext());
                Assert.assertEquals(sql, expected, cursor.getRecord().getLong(0));
                Assert.assertEquals(sql, isCountedByKey, atom.isCountedByKey());
                Assert.assertEquals(sql, !isCountedByKey, atom.isCountedByMatch());
            }
            // The counts belong to the execution that built them.
            Assert.assertFalse(sql, atom.isCountedByKey());
            Assert.assertFalse(sql, atom.isCountedByMatch());
            Assert.assertFalse(sql, atom.isCountedInRounds());
            Assert.assertNull(sql, atom.getFrozenBuild());
        }
    }

    private void assertDifferential(String sql, SqlExecutionContextImpl context, boolean isEnabled) throws Exception {
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
            Assert.assertEquals(sql + "\n" + plan, isEnabled, plan.contains("Hash Join Group By"));
            if (!isEnabled) {
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

    /**
     * Checks that both plans fail with the same error or return the same rows, with $1 bound to 2
     * and then, on the same factories, to 1, and then the fused plan's probe filter. A null probe
     * filter expects the ordinary plan.
     */
    private static void assertOutcome(String sql, @Nullable String probeFilter, SqlExecutionContextImpl context) throws Exception {
        String expected;
        String expectedRebound;
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = engine.select(sql, context)) {
            Assert.assertFalse(plan(baseline, context).contains("Hash Join Group By"));
            expected = outcome(baseline, context);
            bindVariableService.setInt(0, 1);
            expectedRebound = outcome(baseline, context);
        } finally {
            bindVariableService.setInt(0, 2);
            context.setParallelHashJoinGroupByEnabled(true);
        }
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            // Outcomes first, so a regression reports the wrong answer rather than only the plan.
            String plan = plan(factory, context);
            Assert.assertEquals(sql + "\n" + plan, expected, outcome(factory, context));
            bindVariableService.setInt(0, 1);
            Assert.assertEquals(sql + "\n" + plan, expectedRebound, outcome(factory, context));
            Assert.assertEquals(sql + "\n" + plan, probeFilter != null, plan.contains("Hash Join Group By"));
            if (probeFilter != null) {
                Assert.assertTrue(sql + "\n" + plan, plan.contains("probeFilter: " + probeFilter));
            }
        } finally {
            bindVariableService.setInt(0, 2);
        }
    }

    private SqlExecutionContextImpl context(int workers) {
        return new SqlExecutionContextImpl(engine, workers).with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
    }

    /**
     * ca holds 1_000 rows over four days: 100 of each key from 1 to 9 and 100 of a NULL key. cb holds
     * 200 rows over two days: 25 of each key from 5 to 11 and 25 of a NULL key. Every key column holds
     * the same key, and the tables intern the SYMBOL texts in different orders.
     */
    private void createCountTables() throws Exception {
        execute("CREATE TABLE ca (k INT, l LONG, s SYMBOL, vc VARCHAR, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE cb (k INT, l LONG, s SYMBOL, vc VARCHAR, w DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        insertCountProbeRows();
        insertCountBuildRows();
    }

    /**
     * pa holds 100 rows on its first day, valued 1 to 100, 10_000 rows on its second, valued 1_001 to
     * 11_000, and one row on its third, the last partition, which stays native. pb holds 10 rows. Both
     * key their rows from 0 to 9.
     */
    private void createMostlyNativeBuild() throws Exception {
        execute("CREATE TABLE pa (k INT, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("CREATE TABLE pb (k INT, v DOUBLE)");
        execute("INSERT INTO pa SELECT (x % 10)::INT, x, timestamp_sequence('2020-01-01', 1_000_000) FROM long_sequence(100)");
        execute("INSERT INTO pa SELECT (x % 10)::INT, 1_000 + x, timestamp_sequence('2020-01-02', 1_000_000) FROM long_sequence(10_000)");
        execute("INSERT INTO pa VALUES (0, 1_000_000, '2020-01-03')");
        execute("INSERT INTO pb SELECT (x % 10)::INT, x FROM long_sequence(10)");
    }

    // pa holds 2_000 rows over four days, 500 a day, which turn into Parquet row groups of 100 rows, one
    // build frame each, and nativeRows more on its fifth day, its last partition, which stays native.
    // Row x holds the keys that keyValues computes from x, and the value x.
    private void createParquetRowGroupBuild(String keyColumns, String keyValues, long nativeRows) throws Exception {
        setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 100);
        execute("CREATE TABLE pa (" + keyColumns + ", v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO pa SELECT " + keyValues + ", x, timestamp_sequence('2020-01-01', 172_800_000L) FROM long_sequence(2_000)");
        execute("INSERT INTO pa SELECT " + keyValues + ", x, ts FROM ("
                + "SELECT 2_000 + x x, timestamp_sequence('2020-01-05', 1_000_000) ts FROM long_sequence(" + nativeRows + "))");
        execute("ALTER TABLE pa CONVERT PARTITION TO PARQUET WHERE ts < '2020-01-05'");
    }

    private void createTables() throws Exception {
        execute("create table r (plant_id int, reading_ts timestamp, energy_kwh double, irradiance_wm2 double) timestamp(reading_ts) partition by month");
        execute("create table p (plant_id int, country symbol, installed_kwp double)");
        execute("insert into r values (1, '2020-01-01', 10, 100), (3, '2020-01-02', 30, 300), "
                + "(1, '2020-01-03', 20, 200), (2, '2020-02-01', 40, null), (null, '2021-01-01', 50, 500)");
        execute("insert into p values (1, 'ES', 5), (1, 'ES', 7), (1, 'IT', null), (2, null, null), (null, 'ES', 11)");
    }

    /**
     * p holds 4 rows, and b, the build of p's LEFT and RIGHT joins, holds 6 rows with variable-size
     * columns, NULLs included.
     */
    private void createVariableSizeOnFilterTables() throws Exception {
        execute("CREATE TABLE p (id INT, k INT)");
        execute("CREATE TABLE b (id INT, v DOUBLE, vc VARCHAR, str STRING, arr DOUBLE[])");
        execute("""
                INSERT INTO p VALUES
                    (1, 1),
                    (2, 1),
                    (3, 2),
                    (4, 2)
                """);
        execute("""
                INSERT INTO b VALUES
                    (1, 10.0, 'xa', 'sa', ARRAY[1.0]),
                    (1, 20.0, 'yb', 'sb', ARRAY[2.0]),
                    (2, 30.0, 'xc', NULL, ARRAY[3.0]),
                    (3, 40.0, NULL, 'sa', NULL),
                    (5, 50.0, 'xe', 'sa', ARRAY[5.0]),
                    (6, 60.0, 'xf', 'sb', ARRAY[6.0])
                """);
    }

    private void insertCountBuildRows() throws Exception {
        execute("""
                INSERT INTO cb SELECT
                    CASE WHEN x % 8 = 0 THEN NULL ELSE (x % 8 + 4)::INT END,
                    CASE WHEN x % 8 = 0 THEN NULL ELSE x % 8 + 4 END,
                    (CASE WHEN x % 8 = 0 THEN NULL ELSE 'S' || (x % 8 + 4) END)::SYMBOL,
                    (CASE WHEN x % 8 = 0 THEN NULL ELSE 'V' || (x % 8 + 4) END)::VARCHAR,
                    x,
                    timestamp_sequence('2020-01-01', 864_000_000L)
                FROM long_sequence(200)
                """);
    }

    private void insertCountProbeRows() throws Exception {
        execute("""
                INSERT INTO ca SELECT
                    CASE WHEN x % 10 = 0 THEN NULL ELSE (x % 10)::INT END,
                    CASE WHEN x % 10 = 0 THEN NULL ELSE x % 10 END,
                    (CASE WHEN x % 10 = 0 THEN NULL ELSE 'S' || (x % 10) END)::SYMBOL,
                    (CASE WHEN x % 10 = 0 THEN NULL ELSE 'V' || (x % 10) END)::VARCHAR,
                    x,
                    timestamp_sequence('2020-01-01', 345_600_000L)
                FROM long_sequence(1_000)
                """);
    }

    // Decodes every Parquet frame of the scan whole into a pool of its own, the given columns of each
    // or every column without a projection, as a probe reader decodes its payload columns, and returns
    // the bytes that the pool's budget counts for them.
    private long decodedParquetSize(String scan, @Nullable IntHashSet projection, SqlExecutionContextImpl context) throws Exception {
        try (
                RecordCursorFactory factory = engine.select(scan, context);
                PageFrameCursor frameCursor = factory.getPageFrameCursor(context, PartitionFrameCursorFactory.ORDER_ASC);
                PageFrameAddressCache addressCache = new PageFrameAddressCache();
                PageFrameMemoryPool pool = new PageFrameMemoryPool(configuration, Long.MAX_VALUE)
        ) {
            addressCache.of(factory.getMetadata(), frameCursor.getColumnMapping(), frameCursor.isExternal());
            int frameCount = 0;
            PageFrame frame;
            while ((frame = frameCursor.next()) != null) {
                addressCache.add(frameCount++, frame);
            }
            pool.of(addressCache, ParquetDecodeHint.SCATTERED, projection);
            for (int frameIndex = 0; frameIndex < frameCount; frameIndex++) {
                if (addressCache.getFrameFormat(frameIndex) == PartitionFormat.PARQUET) {
                    pool.navigateTo(frameIndex);
                }
            }
            Assert.assertTrue(pool.getCachedFrameCount() > 0);
            return pool.getCachedBytes();
        }
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

    // Runs the factory once and returns the rows that its execution built, from either orientation.
    private static long builtRows(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            // The first read builds, whether or not it returns a row.
            cursor.hasNext();
            RecordCursorFactory current = factory;
            while (!(current instanceof AsyncHashJoinGroupByRecordCursorFactory)) {
                if (current instanceof HashJoinGroupByBuildChoiceRecordCursorFactory choice) {
                    current = choice.isAlternateChosen() ? choice.getAlternate() : choice.getPrimary();
                } else {
                    current = current.getBaseFactory();
                    Assert.assertNotNull(current);
                }
            }
            return ((AsyncHashJoinGroupByRecordCursorFactory) current).getAtom().getFrozenBuild().getRowCount();
        }
    }

    // Appends rows to f, one per key of d in turn, a minute apart from the given day.
    private static void insertFactRows(int rowCount, String day) throws Exception {
        execute("INSERT INTO f SELECT (x % 1_000 + 1)::INT, 'S' || (x % 1_000 + 1), x, timestamp_sequence('" + day
                + "', 60_000_000L) FROM long_sequence(" + rowCount + ")");
    }

    // Reads the result once, and leaves in the counter the clock reads of that execution's reducers.
    private static String resultOf(String sql, SqlExecutionContextImpl context, NetworkSqlExecutionCircuitBreaker breaker, AtomicInteger counter) throws Exception {
        try (RecordCursorFactory factory = engine.select(sql, context)) {
            Assert.assertTrue(sql, plan(factory, context).contains("Hash Join Group By"));
            breaker.resetTimer();
            counter.set(0);
            return result(factory, context);
        }
    }

    // Reads the result twice, or names the error that stops a read.
    private static String outcome(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try {
            return result(factory, context) + result(factory, context);
        } catch (CairoException | ImplicitCastException e) {
            return "error: " + e.getFlyweightMessage();
        }
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
