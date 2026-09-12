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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class HashJoinGroupByQualificationTest extends AbstractCairoTest {
    private static final String[] JOINS = {
            " from r join p on r.id=p.id",
            " from r left join p on r.id=p.id",
            " from p right join r on r.id=p.id",
            " from p left join r on r.id=p.id",
            " from r right join p on r.id=p.id"
    };
    private static final String AGGREGATES = "count(*) n, count(r.id) ri, count(p.id) pi, "
            + "count(r.s) rs, count(p.s) ps, sum(r.d) rd, sum(p.d) pd, avg(r.d) ra, avg(p.d) pa";

    @Test
    public void testRandomizedDifferentialMatrix() throws Exception {
        assertMemoryLeak(() -> {
            // Fixed seeds make the generated SQL and data reproducible independently of test order.
            Rnd rnd = new Rnd(130, 9);
            execute("create table r (id int, g int, d double, s symbol)");
            execute("create table p (id int, g int, d double, s symbol)");
            for (int scenario = 0; scenario < 24; scenario++) {
                execute("truncate table r");
                execute("truncate table p");
                int domain = new int[]{1, 3, 17, 128}[scenario % 4];
                insertRandomRows("r", scenario == 0 ? 0 : 1 + rnd.nextInt(160), domain, scenario, rnd);
                insertRandomRows("p", scenario == 1 ? 0 : 1 + rnd.nextInt(80), domain, scenario, rnd);
                setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, scenario % 2 == 0 ? 1 : Integer.MAX_VALUE);
                for (int workers : new int[]{1, 2, 4}) {
                    try (SqlExecutionContextImpl context = context(engine, workers)) {
                        context.changePageFrameSizes(1, 1 + rnd.nextInt(19));
                        context.setJitMode(scenario % 2 == 0 ? 0 : 2);
                        for (int j = 0; j < JOINS.length; j++) {
                            String build = j < 3 ? "p" : "r";
                            String probe = j < 3 ? "r" : "p";
                            String on = scenario % 3 == 0 ? " and " + build + ".d > " + (rnd.nextInt(17) - 8) : "";
                            String where = switch (scenario % 6) {
                                case 0 -> "";
                                case 1 -> " where " + build + ".d is null";
                                case 2 -> " where " + build + ".d > 1000";
                                case 3 -> " where " + probe + ".d > " + (rnd.nextInt(17) - 8);
                                case 4 -> " where (" + build + ".s = 's1' or " + build + ".s is null)";
                                default -> " where " + probe + ".g >= 0 and " + build + ".d < 4";
                            };
                            String group = scenario % 2 == 0 ? "r.g, p.s" : "r.id, p.g, r.s";
                            String from = JOINS[j] + on + where;
                            assertDifferential("select " + AGGREGATES + from, context, true);
                            assertDifferential("select " + group + ", " + AGGREGATES + from + " order by " + group, context, true);
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testAllPayloadTypesNativeParquetColumnTopsAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 2);
            createTypedTable("r", 0);
            createTypedTable("p", 1);
            String columns = "";
            for (String table : new String[]{"r", "p"}) {
                for (String column : new String[]{"b", "y", "h", "c", "i", "l", "dt", "t", "ns", "f", "d", "s"}) {
                    columns += (columns.isEmpty() ? "" : ", ") + table + "." + column;
                }
            }
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                for (String join : JOINS) {
                    for (boolean keyed : new boolean[]{false, true}) {
                        String sql = "select " + (keyed ? columns + ", " : "") + AGGREGATES + join
                                + (keyed ? " order by " + columns : "");
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            fused(factory);
                            assertAgainstBaseline(sql, factory, context);
                            execute("alter table r convert partition to parquet where t < '2020-01-02'");
                            assertAgainstBaseline(sql, factory, context);
                            execute("alter table p convert partition to parquet where t >= '2020-01-02'");
                            assertAgainstBaseline(sql, factory, context);
                            execute("alter table r convert partition to parquet where t >= '2020-01-02'");
                            execute("alter table p convert partition to parquet where t < '2020-01-02'");
                            assertAgainstBaseline(sql, factory, context);
                            execute("alter table r convert partition to native where t >= 0");
                            execute("alter table p convert partition to native where t >= 0");
                            assertAgainstBaseline(sql, factory, context);
                        }
                    }
                }
                // Force scalar predicates to read the other declared logical types on both inputs.
                String where = " where ";
                for (String table : new String[]{"r", "p"}) {
                    where += (table.equals("r") ? "" : " and ") + "(" + table + ".b or " + table + ".y=0 or "
                            + table + ".h>0 or " + table + ".c='A' or " + table + ".l is null or "
                            + table + ".dt>'2020-01-01' or " + table + ".ns>'2020-01-01' or " + table + ".f>0)";
                }
                for (String table : new String[]{"r", "p"}) {
                    execute("alter table " + table + " alter column id type short");
                    execute("alter table " + table + " convert partition to parquet where t < '2020-01-02'");
                    execute("alter table " + table + " alter column id type int");
                    execute("alter table " + table + " alter column f type double");
                    execute("alter table " + table + " alter column i type long");
                }
                for (String join : JOINS) {
                    assertDifferential("select " + AGGREGATES + join + where, context, true);
                    assertDifferential("select " + columns + ", " + AGGREGATES + join + where + " order by " + columns, context, true);
                }
            }
        });
    }

    @Test
    public void testColumnsAddedAfterParquetConversion() throws Exception {
        assertMemoryLeak(() -> {
            for (String table : new String[]{"r", "p"}) {
                execute("create table " + table + " (id int, t timestamp) timestamp(t) partition by day");
                execute("insert into " + table + " values (1,'2020-01-01'), (2,'2020-01-02')");
                execute("alter table " + table + " convert partition to parquet where t < '2020-01-02'");
                execute("alter table " + table + " add column d double");
                execute("alter table " + table + " add column s symbol");
                execute("insert into " + table + " values (1,'2020-01-02T01',0.5,'new'), (3,'2020-01-03',4,'last')");
            }
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 1);
                for (String join : JOINS) {
                    assertDifferential("select " + AGGREGATES + join, context, true);
                    assertDifferential("select r.s,p.s," + AGGREGATES + join + " order by r.s,p.s", context, true);
                }
            }
        });
    }

    @Test
    public void testFilteredStorageChangesRequireRecompilationAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table r (id int, d double, s symbol, t timestamp) timestamp(t) partition by day");
            execute("create table p (id int, d double, s symbol)");
            execute("insert into r values (1,1,'a','2020-01-01'), (2,2,'b','2020-01-02'), (3,4,'c','2020-01-03')");
            execute("insert into p values (1,4,'a'), (1,8,'b'), (2,null,null)");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (int jit : new int[]{0, 2}) {
                    context.setJitMode(jit);
                    for (int j = 0; j < 3; j++) {
                        for (boolean keyed : new boolean[]{false, true}) {
                            String sql = "select " + (keyed ? "r.s,p.s, " : "") + AGGREGATES + JOINS[j]
                                    + " where r.d>0 and r.t>='2020-01-01' and r.t<'2020-01-04'"
                                    + (keyed ? " order by r.s,p.s" : "");
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                                String explain = plan(factory, context);
                                Assert.assertTrue(explain, explain.contains("probeFilter:"));
                                Assert.assertTrue(explain, explain.contains("Interval forward scan on: r"));
                                assertAgainstBaseline(sql, factory, context);
                                execute("alter table r convert partition to parquet where t < '2020-01-02'");
                                Assert.assertThrows(TableReferenceOutOfDateException.class, () -> result(factory, context));
                                Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                                Assert.assertNull(context.getMemoryTracker());
                                assertDifferential(sql, context, true);
                                execute("alter table r convert partition to parquet where t >= '2020-01-02'");
                                assertDifferential(sql, context, true);
                                execute("alter table r convert partition to native where t >= 0");
                                assertAgainstBaseline(sql, factory, context);
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testExcludedInternalValueTypes() {
        for (int type : new int[]{ColumnType.LONG128, ColumnType.LONG256, ColumnType.STRING, ColumnType.VARCHAR,
                ColumnType.BINARY, ColumnType.UUID, ColumnType.DECIMAL128, ColumnType.ARRAY, ColumnType.RECORD,
                ColumnType.getGeoHashTypeWithBits(8), ColumnType.getGeoHashTypeWithBits(16),
                ColumnType.getGeoHashTypeWithBits(32), ColumnType.getGeoHashTypeWithBits(60)}) {
            Assert.assertFalse(ColumnType.nameOf(type), HashJoinGroupByCandidate.supportsValueType(type));
        }
    }

    @Test
    public void testSourceInvalidationAndSymbolRebinding() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table r (id int, d double, s symbol)");
            execute("create table p (id int, d double, s symbol)");
            execute("insert into r values (1,1,'r'), (2,2,'s')");
            execute("insert into p values (1,4,'old'), (1,null,null)");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                for (String join : JOINS) {
                    for (boolean keyed : new boolean[]{false, true}) {
                        bindVariableService.setDouble(0, 2);
                        bindVariableService.setStr(1, "old");
                        String sql = "select " + (keyed ? "r.s, p.s, " : "")
                                + "count(*) n, sum(r.d*$1) d" + join + " where p.s=$2 or p.s is null"
                                + (keyed ? " order by r.s,p.s" : "");
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            fused(factory);
                            assertAgainstBaseline(sql, factory, context);
                            execute("truncate table p");
                            execute("insert into p values (1,8,'new'), (2,16,'new'), (2,null,null)");
                            bindVariableService.setDouble(0, 0.5);
                            bindVariableService.setStr(1, "new");
                            assertAgainstBaseline(sql, factory, context);
                            execute("truncate table p");
                            assertAgainstBaseline(sql, factory, context);
                            execute("insert into p values (1,4,'old'), (1,null,null)");
                        }
                    }
                }
                for (String table : new String[]{"r", "p"}) {
                    for (boolean keyed : new boolean[]{false, true}) {
                        String sql = "select " + (keyed ? "r.id, " : "") + AGGREGATES + JOINS[1]
                                + (keyed ? " order by r.id" : "");
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                            assertAgainstBaseline(sql, factory, context);
                            execute("alter table " + table + " add column extra int");
                            Assert.assertThrows(TableReferenceOutOfDateException.class, () -> result(factory, context));
                            Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertNull(context.getMemoryTracker());
                            assertDifferential(sql, context, true);
                            execute("alter table " + table + " drop column extra");
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testUnsupportedAccessAndPayloadsKeepOrdinaryPlans() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table r (id int, d double, s symbol index, t timestamp) timestamp(t) partition by day");
            execute("create table p (id int, d double, s symbol, t timestamp) timestamp(t) partition by day");
            execute("insert into r values (1,1,'a','2020-01-01'), (2,2,'b','2020-01-02')");
            execute("insert into p values (1,4,'a','2020-01-01'), (2,8,'b','2020-01-02')");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                for (String sql : new String[]{
                        "select sum(r.d) from r join p on r.s=p.s",
                        "select sum(r.d) from r join p on r.id::long=p.id::long",
                        "select sum(r.d) from r join p on r.id+1=p.id",
                        "select sum(r.d) from r cross join p",
                        "select sum(r.d) from r asof join p on r.id=p.id",
                        "select sum(r.d) from (r latest on t partition by id) r join p on r.id=p.id",
                        "select sum(r.d) from (r union all r) r join p on r.id=p.id",
                        "select sum(r.d) from (select x::int id, x::double d from long_sequence(2)) r join p on r.id=p.id",
                        "select sum(r.d) from (r limit 1) r join p on r.id=p.id",
                        "select sum(d) from (select distinct r.d from r join p on r.id=p.id)",
                        "select sum(d) from (select r.d from r join p on r.id=p.id limit 1)",
                        "select sum(r.d) from r left join p on r.id=p.id and r.d>0",
                        "select sum(r.d) from r left join p on r.id=p.id where r.d>p.d",
                        "select sum(r.d) from r join p on r.id=p.id where r.s='a'"
                }) {
                    assertDifferential(sql, context, false);
                }
                for (String type : new String[]{"string", "varchar", "binary", "uuid", "long256", "decimal(10,2)", "double[]", "geohash(8b)"}) {
                    execute("alter table p add column unsupported " + type);
                    String value = type.equals("double[]") ? "dim_length(p.unsupported, 1)" : "p.unsupported";
                    assertDifferential("select count(*) from r left join p on r.id=p.id where " + value + " = null", context, false);
                    // An unreferenced unsupported column does not disable otherwise eligible execution.
                    assertDifferential("select " + AGGREGATES + JOINS[1], context, true);
                    execute("alter table p drop column unsupported");
                }
            }
        });
    }

    static void assertDifferential(String sql, SqlExecutionContextImpl context, boolean enabled) throws Exception {
        context.setParallelHashJoinGroupByEnabled(true);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            String actualPlan = plan(factory, context);
            Assert.assertEquals(sql + "\n" + actualPlan, enabled, actualPlan.contains("Async Hash Join Group By"));
            assertAgainstBaseline(sql, factory, context);
        }
    }

    static void assertAgainstBaseline(String sql, RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        String expected;
        try (RecordCursorFactory baseline = context.getCairoEngine().select(sql, context)) {
            String baselinePlan = plan(baseline, context);
            Assert.assertFalse(baselinePlan.contains("Async Hash Join Group By"));
            String candidatePlan = plan(factory, context);
            if (!candidatePlan.contains("Async Hash Join Group By")) {
                Assert.assertEquals(sql, baselinePlan, candidatePlan);
            }
            expected = result(baseline, context);
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
        // Type-annotated output catches metadata mismatches as well as values and SYMBOL routing.
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertEquals(sql, expected, result(factory, context));
        Assert.assertNull(context.getMemoryTracker());
    }

    static SqlExecutionContextImpl context(CairoEngine engine, int workers) {
        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, workers)
                .with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    static AsyncHashJoinGroupByRecordCursorFactory fused(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncHashJoinGroupByRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (AsyncHashJoinGroupByRecordCursorFactory) factory;
    }

    static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    static String result(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            return sink.toString();
        }
    }

    private void createTypedTable(String table, int offset) throws Exception {
        execute("create table " + table + " (id int, t timestamp) timestamp(t) partition by day");
        execute("insert into " + table + " values (0,'2020-01-01'), (null,'2020-01-01T01')");
        String[] names = {"b", "y", "h", "c", "i", "l", "dt", "ns", "f", "d", "s"};
        String[] types = {"boolean", "byte", "short", "char", "int", "long", "date", "timestamp_ns", "float", "double", "symbol"};
        for (int i = 0; i < names.length; i++) {
            execute("alter table " + table + " add column " + names[i] + " " + types[i]);
        }
        execute("insert into " + table + " select ((x+" + offset + ")%3)::int, "
                + "timestamp_sequence('2020-01-02', 3600000000), x%2=0, x::byte, x::short, 'A'::char, "
                + "x::int, x, '2020-01-01'::date, '2020-01-01T00:00:00.000000123'::timestamp_ns, "
                + "(x*0.5)::float, case when x%2=0 then null else x*0.25 end, ('s'||x)::symbol from long_sequence(4)");
    }

    private void insertRandomRows(String table, int rows, int domain, int scenario, Rnd rnd) throws Exception {
        if (rows == 0) {
            return;
        }
        StringBuilder sql = new StringBuilder("insert into ").append(table).append(" values ");
        for (int i = 0; i < rows; i++) {
            if (i > 0) {
                sql.append(',');
            }
            sql.append('(');
            if (rnd.nextInt(7) == 0) {
                sql.append("null");
            } else {
                int key = scenario % 3 == 0 && rnd.nextBoolean() ? 0 : rnd.nextInt(domain) - domain / 2;
                sql.append(key);
            }
            sql.append(',').append(rnd.nextInt(scenario % 2 == 0 ? 3 : 80)).append(',');
            sql.append(rnd.nextInt(5) == 0 ? "null" : Double.toString((rnd.nextInt(65) - 32) * 0.25));
            sql.append(',').append(rnd.nextInt(5) == 0 ? "null" : "'s" + rnd.nextInt(4) + "'").append(')');
        }
        execute(sql.toString());
    }
}
