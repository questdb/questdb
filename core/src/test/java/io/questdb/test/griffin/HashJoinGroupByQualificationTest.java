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
import io.questdb.cairo.SymbolMapReaderImpl;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.pool.ResourcePoolSupervisor;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
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
    public void testHighCardinalitySymbolPredicatesAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            for (String table : new String[]{"r", "p"}) {
                // p inserts in reverse order, so equal SYMBOL text has different keys in r and p.
                String x = table.equals("r") ? "x" : "(8193-x)";
                execute("create table " + table + " as (select " + x + "::int id, ('s'||" + x + ")::symbol s, ('s'||" + x + ")::symbol s2, "
                        + "('2020-01-01T00:00:'||lpad((" + x + "%60)::string,2,'0'))::symbol dt, 1.0 d from long_sequence(8192))");
            }
            try (TableReader r = getReader("r"); TableReader p = getReader("p")) {
                Assert.assertNotEquals(r.getSymbolMapReader(1).keyOf("s1"), p.getSymbolMapReader(1).keyOf("s1"));
            }
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(32, 32);
                for (int threshold : new int[]{1, Integer.MAX_VALUE}) {
                    setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_SHARDING_THRESHOLD, threshold);
                    // Every build row translates a distinct SYMBOL key, and threshold 1 forces the sharded merge.
                    for (String key : new String[]{"id", "s"}) {
                        for (String predicate : new String[]{
                                "r.s like 's%'", "r.s ilike '%S%'", "r.s like 's____'", "r.s ~ '^s[1-8]+'",
                                "r.s = r.s2", "r.dt = '2020-01-01T00:00:01'::timestamp", "r.s = p.s",
                                "p.s like 's%'", "p.s ~ '^s[1-8]+'"}) {
                            String from = " from r left join p on r." + key + "=p." + key
                                    + " and p.s like 's%' and p.s=p.s2 where " + predicate;
                            assertDifferential("select r.s, count(*), sum(p.d)" + from + " order by r.s", context, !predicate.equals("r.s = p.s"));
                            assertDifferential("select count(*), sum(p.d)" + from, context, !predicate.equals("r.s = p.s"));
                        }
                    }
                }
                assertDifferential("select min(r.d) from r join p on r.id=p.id where r.s like 's%'", context, true);
                assertDifferential("select mode(r.d) from r join p on r.id=p.id where r.s like 's%'", context, false);
            }
        });
    }

    @Test
    public void testRandomizedDifferentialMatrix() throws Exception {
        assertMemoryLeak(() -> {
            // Fixed seeds make the generated SQL and data reproducible independently of test order.
            Rnd rnd = new Rnd(130, 9);
            // k holds the text of id, so the SYMBOL key joins the same rows as the INT key does.
            execute("create table r (id int, g int, d double, s symbol, k symbol)");
            execute("create table p (id int, g int, d double, s symbol, k symbol)");
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
                            for (String key : new String[]{"r.id=p.id", "r.k=p.k"}) {
                                String from = JOINS[j].replace("r.id=p.id", key) + on + where;
                                assertDifferential("select " + AGGREGATES + from, context, true);
                                assertDifferential("select " + group + ", " + AGGREGATES + from + " order by " + group, context, true);
                            }
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
            try (TableReader r = getReader("r"); TableReader p = getReader("p")) {
                // The SYMBOL key s holds equal text under different symbol keys in r and p.
                Assert.assertNotEquals(r.getSymbolMapReader(r.getMetadata().getColumnIndex("s")).keyOf("s2"),
                        p.getSymbolMapReader(p.getMetadata().getColumnIndex("s")).keyOf("s2"));
            }
            String columns = "";
            for (String table : new String[]{"r", "p"}) {
                for (String column : new String[]{"b", "y", "h", "c", "i", "l", "dt", "t", "ns", "f", "d", "s"}) {
                    columns += (columns.isEmpty() ? "" : ", ") + table + "." + column;
                }
            }
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 2);
                // The SYMBOL key s has a column top on both sides, so null keys above the tops match.
                for (String key : new String[]{"r.id=p.id", "r.s=p.s"}) {
                    for (String join : JOINS) {
                        for (boolean keyed : new boolean[]{false, true}) {
                            String sql = "select " + (keyed ? columns + ", " : "") + AGGREGATES + join.replace("r.id=p.id", key)
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
                for (String key : new String[]{"r.id=p.id", "r.s=p.s"}) {
                    for (String join : JOINS) {
                        String from = join.replace("r.id=p.id", key) + where;
                        assertDifferential("select " + AGGREGATES + from, context, true);
                        assertDifferential("select " + columns + ", " + AGGREGATES + from + " order by " + columns, context, true);
                    }
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
    public void testCoveringIndexInputsKeepOrdinaryPlans() throws Exception {
        assertMemoryLeak(() -> {
            for (String table : new String[]{"r", "p"}) {
                // r and p write 'a' and 'b' first in different orders, so the texts have different symbol keys.
                String first = table.equals("r") ? "a" : "b";
                String second = table.equals("r") ? "b" : "a";
                execute("create table " + table + " (id int, s symbol index type posting include (id,d), d double, ts timestamp) timestamp(ts) partition by day bypass wal");
                execute("insert into " + table + " select x::int, case when x%4=1 then '" + first + "' else '" + second + "' end, "
                        + "x*0.5, x::timestamp from long_sequence(128)");
            }
            engine.releaseAllWriters();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 1);
                for (String key : new String[]{"r.id=p.id", "r.s=p.s"}) {
                    for (boolean isKeyed : new boolean[]{false, true}) {
                        String select = "select " + (isKeyed ? "r.id, " : "") + "sum(r.d), count(*) from r join p on " + key;
                        for (String source : new String[]{"r", "p"}) {
                            String sql = select + " where " + source + ".s='a'";
                            context.setParallelHashJoinGroupByEnabled(false);
                            try (RecordCursorFactory baseline = engine.select(sql, context)) {
                                Assert.assertTrue(plan(baseline, context), plan(baseline, context).contains("CoveringIndex"));
                            }
                            assertDifferential(sql, context, false);
                        }
                        // Without a predicate on the indexed column, the covered key column is an ordinary input.
                        assertDifferential(select + (isKeyed ? " order by r.id" : ""), context, true);
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
            // k holds the text of id, so the SYMBOL key joins the same rows as the INT key does.
            execute("create table r (id int, d double, s symbol, k symbol)");
            execute("create table p (id int, d double, s symbol, k symbol)");
            execute("insert into r values (1,1,'r','1'), (2,2,'s','2'), (null,4,'t',null)");
            execute("insert into p values (1,4,'old','1'), (1,null,null,'1'), (null,32,'old',null)");
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
                for (String key : new String[]{"r.id=p.id", "r.k=p.k"}) {
                    for (String join : JOINS) {
                        for (boolean keyed : new boolean[]{false, true}) {
                            bindVariableService.setDouble(0, 2);
                            bindVariableService.setStr(1, "old");
                            String sql = "select " + (keyed ? "r.s, p.s, " : "")
                                    + "count(*) n, sum(r.d*$1) d" + join.replace("r.id=p.id", key) + " where p.s=$2 or p.s is null"
                                    + (keyed ? " order by r.s,p.s" : "");
                            try (RecordCursorFactory factory = engine.select(sql, context)) {
                                fused(factory);
                                assertAgainstBaseline(sql, factory, context);
                                // TRUNCATE drops the symbol maps, and the new rows assign the key texts in reverse order.
                                execute("truncate table p");
                                execute("insert into p values (2,16,'new','2'), (1,8,'new','1'), (2,null,null,'2'), (3,64,'new','3')");
                                bindVariableService.setDouble(0, 0.5);
                                bindVariableService.setStr(1, "new");
                                assertAgainstBaseline(sql, factory, context);
                                execute("truncate table p");
                                assertAgainstBaseline(sql, factory, context);
                                execute("insert into p values (1,4,'old','1'), (1,null,null,'1'), (null,32,'old',null)");
                            }
                        }
                    }
                }
                for (String key : new String[]{"r.id=p.id", "r.k=p.k"}) {
                    for (String table : new String[]{"r", "p"}) {
                        for (boolean keyed : new boolean[]{false, true}) {
                            String sql = "select " + (keyed ? "r.id, " : "") + AGGREGATES + JOINS[1].replace("r.id=p.id", key)
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
                // A SYMBOL key changed to VARCHAR on either side invalidates the factory. The
                // recompiled query stages the reconciled VARCHAR key until the key is SYMBOL
                // again and the build translates it once per distinct key.
                for (String table : new String[]{"r", "p"}) {
                    for (boolean keyed : new boolean[]{false, true}) {
                        String sql = "select " + (keyed ? "p.s, " : "") + AGGREGATES + JOINS[1].replace("r.id=p.id", "r.k=p.k")
                                + (keyed ? " order by p.s" : "");
                        try (RecordCursorFactory factory = engine.select(sql, context)) {
                            AsyncHashJoinGroupByRecordCursorFactory fused = fused(factory);
                            assertAgainstBaseline(sql, factory, context);
                            execute("alter table " + table + " alter column k type varchar");
                            Assert.assertThrows(TableReferenceOutOfDateException.class, () -> result(factory, context));
                            Assert.assertEquals(0, fused.getAtom().getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertNull(context.getMemoryTracker());
                            assertDifferential(sql, context, true);
                            execute("alter table " + table + " alter column k type symbol");
                            Assert.assertThrows(TableReferenceOutOfDateException.class, () -> result(factory, context));
                            assertDifferential(sql, context, true);
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
                // SYMBOL keys are eligible, also when the probe key column is indexed.
                assertDifferential("select sum(r.d) from r join p on r.s=p.s", context, true);
                assertDifferential("select p.s, sum(r.d) from r join p on r.s=p.s order by p.s", context, true);
                for (String sql : new String[]{
                        "select sum(r.d) from r join p on r.s=p.s where r.s='a'",
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

    @Test
    public void testWalDedupTableWithCoveringIndexedSymbolKey() throws Exception {
        assertMemoryLeak(() -> {
            // r is a WAL fact table with DEDUP on the timestamp and the SYMBOL key, and the key
            // carries a posting index that covers the value columns.
            execute("create table r (k symbol index type posting include (id,d), id int, d double, ts timestamp) "
                    + "timestamp(ts) partition by day wal dedup upsert keys(ts,k)");
            execute("create table p (k symbol, id int, s symbol, d double)");
            // p writes the key texts in a different order, so equal text has different symbol keys.
            execute("insert into p select ('k'||(8-x%9))::symbol, (8-x%9)::int, ('s'||(x%3))::symbol, x*0.5 from long_sequence(12)");
            execute("insert into r select ('k'||((x-1)%7))::symbol, ((x-1)%7)::int, x*0.25, "
                    + "timestamp_sequence('2020-01-01', 3_600_000_000) from long_sequence(96)");
            drainWalQueue();
            ObjList<String> queries = new ObjList<>();
            for (String key : new String[]{"r.id=p.id", "r.k=p.k"}) {
                for (String join : JOINS) {
                    String from = join.replace("r.id=p.id", key);
                    queries.add("select count(*) n, sum(r.d) rd, sum(p.d) pd, count(r.k) rk, count(p.k) pk" + from);
                    queries.add("select p.s, r.k, count(*) n, sum(r.d) rd, sum(p.d) pd" + from + " order by p.s, r.k");
                }
            }
            ObjList<RecordCursorFactory> factories = new ObjList<>();
            try (SqlExecutionContextImpl context = context(engine, 4)) {
                context.changePageFrameSizes(1, 8);
                for (int i = 0; i < queries.size(); i++) {
                    factories.add(engine.select(queries.getQuick(i), context));
                    fused(factories.getLast());
                }
                assertAgainstBaseline(queries, factories, context);
                // Every other row has the (ts, k) of a committed row and replaces it.
                execute("insert into r select ('k'||((2*(x-1))%7))::symbol, ((2*(x-1))%7)::int, -x*1.0, "
                        + "timestamp_sequence('2020-01-01', 7_200_000_000) from long_sequence(48)");
                drainWalQueue();
                assertAgainstBaseline(queries, factories, context);
                // Out-of-order rows replace a row, add a key text only r has, add a key text p has
                // but r lacked, and add a null key. p adds a key text and a null key as well.
                execute("insert into r values ('k0',0,300.0,'2020-01-01'), ('k9',9,200.0,'2020-01-02T01:00'), "
                        + "('k8',8,100.0,'2019-12-31T12:00'), (null,null,50.0,'2020-01-03T00:30')");
                execute("insert into p values ('k10',10,'s9',1.0), (null,null,'s0',2.0)");
                drainWalQueue();
                assertQuery("select count(*) n, count_distinct(k) keys, sum(d) d from r")
                        .withContext(context)
                        .noLeakCheck()
                        .expectSize()
                        .noRandomAccess()
                        .returns("""
                                n\tkeys\td
                                99\t9\t63.0
                                """);
                assertAgainstBaseline(queries, factories, context);
                // A predicate on the indexed key selects the covering index and keeps the ordinary plan.
                for (String key : new String[]{"r.id=p.id", "r.k=p.k"}) {
                    for (String predicate : new String[]{"r.k='k1'", "r.k in ('k1','k9')"}) {
                        String sql = "select sum(r.d), count(*) from r join p on " + key + " where " + predicate;
                        context.setParallelHashJoinGroupByEnabled(false);
                        try (RecordCursorFactory baseline = engine.select(sql, context)) {
                            Assert.assertTrue(plan(baseline, context), plan(baseline, context).contains("CoveringIndex"));
                        }
                        assertDifferential(sql, context, false);
                    }
                    // A predicate on a covered value column does not select the index.
                    assertDifferential("select sum(r.d), count(*) from r join p on " + key + " where r.d > 0", context, true);
                }
            } finally {
                Misc.freeObjList(factories);
            }
        });
    }

    static void assertAgainstBaseline(ObjList<String> queries, ObjList<RecordCursorFactory> factories, SqlExecutionContextImpl context) throws Exception {
        for (int i = 0; i < queries.size(); i++) {
            assertAgainstBaseline(queries.getQuick(i), factories.getQuick(i), context);
        }
    }

    static void assertDifferential(String sql, SqlExecutionContextImpl context, boolean enabled) throws Exception {
        context.setParallelHashJoinGroupByEnabled(true);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            String actualPlan = plan(factory, context);
            Assert.assertEquals(sql + "\n" + actualPlan, enabled, actualPlan.contains("Hash Join Group By"));
            assertAgainstBaseline(sql, factory, context);
        }
    }

    static void assertAgainstBaseline(String sql, RecordCursorFactory factory, SqlExecutionContextImpl context) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        String expected;
        try (RecordCursorFactory baseline = context.getCairoEngine().select(sql, context)) {
            String baselinePlan = plan(baseline, context);
            Assert.assertFalse(baselinePlan.contains("Hash Join Group By"));
            String candidatePlan = plan(factory, context);
            if (!candidatePlan.contains("Hash Join Group By")) {
                Assert.assertEquals(sql, baselinePlan, candidatePlan);
            }
            Assert.assertEquals(sql, baseline.getMetadata().getColumnCount(), factory.getMetadata().getColumnCount());
            for (int column = 0; column < baseline.getMetadata().getColumnCount(); column++) {
                Assert.assertEquals(sql, baseline.getMetadata().getColumnType(column), factory.getMetadata().getColumnType(column));
                Assert.assertEquals(sql, baseline.getMetadata().getColumnName(column), factory.getMetadata().getColumnName(column));
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
                + "(x*0.5)::float, case when x%2=0 then null else x*0.25 end, ('s'||(x+" + offset + "))::symbol from long_sequence(4)");
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
            String key = "null";
            if (rnd.nextInt(7) != 0) {
                key = Integer.toString(scenario % 3 == 0 && rnd.nextBoolean() ? 0 : rnd.nextInt(domain) - domain / 2);
            }
            sql.append(key);
            sql.append(',').append(rnd.nextInt(scenario % 2 == 0 ? 3 : 80)).append(',');
            sql.append(rnd.nextInt(5) == 0 ? "null" : Double.toString((rnd.nextInt(65) - 32) * 0.25));
            sql.append(',').append(rnd.nextInt(5) == 0 ? "null" : "'s" + rnd.nextInt(4) + "'");
            sql.append(',').append(key.equals("null") ? "null" : "'" + key + "'").append(')');
        }
        execute(sql.toString());
    }
}
