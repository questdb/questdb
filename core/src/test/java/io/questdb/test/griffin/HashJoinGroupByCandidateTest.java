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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.LoopingRecordSink;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class HashJoinGroupByCandidateTest extends AbstractCairoTest {
    private static final SingleColumnType BUILD_ROW_COUNT_TYPE = new SingleColumnType(ColumnType.LONG);
    private static final String JOIN = " from r join p on r.plant_id=p.plant_id";

    @Test
    public void testAggregateImplementations() throws Exception {
        assertMemoryLeak(() -> {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("i", ColumnType.INT));
            metadata.add(new TableColumnMetadata("l", ColumnType.LONG));
            metadata.add(new TableColumnMetadata("d", ColumnType.DOUBLE));
            metadata.add(new TableColumnMetadata("s", ColumnType.SYMBOL, IndexType.NONE, 0, true, null));
            FunctionParser parser = new FunctionParser(configuration, engine.getFunctionFactoryCache());
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                for (String expression : new String[]{"sum(d)", "avg(d)", "count()", "count(i)", "count(l)", "count(d)", "count(s)",
                        "sum(coalesce(d, 0.0))", "sum(i)", "avg(l)", "min(d)", "ksum(d)", "stddev(d)", "corr(d, l)", "weighted_avg(i, l)"}) {
                    try (Function function = parser.parseFunction(compiler.testParseExpression(expression, QueryModel.FACTORY.newInstance()), metadata, sqlExecutionContext)) {
                        Assert.assertTrue(expression, HashJoinGroupByCandidate.supportsAggregate(function));
                    }
                }
                for (String expression : new String[]{"first(d)", "last(d)", "count_distinct(i)", "sum(rnd_double())", "mode(d)",
                        "arg_min(d, l)", "corr(d, rnd_double())", "count(i::short)"}) {
                    try (Function function = parser.parseFunction(compiler.testParseExpression(expression, QueryModel.FACTORY.newInstance()), metadata, sqlExecutionContext)) {
                        Assert.assertFalse(expression, HashJoinGroupByCandidate.supportsAggregate(function));
                    }
                }
            }
        });
    }

    @Test
    public void testBuildFilterOwnership() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                HashJoinGroupByCandidate candidate = candidate(compiler,
                        "select r.plant_id, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id and p.installed_kwp > 0");
                Assert.assertNotNull(candidate);
                Assert.assertNotNull(candidate.getBuildOnFilter());
                Assert.assertEquals(0, candidate.getRequiredBuildColumns().size());
                candidate = candidate(compiler,
                        "select r.plant_id, sum(r.energy_kwh) from p right join r on r.plant_id=p.plant_id and p.installed_kwp > 0");
                Assert.assertNotNull(candidate);
                Assert.assertNotNull(candidate.getBuildOnFilter());
                Assert.assertTrue(candidate.isInputSwapped());
                Assert.assertEquals(0, candidate.getRequiredBuildColumns().size());
            }
            assertCandidate("select p.country, sum(r.energy_kwh) from p right join r on r.plant_id=p.plant_id and r.energy_kwh > 0", false);
            assertCandidate("select p.country, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id and (p.installed_kwp > 0 or r.energy_kwh > 0)", false);
            assertCandidate("select p.country, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id where p.installed_kwp > 0 or r.energy_kwh > 0", false);
        });
    }

    @Test
    public void testCompiledArgumentContracts() throws Exception {
        assertMemoryLeak(() -> {
            try (Function aggregate = new SumDoubleGroupByFunction(new DoubleFunction() {
                @Override
                public double getDouble(Record record) {
                    return 0;
                }

                @Override
                public boolean supportsParallelism() {
                    return false;
                }
            })) {
                Assert.assertFalse(HashJoinGroupByCandidate.supportsAggregate(aggregate));
            }
            try (Function aggregate = new SumDoubleGroupByFunction(DoubleColumn.newInstance(0)) { }) {
                Assert.assertFalse(HashJoinGroupByCandidate.supportsAggregate(aggregate));
            }
        });
    }

    @Test
    public void testCompiledKeyColumns() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            String sql = "SELECT count(*) FROM ka LEFT JOIN kb ON ka.sym=kb.sym AND ka.l=kb.l";
            try (SqlCompiler compiler = engine.getSqlCompiler();
                 RecordCursorFactory probeFactory = select("SELECT sym, l FROM ka");
                 RecordCursorFactory buildFactory = select("SELECT l, sym FROM kb")) {
                HashJoinGroupByCandidate candidate = candidate(compiler, sql);
                Assert.assertNotNull(candidate);
                // The analysis addresses base tables; the metadata compiles the keys against the inputs.
                try (HashJoinGroupByMetadata metadata = new HashJoinGroupByMetadata(configuration, new BytecodeAssembler(), candidate,
                        probeFactory.getMetadata(), ints(16, 1), buildFactory.getMetadata(), ints(1, 16))) {
                    Assert.assertEquals("[1,0]", metadata.getProbeKeyColumns().toString());
                    Assert.assertEquals("[0,1]", metadata.getBuildKeyColumns().toString());
                    Assert.assertEquals("ka.l=kb.l and ka.sym=kb.sym", metadata.getCondition());
                    Assert.assertTrue(metadata.hasStaticSymbolTables());
                    Assert.assertFalse(metadata.isSymbolKey());
                }
            }
        });
    }

    @Test
    public void testCompositeKeys() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            // Keys follow the join context, whose order is the optimiser's, not the statement's.
            assertKeys("ka.i=kb.i AND ka.l=kb.l", "1=1:LONG 0=0:INT", false);
            assertKeys("ka.i=kb.i AND ka.l=kb.l AND ka.c=kb.c", "4=4:CHAR 1=1:LONG 0=0:INT", false);
            assertKeys("ka.dt=kb.dt AND ka.u=kb.u", "12=12:UUID 8=8:DATE", false);
            // A lone SYMBOL pair keeps the INT layout; inside a composite key both sides write text.
            assertKeys("ka.sym=kb.sym", "16=16:SYMBOL", true);
            assertKeys("ka.i=kb.i AND ka.sym=kb.sym", "16=16:STRING 0=0:INT", false);
            assertKeys("ka.str=kb.vc AND ka.ts=kb.tn", "9=10:TIMESTAMP_NS 17=18:VARCHAR", false);
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                HashJoinGroupByCandidate candidate = candidate(compiler,
                        "SELECT count(*) FROM ka LEFT JOIN kb ON ka.str=kb.vc AND ka.ts=kb.tn");
                Assert.assertNotNull(candidate);
                HashJoinGroupByKeys keys = candidate.getKeys();
                Assert.assertTrue(keys.isProbeTimestampAsNanos(0));
                Assert.assertFalse(keys.isBuildTimestampAsNanos(0));
                Assert.assertTrue(keys.isProbeStringAsVarchar(1));
                Assert.assertFalse(keys.isBuildStringAsVarchar(1));
            }
        });
    }

    @Test
    public void testDefaultResultsAndOuterPredicatePlacement() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r values (1, '2020-01-01', 10, 100, 1, 'a'), (2, '2020-01-02', 20, 200, 2, 'b'), (3, '2020-01-03', 30, 300, 3, 'c'), (null, '2020-01-04', 40, 400, null, null)");
            execute("insert into p values (1, 'ES', 5, 1, 'a'), (1, 'IT', 7, 1, 'a'), (2, 'DE', 9, 2, 'b'), (null, null, null, null, null)");
            // The keyed group-by and its sort support random access; the scalar aggregates do not.
            assertSql("country\tsum\nES\t10.0\nIT\t10.0\n", "select p.country, sum(r.energy_kwh)" + JOIN + " where p.country in ('ES','IT') order by p.country", true);
            String outer = " from r left join p on r.plant_id=p.plant_id";
            assertSql("count\tcount1\tsum\n2\t0\t70.0\n", "select count(*), count(p.plant_id), sum(r.energy_kwh)" + outer + " where p.installed_kwp is null", false);
            assertSql("count\n0\n", "select count(*)" + outer + " where p.installed_kwp = 42", false);
            assertSql("count\n5\n", "select count(*) from r left join p on r.plant_id=p.plant_id and p.country in ('ES','IT')", false);
            assertSql("country\tfirst\tlast\n\t40.0\t40.0\nDE\t20.0\t20.0\nES\t10.0\t10.0\nIT\t10.0\t10.0\n", "select p.country, first(r.energy_kwh), last(r.energy_kwh)" + JOIN + " order by p.country", true);
            // Eligible shapes select the fused operator by default; order-sensitive aggregates keep the ordinary join.
            assertPlanContains("select p.country, sum(r.energy_kwh)" + JOIN, "physicalJoinType: inner", true);
            assertPlanContains("select p.country, sum(r.energy_kwh)" + outer, "physicalJoinType: left outer", true);
            assertPlanContains("select p.country, first(r.energy_kwh)" + JOIN, "Hash Join Light", false);
        });
    }

    @Test
    public void testGeneralKeyTypes() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            // Every pair the ordinary hash join reconciles becomes a key; only a lone INT pair and
            // a lone SYMBOL pair take the INT layout, and the rest keep the ordinary plan for now.
            assertKeys("ka.i=kb.i", "0=0:INT", true);
            assertKeys("ka.sym=kb.sym", "16=16:SYMBOL", true);
            assertKeys("ka.l=kb.l", "1=1:LONG", false);
            assertKeys("ka.s=kb.s", "2=2:SHORT", false);
            assertKeys("ka.b=kb.b", "3=3:BYTE", false);
            assertKeys("ka.c=kb.c", "4=4:CHAR", false);
            assertKeys("ka.bo=kb.bo", "5=5:BOOLEAN", false);
            assertKeys("ka.f=kb.f", "6=6:FLOAT", false);
            assertKeys("ka.d=kb.d", "7=7:DOUBLE", false);
            assertKeys("ka.dt=kb.dt", "8=8:DATE", false);
            assertKeys("ka.ts=kb.ts", "9=9:TIMESTAMP", false);
            assertKeys("ka.tn=kb.tn", "10=10:TIMESTAMP_NS", false);
            assertKeys("ka.ts=kb.tn", "9=10:TIMESTAMP_NS", false);
            assertKeys("ka.tn=kb.ts", "10=9:TIMESTAMP_NS", false);
            assertKeys("ka.ip=kb.ip", "11=11:IPv4", false);
            assertKeys("ka.u=kb.u", "12=12:UUID", false);
            assertKeys("ka.l256=kb.l256", "13=13:LONG256", false);
            assertKeys("ka.g=kb.g", "14=14:GEOHASH(8c)", false);
            assertKeys("ka.dec=kb.dec", "15=15:DECIMAL(10,2)", false);
            assertKeys("ka.str=kb.str", "17=17:STRING", false);
            assertKeys("ka.vc=kb.vc", "18=18:VARCHAR", false);
            assertKeys("ka.sym=kb.str", "16=17:STRING", false);
            assertKeys("ka.sym=kb.vc", "16=18:VARCHAR", false);
            assertKeys("ka.str=kb.vc", "17=18:VARCHAR", false);
            assertKeys("ka.vc=kb.str", "18=17:VARCHAR", false);
        });
    }

    @Test
    public void testInnerAndOriginalExpressions() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCandidate("select p.country, year(r.reading_ts) yr, month(r.reading_ts) mo, sum(r.energy_kwh), avg(r.irradiance_wm2), sum(r.energy_kwh)/nullif(sum(p.installed_kwp), 0)" + JOIN
                    + " where r.reading_ts >= '2020-01-01' and r.reading_ts < '2025-01-01' and p.country in ('ES','IT') group by p.country, year(r.reading_ts), month(r.reading_ts) order by p.country, yr, mo limit 120", true);
            assertCandidate("select count(*), count(p.plant_id), sum(r.energy_kwh)" + JOIN, true);
            assertCandidate("select r.plant_id, p.country, sum(r.energy_kwh)" + JOIN, true);
            assertCandidate("select p.country, sum(coalesce(r.energy_kwh, 0.0))" + JOIN, true);
        });
    }

    @Test
    public void testOuterAndNormalizedRight() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCandidate("select p.country, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id", true);
            assertCandidate("select p.country, sum(r.energy_kwh) from p right join r on r.plant_id=p.plant_id", true);
            assertCandidate("select p.country, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id where p.installed_kwp is null", true);
            assertCandidate("select p.country, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id and p.country in ('ES','IT')", true);
            assertCandidate("select p.country, sum(r.energy_kwh) from r left join p on r.plant_id=p.plant_id and r.energy_kwh > 0", false);
            assertCandidate("select p.country, sum(r.energy_kwh) from r full join p on r.plant_id=p.plant_id", false);
        });
    }

    @Test
    public void testPayloadPruningAndOrientation() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                HashJoinGroupByCandidate candidate = candidate(compiler,
                        "select p.country, sum(r.energy_kwh) from p right join r on r.plant_id=p.plant_id where p.installed_kwp is null");
                Assert.assertNotNull(candidate);
                Assert.assertTrue(candidate.isInputSwapped());
                Assert.assertEquals(IQueryModel.JOIN_RIGHT_OUTER, candidate.getLogicalJoinType());
                Assert.assertEquals(IQueryModel.JOIN_LEFT_OUTER, candidate.getPhysicalJoinType());
                Assert.assertEquals("r", candidate.getProbeModel().getTableName().toString());
                Assert.assertEquals("p", candidate.getBuildModel().getTableName().toString());
                Assert.assertEquals(0, candidate.getKeys().getProbeColumn(0));
                Assert.assertEquals(0, candidate.getKeys().getBuildColumn(0));
                Assert.assertEquals("[1,2]", candidate.getRequiredBuildColumns().toString());
                // An INNER join builds the smaller table: r is empty and p has one row, so r builds
                // in either order and the payload holds only r's column.
                execute("INSERT INTO p VALUES (1, 'ES', 5, 1, 'a')");
                for (String join : new String[]{"r join p", "p join r"}) {
                    candidate = candidate(compiler, "select p.country, sum(r.energy_kwh) from " + join + " on r.plant_id=p.plant_id");
                    Assert.assertNotNull(join, candidate);
                    Assert.assertEquals(join, join.equals("r join p"), candidate.isInputSwapped());
                    Assert.assertEquals(IQueryModel.JOIN_INNER, candidate.getLogicalJoinType());
                    Assert.assertEquals(IQueryModel.JOIN_INNER, candidate.getPhysicalJoinType());
                    Assert.assertEquals("p", candidate.getProbeModel().getTableName().toString());
                    Assert.assertEquals("r", candidate.getBuildModel().getTableName().toString());
                    Assert.assertEquals(0, candidate.getKeys().getProbeColumn(0));
                    Assert.assertEquals(0, candidate.getKeys().getBuildColumn(0));
                    Assert.assertEquals("[2]", candidate.getRequiredBuildColumns().toString());
                }
            }
        });
    }

    @Test
    public void testProbeFactories() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{"r", "select energy_kwh, plant_id from r", "r where energy_kwh > 0", "r where reading_ts >= '2020-01-01'"}) {
                try (RecordCursorFactory factory = select(sql)) {
                    // QueryProgress is the public compiler wrapper; it owns the tested factory.
                    Assert.assertTrue(sql, HashJoinGroupByCandidate.supportsProbeFactory(factory.getBaseFactory()));
                }
            }
            try (RecordCursorFactory factory = select("select * from long_sequence(5)")) {
                Assert.assertFalse(HashJoinGroupByCandidate.supportsProbeFactory(factory.getBaseFactory()));
            }
        });
    }

    @Test
    public void testProbeFactoryReuseAfterPartitionConversion() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r values (1, '2020-01-01', 10, 100, 1, 'a')");
            try (RecordCursorFactory factory = select("select energy_kwh from r")) {
                for (int pass = 0; pass < 2; pass++) {
                    Assert.assertTrue(HashJoinGroupByCandidate.supportsProbeFactory(factory.getBaseFactory()));
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(10.0, cursor.getRecord().getDouble(0), 0);
                        Assert.assertFalse(cursor.hasNext());
                    }
                    assertCandidate("select p.country, sum(r.energy_kwh)" + JOIN, true);
                    if (pass == 0) {
                        execute("alter table r convert partition to parquet where reading_ts >= 0");
                    }
                }
            }
        });
    }

    @Test
    public void testProjectionAliases() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            assertCandidate("select country, sum(e) from (select p.country country, r.energy_kwh e" + JOIN + ")", true);
            assertCandidate("select c, sum(e) from (select p.country c, r.energy_kwh e" + JOIN + ") where e > 0", true);
            assertCandidate("select p.c, sum(r.e) from (select energy_kwh e, plant_id id from r) r join (select country c, plant_id id from p) p on r.id=p.id", true);
        });
    }

    @Test
    public void testRebindingAndPayloadTypes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.setDouble(0, 1.0);
            String sql = "select p.country, sum(r.energy_kwh)" + JOIN + " where r.energy_kwh > $1";
            assertCandidate(sql, true);
            bindVariableService.setDouble(0, 2.0);
            assertCandidate(sql, true);
            execute("alter table p add column unsupported varchar");
            assertCandidate("select p.country, sum(r.energy_kwh)" + JOIN, true);
            assertCandidate("select p.unsupported, sum(r.energy_kwh)" + JOIN, false);
            // Key type changes rebind the analysis: INT to SYMBOL on both sides and back.
            execute("alter table p alter column plant_id type symbol");
            assertMismatchedKeys("select p.country, sum(r.energy_kwh)" + JOIN);
            execute("alter table r alter column plant_id type symbol");
            assertCandidate("select p.country, sum(r.energy_kwh)" + JOIN, true);
            Assert.assertTrue(isSymbolKey("select p.country, sum(r.energy_kwh)" + JOIN));
            execute("alter table p alter column plant_id type int");
            execute("alter table r alter column plant_id type int");
            assertCandidate("select p.country, sum(r.energy_kwh)" + JOIN, true);
            Assert.assertFalse(isSymbolKey("select p.country, sum(r.energy_kwh)" + JOIN));
            execute("alter table p alter column plant_id type long");
            execute("alter table r alter column plant_id type long");
            assertCandidate("select p.country, sum(r.energy_kwh)" + JOIN, true);
            Assert.assertFalse(isIntKeyed("select p.country, sum(r.energy_kwh)" + JOIN));
        });
    }

    @Test
    public void testRejectedShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{
                    "select p.country, sum(r.energy_kwh) from r join p on r.plant_id+1=p.plant_id",
                    "select p.country, sum(r.energy_kwh)" + JOIN + " where r.energy_kwh > p.installed_kwp",
                    "select p.country, sum(r.energy_kwh)" + JOIN + " join p p2 on r.plant_id=p2.plant_id",
                    "select country, sum(e) from (select p.country country, r.energy_kwh e" + JOIN + " limit 3)",
                    "select country, sum(e) from (select distinct p.country country, r.energy_kwh e" + JOIN + ")",
                    "select p.country, first(r.energy_kwh)" + JOIN,
                    "select p.country, last(r.energy_kwh)" + JOIN,
                    "select p.country, mode(r.energy_kwh)" + JOIN,
                    "select p.country, arg_min(r.energy_kwh, r.plant_id)" + JOIN,
                    "select p.country, count_distinct(r.plant_id)" + JOIN,
                    "select p.country, sum(rnd_double())" + JOIN
            }) {
                assertCandidate(sql, false);
            }
        });
    }

    @Test
    public void testSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("alter table r add column str_key string");
            execute("alter table r add column vc_key varchar");
            execute("alter table p add column str_key string");
            execute("alter table p add column vc_key varchar");
            for (String sql : new String[]{
                    "select p.country, sum(r.energy_kwh) from r join p on r.sym_key=p.sym_key",
                    "select p.country, sum(r.energy_kwh) from r left join p on r.sym_key=p.sym_key",
                    "select p.country, sum(r.energy_kwh) from p right join r on r.sym_key=p.sym_key",
                    "select p.sym_key, count(*), count(p.sym_key) from r join p on p.sym_key=r.sym_key where p.sym_key in ('a','b')",
                    "select count(*) from r left join p on r.sym_key=p.sym_key and p.country = 'ES' where r.sym_key is null"
            }) {
                assertCandidate(sql, true);
                Assert.assertTrue(sql, isSymbolKey(sql));
            }
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                // A RIGHT join builds the SQL left table, so the probe key is r's column.
                HashJoinGroupByCandidate candidate = candidate(compiler,
                        "select p.country, sum(r.energy_kwh) from p right join r on r.sym_key=p.sym_key");
                Assert.assertNotNull(candidate);
                Assert.assertTrue(candidate.isInputSwapped());
                Assert.assertEquals(5, candidate.getKeys().getProbeColumn(0));
                Assert.assertEquals(4, candidate.getKeys().getBuildColumn(0));
            }
            Assert.assertFalse(isSymbolKey("select p.country, sum(r.energy_kwh)" + JOIN));
            assertPlanContains("select p.country, sum(r.energy_kwh) from r join p on r.sym_key=p.sym_key", "symbolKeyJoin: true", true);
            try (RecordCursorFactory factory = select("explain select p.country, sum(r.energy_kwh)" + JOIN);
                 RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                while (cursor.hasNext()) {
                    Assert.assertFalse(Chars.contains(cursor.getRecord().getStrA(0), "symbolKeyJoin"));
                }
            }
            // Text and mixed-type keys reconcile into a staged key, which keeps the ordinary plan
            // until the code generator wires the key sinks.
            for (String on : new String[]{
                    "r.sym_key=p.str_key", "r.str_key=p.sym_key", "r.sym_key=p.vc_key", "r.vc_key=p.sym_key",
                    "r.str_key=p.str_key", "r.vc_key=p.vc_key"
            }) {
                assertCandidate("select p.country, sum(r.energy_kwh) from r join p on " + on, true);
                Assert.assertFalse(on, isIntKeyed("select p.country, sum(r.energy_kwh) from r join p on " + on));
                assertPlanContains("select p.country, sum(r.energy_kwh) from r join p on " + on, "Hash Join", false);
            }
            // SYMBOL against INT fails the ordinary compile as before.
            for (String on : new String[]{"r.sym_key=p.plant_id", "r.plant_id=p.sym_key"}) {
                assertMismatchedKeys("select p.country, sum(r.energy_kwh) from r join p on " + on);
            }
        });
    }

    @Test
    public void testKeySinkLayout() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            // The map stores the reconciled type of each key, in the join context's order.
            assertKeySinkLayout("ka.l=kb.l", "LONG");
            assertKeySinkLayout("ka.i=kb.i AND ka.l=kb.l", "LONG,INT");
            assertKeySinkLayout("ka.str=kb.vc AND ka.ts=kb.tn", "TIMESTAMP_NS,VARCHAR");
            assertKeySinkLayout("ka.i=kb.i AND ka.sym=kb.sym", "STRING,INT");
            assertKeySinkLayout("ka.g=kb.g AND ka.dec=kb.dec AND ka.u=kb.u", "UUID,DECIMAL(10,2),GEOHASH(8c)");
            // The INT layout reads its key off the record and stages nothing, so it has no sinks.
            for (String on : new String[]{"ka.i=kb.i", "ka.sym=kb.sym"}) {
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory probeFactory = select("SELECT * FROM ka");
                     RecordCursorFactory buildFactory = select("SELECT * FROM kb")) {
                    try (HashJoinGroupByMetadata metadata = keyMetadata(compiler, on, probeFactory, buildFactory, probeFactory, buildFactory)) {
                        Assert.assertFalse(on, metadata.isKeyStaged());
                        Assert.assertEquals(on, 0, metadata.getKeyTypes().getColumnCount());
                        Assert.assertNull(on, metadata.newProbeKeySink());
                        Assert.assertNull(on, metadata.newBuildKeySink());
                    }
                }
            }
        });
    }

    @Test
    public void testKeySinksMatchOrdinaryJoin() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            insertKeyRows();
            // Every reconciled pair, staged through the two generated sinks, has to select the
            // same rows the ordinary hash join selects, NULL keys and empty results included.
            for (String on : new String[]{
                    "ka.l=kb.l", "ka.s=kb.s", "ka.b=kb.b", "ka.c=kb.c", "ka.bo=kb.bo",
                    "ka.f=kb.f", "ka.d=kb.d", "ka.dt=kb.dt", "ka.ts=kb.ts", "ka.tn=kb.tn",
                    "ka.ip=kb.ip", "ka.u=kb.u", "ka.l256=kb.l256", "ka.g=kb.g", "ka.dec=kb.dec",
                    "ka.str=kb.str", "ka.vc=kb.vc",
                    // Pairs that reconcile to a third type, so the two sides encode differently.
                    "ka.ts=kb.tn", "ka.tn=kb.ts", "ka.str=kb.vc", "ka.vc=kb.str",
                    "ka.sym=kb.str", "ka.str=kb.sym", "ka.sym=kb.vc", "ka.vc=kb.sym",
                    // Composites, including a SYMBOL pair that compares as text until 2c.
                    "ka.i=kb.i AND ka.l=kb.l",
                    "ka.i=kb.i AND ka.sym=kb.sym",
                    "ka.sym=kb.sym AND ka.str=kb.vc AND ka.l=kb.l AND ka.ts=kb.tn",
                    "ka.vc=kb.str AND ka.dt=kb.dt AND ka.c=kb.c",
                    "ka.l=kb.l AND ka.g=kb.g",
                    // One input's STRING key writes VARCHAR while the other's writes STRING, so
                    // the two inputs must not share one set of per-column encoding flags.
                    "ka.str=kb.vc AND ka.sym=kb.str",
                    "ka.ts=kb.tn AND ka.tn=kb.ts",
                    // A column both sides leave entirely NULL, so every key is NULL.
                    "ka.v=kb.v",
            }) {
                assertKeySinksMatchOrdinaryJoin(on);
            }
            // Projections that put the key columns at different indexes on the two sides, which
            // the sinks address through the compiled indexes rather than the base-table ones.
            assertKeySinksMatchOrdinaryJoin("ka.sym=kb.sym AND ka.l=kb.l",
                    "SELECT v, sym, l FROM ka", "SELECT l, i, sym FROM kb");
            assertKeySinksMatchOrdinaryJoin("ka.str=kb.vc AND ka.sym=kb.str",
                    "SELECT str, sym FROM ka", "SELECT i, vc, str FROM kb");
        });
    }

    @Test
    public void testKeySinksUnderEverySinkType() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            insertKeyRows();
            // RecordSinkFactory returns no class for the looping sink, so the key sinks go through
            // getInstance() and never branch on the class; all three sink types stage the same key.
            for (int sinkType : new int[]{RecordSinkFactory.SINK_TYPE_SINGLE_METHOD,
                    RecordSinkFactory.SINK_TYPE_CHUNKED, RecordSinkFactory.SINK_TYPE_LOOPING}) {
                setProperty(PropertyKey.DEBUG_CAIRO_COPIER_TYPE, sinkType);
                for (String on : new String[]{"ka.l=kb.l", "ka.ts=kb.tn",
                        "ka.sym=kb.sym AND ka.str=kb.vc AND ka.l=kb.l AND ka.ts=kb.tn"}) {
                    assertKeySinksMatchOrdinaryJoin(on);
                }
                try (SqlCompiler compiler = engine.getSqlCompiler();
                     RecordCursorFactory probeFactory = select("SELECT * FROM ka");
                     RecordCursorFactory buildFactory = select("SELECT * FROM kb")) {
                    try (HashJoinGroupByMetadata metadata = keyMetadata(compiler, "ka.l=kb.l",
                            probeFactory, buildFactory, probeFactory, buildFactory)) {
                        boolean isLooping = sinkType == RecordSinkFactory.SINK_TYPE_LOOPING;
                        Assert.assertEquals(isLooping, metadata.newProbeKeySink() instanceof LoopingRecordSink);
                        Assert.assertEquals(isLooping, metadata.newBuildKeySink() instanceof LoopingRecordSink);
                    }
                }
            }
        });
    }

    @Test
    public void testUnsupportedKeyTypes() throws Exception {
        assertMemoryLeak(() -> {
            createKeyTables();
            // A key no RecordSink stages into a map keeps the ordinary plan, which joins it itself.
            for (String on : new String[]{"ka.bin=kb.bin", "ka.arr=kb.arr"}) {
                assertCandidate("SELECT count(*) FROM ka LEFT JOIN kb ON " + on, false);
            }
            // The ordinary plan reports a pair neither plan can reconcile.
            for (String on : new String[]{"ka.i=kb.l", "ka.dt=kb.ts", "ka.sym=kb.i", "ka.f=kb.d"}) {
                assertMismatchedKeys("SELECT count(*) FROM ka LEFT JOIN kb ON " + on);
            }
        });
    }

    private void assertCandidate(String sql, boolean expected) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            Assert.assertEquals(sql, expected, candidate(compiler, sql) != null);
        }
        // All supported and rejected SQL still compile through the existing execution path.
        try (RecordCursorFactory ignored = select(sql)) {
            Assert.assertNotNull(ignored);
        }
    }

    private void assertKeys(String on, String expected, boolean isIntKeyed) throws Exception {
        String sql = "SELECT count(*) FROM ka LEFT JOIN kb ON " + on;
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            HashJoinGroupByCandidate candidate = candidate(compiler, sql);
            Assert.assertNotNull(on, candidate);
            Assert.assertEquals(on, expected, describeKeys(candidate.getKeys()));
            Assert.assertEquals(on, isIntKeyed, candidate.getKeys().isIntKeyed());
        }
        // Nothing generates the key sinks yet, so a staged key keeps the ordinary plan.
        assertPlanContains(sql, isIntKeyed ? "Hash Join Group By" : "Hash Left Outer Join Light", isIntKeyed);
    }

    private void assertKeySinkLayout(String on, String expected) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory probeFactory = select("SELECT * FROM ka");
             RecordCursorFactory buildFactory = select("SELECT * FROM kb")) {
            try (HashJoinGroupByMetadata metadata = keyMetadata(compiler, on, probeFactory, buildFactory, probeFactory, buildFactory)) {
                Assert.assertTrue(on, metadata.isKeyStaged());
                Assert.assertNotNull(on, metadata.newProbeKeySink());
                Assert.assertNotNull(on, metadata.newBuildKeySink());
                StringSink sink = new StringSink();
                ColumnTypes keyTypes = metadata.getKeyTypes();
                for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
                    if (i > 0) {
                        sink.putAscii(',');
                    }
                    sink.put(ColumnType.nameOf(keyTypes.getColumnType(i)));
                }
                Assert.assertEquals(on, expected, sink.toString());
            }
        }
    }

    private void assertKeySinksMatchOrdinaryJoin(String on) throws Exception {
        assertKeySinksMatchOrdinaryJoin(on, "SELECT * FROM ka", "SELECT * FROM kb");
    }

    private void assertKeySinksMatchOrdinaryJoin(String on, String probeSql, String buildSql) throws Exception {
        final long expected = pairCount("SELECT count(*) FROM ka JOIN kb ON " + on);
        // A pair that matches nothing would pass whatever the sinks stage, so refuse one.
        Assert.assertTrue(on + " matched no rows", expected > 0);
        long matched = 0;
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory probeBase = select("SELECT * FROM ka");
             RecordCursorFactory buildBase = select("SELECT * FROM kb");
             RecordCursorFactory probeFactory = select(probeSql);
             RecordCursorFactory buildFactory = select(buildSql)) {
            try (HashJoinGroupByMetadata metadata = keyMetadata(compiler, on, probeFactory, buildFactory, probeBase, buildBase)) {
                Assert.assertTrue(on, metadata.isKeyStaged());
                RecordSink buildKeySink = metadata.newBuildKeySink();
                RecordSink probeKeySink = metadata.newProbeKeySink();
                // One map row per distinct build key, counting the build rows that share it, so a
                // probe hit contributes exactly the pairs the ordinary join would emit for it.
                try (Map map = new OrderedMap(1024, metadata.getKeyTypes(), BUILD_ROW_COUNT_TYPE, 16, 0.6, 1024)) {
                    try (RecordCursor cursor = buildFactory.getCursor(sqlExecutionContext)) {
                        Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            MapKey key = map.withKey();
                            buildKeySink.copy(record, key);
                            MapValue value = key.createValue();
                            value.putLong(0, value.isNew() ? 1 : value.getLong(0) + 1);
                        }
                    }
                    try (RecordCursor cursor = probeFactory.getCursor(sqlExecutionContext)) {
                        Record record = cursor.getRecord();
                        while (cursor.hasNext()) {
                            MapKey key = map.withKey();
                            probeKeySink.copy(record, key);
                            MapValue value = key.findValue();
                            if (value != null) {
                                matched += value.getLong(0);
                            }
                        }
                    }
                }
            }
        }
        Assert.assertEquals(on, expected, matched);
    }

    private void assertMismatchedKeys(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            Assert.assertNull(sql, candidate(compiler, sql));
        }
        try (RecordCursorFactory ignored = select(sql)) {
            Assert.fail("expected a join key type mismatch: " + sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "join column type mismatch");
        }
    }

    private void assertPlanContains(String sql, String expected, boolean fused) throws Exception {
        try (RecordCursorFactory factory = select("explain " + sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            StringSink plan = new StringSink();
            while (cursor.hasNext()) {
                plan.put(cursor.getRecord().getStrA(0)).put('\n');
            }
            Assert.assertTrue(plan.toString(), plan.toString().contains(expected));
            Assert.assertEquals(plan.toString(), fused, plan.toString().contains("Hash Join Group By"));
        }
    }

    private void assertSql(String expected, String sql, boolean isRandomAccessSupported) throws Exception {
        assertQuery(sql).noLeakCheck().expectSize().supportsRandomAccess(isRandomAccessSupported).returns(expected);
    }

    private HashJoinGroupByCandidate candidate(SqlCompiler compiler, String sql) throws Exception {
        IQueryModel model = (IQueryModel) compiler.generateExecutionModel(sql, sqlExecutionContext);
        while (model != null && model.getSelectModelType() != IQueryModel.SELECT_MODEL_GROUP_BY) {
            model = model.getNestedModel();
        }
        Assert.assertNotNull(sql, model);
        StringSink before = new StringSink();
        model.toSink(before);
        HashJoinGroupByCandidate candidate = SqlCodeGenerator.getHashJoinGroupByCandidate(model,
                new FunctionParser(configuration, engine.getFunctionFactoryCache()), sqlExecutionContext);
        StringSink after = new StringSink();
        model.toSink(after);
        Assert.assertEquals("candidate analysis mutated the model", before.toString(), after.toString());
        return candidate;
    }




    private void insertKeyRows() throws Exception {
        // Small overlapping domains with NULLs, so both sides carry duplicate keys, shared keys,
        // keys the other side lacks and NULL keys. BINARY and ARRAY stay NULL: no key uses them.
        for (String name : new String[]{"ka", "kb"}) {
            execute("INSERT INTO " + name + " (i, l, s, b, c, bo, f, d, dt, ts, tn, ip, u, l256, g, dec, sym, str, vc) " +
                    """
                            SELECT rnd_int(0, 2, 1), rnd_int(0, 2, 1)::long, rnd_short(0, 2), rnd_byte(0, 2),
                                   rnd_str('a', 'b', NULL)::char, rnd_boolean(),
                                   rnd_int(0, 2, 1)::float, rnd_int(0, 2, 1)::double,
                                   rnd_int(0, 2, 1)::long::date, rnd_int(0, 2, 1)::long::timestamp,
                                   rnd_int(0, 2, 1)::long::timestamp::timestamp_ns,
                                   rnd_ipv4('10.0.0.1/30', 1),
                                   rnd_str('11111111-1111-1111-1111-111111111111',
                                           '22222222-2222-2222-2222-222222222222', NULL)::uuid,
                                   rnd_str('0x01', '0x02', NULL)::long256,
                                   rnd_str('sp052w92', 'ezs42e44', NULL)::geohash(8c),
                                   rnd_int(0, 2, 1)::decimal(10,2),
                                   rnd_symbol('a', 'b', NULL), rnd_str('a', 'b', NULL), rnd_varchar('a', 'b', NULL)
                            FROM long_sequence(64)""");
        }
    }

    private boolean isIntKeyed(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            HashJoinGroupByCandidate candidate = candidate(compiler, sql);
            Assert.assertNotNull(sql, candidate);
            return candidate.getKeys().isIntKeyed();
        }
    }

    private boolean isSymbolKey(String sql) throws Exception {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            HashJoinGroupByCandidate candidate = candidate(compiler, sql);
            Assert.assertNotNull(sql, candidate);
            return candidate.getKeys().isSymbolKey();
        }
    }

    private HashJoinGroupByMetadata keyMetadata(
            SqlCompiler compiler,
            String on,
            RecordCursorFactory probeFactory,
            RecordCursorFactory buildFactory,
            RecordCursorFactory probeBase,
            RecordCursorFactory buildBase
    ) throws Exception {
        // LEFT JOIN pins ka as the probe input, whatever the optimiser makes of the key order.
        HashJoinGroupByCandidate candidate = candidate(compiler, "SELECT count(*) FROM ka LEFT JOIN kb ON " + on);
        Assert.assertNotNull(on, candidate);
        return new HashJoinGroupByMetadata(configuration, new BytecodeAssembler(), candidate,
                probeFactory.getMetadata(), baseColumns(probeFactory.getMetadata(), probeBase.getMetadata()),
                buildFactory.getMetadata(), baseColumns(buildFactory.getMetadata(), buildBase.getMetadata()));
    }

    private long pairCount(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            Assert.assertTrue(sql, cursor.hasNext());
            return cursor.getRecord().getLong(0);
        }
    }

    /** Maps each column of an input projection to its index in the base table, by name. */
    private static IntList baseColumns(RecordMetadata projection, RecordMetadata base) {
        IntList list = new IntList(projection.getColumnCount());
        for (int i = 0; i < projection.getColumnCount(); i++) {
            list.add(base.getColumnIndex(projection.getColumnName(i)));
        }
        return list;
    }

    private static String describeKeys(HashJoinGroupByKeys keys) {
        StringSink sink = new StringSink();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (i > 0) {
                sink.putAscii(' ');
            }
            sink.put(keys.getProbeColumn(i)).putAscii('=').put(keys.getBuildColumn(i))
                    .putAscii(':').put(ColumnType.nameOf(keys.getType(i)));
        }
        return sink.toString();
    }


    private static IntList ints(int... columns) {
        IntList list = new IntList(columns.length);
        for (int i = 0; i < columns.length; i++) {
            list.add(columns[i]);
        }
        return list;
    }

    private void createKeyTables() throws Exception {
        for (String name : new String[]{"ka", "kb"}) {
            execute("CREATE TABLE " + name + " (i INT, l LONG, s SHORT, b BYTE, c CHAR, bo BOOLEAN, f FLOAT, d DOUBLE, " +
                    "dt DATE, ts TIMESTAMP, tn TIMESTAMP_NS, ip IPV4, u UUID, l256 LONG256, g GEOHASH(8c), " +
                    "dec DECIMAL(10,2), sym SYMBOL, str STRING, vc VARCHAR, bin BINARY, arr DOUBLE[], v DOUBLE)");
        }
    }

    private void createTables() throws Exception {
        execute("create table r (plant_id int, reading_ts timestamp, energy_kwh double, irradiance_wm2 double, long_key long, sym_key symbol) timestamp(reading_ts) partition by month");
        execute("create table p (plant_id int, country symbol, installed_kwp double, long_key long, sym_key symbol)");
    }
}
