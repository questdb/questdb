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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.FunctionParser;
import io.questdb.griffin.HashJoinGroupByCandidate;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.groupby.SumDoubleGroupByFunction;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import org.junit.Assert;
import org.junit.Test;

public class HashJoinGroupByCandidateTest extends AbstractCairoTest {
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
                for (String expression : new String[]{"sum(d)", "avg(d)", "count()", "count(i)", "count(l)", "count(d)", "count(s)", "sum(coalesce(d, 0.0))"}) {
                    try (Function function = parser.parseFunction(compiler.testParseExpression(expression, QueryModel.FACTORY.newInstance()), metadata, sqlExecutionContext)) {
                        Assert.assertTrue(expression, HashJoinGroupByCandidate.supportsAggregate(function));
                    }
                }
                for (String expression : new String[]{"first(d)", "last(d)", "sum(i)", "avg(l)", "min(d)", "count_distinct(i)", "sum(rnd_double())"}) {
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
    public void testDefaultResultsAndOuterPredicatePlacement() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            execute("insert into r values (1, '2020-01-01', 10, 100, 1, 'a'), (2, '2020-01-02', 20, 200, 2, 'b'), (3, '2020-01-03', 30, 300, 3, 'c'), (null, '2020-01-04', 40, 400, null, null)");
            execute("insert into p values (1, 'ES', 5, 1, 'a'), (1, 'IT', 7, 1, 'a'), (2, 'DE', 9, 2, 'b'), (null, null, null, null, null)");
            assertSql("country\tsum\nES\t10.0\nIT\t10.0\n", "select p.country, sum(r.energy_kwh)" + JOIN + " where p.country in ('ES','IT') order by p.country");
            String outer = " from r left join p on r.plant_id=p.plant_id";
            assertSql("count\tcount1\tsum\n2\t0\t70.0\n", "select count(*), count(p.plant_id), sum(r.energy_kwh)" + outer + " where p.installed_kwp is null");
            assertSql("count\n0\n", "select count(*)" + outer + " where p.installed_kwp = 42");
            assertSql("count\n5\n", "select count(*) from r left join p on r.plant_id=p.plant_id and p.country in ('ES','IT')");
            assertSql("country\tfirst\tlast\n\t40.0\t40.0\nDE\t20.0\t20.0\nES\t10.0\t10.0\nIT\t10.0\t10.0\n", "select p.country, first(r.energy_kwh), last(r.energy_kwh)" + JOIN + " order by p.country");
            assertPlanContains("select p.country, sum(r.energy_kwh)" + JOIN, "Hash Join Light");
            assertPlanContains("select p.country, sum(r.energy_kwh)" + outer, "Hash Left Outer Join Light");
            assertPlanContains("select p.country, first(r.energy_kwh)" + JOIN, "Hash Join Light");
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
                Assert.assertEquals(0, candidate.getProbeKeyColumn());
                Assert.assertEquals(0, candidate.getBuildKeyColumn());
                Assert.assertEquals("[1,2]", candidate.getRequiredBuildColumns().toString());
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
            execute("alter table p alter column plant_id type long");
            execute("alter table r alter column plant_id type long");
            assertCandidate("select p.country, sum(r.energy_kwh)" + JOIN, false);
        });
    }

    @Test
    public void testRejectedShapes() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String sql : new String[]{
                    "select p.country, sum(r.energy_kwh) from r join p on r.long_key=p.long_key",
                    "select p.country, sum(r.energy_kwh) from r join p on r.sym_key=p.sym_key",
                    "select p.country, sum(r.energy_kwh) from r join p on r.plant_id+1=p.plant_id",
                    "select p.country, sum(r.energy_kwh)" + JOIN + " and r.long_key=p.long_key",
                    "select p.country, sum(r.energy_kwh)" + JOIN + " where r.energy_kwh > p.installed_kwp",
                    "select p.country, sum(r.energy_kwh)" + JOIN + " join p p2 on r.plant_id=p2.plant_id",
                    "select country, sum(e) from (select p.country country, r.energy_kwh e" + JOIN + " limit 3)",
                    "select country, sum(e) from (select distinct p.country country, r.energy_kwh e" + JOIN + ")",
                    "select p.country, first(r.energy_kwh)" + JOIN,
                    "select p.country, last(r.energy_kwh)" + JOIN,
                    "select p.country, sum(r.plant_id)" + JOIN,
                    "select p.country, min(r.energy_kwh)" + JOIN,
                    "select p.country, count_distinct(r.plant_id)" + JOIN,
                    "select p.country, sum(rnd_double())" + JOIN
            }) {
                assertCandidate(sql, false);
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

    private void assertPlanContains(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = select("explain " + sql);
             RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            StringSink plan = new StringSink();
            while (cursor.hasNext()) {
                plan.put(cursor.getRecord().getStrA(0)).put('\n');
            }
            Assert.assertTrue(plan.toString(), plan.toString().contains(expected));
            Assert.assertFalse(plan.toString(), plan.toString().contains("Async Hash Join Group By"));
        }
    }

    private void assertSql(String expected, String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            QueryAssertion assertion = assertQuery(sql).noLeakCheck().expectSize();
            if (!factory.recordCursorSupportsRandomAccess()) {
                assertion.noRandomAccess();
            }
            assertion.returns(expected);
        }
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

    private void createTables() throws Exception {
        execute("create table r (plant_id int, reading_ts timestamp, energy_kwh double, irradiance_wm2 double, long_key long, sym_key symbol) timestamp(reading_ts) partition by month");
        execute("create table p (plant_id int, country symbol, installed_kwp double, long_key long, sym_key symbol)");
    }
}
