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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.groupby.CountRecordCursorFactory;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class CountAggregateTest extends AbstractCairoTest {
    @Test
    public void testCountNormalizationsAndEmptyInput() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (String count : new String[]{"count()", "count(*)", "count(1)", "count('x')", "count(true)"}) {
                assertAggregate("SELECT " + count + " AS total FROM lp_count", "total\n4\n", 0);
                assertAggregate("SELECT " + count + " FROM lp_count WHERE false", "count\n0\n", 0);
            }
            assertAggregate("SELECT count()", "count\n1\n", 0);
            assertAggregate("SELECT COUNT() FROM lp_count", "count\n4\n", 0);
            assertAggregate("SELECT count() AS COUNT FROM lp_count", "count\n4\n", 0);
            assertAggregate("SELECT count() AS \"COUNT\" FROM lp_count", "count\n4\n", 0);
            assertAggregate("SELECT COUNT FROM (SELECT count() AS COUNT FROM lp_count)", "COUNT\n4\n", 0);
            assertAggregate("SELECT * FROM (SELECT count() AS COUNT FROM lp_count)", "COUNT\n4\n", 0);
            assertAggregate("SELECT count FROM (SELECT count() AS COUNT FROM lp_count)", "count\n4\n", 0);
            assertAggregate("SELECT count() AS \"Number of Rows\" FROM lp_count", "Number of Rows\n4\n", 0);
        });
    }

    @Test
    public void testCountRetainsAggregateRowAndFilterBoundary() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertAggregate("SELECT 7 AS answer FROM (SELECT count() FROM lp_count WHERE false)", "answer\n7\n", 0);
            assertAggregate("SELECT total FROM (SELECT count() AS total FROM lp_count) WHERE total>3", "total\n4\n", 0);
            assertAggregate("SELECT total FROM (SELECT count() AS total FROM lp_count) WHERE total>4", "total\n", 0);
            assertAggregate("SELECT count() FROM (SELECT count() FROM lp_count WHERE false)", "count\n1\n", 0);
        });
    }

    @Test
    public void testCountOverSetsKeepsEqualityAndMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertAggregate("SELECT count() FROM (SELECT id FROM lp_count UNION SELECT id FROM lp_count)", "count\n3\n", 0);
            assertAggregate("SELECT count() FROM (SELECT id FROM lp_count UNION ALL SELECT id FROM lp_count)", "count\n8\n", 0);
            assertAggregate("SELECT count() FROM (SELECT id FROM lp_count EXCEPT SELECT id FROM lp_count WHERE id=1)", "count\n2\n", 0);
            assertAggregate("SELECT count() FROM lp_count UNION ALL SELECT count() FROM lp_count WHERE false", "count\n4\n0\n", 0);
        });
    }

    @Test
    public void testCountRemovesUnobservableSortAndKeepsLimitSelection() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertAggregate("SELECT count() FROM (SELECT id FROM lp_count ORDER BY id DESC)", "count\n4\n", 0);
            assertAggregate("SELECT count() FROM (SELECT id FROM lp_count ORDER BY id DESC LIMIT 2) WHERE id>1", "count\n2\n", 1);
            assertAggregate("SELECT count() FROM lp_count LIMIT 0", "count\n", 0);
            assertAggregate("SELECT count() FROM lp_count LIMIT 1,2", "count\n", 0);
            assertAggregate("SELECT count() AS total FROM lp_count ORDER BY total DESC LIMIT 1", "total\n4\n", 0);
            assertAggregate("SELECT count() FROM lp_count ORDER BY count()", "count\n4\n", 0);
        });
    }

    @Test
    public void testCountUsesExistingSpecializationAndIntervals() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertAggregate("SELECT count() FROM lp_count WHERE ts>='2020-01-02'", "count\n3\n", 0);
            assertAggregate("SELECT count() FROM lp_count WHERE id>1 AND ts>='2020-01-02'", "count\n2\n", 0);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT count() FROM lp_count", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.assertEquals(CountRecordCursorFactory.class, factory.getBaseFactory().getClass());
                    Assert.assertEquals(ColumnType.LONG, factory.getMetadata().getColumnType(0));
                    Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
                    Assert.assertFalse(factory.recordCursorSupportsRandomAccess());
                    assertFactory(factory).withContext(sqlExecutionContext).noRandomAccess().expectSize().returns("count\n4\n");
                }
            }
        });
    }

    @Test
    public void testCountFactorySurvivesCompilerReuseAndRefreshesParameters() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            bindVariableService.setInt(0, 1);
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT count() AS total FROM lp_count WHERE id>$1", sqlExecutionContext).getRecordCursorFactory();
                    assertRowsOnly(retained, "total\n2\n");
                    try (RecordCursorFactory other = compiler.compile("SELECT count() FROM lp_count WHERE false", sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(other, "count\n0\n");
                    }
                    compiler.clear();
                }
                bindVariableService.setInt(0, 2);
                assertRowsOnly(retained, "total\n1\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testCountCanFeedInsert() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            execute("CREATE TABLE lp_totals (total LONG)");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                CairoEngine.execute(compiler, "INSERT INTO lp_totals SELECT count() FROM lp_count", sqlExecutionContext, null);
            }
            assertQuery("SELECT * FROM lp_totals").expectSize().returns("total\n4\n");
        });
    }

    @Test
    public void testWindowCountUsesWindowContext() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile("SELECT count() OVER () FROM lp_count", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(factory, "count\n4\n4\n4\n4\n");
                }
            }
        });
    }

    @Test
    public void testAggregateOutsideAggregateContext() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQuery("SELECT id FROM lp_count LIMIT count()").noLeakCheck()
                    .fails(30, "LIMIT expressions must be convertible to INT");
            assertQuery("SELECT id, row_number() OVER (ORDER BY count()) FROM lp_count").noLeakCheck()
                    .fails(39, "Invalid column: count");
            assertQuery("SELECT id, sum(id) OVER (ORDER BY count() + 1) FROM lp_count").noLeakCheck()
                    .fails(42, "Invalid column: +");
            assertQuery("SELECT count() FROM lp_count SAMPLE BY 1d FROM count()").noLeakCheck()
                    .fails(47, "Aggregate function cannot be passed as an argument");
            assertQuery("SELECT count() FROM lp_count SAMPLE BY 1d FILL(count())").noLeakCheck()
                    .fails(47, "invalid fill value: count");
            assertQuery("SELECT id FROM lp_count WHERE count() > 1").noLeakCheck()
                    .fails(30, "Aggregate function cannot be passed as an argument");
            assertQuery("SELECT id FROM lp_count UNION ALL SELECT id FROM lp_count ORDER BY count(), id").noLeakCheck().expectSize()
                    .returns("""
                            id
                            2
                            3
                            1
                            """);
            assertQuery("SELECT id FROM lp_count UNION ALL SELECT id FROM lp_count ORDER BY count() DESC LIMIT 1").noLeakCheck().expectSize()
                    .returns("""
                            id
                            1
                            """);
        });
    }

    @Test
    public void testRebuiltAggregatesAndWhereAggregateFails() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertAggregate("SELECT count(null) FROM lp_count", "count\n0\n", 0);
            assertAggregate("SELECT ksum(id)+1 total FROM lp_count", "total\n8.0\n", 0);
            assertAggregate("SELECT id,ksum(id) FROM lp_count GROUP BY id ORDER BY id", "id\tksum\n1\t2.0\n2\t2.0\n3\t3.0\n", 1);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_count WHERE count()>1", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("aggregate accepted in WHERE");
                } catch (SqlException e) {
                    Assert.assertEquals(30, e.getPosition());
                    TestUtils.assertEquals("Aggregate function cannot be passed as an argument", e.getFlyweightMessage());
                }
                try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_count WHERE count()", sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("aggregate accepted in WHERE");
                } catch (SqlException e) {
                    Assert.assertEquals(30, e.getPosition());
                    TestUtils.assertEquals("boolean expression expected", e.getFlyweightMessage());
                }
                try (RecordCursorFactory recovery = compiler.compile("SELECT count() FROM lp_count", sqlExecutionContext).getRecordCursorFactory()) {
                    assertRowsOnly(recovery, "count\n4\n");
                }
            }
        });
    }

    @Test
    public void testGroupedValidationRejectsWildcardOrdinalsAndNestedAliases() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> queries = new ObjList<>();
            queries.add("SELECT * FROM lp_count GROUP BY 1");
            queries.add("SELECT lp_count.*,count() FROM lp_count GROUP BY 1");
            queries.add("SELECT id AS key,sum(id) FROM lp_count GROUP BY id ORDER BY key+1");
            queries.add("SELECT id,count() FROM lp_count GROUP BY");
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int i = 0, n = queries.size(); i < n; i++) {
                    final String sql = queries.getQuick(i);
                    String expectedMessage = null;
                    int expectedPosition = -1;
                    try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail(sql);
                    } catch (SqlException e) {
                        expectedMessage = e.getFlyweightMessage().toString();
                        expectedPosition = e.getPosition();
                    }
                    try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail(sql);
                    } catch (SqlException e) {
                        Assert.assertEquals(sql, expectedPosition, e.getPosition());
                        TestUtils.assertEquals(expectedMessage, e.getFlyweightMessage());
                    }
                    try (RecordCursorFactory recovery = compiler.compile("SELECT count() FROM lp_count", sqlExecutionContext).getRecordCursorFactory()) {
                        assertRowsOnly(recovery, "count\n4\n");
                    }
                }
            }
        });
    }

    private static int countSorts(LogicalPlan plan) {
        int count = plan instanceof SortPlan ? 1 : 0;
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            count += countSorts(plan.inputAt(i));
        }
        return count;
    }

    private void assertAggregate(String sql, String expected, int sortCount) throws Exception {
        try (
                SqlCompilerImpl compiler = new SqlCompilerImpl(engine);
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            Assert.assertEquals(sortCount, countSorts(compiler.getPlanForTesting()));
            assertRowsOnly(factory, expected);
        }
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE lp_count (unused STRING,id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        execute("INSERT INTO lp_count VALUES ('a',1,'2020-01-01'),('b',3,'2020-01-02'),('c',2,'2020-01-03'),('d',1,'2020-01-04')");
    }
}
