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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.cairo.CairoTestConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SqlLogicalDistinctTest extends AbstractCairoTest {
    @BeforeClass
    public static void setUpStatic() throws Exception {
        configurationFactory = (root, telemetry, overrides) -> new CairoTestConfiguration(root, telemetry, overrides) {
            @Override
            public boolean isSqlDistinctGroupByRewriteEnabled() {
                return false;
            }
        };
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testComputedOrderExpressionsPreserveDirectDistinctTuple() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows("SELECT DISTINCT id+1 AS v FROM lp_distinct ORDER BY id+1", """
                    v
                    null
                    2
                    3
                    4
                    """);
            assertQueryRows("SELECT DISTINCT id FROM lp_distinct ORDER BY id+1", """
                    id	column
                    null	null
                    1	2
                    2	3
                    3	4
                    """);
            assertQueryRows("SELECT DISTINCT id AS v FROM lp_distinct ORDER BY v+1", """
                    v	column
                    null	null
                    1	2
                    2	3
                    3	4
                    """);
            assertQueryRows("SELECT DISTINCT id+1 AS v FROM lp_distinct ORDER BY v DESC", """
                    v
                    4
                    3
                    2
                    null
                    """);
            assertQueryRows(
                    "SELECT DISTINCT id AS v FROM lp_distinct ORDER BY lp_distinct.id",
                    """
                            v
                            null
                            1
                            2
                            3
                            """
            );
            assertQueryRows("SELECT DISTINCT sym,id FROM lp_distinct ORDER BY 2,1", """
                    sym	id
                    	null
                    A	1
                    B	1
                    B	2
                    C	3
                    """);
        });
    }

    @Test
    public void testEqualityTupleSurvivesOuterPruningAndFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT id FROM (SELECT DISTINCT id,sym FROM lp_distinct) ORDER BY id",
                    """
                            id
                            null
                            1
                            1
                            2
                            3
                            """
            );
            assertQueryRows(
                    "SELECT id FROM (SELECT DISTINCT id,sym FROM lp_distinct) WHERE id>1 ORDER BY id",
                    """
                            id
                            2
                            3
                            """
            );
            assertQueryRows(
                    "SELECT 7 AS v FROM (SELECT DISTINCT id,sym FROM lp_distinct)",
                    """
                            v
                            7
                            7
                            7
                            7
                            7
                            """
            );
        });
    }

    @Test
    public void testGenericDistinctNullsSymbolsAndVariableColumns() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows(
                    "SELECT DISTINCT id,sym,label,note FROM lp_distinct ORDER BY id,sym",
                    """
                            id	sym	label	note
                            null		\t
                            1	A	a	alpha
                            1	B	b	beta
                            2	B	b	beta
                            3	C	c	café
                            """
            );
            assertQueryRows("SELECT DISTINCT id FROM lp_distinct", """
                    id
                    3
                    1
                    null
                    2
                    """);
            assertPlan("SELECT DISTINCT id FROM lp_distinct", "Distinct", "DistinctTimeSeries");
        });
    }

    @Test
    public void testLimitAdviceDoesNotReplaceLimit() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows("SELECT DISTINCT id FROM lp_distinct LIMIT 2", """
                    id
                    3
                    1
                    """);
            assertQueryRows("SELECT DISTINCT id FROM lp_distinct LIMIT 1,3", """
                    id
                    1
                    null
                    """);
            assertQueryRows("SELECT DISTINCT id FROM lp_distinct LIMIT -2", """
                    id
                    null
                    2
                    """);
            assertQueryRows("SELECT DISTINCT id FROM lp_distinct ORDER BY id LIMIT 2", """
                    id
                    null
                    1
                    """);
            assertPlan("SELECT DISTINCT id FROM lp_distinct LIMIT 2", "earlyExit: 2", "DistinctTimeSeries");
            assertPlan("SELECT DISTINCT id FROM lp_distinct ORDER BY id LIMIT 2", "Limit", "earlyExit");
        });
    }

    @Test
    public void testOrderErrorsAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertErrorOnCompilerReuse("SELECT DISTINCT id FROM lp_distinct ORDER BY ts", 45, "ORDER BY expressions must appear in select list. Invalid column: ts");
            assertErrorOnCompilerReuse("SELECT DISTINCT id FROM lp_distinct ORDER BY missing", 45, "Invalid column: missing");
            assertErrorOnCompilerReuse("SELECT DISTINCT id FROM lp_distinct ORDER BY 2", 45, "order column position is out of range [max=1]");
        });
    }

    @Test
    public void testPreparedDistinctFactorySurvivesCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            RecordCursorFactory retained = null;
            try {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    retained = compiler.compile("SELECT DISTINCT id FROM lp_distinct ORDER BY id", sqlExecutionContext)
                            .getRecordCursorFactory();
                    try (RecordCursorFactory other = compiler.compile("SELECT DISTINCT sym FROM lp_distinct LIMIT 1", sqlExecutionContext)
                            .getRecordCursorFactory()) {
                        try (RecordCursor cursor = other.getCursor(sqlExecutionContext)) {
                            Assert.assertTrue(cursor.hasNext());
                            Assert.assertFalse(cursor.hasNext());
                        }
                    }
                    compiler.clear();
                }
                assertFactory(retained).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp()
                        .sizeMayVary().returns("id\nnull\n1\n2\n3\n");
            } finally {
                Misc.free(retained);
            }
        });
    }

    @Test
    public void testTimeSeriesDistinctKeepsInputOrderAndFactoryChoice() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertQueryRows("SELECT DISTINCT ts,id FROM lp_distinct", """
                    ts	id
                    2020-01-01T00:00:00.000000Z	3
                    2020-01-01T00:00:01.000000Z	1
                    2020-01-01T00:00:02.000000Z	null
                    2020-01-01T00:00:03.000000Z	2
                    2020-01-01T00:00:04.000000Z	3
                    """);
            assertQueryRows("SELECT DISTINCT ts,id FROM lp_distinct ORDER BY ts", """
                    ts	id
                    2020-01-01T00:00:00.000000Z	3
                    2020-01-01T00:00:01.000000Z	1
                    2020-01-01T00:00:02.000000Z	null
                    2020-01-01T00:00:03.000000Z	2
                    2020-01-01T00:00:04.000000Z	3
                    """);
            assertQueryRows("SELECT DISTINCT ts,id FROM lp_distinct ORDER BY ts DESC", """
                    ts	id
                    2020-01-01T00:00:04.000000Z	3
                    2020-01-01T00:00:03.000000Z	2
                    2020-01-01T00:00:02.000000Z	null
                    2020-01-01T00:00:01.000000Z	1
                    2020-01-01T00:00:00.000000Z	3
                    """);
            assertQueryRows("SELECT DISTINCT ts,id FROM lp_distinct LIMIT 2", """
                    ts	id
                    2020-01-01T00:00:00.000000Z	3
                    2020-01-01T00:00:01.000000Z	1
                    """);
            assertPlan("SELECT DISTINCT ts,id FROM lp_distinct ORDER BY ts DESC", "DistinctTimeSeries", "Frame backward scan");
            assertPlan("SELECT DISTINCT ts,id FROM lp_distinct ORDER BY ts", "DistinctTimeSeries", "sort");
        });
    }

    @Test
    public void testTimeSeriesDistinctRecordBSurvivesHasNext() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (id INT, ts TIMESTAMP) TIMESTAMP(ts)");
            execute("""
                    INSERT INTO x VALUES
                    (3, '2020-01-01T00:00:00Z'),
                    (3, '2020-01-01T00:00:00Z'),
                    (1, '2020-01-01T00:00:01Z'),
                    (1, '2020-01-01T00:00:01Z'),
                    (null, '2020-01-01T00:00:02Z'),
                    (null, '2020-01-01T00:00:02Z'),
                    (2, '2020-01-01T00:00:03Z'),
                    (3, '2020-01-01T00:00:04Z')
                    """);
            assertQuery("SELECT DISTINCT ts, id FROM x")
                    .noLeakCheck()
                    .withPlanContaining("DistinctTimeSeries")
                    .inferTimestamp()
                    .sizeMayVary()
                    .returns("""
                            ts\tid
                            2020-01-01T00:00:00.000000Z\t3
                            2020-01-01T00:00:01.000000Z\t1
                            2020-01-01T00:00:02.000000Z\tnull
                            2020-01-01T00:00:03.000000Z\t2
                            2020-01-01T00:00:04.000000Z\t3
                            """);
        });
    }

    private static boolean containsDistinct(LogicalPlan plan) {
        if (plan.getType() == LogicalPlan.Type.DISTINCT) {
            return true;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (containsDistinct(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }

    private void assertErrorOnCompilerReuse(String sql, int position, String message) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            for (int reuse = 0; reuse < 2; reuse++) {
                try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    Assert.fail("expected ORDER BY validation failure");
                } catch (SqlException e) {
                    Assert.assertEquals(position, e.getPosition());
                    TestUtils.assertEquals(message, e.getFlyweightMessage());
                }
            }
            try (RecordCursorFactory recovered = compiler.compile("SELECT DISTINCT id FROM lp_distinct LIMIT 1", sqlExecutionContext)
                    .getRecordCursorFactory(); RecordCursor cursor = recovered.getCursor(sqlExecutionContext)) {
                Assert.assertTrue(cursor.hasNext());
            }
        }
    }

    private void assertPlan(String sql, String present, String absent) throws Exception {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink sink = new TextPlanSink();
            sink.of(factory, sqlExecutionContext);
            TestUtils.assertContains(sink.getSink(), present);
            Assert.assertFalse(sink.getSink().toString(), sink.getSink().toString().contains(absent));
        }
    }

    private RecordCursorFactory compile(String sql) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try {
                Assert.assertTrue(containsDistinct(compiler.getLogicalPlanForTesting()));
            } catch (Throwable th) {
                factory.close();
                throw th;
            }
            return factory;
        }
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_distinct(id INT,sym SYMBOL,label STRING,note VARCHAR,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO lp_distinct VALUES
                (3,'C','c','café','2020-01-01T00:00:00Z'),
                (3,'C','c','café','2020-01-01T00:00:00Z'),
                (1,'A','a','alpha','2020-01-01T00:00:01Z'),
                (1,'B','b','beta','2020-01-01T00:00:01Z'),
                (null,null,null,null,'2020-01-01T00:00:02Z'),
                (null,null,null,null,'2020-01-01T00:00:02Z'),
                (2,'B','b','beta','2020-01-01T00:00:03Z'),
                (3,'C','c','café','2020-01-01T00:00:04Z')
                """);
    }
    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
