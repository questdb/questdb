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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SetOperationPlan;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SetOperationTest extends AbstractCairoTest {
    private static final String[] OPERATIONS = {"UNION", "UNION ALL", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL"};

    @Test
    public void testAllOperationsRetainEqualityColumnsAndMultiplicity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (id INT, label STRING)");
            execute("CREATE TABLE lp_set_b (id INT, label STRING)");
            execute("INSERT INTO lp_set_a VALUES (1,'a'),(1,'b'),(2,'a'),(3,'c'),(3,'c'),(null,null)");
            execute("INSERT INTO lp_set_b VALUES (1,'b'),(2,'a'),(2,'a'),(4,'d'),(null,null)");
            {
                final String operation = OPERATIONS[0];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,label FROM lp_set_a " + operation
                                + " SELECT id,label FROM lp_set_b) ORDER BY id",
                        """
                                id
                                null
                                1
                                1
                                2
                                3
                                4
                                """
                );
            }
            {
                final String operation = OPERATIONS[1];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,label FROM lp_set_a " + operation
                                + " SELECT id,label FROM lp_set_b) ORDER BY id",
                        """
                                id
                                null
                                null
                                1
                                1
                                1
                                2
                                2
                                2
                                3
                                3
                                4
                                """
                );
            }
            {
                final String operation = OPERATIONS[2];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,label FROM lp_set_a " + operation
                                + " SELECT id,label FROM lp_set_b) ORDER BY id",
                        """
                                id
                                null
                                1
                                2
                                """
                );
            }
            {
                final String operation = OPERATIONS[3];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,label FROM lp_set_a " + operation
                                + " SELECT id,label FROM lp_set_b) ORDER BY id",
                        """
                                id
                                null
                                1
                                2
                                """
                );
            }
            {
                final String operation = OPERATIONS[4];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,label FROM lp_set_a " + operation
                                + " SELECT id,label FROM lp_set_b) ORDER BY id",
                        """
                                id
                                1
                                3
                                """
                );
            }
            {
                final String operation = OPERATIONS[5];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,label FROM lp_set_a " + operation
                                + " SELECT id,label FROM lp_set_b) ORDER BY id",
                        """
                                id
                                1
                                3
                                3
                                """
                );
            }
        });
    }

    @Test
    public void testCountMismatchPreservesPositionAndCompilerRecovers() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (id INT, value LONG)");
            execute("CREATE TABLE lp_set_b (id INT)");
            execute("INSERT INTO lp_set_a VALUES (1,10)");
            execute("INSERT INTO lp_set_b VALUES (2)");
            final String sql = "SELECT id,value FROM lp_set_a UNION ALL SELECT id FROM lp_set_b";
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                for (int reuse = 0; reuse < 2; reuse++) {
                    try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.fail("expected set column count failure");
                    } catch (SqlException e) {
                        Assert.assertEquals(40, e.getPosition());
                        TestUtils.assertEquals("queries have different number of columns", e.getFlyweightMessage());
                    }
                }
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT id FROM lp_set_a UNION ALL SELECT id FROM lp_set_b", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp()
                            .sizeMayVary().returns("id\n1\n2\n");
                }
            }
        });
    }

    @Test
    public void testNumericWideningAlsoCastsSymbolKeys() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (id INT, s SYMBOL)");
            execute("CREATE TABLE lp_set_b (id LONG, s SYMBOL)");
            execute("INSERT INTO lp_set_a VALUES (1,'b'),(2,'a'),(null,null)");
            execute("INSERT INTO lp_set_b VALUES (2,'a'),(9007199254740993,'b'),(null,null)");
            {
                final String operation = OPERATIONS[0];
                assertRowsOnly(
                        "SELECT * FROM (SELECT id,s FROM lp_set_a " + operation
                                + " SELECT id,s FROM lp_set_b) ORDER BY id,s",
                        """
                                id	s
                                null\t
                                1	b
                                2	a
                                9007199254740993	b
                                """
                );
            }
            {
                final String operation = OPERATIONS[1];
                assertRowsOnly(
                        "SELECT * FROM (SELECT id,s FROM lp_set_a " + operation
                                + " SELECT id,s FROM lp_set_b) ORDER BY id,s",
                        """
                                id	s
                                null\t
                                null\t
                                1	b
                                2	a
                                2	a
                                9007199254740993	b
                                """
                );
            }
            {
                final String operation = OPERATIONS[2];
                assertRowsOnly(
                        "SELECT * FROM (SELECT id,s FROM lp_set_a " + operation
                                + " SELECT id,s FROM lp_set_b) ORDER BY id,s",
                        """
                                id	s
                                null\t
                                2	a
                                """
                );
            }
            {
                final String operation = OPERATIONS[3];
                assertRowsOnly(
                        "SELECT * FROM (SELECT id,s FROM lp_set_a " + operation
                                + " SELECT id,s FROM lp_set_b) ORDER BY id,s",
                        """
                                id	s
                                null\t
                                2	a
                                """
                );
            }
            {
                final String operation = OPERATIONS[4];
                assertRowsOnly(
                        "SELECT * FROM (SELECT id,s FROM lp_set_a " + operation
                                + " SELECT id,s FROM lp_set_b) ORDER BY id,s",
                        """
                                id	s
                                1	b
                                """
                );
            }
            {
                final String operation = OPERATIONS[5];
                assertRowsOnly(
                        "SELECT * FROM (SELECT id,s FROM lp_set_a " + operation
                                + " SELECT id,s FROM lp_set_b) ORDER BY id,s",
                        """
                                id	s
                                1	b
                                """
                );
            }
        });
    }

    @Test
    public void testTimestampMergeFlattensAndWidensWithoutLosingSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (ts TIMESTAMP, id INT, s SYMBOL) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_set_b (ts TIMESTAMP, id LONG, s SYMBOL) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_set_c (ts TIMESTAMP, id DOUBLE, s SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_set_a VALUES (0,1,'b'),(4,5,'a')");
            execute("INSERT INTO lp_set_b VALUES (1,2,'a'),(3,4,null)");
            execute("INSERT INTO lp_set_c VALUES (2,3,'c'),(5,6,'a')");
            {
                final String direction = "ASC";
                final String sql = "SELECT * FROM (SELECT ts,id,s FROM lp_set_a UNION ALL SELECT ts,id,s FROM lp_set_b"
                        + " UNION ALL SELECT ts,id,s FROM lp_set_c) ORDER BY ts " + direction;
                assertRowsOnly(sql, """
                        ts	id	s
                        1970-01-01T00:00:00.000000Z	1.0	b
                        1970-01-01T00:00:00.000001Z	2.0	a
                        1970-01-01T00:00:00.000002Z	3.0	c
                        1970-01-01T00:00:00.000003Z	4.0\t
                        1970-01-01T00:00:00.000004Z	5.0	a
                        1970-01-01T00:00:00.000005Z	6.0	a
                        """);
                try (RecordCursorFactory factory = compile(sql)) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan.getSink(), "Union All Merge");
                    TestUtils.assertContains(plan.getSink(), "branches: 3");
                    Assert.assertFalse(plan.getSink().toString(), plan.getSink().toString().contains("sort"));
                    Assert.assertFalse(plan.getSink().toString(), plan.getSink().toString().contains("Sort"));
                }
            }
            {
                final String direction = "DESC";
                final String sql = "SELECT * FROM (SELECT ts,id,s FROM lp_set_a UNION ALL SELECT ts,id,s FROM lp_set_b"
                        + " UNION ALL SELECT ts,id,s FROM lp_set_c) ORDER BY ts " + direction;
                assertRowsOnly(sql, """
                        ts	id	s
                        1970-01-01T00:00:00.000005Z	6.0	a
                        1970-01-01T00:00:00.000004Z	5.0	a
                        1970-01-01T00:00:00.000003Z	4.0\t
                        1970-01-01T00:00:00.000002Z	3.0	c
                        1970-01-01T00:00:00.000001Z	2.0	a
                        1970-01-01T00:00:00.000000Z	1.0	b
                        """);
                try (RecordCursorFactory factory = compile(sql)) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan.getSink(), "Union All Merge");
                    TestUtils.assertContains(plan.getSink(), "branches: 3");
                    Assert.assertFalse(plan.getSink().toString(), plan.getSink().toString().contains("sort"));
                    Assert.assertFalse(plan.getSink().toString(), plan.getSink().toString().contains("Sort"));
                }
            }
        });
    }

    @Test
    public void testTimestampMergeKeepsLimitBoundaryAndPrecisionFallback() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (ts TIMESTAMP, id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_set_b (ts TIMESTAMP, id INT) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_set_ns (ts TIMESTAMP_NS, id INT) TIMESTAMP(ts)");
            execute("INSERT INTO lp_set_a VALUES (0,1),(4,5)");
            execute("INSERT INTO lp_set_b VALUES (1,2),(3,4)");
            execute("INSERT INTO lp_set_ns VALUES (2000,3)");
            final String limited = "SELECT * FROM (SELECT * FROM (SELECT ts,id FROM lp_set_a LIMIT 1)"
                    + " UNION ALL SELECT ts,id FROM lp_set_b) ORDER BY ts DESC";
            assertRowsOnly(limited, """
                    ts	id
                    1970-01-01T00:00:00.000003Z	4
                    1970-01-01T00:00:00.000001Z	2
                    1970-01-01T00:00:00.000000Z	1
                    """);
            assertPlan(limited, "Encode sort", "Union All Merge");
            final String widened = "SELECT * FROM (SELECT ts,id FROM lp_set_a"
                    + " UNION ALL SELECT ts,id FROM lp_set_ns) ORDER BY ts";
            assertRowsOnly(widened, """
                    ts	id
                    1970-01-01T00:00:00.000000000Z	1
                    1970-01-01T00:00:00.000002000Z	3
                    1970-01-01T00:00:00.000004000Z	5
                    """);
            assertPlan(widened, "Encode sort", "Union All Merge");
            final String extraKey = "SELECT * FROM (SELECT ts,id FROM lp_set_a"
                    + " UNION ALL SELECT ts,id FROM lp_set_b) ORDER BY ts,id";
            assertRowsOnly(extraKey, """
                    ts	id
                    1970-01-01T00:00:00.000000Z	1
                    1970-01-01T00:00:00.000001Z	2
                    1970-01-01T00:00:00.000003Z	4
                    1970-01-01T00:00:00.000004Z	5
                    """);
            assertPlan(extraKey, "Encode sort", "Union All Merge");
        });
    }

    @Test
    public void testTimestampMergeRetainsFactoriesAfterCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts)");
            execute("CREATE TABLE lp_set_b (ts TIMESTAMP, s SYMBOL) TIMESTAMP(ts)");
            execute("INSERT INTO lp_set_a VALUES (0,'b'),(2,'a')");
            execute("INSERT INTO lp_set_b VALUES (1,'a'),(3,null)");
            final String sql = "SELECT * FROM (SELECT ts,s FROM lp_set_a UNION ALL SELECT ts,s FROM lp_set_b) ORDER BY ts";
            final RecordCursorFactory retained;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory ignored = compiler.compile(
                        "SELECT ts FROM lp_set_a UNION ALL SELECT ts FROM lp_set_b", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertNotNull(compiler.getPlanForTesting());
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (retained) {
                final TextPlanSink plan = new TextPlanSink();
                plan.of(retained, sqlExecutionContext);
                TestUtils.assertContains(plan.getSink(), "Union All Merge");
                assertFactory(retained).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns("""
                        ts	s
                        1970-01-01T00:00:00.000000Z	b
                        1970-01-01T00:00:00.000001Z	a
                        1970-01-01T00:00:00.000002Z	a
                        1970-01-01T00:00:00.000003Z\t
                        """);
            }
        });
    }

    @Test
    public void testSymbolDictionariesAndSetSegmentBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (s SYMBOL)");
            execute("CREATE TABLE lp_set_b (s SYMBOL)");
            execute("CREATE TABLE lp_set_c (s SYMBOL)");
            execute("INSERT INTO lp_set_a VALUES ('b'),('a'),(null),('b')");
            execute("INSERT INTO lp_set_b VALUES ('a'),('c'),(null)");
            execute("INSERT INTO lp_set_c VALUES ('b')");
            {
                final String operation = OPERATIONS[0];
                assertRowsOnly(
                        "SELECT * FROM (SELECT s FROM lp_set_a " + operation
                                + " SELECT s FROM lp_set_b) ORDER BY s",
                        """
                                s
                                
                                a
                                b
                                c
                                """
                );
            }
            {
                final String operation = OPERATIONS[1];
                assertRowsOnly(
                        "SELECT * FROM (SELECT s FROM lp_set_a " + operation
                                + " SELECT s FROM lp_set_b) ORDER BY s",
                        """
                                s
                                
                                
                                a
                                a
                                b
                                b
                                c
                                """
                );
            }
            {
                final String operation = OPERATIONS[2];
                assertRowsOnly(
                        "SELECT * FROM (SELECT s FROM lp_set_a " + operation
                                + " SELECT s FROM lp_set_b) ORDER BY s",
                        """
                                s
                                
                                a
                                """
                );
            }
            {
                final String operation = OPERATIONS[3];
                assertRowsOnly(
                        "SELECT * FROM (SELECT s FROM lp_set_a " + operation
                                + " SELECT s FROM lp_set_b) ORDER BY s",
                        """
                                s
                                
                                a
                                """
                );
            }
            {
                final String operation = OPERATIONS[4];
                assertRowsOnly(
                        "SELECT * FROM (SELECT s FROM lp_set_a " + operation
                                + " SELECT s FROM lp_set_b) ORDER BY s",
                        """
                                s
                                b
                                """
                );
            }
            {
                final String operation = OPERATIONS[5];
                assertRowsOnly(
                        "SELECT * FROM (SELECT s FROM lp_set_a " + operation
                                + " SELECT s FROM lp_set_b) ORDER BY s",
                        """
                                s
                                b
                                b
                                """
                );
            }
            assertRowsOnly(
                    "SELECT * FROM (SELECT s FROM lp_set_a UNION ALL SELECT s FROM lp_set_b"
                            + " UNION SELECT s FROM lp_set_c) ORDER BY s",
                    """
                            s
                            
                            a
                            b
                            c
                            """
            );
            assertRowsOnly(
                    "SELECT * FROM (SELECT s FROM lp_set_a UNION ALL SELECT s FROM lp_set_b"
                            + " EXCEPT SELECT s FROM lp_set_c) ORDER BY s",
                    """
                            s
                            
                            a
                            c
                            """
            );
        });
    }

    @Test
    public void testUnionAllPrunesByOrdinalAndReindexesSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (dropa STRING, keepa INT, syma SYMBOL)");
            execute("CREATE TABLE lp_set_b (keepb INT, symb SYMBOL, dropb LONG)");
            execute("INSERT INTO lp_set_a VALUES ('discard',1,'b'),('discard',2,'a')");
            execute("INSERT INTO lp_set_b VALUES (3,'a',10),(4,null,20)");
            final String sql = "SELECT s,id FROM (SELECT keepa id,dropa ignored,syma s FROM lp_set_a"
                    + " UNION ALL SELECT keepb otherid,dropb ignored2,symb others FROM lp_set_b) ORDER BY id";
            assertRowsOnly(sql, """
                    s	id
                    b	1
                    a	2
                    a	3
                    	4
                    """);
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertPruned(compiler.getPlanForTesting(), "dropa", "dropb");
                }
            }
        });
    }

    @Test
    public void testUnionAllPruningPreservesMixedSetEqualityTuples() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (id INT, tag SYMBOL)");
            execute("CREATE TABLE lp_set_b (id INT, tag SYMBOL)");
            execute("CREATE TABLE lp_set_c (id INT, tag SYMBOL)");
            execute("INSERT INTO lp_set_a VALUES (1,'a'),(1,'b'),(2,'c')");
            execute("INSERT INTO lp_set_b VALUES (1,'b'),(2,'c'),(3,'d')");
            execute("INSERT INTO lp_set_c VALUES (1,'c'),(2,'c')");
            {
                final String operation = OPERATIONS[0];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a " + operation
                                + " SELECT id,tag FROM lp_set_b UNION ALL SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                1
                                2
                                2
                                3
                                """
                );
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a UNION ALL SELECT id,tag FROM lp_set_b "
                                + operation + " SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                1
                                2
                                3
                                """
                );
            }
            {
                final String operation = OPERATIONS[1];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a " + operation
                                + " SELECT id,tag FROM lp_set_b UNION ALL SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                1
                                1
                                2
                                2
                                2
                                3
                                """
                );
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a UNION ALL SELECT id,tag FROM lp_set_b "
                                + operation + " SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                1
                                1
                                2
                                2
                                2
                                3
                                """
                );
            }
            {
                final String operation = OPERATIONS[2];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a " + operation
                                + " SELECT id,tag FROM lp_set_b UNION ALL SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                2
                                2
                                """
                );
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a UNION ALL SELECT id,tag FROM lp_set_b "
                                + operation + " SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                2
                                """
                );
            }
            {
                final String operation = OPERATIONS[3];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a " + operation
                                + " SELECT id,tag FROM lp_set_b UNION ALL SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                2
                                2
                                """
                );
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a UNION ALL SELECT id,tag FROM lp_set_b "
                                + operation + " SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                2
                                2
                                """
                );
            }
            {
                final String operation = OPERATIONS[4];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a " + operation
                                + " SELECT id,tag FROM lp_set_b UNION ALL SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                2
                                """
                );
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a UNION ALL SELECT id,tag FROM lp_set_b "
                                + operation + " SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                3
                                """
                );
            }
            {
                final String operation = OPERATIONS[5];
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a " + operation
                                + " SELECT id,tag FROM lp_set_b UNION ALL SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                2
                                """
                );
                assertRowsOnly(
                        "SELECT id FROM (SELECT id,tag FROM lp_set_a UNION ALL SELECT id,tag FROM lp_set_b "
                                + operation + " SELECT id,tag FROM lp_set_c) ORDER BY id",
                        """
                                id
                                1
                                1
                                1
                                3
                                """
                );
            }
        });
    }

    @Test
    public void testUnionAllPruningRetainsCardinalityAndBranchSortDependencies() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (id INT, rank INT)");
            execute("CREATE TABLE lp_set_b (id INT, rank INT)");
            execute("INSERT INTO lp_set_a VALUES (1,30),(2,10),(3,20)");
            execute("INSERT INTO lp_set_b VALUES (4,60),(5,40),(6,50)");
            assertRowsOnly(
                    "SELECT 7 v FROM (SELECT id,rank FROM lp_set_a UNION ALL SELECT id,rank FROM lp_set_b)",
                    """
                            v
                            7
                            7
                            7
                            7
                            7
                            7
                            """
            );
            assertRowsOnly(
                    "SELECT id FROM (SELECT * FROM (SELECT id,rank FROM lp_set_a ORDER BY rank LIMIT 2)"
                            + " UNION ALL SELECT * FROM (SELECT id,rank FROM lp_set_b ORDER BY rank DESC LIMIT 1)) ORDER BY id",
                    """
                            id
                            2
                            3
                            4
                            """
            );
        });
    }

    @Test
    public void testUnionSymbolRestorationWaitsUntilFlatSegmentEnds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_set_a (v SYMBOL)");
            execute("CREATE TABLE lp_set_b (v SYMBOL)");
            execute("CREATE TABLE lp_set_c (v DOUBLE[])");
            execute("INSERT INTO lp_set_a VALUES ('[1.0]')");
            execute("INSERT INTO lp_set_b VALUES ('[2.0]')");
            execute("INSERT INTO lp_set_c VALUES (ARRAY[3.0])");
            assertRowsOnly(
                    "SELECT * FROM (SELECT v FROM lp_set_a UNION ALL SELECT v FROM lp_set_b"
                            + " UNION ALL SELECT v FROM lp_set_c) ORDER BY v",
                    """
                            v
                            [1.0]
                            [2.0]
                            [3.0]
                            """
            );
            assertRowsOnly(
                    "SELECT * FROM (SELECT * FROM (SELECT v FROM lp_set_a UNION ALL SELECT v FROM lp_set_b)"
                            + " UNION ALL SELECT v FROM lp_set_c) ORDER BY v",
                    """
                            v
                            [1.0]
                            [2.0]
                            [3.0]
                            """
            );
        });
    }

    private RecordCursorFactory compile(String sql) throws SqlException {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            final RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try {
                Assert.assertTrue(containsSetOperation(compiler.getPlanForTesting()));
            } catch (Throwable th) {
                factory.close();
                throw th;
            }
            return factory;
        }
    }

    private void assertPlan(String sql, String present, String absent) throws SqlException {
        try (RecordCursorFactory factory = compile(sql)) {
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, sqlExecutionContext);
            TestUtils.assertContains(plan.getSink(), present);
            Assert.assertFalse(plan.getSink().toString(), plan.getSink().toString().contains(absent));
        }
    }

    private static void assertPruned(LogicalPlan plan, String leftUnused, String rightUnused) {
        if (plan instanceof ScanPlan) {
            Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet(leftUnused));
            Assert.assertEquals(-1, plan.getOutput().getColumnIndexQuiet(rightUnused));
            Assert.assertEquals(2, plan.getOutput().getColumnCount());
        } else if (plan instanceof SetOperationPlan) {
            Assert.assertEquals(2, plan.getOutput().getColumnCount());
            Assert.assertEquals(2, plan.inputAt(0).getOutput().getColumnCount());
            Assert.assertEquals(2, plan.inputAt(1).getOutput().getColumnCount());
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            assertPruned(plan.inputAt(i), leftUnused, rightUnused);
        }
    }

    private static boolean containsSetOperation(LogicalPlan plan) {
        if (plan instanceof SetOperationPlan) {
            return true;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            if (containsSetOperation(plan.inputAt(i))) {
                return true;
            }
        }
        return false;
    }
}
