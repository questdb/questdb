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
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RoundTest extends AbstractCairoTest {
    private static final ObjList<String> FUNCTION_NAMES = new ObjList<>("round", "round_down", "round_up", "round_half_even");

    @Test
    public void testConstantAndDynamicScaleSpecializations() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final int i = 0;
                final String name = FUNCTION_NAMES.getQuick(i);
                assertQueryRows(
                        "SELECT id," + name + "(d,0) zero," + name + "(d,2) positive," + name
                                + "(d,-1) negative," + name + "(d,scale) dynamic," + name + "(d::FLOAT,2) promoted FROM lp_round ORDER BY id",
                        """
                                id	zero	positive	negative	dynamic	promoted
                                1	15.0	14.780000000000001	10.0	14.778	14.780000000000001
                                2	-123.0	-123.46000000000001	-120.0	-120.0	-123.46000000000001
                                3	235.0	234.5	230.0	235.0	234.5
                                4	0.0	-0.0	-0.0	-0.0	-0.0
                                5	null	null	null	null	null
                                6	3.0	3.15	0.0	null	3.15
                                7	15.0	14.780000000000001	10.0	null	14.780000000000001
                                8	-123.0	-123.46000000000001	-120.0	null	-123.46000000000001
                                """
                );
                assertQueryRows(
                        "SELECT id," + name + "(d,15) high," + name + "(d,18) boundary," + name
                                + "(d,-18) negative_boundary FROM lp_round ORDER BY id",
                        """
                                id	high	boundary	negative_boundary
                                1	14.777800000000001	null	null
                                2	-123.456	null	null
                                3	234.50000000000003	null	null
                                4	-1.0E-15	null	null
                                5	null	null	null
                                6	3.1450000000000014	null	null
                                7	14.777800000000001	null	null
                                8	-123.456	null	null
                                """
                );
            }
            {
                final int i = 1;
                final String name = FUNCTION_NAMES.getQuick(i);
                assertQueryRows(
                        "SELECT id," + name + "(d,0) zero," + name + "(d,2) positive," + name
                                + "(d,-1) negative," + name + "(d,scale) dynamic," + name + "(d::FLOAT,2) promoted FROM lp_round ORDER BY id",
                        """
                                id	zero	positive	negative	dynamic	promoted
                                1	14.0	14.77	10.0	14.777000000000001	14.77
                                2	-123.0	-123.45	-120.0	-120.0	-123.45
                                3	234.0	234.5	230.0	234.0	234.5
                                4	-0.0	-0.0	-0.0	-0.0	-0.0
                                5	null	null	null	null	null
                                6	3.0	3.14	0.0	null	3.14
                                7	14.0	14.77	10.0	null	14.77
                                8	-123.0	-123.45	-120.0	null	-123.45
                                """
                );
                assertQueryRows(
                        "SELECT id," + name + "(d,15) high," + name + "(d,18) boundary," + name
                                + "(d,-18) negative_boundary FROM lp_round ORDER BY id",
                        """
                                id	high	boundary	negative_boundary
                                1	14.777800000000001	null	null
                                2	-123.456	null	null
                                3	234.50000000000003	null	null
                                4	-1.0E-15	null	null
                                5	null	null	null
                                6	3.1450000000000014	null	null
                                7	14.777800000000001	null	null
                                8	-123.456	null	null
                                """
                );
            }
            {
                final int i = 2;
                final String name = FUNCTION_NAMES.getQuick(i);
                assertQueryRows(
                        "SELECT id," + name + "(d,0) zero," + name + "(d,2) positive," + name
                                + "(d,-1) negative," + name + "(d,scale) dynamic," + name + "(d::FLOAT,2) promoted FROM lp_round ORDER BY id",
                        """
                                id	zero	positive	negative	dynamic	promoted
                                1	15.0	14.780000000000001	20.0	14.778	14.780000000000001
                                2	-124.0	-123.46000000000001	-130.0	-130.0	-123.46000000000001
                                3	235.0	234.51	240.0	235.0	234.51
                                4	-0.0	-0.0	-0.0	-0.0	-0.0
                                5	null	null	null	null	null
                                6	4.0	3.15	10.0	null	3.15
                                7	15.0	14.780000000000001	20.0	null	14.780000000000001
                                8	-124.0	-123.46000000000001	-130.0	null	-123.46000000000001
                                """
                );
                assertQueryRows(
                        "SELECT id," + name + "(d,15) high," + name + "(d,18) boundary," + name
                                + "(d,-18) negative_boundary FROM lp_round ORDER BY id",
                        """
                                id	high	boundary	negative_boundary
                                1	14.777800000000001	null	null
                                2	-123.456	null	null
                                3	234.50000000000003	null	null
                                4	-0.0	null	null
                                5	null	null	null
                                6	3.1450000000000014	null	null
                                7	14.777800000000001	null	null
                                8	-123.456	null	null
                                """
                );
            }
            {
                final int i = 3;
                final String name = FUNCTION_NAMES.getQuick(i);
                assertQueryRows(
                        "SELECT id," + name + "(d,0) zero," + name + "(d,2) positive," + name
                                + "(d,-1) negative," + name + "(d,scale) dynamic," + name + "(d::FLOAT,2) promoted FROM lp_round ORDER BY id",
                        """
                                id	zero	positive	negative	dynamic	promoted
                                1	15.0	14.780000000000001	10.0	14.778	14.780000000000001
                                2	-123.0	-123.46000000000001	-120.0	-120.0	-123.46000000000001
                                3	234.0	234.5	230.0	234.0	234.5
                                4	-0.0	-0.0	-0.0	-0.0	-0.0
                                5	null	null	null	null	null
                                6	3.0	3.14	0.0	null	3.14
                                7	15.0	14.780000000000001	10.0	null	14.780000000000001
                                8	-123.0	-123.46000000000001	-120.0	null	-123.46000000000001
                                """
                );
                assertQueryRows(
                        "SELECT id," + name + "(d,15) high," + name + "(d,18) boundary," + name
                                + "(d,-18) negative_boundary FROM lp_round ORDER BY id",
                        """
                                id	high	boundary	negative_boundary
                                1	14.777800000000001	null	null
                                2	-123.456	null	null
                                3	234.50000000000003	null	null
                                4	-1.0E-15	null	null
                                5	null	null	null
                                6	3.1450000000000014	null	null
                                7	14.777800000000001	null	null
                                8	-123.456	null	null
                                """
                );
            }
        });
    }

    @Test
    public void testDiscardedNativeValueClosesOnBothCompilerPaths() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final ObjList<String> scales = new ObjList<>("null::INT", "1000", "-1000");
            for (int i = 0; i < FUNCTION_NAMES.size(); i++) {
                for (int j = 0; j < scales.size(); j++) {
                    assertQueryRows(
                            "SELECT id," + FUNCTION_NAMES.getQuick(i)
                                    + "(CASE WHEN id IN (1,2,3) THEN d ELSE d+1 END," + scales.getQuick(j)
                                    + ") value FROM lp_round ORDER BY id",
                            """
                                    id	value
                                    1	null
                                    2	null
                                    3	null
                                    4	null
                                    5	null
                                    6	null
                                    7	null
                                    8	null
                                    """
                    );
                }
            }
        });
    }

    @Test
    public void testFailedBindingAfterDiscardAndCompilerRecovery() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            for (int path = 0; path < 2; path++) {
                try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                    for (int i = 0; i < FUNCTION_NAMES.size(); i++) {
                        final String expression = FUNCTION_NAMES.getQuick(i)
                                + "(CASE WHEN id IN (1,2,3) THEN d ELSE d+1 END,null::INT)";
                        assertInvalid(compiler, "SELECT " + expression + "+no_such_function(id) FROM lp_round");
                        assertInvalid(compiler, "SELECT no_such_function(id)+" + expression + " FROM lp_round");
                    }
                    try (RecordCursorFactory factory = compiler.compile(
                            "SELECT round(1.25,1) value FROM lp_round LIMIT 1", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        assertResult(factory, "value\n1.3\n");
                    }
                }
            }
        });
    }

    @Test
    public void testNativeValueSurvivesResetForRetainedScaleBranches() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRetainedScaleBranches("round", """
                    id	positive	negative	dynamic
                    1	14.780000000000001	10.0	14.778
                    2	-123.46000000000001	-120.0	-120.0
                    3	234.5	230.0	235.0
                    4	1.0	0.0	1.0
                    5	null	null	null
                    6	4.15	0.0	null
                    7	15.780000000000001	20.0	null
                    8	-122.46000000000001	-120.0	null
                    """);
            assertRetainedScaleBranches("round_down", """
                    id	positive	negative	dynamic
                    1	14.77	10.0	14.777000000000001
                    2	-123.45	-120.0	-120.0
                    3	234.5	230.0	234.0
                    4	1.0	0.0	1.0
                    5	null	null	null
                    6	4.14	0.0	null
                    7	15.77	10.0	null
                    8	-122.45	-120.0	null
                    """);
            assertRetainedScaleBranches("round_up", """
                    id	positive	negative	dynamic
                    1	14.780000000000001	20.0	14.778
                    2	-123.46000000000001	-130.0	-130.0
                    3	234.51	240.0	235.0
                    4	1.01	10.0	1.01
                    5	null	null	null
                    6	4.15	10.0	null
                    7	15.780000000000001	20.0	null
                    8	-122.46000000000001	-130.0	null
                    """);
            assertRetainedScaleBranches("round_half_even", """
                    id	positive	negative	dynamic
                    1	14.780000000000001	10.0	14.778
                    2	-123.46000000000001	-120.0	-120.0
                    3	234.5	230.0	234.0
                    4	1.0	0.0	1.0
                    5	null	null	null
                    6	4.14	0.0	null
                    7	15.780000000000001	20.0	null
                    8	-122.46000000000001	-120.0	null
                    """);
        });
    }

    @Test
    public void testRuntimeScaleRefreshesOnCursorInitialization() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertRuntimeScale(
                    "round",
                    "id\tvalue\n1\t14.780000000000001\n2\t-123.46000000000001\n3\t234.5\n4\t-0.0\n5\tnull\n6\t3.15\n7\t14.780000000000001\n8\t-123.46000000000001\n",
                    "id\tvalue\n1\t10.0\n2\t-120.0\n3\t230.0\n4\t-0.0\n5\tnull\n6\t0.0\n7\t10.0\n8\t-120.0\n",
                    "id\tvalue\n1\t15.0\n2\t-123.0\n3\t235.0\n4\t-0.0\n5\tnull\n6\t3.0\n7\t15.0\n8\t-123.0\n"
            );
            assertRuntimeScale(
                    "round_down",
                    "id\tvalue\n1\t14.77\n2\t-123.45\n3\t234.5\n4\t-0.0\n5\tnull\n6\t3.14\n7\t14.77\n8\t-123.45\n",
                    "id\tvalue\n1\t10.0\n2\t-120.0\n3\t230.0\n4\t-0.0\n5\tnull\n6\t0.0\n7\t10.0\n8\t-120.0\n",
                    "id\tvalue\n1\t14.0\n2\t-123.0\n3\t234.0\n4\t-0.0\n5\tnull\n6\t3.0\n7\t14.0\n8\t-123.0\n"
            );
            assertRuntimeScale(
                    "round_up",
                    "id\tvalue\n1\t14.780000000000001\n2\t-123.46000000000001\n3\t234.51\n4\t-0.0\n5\tnull\n6\t3.15\n7\t14.780000000000001\n8\t-123.46000000000001\n",
                    "id\tvalue\n1\t20.0\n2\t-130.0\n3\t240.0\n4\t-0.0\n5\tnull\n6\t10.0\n7\t20.0\n8\t-130.0\n",
                    "id\tvalue\n1\t15.0\n2\t-124.0\n3\t235.0\n4\t-0.0\n5\tnull\n6\t4.0\n7\t15.0\n8\t-124.0\n"
            );
            assertRuntimeScale(
                    "round_half_even",
                    "id\tvalue\n1\t14.780000000000001\n2\t-123.46000000000001\n3\t234.5\n4\t-0.0\n5\tnull\n6\t3.14\n7\t14.780000000000001\n8\t-123.46000000000001\n",
                    "id\tvalue\n1\t10.0\n2\t-120.0\n3\t230.0\n4\t-0.0\n5\tnull\n6\t0.0\n7\t10.0\n8\t-120.0\n",
                    "id\tvalue\n1\t15.0\n2\t-123.0\n3\t234.0\n4\t-0.0\n5\tnull\n6\t3.0\n7\t15.0\n8\t-123.0\n"
            );
        });
    }

    private void assertInvalid(SqlCompilerImpl compiler, String sql) throws Exception {
        try (RecordCursorFactory ignored = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
            Assert.fail(sql);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "no_such_function");
        }
    }

    private void assertRetainedScaleBranches(String name, String expected) throws Exception {
        final String value = "CASE WHEN id IN (1,2,3) THEN d ELSE d+1 END";
        final String sql = "SELECT id," + name + "(" + value + ",2) positive," + name + "(" + value
                + ",-1) negative," + name + "(" + value + ",scale) dynamic FROM lp_round ORDER BY id";
        final RecordCursorFactory retained;
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
            try (RecordCursorFactory ignored = compiler.compile("SELECT id FROM lp_round", sqlExecutionContext).getRecordCursorFactory()) {
                Assert.assertNotNull(ignored);
            } catch (Throwable th) {
                retained.close();
                throw th;
            }
        }
        try (RecordCursorFactory factory = retained) {
            assertResult(factory, expected);
        }
    }

    private void assertRuntimeScale(String function, String scaleTwo, String scaleMinusOne, String scaleZero) throws Exception {
        final String outOfRange = "id\tvalue\n1\tnull\n2\tnull\n3\tnull\n4\tnull\n5\tnull\n6\tnull\n7\tnull\n8\tnull\n";
        bindVariableService.setInt(0, 2);
        try (RecordCursorFactory factory = select("SELECT id," + function + "(d,$1) value FROM lp_round ORDER BY id")) {
            assertResult(factory, scaleTwo);
            bindVariableService.setInt(0, -1);
            assertResult(factory, scaleMinusOne);
            bindVariableService.setInt(0, 0);
            assertResult(factory, scaleZero);
            bindVariableService.setInt(0, Numbers.INT_NULL);
            assertResult(factory, outOfRange);
            bindVariableService.setInt(0, 1000);
            assertResult(factory, outOfRange);
        }
    }

    private void assertResult(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_round(unused INT,id INT,d DOUBLE,scale INT)");
        execute("""
                INSERT INTO lp_round VALUES
                (91,1,14.7778,3),
                (92,2,-123.456,-1),
                (93,3,234.5,0),
                (94,4,-0.0,2),
                (95,5,null,2),
                (96,6,3.145,null),
                (97,7,14.7778,1000),
                (98,8,-123.456,-1000)
                """);
    }

    private void assertQueryRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }
}
