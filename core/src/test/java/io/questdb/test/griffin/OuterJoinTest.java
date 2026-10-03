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
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class OuterJoinTest extends AbstractCairoTest {
    @Test
    public void testEmptyInputsAndConstantFalsePreserveOuterRows() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.k=r.k AND false ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "lp_outer_r r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            "lid\trid\n");
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            "lid\trid\n");
                }
            }
            {
                final String join = " FULL JOIN ";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.k=r.k AND false ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    1	null
                                    2	null
                                    3	null
                                    4	null
                                    5	null
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "lp_outer_r r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    null	10
                                    null	11
                                    null	12
                                    null	13
                                    null	14
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            """
                                    lid	rid
                                    1	null
                                    2	null
                                    3	null
                                    4	null
                                    5	null
                                    """);
                    assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                    + "(SELECT * FROM lp_outer_r WHERE false) r ON l.k=r.k ORDER BY lid,rid", isFullFat, null,
                            "lid\trid\n");
                }
            }
        });
    }

    @Test
    public void testKeyedOuterPredicatesAndNativeIn() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                final String label = "Right";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    final String plan = "Hash " + label + " Outer Join" + (isFullFat ? "" : " Light");
                    final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                    assertRows(prefix + "l.k=r.k ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    1	10
                                    1	11
                                    3	12
                                    4	13
                                    """);
                    assertRows(prefix + "l.k=r.k AND l.v<r.v ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	13
                                    null	14
                                    1	10
                                    1	11
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	11
                                    null	13
                                    null	14
                                    1	10
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    1	10
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE l.id=null ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.id=null ORDER BY lid,rid", isFullFat, plan, "lid\trid\n");
                    assertRows("SELECT l.id lid,r.id rid,l.s ls,r.s rs FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.s=r.s ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid	ls	rs
                                    null	14		f
                                    1	10	a	a
                                    1	11	a	a
                                    3	12	c	c
                                    4	13	\t
                                    """);
                }
            }
            {
                final String join = " FULL JOIN ";
                final String label = "Full";
                for (boolean isFullFat : new boolean[]{false, true}) {
                    final String plan = "Hash " + label + " Outer Join" + (isFullFat ? "" : " Light");
                    final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                    assertRows(prefix + "l.k=r.k ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    1	10
                                    1	11
                                    2	null
                                    3	12
                                    4	13
                                    5	null
                                    """);
                    assertRows(prefix + "l.k=r.k AND l.v<r.v ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	13
                                    null	14
                                    1	10
                                    1	11
                                    2	null
                                    3	12
                                    4	null
                                    5	null
                                    """);
                    assertRows(prefix + "l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	11
                                    null	13
                                    null	14
                                    1	10
                                    2	null
                                    3	12
                                    4	null
                                    5	null
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.v IN (11,31) ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    1	10
                                    3	12
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE l.id=null ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    null	14
                                    """);
                    assertRows(prefix + "l.k=r.k WHERE r.id=null ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid
                                    2	null
                                    5	null
                                    """);
                    assertRows("SELECT l.id lid,r.id rid,l.s ls,r.s rs FROM lp_outer_l l" + join
                                    + "lp_outer_r r ON l.s=r.s ORDER BY lid,rid", isFullFat, plan,
                            """
                                    lid	rid	ls	rs
                                    null	14		f
                                    1	10	a	a
                                    1	11	a	a
                                    2	null	b\t
                                    3	12	c	c
                                    4	13	\t
                                    5	null	e\t
                                    """);
                }
            }
        });
    }

    @Test
    public void testNestedLoopOuterPredicatesAndConstantConditions() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                final String plan = "Nested Loop Right Join";
                final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                assertRows(prefix + "l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                1	10
                                1	11
                                1	12
                                1	14
                                2	12
                                2	14
                                3	12
                                3	14
                                5	14
                                """);
                assertRows(prefix + "false ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows(prefix + "true ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                1	10
                                1	11
                                1	12
                                1	13
                                1	14
                                2	10
                                2	11
                                2	12
                                2	13
                                2	14
                                3	10
                                3	11
                                3	12
                                3	13
                                3	14
                                4	10
                                4	11
                                4	12
                                4	13
                                4	14
                                5	10
                                5	11
                                5	12
                                5	13
                                5	14
                                """);
                assertRows(prefix + "l.v<r.v WHERE l.id=null OR r.id=null ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                + "lp_outer_r r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                + "(SELECT * FROM lp_outer_r WHERE false) r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        "lid\trid\n");
            }
            {
                final String join = " FULL JOIN ";
                final String plan = "Nested Loop Full Join";
                final String prefix = "SELECT l.id lid,r.id rid FROM lp_outer_l l" + join + "lp_outer_r r ON ";
                assertRows(prefix + "l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                1	10
                                1	11
                                1	12
                                1	14
                                2	12
                                2	14
                                3	12
                                3	14
                                4	null
                                5	14
                                """);
                assertRows(prefix + "false ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                1	null
                                2	null
                                3	null
                                4	null
                                5	null
                                """);
                assertRows(prefix + "true ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                1	10
                                1	11
                                1	12
                                1	13
                                1	14
                                2	10
                                2	11
                                2	12
                                2	13
                                2	14
                                3	10
                                3	11
                                3	12
                                3	13
                                3	14
                                4	10
                                4	11
                                4	12
                                4	13
                                4	14
                                5	10
                                5	11
                                5	12
                                5	13
                                5	14
                                """);
                assertRows(prefix + "l.v<r.v WHERE l.id=null OR r.id=null ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	13
                                4	null
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM (SELECT * FROM lp_outer_l WHERE false) l" + join
                                + "lp_outer_r r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                null	10
                                null	11
                                null	12
                                null	13
                                null	14
                                """);
                assertRows("SELECT l.id lid,r.id rid FROM lp_outer_l l" + join
                                + "(SELECT * FROM lp_outer_r WHERE false) r ON l.v<r.v ORDER BY lid,rid", false, plan,
                        """
                                lid	rid
                                1	null
                                2	null
                                3	null
                                4	null
                                5	null
                                """);
            }
        });
    }

    @Test
    public void testOuterFactoryAndExplainSurviveCompilerReuse() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final String sql = "SELECT l.id lid,r.id rid FROM lp_outer_l l FULL JOIN lp_outer_r r "
                    + "ON l.k=r.k AND r.v IN (11,31) ORDER BY lid,rid";
            final RecordCursorFactory retained;
            final String expected = """
                    lid	rid
                    null	11
                    null	13
                    null	14
                    1	10
                    2	null
                    3	12
                    4	null
                    5	null
                    """;
            final String expectedPlan;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
                }
                retained = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try {
                    expectedPlan = plan(retained);
                    try (RecordCursorFactory ignored = compiler.compile("SELECT count() FROM lp_outer_l", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(compiler.getPlanForTesting());
                    }
                } catch (Throwable th) {
                    retained.close();
                    throw th;
                }
            }
            try (RecordCursorFactory factory = retained) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                TestUtils.assertEquals(expectedPlan, plan(factory));
            }
        });
    }

    @Test
    public void testTimestampDesignationClearedForBothOuterSides() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            {
                final String join = " RIGHT JOIN ";
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.k=r.k",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-04T00:00:00.000000Z	2020-01-04T00:00:00.000000Z
                                	2020-01-05T00:00:00.000000Z
                                """);
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.v<r.v",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                	2020-01-04T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-05T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                """);
            }
            {
                final String join = " FULL JOIN ";
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.k=r.k",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z\t
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-04T00:00:00.000000Z	2020-01-04T00:00:00.000000Z
                                2020-01-05T00:00:00.000000Z\t
                                	2020-01-05T00:00:00.000000Z
                                """);
                assertRowsWithoutTimestamp("SELECT l.ts lts,r.ts rts FROM lp_outer_l l" + join + "lp_outer_r r ON l.v<r.v",
                        """
                                lts	rts
                                2020-01-01T00:00:00.000000Z	2020-01-01T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-02T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-01T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-02T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-03T00:00:00.000000Z
                                2020-01-03T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                2020-01-04T00:00:00.000000Z\t
                                2020-01-05T00:00:00.000000Z	2020-01-05T00:00:00.000000Z
                                	2020-01-04T00:00:00.000000Z
                                """);
            }
        });
    }

    private void assertRows(String sql, boolean isFullFat, String expectedPlan, String expected) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            compiler.setFullFatJoins(isFullFat);
            try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                if (expectedPlan != null) {
                    TestUtils.assertContains(plan(factory), expectedPlan);
                }
            }
        }
    }

    private void assertRowsWithoutTimestamp(String sql, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertEquals(-1, factory.getMetadata().getTimestampIndex());
        }
        assertRows(sql, false, null, expected);
    }

    private void createRows() throws Exception {
        execute("CREATE TABLE lp_outer_l(id INT,k INT,v INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("CREATE TABLE lp_outer_r(id INT,k INT,v INT,s SYMBOL,ts TIMESTAMP) TIMESTAMP(ts)");
        execute("INSERT INTO lp_outer_l VALUES (1,1,10,'a','2020-01-01'),(2,2,20,'b','2020-01-02'),"
                + "(3,3,30,'c','2020-01-03'),(4,null,null,null,'2020-01-04'),(5,5,50,'e','2020-01-05')");
        execute("INSERT INTO lp_outer_r VALUES (10,1,11,'a','2020-01-01'),(11,1,12,'a','2020-01-02'),"
                + "(12,3,31,'c','2020-01-03'),(13,null,null,null,'2020-01-04'),(14,6,60,'f','2020-01-05')");
    }

    private String plan(RecordCursorFactory factory) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        final StringSink text = new StringSink();
        for (int i = 1, n = sink.getLineCount(); i <= n; i++) {
            text.put(sink.getLine(i)).put('\n');
        }
        return text.toString();
    }
}
