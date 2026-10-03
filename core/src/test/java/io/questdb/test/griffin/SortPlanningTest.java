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
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.TextPlanSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class SortPlanningTest extends AbstractCairoTest {
    @Test
    public void testAsyncTopKFromNativeScan() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_async_top (id INT,label STRING)");
            execute("INSERT INTO lp_async_top VALUES (3,'c'),(1,'a'),(null,null),(2,'b'),(2,'a')");
            final boolean wasParallelTopK = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(true);
            try {
                assertOrder("SELECT id,label FROM lp_async_top ORDER BY id DESC,label LIMIT 3", "Async Top K", """
                        id	label
                        3	c
                        2	a
                        2	b
                        """);
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallelTopK);
            }
        });
    }

    @Test
    public void testComparatorMaterializesComputedSortKeys() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, "false");
        setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MATERIALIZATION_THRESHOLD, "1");
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_materialize (id INT)");
            execute("INSERT INTO lp_materialize VALUES (3),(1),(null),(2)");
            assertOrder("SELECT (id+1)*(id+2) AS value FROM lp_materialize ORDER BY value", "Materialize sort keys", """
                    value
                    null
                    6
                    12
                    20
                    """);
        });
    }

    @Test
    public void testComparatorSortWhenEncodingDisabled() throws Exception {
        setProperty(PropertyKey.CAIRO_SQL_ORDER_BY_SORT_ENABLED, "false");
        assertSorts(false);
    }

    @Test
    public void testEncodedSortWithRandomAndSequentialInputs() throws Exception {
        assertSorts(true);
    }

    @Test
    public void testGroupedLongTopK() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_top (label STRING,value INT)");
            execute("INSERT INTO lp_top VALUES ('a',1),('b',2),('a',3),('c',6),(null,null)");
            assertOrder("SELECT label,sum(value) AS total FROM lp_top GROUP BY label ORDER BY total DESC LIMIT 2", "Long Top K", """
                    label	total
                    c	6
                    a	4
                    """);
        });
    }

    @Test
    public void testLimitThroughColumnProjectionAndTimestampPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_order_ts (id INT,label STRING,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_order_ts VALUES (3,'c','2020-01-01'),(1,'a','2020-01-01'),(4,'d','2020-01-02'),(2,'b','2020-01-02')");
            final boolean wasParallelTopK = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(false);
            try {
                assertOrder("SELECT label FROM lp_order_ts ORDER BY id DESC LIMIT 2", "Encode sort light", """
                        label
                        d
                        c
                        """);
                assertOrder("SELECT label AS name,id AS value FROM lp_order_ts ORDER BY id DESC LIMIT 1,3", "Encode sort light", """
                        name	value
                        c	3
                        b	2
                        """);
                assertOrder("SELECT label FROM lp_order_ts ORDER BY ts,id LIMIT 1,3", "Encode sort light", """
                        label
                        c
                        b
                        """);
                assertOrder("SELECT label FROM lp_order_ts ORDER BY ts DESC,id DESC LIMIT -2", "Encode sort light", """
                        label
                        c
                        a
                        """);
                assertOrder("SELECT label FROM lp_order_ts ORDER BY ts DESC LIMIT 2", "Limit", """
                        label
                        b
                        d
                        """);
                bindVariableService.setLong(0, 1);
                bindVariableService.setLong(1, 3);
                try (RecordCursorFactory factory = select("SELECT label FROM lp_order_ts ORDER BY id LIMIT $1,$2")) {
                    assertRows(factory, """
                            label
                            b
                            c
                            """);
                    bindVariableService.setLong(0, -3);
                    bindVariableService.setLong(1, -1);
                    assertRows(factory, """
                            label
                            b
                            c
                            """);
                    bindVariableService.setLong(0, 1);
                    bindVariableService.setLong(1, -1);
                    assertRows(factory, """
                            label
                            b
                            c
                            """);
                }
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallelTopK);
            }
        });
    }

    @Test
    public void testNestedAggregateOrderKeyStaysHidden() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_order_agg (id INT,k INT,i INT)");
            execute("INSERT INTO lp_order_agg VALUES (1,1,1),(2,1,2),(3,2,3),(4,3,null),(5,4,null)");
            final String ordered = "SELECT k,sum(i+2) AS total FROM lp_order_agg GROUP BY k ORDER BY sum(i*2),k LIMIT 3";
            final String expected = """
                    k\ttotal
                    3\tnull
                    4\tnull
                    1\t7
                    """;
            assertNestedRows(ordered, expected);
            assertNestedRows(
                    "(" + ordered + ") UNION ALL (" + ordered + ")",
                    """
                            k\ttotal
                            3\tnull
                            4\tnull
                            1\t7
                            3\tnull
                            4\tnull
                            1\t7
                            """
            );
            assertNestedRows("SELECT * FROM (" + ordered + ")", expected);
            assertNestedRows("WITH q AS (" + ordered + ") SELECT * FROM q", expected);
            assertNestedRows("DECLARE @q := (" + ordered + ") SELECT * FROM @q", expected);
            assertNestedRows(
                    "SELECT * FROM (" + ordered + ") UNION ALL SELECT * FROM (" + ordered + ")",
                    """
                            k\ttotal
                            3\tnull
                            4\tnull
                            1\t7
                            3\tnull
                            4\tnull
                            1\t7
                            """
            );
            assertNestedRows(
                    "SELECT * FROM (" + ordered + ") q JOIN lp_order_agg t ON q.k = t.id",
                    """
                            k\ttotal\tid\tk1\ti
                            3\tnull\t3\t2\t3
                            4\tnull\t4\t3\tnull
                            1\t7\t1\t1\t1
                            """
            );
            assertNestedRows("SELECT q.* FROM (" + ordered + ") q JOIN lp_order_agg t ON q.k = t.id", expected);
            assertNestedRows(
                    "SELECT * FROM (SELECT k,sum(i) AS total FROM lp_order_agg GROUP BY k ORDER BY sum(i)+1,sum(i*2),k)",
                    """
                            k\ttotal
                            3\tnull
                            4\tnull
                            1\t3
                            2\t3
                            """
            );
            assertException("SELECT sum FROM (" + ordered + ")", 7, "Invalid column: sum");
            final String distinct = "SELECT DISTINCT sum(i) AS total FROM lp_order_agg GROUP BY k ORDER BY sum(i*2)";
            assertException(distinct, 70, "ORDER BY expressions must appear in select list. Invalid column: sum");
            assertException("SELECT * FROM (" + distinct + ")", 85, "ORDER BY expressions must appear in select list. Invalid column: sum");
            assertQuery(ordered).noLeakCheck().assertsPlan("""
                    SelectedRecord
                        Encode sort light lo: 3
                          keys: [sum, k]
                            VirtualRecord
                              functions: [k,sum+COUNT*2,sum*2]
                                GroupBy vectorized: true workers: 1
                                  keys: [k]
                                  values: [sum(i),count(i)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_order_agg
                    """);
            assertQuery("SELECT * FROM (" + ordered + ")").noLeakCheck().assertsPlan("""
                    SelectedRecord
                        Encode sort light lo: 3
                          keys: [sum, k]
                            VirtualRecord
                              functions: [k,sum+COUNT*2,sum*2]
                                GroupBy vectorized: true workers: 1
                                  keys: [k]
                                  values: [sum(i),count(i)]
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: lp_order_agg
                    """);
        });
    }

    private void assertNestedRows(String sql, String expected) throws Exception {
        assertQuery(sql).noLeakCheck().inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
    }

    private void assertSorts(boolean encoded) throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(encoded, configuration.isSqlOrderBySortEnabled());
            execute("CREATE TABLE lp_sort (id INT,label STRING,note VARCHAR)");
            execute("INSERT INTO lp_sort VALUES (3,'c','β'),(1,'a','a'),(null,null,null),(2,'b','c')");
            assertOrder("SELECT id,label,note FROM lp_sort ORDER BY id DESC", encoded ? "Encode sort light" : "Sort light", """
                    id	label	note
                    3	c	β
                    2	b	c
                    1	a	a
                    null	\t
                    """);
            assertOrder("SELECT id,label,note FROM lp_sort ORDER BY label,note DESC,id", encoded ? "Encode sort light" : "Sort light", """
                    id	label	note
                    null	\t
                    1	a	a
                    2	b	c
                    3	c	β
                    """);
            assertOrder("SELECT * FROM (SELECT * FROM lp_sort UNION ALL SELECT * FROM lp_sort) ORDER BY id,label DESC",
                    encoded ? "Encode sort" : "Sort", """
                            id	label	note
                            null	\t
                            null	\t
                            1	a	a
                            1	a	a
                            2	b	c
                            2	b	c
                            3	c	β
                            3	c	β
                            """);
            assertOrder("SELECT id+1 AS value FROM lp_sort ORDER BY value", encoded ? "Encode sort light" : "Sort light", """
                    value
                    null
                    2
                    3
                    4
                    """);
            final boolean wasParallelTopK = sqlExecutionContext.isParallelTopKEnabled();
            sqlExecutionContext.setParallelTopKEnabled(false);
            try {
                final String algorithm = encoded ? "Encode sort light lo:" : "Sort light lo:";
                assertOrder("SELECT id FROM lp_sort ORDER BY id DESC LIMIT 2", algorithm, """
                        id
                        3
                        2
                        """);
                assertOrder("SELECT id FROM lp_sort ORDER BY id DESC LIMIT -2", algorithm, """
                        id
                        1
                        null
                        """);
                assertOrder("SELECT id FROM lp_sort ORDER BY id DESC LIMIT 1,3", algorithm, """
                        id
                        2
                        1
                        """);
                bindVariableService.setLong(0, 2);
                assertOrder("SELECT id FROM lp_sort ORDER BY id DESC LIMIT $1", algorithm, """
                        id
                        3
                        2
                        """);
                assertOrder("SELECT id FROM lp_sort ORDER BY id DESC LIMIT 1,-1", "Limit", """
                        id
                        2
                        1
                        """);
            } finally {
                sqlExecutionContext.setParallelTopKEnabled(wasParallelTopK);
            }
        });
    }

    private void assertOrder(String sql, String algorithm, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            final TextPlanSink plan = new TextPlanSink();
            plan.of(factory, sqlExecutionContext);
            TestUtils.assertContains(plan.getSink(), algorithm);
            assertRows(factory, expected);
        }
    }

    private void assertRows(RecordCursorFactory factory, String expected) throws Exception {
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }
}
