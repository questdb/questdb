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

package io.questdb.test.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.orderby.EncodedSortLimitedLightRecordCursorFactory;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class OrderByLimitTest extends AbstractCairoTest {
    // The second branch comes first, so input order differs from id order and ties show it.
    private static final String UNION = "(SELECT * FROM t WHERE id > 5 UNION ALL SELECT * FROM t WHERE id <= 5)";

    @Test
    public void testAsOfJoinWithNestedTopK() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE 'trades' (" +
                            "  ts TIMESTAMP," +
                            "  price DOUBLE," +
                            "  size INT" +
                            ") TIMESTAMP(ts) PARTITION BY day;"
            );
            execute(
                    "CREATE TABLE 'order_book' (" +
                            "  ts TIMESTAMP," +
                            "  bid_price DOUBLE," +
                            "  bid_size INT," +
                            "  task_price DOUBLE," +
                            "  task_size INT" +
                            ") TIMESTAMP(ts) PARTITION BY day;"
            );

            execute("INSERT INTO 'trades' VALUES ('2020-01-01T01:01', 42, 42);");
            execute("INSERT INTO 'trades' VALUES ('2020-01-01T01:02', 42, 42);");
            execute("INSERT INTO 'order_book' VALUES ('2020-01-01T01:00', 1, 1, 2, 2);");
            execute("INSERT INTO 'order_book' VALUES ('2020-01-01T01:01', 3, 3, 4, 4);");

            assertQuery("SELECT * " +
                    "FROM (" +
                    "  SELECT *" +
                    "  FROM (" +
                    "    SELECT *" +
                    "    FROM trades" +
                    "    WHERE price = '42.0'" +
                    "    ORDER BY price, size, ts" +
                    "    LIMIT 1" +
                    "  )" +
                    "  ORDER BY ts" +
                    ")" +
                    "ASOF JOIN order_book")
                    .timestamp("ts")
                    .noRandomAccess()
                    .expectSize()
                    .returns("""
                            ts\tprice\tsize\tts1\tbid_price\tbid_size\ttask_price\ttask_size
                            2020-01-01T01:01:00.000000Z\t42.0\t42\t2020-01-01T01:01:00.000000Z\t3.0\t3\t4.0\t4
                            """);
        });
    }

    @Test
    public void testCopyingTopKBindVariableLimits() throws Exception {
        assertMemoryLeak(() -> {
            createSmallTable();
            bindVariableService.setLong(0, 3L);
            try (RecordCursorFactory factory = select("SELECT id FROM " + UNION + " ORDER BY id LIMIT $1")) {
                TestUtils.assertFactoryInTree(
                        factory,
                        EncodedSortLimitedLightRecordCursorFactory.class,
                        "the query is no longer routed to the copying top-K"
                );
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n1\n2\n3\n", cursor, factory.getMetadata());
                }
                // The cached factory re-reads the limit on every execution, including a sign flip.
                bindVariableService.setLong(0, -3L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n8\n9\n10\n", cursor, factory.getMetadata());
                }
            }

            bindVariableService.clear();
            bindVariableService.setLong(0, 2L);
            bindVariableService.setLong(1, 5L);
            try (RecordCursorFactory factory = select("SELECT id FROM " + UNION + " ORDER BY id DESC LIMIT $1, $2")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n8\n7\n6\n", cursor, factory.getMetadata());
                }
                bindVariableService.setLong(0, -5L);
                bindVariableService.setLong(1, -2L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n5\n4\n3\n", cursor, factory.getMetadata());
                }
                // lo >= 0 with hi < 0 keeps every row and skips from both ends.
                bindVariableService.setLong(0, 2L);
                bindVariableService.setLong(1, -3L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n8\n7\n6\n5\n4\n", cursor, factory.getMetadata());
                }
            }
        });
    }

    @Test
    public void testCopyingTopKCompaction() throws Exception {
        // Every row of an ascending input enters a descending top-K, so the buffer and the
        // row copies compact repeatedly; the var-size columns travel through each compaction.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE big AS (
                                SELECT
                                    x id,
                                    (x % 2)::INT k,
                                    ('s' || (100_000 + x))::STRING s,
                                    ('v' || (100_000 + x))::VARCHAR v,
                                    ARRAY[x::DOUBLE, -x::DOUBLE] arr,
                                    rnd_bin(1, 8, 0) bin,
                                    ('y' || (x % 3))::SYMBOL sym,
                                    timestamp_sequence(0, 1_000) ts
                                FROM long_sequence(10_000)
                            ) TIMESTAMP(ts)"""
            );
            final String union = "(SELECT * FROM big WHERE id <= 5_000 UNION ALL SELECT * FROM big WHERE id > 5_000)";
            assertMatchesReference(
                    "SELECT * FROM " + union + " ORDER BY id DESC LIMIT 3",
                    "SELECT * FROM big WHERE id > 9_997 ORDER BY id DESC"
            );
            assertMatchesReference(
                    "SELECT * FROM " + union + " ORDER BY id LIMIT -3",
                    "SELECT * FROM big WHERE id > 9_997"
            );
            assertMatchesReference(
                    "SELECT * FROM " + union + " ORDER BY s DESC LIMIT 2, 5",
                    "SELECT * FROM big WHERE id BETWEEN 9_996 AND 9_998 ORDER BY id DESC"
            );
            assertMatchesReference(
                    "SELECT * FROM " + union + " ORDER BY v DESC LIMIT -5, -2",
                    "SELECT * FROM big WHERE id BETWEEN 3 AND 5 ORDER BY id DESC"
            );
            assertMatchesReference(
                    "SELECT * FROM " + union + " ORDER BY sym, id DESC LIMIT 3",
                    "SELECT * FROM big WHERE id IN (9_999, 9_996, 9_993) ORDER BY id DESC"
            );
            assertMatchesReference(
                    "SELECT * FROM " + union + " ORDER BY id DESC LIMIT 3_000",
                    "SELECT * FROM big WHERE id > 7_000 ORDER BY id DESC"
            );
            // Ties with the boundary row keep the input order across compactions.
            assertQuery("SELECT id, k FROM " + union + " ORDER BY k DESC LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\tk\n1\t1\n3\t1\n5\t1\n");
            assertQuery("SELECT id, k FROM " + union + " ORDER BY k LIMIT -3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\tk\n9995\t1\n9997\t1\n9999\t1\n");
        });
    }

    @Test
    public void testCopyingTopKEmptyInputAndLargeLimit() throws Exception {
        assertMemoryLeak(() -> {
            createSmallTable();
            assertQuery("SELECT id, k FROM (SELECT * FROM t WHERE id < 0 UNION ALL SELECT * FROM t WHERE id < 0) ORDER BY k LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\tk\n");
            assertQuery("SELECT id FROM " + UNION + " ORDER BY id DESC LIMIT 20")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n");
            assertQuery("SELECT id FROM " + UNION + " ORDER BY id DESC LIMIT -20")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\n10\n9\n8\n7\n6\n5\n4\n3\n2\n1\n");
        });
    }

    @Test
    public void testCopyingTopKLimitShapes() throws Exception {
        assertMemoryLeak(() -> {
            createSmallTable();
            assertLimit("3", "id\n1\n2\n3\n");
            assertLimit("-3", "id\n8\n9\n10\n");
            assertLimit("2, 5", "id\n3\n4\n5\n");
            assertLimit("5, 2", "id\n3\n4\n5\n");
            assertLimit("-5, -2", "id\n6\n7\n8\n");
            assertLimit("3, 3", "id\n");
            assertLimit("0", "id\n");
            // A negative lo with a positive hi used to fail here; the top-K selects what a random-access base does.
            assertLimit("-3, 2", printReference("SELECT id FROM t ORDER BY id LIMIT -3, 2"));
            // lo >= 0 with hi < 0 keeps the full sort and a separate limit.
            assertQuery("SELECT id FROM " + UNION + " ORDER BY id LIMIT 2, -3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining("Encode sort lo:")
                    .returns("id\n3\n4\n5\n6\n7\n");
        });
    }

    @Test
    public void testCopyingTopKNullsAndVarSizeKeys() throws Exception {
        assertMemoryLeak(() -> {
            createSmallTable();
            assertQuery("SELECT id, d FROM " + UNION + " ORDER BY d LIMIT 4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\td
                            10\t0.0
                            5\t0.0
                            6\t1.0
                            1\t1.0
                            """);
            assertQuery("SELECT id, d FROM " + UNION + " ORDER BY d DESC LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\td
                            8\tnull
                            4\tnull
                            9\t4.0
                            """);
            assertQuery("SELECT id, s, v FROM " + UNION + " ORDER BY s DESC, v LIMIT 5")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ts\tv
                            3\ts3\tv0
                            7\ts3\tv1
                            6\ts2\t
                            2\ts2\tv2
                            9\ts1\tv0
                            """);
            assertQuery("SELECT id, s, v FROM " + UNION + " ORDER BY v, s LIMIT -3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\ts\tv
                            5\t\tv2
                            8\ts0\tv2
                            2\ts2\tv2
                            """);
        });
    }

    @Test
    public void testCopyingTopKOverStreamSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE ss AS (
                                SELECT (x % 7)::INT v, (x * x * 10_000)::TIMESTAMP ts
                                FROM long_sequence(300)
                            ) TIMESTAMP(ts) PARTITION BY HOUR"""
            );
            execute("CREATE TABLE agg AS (SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 1s)");
            assertQuery("SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 1s ORDER BY c DESC, ts LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Encode sort", "lo: 3", "Stream Group By")
                    .returns(printReference("SELECT * FROM agg ORDER BY c DESC, ts LIMIT 3"));
            assertQuery("SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 1s ORDER BY s, ts DESC LIMIT 2, 6")
                    .noLeakCheck()
                    .expectSize()
                    .returns(printReference("SELECT * FROM agg ORDER BY s, ts DESC LIMIT 2, 6"));
            assertQuery("SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 1s ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .noRandomAccess()
                    .withPlanContaining("Frame backward scan")
                    .withPlanNotContaining("sort")
                    .timestampDesc("ts")
                    .returns(printReference("SELECT * FROM agg ORDER BY ts DESC LIMIT 3"));
            // An outer negative limit rewinds the top-K, which replays its copies instead of
            // re-running the stream, so the plan needs no materialization.
            assertQuery("SELECT * FROM (SELECT ts, count() c FROM ss SAMPLE BY 1s ORDER BY c DESC, ts LIMIT 5) LIMIT -2")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanNotContaining("Materialize")
                    .returns(printReference("SELECT * FROM (SELECT ts, c FROM agg ORDER BY c DESC, ts LIMIT 5) LIMIT -2"));
        });
    }

    @Test
    public void testCopyingTopKPlanAndTies() throws Exception {
        assertMemoryLeak(() -> {
            createSmallTable();
            assertQuery("SELECT id, k FROM " + UNION + " ORDER BY k DESC, id LIMIT 4")
                    .noLeakCheck()
                    .expectSize()
                    .withPlan("""
                            Encode sort lo: 4
                              keys: [k desc, id]
                                Union All
                                    Async JIT Filter workers: 1
                                      filter: 5<id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                                    Async JIT Filter workers: 1
                                      filter: 5>=id
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: t
                            """)
                    .returns("""
                            id\tk
                            2\t2
                            5\t2
                            8\t2
                            1\t1
                            """);
            // Ties keep the input order, as the full sort does: ids 6..10 come before 1..5.
            assertQuery("SELECT id, k FROM " + UNION + " ORDER BY k LIMIT 5")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tk
                            6\t0
                            9\t0
                            3\t0
                            7\t1
                            10\t1
                            """);
            assertQuery("SELECT id, k FROM " + UNION + " ORDER BY k DESC LIMIT 4")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tk
                            8\t2
                            2\t2
                            5\t2
                            7\t1
                            """);
            assertQuery("SELECT id, k FROM " + UNION + " ORDER BY k LIMIT -2")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tk
                            2\t2
                            5\t2
                            """);
            assertQuery("SELECT id, sym FROM " + UNION + " ORDER BY sym DESC, id DESC LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            id\tsym
                            9\ty1
                            7\ty1
                            5\ty1
                            """);
        });
    }

    @Test
    public void testCopyingTopKWithinSortKeyBudget() throws Exception {
        // A full sort of the input needs more entries than the budget holds, while
        // the top-K keeps at most its compaction trigger of entries.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES, 131_072);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE big AS (SELECT x id FROM long_sequence(10_000))");
            final String union = "(SELECT * FROM big WHERE id <= 5_000 UNION ALL SELECT * FROM big WHERE id > 5_000)";
            assertQuery("SELECT id FROM " + union + " ORDER BY id DESC LIMIT 3")
                    .noLeakCheck()
                    .expectSize()
                    .returns("id\n10000\n9999\n9998\n");
            assertExceptionNoLeakCheck("SELECT id FROM " + union + " ORDER BY id DESC", -1, "memory exceeded in EncodedSort");
        });
    }

    @Test
    public void testNegativeLimitDescOrderBy() throws Exception {
        assertQuery("select price, ts " +
                "from x " +
                "where price > 0 AND ts >= '2024-01-01' AND ts <= '2024-12-31' " +
                "order by ts desc limit -1")
                .ddl("create table x as " +
                        "(" +
                        "select" +
                        " rnd_double() price," +
                        " timestamp_sequence('2024', 24L*60*60*1000*1000) ts" +
                        " from" +
                        " long_sequence(2)" +
                        ") timestamp(ts) partition by day")
                .timestampDesc("ts")
                .expectSize()
                .returns("""
                        price\tts
                        0.6607777894187332\t2024-01-01T00:00:00.000000Z
                        """);
    }

    private static void createSmallTable() throws SqlException {
        execute(
                """
                        CREATE TABLE t AS (
                            SELECT
                                x id,
                                (x % 3)::INT k,
                                CASE WHEN x % 4 = 0 THEN NULL ELSE (x % 5)::DOUBLE END d,
                                CASE WHEN x % 5 = 0 THEN NULL ELSE ('s' || (x % 4))::STRING END s,
                                CASE WHEN x % 6 = 0 THEN NULL ELSE ('v' || (x % 3))::VARCHAR END v,
                                ('y' || (x % 2))::SYMBOL sym,
                                timestamp_sequence(0, 1_000_000) ts
                            FROM long_sequence(10)
                        ) TIMESTAMP(ts)"""
        );
    }

    private static String printReference(String query) throws SqlException {
        final StringSink sink = new StringSink();
        printSql(query, sink);
        return sink.toString();
    }

    private void assertLimit(String limit, String expected) throws Exception {
        assertQuery("SELECT id FROM " + UNION + " ORDER BY id LIMIT " + limit)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("Encode sort", "lo:")
                .returns(expected);
    }

    private void assertMatchesReference(String query, String referenceQuery) throws Exception {
        assertQuery(query)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("Encode sort", "lo:", "Union All")
                .returns(printReference(referenceQuery));
    }
}
