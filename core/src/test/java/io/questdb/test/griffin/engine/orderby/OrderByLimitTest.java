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
    private static final String GROUPS = "(SELECT a.ts, a.id, a.s, a.v, b.ts bts FROM g a ASOF JOIN g b)";
    private static final String GROUPS_REFERENCE = "(SELECT ts, id, s, v, ts bts FROM g)";
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
    public void testCopyingTopKAgainstScanKeepsNewestGroups() throws Exception {
        assertMemoryLeak(() -> {
            createGroupTable();
            for (String limit : new String[]{"6", "3_000", "2, 7", "9_223_372_036_854_775_807"}) {
                assertQuery("SELECT * FROM " + GROUPS + " ORDER BY ts DESC LIMIT " + limit)
                        .noLeakCheck()
                        .timestampDesc("ts")
                        .expectSize()
                        .withPlanContaining("Encode sort lo:", "partiallySorted: true", "AsOf Join")
                        .returns(printReference("SELECT * FROM " + GROUPS_REFERENCE + " ORDER BY ts DESC, id LIMIT " + limit));
            }
            assertQuery("SELECT * FROM " + GROUPS + " ORDER BY ts DESC, s DESC LIMIT 5")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns(printReference("SELECT * FROM " + GROUPS_REFERENCE + " ORDER BY ts DESC, s DESC LIMIT 5"));
            bindVariableService.setStr(0, "-1h");
            assertQuery("SELECT * FROM generate_series('2024-01-02', '2024-01-01', $1) ORDER BY 1 DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("generate_series")
                    .expectSize()
                    .withPlanContaining("Encode sort lo: 3")
                    .withPlanNotContaining("partiallySorted")
                    .returns("""
                            generate_series
                            2024-01-02T00:00:00.000000Z
                            2024-01-01T23:00:00.000000Z
                            2024-01-01T22:00:00.000000Z
                            """);
            bindVariableService.clear();
            assertQuery("SELECT * FROM " + GROUPS + " ORDER BY ts DESC LIMIT 0")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("ts\tid\ts\tv\tbts\n");

            bindVariableService.setLong(0, 3L);
            try (RecordCursorFactory factory = select("SELECT id FROM " + GROUPS + " ORDER BY ts DESC LIMIT $1")) {
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n19997\n19998\n19999\n", cursor, factory.getMetadata());
                }
                bindVariableService.setLong(0, -3L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n2\n3\n4\n", cursor, factory.getMetadata());
                }
                bindVariableService.setLong(0, 5L);
                try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                    assertCursorTwoPass("id\n19997\n19998\n19999\n20000\n19993\n", cursor, factory.getMetadata());
                }
            }

            execute("CREATE TABLE same AS (SELECT x id, 0::TIMESTAMP ts FROM long_sequence(10_000)) TIMESTAMP(ts)");
            assertQuery("SELECT a.ts, a.id, b.ts bts FROM same a ASOF JOIN same b ORDER BY a.ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .withPlanContaining("partiallySorted: true")
                    .returns("""
                            ts\tid\tbts
                            1970-01-01T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testCopyingTopKAlongTimestampWithinSortValueBudget() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 4_096);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 262_144);
        assertMemoryLeak(() -> {
            createGroupTable();
            final String join = "(SELECT a.ts, a.id, b.ts bts FROM g a ASOF JOIN g b)";
            assertQuery("SELECT * FROM " + join + " ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            ts\tid\tbts
                            1970-01-01T00:00:04.999000Z\t19997\t1970-01-01T00:00:04.999000Z
                            1970-01-01T00:00:04.999000Z\t19998\t1970-01-01T00:00:04.999000Z
                            1970-01-01T00:00:04.999000Z\t19999\t1970-01-01T00:00:04.999000Z
                            """);
            assertExceptionNoLeakCheck("SELECT * FROM " + join + " ORDER BY ts DESC", -1, "cairo.sql.sort.value.max.bytes");

            execute("CREATE TABLE same AS (SELECT x id, 0::TIMESTAMP ts FROM long_sequence(20_000)) TIMESTAMP(ts)");
            final String same = "(SELECT a.ts, a.id, b.ts bts FROM same a ASOF JOIN same b)";
            assertQuery("SELECT * FROM " + same + " ORDER BY ts DESC LIMIT 3")
                    .noLeakCheck()
                    .timestampDesc("ts")
                    .expectSize()
                    .returns("""
                            ts\tid\tbts
                            1970-01-01T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery("SELECT * FROM " + same + " ORDER BY ts, id DESC LIMIT 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tid\tbts
                            1970-01-01T00:00:00.000000Z\t20000\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t19999\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t19998\t1970-01-01T00:00:00.000000Z
                            """);
            assertExceptionNoLeakCheck("SELECT * FROM " + same + " ORDER BY ts DESC", -1, "cairo.sql.sort.value.max.bytes");
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
    public void testCopyingTopKOverSerialSampleBy() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE ss AS (
                                SELECT (x % 7)::INT v, ('k' || (x % 3))::SYMBOL k, (x * 10_000)::TIMESTAMP ts
                                FROM long_sequence(200_000)
                            ) TIMESTAMP(ts) PARTITION BY HOUR"""
            );
            execute("CREATE TABLE agg AS (SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 100T)");
            execute("CREATE TABLE kagg AS (SELECT ts, k, count() c, sum(v) s FROM ss SAMPLE BY 100T)");
            sqlExecutionContext.setParallelGroupByEnabled(false);
            assertQuery("SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 100T ORDER BY ts DESC LIMIT 5")
                    .noLeakCheck()
                    .expectSize()
                    .timestampDesc("ts")
                    .withPlanContaining("Encode sort lo: 5 partiallySorted: true", "Stream Group By")
                    .withPlanNotContaining("Limit")
                    .returns(printReference("SELECT * FROM agg ORDER BY ts DESC LIMIT 5"));
            assertQuery("SELECT ts, k, count() c, sum(v) s FROM ss SAMPLE BY 100T ORDER BY ts DESC, k LIMIT 7")
                    .noLeakCheck()
                    .expectSize()
                    .timestampDesc("ts")
                    .returns(printReference("SELECT * FROM kagg ORDER BY ts DESC, k LIMIT 7"));
            assertQuery("SELECT ts, k, count() c, sum(v) s FROM ss SAMPLE BY 100T ORDER BY ts, k DESC LIMIT 7")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("partiallySorted: true", "Stream Group By")
                    .returns(printReference("SELECT * FROM kagg ORDER BY ts, k DESC LIMIT 7"));

            sqlExecutionContext.setParallelGroupByEnabled(true);
            assertQuery("SELECT * FROM (SELECT ts, count() c, sum(v) s FROM ss SAMPLE BY 100T ORDER BY ts DESC LIMIT 10_000) ORDER BY ts, c LIMIT 4")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanContaining("partiallySorted: true", "Frame backward scan")
                    .returns(printReference("SELECT * FROM (SELECT * FROM agg ORDER BY ts DESC LIMIT 10_000) ORDER BY ts, c LIMIT 4"));
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
    public void testCopyingTopKIgnoresLightValueBudget() throws Exception {
        // The light value budget holds 4_096 entries, fewer than the limit keeps.
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES, 32_768);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE big AS (SELECT x id, x::VARCHAR v FROM long_sequence(10_000))");
            final String union = "(SELECT * FROM big WHERE id <= 5_000 UNION ALL SELECT * FROM big WHERE id > 5_000)";
            assertMatchesReference(
                    "SELECT id FROM " + union + " ORDER BY id LIMIT 6_000",
                    "SELECT id FROM big WHERE id <= 6_000"
            );
            assertMatchesReference(
                    "SELECT id FROM " + union + " ORDER BY id DESC LIMIT 4_095",
                    "SELECT id FROM big WHERE id > 5_905 ORDER BY id DESC"
            );
            for (String query : new String[]{
                    "SELECT id FROM big ORDER BY id DESC",
                    "SELECT id FROM big WHERE id > 0 ORDER BY id DESC",
                    "SELECT v FROM big WHERE id > 0 ORDER BY v DESC"
            }) {
                assertExceptionNoLeakCheck(
                        query,
                        -1,
                        "memory exceeded in EncodedSort (raise cairo.sql.sort.key.max.bytes or cairo.sql.sort.light.value.max.bytes)"
                );
            }
        });
    }

    @Test
    public void testCopyingTopKStopsEarlyAlongScan() throws Exception {
        assertMemoryLeak(() -> {
            createGroupTable();
            for (String orderBy : new String[]{" ORDER BY ts, id DESC LIMIT 6", " ORDER BY ts, s LIMIT 2, 9", " ORDER BY ts, id LIMIT 30_000"}) {
                assertQuery("SELECT * FROM " + GROUPS + orderBy)
                        .noLeakCheck()
                        .timestamp("ts")
                        .expectSize()
                        .withPlanContaining("partiallySorted: true", "AsOf Join")
                        .returns(printReference("SELECT * FROM " + GROUPS_REFERENCE + orderBy));
            }
            execute("CREATE TABLE same AS (SELECT x id, 0::TIMESTAMP ts FROM long_sequence(10_000)) TIMESTAMP(ts)");
            assertQuery("SELECT a.ts, a.id, b.ts bts FROM same a ASOF JOIN same b ORDER BY a.ts, a.id DESC LIMIT 3")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tid\tbts
                            1970-01-01T00:00:00.000000Z\t10000\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t9999\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t9998\t1970-01-01T00:00:00.000000Z
                            """);
            assertQuery("SELECT a.ts, a.id, b.ts bts FROM same a ASOF JOIN same b ORDER BY a.ts, a.id DESC LIMIT 3_000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns(printReference("SELECT ts, id, ts bts FROM same ORDER BY ts, id DESC LIMIT 3_000"));
        });
    }

    @Test
    public void testCopyingTopKStopsEarlyWithinSortValueBudget() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_PAGE_SIZE, 1_024);
        node1.setProperty(PropertyKey.CAIRO_SQL_SORT_VALUE_MAX_BYTES, 1_024);
        assertMemoryLeak(() -> {
            createGroupTable();
            final String join = "(SELECT a.ts, a.id, b.ts bts FROM g a ASOF JOIN g b)";
            assertQuery("SELECT * FROM " + join + " ORDER BY ts, id DESC LIMIT 6")
                    .noLeakCheck()
                    .timestamp("ts")
                    .expectSize()
                    .returns("""
                            ts\tid\tbts
                            1970-01-01T00:00:00.000000Z\t4\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t3\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t1\t1970-01-01T00:00:00.000000Z
                            1970-01-01T00:00:00.001000Z\t8\t1970-01-01T00:00:00.001000Z
                            1970-01-01T00:00:00.001000Z\t7\t1970-01-01T00:00:00.001000Z
                            """);
            assertExceptionNoLeakCheck("SELECT * FROM " + join + " ORDER BY id DESC, ts LIMIT 6", -1, "cairo.sql.sort.value.max.bytes");
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
            assertExceptionNoLeakCheck(
                    "SELECT id FROM " + union + " ORDER BY id DESC LIMIT 9_000",
                    -1,
                    "limit of 131072 memory exceeded in EncodedSort (raise cairo.sql.sort.key.max.bytes)"
            );
            bindVariableService.setLong(0, 2L);
            bindVariableService.setLong(1, -3L);
            assertExceptionNoLeakCheck(
                    "SELECT id FROM (SELECT * FROM big UNION ALL SELECT * FROM big) ORDER BY id LIMIT $1, $2",
                    -1,
                    "limit of 131072 memory exceeded in EncodedSort (raise cairo.sql.sort.key.max.bytes)"
            );
        });
    }

    @Test
    public void testInputOfSortedRuns() throws Exception {
        // Each table holds the ids 0..8_999 as three interleaved runs, ascending or descending, so the top-K
        // buffer and the full sort receive several sorted runs to merge.
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE asc_runs AS (
                        SELECT ((x - 1) % 3_000) * 3 + (x - 1) / 3_000 id, ('v' || (100_000 + ((x - 1) % 3_000) * 3 + (x - 1) / 3_000))::VARCHAR v
                        FROM long_sequence(9_000)
                    )""");
            execute("""
                    CREATE TABLE desc_runs AS (
                        SELECT (2_999 - (x - 1) % 3_000) * 3 + (x - 1) / 3_000 id, ('v' || (100_000 + (2_999 - (x - 1) % 3_000) * 3 + (x - 1) / 3_000))::VARCHAR v
                        FROM long_sequence(9_000)
                    )""");
            final String first = printReference("SELECT x - 1 id, ('v' || (99_999 + x))::VARCHAR v FROM long_sequence(1_000)");
            final String last = printReference("SELECT 7_999 + x id, ('v' || (107_999 + x))::VARCHAR v FROM long_sequence(1_000)");
            final String top = printReference("SELECT 9_000 - x id, ('v' || (109_000 - x))::VARCHAR v FROM long_sequence(1_000)");
            final String all = printReference("SELECT x - 1 id, ('v' || (99_999 + x))::VARCHAR v FROM long_sequence(9_000)");
            for (String table : new String[]{"asc_runs", "desc_runs"}) {
                for (String input : new String[]{table, "(SELECT * FROM " + table + " WHERE id >= 0 UNION ALL SELECT * FROM " + table + " WHERE id < 0)"}) {
                    final String select = "SELECT id, v FROM " + input;
                    assertTopK(select + " ORDER BY id LIMIT 1_000", first);
                    assertTopK(select + " ORDER BY v LIMIT 1_000", first);
                    assertTopK(select + " ORDER BY id LIMIT -1_000", last);
                    assertTopK(select + " ORDER BY id DESC LIMIT 1_000", top);
                    assertTopK(select + " ORDER BY v DESC LIMIT 1_000", top);
                    // A full sort lends the native sort no scratch, so it sorts the runs again as one array.
                    for (String key : new String[]{"id", "v"}) {
                        assertQuery(select + " ORDER BY " + key)
                                .noLeakCheck()
                                .expectSize()
                                .sizeMayVary()
                                .withPlanContaining("Encode sort")
                                .returns(all);
                    }
                }
            }
            // The first run lies wholly above the second, so the merge rotates the runs instead of interleaving them.
            execute("""
                    CREATE TABLE disjoint_runs AS (
                        SELECT CASE WHEN x <= 1_000 THEN x + 1_999 ELSE x - 1_001 END id,
                               ('v' || (100_000 + CASE WHEN x <= 1_000 THEN x + 1_999 ELSE x - 1_001 END))::VARCHAR v
                        FROM long_sequence(3_000)
                    )""");
            final String disjoint = printReference("SELECT x - 1 id, ('v' || (99_999 + x))::VARCHAR v FROM long_sequence(3_000)");
            for (String input : new String[]{"disjoint_runs", "(SELECT * FROM disjoint_runs WHERE id >= 0 UNION ALL SELECT * FROM disjoint_runs WHERE id < 0)"}) {
                assertTopK("SELECT id, v FROM " + input + " ORDER BY id LIMIT 5_000", disjoint);
                assertTopK("SELECT id, v FROM " + input + " ORDER BY v LIMIT 5_000", disjoint);
            }
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

    private static void createGroupTable() throws SqlException {
        execute(
                """
                        CREATE TABLE g AS (
                            SELECT x id, ('s' || x)::STRING s, ('v' || x)::VARCHAR v, ((x - 1) / 4 * 1_000)::TIMESTAMP ts
                            FROM long_sequence(20_000)
                        ) TIMESTAMP(ts)"""
        );
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

    private void assertTopK(String query, String expected) throws Exception {
        assertQuery(query)
                .noLeakCheck()
                .expectSize()
                .withPlanContaining("lo:")
                .returns(expected);
    }
}
