/*******************************************************************************
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

import io.questdb.cairo.SqlJitMode;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * Test order by asc and desc with filter(s)/where clause
 */
public class OrderByWithFilterTest extends AbstractCairoTest {

    static final int ORDER_ASC = 0;
    static final int ORDER_DESC = 1;

    @Test
    public void testOrderByAscInOverClause() throws Exception {
        String expected = """
                ts\ttemp
                1970-05-23T02:00:00.000000Z\t0.004941225
                1971-02-21T16:00:00.000000Z\t0.30323267
                """;
        String direction = "asc";

        assertOrderByInOverClause(expected, direction);
    }

    @Test
    public void testOrderByAscWithByteFilter() throws Exception {
        testOrderByWithFilter("byte", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithCharFilter() throws Exception {
        testOrderByWithFilter("char", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithDoubleFilter() throws Exception {
        testOrderByWithFilter("double", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithFloatFilter() throws Exception {
        testOrderByWithFilter("float", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithIntFilter() throws Exception {
        testOrderByWithFilter("int", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithLongFilter() throws Exception {
        testOrderByWithFilter("long", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithShortFilter() throws Exception {
        testOrderByWithFilter("short", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithStringFilter() throws Exception {
        testOrderByWithFilter("string", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithSymbolFilter() throws Exception {
        testOrderByWithFilter("symbol", ORDER_ASC);
    }

    @Test
    public void testOrderByAscWithTimestampFilter() throws Exception {
        testOrderByWithFilter("timestamp", ORDER_ASC);
    }

    @Test
    public void testOrderByDescInOverClause() throws Exception {
        String expected = """
                ts\ttemp
                1970-04-23T22:00:00.000000Z\t99.97797
                1971-02-02T02:00:00.000000Z\t98.336945
                """;
        String direction = "desc";

        assertOrderByInOverClause(expected, direction);
    }

    @Test // triggers DeferredSingleSymbolFilterPageFrameRecordCursorFactory
    public void testOrderByDescSelectByIndexedSymbolColumn() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 10, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, case when x <= 5 then 'ABC' when x <= 7 then 'DEF' else 'GHI' end," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery(
                """
                        l\ts\tts
                        7\tDEF\t2022-01-09T22:40:00.000000Z
                        6\tDEF\t2022-01-08T18:53:20.000000Z
                        """,
                "select l, s, ts from trips where s = 'DEF' order by ts desc",
                null,
                "ts###DESC",
                true,
                false
        );
    }

    @Test
    public void testOrderByDescWithByteFilter() throws Exception {
        testOrderByWithFilter("byte", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithCharFilter() throws Exception {
        testOrderByWithFilter("char", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithDoubleFilter() throws Exception {
        testOrderByWithFilter("double", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithFilterOnExcludedValuesRecordCursorFactory() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );
        assertQuery("""
                        l\ts\tts
                        9\tA0\t2022-01-12T06:13:20.000000Z
                        8\tA2\t2022-01-11T02:26:40.000000Z
                        6\tA0\t2022-01-08T18:53:20.000000Z
                        5\tA2\t2022-01-07T15:06:40.000000Z
                        3\tA0\t2022-01-05T07:33:20.000000Z
                        2\tA2\t2022-01-04T03:46:40.000000Z
                        """,
                "select l, s, ts from trips where s != 'A1' and test_match() order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByDescWithFilterOnSubQueryRecordCursorFactory() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 10, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, case when x<=3 then 'ABC' when x>6 and x <= 9 then 'DEF' else 'GHI' end," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery("""
                        l\ts\tts
                        9\tDEF\t2022-01-12T06:13:20.000000Z
                        8\tDEF\t2022-01-11T02:26:40.000000Z
                        7\tDEF\t2022-01-09T22:40:00.000000Z
                        3\tABC\t2022-01-05T07:33:20.000000Z
                        2\tABC\t2022-01-04T03:46:40.000000Z
                        1\tABC\t2022-01-03T00:00:00.000000Z
                        """,
                "select l, s, ts from trips where s in (select 'DEF' union all select 'ABC' ) and length(s) = 3 order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByDescWithFilterOnSubQueryRecordCursorFactoryVarchar() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 10, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, case when x<=3 then 'ABC' when x>6 and x <= 9 then 'DEF' else 'GHI' end," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery("""
                        l\ts\tts
                        9\tDEF\t2022-01-12T06:13:20.000000Z
                        8\tDEF\t2022-01-11T02:26:40.000000Z
                        7\tDEF\t2022-01-09T22:40:00.000000Z
                        3\tABC\t2022-01-05T07:33:20.000000Z
                        2\tABC\t2022-01-04T03:46:40.000000Z
                        1\tABC\t2022-01-03T00:00:00.000000Z
                        """,
                "select l, s, ts from trips where s in (select 'DEF'::varchar union all select 'ABC'::varchar ) and length(s) = 3 order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByDescWithFilterOnValuesRecordCursorFactory() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );
        //A0, A1, A2, A0, A1, A2, A0, A1, A2, A0
        assertQuery("""
                        l\ts\tts
                        9\tA0\t2022-01-12T06:13:20.000000Z
                        8\tA2\t2022-01-11T02:26:40.000000Z
                        6\tA0\t2022-01-08T18:53:20.000000Z
                        5\tA2\t2022-01-07T15:06:40.000000Z
                        3\tA0\t2022-01-05T07:33:20.000000Z
                        2\tA2\t2022-01-04T03:46:40.000000Z
                        """,
                "select l, s, ts from trips where s in ('A2', 'A0') and length(s) = 2 order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByDescWithFloatFilter() throws Exception {
        testOrderByWithFilter("float", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithIntFilter() throws Exception {
        testOrderByWithFilter("int", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithLongFilter() throws Exception {
        testOrderByWithFilter("long", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithMultipleNotEqualsSymbolConditions() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );
        assertQuery("""
                        l\ts\tts
                        8\tA2\t2022-01-11T02:26:40.000000Z
                        5\tA2\t2022-01-07T15:06:40.000000Z
                        2\tA2\t2022-01-04T03:46:40.000000Z
                        """,
                "select l, s, ts from trips where s != 'A1' and s != 'A0' and test_match() order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByDescWithPageFrameRecordCursorFactory() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long,s symbol index capacity 5, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x, 'A' || ( x%3 )," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );
        //A0, A1, A2, A0, A1, A2, A0, A1, A2, A0
        assertQuery("""
                        l\ts\tts
                        8\tA2\t2022-01-11T02:26:40.000000Z
                        5\tA2\t2022-01-07T15:06:40.000000Z
                        2\tA2\t2022-01-04T03:46:40.000000Z
                        """,
                "select l, s, ts from trips where s = 'A2' and test_match() order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByDescWithShortFilter() throws Exception {
        testOrderByWithFilter("short", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithStringFilter() throws Exception {
        testOrderByWithFilter("string", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithSymbolFilter() throws Exception {
        testOrderByWithFilter("symbol", ORDER_DESC);
    }

    @Test
    public void testOrderByDescWithTimestampFilter() throws Exception {
        testOrderByWithFilter("timestamp", ORDER_DESC);
    }

    @Test
    public void testOrderByNonPrefixedColumnNotOnSelectList1() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (\s
                                ts TIMESTAMP,
                                address SYMBOL,
                                workspace SYMBOL,
                                method_id SYMBOL
                        ) timestamp(ts)""");

            execute("insert into tab " +
                    "select dateadd('m', x::int, 0), " +
                    " 'A' || (10-x), " +
                    " case when x < 6 then 'a' else 'b' end, " +
                    " case when x < 3 then 'c' else 'd' end " +
                    "from long_sequence(10)");

            String query = """
                    select timestamp_floor('m', ts) as month, address || workspace as uid
                        from tab
                        where workspace = 'a' and method_id = 'd'
                        order by address""";

            assertPlanNoLeakCheck(query, """
                    SelectedRecord
                        Sort light
                          keys: [address]
                            VirtualRecord
                              functions: [timestamp_floor('minute',ts),concat([address,workspace]),address]
                                SelectedRecord
                                    Async JIT Filter workers: 1
                                      filter: (workspace='a' and method_id='d')
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                    """);

            assertQuery("""
                    month\tuid
                    1970-01-01T00:05:00.000000Z\tA5a
                    1970-01-01T00:04:00.000000Z\tA6a
                    1970-01-01T00:03:00.000000Z\tA7a
                    """, query, null, true, false);
        });
    }

    @Test
    public void testOrderByNonPrefixedColumnNotOnSelectList2() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (
                                ts TIMESTAMP,
                                address SYMBOL,
                                workspace SYMBOL,
                                method_id SYMBOL
                        ) timestamp(ts)""");

            execute("insert into tab " +
                    "select dateadd('m', x::int, 0), " +
                    " 'A' || x, " +
                    " case when x < 6 then 'a' else 'b' end, " +
                    " case when x < 3 then 'c' else 'd' end " +
                    "from long_sequence(10)");

            String query = """
                    select timestamp_floor('m', ts) as month, address || workspace as uid
                        from tab
                        where workspace = 'a' and method_id = 'd'
                        order by ts, month, method_id""";

            assertPlanNoLeakCheck(query, """
                    SelectedRecord
                        Sort light
                          keys: [ts, month, method_id]
                            VirtualRecord
                              functions: [timestamp_floor('minute',ts),concat([address,workspace]),ts,method_id]
                                Async JIT Filter workers: 1
                                  filter: (workspace='a' and method_id='d')
                                    PageFrame
                                        Row forward scan
                                        Frame forward scan on: tab
                    """);

            assertQuery("""
                    month\tuid
                    1970-01-01T00:03:00.000000Z\tA3a
                    1970-01-01T00:04:00.000000Z\tA4a
                    1970-01-01T00:05:00.000000Z\tA5a
                    """, query, null, true, false);
        });
    }

    @Test//test with join
    public void testOrderByNonPrefixedColumnNotOnSelectList4() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE tab (
                                ts TIMESTAMP,
                                address SYMBOL,
                                workspace SYMBOL,
                                method_id SYMBOL
                        ) timestamp(ts)""");

            execute("insert into tab " +
                    "select dateadd('m', x::int, 1), " +
                    " 'A' || x, " +
                    " case when x < 6 then 'a' else 'b' end, " +
                    " case when x < 3 then 'c' else 'd' end " +
                    "from long_sequence(10)");

            String query = """
                    select timestamp_floor('m', t2.ts) as month,t1.ts, t1.address || t2.workspace as uid
                        from tab t1 \
                        join tab t2 on t1.workspace = t2.workspace and t1.method_id = t2.method_id \
                        where t1.workspace = 'a' and t1.method_id = 'd'
                        order by t2.ts desc""";

            assertPlanNoLeakCheck(query, """
                    SelectedRecord
                        Sort
                          keys: [ts desc]
                            VirtualRecord
                              functions: [timestamp_floor('minute',ts),ts1,concat([address,workspace]),ts]
                                SelectedRecord
                                    Hash Join Light
                                      condition: t2.method_id=t1.method_id and t2.workspace=t1.workspace
                                        Async JIT Filter workers: 1
                                          filter: (workspace='a' and method_id='d')
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: tab
                                        Hash
                                            Async JIT Filter workers: 1
                                              filter: (method_id='d' and workspace='a')
                                                PageFrame
                                                    Row forward scan
                                                    Frame forward scan on: tab
                    """);

            assertQuery(
                    """
                            month\tts1\tuid
                            1970-01-01T00:05:00.000000Z\t1970-01-01T00:03:00.000001Z\tA3a
                            1970-01-01T00:05:00.000000Z\t1970-01-01T00:04:00.000001Z\tA4a
                            1970-01-01T00:05:00.000000Z\t1970-01-01T00:05:00.000001Z\tA5a
                            1970-01-01T00:04:00.000000Z\t1970-01-01T00:03:00.000001Z\tA3a
                            1970-01-01T00:04:00.000000Z\t1970-01-01T00:04:00.000001Z\tA4a
                            1970-01-01T00:04:00.000000Z\t1970-01-01T00:05:00.000001Z\tA5a
                            1970-01-01T00:03:00.000000Z\t1970-01-01T00:03:00.000001Z\tA3a
                            1970-01-01T00:03:00.000000Z\t1970-01-01T00:04:00.000001Z\tA4a
                            1970-01-01T00:03:00.000000Z\t1970-01-01T00:05:00.000001Z\tA5a
                            """,
                    query,
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testOrderByPrefixedColumnNotOnSelectList1() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE trips (
                      vendor_id SYMBOL,
                      pickup_datetime TIMESTAMP,
                      tax DOUBLE,
                      mta_tax DOUBLE
                    ) timestamp (pickup_datetime) PARTITION BY MONTH;""");

            execute("insert into trips " +
                    "select 'A' || x, dateadd('s', x::int, '2019-06-30T00:00:00.000000Z'), x::timestamp, x, x%2 from long_sequence(10)");

            String query = "select a.vendor_id from " +
                    "trips a " +
                    "where pickup_datetime >= '2019-06-30T00:00:00.000000Z' " +
                    "and vendor_id in ('A1', 'A2') " +
                    "order by a.mta_tax;";

            assertPlanNoLeakCheck(query, """
                    SelectedRecord
                        Sort light
                          keys: [mta_tax]
                            SelectedRecord
                                Async JIT Filter workers: 1
                                  filter: vendor_id in [A1,A2]
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: trips
                                          intervals: [("2019-06-30T00:00:00.000000Z","MAX")]
                    """);

            assertQuery("""
                    vendor_id
                    A1
                    A2
                    """, query, null, true, false);
        });
    }

    @Test
    public void testOrderByPrefixedColumnNotOnSelectList2() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE t1 (
                      vendor_id SYMBOL,
                      pickup_datetime TIMESTAMP,
                      tax DOUBLE,
                      mta_tax DOUBLE
                    ) timestamp (pickup_datetime) PARTITION BY MONTH""");
            execute("""
                    CREATE TABLE t2 (
                      vendor_id SYMBOL,
                      mta_tax DOUBLE
                    )""");

            execute("insert into t1 " +
                    "select 'A' || x, dateadd('s', x::int, '2019-06-30T00:00:00.000000Z'), x::timestamp, x, 0 from long_sequence(10)");

            execute("insert into t2 " +
                    "select 'A' || x, -x from long_sequence(10)");

            String query = "select a.vendor_id " +
                    "from t1 a " +
                    "join t2 b on a.vendor_id = b.vendor_id " +
                    "where a.pickup_datetime >= '2019-06-30T00:00:00.000000Z' " +
                    "and b.vendor_id in ('A1', 'A2') " +
                    "order by b.mta_tax;";

            assertPlanNoLeakCheck(query, """
                    SelectedRecord
                        Sort
                          keys: [mta_tax]
                            SelectedRecord
                                Hash Join Light
                                  condition: b.vendor_id=a.vendor_id
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: t1
                                          intervals: [("2019-06-30T00:00:00.000000Z","MAX")]
                                    Hash
                                        Async JIT Filter workers: 1
                                          filter: vendor_id in [A1,A2]
                                            PageFrame
                                                Row forward scan
                                                Frame forward scan on: t2
                    """);

            assertQuery(
                    """
                            vendor_id
                            A2
                            A1
                            """,
                    query,
                    null,
                    true,
                    false
            );
        });
    }

    @Test
    public void testOrderByTimestampAndOtherField() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tab (" +
                    "  ts TIMESTAMP," +
                    "  key STRING," +
                    "  value int " +
                    ") timestamp (ts) PARTITION BY DAY");
            execute("insert into tab values (0, 'c', 1), (0, 'b', 2), (0, 'a', 3), (1, 'd', 4), (2, 'e', 5)");

            assertPlanNoLeakCheck("SELECT key " +
                            "FROM tab " +
                            "WHERE key IS NOT NULL " +
                            "ORDER BY ts, key " +
                            "LIMIT 10",
                    """
                            SelectedRecord
                                Sort light lo: 10 partiallySorted: true
                                  keys: [ts, key]
                                    Async JIT Filter workers: 1
                                      filter: key is not null
                                        PageFrame
                                            Row forward scan
                                            Frame forward scan on: tab
                            """);
        });

        assertLimitQueries("""
                        ts\tkey\tvalue
                        1970-01-01T00:00:00.000000Z\ta\t3
                        1970-01-01T00:00:00.000000Z\tb\t2
                        1970-01-01T00:00:00.000000Z\tc\t1
                        1970-01-01T00:00:00.000001Z\td\t4
                        1970-01-01T00:00:00.000002Z\te\t5
                        """,
                "SELECT * " +
                        "FROM tab " +
                        "WHERE key IS NOT NULL " +
                        "ORDER BY ts, key " +
                        "LIMIT ", "ts");

        assertLimitQueries("""
                        ts\tkey\tvalue
                        1970-01-01T00:00:00.000000Z\tc\t1
                        1970-01-01T00:00:00.000000Z\tb\t2
                        1970-01-01T00:00:00.000000Z\ta\t3
                        1970-01-01T00:00:00.000001Z\td\t4
                        1970-01-01T00:00:00.000002Z\te\t5
                        """,
                "SELECT * " +
                        "FROM tab " +
                        "WHERE key IS NOT NULL " +
                        "ORDER BY ts, key DESC " +
                        "LIMIT ", "ts");

        assertLimitQueries("""
                        ts\tkey\tvalue
                        1970-01-01T00:00:00.000002Z\te\t5
                        1970-01-01T00:00:00.000001Z\td\t4
                        1970-01-01T00:00:00.000000Z\ta\t3
                        1970-01-01T00:00:00.000000Z\tb\t2
                        1970-01-01T00:00:00.000000Z\tc\t1
                        """,
                "SELECT * " +
                        "FROM tab " +
                        "WHERE key IS NOT NULL " +
                        "ORDER BY ts desc, key " +
                        "LIMIT ", "ts###DESC");

        assertLimitQueries("""
                        ts\tkey\tvalue
                        1970-01-01T00:00:00.000002Z\te\t5
                        1970-01-01T00:00:00.000001Z\td\t4
                        1970-01-01T00:00:00.000000Z\tc\t1
                        1970-01-01T00:00:00.000000Z\tb\t2
                        1970-01-01T00:00:00.000000Z\ta\t3
                        """,
                "SELECT * " +
                        "FROM tab " +
                        "WHERE key IS NOT NULL " +
                        "ORDER BY ts desc, key desc " +
                        "LIMIT ", "ts###DESC");
    }

    @Test
    public void testOrderByTimestampWithColumnTops() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-01T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 10000000000) " +
                        "  from long_sequence(10);",
                "alter table trips add col1 int",
                "alter table trips add col2 string",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-02T12:00:00', 'yyyy-MM-ddTHH:mm:ss'), 10000000000), " +
                        "  x * 10, " +
                        "  cast(x * 100 as string) " +
                        "  from long_sequence(10)"
        );

        assertQuery("""
                        l\tts\tcol1\tcol2
                        10\t2022-01-03T13:00:00.000000Z\t100\t1000
                        9\t2022-01-03T10:13:20.000000Z\t90\t900
                        8\t2022-01-03T07:26:40.000000Z\t80\t800
                        10\t2022-01-02T01:00:00.000000Z\tnull\t
                        """,
                "select l as l, ts, col1, col2 from trips where l > 7 order by ts desc limit 4",
                null, "ts###DESC", true, false
        );

        assertQuery("""
                        l\tts\tcol1\tcol2
                        1010\t2022-01-03T13:00:00.000000Z\t100\t1000
                        1009\t2022-01-03T10:13:20.000000Z\t90\t900
                        1008\t2022-01-03T07:26:40.000000Z\t80\t800
                        1010\t2022-01-02T01:00:00.000000Z\tnull\t
                        """,
                "select l + 1000 as l, ts, col1, col2 from trips where l > 7 order by ts desc limit 4",
                null, "ts###DESC", true, false
        );

        assertQuery("""
                        l\tts\tcol1\tcol2
                        9\t2022-01-01T22:13:20.000000Z\tnull\t
                        10\t2022-01-02T01:00:00.000000Z\tnull\t
                        9\t2022-01-03T10:13:20.000000Z\t90\t900
                        10\t2022-01-03T13:00:00.000000Z\t100\t1000
                        """,
                "select l, ts, col1, col2 from trips where l > 8 order by ts",
                null, "ts", true, false
        );

        assertQuery("""
                        l\tts\tcol1\tcol2
                        4\t2022-01-02T20:20:00.000000Z\t40\t400
                        10\t2022-01-02T01:00:00.000000Z\tnull\t
                        9\t2022-01-01T22:13:20.000000Z\tnull\t
                        8\t2022-01-01T19:26:40.000000Z\tnull\t
                        7\t2022-01-01T16:40:00.000000Z\tnull\t
                        """,
                "select l, ts, col1, col2 from trips where ts between '2022-01-01T14' and '2022-01-02T23' and l > 3 order by ts desc limit 5",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByTimestampWithComplexJitFilter() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(1000);"
        );

        assertQuery("""
                        l\tts
                        5\t2022-01-07T15:06:40.000000Z
                        """,
                "select l, ts from trips " +
                        "where l <=5 and ts < to_timestamp('2022-01-08T00:00:00', 'yyyy-MM-ddTHH:mm:ss') " +
                        "order by ts desc limit 1",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByTimestampWithComplexJitFilterAndLimit() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by month;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(1000);"
        );

        assertQuery("""
                        l\tts
                        2\t2022-01-04T03:46:40.000000Z
                        1\t2022-01-03T00:00:00.000000Z
                        """,
                "select l, ts from trips " +
                        "where l <=5 and ts < to_timestamp('2022-01-08T00:00:00', 'yyyy-MM-ddTHH:mm:ss') " +
                        "order by ts desc limit 3, 5",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByTimestampWithJitAndIntervalFilters() throws Exception {

        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by day;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence('2022-01-03', 50000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery("""
                        l\tts
                        3\t2022-01-04T03:46:40.000000Z
                        2\t2022-01-03T13:53:20.000000Z
                        1\t2022-01-03T00:00:00.000000Z
                        """,
                "select l, ts from trips " +
                        "where l <=5 and ts < '2022-01-04T04' " +
                        "order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByTimestampWithJitFilterAndLimitAsc() throws Exception {

        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery("""
                        l\tts
                        1\t2022-01-03T00:00:00.000000Z
                        2\t2022-01-04T03:46:40.000000Z
                        3\t2022-01-05T07:33:20.000000Z
                        4\t2022-01-06T11:20:00.000000Z
                        5\t2022-01-07T15:06:40.000000Z
                        """,
                "select l, ts from trips where l <=5 order by ts asc limit 5",
                null, "ts###ASC", true, false
        );
    }

    @Test
    public void testOrderByTimestampWithJitFilterAndLimitDesc() throws Exception {

        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery("""
                        l\tts
                        5\t2022-01-07T15:06:40.000000Z
                        4\t2022-01-06T11:20:00.000000Z
                        3\t2022-01-05T07:33:20.000000Z
                        2\t2022-01-04T03:46:40.000000Z
                        1\t2022-01-03T00:00:00.000000Z
                        """,
                "select l, ts from trips where l <=5 order by ts desc limit 5",
                null, "ts###DESC", true, false
        );
    }

    @Test
    public void testOrderByTimestampWithJitFilterDesc() throws Exception {
        testOrderByTimestampWithFilterDesc();
    }

    @Test
    public void testOrderByTimestampWithNonJitFilter() throws Exception {
        int jitMode = sqlExecutionContext.getJitMode();
        try {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            runQueries(
                    "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                    "insert into trips " +
                            "  select x," +
                            "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                            "  from long_sequence(10);"
            );

            assertQuery("""
                            l\tts
                            5\t2022-01-07T15:06:40.000000Z
                            4\t2022-01-06T11:20:00.000000Z
                            3\t2022-01-05T07:33:20.000000Z
                            2\t2022-01-04T03:46:40.000000Z
                            1\t2022-01-03T00:00:00.000000Z
                            """,
                    "select l, ts from trips where l <=5 order by ts desc limit 5",
                    null, "ts###DESC", true, false
            );
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    @Test
    public void testOrderByTimestampWithNonJitFilterDesc() throws Exception {
        int jitMode = sqlExecutionContext.getJitMode();
        try {
            sqlExecutionContext.setJitMode(SqlJitMode.JIT_MODE_DISABLED);
            testOrderByTimestampWithFilterDesc();
        } finally {
            sqlExecutionContext.setJitMode(jitMode);
        }
    }

    @Test
    public void testOrderByWithFilterAndIPv4ConversionToLong() throws Exception {
        assertMemoryLeak(() -> {
            execute("""
                    CREATE TABLE 'network_nodes_test' (\s
                    \ttimestamp TIMESTAMP,
                    \tnode_name SYMBOL CAPACITY 65536 CACHE INDEX CAPACITY 65536,
                    \thost_ip IPv4,
                    \tstatus SYMBOL CAPACITY 8 CACHE
                    ) timestamp(timestamp) PARTITION by DAY BYPASS WAL
                    WITH maxUncommittedRows=500000, o3MaxLag=600000000us;""");

            execute("""
                    insert into network_nodes_test
                      select
                        rnd_timestamp(to_timestamp('20241231', 'yyyyMMdd'),to_timestamp('20250101', 'yyyyMMdd'),0),
                        rnd_symbol('node01','node02','node03'),
                        rnd_ipv4('10.13.0.0/16',0),
                        rnd_symbol('active','removed')
                      from long_sequence(30);
                    """);

            // this would fail with an UnsupportedOperationException due to getLongIPv4 not being implemented
            // for SelectedRecord
            assertSql("""
                            timestamp\tnode_name\thost_ip\tstatus
                            2024-12-31T19:10:58.038243Z\tnode01\t10.13.2.123\tactive
                            2024-12-31T13:35:33.630915Z\tnode03\t10.13.31.14\tactive
                            2024-12-31T12:09:29.743508Z\tnode03\t10.13.31.173\tactive
                            2024-12-31T05:28:56.199865Z\tnode03\t10.13.35.79\tactive
                            2024-12-31T14:04:07.197985Z\tnode03\t10.13.37.167\tactive
                            2024-12-31T08:12:46.122052Z\tnode01\t10.13.57.52\tactive
                            2024-12-31T22:29:40.370707Z\tnode02\t10.13.72.212\tactive
                            2024-12-31T19:23:32.364885Z\tnode02\t10.13.112.55\tactive
                            2024-12-31T02:22:01.436568Z\tnode02\t10.13.128.249\tactive
                            2024-12-31T04:42:29.244760Z\tnode02\t10.13.136.54\tactive
                            2024-12-31T23:26:59.485737Z\tnode01\t10.13.144.59\tactive
                            2024-12-31T07:23:12.483203Z\tnode03\t10.13.151.135\tactive
                            2024-12-31T10:17:14.723035Z\tnode01\t10.13.157.242\tactive
                            2024-12-31T21:31:53.805150Z\tnode01\t10.13.166.106\tactive
                            2024-12-31T09:30:33.694129Z\tnode02\t10.13.168.230\tactive
                            2024-12-31T22:56:53.598432Z\tnode03\t10.13.213.95\tactive
                            2024-12-31T07:27:38.262625Z\tnode02\t10.13.217.59\tactive
                            2024-12-31T04:22:52.424548Z\tnode02\t10.13.237.229\tactive
                            2024-12-31T06:40:02.794603Z\tnode02\t10.13.249.36\tactive
                            2024-12-31T14:59:11.599601Z\tnode01\t10.13.249.187\tactive
                            2024-12-31T18:42:59.090116Z\tnode03\t10.13.253.254\tactive
                            """,
                    """
                            select * from (network_nodes_test LATEST on timestamp PARTITION by host_ip)
                            where status = 'active'
                            order by host_ip;""");
        });
    }

    private void assertLimitQueries(String result, String query, String expectedTimestamp) throws Exception {
        int firstLineStart = result.indexOf('\n') + 1;
        String header = result.substring(0, firstLineStart);

        for (int hi = 0, hiIdx = 0; hi < 10; hi++) {
            hiIdx = result.indexOf('\n', hiIdx + 1);
            if (hiIdx == -1) {
                hiIdx = result.length();
            } else {
                hiIdx++;
            }

            for (int lo = 0, loIdx = 0; lo <= hi; lo++) {
                loIdx = result.indexOf('\n', loIdx + 1);
                if (loIdx == -1) {
                    loIdx = result.length();
                } else {
                    loIdx++;
                }

                String expected = header + result.substring(loIdx, hiIdx);

                assertQuery(
                        expected,
                        query + " " + lo + ", " + hi,
                        expectedTimestamp,
                        true,
                        true
                );
            }
        }
    }

    private void assertOrderByInOverClause(String expected, String direction) throws Exception {
        assertQuery(expected,
                "select ts, temp from \n" +
                        "( \n" +
                        "  select temp, ts, \n" +
                        "         row_number() over (partition by timestamp_floor('y', ts) order by temp " + direction + ")  rid \n" +
                        "  from weather \n" +
                        ") inq \n" +
                        "where rid = 1 \n" +
                        "order by ts",
                "create table weather as " +
                        "(select cast(x*36000000000 as timestamp) ts, \n" +
                        "  rnd_float(0)*100 temp from long_sequence(1000));", "ts"
        );
    }

    private void runQueries(String... queries) throws Exception {
        assertMemoryLeak(() -> {
            for (String query : queries) {
                execute(query);
            }
        });
    }

    private void testOrderByTimestampWithFilterDesc() throws Exception {
        runQueries(
                "CREATE TABLE trips(l long, ts TIMESTAMP) timestamp(ts) partition by year;",
                "insert into trips " +
                        "  select x," +
                        "  timestamp_sequence(to_timestamp('2022-01-03T00:00:00', 'yyyy-MM-ddTHH:mm:ss'), 100000000000) " +
                        "  from long_sequence(10);"
        );

        assertQuery("""
                        l\tts
                        5\t2022-01-07T15:06:40.000000Z
                        4\t2022-01-06T11:20:00.000000Z
                        3\t2022-01-05T07:33:20.000000Z
                        2\t2022-01-04T03:46:40.000000Z
                        1\t2022-01-03T00:00:00.000000Z
                        """,
                "select l, ts from trips where l <= 5 order by ts desc",
                null, "ts###DESC", true, false
        );
    }

    private void testOrderByWithFilter(String type, int order) throws Exception {

        String function;
        if ("double".equals(type) || "float".equals(type)) {
            function = "4+rnd_#TYPE#(50)*100";
        } else if ("short".equals(type) || "byte".equals(type)) {
            function = "rnd_#TYPE#(4,100)";
        } else if ("char".equals(type)) {
            function = "cast(rnd_byte(4,100) as char)";
        } else if ("symbol".equals(type)) {
            function = "cast('' || rnd_int(4,100,50) as symbol)";
        } else if ("string".equals(type)) {
            function = "'' || rnd_int(4,100,50)";
        } else {
            function = "rnd_#TYPE#(4,100,50)";
        }

        runQueries(
                "CREATE TABLE test(x #TYPE#, ts TIMESTAMP) timestamp(ts) partition by month;".replace("#TYPE#", type),
                //should create 3+ partitions with randomly ordered x values
                ("""
                        insert into test \
                        select #FUNC#,
                            timestamp_sequence('2022-01-01'::timestamp, 100000000000)
                        from long_sequence(100)
                        union all\s
                        select cast(1 as #TYPE#), rnd_timestamp('2022-01-01'::timestamp, '2022-01-03'::timestamp + 33*100000000000 , 0)
                        union all  \
                        select cast(2 as #TYPE#), rnd_timestamp('2022-01-01'::timestamp + 34*100000000000, '2022-01-03'::timestamp + 66*100000000000 , 0)
                        union all \
                        select cast(3 as #TYPE#), rnd_timestamp('2022-01-01'::timestamp + 67*100000000000, '2022-01-03'::timestamp + 100*100000000000 , 0)
                        """)
                        .replace("#FUNC#", function)
                        .replace("#TYPE#", type)
        );
        //add new column and create more partitions to trigger jit col tops case
        assertMemoryLeak(() -> execute("alter table test add column y double;"));
        runQueries(("insert into test select #FUNC#, timestamp_sequence('2022-01-01'::timestamp + 100*100000000000, 100000000000), rnd_double() " +
                "from long_sequence(100) ")
                .replace("#FUNC#", function)
                .replace("#TYPE#", type));

        String expectedResult = switch (type) {
            case "float" -> order == ORDER_ASC ? "x\n1.0\n2.0\n3.0\n" : "x\n3.0\n2.0\n1.0\n";
            case "double" -> order == ORDER_ASC ? "x\n1.0\n2.0\n3.0\n" : "x\n3.0\n2.0\n1.0\n";
            case "timestamp" ->
                    order == ORDER_ASC ? "x\n1970-01-01T00:00:00.000001Z\n1970-01-01T00:00:00.000002Z\n1970-01-01T00:00:00.000003Z\n" :
                            "x\n1970-01-01T00:00:00.000003Z\n1970-01-01T00:00:00.000002Z\n1970-01-01T00:00:00.000001Z\n";
            case "char" -> order == ORDER_ASC ? "x\n\u0001\n\u0002\n\u0003\n" : "x\n\u0003\n\u0002\n\u0001\n";
            default -> order == ORDER_ASC ? "x\n1\n2\n3\n" : "x\n3\n2\n1\n";
        };

        if ("string".equals(type) || "symbol".equals(type)) {
            assertQuery(expectedResult,
                    ("select x from test where x in ('1', '2', '3') and y = null order by ts " + (order == ORDER_ASC ? "asc" : "desc")).replace("#TYPE#", type),
                    null, null, true, false
            );
        } else if ("char".equals(type)) {
            assertQuery(expectedResult,
                    ("select x from test where x in (cast(1 as char), cast(2 as char), cast(3 as char)) and y = null order by ts " + (order == ORDER_ASC ? "asc" : "desc")).replace("#TYPE#", type),
                    null, null, true, false
            );
        } else {
            assertQuery(expectedResult,
                    ("select x from test where x <= 3 and y = null order by ts " + (order == ORDER_ASC ? "asc" : "desc")).replace("#TYPE#", type),
                    null, null, true, false
            );
        }
    }
}
