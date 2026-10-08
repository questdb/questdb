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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A comparison of a TIMESTAMP with text that does not parse as a timestamp means the same, and fails with the same
 * error, whether interval extraction over the designated timestamp implements it, a filter above a LIMIT evaluates
 * it, the optimiser prunes or folds it, or CREATE VIEW validates it.
 */
public class TimestampTextComparisonTest extends AbstractCairoTest {
    private static final String ALL = """
            v
            1
            2
            3
            4
            5
            """;
    private static final String[] TABLES = {"x", "xn"};

    private static void createTables() throws Exception {
        for (String table : TABLES) {
            final String type = table.equals("x") ? "TIMESTAMP" : "TIMESTAMP_NS";
            execute("CREATE TABLE " + table + " (ts " + type + ", t2 " + type + ", v INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO " + table + " VALUES " + """
                    ('2024-01-10', '2024-01-10', 1),
                    ('2024-01-31T23:00', '2024-01-31T23:00', 2),
                    ('2024-02-09', '2024-02-09', 3),
                    ('2024-03-01', '2024-03-01', 4),
                    ('2024-03-10', '2024-03-10', 5)
                    """);
        }
    }

    @Test
    public void testAlwaysInvalidText() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String table : TABLES) {
                assertError(table, "ts = 'abc'", 5, "invalid timestamp");
                assertError(table, "'abc' = ts", 0, "invalid timestamp");
                assertError(table, "ts = 'a;b'", 5, "not a timestamp, use IN keyword with intervals");
                assertError(table, "ts < 'abc'", 5, "Invalid date [str='abc']");
                assertError(table, "ts >= 'abc'", 6, "Invalid date [str='abc']");
                assertError(table, "ts BETWEEN 'abc' AND '2024'", 11, "Invalid date");
                assertError(table, "ts = 'abc' OR ts = '2024'", 5, "invalid timestamp");
                assertError(table, "ts = 'ab' || 'c'", 10, "Invalid date [str=abc]");
                assertError(table, "ts = 'abc'::symbol", 10, "Invalid date [str=abc]");
                assertError(table, "ts < 'abc'::symbol", 10, "Invalid date [str=abc]");
                assertError(table, "ts != 'a;b'::symbol", 11, "Not a date, use IN keyword with intervals");
                assertError(table, "ts != '2024-01;' || '1d'", 17, "Not a date, use IN keyword with intervals");
                assertError(table, "ts != 'abc'", 6, "invalid timestamp");
                assertError(table, "ts <> '1583077401000000'", 6, "invalid timestamp");
                assertError(table, "ts IN ('2024-01;1d', 'abc')", 21, "Invalid date");
                assertError(table, "ts IN ('abc', 'def')", 14, "Invalid date");
                assertError(table, "ts IN ('$today', '2024')", 7, "Invalid date");
                assertError(table, "ts IN ('abc'::symbol, '2024')", 12, "Invalid date [str=abc]");
            }
        });
    }

    @Test
    public void testDateComparisonReportsParserErrors() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE xd (d DATE, ts TIMESTAMP, v INT) TIMESTAMP(ts) PARTITION BY DAY");
            assertError("xd", "d BETWEEN 'abc' AND '2014'", 10, "Invalid date [str=abc]");
            assertError("xd", "d BETWEEN '2014' AND 'abc'", 21, "Invalid date [str=abc]");
            assertError("xd", "d < 'abc'", 4, "Invalid date [str=abc]");
            assertError("xd", "'abc' >= d", 0, "Invalid date [str=abc]");
            assertError("xd", "d = 'abc'", 4, "Invalid date [str=abc]");
            assertError("xd", "d != 'abc'", 5, "Invalid date [str=abc]");
            assertError("xd", "d IN ('2014', 'abc')", 14, "Invalid date [str=abc]");
        });
    }

    @Test
    public void testInEpochLiteral() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String table : TABLES) {
                assertError(table, "ts IN '1583077401000000'", 6, "Invalid date: 1583077401000000");
                assertError(table, "ts IN ('1583077401000000')", 7, "Invalid date: 1583077401000000");
                assertError(table, "ts NOT IN '1583077401000000'", 10, "Invalid date: 1583077401000000");
                assertError(table, "t2 IN '1583077401000000'", 6, "Invalid date: 1583077401000000");
                assertError(table, "ts IN ('1583077401000000', '2024')", 7, "Invalid date");
            }
        });
    }

    @Test
    public void testPartialTimestampText() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String table : TABLES) {
                assertSameOnEveryPath(table, "ts != '2024-01'", ALL,
                        "[(\"MIN\",\"2023-12-31T23:59:59.999999Z\"),(\"2024-01-01T00:00:00.000001Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "'2024-01' <> ts", ALL,
                        "[(\"MIN\",\"2023-12-31T23:59:59.999999Z\"),(\"2024-01-01T00:00:00.000001Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "ts != '2024-01'::varchar", ALL,
                        "[(\"MIN\",\"2023-12-31T23:59:59.999999Z\"),(\"2024-01-01T00:00:00.000001Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "ts != '2024-01-31T23'", """
                        v
                        1
                        3
                        4
                        5
                        """, "[(\"MIN\",\"2024-01-31T22:59:59.999999Z\"),(\"2024-01-31T23:00:00.000001Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "ts != '2024-03'", """
                        v
                        1
                        2
                        3
                        5
                        """, "[(\"MIN\",\"2024-02-29T23:59:59.999999Z\"),(\"2024-03-01T00:00:00.000001Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "ts != '2024-03-01T00:00:00.000000Z'", """
                        v
                        1
                        2
                        3
                        5
                        """, "[(\"MIN\",\"2024-02-29T23:59:59.999999");
                assertQuery("SELECT v FROM " + table + " WHERE ts != '2024-01'::symbol").noLeakCheck().returns(ALL);
                assertQuery("SELECT v FROM (SELECT * FROM " + table + " LIMIT 100) WHERE ts != '2024-01'::symbol").noLeakCheck().returns(ALL);
                assertQuery("SELECT v FROM " + table + " WHERE t2 != '2024-01'::symbol").noLeakCheck().returns(ALL);
                assertSameOnEveryPath(table, "ts = '2024-03-01'", """
                        v
                        4
                        """, "[(\"2024-03-01T00:00:00.000000Z\",\"2024-03-01T00:00:00.000000Z\")]");
                assertSameOnEveryPath(table, "ts = '2024-01'", "v\n", "[(\"2024-01-01T00:00:00.000000Z\",\"2024-01-01T00:00:00.000000Z\")]");
                assertSameOnEveryPath(table, "ts IN '2024-01'", """
                        v
                        1
                        2
                        """, "[(\"2024-01-01T00:00:00.000000Z\",\"2024-01-31T23:59:59.999999Z\")]");
                assertSameOnEveryPath(table, "ts IN ('2024-01')", """
                        v
                        1
                        2
                        """, "[(\"2024-01-01T00:00:00.000000Z\",\"2024-01-31T23:59:59.999999Z\")]");
                assertSameOnEveryPath(table, "ts NOT IN '2024-01'", """
                        v
                        3
                        4
                        5
                        """, "[(\"MIN\",\"2023-12-31T23:59:59.999999Z\"),(\"2024-02-01T00:00:00.000000Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "ts IN ('2024-01', '2024-03-01')", """
                        v
                        4
                        """, "[(\"2024-01-01T00:00:00.000000Z\",\"2024-01-01T00:00:00.000000Z\"),(\"2024-03-01T00:00:00.000000Z\",\"2024-03-01T00:00:00.000000Z\")]");
                assertSameOnEveryPath(table, "ts NOT IN ('2024-01', '2024-03-01')", """
                        v
                        1
                        2
                        3
                        5
                        """, "[(\"MIN\",\"2023-12-31T23:59:59.999999Z\"),(\"2024-01-01T00:00:00.000001Z\",\"2024-02-29T23:59:59.999999Z\"),(\"2024-03-01T00:00:00.000001Z\",\"MAX\")]");
                assertSameOnEveryPath(table, "ts BETWEEN '2024-01' AND '2024-03'", """
                        v
                        1
                        2
                        3
                        4
                        """, "[(\"2024-01-01T00:00:00.000000Z\",\"2024-03-01T00:00:00.000000Z\")]");
                assertSameOnEveryPath(table, "ts <= '2024-03'", """
                        v
                        1
                        2
                        3
                        4
                        """, "[(\"MIN\",\"2024-03-01T00:00:00.000000Z\")]");
                assertSameOnEveryPath(table, "ts > '2024-01-31T23'", """
                        v
                        3
                        4
                        5
                        """, "[(\"2024-01-31T23:00:00.000001Z\",\"MAX\")]");
            }
        });
    }

    @Test
    public void testExclusionOfIntervalText() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String table : TABLES) {
                assertError(table, "ts != '2024-01;1d'", 6, "not a timestamp, use IN keyword with intervals");
                assertError(table, "ts <> '2024-01;1d'", 6, "not a timestamp, use IN keyword with intervals");
                assertError(table, "'2024-01;1d' != ts", 0, "not a timestamp, use IN keyword with intervals");
                assertError(table, "t2 != '2024-01;1d'", 6, "not a timestamp, use IN keyword with intervals");
                assertError(table, "dateadd('d', 1, ts) != '2024-01;1d'", 23, "not a timestamp, use IN keyword with intervals");
                assertSameOnEveryPath(table, "ts NOT IN '2024-01;1d'", """
                        v
                        3
                        4
                        5
                        """, "[(\"MIN\",\"2023-12-31T23:59:59.999999Z\"),(\"2024-02-01T00:00:00.000000Z\",\"MAX\")]");
            }
        });
    }

    @Test
    public void testNegatedEqualityMatchesExclusion() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            for (String table : TABLES) {
                final String expected = """
                        v	b
                        1	true
                        2	true
                        3	true
                        4	false
                        5	true
                        """;
                assertQuery("SELECT v, NOT (ts = '2024-03') b FROM " + table).noLeakCheck().expectSize().returns(expected);
                assertQuery("SELECT v, ts != '2024-03' b FROM " + table).noLeakCheck().expectSize().returns(expected);
                assertQuery("SELECT v FROM " + table + " WHERE NOT (ts = '2024-03')").noLeakCheck().returns("""
                        v
                        1
                        2
                        3
                        5
                        """);
            }
        });
    }

    @Test
    public void testInListOfIntervalText() throws Exception {
        assertMemoryLeak(() -> {
            createTables();
            final String inside = """
                    v
                    1
                    2
                    4
                    """;
            final String outside = """
                    v
                    3
                    5
                    """;
            for (String table : TABLES) {
                assertSameEverywhere(table, "ts IN ('2024-01;1d', '2024-03-01')", inside, "intervals: [(\"2024-01-01T00:00:00.000000");
                assertSameEverywhere(table, "ts IN ('2024-01;1d'::varchar, '2024-03-01')", inside, "intervals: [(\"2024-01-01T00:00:00.000000");
                assertSameEverywhere(table, "ts NOT IN ('2024-01;1d', '2024-03-01')", outside, "intervals: [(\"MIN\",\"2023-12-31T23:59:59.999999");
                assertSameEverywhere(table, "ts IN ('2024-01;1d'::symbol)", """
                        v
                        1
                        2
                        """, "intervals: [(\"2024-01-01T00:00:00.000000");
                assertQuery("SELECT v FROM " + table + " WHERE t2 IN ('2024-01;1d', '2024-03-01')").noLeakCheck().returns(inside);
                assertQuery("SELECT v FROM " + table + " WHERE t2 IN ('2024-01;1d'::symbol)").noLeakCheck().returns("""
                        v
                        1
                        2
                        """);
                assertQuery("SELECT v FROM " + table + " WHERE ts IN (NULL, '2024-01;1d', '2024-03-01') OR ts = '2024-03-10'").noLeakCheck().returns("""
                        v
                        1
                        2
                        4
                        5
                        """);
            }
        });
    }

    private void assertError(String table, String predicate, int offset, String message) throws Exception {
        final String direct = "SELECT v FROM " + table + " WHERE " + predicate;
        assertQuery(direct).noLeakCheck().fails(direct.indexOf(predicate) + offset, message);
        final String limited = "SELECT v FROM (SELECT * FROM " + table + " LIMIT 100) WHERE " + predicate;
        assertQuery(limited).noLeakCheck().fails(limited.indexOf(predicate) + offset, message);
        final String pruned = "SELECT v FROM (SELECT v, " + predicate + " b FROM " + table + ")";
        assertQuery(pruned).noLeakCheck().fails(pruned.indexOf(predicate) + offset, message);
        final String folded = direct + " OR true";
        assertQuery(folded).noLeakCheck().fails(folded.indexOf(predicate) + offset, message);
        final String view = "CREATE VIEW v AS (" + direct + ")";
        assertException(view, view.indexOf(predicate) + offset, message);
    }

    private static String atPrecision(String table, String intervals) {
        return table.equals("x") ? intervals : intervals
                .replace(".999999Z", ".999999999Z")
                .replace(".000000Z", ".000000000Z")
                .replace(".000001Z", ".000000001Z");
    }

    private void assertSameOnEveryPath(String table, String predicate, String expected, String intervals) throws Exception {
        assertSameEverywhere(table, predicate, expected, "intervals: " + atPrecision(table, intervals));
        assertQuery("SELECT v FROM " + table + " WHERE " + predicate.replaceAll("\\bts\\b", "t2")).noLeakCheck().returns(expected);
    }

    private void assertSameEverywhere(String table, String predicate, String expected, String intervals) throws Exception {
        final String direct = "SELECT v FROM " + table + " WHERE " + predicate;
        assertQuery(direct).noLeakCheck().withPlanContaining("Interval forward scan on: " + table, intervals).returns(expected);
        assertQuery("SELECT v FROM (SELECT * FROM " + table + " LIMIT 100) WHERE " + predicate).noLeakCheck().returns(expected);
        assertQuery("SELECT v FROM (SELECT v, " + predicate + " b FROM " + table + ")").noLeakCheck().expectSize().returns("""
                v
                1
                2
                3
                4
                5
                """);
        assertQuery(direct + " OR true").noLeakCheck().expectSize().returns("""
                v
                1
                2
                3
                4
                5
                """);
        execute("CREATE VIEW v AS (" + direct + ")");
        assertQuery("SELECT * FROM v").noLeakCheck().returns(expected);
        execute("DROP VIEW v");
    }

}
