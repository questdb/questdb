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

import io.questdb.cairo.SqlJitMode;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.LongPredicate;

/**
 * A timestamp literal finer than the column compares exactly, at the literal's precision, wherever the
 * predicate runs: interval scan, JIT or Java filter, join filter, runtime bound or Parquet pruning.
 * Binding rewrites a constant comparison into the same comparison at the column's precision. Nanosecond
 * values end at year 2262: a literal in a later year with a sub-microsecond fraction takes microsecond
 * precision, and its extra digits are dropped (see {@link #testFractionBeyondNanosecondRangeKeepsMicroseconds}).
 */
public class TimestampLiteralPrecisionTest extends AbstractCairoTest {
    private static final DateTimeFormatter NANOS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'").withZone(ZoneOffset.UTC);
    private static final String[] OPERATORS = {"=", "!=", "<", "<=", ">", ">="};
    private static final long[] ROWS = {0, 1, 2, 999, 1000};

    @Test
    public void testBetweenAndInBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (boolean isJit : new boolean[]{true, false}) {
                setJit(isJit);
                for (long literal : literals()) {
                    final long upper = literal + 998_001;
                    final String lo = literal(literal);
                    final String hi = literal(upper);
                    assertRows("ts BETWEEN " + lo + " AND " + hi, row -> row >= literal && row <= upper);
                    assertRows("ts BETWEEN " + hi + " AND " + lo, row -> row >= literal && row <= upper);
                    assertRows("ts NOT BETWEEN " + lo + " AND " + hi, row -> row < literal || row > upper);
                    assertRows("ts IN (" + lo + ")", row -> row == literal);
                    assertRows("ts IN (" + lo + ", " + hi + ")", row -> row == literal || row == upper);
                    assertRows("ts NOT IN (" + lo + ", " + hi + ")", row -> row != literal && row != upper);
                    assertRows("ts IN " + lo, row -> row == literal);
                    assertRows("ts NOT IN " + lo, row -> row != literal);
                    assertRows("ts = " + lo + " OR ts = " + hi, row -> row == literal || row == upper);
                    assertRows("t2 BETWEEN " + lo + " AND " + hi, row -> row >= literal && row <= upper);
                    assertRows("t2 IN (" + lo + ", " + hi + ")", row -> row == literal || row == upper);
                }
            }
        });
    }

    @Test
    public void testComparisonAgreesAcrossJoins() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE a (ts TIMESTAMP, id INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("CREATE TABLE b (ts TIMESTAMP, id INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO a VALUES ('2024-01-01T00:00:00', 1), ('2024-01-01T00:00:02', 2)");
            execute("INSERT INTO b VALUES ('2024-01-01T00:00:01', 1), ('2024-01-01T00:00:03', 2)");
            final String[] sources = {
                    "a",
                    "a JOIN b ON a.id = b.id",
                    "a LEFT JOIN b ON a.id = b.id",
                    "a FULL JOIN b ON a.id = b.id",
                    "b RIGHT JOIN a ON a.id = b.id",
                    "b CROSS JOIN a"
            };
            for (String source : sources) {
                for (String extra : new String[]{"", " OR a.id < 0"}) {
                    assertQuery("SELECT DISTINCT a.id FROM " + source + " WHERE a.ts = '2024-01-01T00:00:00.000000500'" + extra)
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("id\n");
                    assertQuery("SELECT DISTINCT a.id FROM " + source + " WHERE a.ts < '2024-01-01T00:00:00.000000500'" + extra)
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("""
                                    id
                                    1
                                    """);
                    assertQuery("SELECT DISTINCT a.id FROM " + source + " WHERE a.ts != '2024-01-01T00:00:00.000000500'" + extra + " ORDER BY 1")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("""
                                    id
                                    1
                                    2
                                    """);
                }
            }
        });
    }

    @Test
    public void testOperatorBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            for (boolean isJit : new boolean[]{true, false}) {
                setJit(isJit);
                for (long literal : literals()) {
                    final String text = literal(literal);
                    for (String operator : OPERATORS) {
                        assertRows("ts " + operator + " " + text, row -> compare(row, operator, literal));
                        assertRows(text + " " + operator + " ts", row -> compare(literal, operator, row));
                        assertRows("t2 " + operator + " " + text, row -> compare(row, operator, literal));
                        assertRows("ts " + operator + " " + text + "::timestamp_ns", row -> compare(row, operator, literal));
                        assertRows("dateadd('s', 1, ts) " + operator + " " + literal(literal + 1_000_000_000), row -> compare(row, operator, literal));
                    }
                }
            }
        });
    }

    @Test
    public void testParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("INSERT INTO p VALUES ('2024-01-01', 99, '2024-01-01')");
            execute("ALTER TABLE p CONVERT PARTITION TO PARQUET WHERE ts < '2024-01-01'");
            for (long literal : literals()) {
                final String text = literal(literal);
                for (String operator : OPERATORS) {
                    assertRows("ts " + operator + " " + text + " AND id < 99", row -> compare(row, operator, literal));
                    assertRows("t2 " + operator + " " + text + " AND id < 99", row -> compare(row, operator, literal));
                }
                assertRows("t2 BETWEEN " + text + " AND " + literal(literal + 998_001) + " AND id < 99", row -> row >= literal && row <= literal + 998_001);
            }
        });
    }

    @Test
    public void testPlans() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            setJit(true);
            assertQuery("SELECT id FROM p WHERE ts = '1970-01-01T00:00:00.000001500Z'")
                    .noLeakCheck()
                    .withPlanContaining("Empty table")
                    .returns("id\n");
            assertQuery("SELECT id FROM p WHERE ts < '1970-01-01T00:00:00.000001500Z'")
                    .noLeakCheck()
                    .withPlanContaining("intervals: [(\"MIN\",\"1970-01-01T00:00:00.000001Z\")]")
                    .returns("""
                            id
                            0
                            1
                            """);
            assertQuery("SELECT id FROM p WHERE ts > '1969-12-31T23:59:59.999999500Z'")
                    .noLeakCheck()
                    .withPlanContaining("intervals: [(\"1970-01-01T00:00:00.000000Z\",\"MAX\")]")
                    .returns("""
                            id
                            0
                            1
                            2
                            3
                            4
                            """);
            assertQuery("SELECT id FROM p WHERE ts = '1970-01-01T00:00:00.000001000Z' OR ts = '1970-01-01T00:00:00.000002500Z'")
                    .noLeakCheck()
                    .withPlanContaining("intervals: [(\"1970-01-01T00:00:00.000001Z\",\"1970-01-01T00:00:00.000001Z\")]")
                    .returns("""
                            id
                            1
                            """);
            assertQuery("SELECT id FROM p WHERE t2 < '1970-01-01T00:00:00.000002000Z' AND id > 0")
                    .noLeakCheck()
                    .withPlanContaining("Async JIT Filter")
                    .returns("""
                            id
                            1
                            """);
            assertQuery("SELECT id FROM p WHERE t2 < '1970-01-01T00:00:00.000001500Z' AND id > 0")
                    .noLeakCheck()
                    .withPlanContaining("Async JIT Filter", "filter: (t2<1970-01-01T00:00:00.000002Z and 0<id)")
                    .returns("""
                            id
                            1
                            """);
            assertQuery("SELECT id FROM p WHERE t2 = '1970-01-01T00:00:00.000001500Z' OR id > 3")
                    .noLeakCheck()
                    .withPlanContaining("Async JIT Filter")
                    .returns("""
                            id
                            4
                            """);
        });
    }

    @Test
    public void testDateBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE d (dt DATE, id INT)");
            execute("INSERT INTO d VALUES (0::date, 0), (1::date, 1), (2::date, 2), (1000::date, 3), (null, 4)");
            final long[] dates = {0, 1, 2, 1000};
            for (boolean isJit : new boolean[]{true, false}) {
                setJit(isJit);
                for (long micros : new long[]{-1_001, -1_000, -999, -1, 0, 1, 999, 1_000, 1_001, 1_500, 2_000}) {
                    final String text = literal(micros * 1_000);
                    for (String operator : OPERATORS) {
                        assertDates(dates, "dt " + operator + " " + text, row -> compare(row, operator, micros), operator.equals("!="));
                        assertDates(dates, text + " " + operator + " dt", row -> compare(micros, operator, row), operator.equals("!="));
                    }
                    final long upper = micros + 999;
                    assertDates(dates, "dt BETWEEN " + text + " AND " + literal(upper * 1_000), row -> row >= micros && row <= upper, false);
                    assertDates(dates, "dt IN (" + text + ", " + literal(upper * 1_000) + ")", row -> row == micros || row == upper, false);
                }
            }
        });
    }

    @Test
    public void testIntervalStringAtNanosecondRangeEdges() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE edge (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO edge VALUES ('2262-04-11T23:47:16.854775Z'), ('2262-04-11T23:47:16.854776Z'), ('3000-01-01T00:00:00.000000Z')");
            assertQuery("SELECT ts FROM edge WHERE ts IN '3000-01-01T00:00:00.000000000Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts
                            3000-01-01T00:00:00.000000Z
                            """);
            assertQuery("SELECT ts FROM edge WHERE ts IN '2262-04-11T23:47:16.854775807Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts
                            2262-04-11T23:47:16.854775Z
                            """);
            assertQuery("SELECT ts FROM edge WHERE ts NOT IN '2262-04-11T23:47:16.854775807Z'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts
                            2262-04-11T23:47:16.854776Z
                            3000-01-01T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testFractionBeyondNanosecondRangeKeepsMicroseconds() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE f (ts TIMESTAMP, id INT) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO f VALUES ('3000-01-01T00:00:00.000000Z', 1), ('3000-01-01T00:00:00.000001Z', 2)");
            assertQuery("SELECT id FROM f WHERE ts < '3000-01-01T00:00:00.000000500Z'")
                    .noLeakCheck()
                    .returns("id\n");
            assertQuery("SELECT id FROM f WHERE ts = '3000-01-01T00:00:00.000000500Z'")
                    .noLeakCheck()
                    .returns("""
                            id
                            1
                            """);
        });
    }

    @Test
    public void testNotEqualsKeepsNullRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (t TIMESTAMP, d DATE, id INT)");
            execute("INSERT INTO n VALUES (1::timestamp, 1::date, 1), (null, null, 2)");
            for (String predicate : new String[]{
                    "t != '1970-01-01T00:00:00.000001000Z'",
                    "t != '1970-01-01T00:00:00.000001500Z'",
                    "d != '1970-01-01T00:00:00.001000Z'",
                    "d != '1970-01-01T00:00:00.001500Z'"
            }) {
                final boolean isRepresentable = predicate.contains("1000Z") || predicate.contains("001000Z");
                assertQuery("SELECT id FROM n WHERE " + predicate + " ORDER BY id")
                        .noLeakCheck()
                        .sizeMayVary()
                        .returns(isRepresentable ? "id\n2\n" : "id\n1\n2\n");
            }
        });
    }

    @Test
    public void testRuntimeBounds() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE q (v TIMESTAMP_NS, s STRING)");
            for (long literal : literals()) {
                bindVariableService.clear();
                bindVariableService.setTimestampNano("b", literal);
                execute("TRUNCATE TABLE q");
                execute("INSERT INTO q VALUES (" + literal + "::timestamp_ns, " + literal(literal) + ")");
                for (String operator : OPERATORS) {
                    assertRows("ts " + operator + " :b", row -> compare(row, operator, literal));
                    assertRows("ts " + operator + " (SELECT v FROM q)", row -> compare(row, operator, literal));
                    assertRows("ts " + operator + " (SELECT s FROM q)", row -> compare(row, operator, literal));
                }
                assertRows("ts BETWEEN :b AND " + literal(literal + 998_001), row -> row >= literal && row <= literal + 998_001);
                assertRows("ts BETWEEN (SELECT v FROM q) AND " + literal(literal + 998_001), row -> row >= literal && row <= literal + 998_001);
                assertRows("ts IN (:b)", row -> row == literal);
                assertRows("ts NOT IN (:b)", row -> row != literal);
            }
        });
    }

    private static boolean compare(long left, String operator, long right) {
        return switch (operator) {
            case "=" -> left == right;
            case "!=" -> left != right;
            case "<" -> left < right;
            case "<=" -> left <= right;
            case ">" -> left > right;
            default -> left >= right;
        };
    }

    private static String literal(long nanos) {
        return "'" + NANOS.format(Instant.ofEpochSecond(Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L))) + "'";
    }

    private static long[] literals() {
        return new long[]{-1_001, -1_000, -999, -1, 0, 1, 999, 1_000, 1_001, 1_999, 2_000, 2_001, 999_999, 1_000_000, 1_000_001};
    }

    private static void setJit(boolean isJit) {
        sqlExecutionContext.setJitMode(isJit ? SqlJitMode.JIT_MODE_ENABLED : SqlJitMode.JIT_MODE_DISABLED);
    }

    /**
     * Asserts the predicate alone, inside an OR that defeats interval extraction, and beside another
     * conjunct, against rows compared at nanosecond precision.
     */
    private void assertRows(String predicate, LongPredicate isKept) throws Exception {
        final StringBuilder expected = new StringBuilder("id\n");
        for (int i = 0; i < ROWS.length; i++) {
            if (isKept.test(ROWS[i] * 1_000)) {
                expected.append(i).append('\n');
            }
        }
        for (String query : new String[]{
                "SELECT id FROM p WHERE " + predicate,
                "SELECT id FROM p WHERE " + predicate + " OR id < 0",
                "SELECT id FROM p WHERE id >= 0 AND (" + predicate + ")"
        }) {
            assertQuery(query).noLeakCheck().sizeMayVary().returns(expected);
        }
    }

    private void assertDates(long[] dates, String predicate, LongPredicate isKept, boolean isNullKept) throws Exception {
        final StringBuilder expected = new StringBuilder("id\n");
        for (int i = 0; i < dates.length; i++) {
            if (isKept.test(dates[i] * 1_000)) {
                expected.append(i).append('\n');
            }
        }
        if (isNullKept) {
            expected.append(dates.length).append('\n');
        }
        assertQuery("SELECT id FROM d WHERE " + predicate + " ORDER BY id").noLeakCheck().sizeMayVary().returns(expected);
        assertQuery("SELECT id FROM d WHERE (" + predicate + ") OR id < 0 ORDER BY id").noLeakCheck().sizeMayVary().returns(expected);
    }

    private void createTable() throws Exception {
        execute("CREATE TABLE p (ts TIMESTAMP, id INT, t2 TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        final StringBuilder insert = new StringBuilder("INSERT INTO p VALUES ");
        for (int i = 0; i < ROWS.length; i++) {
            insert.append(i > 0 ? ", " : "").append('(').append(ROWS[i]).append("::timestamp, ").append(i).append(", ")
                    .append(ROWS[i]).append("::timestamp)");
        }
        execute(insert);
    }
}
