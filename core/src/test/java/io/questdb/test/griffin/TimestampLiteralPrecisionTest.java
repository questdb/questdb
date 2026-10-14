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
import io.questdb.griffin.SqlCompiler;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.LongPredicate;

/**
 * A timestamp literal finer than the column compares exactly, at the literal's precision, wherever the
 * predicate runs: interval scan, JIT or Java filter, join filter, runtime bound or Parquet pruning.
 * Binding rewrites a constant comparison into the same comparison at the column's precision.
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
    public void testHistoricalIntervalFractions() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE h (ts TIMESTAMP, id INT)");
            execute("""
                    INSERT INTO h VALUES
                    ('1500-01-01T00:00:00.500000Z', 1),
                    ('1677-09-21T00:12:43.145225Z', 2),
                    ('1677-09-21T00:12:44.000000Z', 3),
                    ('1500-01-01T00:00:02.000000Z', 4),
                    (null, 5)
                    """);
            final ObjList<String> intervals = new ObjList<>(
                    "1500-01-01T00:00:00.000000001;1s",
                    "1500-01-[01..02]T00:00:00.000000001;1s",
                    "[1500]-01-01T00:00:00.000000001;1s",
                    "[1500,1501]-01-01T00:00:00.000000001;1s",
                    "[2020,1500]-01-01T00:00:00.000000001;1s",
                    "[1500..1501]-01-01T00:00:00.000000001;1s",
                    "[1500-01-01,1500-01-02]T00:00:00.000000001;1s",
                    "[2020-01-01,1500-01-01]T00:00:00.000000001;1s",
                    "[[2020,1500]-01-01]T00:00:00.000000001;1s",
                    "[1500]T00:00:00.000000001;1s"
            );
            for (boolean isJit : new boolean[]{true, false}) {
                setJit(isJit);
                for (int i = 0; i < intervals.size(); i++) {
                    final String interval = "'" + intervals.getQuick(i) + "'";
                    assertQuery("SELECT id FROM h WHERE ts IN " + interval + " ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("id\n1\n");
                    assertQuery("SELECT id FROM h WHERE ts NOT IN " + interval + " ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns("id\n2\n3\n4\n5\n");
                }
            }
        });
    }

    @Test
    public void testIntervalBracketFieldPrecision() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> intervals = new ObjList<>(
                    "2020-[0001]-01T00:00:00.000000001;1s",
                    "2020-01-[0001]T00:00:00.000000001;1s",
                    "2020-01-01T[0000]:00:00.000000001;1s",
                    "2020-[01,0001]-[01..0001]T00:[0000]:[0000].000000001;1s",
                    "[2020-[0001]-01,2020-01-[0001]]T00:00:00.000000001;1s"
            );
            for (int i = 0; i < intervals.size(); i++) {
                final String interval = intervals.getQuick(i);
                assertQuery("SELECT '2020-01-01T00:00:00.000000Z'::TIMESTAMP IN '" + interval
                        + "' AS at_start, '2020-01-01T00:00:00.500000Z'::TIMESTAMP IN '" + interval + "' AS middle")
                        .noLeakCheck()
                        .expectSize()
                        .returns("at_start\tmiddle\nfalse\ttrue\n");
            }
        });
    }

    @Test
    public void testIntervalBracketFractionRangePrecision() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> fractions = new ObjList<>(
                    "123456001", "[123456001..123456001]", "[123456001..123456002]"
            );
            for (int i = 0; i < fractions.size(); i++) {
                final String interval = "2020-01-01T00:00:00." + fractions.getQuick(i) + ";1s";
                assertQuery("SELECT '2020-01-01T00:00:00.123456Z'::TIMESTAMP IN '" + interval
                        + "' AS at_start, '2020-01-01T00:00:00.123457Z'::TIMESTAMP IN '" + interval + "' AS inside")
                        .noLeakCheck()
                        .expectSize()
                        .returns("at_start\tinside\nfalse\ttrue\n");
            }
        });
    }

    @Test
    public void testIntervalFractionPrecisionRange() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> microDates = new ObjList<>(
                    "-001-01-01", "-123-01-01", "+001-01-01", "0001-01-01", "1500-01-01", "1677-01-01",
                    "1677-09-21", "1677-12-01", "2262-01-01", "3000-01-01"
            );
            for (int i = 0; i < microDates.size(); i++) {
                final String date = microDates.getQuick(i);
                assertQuery("SELECT '" + date + "T00:00:00.000000Z'::TIMESTAMP IN '" + date
                        + "T00:00:00.000000001;1s' AS at_start, '" + date
                        + "T00:00:00.500000Z'::TIMESTAMP IN '" + date + "T00:00:00.000000001;1s' AS middle")
                        .noLeakCheck()
                        .expectSize()
                        .returns("at_start\tmiddle\ntrue\ttrue\n");
            }
            assertQuery("SELECT '2262-01-01T00:00:00.000000Z'::TIMESTAMP IN '[2261..2262]-01-01T00:00:00.000000001;1s' AS at_start")
                    .noLeakCheck()
                    .expectSize()
                    .returns("at_start\ntrue\n");
            final ObjList<String> nanoDates = new ObjList<>("1678-01-01", "2020-01-01", "2261-12-01");
            for (int i = 0; i < nanoDates.size(); i++) {
                final String date = nanoDates.getQuick(i);
                assertQuery("SELECT '" + date + "T00:00:00.000000Z'::TIMESTAMP IN '" + date
                        + "T00:00:00.000000001;1s' AS at_start, '" + date
                        + "T00:00:00.500000Z'::TIMESTAMP IN '" + date + "T00:00:00.000000001;1s' AS middle")
                        .noLeakCheck()
                        .expectSize()
                        .returns("at_start\tmiddle\nfalse\ttrue\n");
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
    public void testFractionOutsideNanosecondRangeAgreesAcrossPredicates() throws Exception {
        assertMemoryLeak(() -> {
            final ObjList<String> microDates = new ObjList<>("-001-01-01", "0001-01-01", "1500-01-01", "3000-01-01");
            final ObjList<String> nanoDates = new ObjList<>("1677-12-01", "1678-01-01", "2020-01-01", "2261-12-01", "2262-01-01");
            final ObjList<String> dates = new ObjList<>();
            dates.addAll(microDates);
            dates.addAll(nanoDates);
            final StringBuilder insert = new StringBuilder("INSERT INTO o VALUES ");
            for (int i = 0, n = dates.size(); i < n; i++) {
                insert.append(i == 0 ? "" : ", ")
                        .append("('").append(dates.getQuick(i)).append("T00:00:00.000000Z', ").append(2 * i).append("), ")
                        .append("('").append(dates.getQuick(i)).append("T00:00:00.000001Z', ").append(2 * i + 1).append(')');
            }
            execute("CREATE TABLE o (ts TIMESTAMP, id INT)");
            execute(insert);
            for (boolean isJit : new boolean[]{true, false}) {
                setJit(isJit);
                for (int i = 0, n = dates.size(); i < n; i++) {
                    final String date = dates.getQuick(i);
                    final String literal = "'" + date + "T00:00:00.000000001'";
                    final boolean isTruncated = i < microDates.size();
                    final boolean isIntervalTruncated = isTruncated || date.startsWith("1677") || date.startsWith("2262");
                    final String fromLiteral = isTruncated ? "id\n" + 2 * i + "\n" + (2 * i + 1) + "\n" : "id\n" + (2 * i + 1) + "\n";
                    assertQuery("SELECT id FROM o WHERE ts >= " + literal + " AND ts < '" + date + "T00:00:01' ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns(fromLiteral);
                    assertQuery("SELECT id FROM o WHERE ts IN '" + date + "T00:00:00.000000001;1s' ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns(isIntervalTruncated ? "id\n" + 2 * i + "\n" + (2 * i + 1) + "\n" : fromLiteral);
                    assertQuery("SELECT id FROM o WHERE ts = " + literal + " ORDER BY id")
                            .noLeakCheck()
                            .sizeMayVary()
                            .returns(isTruncated ? "id\n" + 2 * i + "\n" : "id\n");
                }
            }
        });
    }

    @Test
    public void testNanoProjectionKeepsMicrosecondsBeyondNanosecondRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP_NS)");
            execute("INSERT INTO n VALUES (0), (9_223_372_036_854_775_000), (9_223_372_036_854_775_807), (null)");
            final ObjList<String> literals = new ObjList<>(
                    "'2262-04-11T23:47:16.854775808Z'",
                    "'2262-04-11T23:47:16.854775809Z'"
            );
            for (int i = 0; i < literals.size(); i++) {
                final String text = literals.getQuick(i);
                assertQuery("SELECT ts = " + text + " AS eq, ts < " + text + " AS lt, ts > " + text + " AS gt FROM n")
                        .noLeakCheck()
                        .expectSize()
                        .returns("""
                                eq\tlt\tgt
                                false\ttrue\tfalse
                                true\tfalse\tfalse
                                false\tfalse\ttrue
                                false\tfalse\tfalse
                                """);
            }
        });
    }

    @Test
    public void testNanoProjectionPreservesLiteralPrecision() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP_NS, id INT)");
            execute("""
                    INSERT INTO n VALUES
                    (1_577_836_800_000_000_001, 0),
                    (9_214_646_400_000_000_001, 1),
                    (9_223_372_036_854_775_807, 2)
                    """);
            final ObjList<String> literals = new ObjList<>(
                    "'2020-01-01T00:00:00.000000001+00:00'",
                    "'2262-01-01T00:00:00.000000001Z'",
                    "'2262-04-11T23:47:16.854775807Z'"
            );
            for (int i = 0; i < literals.size(); i++) {
                final String text = literals.getQuick(i);
                assertQuery("SELECT ts = " + text + " AS eq, ts < " + text + " AS lt, ts > " + text + " AS gt, "
                        + text + " = ts AS rev_eq, " + text + " < ts AS rev_lt, " + text + " > ts AS rev_gt, "
                        + "ts BETWEEN " + text + " AND " + text + " AS between_match FROM n WHERE id = " + i)
                        .noLeakCheck()
                        .returns("""
                                eq\tlt\tgt\trev_eq\trev_lt\trev_gt\tbetween_match
                                true\tfalse\tfalse\ttrue\tfalse\tfalse\ttrue
                                """);
            }
        });
    }

    @Test
    public void testNanoProjectionRejectsOutOfRangeLiterals() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE n (ts TIMESTAMP_NS)");
            execute("INSERT INTO n VALUES (0), (1), (null)");
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                final ObjList<String> literals = new ObjList<>(
                        "'2263-01-01'",
                        "'2263-01-01T00:00:00.000000001Z'",
                        "'2262-04-11T23:47:16.854776Z'",
                        "'1677-09-21T00:12:43.145224Z'"
                );
                for (int i = 0; i < literals.size(); i++) {
                    final String text = literals.getQuick(i);
                    for (String operator : OPERATORS) {
                        assertQuery("SELECT ts " + operator + " " + text + " FROM n")
                                .withCompiler(compiler)
                                .noLeakCheck()
                                .failsWith("[TIMESTAMP -> TIMESTAMP_NS]");
                        assertQuery("SELECT " + text + " " + operator + " ts FROM n")
                                .withCompiler(compiler)
                                .noLeakCheck()
                                .failsWith("[TIMESTAMP -> TIMESTAMP_NS]");
                    }
                    assertQuery("SELECT ts BETWEEN '1970-01-01' AND " + text + " FROM n")
                            .withCompiler(compiler)
                            .noLeakCheck()
                            .failsWith("[TIMESTAMP -> TIMESTAMP_NS]");
                    assertQuery("SELECT ts BETWEEN " + text + " AND '1970-01-01' FROM n")
                            .withCompiler(compiler)
                            .noLeakCheck()
                            .failsWith("[TIMESTAMP -> TIMESTAMP_NS]");
                    assertQuery("SELECT ts < '2261-01-01', ts = '1970-01-01T00:00:00.000000001Z', ts = null FROM n")
                            .withCompiler(compiler)
                            .noLeakCheck()
                            .expectSize()
                            .returns("""
                                    column\tcolumn1\tcolumn2
                                    true\tfalse\tfalse
                                    true\ttrue\tfalse
                                    false\tfalse\ttrue
                                    """);
                }
            }
        });
    }

    @Test
    public void testNanosecondBoundaryCasts() throws Exception {
        assertMemoryLeak(() -> assertQuery("""
                SELECT '1677-09-21T00:12:43.145224193Z'::TIMESTAMP_NS::LONG AS lo,
                       '1677-09-21T00:12:43.145224194Z'::TIMESTAMP_NS::LONG AS adjacent,
                       '2262-04-11T23:47:16.854775807Z'::TIMESTAMP_NS::LONG AS hi,
                       '1677-09-21T00:12:43.145224193Z'::TIMESTAMP_NS IS NULL AS is_null,
                       NULL::TIMESTAMP_NS::LONG AS null_cast
                """)
                .noLeakCheck()
                .expectSize()
                .returns("""
                        lo\tadjacent\thi\tis_null\tnull_cast
                        -9223372036854775807\t-9223372036854775806\t9223372036854775807\tfalse\tnull
                        """));
    }

    @Test
    public void testNanosecondBoundaryPredicates() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE boundary (ts TIMESTAMP_NS, id INT)");
            execute("""
                    INSERT INTO boundary VALUES
                    (-9_223_372_036_854_775_807, 1), (-9_223_372_036_854_775_806, 2),
                    (-9_223_372_036_854_775_000, 3), (0, 4),
                    (9_223_372_036_854_775_806, 5), (9_223_372_036_854_775_807, 6), (null, 7)
                    """);
            execute("CREATE TABLE designated (ts TIMESTAMP_NS, id INT) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO designated VALUES (0, 1), (1, 2), (1_577_836_800_000_000_000, 3)");
            final String lo = "'1677-09-21T00:12:43.145224193Z'";
            final String adjacent = "'1677-09-21T00:12:43.145224194Z'";
            final String hi = "'2262-04-11T23:47:16.854775807Z'";
            for (boolean isJit : new boolean[]{true, false}) {
                setJit(isJit);
                assertQuery("SELECT id FROM boundary WHERE ts < " + lo).noLeakCheck().returns("id\n");
                assertQuery("SELECT id FROM boundary WHERE ts = " + lo).noLeakCheck().returns("id\n1\n");
                assertQuery("SELECT id FROM boundary WHERE " + lo + " = ts").noLeakCheck().returns("id\n1\n");
                assertQuery("SELECT id FROM boundary WHERE ts < " + adjacent).noLeakCheck().returns("id\n1\n");
                assertQuery("SELECT id FROM boundary WHERE ts >= " + hi).noLeakCheck().returns("id\n6\n");
                assertQuery("SELECT id FROM boundary WHERE ts = " + hi).noLeakCheck().returns("id\n6\n");
                assertQuery("SELECT id FROM boundary WHERE ts IS NULL").noLeakCheck().returns("id\n7\n");
                assertQuery("SELECT id FROM designated WHERE ts < " + lo).noLeakCheck().returns("id\n");
                assertQuery("SELECT id FROM designated WHERE ts > " + lo).noLeakCheck().returns("id\n1\n2\n3\n");
                assertQuery("SELECT id FROM designated WHERE ts > " + hi).noLeakCheck().returns("id\n");
            }
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
