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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8String;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class IntervalExtractionTest extends AbstractCairoTest {
    private static final String[] MICRO_TIMES = {"2020-01-01T23:59:59.999998Z", "2020-01-01T23:59:59.999999Z",
            "2020-01-02T00:00:00.000000Z", "2020-01-02T00:00:00.000001Z", "2020-01-03T00:00:00.000000Z"};
    private static final String[] NANO_TIMES = {"2020-01-01T23:59:59.999999998Z", "2020-01-01T23:59:59.999999999Z",
            "2020-01-02T00:00:00.000000000Z", "2020-01-02T00:00:00.000000001Z", "2020-01-03T00:00:00.000000000Z"};

    @Test
    public void testConjunctionAndDescendingScanUseReaderTimestampIndex() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts >= '2020-01-01T23:59:59.999999Z'"
                            + " AND ts <= '2020-01-02T00:00:00.000001Z' ORDER BY ts DESC",
                    "Interval backward scan",
                    false,
                    "PageFrame > Row backward scan > Interval backward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000001Z	4
                            2020-01-02T00:00:00.000000Z	3
                            2020-01-01T23:59:59.999999Z	2
                            """
            );
            // The timestamp is reader column 2 and projected column 0; removing it
            // from the visible output must not change the interval's reader index.
            assertInterval(
                    "SELECT id FROM lp_interval WHERE ts >= '2020-01-01T23:59:59.999999Z'"
                            + " AND '2020-01-02T00:00:00.000001Z' >= ts ORDER BY ts DESC",
                    "Interval backward scan",
                    false,
                    "SelectedRecord > PageFrame > Row backward scan > Interval backward scan on: lp_interval",
                    """
                            id
                            4
                            3
                            2
                            """
            );
        });
    }

    @Test
    public void testEmptyIntersectionReturnsNoRowsWithoutResidual() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertInterval(
                    "SELECT id FROM lp_interval WHERE ts > '2020-01-03T00:00:00.000000Z'"
                            + " AND ts < '2020-01-02T00:00:00.000000Z'",
                    "Interval forward scan",
                    false,
                    "SelectedRecord > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    "id\n"
            );
        });
    }

    @Test
    public void testFactoryRetainsIntervalsAfterCompilerReuseAndClose() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String sql = "SELECT ts,id FROM lp_interval WHERE ts >= '2020-01-02T00:00:00.000000Z'"
                    + " AND ts < '2020-01-03T00:00:00.000000Z' ORDER BY ts";
            final RecordCursorFactory actual;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                actual = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try (RecordCursorFactory ignored = compiler.compile(
                        "SELECT id FROM lp_interval WHERE ts < '2020-01-02T00:00:00.000000Z'", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    compiler.clear();
                } catch (Throwable th) {
                    actual.close();
                    throw th;
                }
            }
            try (actual) {
                assertPlan(
                        actual,
                        "Interval forward scan",
                        false,
                        "PageFrame > Row forward scan > Interval forward scan on: lp_interval"
                );
                assertRowsOnly(actual, rows("ts\tid", false, "3 4"));
            }
        });
    }

    @Test
    public void testIntegerBoundsAreEpochsInColumnUnit() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                execute("CREATE TABLE lp_epoch (id INT, ts " + (isNanos ? "TIMESTAMP_NS" : "TIMESTAMP") + ") TIMESTAMP(ts) PARTITION BY DAY");
                execute("INSERT INTO lp_epoch VALUES (1, 0), (2, 1_000_000), (3, 2_000_000), (4, 3_000_000), (5, 3_000_000_000)");
                assertEpochScan("ts = 0 OR ts = 1000000 OR ts = 2000000", isNanos, points(isNanos, 0, 1_000_000, 2_000_000), 1, 2, 3);
                assertEpochScan("ts = 3000000000 OR 1000000 = ts", isNanos, points(isNanos, 1_000_000, 3_000_000_000L), 2, 5);
                assertEpochScan("ts = 2::BYTE OR ts = 2000000::LONG OR ts = 7::SHORT", isNanos, points(isNanos, 2, 7, 2_000_000), 3);
                assertEpochScan("ts IN (0, 3000000000)", isNanos, points(isNanos, 0, 3_000_000_000L), 1, 5);
                assertEpochScan("ts IN (1000000) OR ts = 0", isNanos, points(isNanos, 0, 1_000_000), 1, 2);
                assertEpochScan("ts = NULL OR ts = 1000000", isNanos, "(\"MIN\",\"MIN\")," + points(isNanos, 1_000_000), 2);
                assertEpochScan("ts > 2000000", isNanos, "(\"" + epoch(2_000_001, isNanos) + "\",\"MAX\")", 4, 5);
                assertEpochScan("ts BETWEEN 1000000 AND 3000000000", isNanos,
                        "(\"" + epoch(1_000_000, isNanos) + "\",\"" + epoch(3_000_000_000L, isNanos) + "\")", 2, 3, 4, 5);
                execute("DROP TABLE lp_epoch");
            });
        }
    }

    @Test
    public void testMicrosecondAndNanosecondComparisonDirections() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                final String point = nanos ? "'2020-01-02T00:00:00.000000001Z'" : "'2020-01-02T00:00:00.000001Z'";
                final String[] operators = {"=", "<", "<=", ">", ">="};
                final String[] forward = {"4", "1 2 3", "1 2 3 4", "5", "4 5"};
                final String[] reversed = {"4", "5", "4 5", "1 2 3", "1 2 3 4"};
                for (int i = 0; i < operators.length; i++) {
                    assertInterval(
                            "SELECT ts,id FROM lp_interval WHERE ts " + operators[i] + ' ' + point,
                            "Interval forward scan",
                            false,
                            "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                            rows("ts\tid", nanos, forward[i])
                    );
                    assertInterval(
                            "SELECT ts,id FROM lp_interval WHERE " + point + ' ' + operators[i] + " ts",
                            "Interval forward scan",
                            false,
                            "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                            rows("ts\tid", nanos, reversed[i])
                    );
                }
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testNanosecondDomainEdgesDoNotWrap() throws Exception {
        assertMemoryLeak(() -> {
            createRows(true);
            final String max = "'2262-04-11T23:47:16.854775807Z'";
            final String min = "'1677-09-21T00:12:43.145224193Z'";
            // These are legal predicate bounds but lie outside the range of
            // values accepted for stored designated nanosecond timestamps.
            assertInterval("SELECT ts,id FROM lp_interval WHERE ts > " + max, "Empty table", false, "Empty table", "ts\tid\n");
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE " + max + " < ts",
                    "Empty table",
                    false,
                    "Empty table",
                    "ts\tid\n"
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts >= " + max,
                    "Interval forward scan",
                    false,
                    "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    "ts\tid\n"
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE " + max + " >= ts",
                    "Interval forward scan",
                    false,
                    "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-01T23:59:59.999999998Z	1
                            2020-01-01T23:59:59.999999999Z	2
                            2020-01-02T00:00:00.000000000Z	3
                            2020-01-02T00:00:00.000000001Z	4
                            2020-01-03T00:00:00.000000000Z	5
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts < " + min,
                    "Interval forward scan",
                    false,
                    "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    "ts\tid\n"
            );
        });
    }

    @Test
    public void testNegatedRuntimeTimestampComparisonsSubtractTheirRange() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final long point = isNanos ? 1_577_923_200_000_000_001L : 1_577_923_200_000_001L;
                final String[] predicates = {
                        "NOT (ts < $1)", "NOT ($1 > ts)", "NOT (ts <= $1)", "NOT ($1 >= ts)",
                        "NOT (ts > $1)", "NOT ($1 < ts)", "NOT (ts >= $1)", "NOT ($1 <= ts)"
                };
                final long[] bounds = {point, point - 2, Numbers.LONG_NULL, Long.MAX_VALUE};
                final String[][] expectedIds = {
                        {"4 5", "2 3 4 5", "1 2 3 4 5", ""},
                        {"5", "3 4 5", "1 2 3 4 5", ""},
                        {"1 2 3 4", "1 2", "1 2 3 4 5", "1 2 3 4 5"},
                        {"1 2 3", "1", "1 2 3 4 5", "1 2 3 4 5"}
                };
                for (int i = 0; i < predicates.length; i++) {
                    setTimestamp(0, point, isNanos);
                    try (RecordCursorFactory factory = select("SELECT ts,id FROM lp_interval WHERE " + predicates[i])) {
                        assertPlan(factory, "Interval forward scan", false, "PageFrame > Row forward scan > Interval forward scan on: lp_interval");
                        for (int b = 0; b < bounds.length; b++) {
                            setTimestamp(0, bounds[b], isNanos);
                            assertRowsOnly(factory, rows("ts\tid", isNanos, expectedIds[i / 2][b]));
                        }
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testNegatedTimestampComparisonsKeepStaticIntervals() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final String fraction = isNanos ? "00000000" : "00000";
                final String point = "'2020-01-02T00:00:00." + fraction + "1Z'";
                final String[] predicates = {
                        "NOT (ts < " + point + ")", "NOT (" + point + " > ts)", "NOT (ts <= " + point + ")", "NOT (" + point + " >= ts)",
                        "NOT (ts > " + point + ")", "NOT (" + point + " < ts)", "NOT (ts >= " + point + ")", "NOT (" + point + " <= ts)"
                };
                final String[] intervals = {
                        "(\"2020-01-02T00:00:00." + fraction + "1Z\",\"MAX\")",
                        "(\"2020-01-02T00:00:00." + fraction + "2Z\",\"MAX\")",
                        "(\"MIN\",\"2020-01-02T00:00:00." + fraction + "1Z\")",
                        "(\"MIN\",\"2020-01-02T00:00:00." + fraction + "0Z\")"
                };
                final String[] expectedIds = {"4 5", "5", "1 2 3 4", "1 2 3"};
                for (int i = 0; i < predicates.length; i++) {
                    assertQuery("SELECT ts,id FROM lp_interval WHERE " + predicates[i])
                            .noLeakCheck()
                            .timestamp("ts")
                            .withPlan("""
                                    PageFrame
                                        Row forward scan
                                        Interval forward scan on: lp_interval
                                          intervals: [""" + intervals[i / 2] + "]\n")
                            .returns(rows("ts\tid", isNanos, expectedIds[i / 2]));
                }
                assertQuery("SELECT ts,id FROM lp_interval WHERE NOT (ts < NULL)")
                        .noLeakCheck()
                        .timestamp("ts")
                        .returns(rows("ts\tid", isNanos, "1 2 3 4 5"));
                assertQuery("SELECT ts,id FROM lp_interval WHERE NOT (dateadd('h', 1, ts) < '2020-01-02T01:00:00." + fraction + "1Z')")
                        .noLeakCheck()
                        .timestamp("ts")
                        .withPlanContaining("Interval forward scan on: lp_interval")
                        .withPlanNotContaining("Filter")
                        .returns(rows("ts\tid", isNanos, "4 5"));
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testNegatedNullBoundOverProjectedOffsetKeepsEveryRow() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String shifted = "SELECT id FROM (SELECT id, dateadd('h', -1, ts) shifted FROM lp_interval) WHERE ";
            assertQuery(shifted + "NOT (shifted > NULL::timestamp)")
                    .noLeakCheck()
                    .returns("id\n1\n2\n3\n4\n5\n");
            assertQuery(shifted + "shifted > NULL::timestamp")
                    .noLeakCheck()
                    .returns("id\n");
        });
    }

    @Test
    public void testOrAndMixedPrecisionColumnsRemainResidual() throws Exception {
        assertMemoryLeak(() -> {
            createRows(true);
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts < '2020-01-02T00:00:00.000000000Z'"
                            + " OR ts > '2020-01-02T00:00:00.000000001Z'",
                    "Frame forward scan",
                    true,
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-01T23:59:59.999999998Z	1
                            2020-01-01T23:59:59.999999999Z	2
                            2020-01-03T00:00:00.000000000Z	5
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts > other",
                    "Frame forward scan",
                    true,
                    "SelectedRecord > Async Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000001Z	4
                            2020-01-03T00:00:00.000000000Z	5
                            """
            );
        });
    }

    @Test
    public void testPartialConjunctionKeepsNonTimestampFilter() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts >= '2020-01-02T00:00:00.000000Z' AND id < 5",
                    "Interval forward scan",
                    true,
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000Z	3
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE id > 2 AND ts <= '2020-01-02T00:00:00.000001Z'",
                    "Interval forward scan",
                    true,
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000Z	3
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
        });
    }

    @Test
    public void testRuntimeDateComparisonsUseIntervalScan() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final ObjList<String> operators = new ObjList<>("=", "<", "<=", ">", ">=", "!=", "<>");
                final long[] bounds = {1_577_923_200_000L, 1_577_923_199_999L, 1_578_009_600_000L, Numbers.LONG_NULL};
                for (int i = 0; i < operators.size(); i++) {
                    final String predicate = "ts" + operators.getQuick(i) + "$1";
                    bindVariableService.clear();
                    bindVariableService.setDate(0, bounds[0]);
                    try (RecordCursorFactory factory = select("SELECT ts,id FROM lp_interval WHERE " + predicate)) {
                        assertPlan(factory, "Interval forward scan", false, "PageFrame > Row forward scan > Interval forward scan on: lp_interval");
                        for (long bound : bounds) {
                            bindVariableService.setDate(0, bound);
                            try (RecordCursorFactory literal = select("SELECT ts,id FROM lp_interval WHERE "
                                    + predicate.replace("$1", (bound == Numbers.LONG_NULL ? "NULL" : Long.toString(bound)) + "::DATE"))) {
                                assertRowsOnly(factory, printFactory(literal));
                            }
                        }
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testRuntimeTextComparisonsUseIntervalScan() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final ObjList<String> operators = new ObjList<>("=", "<", "<=", ">", ">=", "!=", "<>");
                final ObjList<String> bounds = new ObjList<>("2020-01-02T00:00:00.000001Z", "2020-01-02T00:00:00.000000001Z",
                        "2020-01-02", "2020-01-01T23:59:59.999999999Z", "2020-01-01T23:59:59.9999995Z", null);
                for (int i = 0; i < operators.size(); i++) {
                    for (int reversed = 0; reversed < 2; reversed++) {
                        final String predicate = reversed == 1 ? "$1" + operators.getQuick(i) + "ts" : "ts" + operators.getQuick(i) + "$1";
                        for (int isVarchar = 0; isVarchar < 2; isVarchar++) {
                            bindVariableService.clear();
                            setText(isVarchar == 1, bounds.getQuick(0));
                            try (RecordCursorFactory factory = select("SELECT ts,id FROM lp_interval WHERE " + predicate)) {
                                assertPlan(factory, "Interval forward scan", false, "PageFrame > Row forward scan > Interval forward scan on: lp_interval");
                                for (int b = 0; b < bounds.size(); b++) {
                                    final String bound = bounds.getQuick(b);
                                    setText(isVarchar == 1, bound);
                                    try (RecordCursorFactory literal = select("SELECT ts,id FROM lp_interval WHERE "
                                            + predicate.replace("$1", bound == null ? "NULL" : "'" + bound + "'"))) {
                                        assertRowsOnly(factory, printFactory(literal));
                                    }
                                }
                            }
                        }
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testRuntimeTimestampComparisonsRebindAfterCompilerClose() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final long point = isNanos ? 1_577_923_200_000_000_001L : 1_577_923_200_000_001L;
                final ObjList<String> operators = new ObjList<>("=", "<", "<=", ">", ">=", "!=", "<>");
                final long[] bounds = {point, point - 2, Numbers.LONG_NULL, Long.MAX_VALUE, point};
                final String[][] expectedIds = {
                        {"4", "2", "", "", "4"},
                        {"4", "2", "", "", "4"},
                        {"1 2 3", "1", "", "1 2 3 4 5", "1 2 3"},
                        {"5", "3 4 5", "", "", "5"},
                        {"1 2 3 4", "1 2", "", "1 2 3 4 5", "1 2 3 4"},
                        {"4 5", "2 3 4 5", "", "", "4 5"},
                        {"5", "3 4 5", "", "", "5"},
                        {"1 2 3", "1", "", "1 2 3 4 5", "1 2 3"},
                        {"4 5", "2 3 4 5", "", "", "4 5"},
                        {"1 2 3 4", "1 2", "", "1 2 3 4 5", "1 2 3 4"},
                        {"1 2 3 5", "1 3 4 5", "1 2 3 4 5", "1 2 3 4 5", "1 2 3 5"},
                        {"1 2 3 5", "1 3 4 5", "1 2 3 4 5", "1 2 3 4 5", "1 2 3 5"},
                        {"1 2 3 5", "1 3 4 5", "1 2 3 4 5", "1 2 3 4 5", "1 2 3 5"},
                        {"1 2 3 5", "1 3 4 5", "1 2 3 4 5", "1 2 3 4 5", "1 2 3 5"}
                };
                for (int i = 0; i < operators.size(); i++) {
                    final String operator = operators.getQuick(i);
                    for (int reversed = 0; reversed < 2; reversed++) {
                        setTimestamp(0, point, isNanos);
                        final String sql = "SELECT ts,id FROM lp_interval WHERE "
                                + (reversed == 1 ? "$1" + operator + "ts" : "ts" + operator + "$1");
                        try (RecordCursorFactory factory = select(sql)) {
                            assertPlan(
                                    factory,
                                    "Interval forward scan",
                                    false,
                                    "PageFrame > Row forward scan > Interval forward scan on: lp_interval"
                            );
                            for (int b = 0; b < bounds.length; b++) {
                                setTimestamp(0, bounds[b], isNanos);
                                assertRowsOnly(factory, rows("ts\tid", isNanos, expectedIds[i * 2 + reversed][b]));
                            }
                        }
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testRuntimeTimestampUnionsPreserveNullIdentityAndConjunctOrder() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final long point = isNanos ? 1_577_923_200_000_000_001L : 1_577_923_200_000_001L;
                final String literal = isNanos ? "'2020-01-02T00:00:00.000000001Z'" : "'2020-01-02T00:00:00.000001Z'";
                final ObjList<String> predicates = new ObjList<>("ts=$1 OR $2=ts", "ts=$1 OR ts=" + literal,
                        "ts=" + literal + " OR ts=$1", "ts>$1 AND ts<=$2", "ts>=$1 AND (ts=$1 OR ts=$2)",
                        "(ts=$1 OR ts=$2) AND ts>=$1", "ts!=$1 AND ts!=$2", "ts>=$2 AND ts!=$1",
                        "ts!=$1 AND (ts=$1 OR ts=$2)", "(ts=$1 OR ts=$2) AND ts!=$1",
                        "NOT(ts=$1)", "NOT(ts=$1 OR ts=$2)");
                final String[][] expectedIds = {
                        {"4 2", "4", "2", "", "4 2"},
                        {"4 2", "4", "4 2", "4", "4 2"},
                        {"4 2", "4", "4 2", "4", "4 2"},
                        {"4 3", "", "", "", "4 3"},
                        {"4 2", "", "2", "", "4 2"},
                        {"4 2", "", "2", "", "4 2"},
                        {"5 3 1", "5 3 2 1", "5 4 3 1", "5 4 3 2 1", "5 3 1"},
                        {"5 4", "5 4", "", "", "5 4"},
                        {"4", "4", "", "", "4"},
                        {"4", "4", "", "", "4"},
                        {"5 4 3 1", "5 4 3 2 1", "5 4 3 1", "5 4 3 2 1", "5 4 3 1"},
                        {"5 3 1", "5 3 2 1", "5 4 3 1", "5 4 3 2 1", "5 3 1"}
                };
                final String[] shapes = {"PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "Async JIT Filter workers: 1 > PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "Async JIT Filter workers: 1 > PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval", "PageFrame > Row backward scan > Interval backward scan on: lp_interval"};
                for (int i = 0; i < predicates.size(); i++) {
                    final String predicate = predicates.getQuick(i);
                    setTimestamp(0, point - 2, isNanos);
                    setTimestamp(1, point, isNanos);
                    final String sql = "SELECT id,ts FROM lp_interval WHERE " + predicate + " ORDER BY ts DESC";
                    try (RecordCursorFactory factory = select(sql)) {
                        assertPlan(factory, "Interval backward scan", predicate.startsWith("("), shapes[i]);
                        for (int state = 0; state < 5; state++) {
                            setTimestamp(0, state == 1 || state == 3 ? Numbers.LONG_NULL : point - 2, isNanos);
                            setTimestamp(1, state == 2 || state == 3 ? Numbers.LONG_NULL : point, isNanos);
                            assertRowsOnly(factory, rows("id\tts", isNanos, expectedIds[i][state]));
                        }
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testRuntimeTimestampFunctionsKeepNamedParameterLinksAndResidual() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final long point = isNanos ? 1_577_923_200_000_000_001L : 1_577_923_200_000_001L;
                final long second = isNanos ? 1_000_000_000L : 1_000_000L;
                if (isNanos) {
                    bindVariableService.setTimestampNano("bound", point - second);
                } else {
                    bindVariableService.setTimestamp("bound", point - second);
                }
                final String sql = "SELECT ts,id FROM lp_interval WHERE ts>=dateadd('s',1,:bound) AND id IN(1,2,4,5)";
                final long[] bounds = {point - second, point - second - 2, Numbers.LONG_NULL, point - second};
                final String[] expectedIds = {"4 5", "2 4 5", "", "4 5"};
                try (RecordCursorFactory factory = select(sql)) {
                    assertPlan(
                            factory,
                            "Interval forward scan",
                            true,
                            "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval"
                    );
                    for (int b = 0; b < bounds.length; b++) {
                        if (isNanos) {
                            bindVariableService.setTimestampNano("bound", bounds[b]);
                        } else {
                            bindVariableService.setTimestamp("bound", bounds[b]);
                        }
                        assertRowsOnly(factory, rows("ts\tid", isNanos, expectedIds[b]));
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testTimestampEqualityDisjunctionUsesIntervalUnion() throws Exception {
        for (boolean isNanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final String first = isNanos ? "'2020-01-01T23:59:59.999999999Z'" : "'2020-01-01T23:59:59.999999Z'";
                final String second = isNanos ? "'2020-01-02T00:00:00.000000001Z'" : "'2020-01-02T00:00:00.000001Z'";
                final String predicate = "ts=" + first + " OR " + second + "=ts OR ts=" + first;
                assertInterval(
                        "SELECT ts,id FROM lp_interval WHERE " + predicate,
                        "Interval forward scan",
                        false,
                        "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                        rows("ts\tid", isNanos, "2 4")
                );
                assertInterval(
                        "SELECT id,ts FROM lp_interval WHERE " + predicate + " ORDER BY ts DESC",
                        "Interval backward scan",
                        false,
                        "PageFrame > Row backward scan > Interval backward scan on: lp_interval",
                        rows("id\tts", isNanos, "4 2")
                );
                assertResidual(
                        "SELECT id FROM lp_interval WHERE (" + predicate + ") AND id IN(1,2,4)",
                        "filter: id in [1,2,4]",
                        "SelectedRecord > Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                        "id\n2\n4\n"
                );
                assertInterval(
                        "SELECT id FROM lp_interval WHERE ts=null OR ts=" + second,
                        "Interval forward scan",
                        false,
                        "SelectedRecord > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                        "id\n4\n"
                );
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testTimestampUnionPreservesConjunctOrderAndDeclinesMixedBranches() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String first = "'2020-01-01T23:59:59.999999Z'";
            final String second = "'2020-01-02T00:00:00.000001Z'";
            final String union = "(ts=" + first + " OR ts=" + second + ')';
            assertResidual(
                    "SELECT ts,id FROM lp_interval WHERE " + union + " AND ts>" + first,
                    "filter: (2020-01-01T23:59:59.999999Z=ts or 2020-01-02T00:00:00.000001Z=ts)",
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertResidual(
                    "SELECT ts,id FROM lp_interval WHERE " + union
                            + " AND (ts=" + second + " OR ts='2020-01-03T00:00:00.000000Z')",
                    "filter: (2020-01-01T23:59:59.999999Z=ts or 2020-01-02T00:00:00.000001Z=ts)",
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts>" + first + " AND " + union,
                    "Interval forward scan",
                    false,
                    "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE " + union + " AND (id>0 AND ts>" + first + ')',
                    "Interval forward scan",
                    true,
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts=" + first + " OR id=4",
                    "Frame forward scan",
                    true,
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-01T23:59:59.999999Z	2
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts=" + first + " OR (ts=" + second + " AND id=4)",
                    "Frame forward scan",
                    true,
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-01T23:59:59.999999Z	2
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertInterval(
                    "SELECT ts,id FROM lp_interval WHERE ts=" + first + " OR other=" + second,
                    "Frame forward scan",
                    true,
                    "SelectedRecord > Async JIT Filter workers: 1 > PageFrame > Row forward scan > Frame forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-01T23:59:59.999999Z	2
                            """
            );
        });
    }

    @Test
    public void testPartialConjunctionPlanContainsOnlyUnextractedConditions() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertResidual(
                    "SELECT ts,id FROM lp_interval WHERE ts >= '2020-01-02T00:00:00.000000Z' AND id < 5",
                    "filter: id<5",
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000Z	3
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertResidual(
                    "SELECT ts,id FROM lp_interval WHERE id > 2 AND ts <= '2020-01-02T00:00:00.000001Z'",
                    "filter: 2<id",
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000Z	3
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
            assertResidual(
                    "SELECT ts,id FROM lp_interval WHERE ts >= '2020-01-02T00:00:00.000000Z'"
                            + " AND id > 2 AND ts < '2020-01-03T00:00:00.000000Z' AND id IN (3,4,6)",
                    "filter: (2<id and id in [3,4,6])",
                    "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                    """
                            ts	id
                            2020-01-02T00:00:00.000000Z	3
                            2020-01-02T00:00:00.000001Z	4
                            """
            );
        });
    }

    @Test
    public void testExtractedConjunctionKeepsIndependentNativeResidualAfterResetAndFailure() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            final String sql = "SELECT ts,id FROM lp_interval WHERE ts >= '2020-01-02T00:00:00.000000Z'"
                    + " AND id IN (3,4,6) AND ts < '2020-01-03T00:00:00.000000Z'";
            final RecordCursorFactory actual;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                actual = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT id FROM lp_interval WHERE lp_missing_fn(id)>0 AND id IN (1,2,3)", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        Assert.fail("unknown function must fail after binding its sibling");
                    } catch (SqlException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "unknown function name");
                    }
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT id FROM lp_interval WHERE ts < '2020-01-02T00:00:00.000000Z'", sqlExecutionContext
                    ).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                } catch (Throwable th) {
                    actual.close();
                    throw th;
                }
            }
            try (actual) {
                assertPlan(
                        actual,
                        "Interval forward scan",
                        true,
                        "Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval"
                );
                TestUtils.assertEquals("filter: id in [3,4,6]", filterLine(actual));
                assertRowsOnly(actual, rows("ts\tid", false, "3 4"));
            }
        });
    }

    @Test
    public void testTypedTimestampExclusionsRemovePointsInsteadOfLiteralIntervals() throws Exception {
        for (int precision = 0; precision < 2; precision++) {
            final boolean isNanos = precision == 1;
            assertMemoryLeak(() -> {
                createRows(isNanos);
                final String type = isNanos ? "TIMESTAMP_NS" : "TIMESTAMP";
                final String point = "CAST('2020-01-02' AS " + type + ')';
                assertInterval(
                        "SELECT id,ts FROM lp_interval WHERE ts!=" + point,
                        "Interval forward scan",
                        false,
                        "PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                        rows("id\tts", isNanos, "1 2 4 5")
                );
                assertInterval(
                        "SELECT id,ts FROM lp_interval WHERE " + point + "<>ts ORDER BY ts DESC",
                        "Interval backward scan",
                        false,
                        "PageFrame > Row backward scan > Interval backward scan on: lp_interval",
                        rows("id\tts", isNanos, "5 4 2 1")
                );
                assertInterval(
                        "SELECT id FROM lp_interval WHERE ts!=CAST(null AS " + type + ')',
                        "Interval forward scan",
                        false,
                        "SelectedRecord > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                        "id\n1\n2\n3\n4\n5\n"
                );
                assertInterval(
                        "SELECT id FROM lp_interval WHERE ts!=" + point + " AND id<5",
                        "Interval forward scan",
                        true,
                        "SelectedRecord > Async JIT Filter workers: 1 > PageFrame > Row forward scan > Interval forward scan on: lp_interval",
                        "id\n1\n2\n4\n"
                );
                execute("DROP TABLE lp_interval");
            });
        }
    }

    @Test
    public void testUnionTreatsEmptyDisjunctAsIdentity() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p_m (k INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO p_m VALUES
                    (1, '2024-01-01T10:00:00.000000Z'),
                    (2, '2024-01-01T11:00:00.000000Z'),
                    (3, '2024-01-01T12:00:00.000000Z'),
                    (4, '2024-01-02T10:00:00.000000Z')
                    """);
            final String finer = "ts IN '2024-01-01T10:00:00.0000001Z'";
            final String hour = "[(\"2024-01-01T12:00:00.000000Z\",\"2024-01-01T12:59:59.999999Z\")]";
            final String point = "[(\"2024-01-01T12:00:00.000000Z\",\"2024-01-01T12:00:00.000000Z\")]";
            final String day = "[(\"2024-01-01T00:00:00.000000Z\",\"2024-01-01T23:59:59.999999Z\")]";
            final String row3 = "k\tts\n3\t2024-01-01T12:00:00.000000Z\n";
            final String day1 = """
                    k\tts
                    1\t2024-01-01T10:00:00.000000Z
                    2\t2024-01-01T11:00:00.000000Z
                    3\t2024-01-01T12:00:00.000000Z
                    """;
            assertUnion("p_m", finer + " OR ts IN '2024-01-01T12'", hour, row3);
            assertUnion("p_m", "ts IN '2024-01-01T12' OR " + finer, hour, row3);
            assertUnion("p_m", finer + " OR ts = '2024-01-01T12:00:00Z'", point, row3);
            assertUnion("p_m", "ts IN '2024-01-01' AND (" + finer + " OR ts IN '2024-01-01T12')", hour, row3);
            assertUnion("p_m", finer + " OR ts IN '2024-01-01T11:00:00.0000001Z' OR ts IN '2024-01-01T12'", hour, row3);
            assertUnion("p_m", "ts IN '2024-01-01T11' OR " + finer + " OR ts IN '2024-01-01T12'",
                    "[(\"2024-01-01T11:00:00.000000Z\",\"2024-01-01T11:59:59.999999Z\")," + hour.substring(1), """
                            k\tts
                            2\t2024-01-01T11:00:00.000000Z
                            3\t2024-01-01T12:00:00.000000Z
                            """);
            assertUnion("p_m", finer + " OR ts = (SELECT ts FROM p_m WHERE k = 3)", point, row3);
            assertUnion("p_m", "ts IN '2024-01-02#Mon' OR ts IN '2024-01-01'", day, day1);
            assertUnion("p_m", "ts IN '2024-01-01' OR ts IN '2024-01-02#Mon'", day, day1);
            assertUnion("p_m", "ts IN ('2024-01-01T12:00:00.000000Z', '2024-01-02#Mon')", point, row3);
            bindVariableService.clear();
            bindVariableService.setTimestamp(0, 1_704_110_400_000_000L);
            assertUnion("p_m", finer + " OR ts = $1", point, row3);
            bindVariableService.setTimestamp(0, Numbers.LONG_NULL);
            assertUnion("p_m", finer + " OR ts = $1", "[]", "k\tts\n");
            bindVariableService.clear();
            assertEmptyUnion("p_m", finer + " OR ts IN '2024-01-02#Mon'");
            assertEmptyUnion("p_m", "ts IN '2024-01-02#Mon' OR " + finer + " OR ts IN '2024-01-01T11:00:00.0000001Z'");
            assertEmptyUnion("p_m", "ts IN ('2024-01-02#Mon', '2024-01-03#Mon')");

            execute("CREATE TABLE p_n (k INT, ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO p_n VALUES
                    (1, '2024-01-01T10:00:00.000000000Z'),
                    (2, '2024-01-01T11:00:00.000000000Z'),
                    (3, '2024-01-01T12:00:00.000000000Z'),
                    (4, '2024-01-02T10:00:00.000000000Z')
                    """);
            final String nanoDay = "[(\"2024-01-01T00:00:00.000000000Z\",\"2024-01-01T23:59:59.999999999Z\")]";
            final String nanoPoint = "[(\"2024-01-01T12:00:00.000000000Z\",\"2024-01-01T12:00:00.000000000Z\")]";
            final String nanoRow3 = "k\tts\n3\t2024-01-01T12:00:00.000000000Z\n";
            assertUnion("p_n", "ts IN '2024-01-02#Mon' OR ts IN '2024-01-01'", nanoDay, """
                    k\tts
                    1\t2024-01-01T10:00:00.000000000Z
                    2\t2024-01-01T11:00:00.000000000Z
                    3\t2024-01-01T12:00:00.000000000Z
                    """);
            assertUnion("p_n", "ts IN ('2024-01-01T12:00:00.000000000Z', '2024-01-02#Mon')", nanoPoint, nanoRow3);
            assertUnion("p_n", "ts IN '2024-01-02#Mon' OR ts = (SELECT ts FROM p_n WHERE k = 3)", nanoPoint, nanoRow3);
            bindVariableService.setTimestampNano(0, 1_704_110_400_000_000_000L);
            assertUnion("p_n", "ts IN '2024-01-02#Mon' OR ts = $1", nanoPoint, nanoRow3);
            bindVariableService.clear();
            assertEmptyUnion("p_n", "ts IN '2024-01-02#Mon' OR ts IN '2024-01-03#Mon'");
        });
    }

    @Test
    public void testUndefinedParametersCompareAtNanosecondPrecision() throws Exception {
        assertMemoryLeak(() -> {
            createRows(true);
            execute("CREATE TABLE lp_nanos (id INT, ts TIMESTAMP, ns TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY");
            execute("""
                    INSERT INTO lp_nanos VALUES
                    (1, '2020-01-01', '2020-01-02T00:00:00.000000001Z'),
                    (2, '2020-01-02', '2020-01-02T00:00:00.000000002Z')
                    """);
            final ObjList<String> queries = new ObjList<>(
                    "SELECT ts,id FROM lp_interval WHERE ts = $1",
                    "SELECT ts,id FROM lp_interval WHERE ts > $1",
                    "SELECT ts,id FROM lp_interval WHERE $1 > ts",
                    "SELECT ts,id FROM lp_interval WHERE ts BETWEEN $1 AND $1",
                    "SELECT ts,id FROM lp_interval WHERE ts != $1 AND id < 5",
                    "SELECT ts,id FROM lp_interval WHERE ts = $1 OR id = -1",
                    "SELECT ts,id FROM (SELECT * FROM lp_interval LIMIT 5) WHERE ts = $1",
                    "SELECT id, ts = $1 eq FROM lp_interval",
                    "SELECT id FROM lp_nanos WHERE ns = $1"
            );
            final ObjList<String> values = new ObjList<>("2020-01-02T00:00:00.000000001Z", "1577923200000000001");
            final ObjList<String> literals = new ObjList<>("'2020-01-02T00:00:00.000000001Z'", "1577923200000000001");
            for (int i = 0; i < queries.size(); i++) {
                final String sql = queries.getQuick(i);
                bindVariableService.clear();
                try (RecordCursorFactory factory = select(sql)) {
                    Assert.assertEquals(sql, ColumnType.TIMESTAMP_NANO, bindVariableService.getFunction(0).getType());
                    for (int v = 0; v < values.size(); v++) {
                        bindVariableService.setStr(0, values.getQuick(v));
                        try (RecordCursorFactory literal = select(sql.replace("$1", literals.getQuick(v)))) {
                            assertRowsOnly(factory, printFactory(literal));
                        }
                    }
                }
            }
            bindVariableService.clear();
        });
    }

    private static String epoch(long value, boolean isNanos) {
        final long unit = isNanos ? 1_000_000_000L : 1_000_000L;
        final long seconds = value / unit;
        return String.format(isNanos ? "1970-01-01T%02d:%02d:%02d.%09dZ" : "1970-01-01T%02d:%02d:%02d.%06dZ",
                seconds / 3600, seconds / 60 % 60, seconds % 60, value % unit);
    }

    private static String points(boolean isNanos, long... values) {
        final StringBuilder sb = new StringBuilder();
        for (long value : values) {
            if (!sb.isEmpty()) {
                sb.append(',');
            }
            final String text = epoch(value, isNanos);
            sb.append("(\"").append(text).append("\",\"").append(text).append("\")");
        }
        return sb.toString();
    }

    private static String rows(String header, boolean isNanos, String ids) {
        final StringBuilder sink = new StringBuilder(header).append('\n');
        if (!ids.isEmpty()) {
            final String[] columns = header.split("\t");
            for (String id : ids.split(" ")) {
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        sink.append('\t');
                    }
                    sink.append(columns[i].equals("ts") ? (isNanos ? NANO_TIMES : MICRO_TIMES)[Integer.parseInt(id) - 1] : id);
                }
                sink.append('\n');
            }
        }
        return sink.toString();
    }

    private void assertEpochScan(String predicate, boolean isNanos, String intervals, long... ids) throws Exception {
        final long[] values = {0, 1_000_000, 2_000_000, 3_000_000, 3_000_000_000L};
        final StringBuilder expected = new StringBuilder("id\tts\n");
        for (long id : ids) {
            expected.append(id).append('\t').append(epoch(values[(int) id - 1], isNanos)).append('\n');
        }
        assertQuery("SELECT * FROM lp_epoch WHERE " + predicate)
                .noLeakCheck()
                .timestamp("ts")
                .withPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on: lp_epoch
                              intervals: [""" + intervals + "]\n")
                .returns(expected);
    }

    private void assertEmptyUnion(String table, String predicate) throws Exception {
        assertQuery("SELECT * FROM " + table + " WHERE " + predicate)
                .noLeakCheck()
                .timestamp("ts")
                .withPlan("Empty table\n")
                .returns("k\tts\n");
    }

    private void assertUnion(String table, String predicate, String intervals, String expected) throws Exception {
        assertQuery("SELECT * FROM " + table + " WHERE " + predicate)
                .noLeakCheck()
                .timestamp("ts")
                .withPlan("""
                        PageFrame
                            Row forward scan
                            Interval forward scan on:\s""" + table + "\n      intervals: " + intervals + "\n")
                .returns(expected);
    }

    private void setText(boolean isVarchar, String value) throws SqlException {
        if (isVarchar) {
            bindVariableService.setVarchar(0, value == null ? null : new Utf8String(value));
        } else {
            bindVariableService.setStr(0, value);
        }
    }

    private void setTimestamp(int index, long value, boolean isNanos) throws SqlException {
        if (isNanos) {
            bindVariableService.setTimestampNano(index, value);
        } else {
            bindVariableService.setTimestamp(index, value);
        }
    }

    private void assertResidual(String sql, String filter, String shape, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertPlan(factory, "Interval forward scan", true, shape);
            TestUtils.assertEquals(filter, filterLine(factory));
            assertRowsOnly(factory, expected);
        }
    }

    private String filterLine(RecordCursorFactory factory) {
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        for (int i = 1, n = plan.getLineCount(); i <= n; i++) {
            final String line = plan.getLine(i).toString().trim();
            if (line.startsWith("filter:")) {
                return line;
            }
        }
        Assert.fail("expected a residual filter: " + plan.getSink());
        return null;
    }

    private void assertInterval(String sql, String scan, boolean residual, String shape, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertPlan(factory, scan, residual, shape);
            assertRowsOnly(factory, expected);
        }
    }

    private void assertPlan(RecordCursorFactory factory, String scan, boolean residual, String shape) {
        TestUtils.assertEquals(shape, PlanShape.of(factory, sqlExecutionContext));
        final TextPlanSink plan = new TextPlanSink();
        plan.of(factory, sqlExecutionContext);
        TestUtils.assertContains(plan.getSink(), scan);
        final String text = plan.getSink().toString();
        if ("Empty table".equals(scan)) {
            Assert.assertEquals(scan, text);
        }
        Assert.assertEquals(text, residual, text.contains("Filter"));
        if (scan.startsWith("Frame")) {
            Assert.assertFalse(text, text.contains("Interval"));
        }
    }

    private void createRows(boolean nanos) throws SqlException {
        execute("CREATE TABLE lp_interval (unused STRING, id INT, ts " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                + ", other TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        final String[] times = nanos ? NANO_TIMES : MICRO_TIMES;
        for (int i = 0; i < times.length; i++) {
            execute("INSERT INTO lp_interval VALUES ('unused'," + (i + 1) + ",'" + times[i]
                    + "','2020-01-02T00:00:00.000000Z')");
        }
    }
}
