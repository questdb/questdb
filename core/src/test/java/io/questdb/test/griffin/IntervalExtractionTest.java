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
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
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
