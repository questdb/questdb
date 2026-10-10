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
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class MonotonicIntervalTest extends AbstractCairoTest {
    private static final String[] MICRO_TIMES = {"2020-01-01T00:00:00.000000Z", "2020-01-01T23:59:59.999999Z",
            "2020-01-02T00:00:00.000000Z", "2020-01-02T00:00:00.000001Z", "2020-01-31T12:00:00.000000Z",
            "2020-02-29T12:00:00.000000Z", "2020-03-29T00:30:00.000000Z", "2020-03-29T01:30:00.000000Z",
            "2021-06-01T00:00:00.000000Z"};
    private static final String[] NANO_TIMES = {"2020-01-01T00:00:00.000000000Z", "2020-01-01T23:59:59.999999999Z",
            "2020-01-02T00:00:00.000000000Z", "2020-01-02T00:00:00.000000001Z", "2020-01-31T12:00:00.000000000Z",
            "2020-02-29T12:00:00.000000000Z", "2020-03-29T00:30:00.000000000Z", "2020-03-29T01:30:00.000000000Z",
            "2021-06-01T00:00:00.000000000Z"};

    @Test
    public void testConstantChainsInBothComparisonDirections() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                final String type = nanos ? "TIMESTAMP_NS" : "TIMESTAMP";
                final String point = "'2020-01-02T00:00:00.000000000Z'";
                final String[] expressions = {"date_trunc('day',ts)", "timestamp_floor('d',ts)",
                        "timestamp_ceil('d',ts)", "dateadd('d',1,ts)", "ts+1L", "ts-1L",
                        "dateadd('h',-1,timestamp_floor('d',ts))", "CAST(ts AS " + type + ")"};
                final String[] operators = {"=", "<", "<=", ">", ">="};
                final String[][] forward = nanos ? new String[][]{
                        {"I:3 4", "I:1 2", "I:1 2 3 4", "I:5 6 7 8 9", "I:3 4 5 6 7 8 9"},
                        {"I:3 4", "I:1 2", "I:1 2 3 4", "I:5 6 7 8 9", "I:3 4 5 6 7 8 9"},
                        {"I:1 2", "SF:", "SF:1 2", "I:3 4 5 6 7 8 9", "I:1 2 3 4 5 6 7 8 9"},
                        {"I:1", "SF:", "SF:1", "I:2 3 4 5 6 7 8 9", "I:1 2 3 4 5 6 7 8 9"},
                        {"I:2", "SF:1", "SF:1 2", "I:3 4 5 6 7 8 9", "I:2 3 4 5 6 7 8 9"},
                        {"I:4", "I:1 2 3", "I:1 2 3 4", "I:5 6 7 8 9", "I:4 5 6 7 8 9"},
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"I:3", "I:1 2", "I:1 2 3", "I:4 5 6 7 8 9", "I:3 4 5 6 7 8 9"}
                } : new String[][]{
                        {"I:3 4", "I:1 2", "I:1 2 3 4", "I:5 6 7 8 9", "I:3 4 5 6 7 8 9"},
                        {"I:3 4", "I:1 2", "I:1 2 3 4", "I:5 6 7 8 9", "I:3 4 5 6 7 8 9"},
                        {"I:1 2", "SF:", "SF:1 2", "I:3 4 5 6 7 8 9", "I:1 2 3 4 5 6 7 8 9"},
                        {"I:1", "I:", "I:1", "I:2 3 4 5 6 7 8 9", "I:1 2 3 4 5 6 7 8 9"},
                        {"I:2", "I:1", "I:1 2", "I:3 4 5 6 7 8 9", "I:2 3 4 5 6 7 8 9"},
                        {"I:4", "I:1 2 3", "I:1 2 3 4", "I:5 6 7 8 9", "I:4 5 6 7 8 9"},
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"I:3", "I:1 2", "I:1 2 3", "I:4 5 6 7 8 9", "I:3 4 5 6 7 8 9"}
                };
                final String[][] reversed = nanos ? new String[][]{
                        {"I:4 3", "I:9 8 7 6 5", "I:9 8 7 6 5 4 3", "I:2 1", "I:4 3 2 1"},
                        {"I:4 3", "I:9 8 7 6 5", "I:9 8 7 6 5 4 3", "I:2 1", "I:4 3 2 1"},
                        {"I:2 1", "I:9 8 7 6 5 4 3", "I:9 8 7 6 5 4 3 2 1", "SF:", "SF:2 1"},
                        {"I:1", "I:9 8 7 6 5 4 3 2", "I:9 8 7 6 5 4 3 2 1", "SF:", "SF:1"},
                        {"I:2", "I:9 8 7 6 5 4 3", "I:9 8 7 6 5 4 3 2", "SF:1", "SF:2 1"},
                        {"I:4", "I:9 8 7 6 5", "I:9 8 7 6 5 4", "I:3 2 1", "I:4 3 2 1"},
                        {"EO:", "I:9 8 7 6 5", "I:9 8 7 6 5", "I:4 3 2 1", "I:4 3 2 1"},
                        {"I:3", "I:9 8 7 6 5 4", "I:9 8 7 6 5 4 3", "I:2 1", "I:3 2 1"}
                } : new String[][]{
                        {"I:4 3", "I:9 8 7 6 5", "I:9 8 7 6 5 4 3", "I:2 1", "I:4 3 2 1"},
                        {"I:4 3", "I:9 8 7 6 5", "I:9 8 7 6 5 4 3", "I:2 1", "I:4 3 2 1"},
                        {"I:2 1", "I:9 8 7 6 5 4 3", "I:9 8 7 6 5 4 3 2 1", "SF:", "SF:2 1"},
                        {"I:1", "I:9 8 7 6 5 4 3 2", "I:9 8 7 6 5 4 3 2 1", "I:", "I:1"},
                        {"I:2", "I:9 8 7 6 5 4 3", "I:9 8 7 6 5 4 3 2", "I:1", "I:2 1"},
                        {"I:4", "I:9 8 7 6 5", "I:9 8 7 6 5 4", "I:3 2 1", "I:4 3 2 1"},
                        {"EO:", "I:9 8 7 6 5", "I:9 8 7 6 5", "I:4 3 2 1", "I:4 3 2 1"},
                        {"I:3", "I:9 8 7 6 5 4", "I:9 8 7 6 5 4 3", "I:2 1", "I:3 2 1"}
                };
                for (int e = 0; e < expressions.length; e++) {
                    for (int o = 0; o < operators.length; o++) {
                        assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE " + expressions[e] + operators[o] + point, nanos, forward[e][o]);
                        assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE " + point + operators[o] + expressions[e] + " ORDER BY ts DESC",
                                nanos, reversed[e][o]);
                    }
                }
                execute("DROP TABLE lp_monotonic");
            });
        }
    }

    @Test
    public void testConstantBoundPrecisionAndIntegerOutput() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                final String[] expressions = {"timestamp_floor('d',ts)", "date_trunc('microsecond',ts)",
                        "CAST(ts AS TIMESTAMP)", "CAST(ts AS TIMESTAMP_NS)"};
                final String[] operators = {"=", "<", "<=", ">", ">="};
                final String[][] textBound = nanos ? new String[][]{
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"E:", "IF:1 2 3 4", "IF:1 2 3 4", "IF:5 6 7 8 9", "IF:5 6 7 8 9"},
                        {"I:4", "I:1 2 3", "I:1 2 3 4", "I:5 6 7 8 9", "I:4 5 6 7 8 9"}
                } : new String[][]{
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"E:", "I:1 2 3", "I:1 2 3", "I:4 5 6 7 8 9", "I:4 5 6 7 8 9"},
                        {"E:", "I:1 2 3", "I:1 2 3", "I:4 5 6 7 8 9", "I:4 5 6 7 8 9"},
                        {"E:", "I:1 2 3", "I:1 2 3", "IF:4 5 6 7 8 9", "IF:4 5 6 7 8 9"}
                };
                final String[][] typedBound = nanos ? new String[][]{
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"E:", "IF:1 2 3 4", "IF:1 2 3 4", "IF:5 6 7 8 9", "IF:5 6 7 8 9"},
                        {"I:4", "I:1 2 3", "I:1 2 3 4", "I:5 6 7 8 9", "I:4 5 6 7 8 9"}
                } : new String[][]{
                        {"E:", "I:1 2 3 4", "I:1 2 3 4", "I:5 6 7 8 9", "I:5 6 7 8 9"},
                        {"E:", "I:1 2 3", "I:1 2 3", "I:4 5 6 7 8 9", "I:4 5 6 7 8 9"},
                        {"E:", "I:1 2 3", "I:1 2 3", "I:4 5 6 7 8 9", "I:4 5 6 7 8 9"},
                        {"E:", "I:1 2 3", "I:1 2 3", "IF:4 5 6 7 8 9", "IF:4 5 6 7 8 9"}
                };
                for (int e = 0; e < expressions.length; e++) {
                    for (int o = 0; o < operators.length; o++) {
                        assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE " + expressions[e] + operators[o]
                                + "'2020-01-02T00:00:00.000000001Z'", nanos, textBound[e][o]);
                        assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE " + expressions[e] + operators[o]
                                + "CAST('2020-01-02T00:00:00.000000001Z' AS TIMESTAMP_NS)", nanos, typedBound[e][o]);
                    }
                }
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE year(ts)=2020", nanos, "I:1 2 3 4 5 6 7 8");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE 2021<=year(ts)", nanos, "I:9");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE year(ts)>=CAST(2020 AS DOUBLE)", nanos, "SF:1 2 3 4 5 6 7 8 9");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE CAST(ts AS LONG)>=1577923200000000L", nanos,
                        nanos ? "I:1 2 3 4 5 6 7 8 9" : "I:3 4 5 6 7 8 9");
                execute("DROP TABLE lp_monotonic");
            });
        }
    }

    @Test
    public void testConjunctionResidualAndNonNativeOperands() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            assertScan("SELECT id FROM lp_monotonic WHERE timestamp_floor('d',ts)>='2020-01-02' AND id IN (2,3,4,5)", "IF", "id\n3\n4\n5\n");
            assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE id>1 AND dateadd('d',1,ts)<'2020-02-01'", false, "IF:2 3 4");
            assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE timestamp_floor('d',other)>'2020-01-02'", false, "SF:");
            assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE abs(year(ts))>2020", false, "SF:9");
            assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE timestamp_floor('d',ts)=timestamp_floor('d',other)", false, "SF:3 4");
            assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE timestamp_floor('d',ts)!='2020-01-02'", false, "SF:1 2 5 6 7 8 9");
            assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE timestamp_floor('d',ts)='2020-01-02' OR id=5", false, "SF:3 4 5");
            assertMonotonic("SELECT ts,id FROM (SELECT ts,id FROM lp_monotonic LIMIT 2) WHERE timestamp_floor('d',ts)>'2020-01-02'", false, "SF:");
        });
    }

    @Test
    public void testCalendarAndTimezoneSupersetsKeepTheirFilters() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE dateadd('M',1,ts)='2020-02-29T12:00:00Z'", nanos, "IF:5");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE dateadd('y',1,ts)>='2021-02-28T12:00:00Z'", nanos, "IF:6 7 8 9");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE to_timezone(ts,'Europe/London')>='2020-03-29T01:00:00Z'", nanos, "IF:8 9");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE to_timezone(ts,'Europe/London')='2020-03-29T02:30:00Z'", nanos, "IF:8");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE to_utc(ts,'Europe/London')<'2020-03-29T02:00:00Z'", nanos, "IF:1 2 3 4 5 6 7 8");
                assertMonotonic("SELECT ts,id FROM lp_monotonic WHERE to_timezone(ts,'+02:00')>='2020-01-02T01:00:00Z'", nanos, "I:2 3 4 5 6 7 8 9");
                execute("DROP TABLE lp_monotonic");
            });
        }
    }

    @Test
    public void testRuntimeBoundsRebindAfterCompilerReuseAndClose() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                final long point = nanos ? 1_577_923_200_000_000_000L : 1_577_923_200_000_000L;
                final String[] operators = {"=", "<", "<=", ">", ">="};
                final long[] values = {point, point - 1, Numbers.LONG_NULL, Long.MAX_VALUE, point};
                final String[][] expected = {
                        {"IF:", "IF:", "IF:", "IF:", "IF:"},
                        {"IF:", "IF:", "IF:", "IF:", "IF:"},
                        {"IF:1 2 3 4", "IF:1 2 3 4", "IF:", "IF:1 2 3 4 5 6 7 8 9", "IF:1 2 3 4"},
                        {"IF:5 6 7 8 9", "IF:5 6 7 8 9", "IF:", "IF:", "IF:5 6 7 8 9"},
                        {"IF:1 2 3 4", "IF:1 2 3 4", "IF:", "IF:1 2 3 4 5 6 7 8 9", "IF:1 2 3 4"},
                        {"IF:5 6 7 8 9", "IF:5 6 7 8 9", "IF:", "IF:", "IF:5 6 7 8 9"},
                        {"IF:5 6 7 8 9", "IF:5 6 7 8 9", "IF:", "IF:", "IF:5 6 7 8 9"},
                        {"IF:1 2 3 4", "IF:1 2 3 4", "IF:", "IF:1 2 3 4 5 6 7 8 9", "IF:1 2 3 4"},
                        {"IF:5 6 7 8 9", "IF:5 6 7 8 9", "IF:", "IF:", "IF:5 6 7 8 9"},
                        {"IF:1 2 3 4", "IF:1 2 3 4", "IF:", "IF:1 2 3 4 5 6 7 8 9", "IF:1 2 3 4"}
                };
                for (int o = 0; o < operators.length; o++) {
                    for (int reversed = 0; reversed < 2; reversed++) {
                        setTimestamp(point, nanos);
                        final String operand = "dateadd('h',-1,timestamp_floor('d',ts))";
                        final String query = "SELECT ts,id FROM lp_monotonic WHERE "
                                + (reversed == 1 ? "$1" + operators[o] + operand : operand + operators[o] + "$1");
                        try (RecordCursorFactory factory = select(query)) {
                            for (int v = 0; v < values.length; v++) {
                                setTimestamp(values[v], nanos);
                                assertMonotonic(factory, nanos, expected[o * 2 + reversed][v]);
                            }
                        }
                    }
                }
                bindVariableService.clear();
                bindVariableService.setInt(0, 2020);
                final String[] yearExpected = {"IF:1 2 3 4 5 6 7 8 9", "IF:9", "IF:", "IF:", "IF:1 2 3 4 5 6 7 8 9"};
                try (RecordCursorFactory factory = select("SELECT ts,id FROM lp_monotonic WHERE year(ts)>=$1")) {
                    final int[] years = {2020, 2021, 999999, Numbers.INT_NULL, 2020};
                    for (int v = 0; v < years.length; v++) {
                        bindVariableService.setInt(0, years[v]);
                        assertMonotonic(factory, nanos, yearExpected[v]);
                    }
                }
                bindVariableService.clear();
                execute("DROP TABLE lp_monotonic");
            });
        }
    }

    @Test
    public void testRuntimePrecisionConversionAndCalendarBounds() throws Exception {
        for (boolean nanos : new boolean[]{false, true}) {
            assertMemoryLeak(() -> {
                createRows(nanos);
                final long micro = 1_577_923_200_000_000L;
                final String[] expressions = {"timestamp_floor('d',ts)", "dateadd('M',1,ts)", "to_timezone(ts,'Europe/London')"};
                final String[] expected = nanos ? new String[]{"IF:3 4 5 6 7 8 9", "IF:1 2 3 4 5 6 7 8 9", "IF:3 4 5 6 7 8 9"} : new String[]{"IF:5 6 7 8 9", "IF:1 2 3 4 5 6 7 8 9", "IF:4 5 6 7 8 9"};
                for (int e = 0; e < expressions.length; e++) {
                    setTimestamp(nanos ? micro : micro * 1000 + 1, !nanos);
                    try (RecordCursorFactory factory = select("SELECT ts,id FROM lp_monotonic WHERE " + expressions[e] + ">=$1")) {
                        assertMonotonic(factory, nanos, expected[e]);
                        setTimestamp(Numbers.LONG_NULL, !nanos);
                        assertMonotonic(factory, nanos, "IF:");
                    }
                    bindVariableService.clear();
                }
                execute("DROP TABLE lp_monotonic");
            });
        }
    }

    @Test
    public void testOverflowDeclinesOrKeepsRequiredResidual() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_monotonic(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO lp_monotonic VALUES (1,'2020-01-01'),(2,'9999-12-31T23:59:59.999999Z')");
            assertScan("SELECT ts,id FROM lp_monotonic WHERE ts+9000000000000000000L<'2022-01-01'", "SF",
                    "ts\tid\n9999-12-31T23:59:59.999999Z\t2\n");
            assertScan("SELECT ts,id FROM lp_monotonic WHERE ts+5000000000000000000L+5000000000000000000L<'2022-01-01'", "SF",
                    "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n9999-12-31T23:59:59.999999Z\t2\n");
            bindVariableService.setTimestamp(0, -300_000_000_000_000_000L);
            assertScan("SELECT ts,id FROM lp_monotonic WHERE ts+9000000000000000000L>=$1", "IF", "ts\tid\n2020-01-01T00:00:00.000000Z\t1\n");
            bindVariableService.clear();
            execute("DROP TABLE lp_monotonic");
            execute("CREATE TABLE lp_monotonic(id INT,ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY YEAR");
            execute("INSERT INTO lp_monotonic VALUES (1,'2020-01-01'),(2,'2261-12-31T23:00:00.000000000Z')");
            assertScan("SELECT ts,id FROM lp_monotonic WHERE ts+17280000000000000L<'2022-01-01'", "SF",
                    "ts\tid\n2020-01-01T00:00:00.000000000Z\t1\n2261-12-31T23:00:00.000000000Z\t2\n");
            assertScan("SELECT ts,id FROM lp_monotonic WHERE ts+17280000000000000L>='2022-01-01'", "I", "ts\tid\n");
            assertScan("SELECT ts,id FROM lp_monotonic WHERE timestamp_floor('d',ts)>'2262-04-11T23:47:16.854775807Z'", "E", "ts\tid\n");
        });
    }


    @Test
    public void testNegativeEpochTextAndTypedBoundsCompareExactly() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lp_monotonic(id INT,ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO lp_monotonic VALUES (1,'1970-01-01T00:00:00.000000Z')");
            final String textBound = "SELECT id FROM lp_monotonic WHERE ts-1L<'1969-12-31T23:59:59.999999999Z'";
            final String typedBound = "SELECT id FROM lp_monotonic WHERE ts-1L<CAST('1969-12-31T23:59:59.999999999Z' AS TIMESTAMP_NS)";
            assertScan(textBound, "I", "id\n1\n");
            assertScan(typedBound, "I", "id\n1\n");
            for (String type : new String[]{"STRING", "VARCHAR"}) {
                assertScan("SELECT id FROM lp_monotonic WHERE ts-1L<CAST('1969-12-31T23:59:59.999999999Z' AS " + type + ')', "I", "id\n1\n");
            }
        });
    }


    @Test
    public void testRetainedFactoriesSurviveADeclinedAndAnEmptyCompilation() throws Exception {
        assertMemoryLeak(() -> {
            createRows(false);
            bindVariableService.setTimestamp(0, 1_577_923_200_000_000L);
            final String query = "SELECT ts,id FROM lp_monotonic WHERE timestamp_floor('d',ts)>=$1 AND id IN(3,4,5)";
            final RecordCursorFactory actual;
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                actual = compiler.compile(query, sqlExecutionContext).getRecordCursorFactory();
                try {
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT id FROM lp_monotonic WHERE abs(year(ts))>2020", sqlExecutionContext).getRecordCursorFactory()) {
                        Assert.assertNotNull(ignored);
                    }
                    try (RecordCursorFactory ignored = compiler.compile(
                            "SELECT id FROM lp_monotonic WHERE timestamp_floor('d',ts)>=$1 AND ts<'1970-01-01'",
                            sqlExecutionContext).getRecordCursorFactory()) {
                        compiler.clear();
                    }
                } catch (Throwable th) {
                    actual.close();
                    throw th;
                }
            }
            try (actual) {
                assertMonotonic(actual, false, "IF:3 4 5");
                bindVariableService.setTimestamp(0, 0);
                assertMonotonic(actual, false, "IF:3 4 5");
            }
            bindVariableService.clear();
        });
    }


    private static String rows(boolean nanos, String ids) {
        final StringBuilder sink = new StringBuilder("ts\tid\n");
        if (!ids.isEmpty()) {
            for (String id : ids.split(" ")) {
                sink.append((nanos ? NANO_TIMES : MICRO_TIMES)[Integer.parseInt(id) - 1]).append('\t').append(id).append('\n');
            }
        }
        return sink.toString();
    }

    private void assertMonotonic(String sql, boolean nanos, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertMonotonic(factory, nanos, expected);
        }
    }

    // expected is "<scan>:<ids>", see assertScan() for the scan codes
    private void assertMonotonic(RecordCursorFactory factory, boolean nanos, String expected) throws Exception {
        final int colon = expected.indexOf(':');
        assertScan(factory, expected.substring(0, colon), rows(nanos, expected.substring(colon + 1)));
    }

    private void assertScan(String sql, String scan, String expected) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            assertScan(factory, scan, expected);
        }
    }

    // scan: E = empty table, I = interval scan, S = full scan; suffix F = residual filter, O = sort
    private void assertScan(RecordCursorFactory factory, String scan, String expected) throws Exception {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, sqlExecutionContext);
        final String plan = sink.getSink().toString();
        Assert.assertEquals(plan, scan.charAt(0) == 'E', plan.contains("Empty table"));
        Assert.assertEquals(plan, scan.charAt(0) == 'I', plan.contains("Interval "));
        Assert.assertEquals(plan, scan.indexOf('F') > 0, plan.contains("Filter"));
        Assert.assertEquals(plan, scan.indexOf('O') > 0, plan.contains("sort"));
        assertFactory(factory).withContext(sqlExecutionContext).inferTimestamp().inferRandomAccess().sizeMayVary().returns(expected);
    }

    private void createRows(boolean nanos) throws SqlException {
        execute("CREATE TABLE lp_monotonic(id INT,ts " + (nanos ? "TIMESTAMP_NS" : "TIMESTAMP")
                + ",other TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
        final String[] times = nanos ? NANO_TIMES : MICRO_TIMES;
        for (int i = 0; i < times.length; i++) {
            execute("INSERT INTO lp_monotonic VALUES (" + (i + 1) + ",'" + times[i] + "','2020-01-02')");
        }
    }

    private void setTimestamp(long value, boolean nanos) throws SqlException {
        if (nanos) {
            bindVariableService.setTimestampNano(0, value);
        } else {
            bindVariableService.setTimestamp(0, value);
        }
    }
}
