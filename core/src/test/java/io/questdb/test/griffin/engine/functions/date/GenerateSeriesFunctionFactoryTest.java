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

package io.questdb.test.griffin.engine.functions.date;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import io.questdb.test.tools.StationaryMicrosClock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GenerateSeriesFunctionFactoryTest extends BaseFunctionFactoryTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testMicrosClock = StationaryMicrosClock.INSTANCE;
        testNanoClock = StationaryNanosClock.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDivideByZero() throws Exception {
        assertQuery("generate_series(-5, 5, 0);")
                .fails(23, "step cannot be zero");
        assertQuery("generate_series(-5.0, 5.0, 0.0);")
                .fails(27, "step cannot be zero");
        assertQuery("generate_series((-5)::timestamp, (5)::timestamp, 0);")
                .fails(49, "step cannot be zero");
        assertQuery("generate_series('2000', '2020', '0d');")
                .fails(32, "step cannot be zero");
        assertQuery("generate_series('2000', '2020', '0');")
                .fails(32, "invalid period");
        assertQuery("generate_series('2000', '2020', '-0');")
                .fails(32, "invalid period");
    }

    @Test
    public void testDoubleBindVariables() throws Exception {
        bindVariableService.setDouble("d1", -5.2);
        bindVariableService.setDouble("d2", 5.8);
        bindVariableService.setDouble("d3", 1.3);

        assertQuery("generate_series(:d1, :d2, :d3);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        -5.2
                        -3.9000000000000004
                        -2.6
                        -1.3
                        0.0
                        1.3
                        2.6
                        3.9000000000000004
                        5.2
                        """);

        bindVariableService.setDouble("d3", 2.8);

        assertQuery("generate_series(:d1, :d2, :d3);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        -5.2
                        -2.4000000000000004
                        0.39999999999999947
                        3.1999999999999993
                        """);
    }

    @Test
    public void testDoubleDefaultGeneration() throws Exception {
        assertQuery("generate_series(-5.0, 5.0);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        -5.0
                        -4.0
                        -3.0
                        -2.0
                        -1.0
                        0.0
                        1.0
                        2.0
                        3.0
                        4.0
                        5.0
                        """);
    }

    @Test
    public void testDoubleGenerationNulls() throws Exception {
        assertQuery("generate_series(null, 5.0, 3.0);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2.0, null, 3.0);")
                .fails(21, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2.0, 5.0, null);")
                .fails(26, "step argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null, null, 3.0);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2.0, null, null);")
                .fails(21, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null, 5.0, null);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null, null, null);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testDoubleNegativeGeneration() throws Exception {
        assertQuery("generate_series(-5.0, 5.0, -2.0);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        5.0
                        3.0
                        1.0
                        -1.0
                        -3.0
                        -5.0
                        """);
    }

    @Test
    public void testDoubleNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series(-5.0, 5.0, -2.0);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        5.0
                        3.0
                        1.0
                        -1.0
                        -3.0
                        -5.0
                        """);
    }

    @Test
    public void testDoubleNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series(-5, 5, -2.4);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        5.0
                        2.6
                        0.20000000000000018
                        -2.1999999999999997
                        -4.6
                        """);
    }

    @Test
    public void testDoubleNoRange() throws Exception {
        assertQuery("generate_series(2.0, 2.0, 3.0);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        2.0
                        """);
    }

    @Test
    public void testDoublePositiveGeneration() throws Exception {
        assertQuery("generate_series(-5.0, 5.0, 2.0);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        -5.0
                        -3.0
                        -1.0
                        1.0
                        3.0
                        5.0
                        """);
    }

    @Test
    public void testDoublePositiveGenerationReverse() throws Exception {
        assertQuery("generate_series(5.0, -5.0, 2.0);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        -5.0
                        -3.0
                        -1.0
                        1.0
                        3.0
                        5.0
                        """);
    }

    @Test
    public void testDoublePositiveGenerationUneven() throws Exception {
        assertQuery("generate_series(-5.0, 5.0, 2.4);")
                .noRandomAccess()
                .returns("""
                        generate_series
                        -5.0
                        -2.6
                        -0.20000000000000018
                        2.1999999999999997
                        4.6
                        """);
    }

    @Test
    public void testDoubleWithLimit() throws Exception {
        assertQuery("generate_series(1d, 10000000d) LIMIT 1")
                .noRandomAccess()
                .returns("""
                        generate_series
                        1.0
                        """);
        assertQuery("generate_series(1d, 10000000d) LIMIT -1")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        generate_series
                        1.0E7
                        """);
    }

    @Test
    public void testDoubleWithOrdering() throws Exception {
        assertQuery("generate_series(-5.0d, 5.0d, 1.0d) ORDER BY generate_series DESC;")
                .returns("""
                        generate_series
                        5.0
                        4.0
                        3.0
                        2.0
                        1.0
                        0.0
                        -1.0
                        -2.0
                        -3.0
                        -4.0
                        -5.0
                        """);
        assertQuery("generate_series(-5.0d, 5.0d, -1.0d)")
                .noRandomAccess()
                .returns("""
                        generate_series
                        5.0
                        4.0
                        3.0
                        2.0
                        1.0
                        0.0
                        -1.0
                        -2.0
                        -3.0
                        -4.0
                        -5.0
                        """);
    }

    @Test
    public void testLongBindVariables() throws Exception {
        bindVariableService.setLong("l1", -5);
        bindVariableService.setLong("l2", 5);
        bindVariableService.setLong("l3", 1);

        assertQuery("generate_series(:l1, :l2, :l3);")
                .expectSize()
                .returns("""
                        generate_series
                        -5
                        -4
                        -3
                        -2
                        -1
                        0
                        1
                        2
                        3
                        4
                        5
                        """);

        bindVariableService.setLong("l3", 3);

        assertQuery("generate_series(:l1, :l2, :l3);")
                .expectSize()
                .returns("""
                        generate_series
                        -5
                        -2
                        1
                        4
                        """);
    }

    @Test
    public void testLongDefaultGeneration() throws Exception {
        assertQuery("generate_series(-5, 5);")
                .expectSize()
                .returns("""
                        generate_series
                        -5
                        -4
                        -3
                        -2
                        -1
                        0
                        1
                        2
                        3
                        4
                        5
                        """);
    }

    @Test
    public void testLongGenerationNulls() throws Exception {
        assertQuery("generate_series(null, 5, 3);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2, null, 3);")
                .fails(19, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2, 5, null);")
                .fails(22, "step argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null, null, 3);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2, null, null);")
                .fails(19, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null, 5, null);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null, null, null);")
                .fails(16, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testLongNegativeGeneration() throws Exception {
        assertQuery("generate_series(-5, 5, -2);")
                .expectSize()
                .returns("""
                        generate_series
                        5
                        3
                        1
                        -1
                        -3
                        -5
                        """);
    }

    @Test
    public void testLongNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series(5, -5, -2);")
                .expectSize()
                .returns("""
                        generate_series
                        5
                        3
                        1
                        -1
                        -3
                        -5
                        """);
    }

    @Test
    public void testLongNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series(-5, 5, -3);")
                .expectSize()
                .returns("""
                        generate_series
                        5
                        2
                        -1
                        -4
                        """);
    }

    @Test
    public void testLongNoRange() throws Exception {
        assertQuery("generate_series(2, 2, 3);")
                .expectSize()
                .returns("""
                        generate_series
                        2
                        """);
    }

    @Test
    public void testLongPositiveGeneration() throws Exception {
        assertQuery("generate_series(-5, 5, 2);")
                .expectSize()
                .returns("""
                        generate_series
                        -5
                        -3
                        -1
                        1
                        3
                        5
                        """);
    }

    @Test
    public void testLongPositiveGenerationReverse() throws Exception {
        assertQuery("generate_series(5, -5, 2);")
                .expectSize()
                .returns("""
                        generate_series
                        -5
                        -3
                        -1
                        1
                        3
                        5
                        """);
    }

    @Test
    public void testLongPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series(-5, 5, 3);")
                .expectSize()
                .returns("""
                        generate_series
                        -5
                        -2
                        1
                        4
                        """);
    }

    @Test
    public void testLongWithLimit() throws Exception {
        assertQuery("generate_series(1L, 10000000L) LIMIT 1")
                .expectSize()
                .returns("""
                        generate_series
                        1
                        """);
        assertQuery("generate_series(1L, 10000000L) LIMIT -1")
                .expectSize()
                .returns("""
                        generate_series
                        10000000
                        """);
    }

    @Test
    public void testLongWithOrdering() throws Exception {
        assertQuery("generate_series(-5, 5, 1) ORDER BY generate_series DESC;")
                .expectSize()
                .returns("""
                        generate_series
                        5
                        4
                        3
                        2
                        1
                        0
                        -1
                        -2
                        -3
                        -4
                        -5
                        """);
        assertQuery("generate_series(-5, 5, -1)")
                .expectSize()
                .returns("""
                        generate_series
                        5
                        4
                        3
                        2
                        1
                        0
                        -1
                        -2
                        -3
                        -4
                        -5
                        """);
    }

    @Test
    public void testPeriodErrorPosition() throws Exception {
        assertQuery("generate_series(1, 100, '11');")
                .fails(24, "invalid period [period=11]");
    }

    @Test
    public void testPeriodWithoutStride() throws Exception {
        // this test is just checking for compilation errors, nothing more
        printSql("generate_series(-5, 5, 'y');");
        printSql("generate_series(-5, 5, 'M');");
        printSql("generate_series(-5, 5, 'w');");
        printSql("generate_series(-5, 5, 'd');");
        printSql("generate_series(-5, 5, 'h');");
        printSql("generate_series(-5, 5, 'm');");
        printSql("generate_series(-5, 5, 's');");
        printSql("generate_series(-5, 5, 'T');");
        printSql("generate_series(-5, 5, 'U');");
        printSql("generate_series(-5::timestamp_ns, 5::timestamp_ns, 'n');");
        printSql("generate_series(-5, 5, '-y');");
        printSql("generate_series(-5, 5, '-M');");
        printSql("generate_series(-5, 5, '-w');");
        printSql("generate_series(-5, 5, '-d');");
        printSql("generate_series(-5, 5, '-H');");
        printSql("generate_series(-5, 5, '-m');");
        printSql("generate_series(-5, 5, '-s');");
        printSql("generate_series(-5, 5, '-T');");
        printSql("generate_series(-5, 5, '-U');");
        printSql("generate_series(-5::timestamp_ns, 5::timestamp_ns, '-n');");
    }

    @Test
    public void testScanDirectionReflectsConstantOrBindVariableStep() throws Exception {
        assertMemoryLeak(() -> {
            // A constant step lets the factory report the scan order at plan time.
            // String (period) step -> GenerateSeriesTimestampStringRecordCursorFactory.
            assertScanDirection(RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                    "generate_series('2025-01-01', '2025-02-01', '1d')");
            assertScanDirection(RecordCursorFactory.SCAN_DIRECTION_BACKWARD,
                    "generate_series('2025-02-01', '2025-01-01', '-1d')");
            // Long (micros) step -> GenerateSeriesTimestampRecordCursorFactory.
            assertScanDirection(RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                    "generate_series('2025-01-01'::timestamp, '2025-02-01'::timestamp, 86_400_000_000)");
            assertScanDirection(RecordCursorFactory.SCAN_DIRECTION_BACKWARD,
                    "generate_series('2025-01-01'::timestamp, '2025-02-01'::timestamp, -86_400_000_000)");

            // A bind-variable step is only known at runtime, so the scan order cannot be
            // guaranteed at plan time: the factory must report SCAN_DIRECTION_OTHER instead of
            // reading the unbound step function and guessing a direction from it.
            bindVariableService.setStr("stepStr", "1d");
            assertScanDirection(RecordCursorFactory.SCAN_DIRECTION_OTHER,
                    "generate_series('2025-01-01', '2025-02-01', :stepStr)");

            bindVariableService.setLong("stepLong", 86_400_000_000L);
            assertScanDirection(RecordCursorFactory.SCAN_DIRECTION_OTHER,
                    "generate_series('2025-01-01'::timestamp, '2025-02-01'::timestamp, :stepLong)");
        });
    }

    @Test
    public void testTimestampLongBindVariables() throws Exception {
        bindVariableService.setTimestamp("t1", -5);
        bindVariableService.setTimestamp("t2", 5);
        bindVariableService.setTimestamp("t3", 1);

        assertQuery("generate_series(:t1, :t2, :t3);")
                .noLeakCheck()
                .returnsOnce("""
                generate_series
                1969-12-31T23:59:59.999995Z
                1969-12-31T23:59:59.999996Z
                1969-12-31T23:59:59.999997Z
                1969-12-31T23:59:59.999998Z
                1969-12-31T23:59:59.999999Z
                1970-01-01T00:00:00.000000Z
                1970-01-01T00:00:00.000001Z
                1970-01-01T00:00:00.000002Z
                1970-01-01T00:00:00.000003Z
                1970-01-01T00:00:00.000004Z
                1970-01-01T00:00:00.000005Z
                """);

        bindVariableService.setLong("t3", 3);

        assertQuery("generate_series(:t1, :t2, :t3);")
                .noLeakCheck()
                .returnsOnce("""
                generate_series
                1969-12-31T23:59:59.999995Z
                1969-12-31T23:59:59.999998Z
                1970-01-01T00:00:00.000001Z
                1970-01-01T00:00:00.000004Z
                """);
    }

    @Test
    public void testTimestampLongGenerationNulls() throws Exception {
        assertQuery("generate_series(null::timestamp, 5::timestamp, 3::timestamp);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp, null::timestamp, 3::timestamp);")
                .fails(34, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp, 5::timestamp, null::timestamp);")
                .fails(48, "step argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp, null::timestamp, 3::timestamp);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp, null::timestamp, null::timestamp);")
                .fails(34, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp, 5::timestamp, null::timestamp);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp, null::timestamp, null::timestamp);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");

        assertQuery("generate_series(null::timestamp_ns, 5::timestamp_ns, 3::timestamp_ns);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp_ns, null::timestamp_ns, 3::timestamp_ns);")
                .fails(37, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp_ns, 5::timestamp_ns, null::timestamp_ns);")
                .fails(54, "step argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp_ns, null::timestamp_ns, 3::timestamp_ns);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp_ns, null::timestamp_ns, null::timestamp_ns);")
                .fails(37, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp_ns, 5::timestamp_ns, null::timestamp_ns);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp_ns, null::timestamp_ns, null::timestamp_ns);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testTimestampLongNegativeGeneration() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, (-2)::timestamp);")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000001Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999995Z
                        """);

        assertQuery("generate_series((-5000)::timestamp_ns, 5::timestamp, (-2000)::timestamp_ns);")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000001000Z
                        1969-12-31T23:59:59.999999000Z
                        1969-12-31T23:59:59.999997000Z
                        1969-12-31T23:59:59.999995000Z
                        """);
    }

    @Test
    public void testTimestampLongNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series(5::timestamp, (-5)::timestamp, (-2)::timestamp);")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000001Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999995Z
                        """);

        assertQuery("generate_series(5000::timestamp_ns, (-5)::timestamp, (-2000)::timestamp_ns);")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000001000Z
                        1969-12-31T23:59:59.999999000Z
                        1969-12-31T23:59:59.999997000Z
                        1969-12-31T23:59:59.999995000Z
                        """);
    }

    @Test
    public void testTimestampLongNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, (-3)::timestamp);")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000002Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999996Z
                        """);
    }

    @Test
    public void testTimestampLongNoRange() throws Exception {
        assertQuery("generate_series(2::timestamp, 2::timestamp, 3::timestamp);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000002Z
                        """);

        assertQuery("generate_series(2000::timestamp_ns, 2::timestamp, 3::timestamp);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000002000Z
                        """);
    }

    @Test
    public void testTimestampLongPositiveGeneration() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, 2::timestamp);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1969-12-31T23:59:59.999995Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999999Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000005Z
                        """);

        assertQuery("generate_series((-5)::timestamp_ns, 5::timestamp_ns, 2::timestamp_ns);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1969-12-31T23:59:59.999999995Z
                        1969-12-31T23:59:59.999999997Z
                        1969-12-31T23:59:59.999999999Z
                        1970-01-01T00:00:00.000000001Z
                        1970-01-01T00:00:00.000000003Z
                        1970-01-01T00:00:00.000000005Z
                        """);
    }

    @Test
    public void testTimestampLongPositiveGenerationReverse() throws Exception {
        assertQuery("generate_series(5::timestamp, (-5)::timestamp, 2::timestamp);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1969-12-31T23:59:59.999995Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999999Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000005Z
                        """);

        assertQuery("generate_series(5000::timestamp_ns, (-5)::timestamp, 2000::timestamp_ns);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1969-12-31T23:59:59.999995000Z
                        1969-12-31T23:59:59.999997000Z
                        1969-12-31T23:59:59.999999000Z
                        1970-01-01T00:00:00.000001000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000005000Z
                        """);
    }

    @Test
    public void testTimestampLongPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, 3);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1969-12-31T23:59:59.999995Z
                        1969-12-31T23:59:59.999998Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000004Z
                        """);

        assertQuery("generate_series((-5)::timestamp, 5000::timestamp_ns, 3000);")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1969-12-31T23:59:59.999995000Z
                        1969-12-31T23:59:59.999998000Z
                        1970-01-01T00:00:00.000001000Z
                        1970-01-01T00:00:00.000004000Z
                        """);
    }

    @Test
    public void testTimestampNanoLongBindVariables() throws Exception {
        bindVariableService.setTimestampNano("t1", -5);
        bindVariableService.setTimestampNano("t2", 5);
        bindVariableService.setTimestampNano("t3", 1);

        assertQuery("generate_series(:t1, :t2, :t3);")
                .noLeakCheck()
                .returnsOnce("""
                generate_series
                1969-12-31T23:59:59.999999995Z
                1969-12-31T23:59:59.999999996Z
                1969-12-31T23:59:59.999999997Z
                1969-12-31T23:59:59.999999998Z
                1969-12-31T23:59:59.999999999Z
                1970-01-01T00:00:00.000000000Z
                1970-01-01T00:00:00.000000001Z
                1970-01-01T00:00:00.000000002Z
                1970-01-01T00:00:00.000000003Z
                1970-01-01T00:00:00.000000004Z
                1970-01-01T00:00:00.000000005Z
                """);

        bindVariableService.setLong("t3", 3);

        assertQuery("generate_series(:t1, :t2, :t3);")
                .noLeakCheck()
                .returnsOnce("""
                generate_series
                1969-12-31T23:59:59.999999995Z
                1969-12-31T23:59:59.999999998Z
                1970-01-01T00:00:00.000000001Z
                1970-01-01T00:00:00.000000004Z
                """);
    }

    @Test
    public void testTimestampStringBindVariables() throws Exception {
        bindVariableService.setStr("t1", "2025-01-01");
        bindVariableService.setStr("t2", "2025-02-01");
        bindVariableService.setStr("t3", "5d");

        assertQuery("generate_series(:t1, :t2, :t3);")
                .noLeakCheck()
                .returnsOnce("""
                generate_series
                2025-01-01T00:00:00.000000000Z
                2025-01-06T00:00:00.000000000Z
                2025-01-11T00:00:00.000000000Z
                2025-01-16T00:00:00.000000000Z
                2025-01-21T00:00:00.000000000Z
                2025-01-26T00:00:00.000000000Z
                2025-01-31T00:00:00.000000000Z
                """);

        bindVariableService.setStr("t3", "1w");

        assertQuery("generate_series(:t1, :t2, :t3);")
                .noLeakCheck()
                .returnsOnce("""
                generate_series
                2025-01-01T00:00:00.000000000Z
                2025-01-08T00:00:00.000000000Z
                2025-01-15T00:00:00.000000000Z
                2025-01-22T00:00:00.000000000Z
                2025-01-29T00:00:00.000000000Z
                """);
    }

    @Test
    public void testTimestampStringGenerationNulls() throws Exception {
        assertQuery("generate_series(null::timestamp, 5::timestamp, '3U');")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp, null::timestamp, '3U');")
                .fails(34, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp, 5::timestamp, null);")
                .fails(44, "step argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp, null::timestamp, '3U');")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp, null::timestamp, null);")
                .fails(34, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp, 5::timestamp, null);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp, null::timestamp, null);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");

        assertQuery("generate_series(null::timestamp_ns, 5::timestamp_ns, '3U');")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp_ns, null::timestamp_ns, '3U');")
                .fails(37, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp_ns, 5::timestamp_ns, null);")
                .fails(50, "step argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp_ns, null::timestamp_ns, '3U');")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(2::timestamp_ns, null::timestamp_ns, null);")
                .fails(37, "end argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp_ns, 5::timestamp_ns, null);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
        assertQuery("generate_series(null::timestamp_ns, null::timestamp_ns, null);")
                .fails(20, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testTimestampStringNegativeGeneration() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, '-2U');")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000001Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999995Z
                        """);
        assertQuery("generate_series((-5000)::timestamp_ns, 5::timestamp, '-2U');")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000001000Z
                        1969-12-31T23:59:59.999999000Z
                        1969-12-31T23:59:59.999997000Z
                        1969-12-31T23:59:59.999995000Z
                        """);
    }

    @Test
    public void testTimestampStringNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series(5::timestamp, (-5)::timestamp, '-2U');")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000001Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999995Z
                        """);

        assertQuery("generate_series(5000::timestamp_ns, (-5)::timestamp, '-2U');")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000001000Z
                        1969-12-31T23:59:59.999999000Z
                        1969-12-31T23:59:59.999997000Z
                        1969-12-31T23:59:59.999995000Z
                        """);
    }

    @Test
    public void testTimestampStringNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, '-3U');")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000002Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999996Z
                        """);

        assertQuery("generate_series((-5000)::timestamp_ns, 5000::timestamp_ns, '-3U');")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000002000Z
                        1969-12-31T23:59:59.999999000Z
                        1969-12-31T23:59:59.999996000Z
                        """);
    }

    @Test
    public void testTimestampStringNoRange() throws Exception {
        assertQuery("generate_series(2::timestamp, 2::timestamp, '1U');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000002Z
                        """);

        assertQuery("generate_series(2::timestamp_ns, 2::timestamp_ns, '1n');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000000002Z
                        """);
    }

    @Test
    public void testTimestampStringPositiveGeneration() throws Exception {
        assertQuery("generate_series('2025-01-01', '2025-02-01', '1d');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000Z
                        2025-01-02T00:00:00.000000Z
                        2025-01-03T00:00:00.000000Z
                        2025-01-04T00:00:00.000000Z
                        2025-01-05T00:00:00.000000Z
                        2025-01-06T00:00:00.000000Z
                        2025-01-07T00:00:00.000000Z
                        2025-01-08T00:00:00.000000Z
                        2025-01-09T00:00:00.000000Z
                        2025-01-10T00:00:00.000000Z
                        2025-01-11T00:00:00.000000Z
                        2025-01-12T00:00:00.000000Z
                        2025-01-13T00:00:00.000000Z
                        2025-01-14T00:00:00.000000Z
                        2025-01-15T00:00:00.000000Z
                        2025-01-16T00:00:00.000000Z
                        2025-01-17T00:00:00.000000Z
                        2025-01-18T00:00:00.000000Z
                        2025-01-19T00:00:00.000000Z
                        2025-01-20T00:00:00.000000Z
                        2025-01-21T00:00:00.000000Z
                        2025-01-22T00:00:00.000000Z
                        2025-01-23T00:00:00.000000Z
                        2025-01-24T00:00:00.000000Z
                        2025-01-25T00:00:00.000000Z
                        2025-01-26T00:00:00.000000Z
                        2025-01-27T00:00:00.000000Z
                        2025-01-28T00:00:00.000000Z
                        2025-01-29T00:00:00.000000Z
                        2025-01-30T00:00:00.000000Z
                        2025-01-31T00:00:00.000000Z
                        2025-02-01T00:00:00.000000Z
                        """);

        assertQuery("generate_series('2025-01-01'::timestamp_ns, '2025-02-01', '1d');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000000Z
                        2025-01-02T00:00:00.000000000Z
                        2025-01-03T00:00:00.000000000Z
                        2025-01-04T00:00:00.000000000Z
                        2025-01-05T00:00:00.000000000Z
                        2025-01-06T00:00:00.000000000Z
                        2025-01-07T00:00:00.000000000Z
                        2025-01-08T00:00:00.000000000Z
                        2025-01-09T00:00:00.000000000Z
                        2025-01-10T00:00:00.000000000Z
                        2025-01-11T00:00:00.000000000Z
                        2025-01-12T00:00:00.000000000Z
                        2025-01-13T00:00:00.000000000Z
                        2025-01-14T00:00:00.000000000Z
                        2025-01-15T00:00:00.000000000Z
                        2025-01-16T00:00:00.000000000Z
                        2025-01-17T00:00:00.000000000Z
                        2025-01-18T00:00:00.000000000Z
                        2025-01-19T00:00:00.000000000Z
                        2025-01-20T00:00:00.000000000Z
                        2025-01-21T00:00:00.000000000Z
                        2025-01-22T00:00:00.000000000Z
                        2025-01-23T00:00:00.000000000Z
                        2025-01-24T00:00:00.000000000Z
                        2025-01-25T00:00:00.000000000Z
                        2025-01-26T00:00:00.000000000Z
                        2025-01-27T00:00:00.000000000Z
                        2025-01-28T00:00:00.000000000Z
                        2025-01-29T00:00:00.000000000Z
                        2025-01-30T00:00:00.000000000Z
                        2025-01-31T00:00:00.000000000Z
                        2025-02-01T00:00:00.000000000Z
                        """);
    }

    @Test
    public void testTimestampStringPositiveGenerationReverse() throws Exception {
        assertQuery("generate_series('2025-02-01', '2025-01-01', '5d');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000Z
                        2025-01-06T00:00:00.000000Z
                        2025-01-11T00:00:00.000000Z
                        2025-01-16T00:00:00.000000Z
                        2025-01-21T00:00:00.000000Z
                        2025-01-26T00:00:00.000000Z
                        2025-01-31T00:00:00.000000Z
                        """);
        assertQuery("generate_series('2025-02-01', '2025-01-01'::timestamp_ns, '5d');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000000Z
                        2025-01-06T00:00:00.000000000Z
                        2025-01-11T00:00:00.000000000Z
                        2025-01-16T00:00:00.000000000Z
                        2025-01-21T00:00:00.000000000Z
                        2025-01-26T00:00:00.000000000Z
                        2025-01-31T00:00:00.000000000Z
                        """);
    }

    @Test
    public void testTimestampStringPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series('2025-01-01', '2025-02-01', '5d');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000Z
                        2025-01-06T00:00:00.000000Z
                        2025-01-11T00:00:00.000000Z
                        2025-01-16T00:00:00.000000Z
                        2025-01-21T00:00:00.000000Z
                        2025-01-26T00:00:00.000000Z
                        2025-01-31T00:00:00.000000Z
                        """);

        assertQuery("generate_series('2025-01-01'::timestamp_ns, '2025-02-01', '5d');")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        2025-01-01T00:00:00.000000000Z
                        2025-01-06T00:00:00.000000000Z
                        2025-01-11T00:00:00.000000000Z
                        2025-01-16T00:00:00.000000000Z
                        2025-01-21T00:00:00.000000000Z
                        2025-01-26T00:00:00.000000000Z
                        2025-01-31T00:00:00.000000000Z
                        """);
    }

    @Test
    public void testTimestampStringSizeExpected() throws Exception {
        assertQuery("generate_series('2020-01-01', '2025-02-01', '2y');")
                .timestamp("generate_series")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        generate_series
                        2020-01-01T00:00:00.000000Z
                        2022-01-01T00:00:00.000000Z
                        2024-01-01T00:00:00.000000Z
                        """);
        assertQuery("generate_series('2020-01-01', '2025-02-01', '8M');")
                .timestamp("generate_series")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        generate_series
                        2020-01-01T00:00:00.000000Z
                        2020-09-01T00:00:00.000000Z
                        2021-05-01T00:00:00.000000Z
                        2022-01-01T00:00:00.000000Z
                        2022-09-01T00:00:00.000000Z
                        2023-05-01T00:00:00.000000Z
                        2024-01-01T00:00:00.000000Z
                        2024-09-01T00:00:00.000000Z
                        """);

        assertQuery("generate_series('2020-01-01'::timestamp_ns, '2025-02-01', '2y');")
                .timestamp("generate_series")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        generate_series
                        2020-01-01T00:00:00.000000000Z
                        2022-01-01T00:00:00.000000000Z
                        2024-01-01T00:00:00.000000000Z
                        """);
        assertQuery("generate_series('2020-01-01', '2025-02-01'::timestamp_ns, '8M');")
                .timestamp("generate_series")
                .noRandomAccess()
                .expectSize()
                .returns("""
                        generate_series
                        2020-01-01T00:00:00.000000000Z
                        2020-09-01T00:00:00.000000000Z
                        2021-05-01T00:00:00.000000000Z
                        2022-01-01T00:00:00.000000000Z
                        2022-09-01T00:00:00.000000000Z
                        2023-05-01T00:00:00.000000000Z
                        2024-01-01T00:00:00.000000000Z
                        2024-09-01T00:00:00.000000000Z
                        """);
    }

    @Test
    public void testTimestampStringWithLimit() throws Exception {
        assertQuery("generate_series(1::timestamp, 10000000::timestamp, '1U') LIMIT 1")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000001Z
                        """);
        assertQuery("generate_series(1::timestamp, 10000000::timestamp, '1U') LIMIT -1")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:10.000000Z
                        """);

        assertQuery("generate_series(1::timestamp_ns, 10000000::timestamp_ns, '1n') LIMIT 1")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000000001Z
                        """);
        assertQuery("generate_series(1000::timestamp_ns, 10000000000::timestamp_ns, '1U') LIMIT -1")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:10.000000000Z
                        """);
    }

    @Test
    public void testTimestampStringWithOrdering() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, '1U') ORDER BY generate_series DESC;")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000004Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000000Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999998Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999996Z
                        1969-12-31T23:59:59.999995Z
                        """);

        assertQuery("generate_series((-5000)::timestamp_ns, 5::timestamp, '1U') ORDER BY generate_series DESC;")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005000Z
                        1970-01-01T00:00:00.000004000Z
                        1970-01-01T00:00:00.000003000Z
                        1970-01-01T00:00:00.000002000Z
                        1970-01-01T00:00:00.000001000Z
                        1970-01-01T00:00:00.000000000Z
                        1969-12-31T23:59:59.999999000Z
                        1969-12-31T23:59:59.999998000Z
                        1969-12-31T23:59:59.999997000Z
                        1969-12-31T23:59:59.999996000Z
                        1969-12-31T23:59:59.999995000Z
                        """);
    }

    @Test
    public void testTimestampWithLimit() throws Exception {
        assertQuery("generate_series(1::timestamp, 10000000::timestamp, 1) LIMIT 1")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000001Z
                        """);
        assertQuery("generate_series(1::timestamp, 10000000::timestamp, 1) LIMIT -1")
                .timestamp("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:10.000000Z
                        """);
    }

    @Test
    public void testTimestampWithOrdering() throws Exception {
        assertQuery("generate_series((-5)::timestamp, 5::timestamp, 1::timestamp) ORDER BY generate_series DESC;")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000004Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000000Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999998Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999996Z
                        1969-12-31T23:59:59.999995Z
                        """);
        assertQuery("generate_series((-5)::timestamp, (5)::timestamp, -1::timestamp)")
                .timestampDesc("generate_series")
                .expectSize()
                .returns("""
                        generate_series
                        1970-01-01T00:00:00.000005Z
                        1970-01-01T00:00:00.000004Z
                        1970-01-01T00:00:00.000003Z
                        1970-01-01T00:00:00.000002Z
                        1970-01-01T00:00:00.000001Z
                        1970-01-01T00:00:00.000000Z
                        1969-12-31T23:59:59.999999Z
                        1969-12-31T23:59:59.999998Z
                        1969-12-31T23:59:59.999997Z
                        1969-12-31T23:59:59.999996Z
                        1969-12-31T23:59:59.999995Z
                        """);
    }

    @Test
    public void testVariationsOfPeriods() throws Exception {
        assertSqlWithTypes("""
                        generate_series
                        2025-01-01T00:00:00.000000Z:TIMESTAMP
                        2025-01-12T00:00:00.000000Z:TIMESTAMP
                        2025-01-23T00:00:00.000000Z:TIMESTAMP
                        """,
                "generate_series(('2025-01-01')::timestamp, ('2025-02-01')::timestamp, '11d');");
        assertSqlWithTypes("""
                        generate_series
                        2025-02-01T00:00:00.000000Z:TIMESTAMP
                        2025-01-21T00:00:00.000000Z:TIMESTAMP
                        2025-01-10T00:00:00.000000Z:TIMESTAMP
                        """,
                "generate_series(('2025-01-01')::timestamp, ('2025-02-01')::timestamp, '-11d');");

        assertSqlWithTypes("""
                        generate_series
                        2025-01-01T00:00:00.000000000Z:TIMESTAMP_NS
                        2025-01-12T00:00:00.000000000Z:TIMESTAMP_NS
                        2025-01-23T00:00:00.000000000Z:TIMESTAMP_NS
                        """,
                "generate_series(('2025-01-01')::timestamp_ns, ('2025-02-01')::timestamp_ns, '11d');");
        assertSqlWithTypes("""
                        generate_series
                        2025-02-01T00:00:00.000000000Z:TIMESTAMP_NS
                        2025-01-21T00:00:00.000000000Z:TIMESTAMP_NS
                        2025-01-10T00:00:00.000000000Z:TIMESTAMP_NS
                        """,
                "generate_series(('2025-01-01')::timestamp_ns, ('2025-02-01')::timestamp_ns, '-11d');");
    }

    private void assertScanDirection(int expectedScanDirection, CharSequence query) throws SqlException {
        // getScanDirection() is plan-time metadata, so it must be read before the cursor is
        // opened: once opened, the string-step factory derives the direction from the cursor's
        // stride rather than from the (constant or bind-variable) step function.
        try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
            Assert.assertEquals(expectedScanDirection, factory.getScanDirection());
        }
    }
}
