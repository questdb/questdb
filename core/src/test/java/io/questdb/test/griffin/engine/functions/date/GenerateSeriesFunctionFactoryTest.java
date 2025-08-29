/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.std.datetime.nanotime.StationaryNanosClock;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import io.questdb.test.tools.StationaryMicrosClock;
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
        assertException("generate_series(-5, 5, 0);", 23, "step cannot be zero");
        assertException("generate_series(-5.0, 5.0, 0.0);", 27, "step cannot be zero");
        assertException("generate_series((-5)::timestamp, (5)::timestamp, 0);", 49, "step cannot be zero");
        assertException("generate_series('2000', '2020', '0d');", 32, "step cannot be zero");
        assertException("generate_series('2000', '2020', '0');", 32, "invalid period");
        assertException("generate_series('2000', '2020', '-0');", 32, "invalid period");
    }

    @Test
    public void testDoubleBindVariables() throws Exception {
        bindVariableService.setDouble("d1", -5.2);
        bindVariableService.setDouble("d2", 5.8);
        bindVariableService.setDouble("d3", 1.3);

        assertQuery("generate_series\n" +
                        "-5.2\n" +
                        "-3.9000000000000004\n" +
                        "-2.6\n" +
                        "-1.3\n" +
                        "0.0\n" +
                        "1.3\n" +
                        "2.6\n" +
                        "3.9000000000000004\n" +
                        "5.2\n",
                "generate_series(:d1, :d2, :d3);",
                null,
                false,
                false);

        bindVariableService.setDouble("d3", 2.8);

        assertQuery("generate_series\n" +
                        "-5.2\n" +
                        "-2.4000000000000004\n" +
                        "0.39999999999999947\n" +
                        "3.1999999999999993\n",
                "generate_series(:d1, :d2, :d3);",
                null,
                false,
                false);
    }

    @Test
    public void testDoubleDefaultGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "-5.0\n" +
                        "-4.0\n" +
                        "-3.0\n" +
                        "-2.0\n" +
                        "-1.0\n" +
                        "0.0\n" +
                        "1.0\n" +
                        "2.0\n" +
                        "3.0\n" +
                        "4.0\n" +
                        "5.0\n",
                "generate_series(-5.0, 5.0);",
                null,
                false,
                false);
    }

    @Test
    public void testDoubleGenerationNulls() throws Exception {
        assertException("generate_series(null, 5.0, 3.0);", 16, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2.0, null, 3.0);", 21, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2.0, 5.0, null);", 26, "step argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null, null, 3.0);", 16, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2.0, null, null);", 21, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null, 5.0, null);", 16, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null, null, null);", 16, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testDoubleNegativeGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "1.0\n" +
                        "-1.0\n" +
                        "-3.0\n" +
                        "-5.0\n",
                "generate_series(-5.0, 5.0, -2.0);",
                null,
                false,
                false);
    }

    @Test
    public void testDoubleNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "5.0\n" +
                        "3.0\n" +
                        "1.0\n" +
                        "-1.0\n" +
                        "-3.0\n" +
                        "-5.0\n",
                "generate_series(-5.0, 5.0, -2.0);",
                null,
                false,
                false);
    }

    @Test
    public void testDoubleNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "5.0\n" +
                        "2.6\n" +
                        "0.20000000000000018\n" +
                        "-2.1999999999999997\n" +
                        "-4.6\n",
                "generate_series(-5, 5, -2.4);",
                null,
                false,
                false);
    }

    @Test
    public void testDoubleNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "2.0\n",
                "generate_series(2.0, 2.0, 3.0);",
                null,
                false,
                false);
    }

    @Test
    public void testDoublePositiveGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "-5.0\n" +
                        "-3.0\n" +
                        "-1.0\n" +
                        "1.0\n" +
                        "3.0\n" +
                        "5.0\n",
                "generate_series(-5.0, 5.0, 2.0);",
                null,
                false,
                false);
    }

    @Test
    public void testDoublePositiveGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "-5.0\n" +
                        "-3.0\n" +
                        "-1.0\n" +
                        "1.0\n" +
                        "3.0\n" +
                        "5.0\n",
                "generate_series(5.0, -5.0, 2.0);",
                null,
                false,
                false);
    }

    @Test
    public void testDoublePositiveGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "-5.0\n" +
                        "-2.6\n" +
                        "-0.20000000000000018\n" +
                        "2.1999999999999997\n" +
                        "4.6\n",
                "generate_series(-5.0, 5.0, 2.4);",
                null,
                false,
                false);
    }

    @Test
    public void testDoubleWithLimit() throws Exception {
        assertQuery("generate_series\n" +
                        "1.0\n",
                "generate_series(1d, 10000000d) LIMIT 1",
                null,
                false,
                true);
        assertQuery("generate_series\n" +
                        "1.0E7\n",
                "generate_series(1d, 10000000d) LIMIT -1",
                null,
                false,
                true);
    }

    @Test
    public void testDoubleWithOrdering() throws Exception {
        assertQuery(
                "generate_series\n" +
                        "5.0\n" +
                        "4.0\n" +
                        "3.0\n" +
                        "2.0\n" +
                        "1.0\n" +
                        "0.0\n" +
                        "-1.0\n" +
                        "-2.0\n" +
                        "-3.0\n" +
                        "-4.0\n" +
                        "-5.0\n",
                "generate_series(-5.0d, 5.0d, 1.0d) ORDER BY generate_series DESC;",
                null,
                true,
                false
        );
        assertQuery(
                "generate_series\n" +
                        "5.0\n" +
                        "4.0\n" +
                        "3.0\n" +
                        "2.0\n" +
                        "1.0\n" +
                        "0.0\n" +
                        "-1.0\n" +
                        "-2.0\n" +
                        "-3.0\n" +
                        "-4.0\n" +
                        "-5.0\n",
                "generate_series(-5.0d, 5.0d, -1.0d)",
                null,
                false,
                false
        );
    }

    @Test
    public void testLongBindVariables() throws Exception {
        bindVariableService.setLong("l1", -5);
        bindVariableService.setLong("l2", 5);
        bindVariableService.setLong("l3", 1);

        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-4\n" +
                        "-3\n" +
                        "-2\n" +
                        "-1\n" +
                        "0\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n",
                "generate_series(:l1, :l2, :l3);",
                null,
                true,
                true);

        bindVariableService.setLong("l3", 3);

        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-2\n" +
                        "1\n" +
                        "4\n",
                "generate_series(:l1, :l2, :l3);",
                null,
                true,
                true);
    }

    @Test
    public void testLongDefaultGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-4\n" +
                        "-3\n" +
                        "-2\n" +
                        "-1\n" +
                        "0\n" +
                        "1\n" +
                        "2\n" +
                        "3\n" +
                        "4\n" +
                        "5\n",
                "generate_series(-5, 5);",
                null,
                true,
                true);
    }

    @Test
    public void testLongGenerationNulls() throws Exception {
        assertException("generate_series(null, 5, 3);", 16, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2, null, 3);", 19, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2, 5, null);", 22, "step argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null, null, 3);", 16, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2, null, null);", 19, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null, 5, null);", 16, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null, null, null);", 16, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testLongNegativeGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "5\n" +
                        "3\n" +
                        "1\n" +
                        "-1\n" +
                        "-3\n" +
                        "-5\n",
                "generate_series(-5, 5, -2);",
                null,
                true,
                true);
    }

    @Test
    public void testLongNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "5\n" +
                        "3\n" +
                        "1\n" +
                        "-1\n" +
                        "-3\n" +
                        "-5\n",
                "generate_series(5, -5, -2);",
                null,
                true,
                true);
    }

    @Test
    public void testLongNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "5\n" +
                        "2\n" +
                        "-1\n" +
                        "-4\n",
                "generate_series(-5, 5, -3);",
                null,
                true,
                true);
    }

    @Test
    public void testLongNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "2\n",
                "generate_series(2, 2, 3);",
                null,
                true,
                true);
    }

    @Test
    public void testLongPositiveGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-3\n" +
                        "-1\n" +
                        "1\n" +
                        "3\n" +
                        "5\n",
                "generate_series(-5, 5, 2);",
                null,
                true,
                true);
    }

    @Test
    public void testLongPositiveGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-3\n" +
                        "-1\n" +
                        "1\n" +
                        "3\n" +
                        "5\n",
                "generate_series(5, -5, 2);",
                null,
                true,
                true);
    }

    @Test
    public void testLongPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-2\n" +
                        "1\n" +
                        "4\n",
                "generate_series(-5, 5, 3);",
                null,
                true,
                true);
    }

    @Test
    public void testLongWithLimit() throws Exception {
        assertQuery("generate_series\n" +
                        "1\n",
                "generate_series(1L, 10000000L) LIMIT 1",
                null,
                true,
                true);
        assertQuery("generate_series\n" +
                        "10000000\n",
                "generate_series(1L, 10000000L) LIMIT -1",
                null,
                true,
                true);
    }

    @Test
    public void testLongWithOrdering() throws Exception {
        assertQuery(
                "generate_series\n" +
                        "5\n" +
                        "4\n" +
                        "3\n" +
                        "2\n" +
                        "1\n" +
                        "0\n" +
                        "-1\n" +
                        "-2\n" +
                        "-3\n" +
                        "-4\n" +
                        "-5\n",
                "generate_series(-5, 5, 1) ORDER BY generate_series DESC;",
                null,
                true,
                true
        );
        assertQuery(
                "generate_series\n" +
                        "5\n" +
                        "4\n" +
                        "3\n" +
                        "2\n" +
                        "1\n" +
                        "0\n" +
                        "-1\n" +
                        "-2\n" +
                        "-3\n" +
                        "-4\n" +
                        "-5\n",
                "generate_series(-5, 5, -1)",
                null,
                true,
                true
        );
    }

    @Test
    public void testPeriodErrorPosition() throws Exception {
        assertException("generate_series(1, 100, '11');", 24, "invalid period [period=11]");
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
    public void testTimestampLongBindVariables() throws Exception {
        bindVariableService.setTimestamp("t1", -5);
        bindVariableService.setTimestamp("t2", 5);
        bindVariableService.setTimestamp("t3", 1);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999996Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000005Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                true,
                true);

        bindVariableService.setLong("t3", 3);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000004Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampLongGenerationNulls() throws Exception {
        assertException("generate_series(null::timestamp, 5::timestamp, 3::timestamp);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp, null::timestamp, 3::timestamp);", 34, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp, 5::timestamp, null::timestamp);", 48, "step argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp, null::timestamp, 3::timestamp);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp, null::timestamp, null::timestamp);", 34, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp, 5::timestamp, null::timestamp);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp, null::timestamp, null::timestamp);", 20, "start argument must be a non-null constant or bind variable constant");

        assertException("generate_series(null::timestamp_ns, 5::timestamp_ns, 3::timestamp_ns);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp_ns, null::timestamp_ns, 3::timestamp_ns);", 37, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp_ns, 5::timestamp_ns, null::timestamp_ns);", 54, "step argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp_ns, null::timestamp_ns, 3::timestamp_ns);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp_ns, null::timestamp_ns, null::timestamp_ns);", 37, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp_ns, 5::timestamp_ns, null::timestamp_ns);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp_ns, null::timestamp_ns, null::timestamp_ns);", 20, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testTimestampLongNegativeGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, (-2)::timestamp);",
                "generate_series###DESC",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1969-12-31T23:59:59.999997000Z\n" +
                        "1969-12-31T23:59:59.999995000Z\n",
                "generate_series((-5000)::timestamp_ns, 5::timestamp, (-2000)::timestamp_ns);",
                "generate_series###DESC",
                true,
                true);
    }

    @Test
    public void testTimestampLongNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series(5::timestamp, (-5)::timestamp, (-2)::timestamp);",
                "generate_series###DESC",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1969-12-31T23:59:59.999997000Z\n" +
                        "1969-12-31T23:59:59.999995000Z\n",
                "generate_series(5000::timestamp_ns, (-5)::timestamp, (-2000)::timestamp_ns);",
                "generate_series###DESC",
                true,
                true);
    }

    @Test
    public void testTimestampLongNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999996Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, (-3)::timestamp);",
                "generate_series###DESC",
                true,
                true);
    }

    @Test
    public void testTimestampLongNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000002Z\n",
                "generate_series(2::timestamp, 2::timestamp, 3::timestamp);",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000002000Z\n",
                "generate_series(2000::timestamp_ns, 2::timestamp, 3::timestamp);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampLongPositiveGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000005Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, 2::timestamp);",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999999995Z\n" +
                        "1969-12-31T23:59:59.999999997Z\n" +
                        "1969-12-31T23:59:59.999999999Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n" +
                        "1970-01-01T00:00:00.000000005Z\n",
                "generate_series((-5)::timestamp_ns, 5::timestamp_ns, 2::timestamp_ns);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampLongPositiveGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000005Z\n",
                "generate_series(5::timestamp, (-5)::timestamp, 2::timestamp);",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995000Z\n" +
                        "1969-12-31T23:59:59.999997000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000005000Z\n",
                "generate_series(5000::timestamp_ns, (-5)::timestamp, 2000::timestamp_ns);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampLongPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000004Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, 3);",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995000Z\n" +
                        "1969-12-31T23:59:59.999998000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1970-01-01T00:00:00.000004000Z\n",
                "generate_series((-5)::timestamp, 5000::timestamp_ns, 3000);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampNanoLongBindVariables() throws Exception {
        bindVariableService.setTimestampNano("t1", -5);
        bindVariableService.setTimestampNano("t2", 5);
        bindVariableService.setTimestampNano("t3", 1);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999999995Z\n" +
                        "1969-12-31T23:59:59.999999996Z\n" +
                        "1969-12-31T23:59:59.999999997Z\n" +
                        "1969-12-31T23:59:59.999999998Z\n" +
                        "1969-12-31T23:59:59.999999999Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000002Z\n" +
                        "1970-01-01T00:00:00.000000003Z\n" +
                        "1970-01-01T00:00:00.000000004Z\n" +
                        "1970-01-01T00:00:00.000000005Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                true,
                true);

        bindVariableService.setLong("t3", 3);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999999995Z\n" +
                        "1969-12-31T23:59:59.999999998Z\n" +
                        "1970-01-01T00:00:00.000000001Z\n" +
                        "1970-01-01T00:00:00.000000004Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringBindVariables() throws Exception {
        bindVariableService.setStr("t1", "2025-01-01");
        bindVariableService.setStr("t2", "2025-02-01");
        bindVariableService.setStr("t3", "5d");

        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000000Z\n" +
                        "2025-01-06T00:00:00.000000000Z\n" +
                        "2025-01-11T00:00:00.000000000Z\n" +
                        "2025-01-16T00:00:00.000000000Z\n" +
                        "2025-01-21T00:00:00.000000000Z\n" +
                        "2025-01-26T00:00:00.000000000Z\n" +
                        "2025-01-31T00:00:00.000000000Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                true,
                true);

        bindVariableService.setStr("t3", "1w");

        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000000Z\n" +
                        "2025-01-08T00:00:00.000000000Z\n" +
                        "2025-01-15T00:00:00.000000000Z\n" +
                        "2025-01-22T00:00:00.000000000Z\n" +
                        "2025-01-29T00:00:00.000000000Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringGenerationNulls() throws Exception {
        assertException("generate_series(null::timestamp, 5::timestamp, '3U');", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp, null::timestamp, '3U');", 34, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp, 5::timestamp, null);", 44, "step argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp, null::timestamp, '3U');", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp, null::timestamp, null);", 34, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp, 5::timestamp, null);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp, null::timestamp, null);", 20, "start argument must be a non-null constant or bind variable constant");

        assertException("generate_series(null::timestamp_ns, 5::timestamp_ns, '3U');", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp_ns, null::timestamp_ns, '3U');", 37, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp_ns, 5::timestamp_ns, null);", 50, "step argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp_ns, null::timestamp_ns, '3U');", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(2::timestamp_ns, null::timestamp_ns, null);", 37, "end argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp_ns, 5::timestamp_ns, null);", 20, "start argument must be a non-null constant or bind variable constant");
        assertException("generate_series(null::timestamp_ns, null::timestamp_ns, null);", 20, "start argument must be a non-null constant or bind variable constant");
    }

    @Test
    public void testTimestampStringNegativeGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, '-2U');",
                "generate_series###DESC",
                true,
                true);
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1969-12-31T23:59:59.999997000Z\n" +
                        "1969-12-31T23:59:59.999995000Z\n",
                "generate_series((-5000)::timestamp_ns, 5::timestamp, '-2U');",
                "generate_series###DESC",
                true,
                true);
    }

    @Test
    public void testTimestampStringNegativeGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series(5::timestamp, (-5)::timestamp, '-2U');",
                "generate_series###DESC",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1969-12-31T23:59:59.999997000Z\n" +
                        "1969-12-31T23:59:59.999995000Z\n",
                "generate_series(5000::timestamp_ns, (-5)::timestamp, '-2U');",
                "generate_series###DESC",
                true,
                true);
    }

    @Test
    public void testTimestampStringNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999996Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, '-3U');",
                "generate_series###DESC",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000002000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1969-12-31T23:59:59.999996000Z\n",
                "generate_series((-5000)::timestamp_ns, 5000::timestamp_ns, '-3U');",
                "generate_series###DESC",
                true,
                true);
    }

    @Test
    public void testTimestampStringNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000002Z\n",
                "generate_series(2::timestamp, 2::timestamp, '1U');",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000000002Z\n",
                "generate_series(2::timestamp_ns, 2::timestamp_ns, '1n');",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringPositiveGeneration() throws Exception {
        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000Z\n" +
                        "2025-01-02T00:00:00.000000Z\n" +
                        "2025-01-03T00:00:00.000000Z\n" +
                        "2025-01-04T00:00:00.000000Z\n" +
                        "2025-01-05T00:00:00.000000Z\n" +
                        "2025-01-06T00:00:00.000000Z\n" +
                        "2025-01-07T00:00:00.000000Z\n" +
                        "2025-01-08T00:00:00.000000Z\n" +
                        "2025-01-09T00:00:00.000000Z\n" +
                        "2025-01-10T00:00:00.000000Z\n" +
                        "2025-01-11T00:00:00.000000Z\n" +
                        "2025-01-12T00:00:00.000000Z\n" +
                        "2025-01-13T00:00:00.000000Z\n" +
                        "2025-01-14T00:00:00.000000Z\n" +
                        "2025-01-15T00:00:00.000000Z\n" +
                        "2025-01-16T00:00:00.000000Z\n" +
                        "2025-01-17T00:00:00.000000Z\n" +
                        "2025-01-18T00:00:00.000000Z\n" +
                        "2025-01-19T00:00:00.000000Z\n" +
                        "2025-01-20T00:00:00.000000Z\n" +
                        "2025-01-21T00:00:00.000000Z\n" +
                        "2025-01-22T00:00:00.000000Z\n" +
                        "2025-01-23T00:00:00.000000Z\n" +
                        "2025-01-24T00:00:00.000000Z\n" +
                        "2025-01-25T00:00:00.000000Z\n" +
                        "2025-01-26T00:00:00.000000Z\n" +
                        "2025-01-27T00:00:00.000000Z\n" +
                        "2025-01-28T00:00:00.000000Z\n" +
                        "2025-01-29T00:00:00.000000Z\n" +
                        "2025-01-30T00:00:00.000000Z\n" +
                        "2025-01-31T00:00:00.000000Z\n" +
                        "2025-02-01T00:00:00.000000Z\n",
                "generate_series('2025-01-01', '2025-02-01', '1d');",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000000Z\n" +
                        "2025-01-02T00:00:00.000000000Z\n" +
                        "2025-01-03T00:00:00.000000000Z\n" +
                        "2025-01-04T00:00:00.000000000Z\n" +
                        "2025-01-05T00:00:00.000000000Z\n" +
                        "2025-01-06T00:00:00.000000000Z\n" +
                        "2025-01-07T00:00:00.000000000Z\n" +
                        "2025-01-08T00:00:00.000000000Z\n" +
                        "2025-01-09T00:00:00.000000000Z\n" +
                        "2025-01-10T00:00:00.000000000Z\n" +
                        "2025-01-11T00:00:00.000000000Z\n" +
                        "2025-01-12T00:00:00.000000000Z\n" +
                        "2025-01-13T00:00:00.000000000Z\n" +
                        "2025-01-14T00:00:00.000000000Z\n" +
                        "2025-01-15T00:00:00.000000000Z\n" +
                        "2025-01-16T00:00:00.000000000Z\n" +
                        "2025-01-17T00:00:00.000000000Z\n" +
                        "2025-01-18T00:00:00.000000000Z\n" +
                        "2025-01-19T00:00:00.000000000Z\n" +
                        "2025-01-20T00:00:00.000000000Z\n" +
                        "2025-01-21T00:00:00.000000000Z\n" +
                        "2025-01-22T00:00:00.000000000Z\n" +
                        "2025-01-23T00:00:00.000000000Z\n" +
                        "2025-01-24T00:00:00.000000000Z\n" +
                        "2025-01-25T00:00:00.000000000Z\n" +
                        "2025-01-26T00:00:00.000000000Z\n" +
                        "2025-01-27T00:00:00.000000000Z\n" +
                        "2025-01-28T00:00:00.000000000Z\n" +
                        "2025-01-29T00:00:00.000000000Z\n" +
                        "2025-01-30T00:00:00.000000000Z\n" +
                        "2025-01-31T00:00:00.000000000Z\n" +
                        "2025-02-01T00:00:00.000000000Z\n",
                "generate_series('2025-01-01'::timestamp_ns, '2025-02-01', '1d');",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringPositiveGenerationReverse() throws Exception {
        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000Z\n" +
                        "2025-01-06T00:00:00.000000Z\n" +
                        "2025-01-11T00:00:00.000000Z\n" +
                        "2025-01-16T00:00:00.000000Z\n" +
                        "2025-01-21T00:00:00.000000Z\n" +
                        "2025-01-26T00:00:00.000000Z\n" +
                        "2025-01-31T00:00:00.000000Z\n",
                "generate_series('2025-02-01', '2025-01-01', '5d');",
                "generate_series",
                true,
                true);
        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000000Z\n" +
                        "2025-01-06T00:00:00.000000000Z\n" +
                        "2025-01-11T00:00:00.000000000Z\n" +
                        "2025-01-16T00:00:00.000000000Z\n" +
                        "2025-01-21T00:00:00.000000000Z\n" +
                        "2025-01-26T00:00:00.000000000Z\n" +
                        "2025-01-31T00:00:00.000000000Z\n",
                "generate_series('2025-02-01', '2025-01-01'::timestamp_ns, '5d');",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000Z\n" +
                        "2025-01-06T00:00:00.000000Z\n" +
                        "2025-01-11T00:00:00.000000Z\n" +
                        "2025-01-16T00:00:00.000000Z\n" +
                        "2025-01-21T00:00:00.000000Z\n" +
                        "2025-01-26T00:00:00.000000Z\n" +
                        "2025-01-31T00:00:00.000000Z\n",
                "generate_series('2025-01-01', '2025-02-01', '5d');",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000000Z\n" +
                        "2025-01-06T00:00:00.000000000Z\n" +
                        "2025-01-11T00:00:00.000000000Z\n" +
                        "2025-01-16T00:00:00.000000000Z\n" +
                        "2025-01-21T00:00:00.000000000Z\n" +
                        "2025-01-26T00:00:00.000000000Z\n" +
                        "2025-01-31T00:00:00.000000000Z\n",
                "generate_series('2025-01-01'::timestamp_ns, '2025-02-01', '5d');",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringSizeExpected() throws Exception {
        assertQuery("generate_series\n" +
                        "2020-01-01T00:00:00.000000Z\n" +
                        "2022-01-01T00:00:00.000000Z\n" +
                        "2024-01-01T00:00:00.000000Z\n",
                "generate_series('2020-01-01', '2025-02-01', '2y');",
                "generate_series",
                false,
                true);
        assertQuery("generate_series\n" +
                        "2020-01-01T00:00:00.000000Z\n" +
                        "2020-09-01T00:00:00.000000Z\n" +
                        "2021-05-01T00:00:00.000000Z\n" +
                        "2022-01-01T00:00:00.000000Z\n" +
                        "2022-09-01T00:00:00.000000Z\n" +
                        "2023-05-01T00:00:00.000000Z\n" +
                        "2024-01-01T00:00:00.000000Z\n" +
                        "2024-09-01T00:00:00.000000Z\n",
                "generate_series('2020-01-01', '2025-02-01', '8M');",
                "generate_series",
                false,
                true);

        assertQuery("generate_series\n" +
                        "2020-01-01T00:00:00.000000000Z\n" +
                        "2022-01-01T00:00:00.000000000Z\n" +
                        "2024-01-01T00:00:00.000000000Z\n",
                "generate_series('2020-01-01'::timestamp_ns, '2025-02-01', '2y');",
                "generate_series",
                false,
                true);
        assertQuery("generate_series\n" +
                        "2020-01-01T00:00:00.000000000Z\n" +
                        "2020-09-01T00:00:00.000000000Z\n" +
                        "2021-05-01T00:00:00.000000000Z\n" +
                        "2022-01-01T00:00:00.000000000Z\n" +
                        "2022-09-01T00:00:00.000000000Z\n" +
                        "2023-05-01T00:00:00.000000000Z\n" +
                        "2024-01-01T00:00:00.000000000Z\n" +
                        "2024-09-01T00:00:00.000000000Z\n",
                "generate_series('2020-01-01', '2025-02-01'::timestamp_ns, '8M');",
                "generate_series",
                false,
                true);
    }

    @Test
    public void testTimestampStringWithLimit() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000001Z\n",
                "generate_series(1::timestamp, 10000000::timestamp, '1U') LIMIT 1",
                "generate_series",
                true,
                true);
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:10.000000Z\n",
                "generate_series(1::timestamp, 10000000::timestamp, '1U') LIMIT -1",
                "generate_series",
                true,
                true);

        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000000001Z\n",
                "generate_series(1::timestamp_ns, 10000000::timestamp_ns, '1n') LIMIT 1",
                "generate_series",
                true,
                true);
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:10.000000000Z\n",
                "generate_series(1000::timestamp_ns, 10000000000::timestamp_ns, '1U') LIMIT -1",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampStringWithOrdering() throws Exception {
        assertQuery(
                "generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999996Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, '1U') ORDER BY generate_series DESC;",
                "generate_series###DESC",
                true,
                true
        );

        assertQuery(
                "generate_series\n" +
                        "1970-01-01T00:00:00.000005000Z\n" +
                        "1970-01-01T00:00:00.000004000Z\n" +
                        "1970-01-01T00:00:00.000003000Z\n" +
                        "1970-01-01T00:00:00.000002000Z\n" +
                        "1970-01-01T00:00:00.000001000Z\n" +
                        "1970-01-01T00:00:00.000000000Z\n" +
                        "1969-12-31T23:59:59.999999000Z\n" +
                        "1969-12-31T23:59:59.999998000Z\n" +
                        "1969-12-31T23:59:59.999997000Z\n" +
                        "1969-12-31T23:59:59.999996000Z\n" +
                        "1969-12-31T23:59:59.999995000Z\n",
                "generate_series((-5000)::timestamp_ns, 5::timestamp, '1U') ORDER BY generate_series DESC;",
                "generate_series###DESC",
                true,
                true
        );
    }

    @Test
    public void testTimestampWithLimit() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000001Z\n",
                "generate_series(1::timestamp, 10000000::timestamp, 1) LIMIT 1",
                "generate_series",
                true,
                true);
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:10.000000Z\n",
                "generate_series(1::timestamp, 10000000::timestamp, 1) LIMIT -1",
                "generate_series",
                true,
                true);
    }

    @Test
    public void testTimestampWithOrdering() throws Exception {
        assertQuery(
                "generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999996Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, 1::timestamp) ORDER BY generate_series DESC;",
                "generate_series###DESC",
                true,
                true
        );
        assertQuery(
                "generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000004Z\n" +
                        "1970-01-01T00:00:00.000003Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000000Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1969-12-31T23:59:59.999997Z\n" +
                        "1969-12-31T23:59:59.999996Z\n" +
                        "1969-12-31T23:59:59.999995Z\n",
                "generate_series((-5)::timestamp, (5)::timestamp, -1::timestamp)",
                "generate_series###DESC",
                true,
                true
        );
    }

    @Test
    public void testVariationsOfPeriods() throws Exception {
        assertSqlWithTypes("generate_series\n" +
                        "2025-01-01T00:00:00.000000Z:TIMESTAMP\n" +
                        "2025-01-12T00:00:00.000000Z:TIMESTAMP\n" +
                        "2025-01-23T00:00:00.000000Z:TIMESTAMP\n",
                "generate_series(('2025-01-01')::timestamp, ('2025-02-01')::timestamp, '11d');");
        assertSqlWithTypes("generate_series\n" +
                        "2025-02-01T00:00:00.000000Z:TIMESTAMP\n" +
                        "2025-01-21T00:00:00.000000Z:TIMESTAMP\n" +
                        "2025-01-10T00:00:00.000000Z:TIMESTAMP\n",
                "generate_series(('2025-01-01')::timestamp, ('2025-02-01')::timestamp, '-11d');");

        assertSqlWithTypes("generate_series\n" +
                        "2025-01-01T00:00:00.000000000Z:TIMESTAMP_NS\n" +
                        "2025-01-12T00:00:00.000000000Z:TIMESTAMP_NS\n" +
                        "2025-01-23T00:00:00.000000000Z:TIMESTAMP_NS\n",
                "generate_series(('2025-01-01')::timestamp_ns, ('2025-02-01')::timestamp_ns, '11d');");
        assertSqlWithTypes("generate_series\n" +
                        "2025-02-01T00:00:00.000000000Z:TIMESTAMP_NS\n" +
                        "2025-01-21T00:00:00.000000000Z:TIMESTAMP_NS\n" +
                        "2025-01-10T00:00:00.000000000Z:TIMESTAMP_NS\n",
                "generate_series(('2025-01-01')::timestamp_ns, ('2025-02-01')::timestamp_ns, '-11d');");
    }
}
