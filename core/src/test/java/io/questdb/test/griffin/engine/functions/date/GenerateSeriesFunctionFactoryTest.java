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

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.griffin.BaseFunctionFactoryTest;
import io.questdb.test.tools.StationaryMicrosClock;
import org.junit.BeforeClass;
import org.junit.Test;

public class GenerateSeriesFunctionFactoryTest extends BaseFunctionFactoryTest {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        testMicrosClock = StationaryMicrosClock.INSTANCE;
        AbstractCairoTest.setUpStatic();
    }

    @Test
    public void testDoubleBindVariables() throws Exception {
        bindVariableService.setDouble("d1", -5.2);
        bindVariableService.setDouble("d2", 5.8);
        bindVariableService.setDouble("d3", 1.3);

        assertQuery("generate_series\n" +
                        "-5.2\n" +
                        "-3.9000000000000004\n" +
                        "-2.6000000000000005\n" +
                        "-1.3000000000000005\n" +
                        "-4.440892098500626E-16\n" +
                        "1.2999999999999996\n" +
                        "2.5999999999999996\n" +
                        "3.8999999999999995\n" +
                        "5.199999999999999\n",
                "generate_series(:d1, :d2, :d3);",
                null,
                false,
                true);

        bindVariableService.setDouble("d3", 2.8);

        assertQuery("generate_series\n" +
                        "-5.2\n" +
                        "-2.4000000000000004\n" +
                        "0.39999999999999947\n" +
                        "3.1999999999999993\n",
                "generate_series(:d1, :d2, :d3);",
                null,
                false,
                true);
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
                true);
    }

    @Test
    public void testDoubleGenerationNulls() throws Exception {
        assertException("generate_series(null, 5.0, 3.0);", 0, "arguments cannot be null");
        assertException("generate_series(2.0, null, 3.0);", 0, "arguments cannot be null");
        assertException("generate_series(2.0, 5.0, null);", 0, "arguments cannot be null");
        assertException("generate_series(null, null, 3.0);", 0, "arguments cannot be null");
        assertException("generate_series(2.0, null, null);", 0, "arguments cannot be null");
        assertException("generate_series(null, 5.0, null);", 0, "arguments cannot be null");
        assertException("generate_series(null, null, null);", 0, "arguments cannot be null");
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
                true);
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
                true);
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
                true);
    }

    @Test
    public void testDoubleNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "2.0\n",
                "generate_series(2.0, 2.0, 3.0);",
                null,
                false,
                true);
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
                true);
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
                true);
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
                true);
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
                false,
                true);

        bindVariableService.setLong("l3", 3);

        assertQuery("generate_series\n" +
                        "-5\n" +
                        "-2\n" +
                        "1\n" +
                        "4\n",
                "generate_series(:l1, :l2, :l3);",
                null,
                false,
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
                false,
                true);
    }

    @Test
    public void testLongGenerationNulls() throws Exception {
        assertException("generate_series(null, 5, 3);", 0, "arguments cannot be null");
        assertException("generate_series(2, null, 3);", 0, "arguments cannot be null");
        assertException("generate_series(2, 5, null);", 0, "arguments cannot be null");
        assertException("generate_series(null, null, 3);", 0, "arguments cannot be null");
        assertException("generate_series(2, null, null);", 0, "arguments cannot be null");
        assertException("generate_series(null, 5, null);", 0, "arguments cannot be null");
        assertException("generate_series(null, null, null);", 0, "arguments cannot be null");
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
                false,
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
                false,
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
                false,
                true);
    }

    @Test
    public void testLongNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "2\n",
                "generate_series(2, 2, 3);",
                null,
                false,
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
                false,
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
                false,
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
                false,
                true);
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
                false,
                true);

        bindVariableService.setLong("t3", 3);

        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000004Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                false,
                true);
    }

    @Test
    public void testTimestampLongGenerationNulls() throws Exception {
        assertException("generate_series(null::timestamp, 5::timestamp, 3::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(2::timestamp, null::timestamp, 3::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(2::timestamp, 5::timestamp, null::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(null::timestamp, null::timestamp, 3::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(2::timestamp, null::timestamp, null::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(null::timestamp, 5::timestamp, null::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(null::timestamp, null::timestamp, null::timestamp);", 0, "arguments cannot be null");
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
                false,
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
                false,
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
                false,
                true);
    }

    @Test
    public void testTimestampLongNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000002Z\n",
                "generate_series(2::timestamp, 2::timestamp, 3::timestamp);",
                "generate_series",
                false,
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
                false,
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
                false,
                true);
    }

    @Test
    public void testTimestampLongPositiveGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "1969-12-31T23:59:59.999995Z\n" +
                        "1969-12-31T23:59:59.999998Z\n" +
                        "1970-01-01T00:00:00.000001Z\n" +
                        "1970-01-01T00:00:00.000004Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, 3::timestamp);",
                "generate_series",
                false,
                true);
    }

    @Test
    public void testTimestampStringBindVariables() throws Exception {
        bindVariableService.setStr("t1", "2025-01-01");
        bindVariableService.setStr("t2", "2025-02-01");
        bindVariableService.setStr("t3", "5d");

        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000Z\n" +
                        "2025-01-06T00:00:00.000000Z\n" +
                        "2025-01-11T00:00:00.000000Z\n" +
                        "2025-01-16T00:00:00.000000Z\n" +
                        "2025-01-21T00:00:00.000000Z\n" +
                        "2025-01-26T00:00:00.000000Z\n" +
                        "2025-01-31T00:00:00.000000Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                false,
                true);

        bindVariableService.setStr("t3", "1w");

        assertQuery("generate_series\n" +
                        "2025-01-01T00:00:00.000000Z\n" +
                        "2025-01-08T00:00:00.000000Z\n" +
                        "2025-01-15T00:00:00.000000Z\n" +
                        "2025-01-22T00:00:00.000000Z\n" +
                        "2025-01-29T00:00:00.000000Z\n",
                "generate_series(:t1, :t2, :t3);",
                "generate_series",
                false,
                true);
    }


    @Test
    public void testTimestampStringGenerationNulls() throws Exception {
        assertException("generate_series(null::timestamp, 5::timestamp, 3::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(2::timestamp, null::timestamp, 3::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(2::timestamp, 5::timestamp, null::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(null::timestamp, null::timestamp, 3::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(2::timestamp, null::timestamp, null::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(null::timestamp, 5::timestamp, null::timestamp);", 0, "arguments cannot be null");
        assertException("generate_series(null::timestamp, null::timestamp, null::timestamp);", 0, "arguments cannot be null");
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
                "generate_series((-5)::timestamp, 5::timestamp, (-2)::timestamp);",
                "generate_series###DESC",
                false,
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
                "generate_series(5::timestamp, (-5)::timestamp, (-2)::timestamp);",
                "generate_series###DESC",
                false,
                true);
    }

    @Test
    public void testTimestampStringNegativeGenerationUneven() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000005Z\n" +
                        "1970-01-01T00:00:00.000002Z\n" +
                        "1969-12-31T23:59:59.999999Z\n" +
                        "1969-12-31T23:59:59.999996Z\n",
                "generate_series((-5)::timestamp, 5::timestamp, (-3)::timestamp);",
                "generate_series###DESC",
                false,
                true);
    }

    @Test
    public void testTimestampStringNoRange() throws Exception {
        assertQuery("generate_series\n" +
                        "1970-01-01T00:00:00.000002Z\n",
                "generate_series(2::timestamp, 2::timestamp, '1u');",
                "generate_series",
                false,
                true);
    }

    @Test
    public void testTimestampStringNoSizeExpected() throws Exception {
        assertQuery("generate_series\n" +
                        "2020-01-01T00:00:00.000000Z\n" +
                        "2022-01-01T00:00:00.000000Z\n" +
                        "2024-01-01T00:00:00.000000Z\n",
                "generate_series('2020-01-01', '2025-02-01', '2y');",
                "generate_series",
                false,
                false);
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
                false);
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
                false,
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
                false,
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
                false,
                true);
    }
}
