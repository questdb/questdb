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

import org.junit.Test;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;

public class TimestampAddFunctionFactoryTest extends AbstractFunctionFactoryTest {


    @Test
    public void testCenterEmptyChar() throws Exception {
        assertMemoryLeak(() -> call(Character.MIN_VALUE, 5, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testDaySimple() throws Exception {
        assertMemoryLeak(() -> call('d', 5, 1587275359886758L).andAssert(1587707359886758L, 0.0001));
    }

    @Test
    public void testDaySimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('d', -5, 1587275359886758L).andAssert(1586843359886758L, 0.0001));
    }

    @Test
    public void testHourSimple() throws Exception {
        assertMemoryLeak(() -> call('h', 5, 1587275359886758L).andAssert(1587293359886758L, 0.0001));
    }

    @Test
    public void testHourSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('h', -5, 1587275359886758L).andAssert(1587257359886758L, 0.0001));
    }

    @Test
    public void testIntervalConstantPeriodVariableDayLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('d', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableDayRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-04-20T05:49:19.886758Z\n" +
                        "\n",
                "select dateadd('d', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableHourLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('h', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableHourRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-04-19T06:49:19.886758Z\n" +
                        "\n",
                "select dateadd('h', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMinuteLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('m', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMinuteRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-04-19T05:50:19.886758Z\n" +
                        "\n",
                "select dateadd('m', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMonthLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('M', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMonthRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-05-19T05:49:19.886758Z\n" +
                        "\n",
                "select dateadd('M', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableSecondLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('s', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableSecondRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-04-19T05:49:20.886758Z\n" +
                        "\n",
                "select dateadd('s', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableWeekLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('w', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableWeekRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2020-04-26T05:49:19.886758Z\n" +
                        "\n",
                "select dateadd('w', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableYearLeftNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "\n" +
                        "\n",
                "select dateadd('y', cast(x as int), Cast(NaN as Long)) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableYearRightNaN() throws Exception {
        assertQuery(
                "dateadd\n" +
                        "2021-04-19T05:49:19.886758Z\n" +
                        "\n",
                "select dateadd('y', case when x = 1 then cast(x as int) else Cast(NaN as int) end, 1587275359886758L) from long_sequence(2)",
                null,
                true,
                true
        );
    }

    @Test
    public void testLeftNaNDay() throws Exception {
        assertMemoryLeak(() -> call('d', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNHour() throws Exception {
        assertMemoryLeak(() -> call('h', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNMicro() throws Exception {
        assertMemoryLeak(() -> call('u', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNMilli() throws Exception {
        assertMemoryLeak(() -> call('T', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNMinute() throws Exception {
        assertMemoryLeak(() -> call('m', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNMonth() throws Exception {
        assertMemoryLeak(() -> call('M', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNSecond() throws Exception {
        assertMemoryLeak(() -> call('s', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNWeek() throws Exception {
        assertMemoryLeak(() -> call('w', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNaNYear() throws Exception {
        assertMemoryLeak(() -> call('y', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testLeftNan() throws Exception {
        assertMemoryLeak(() -> call('d', 5, Numbers.LONG_NULL).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testMicroSimple() throws Exception {
        assertMemoryLeak(() -> call('u', 5, 1587275359886758L).andAssert(1587275359886763L, 0.0001));
    }

    @Test
    public void testMicroSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('u', -5, 1587275359886758L).andAssert(1587275359886753L, 0.0001));
    }

    @Test
    public void testMilliSimple() throws Exception {
        assertMemoryLeak(() -> call('T', 5, 1587275359886758L).andAssert(1587275359891758L, 0.0001));
    }

    @Test
    public void testMilliSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('T', -5, 1587275359886758L).andAssert(1587275359881758L, 0.0001));
    }

    @Test
    public void testMinuteSimple() throws Exception {
        assertMemoryLeak(() -> call('m', 5, 1587275359886758L).andAssert(1587275659886758L, 0.0001));
    }

    @Test
    public void testMinuteSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('m', -5, 1587275359886758L).andAssert(1587275059886758L, 0.0001));
    }

    @Test
    public void testMonthSimple() throws Exception {
        assertMemoryLeak(() -> call('M', 5, 1587275359886758L).andAssert(1600494559886758L, 0.0001));
    }

    @Test
    public void testMonthSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('M', -5, 1587275359886758L).andAssert(1574142559886758L, 0.0001));
    }

    @Test
    public void testRightNaNDay() throws Exception {
        assertMemoryLeak(() -> call('d', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNHour() throws Exception {
        assertMemoryLeak(() -> call('h', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNMicro() throws Exception {
        assertMemoryLeak(() -> call('u', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNMilli() throws Exception {
        assertMemoryLeak(() -> call('T', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNMinute() throws Exception {
        assertMemoryLeak(() -> call('m', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNMonth() throws Exception {
        assertMemoryLeak(() -> call('M', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNSecond() throws Exception {
        assertMemoryLeak(() -> call('s', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNWeek() throws Exception {
        assertMemoryLeak(() -> call('w', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testRightNaNYear() throws Exception {
        assertMemoryLeak(() -> call('y', Numbers.INT_NULL, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testSecondSimple() throws Exception {
        assertMemoryLeak(() -> call('s', 5, 1587275359886758L).andAssert(1587275364886758L, 0.0001));
    }

    @Test
    public void testSecondSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('s', -5, 1587275359886758L).andAssert(1587275354886758L, 0.0001));
    }

    @Test
    public void testUnknownPeriod() throws Exception {
        assertMemoryLeak(() -> call('q', 5, 1587275359886758L).andAssert(Double.NaN, 0.0001));
    }

    @Test
    public void testWeekSimple() throws Exception {
        assertMemoryLeak(() -> call('w', 5, 1587275359886758L).andAssert(1590299359886758L, 0.0001));
    }

    @Test
    public void testWeekSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('w', -5, 1587275359886758L).andAssert(1584251359886758L, 0.0001));
    }

    @Test
    public void testYearsSimple() throws Exception {
        assertMemoryLeak(() -> call('y', 5, 1587275359886758L).andAssert(1745041759886758L, 0.0001));
    }

    @Test
    public void testYearsSimpleNeg() throws Exception {
        assertMemoryLeak(() -> call('y', -5, 1587275359886758L).andAssert(1429422559886758L, 0.0001));
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampAddFunctionFactory();
    }
}
