/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.date.TimestampAddFunctionFactory;
import io.questdb.std.Numbers;
import org.junit.Test;

public class TimestampAddFunctionFactoryTest extends AbstractFunctionFactoryTest {


    @Test
    public void testCenterEmptyChar() throws SqlException {
        call(Character.MIN_VALUE, 5, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testDaySimple() throws SqlException {
        call('d', 5, 1587275359886758L).andAssert(1587707359886758L, 0.0001);
    }

    @Test
    public void testDaySimpleNeg() throws SqlException {
        call('d', -5, 1587275359886758L).andAssert(1586843359886758L, 0.0001);
    }

    @Test
    public void testHourSimple() throws SqlException {
        call('h', 5, 1587275359886758L).andAssert(1587293359886758L, 0.0001);
    }

    @Test
    public void testHourSimpleNeg() throws SqlException {
        call('h', -5, 1587275359886758L).andAssert(1587257359886758L, 0.0001);
    }

    @Test
    public void testIntervalConstantPeriodVariableDayLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableDayRightNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableHourLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableHourRightNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableMinuteLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableMinuteRightNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableMonthLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableMonthRightNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableSecondLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableSecondRightNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableWeekLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableWeekRightNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableYearLeftNaN() throws SqlException {
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
    public void testIntervalConstantPeriodVariableYearRightNaN() throws SqlException {
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
    public void testLeftNaNDay() throws SqlException {
        call('d', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNaNHour() throws SqlException {
        call('h', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNaNMinute() throws SqlException {
        call('m', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNaNMonth() throws SqlException {
        call('M', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNaNSecond() throws SqlException {
        call('s', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNaNWeek() throws SqlException {
        call('w', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNaNYear() throws SqlException {
        call('y', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testLeftNan() throws SqlException {
        call('d', 5, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testMinuteSimple() throws SqlException {
        call('m', 5, 1587275359886758L).andAssert(1587275659886758L, 0.0001);
    }

    @Test
    public void testMinuteSimpleNeg() throws SqlException {
        call('m', -5, 1587275359886758L).andAssert(1587275059886758L, 0.0001);
    }

    @Test
    public void testMonthSimple() throws SqlException {
        call('M', 5, 1587275359886758L).andAssert(1600494559886758L, 0.0001);
    }

    @Test
    public void testMonthSimpleNeg() throws SqlException {
        call('M', -5, 1587275359886758L).andAssert(1574142559886758L, 0.0001);
    }

    @Test
    public void testRightNaNDay() throws SqlException {
        call('d', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testRightNaNHour() throws SqlException {
        call('h', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testRightNaNMinute() throws SqlException {
        call('m', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testRightNaNMonth() throws SqlException {
        call('M', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testRightNaNSecond() throws SqlException {
        call('s', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testRightNaNWeek() throws SqlException {
        call('w', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testRightNaNYear() throws SqlException {
        call('y', Numbers.INT_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testSecondSimple() throws SqlException {
        call('s', 5, 1587275359886758L).andAssert(1587275364886758L, 0.0001);
    }

    @Test
    public void testSecondSimpleNeg() throws SqlException {
        call('s', -5, 1587275359886758L).andAssert(1587275354886758L, 0.0001);
    }

    @Test
    public void testUnknownPeriod() throws SqlException {
        call('q', 5, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testWeekSimple() throws SqlException {
        call('w', 5, 1587275359886758L).andAssert(1590299359886758L, 0.0001);
    }

    @Test
    public void testWeekSimpleNeg() throws SqlException {
        call('w', -5, 1587275359886758L).andAssert(1584251359886758L, 0.0001);
    }

    @Test
    public void testYearsSimple() throws SqlException {
        call('y', 5, 1587275359886758L).andAssert(1745041759886758L, 0.0001);
    }

    @Test
    public void testYearsSimpleNeg() throws SqlException {
        call('y', -5, 1587275359886758L).andAssert(1429422559886758L, 0.0001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampAddFunctionFactory();
    }

}