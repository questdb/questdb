/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.date;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;
import org.junit.Test;

public class AddPeriodToTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {


    @Test
    public void testDaySimple() throws SqlException {
        call(1587275359886758L, 'd', 5).andAssert(1587707359886758L, 0.0001);
    }

    @Test
    public void testWeekSimple() throws SqlException {
        call(1587275359886758L, 'w', 5).andAssert(1590299359886758L, 0.0001);
    }

    @Test
    public void testHourSimple() throws SqlException {
        call(1587275359886758L, 'h', 5).andAssert(1587293359886758L, 0.0001);
    }

    @Test
    public void testSecondSimple() throws SqlException {
        call(1587275359886758L, 's', 5).andAssert(1587275364886758L, 0.0001);
    }

    @Test
    public void testMinuteSimple() throws SqlException {
        call(1587275359886758L, 'm', 5).andAssert(1587275659886758L, 0.0001);
    }

    @Test
    public void testMonthSimple() throws SqlException {
        call(1587275359886758L, 'M', 5).andAssert(1600494559886758L, 0.0001);
    }

    @Test
    public void testYearsSimple() throws SqlException {
        call(1587275359886758L, 'y', 5).andAssert(1745041759886758L, 0.0001);
    }

    @Test
    public void testDaySimpleNeg() throws SqlException {
        call(1587275359886758L, 'd', -5).andAssert(1586843359886758L, 0.0001);
    }

    @Test
    public void testWeekSimpleNeg() throws SqlException {
        call(1587275359886758L, 'w', -5).andAssert(1584251359886758L, 0.0001);
    }

    @Test
    public void testHourSimpleNeg() throws SqlException {
        call(1587275359886758L, 'h', -5).andAssert(1587257359886758L, 0.0001);
    }

    @Test
    public void testSecondSimpleNeg() throws SqlException {
        call(1587275359886758L, 's', -5).andAssert(1587275354886758L, 0.0001);
    }

    @Test
    public void testMinuteSimpleNeg() throws SqlException {
        call(1587275359886758L, 'm', -5).andAssert(1587275059886758L, 0.0001);
    }

    @Test
    public void testMonthSimpleNeg() throws SqlException {
        call(1587275359886758L, 'M', -5).andAssert(1574142559886758L, 0.0001);
    }

    @Test
    public void testYearsSimpleNeg() throws SqlException {
        call(1587275359886758L, 'y', -5).andAssert(1429422559886758L, 0.0001);
    }

    @Test
    public void testLeftNan() throws SqlException {
        call(Numbers.LONG_NaN, 'd', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testCenterEmptyChar() throws SqlException {
        call(1587275359886758L, Character.MIN_VALUE, 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }
    
    @Test
    public void testUnknownPeriod() throws SqlException {
        call(1587275359886758L, 'q', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNSecond() throws SqlException {
        call(1587275359886758L, 's', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNMinute() throws SqlException {
        call(1587275359886758L, 'm', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNHour() throws SqlException {
        call(1587275359886758L, 'h', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNDay() throws SqlException {
        call(1587275359886758L, 'd', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNWeek() throws SqlException {
        call(1587275359886758L, 'w', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNMonth() throws SqlException {
        call(1587275359886758L, 'M', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testRightNaNYear() throws SqlException {
        call(1587275359886758L, 'y', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNSecond() throws SqlException {
        call(Numbers.LONG_NaN, 's', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNMinute() throws SqlException {
        call(Numbers.LONG_NaN, 'm', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNHour() throws SqlException {
        call(Numbers.LONG_NaN, 'h', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNDay() throws SqlException {
        call(Numbers.LONG_NaN, 'd', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNWeek() throws SqlException {
        call(Numbers.LONG_NaN, 'w', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNMonth() throws SqlException {
        call(Numbers.LONG_NaN, 'M', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testLeftNaNYear() throws SqlException {
        call(Numbers.LONG_NaN, 'y', 5).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testIntervalConstantPeriodVariableSecondLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 's', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMinuteLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 'm', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableHourLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 'h', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableDayLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 'd', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableWeekLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 'w', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMonthLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 'M', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableYearLeftNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "\n" +
                        "\n",
                "select add_period(Cast(NaN as Long), 'y', cast(x as int)) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableSecondRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2020-04-19T05:49:20.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 's', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMinuteRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2020-04-19T05:50:19.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 'm', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableHourRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2020-04-19T06:49:19.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 'h', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableDayRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2020-04-20T05:49:19.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 'd', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableWeekRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2020-04-26T05:49:19.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 'w', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableMonthRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2020-05-19T05:49:19.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 'M', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Test
    public void testIntervalConstantPeriodVariableYearRightNaN() throws SqlException {
        assertQuery(
                "add_period\n" +
                        "2021-04-19T05:49:19.886758Z\n" +
                        "\n",
                "select add_period(1587275359886758L, 'y', case when x = 1 then cast(x as int) else Cast(NaN as int) end) from long_sequence(2)",
                null,
                true
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new AddPeriodToTimestampFunctionFactory();
    }

}