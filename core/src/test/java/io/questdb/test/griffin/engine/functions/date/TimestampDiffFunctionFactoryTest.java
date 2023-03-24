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
import io.questdb.griffin.engine.functions.date.TimestampDiffFunctionFactory;
import io.questdb.std.Numbers;
import org.junit.Test;

public class TimestampDiffFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testDayConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('d', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDayConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "365\n" +
                        "365\n",
                "select datediff('d', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDayConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('d', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testDayEndNan() throws SqlException {
        call('d', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testDayNegative() throws SqlException {
        call('d', 1587707359886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testDaySimple() throws SqlException {
        call('d', 1587275359886758L, 1587707359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testDayStartNan() throws SqlException {
        call('d', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testHourConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('h', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "8760\n" +
                        "8760\n",
                "select datediff('h', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('h', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testHourEndNan() throws SqlException {
        call('h', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testHourNegative() throws SqlException {
        call('h', 1587293359886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testHourSimple() throws SqlException {
        call('h', 1587275359886758L, 1587293359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testHourStartNan() throws SqlException {
        call('h', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testMinuteConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('m', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "525600\n" +
                        "525600\n",
                "select datediff('m', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('m', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMinuteEndNan() throws SqlException {
        call('m', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testMinuteNegative() throws SqlException {
        call('m', 1587275659886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testMinuteSimple() throws SqlException {
        call('m', 1587275359886758L, 1587275659886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testMinuteStartNan() throws SqlException {
        call('m', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testMonthConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('M', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMonthConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "12\n" +
                        "12\n",
                "select datediff('M', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMonthConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('M', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testMonthEndNan() throws SqlException {
        call('M', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testMonthNegative() throws SqlException {
        call('M', 1600494559886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testMonthSimple() throws SqlException {
        call('M', 1587275359886758L, 1600494559886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testMonthStartNan() throws SqlException {
        call('M', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testSecondConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "31536000\n" +
                        "31536000\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('s', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testSecondEndNan() throws SqlException {
        call('s', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testSecondNegative() throws SqlException {
        call('s', 1587275364886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testSecondSimple() throws SqlException {
        call('s', 1587275359886758L, 1587275364886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testSecondStartNan() throws SqlException {
        call('s', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testUnknownPeriod() throws SqlException {
        call('/', 1587275359886758L, 1587275364886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testWeekConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('w', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "52\n" +
                        "52\n",
                "select datediff('w', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('w', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testWeekEndNan() throws SqlException {
        call('w', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testWeekNegative() throws SqlException {
        call('w', 1590299359886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testWeekSimple() throws SqlException {
        call('w', 1587275359886758L, 1590299359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testWeekStartNan() throws SqlException {
        call('w', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testYearConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('y', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testYearConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "1\n" +
                        "1\n",
                "select datediff('y', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testYearConstantStartNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('y', cast(NaN as long), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
                true,
                true
        );
    }

    @Test
    public void testYearEndNan() throws SqlException {
        call('y', 1587275364886758L, Numbers.LONG_NaN).andAssert(Double.NaN, 0.0001);
    }

    @Test
    public void testYearNegative() throws SqlException {
        call('y', 1745041759886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testYearSimple() throws SqlException {
        call('y', 1587275359886758L, 1745041759886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testYearStartNan() throws SqlException {
        call('y', Numbers.LONG_NaN, 1587275359886758L).andAssert(Double.NaN, 0.0001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampDiffFunctionFactory();
    }

}