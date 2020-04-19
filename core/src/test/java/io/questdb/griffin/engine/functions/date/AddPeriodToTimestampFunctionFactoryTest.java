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
    public void testRightNaN() throws SqlException {
        call(1587275359886758L, 'd', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testUnknownPeriod() throws SqlException {
        call(1587275359886758L, 'q', Numbers.INT_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new AddPeriodToTimestampFunctionFactory();
    }

}