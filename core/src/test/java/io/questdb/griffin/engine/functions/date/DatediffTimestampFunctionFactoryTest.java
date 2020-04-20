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

public class DatediffTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testUnknownPeriod() throws SqlException {
        call('/', 1587275359886758L, 1587275364886758L).andAssert(Numbers.LONG_NaN, 0.0001);
    }


    @Test
    public void testSecondSimple() throws SqlException {
        call('s', 1587275359886758L, 1587275364886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testSecondNegative() throws SqlException {
        call('s', 1587275364886758L, 1587275359886758L).andAssert(5, 0.0001);
    }

    @Test
    public void testSecondStartNan() throws SqlException {
        call('s', Numbers.LONG_NaN, 1587275359886758L).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testSecondEndNan() throws SqlException {
        call('s', 1587275364886758L, Numbers.LONG_NaN).andAssert(Numbers.LONG_NaN, 0.0001);
    }

    @Test
    public void testSecondConstantSimple() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "31536000\n" +
                        "31536000\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), to_timestamp(concat('202', x+1),'yyyy')) from long_sequence(2);",
                null,
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
                true
        );
    }

    @Test
    public void testSecondConstantEndNaN() throws SqlException {
        assertQuery(
                "datediff\n" +
                        "NaN\n" +
                        "NaN\n",
                "select datediff('s', to_timestamp(concat('202',x),'yyyy'), cast(NaN as long)) from long_sequence(2);",
                null,
                true
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new DatediffTimestampFunctionFactory();
    }

}