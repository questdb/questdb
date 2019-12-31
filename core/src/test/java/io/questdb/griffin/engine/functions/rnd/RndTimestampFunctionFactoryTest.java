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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import io.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class RndTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testInvalidNaNRate() {
        assertFailure(60, "invalid NaN rate", 0L, 10000L, -2);
    }

    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 100000L, 0L, 1);
    }

    @Test
    public void testNaNs() throws SqlException {
        Invocation invocation = call(0L, 10000L, 2);
        Function function = invocation.getFunction1();
        int nanCount = 0;
        for (int i = 0; i < 1000; i++) {
            long value = function.getTimestamp(null);
            if (value == Numbers.LONG_NaN) {
                nanCount++;
            }
        }
        Assert.assertTrue(nanCount > 0);
    }

    @Test
    public void testNoNaNs() throws SqlException {
        Invocation invocation = call(0L, 10000L, 0);
        Function function = invocation.getFunction1();
        int nanCount = 0;
        for (int i = 0; i < 1000; i++) {
            long value = function.getTimestamp(null);
            if (value == Numbers.LONG_NaN) {
                nanCount++;
            }
        }
        Assert.assertEquals(0, nanCount);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndTimestampFunctionFactory();
    }
}