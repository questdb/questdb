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
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import org.junit.Assert;
import org.junit.Test;

public class RndDoubleCCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNaNRate() {
        assertFailure(11, "invalid NaN rate", -1);
    }

    @Test
    public void testNoNaN() throws SqlException {
        assertFunction(call(0), false);
    }

    @Test
    public void testSimple() throws SqlException {
        assertFunction(call(4), true);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndDoubleCCFunctionFactory();
    }

    private void assertFunction(Invocation invocation, boolean expectNaN) {
        assertFunction(invocation.getFunction1(), invocation.getRecord(), expectNaN);
        assertFunction(invocation.getFunction2(), invocation.getRecord(), expectNaN);
    }

    private void assertFunction(Function function, Record record, boolean expectNaN) {
        int nanCount = 0;
        for (int i = 0; i < 100; i++) {
            double d = function.getDouble(record);
            if (d != d) {
                nanCount++;
            } else {
                Assert.assertTrue(d > 0 && d < 1);
            }
        }
        if (expectNaN) {
            Assert.assertTrue(nanCount > 0);
        } else {
            Assert.assertEquals(0, nanCount);
        }
    }
}