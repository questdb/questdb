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
import org.junit.Assert;
import org.junit.Test;

public class RndStrFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testFixLength() throws SqlException {
        Invocation invocation = call(3, 3, 5);
        assertFixedLengthFunction(invocation.getFunction1());
        assertFixedLengthFunction(invocation.getFunction2());
    }

    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 5, 3, 6);
        assertFailure(0, "invalid range", 0, 3, 6);
        assertFailure(13, "rate must be positive", 1, 10, -1);
    }

    @Test
    public void testInvalidRate() {
        assertFailure(12, "rate must be positive", 3, 6, -1);
    }

    @Test
    public void testNoNulls() throws SqlException {
        assertFunction(2, 10, 0);
    }

    @Test
    public void testPositive() throws SqlException {
        assertFunction(5, 8, 5);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndStrFunctionFactory();
    }

    private void assertFixedLengthFunction(Function function) {
        assertFixedLengthFunctionA(function);
        assertFixedLengthFunctionB(function);
    }

    private void assertFixedLengthFunctionA(Function function) {
        int nullCount = 0;
        for (int i = 0; i < 100; i++) {
            CharSequence value = function.getStr(null);
            if (value == null) {
                nullCount++;
            } else {
                int len = value.length();
                Assert.assertEquals(3, len);
            }
        }
        Assert.assertTrue(nullCount > 0);
    }

    private void assertFixedLengthFunctionB(Function function) {
        int nullCount = 0;
        for (int i = 0; i < 100; i++) {
            CharSequence value = function.getStrB(null);
            if (value == null) {
                nullCount++;
            } else {
                int len = value.length();
                Assert.assertEquals(3, len);
            }
        }
        Assert.assertTrue(nullCount > 0);
    }

    private void assertFunction(int lo, int hi, int nullRate) throws SqlException {
        assertFunctionA(lo, hi, nullRate);
        assertFunctionB(lo, hi, nullRate);
    }

    private void assertFunctionA(int lo, int hi, int nullRate) throws SqlException {
        Invocation invocation = call(lo, hi, nullRate);
        Function function = invocation.getFunction1();
        int nullCount = 0;
        for (int i = 0; i < 100; i++) {
            CharSequence value = function.getStr(null);
            if (value == null) {
                nullCount++;
            } else {
                int len = value.length();
                Assert.assertTrue(len <= hi && len >= lo);
            }
        }
        if (nullRate > 0) {
            Assert.assertTrue(nullCount > 0);
        } else {
            Assert.assertEquals(0, nullCount);
        }
    }

    private void assertFunctionB(int lo, int hi, int nullRate) throws SqlException {
        Invocation invocation = call(lo, hi, nullRate);
        Function function = invocation.getFunction1();
        int nullCount = 0;
        for (int i = 0; i < 100; i++) {
            CharSequence value = function.getStrB(null);
            if (value == null) {
                nullCount++;
            } else {
                int len = value.length();
                Assert.assertTrue(len <= hi && len >= lo);
            }
        }
        if (nullRate > 0) {
            Assert.assertTrue(nullCount > 0);
        } else {
            Assert.assertEquals(0, nullCount);
        }
    }

}