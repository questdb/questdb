/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.griffin.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import com.questdb.std.Rnd;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class RndStrFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testExpectedOutcome() throws SqlException {
        Invocation invocation = call(4, 10, 2);
        Function function = invocation.getFunction1();

        Assert.assertTrue(function instanceof RandomFunction);
        ((RandomFunction) function).init(new Rnd()); // stable generator
        TestUtils.assertEquals("JWCPSW", function.getStr(null));
    }

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