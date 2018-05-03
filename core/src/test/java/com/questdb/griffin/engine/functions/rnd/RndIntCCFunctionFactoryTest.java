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

import com.questdb.cairo.sql.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import com.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class RndIntCCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 20, 10, 0);
        assertFailure(0, "invalid range", 5, 5, 0);
        assertFailure(12, "invalid NaN rate", 1, 4, -1);
    }

    @Test
    public void testNegativeRange() throws SqlException {
        assertValues(-134, -40, 4);
    }

    @Test
    public void testNoNaNs() throws SqlException {
        assertValues(10, 20, 0);
    }

    @Test
    public void testPositiveRange() throws SqlException {
        assertValues(10, 20, 5);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndIntCCFunctionFactory();
    }

    private void assertFunctionValues(int lo, int hi, boolean expectNan, Invocation invocation, Function function1) {
        int nanCount = 0;
        for (int i = 0; i < 1000; i++) {
            int value = function1.getInt(invocation.getRecord());
            if (value == Numbers.INT_NaN) {
                nanCount++;
            } else {
                Assert.assertTrue(value <= hi && value >= lo);
            }
        }
        if (expectNan) {
            Assert.assertTrue(nanCount > 0);
        } else {
            Assert.assertEquals(0, nanCount);
        }
    }

    private void assertValues(int lo, int hi, int nanRate) throws SqlException {
        Invocation invocation = call(lo, hi, nanRate);
        assertFunctionValues(lo, hi, nanRate > 0, invocation, invocation.getFunction1());
        assertFunctionValues(lo, hi, nanRate > 0, invocation, invocation.getFunction2());
    }
}