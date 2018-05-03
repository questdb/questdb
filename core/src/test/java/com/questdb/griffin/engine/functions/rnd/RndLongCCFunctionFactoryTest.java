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

public class RndLongCCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testInvalidRange() {
        assertFailure(0, "invalid range", 20L, 10L, 0);
        assertFailure(0, "invalid range", 5L, 5L, 0);
        assertFailure(13, "invalid NaN rate", 1L, 4L, -1);
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

    private void assertFunctionValues(long lo, long hi, boolean expectNan, Invocation invocation, Function function1) {
        int nanCount = 0;
        for (int i = 0; i < 1000; i++) {
            long value = function1.getLong(invocation.getRecord());
            if (value == Numbers.LONG_NaN) {
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

    private void assertValues(long lo, long hi, int nanRate) throws SqlException {
        Invocation invocation = call(lo, hi, nanRate);
        assertFunctionValues(lo, hi, nanRate > 0, invocation, invocation.getFunction1());
        assertFunctionValues(lo, hi, nanRate > 0, invocation, invocation.getFunction2());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndLongCCFunctionFactory();
    }
}