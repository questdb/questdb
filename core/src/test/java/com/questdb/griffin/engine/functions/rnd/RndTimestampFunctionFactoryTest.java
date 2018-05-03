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

public class RndTimestampFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testInvalidNaNRate() {
        assertFailure(50, "invalid NaN rate", 0L, 10000L, -2);
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