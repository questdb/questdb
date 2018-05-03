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

package com.questdb.griffin.engine.functions.date;

import com.questdb.cairo.sql.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import com.questdb.std.Numbers;
import org.junit.Assert;
import org.junit.Test;

public class TimestampSequenceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testIncrement1() throws SqlException {
        assertFunction(call(0L, 1000L).getFunction1());
    }

    @Test
    public void testIncrement2() throws SqlException {
        assertFunction(call(0L, 1000L).getFunction2());
    }

    @Test
    public void testNaN() throws SqlException {
        call(Numbers.LONG_NaN, 1000L).andAssert(Numbers.LONG_NaN);
    }

    @Test
    public void testNegativeIncrement() {
        assertFailure(35, "positive increment expected", 0L, -100L);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new TimestampSequenceFunctionFactory();
    }

    private void assertFunction(Function function) {
        long next = 0;
        for (int i = 0; i < 10; i++) {
            Assert.assertEquals(next, function.getTimestamp(null));
            next += 1000L;
        }
    }
}