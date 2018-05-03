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
import com.questdb.cairo.sql.Record;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import org.junit.Assert;
import org.junit.Test;

public class RndDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {
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
        return new RndDoubleFunctionFactory();
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