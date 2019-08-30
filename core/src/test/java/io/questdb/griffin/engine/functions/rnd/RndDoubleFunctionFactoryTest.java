/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import org.junit.Assert;
import org.junit.Test;

public class RndDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNoNaN() throws SqlException {
        assertFunction(call());
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndDoubleFunctionFactory();
    }

    private void assertFunction(Invocation invocation) {
        assertFunction(invocation.getFunction1(), invocation.getRecord());
        assertFunction(invocation.getFunction2(), invocation.getRecord());
    }

    private void assertFunction(Function function, Record record) {
        int nanCount = 0;
        for (int i = 0; i < 100; i++) {
            double d = function.getDouble(record);
            if (d != d) {
                nanCount++;
            } else {
                Assert.assertTrue(d > 0 && d < 1);
            }
        }
        Assert.assertEquals(0, nanCount);
    }
}