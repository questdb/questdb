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

package com.questdb.griffin.engine.functions.math;

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.std.Numbers;
import org.junit.Test;

public class SubLongFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testLeftNan() throws SqlException {
        call(Numbers.LONG_NaN, 5L).andAssert(Numbers.LONG_NaN);
    }

    @Test
    public void testNegative() throws SqlException {
        call(-3L, 4L).andAssert(-7L);
    }

    @Test
    public void testRightNan() throws SqlException {
        call(123L, Numbers.LONG_NaN).andAssert(Numbers.LONG_NaN);
    }

    @Test
    public void testSimple() throws SqlException {
        call(10L, 8L).andAssert(2L);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SubLongFunctionFactory();
    }
}