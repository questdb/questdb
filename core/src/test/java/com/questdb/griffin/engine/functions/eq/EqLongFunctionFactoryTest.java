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

package com.questdb.griffin.engine.functions.eq;

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.std.Numbers;
import org.junit.Test;

public class EqLongFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException {
        call(150L, 150L).andAssert(true);
    }

    @Test
    public void testNotEquals() throws SqlException {
        call(10L, 20L).andAssert(false);
    }

    @Test
    public void testLeftNaN() throws SqlException {
        call(Numbers.LONG_NaN, 77L).andAssert(false);
    }

    @Test
    public void testRightNaN() throws SqlException {
        call(77L, Numbers.LONG_NaN).andAssert(false);
    }

    @Test
    public void testNullEqualsNull() throws SqlException {
        call(Numbers.LONG_NaN, Numbers.LONG_NaN).andAssert(true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqLongFunctionFactory();
    }
}