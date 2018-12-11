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
import org.junit.Test;

public class EqDoubleFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEquals() throws SqlException {
        call(150.123, 150.123).andAssert(true);
    }

    @Test
    public void testEqualsNegative() throws SqlException {
        call(-150.123, -150.123).andAssert(true);
    }

    @Test
    public void testNotEquals() throws SqlException {
        call(10.123, 20.134).andAssert(false);
    }

    @Test
    public void testLeftNaN() throws SqlException {
        call(Double.NaN, 77.9).andAssert(false);
    }

    @Test
    public void testRightNaN() throws SqlException {
        call(77.1, Double.NaN).andAssert(false);
    }

    @Test
    public void testNullEqualsNull() throws SqlException {
        call(Double.NaN, Double.NaN).andAssert(true);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqDoubleFunctionFactory();
    }
}