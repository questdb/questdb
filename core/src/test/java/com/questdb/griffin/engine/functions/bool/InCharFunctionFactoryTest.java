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

package com.questdb.griffin.engine.functions.bool;

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class InCharFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBadConstant() {
        assertFailure(7, "CHAR constant expected", 'i', "an", 10);
    }

    @Test
    public void testNoMatch() throws SqlException {
        call('x', 'y', 'z').andAssert(false);
    }

    @Test
    public void testNullConstant() {
        assertFailure(11, "CHAR constant expected", 'c', 'c', null);
    }

    @Test
    public void testTwoArgs() throws SqlException {
        call('x', 'y', 'x').andAssert(true);
    }

    @Test
    public void testZeroArgs() throws SqlException {
        call('y').andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new InCharFunctionFactory();
    }
}
