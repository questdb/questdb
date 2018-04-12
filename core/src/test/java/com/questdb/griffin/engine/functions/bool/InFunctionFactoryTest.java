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

package com.questdb.griffin.engine.functions.bool;

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class InFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBadConstant() {
        assertFailure(11, "STRING constant expected", "x", "a", 10);
    }

    @Test
    public void testNoMatch() throws SqlException {
        call("x", "a", "b").andAssert(false);
    }

    @Test
    public void testNullConstant() {
        assertFailure(11, "NULL is not allowed", "x", "a", null);
    }

    @Test
    public void testTwoArgs() throws SqlException {
        call("x", "x", "y").andAssert(true);
    }

    @Test
    public void testZeroArgs() throws SqlException {
        call("x").andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new InFunctionFactory();
    }
}
