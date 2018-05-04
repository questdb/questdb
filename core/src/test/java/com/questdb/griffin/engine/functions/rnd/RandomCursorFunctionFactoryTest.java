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

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.griffin.engine.functions.math.NegIntFunctionFactory;
import org.junit.Test;

// this test will only test edge cases in factory, testing results with
// current state of function test harness is not possible, because passing of
// arbitrary functions as arguments is not supported.
//
// full tests are done using sql compiler
//
public class RandomCursorFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNegativeRecordCount() {
        assertFailure(14, "invalid record count", -1L, "x", null);
    }

    @Test
    public void testNoColumnDefs() {
        assertFailure(0, "not enough arguments", 10L);
    }

    @Test
    public void testNoColumnFunction() {
        assertFailure(0, "invalid number of arguments", 10L, "x");
    }

    @Test
    public void testNonConstantColumnName() {
        // assert will create two invocations, one is with constants, which we expect under normal circumstances
        // the other is invocation with function as parameter, which is where we expect factory to throw exception
        assertFailure(false, 17, "STRING constant expected", 10L, "x", 5L, null, 6L);
    }

    @Test
    public void testNullColumnName() {
        assertFailure(true, 23, "column name must not be NULL", 10L, "x", 5L, null, 6L);
    }

    @Override
    protected void addExtraFunctions() {
        functions.add(new NegIntFunctionFactory());
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RandomCursorFunctionFactory();
    }
}