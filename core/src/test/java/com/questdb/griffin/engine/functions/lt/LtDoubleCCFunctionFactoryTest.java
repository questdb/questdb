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

package com.questdb.griffin.engine.functions.lt;

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LtDoubleCCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testAll() throws SqlException {
        call(10.34, 45.99999).andAssert(true);
        call(10.99999, 10.999).andAssert(false);
        call(Double.NaN, 10.999).andAssert(false);
        call(54.34, Double.NaN).andAssert(false);
        call(Double.NaN, Double.NaN).andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LtDoubleCCFunctionFactory();
    }
}