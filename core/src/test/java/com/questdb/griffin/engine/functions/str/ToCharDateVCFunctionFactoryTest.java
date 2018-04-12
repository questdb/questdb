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

package com.questdb.griffin.engine.functions.str;

import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.time.DateFormatUtils;
import org.junit.Test;

public class ToCharDateVCFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNaN() throws SqlException {
        call(Numbers.LONG_NaN, "dd/MM/yyyy hh:mm:ss").andAssert(null);
    }

    @Test
    public void testNullFormat() {
        assertFailure(11, "format must not be null", 0L, null);
    }

    @Test
    public void testSimple() throws SqlException, NumericException {
        call(DateFormatUtils.parseDateTime("2018-03-10T11:03:33.123Z"),
                "dd/MM/yyyy hh:mm:ss").andAssert("10/03/2018 12:03:33");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToCharDateVCFunctionFactory();
    }
}