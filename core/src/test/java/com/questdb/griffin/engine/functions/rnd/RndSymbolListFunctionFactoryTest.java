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
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.AbstractFunctionFactoryTest;
import com.questdb.std.CharSequenceHashSet;
import org.junit.Assert;
import org.junit.Test;

public class RndSymbolListFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNonConstant() {
        assertFailure(false, 11, "STRING constant expected", "ABC", "CDE");
    }

    @Test
    public void testNonString() {
        assertFailure(true, 17, "STRING constant expected", "ABC", 1);
    }

    @Test
    public void testSimple() throws SqlException {
        CharSequenceHashSet set1 = new CharSequenceHashSet();
        set1.add("ABC");
        set1.add("CDE");
        set1.add("XYZ");
        set1.add(null);

        CharSequenceHashSet set2 = new CharSequenceHashSet();
        Invocation invocation = callCustomised(true, "ABC", "CDE", "XYZ", null);
        for (int i = 0; i < 1000; i++) {
            Assert.assertTrue(set1.contains(invocation.getFunction1().getSymbol(null)));
            Assert.assertTrue(set1.contains(invocation.getFunction2().getSymbol(null)));
            set2.add(invocation.getFunction1().getSymbol(null));
        }

        for (int i = 0, n = set1.size(); i < n; i++) {
            Assert.assertTrue(set2.contains(set1.get(i)));
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new RndSymbolListFunctionFactory();
    }
}