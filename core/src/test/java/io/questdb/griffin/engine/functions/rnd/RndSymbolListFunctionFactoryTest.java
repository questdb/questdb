/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.rnd;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.CharSequenceHashSet;
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
        Invocation invocation = callCustomised(true, true, "ABC", "CDE", "XYZ", null);
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