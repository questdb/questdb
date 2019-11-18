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

package io.questdb.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;
import org.junit.Test;

public class SubStrVVFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNaN() throws SqlException {
        call("abc", Numbers.INT_NaN).andAssert(null);
    }

    @Test
    public void testNegativeStart() throws SqlException {
        call("abcd", -2).andAssert("");
    }

    @Test
    public void testNull() throws SqlException {
        call(null, 2).andAssert(null);
    }

    @Test
    public void testSimple() throws SqlException {
        call("xyz", 2).andAssert("z");
    }

    @Test
    public void testStartOutOfBounds() throws SqlException {
        call("abc", 3).andAssert("");
    }

    @Test
    public void testStartOutOfBounds2() throws SqlException {
        call("abc", 5).andAssert("");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SubStrFunctionFactory();
    }
}