/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.griffin.engine.functions.lt;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.lt.LtLongFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LtLongFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testAll() throws SqlException {
        call(1024L, 4560L).andAssert(true);
        call(-13L, -1L).andAssert(true);
        call(77L, 77L).andAssert(false);
        call(-7L, 7L).andAssert(true);
        call(1L, -1L).andAssert(false);
        call(-10L, -10L).andAssert(false);
        call(Numbers.LONG_NULL, 7L).andAssert(false);
        call(42L, Numbers.LONG_NULL).andAssert(false);
        call(Numbers.LONG_NULL, Numbers.LONG_NULL).andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LtLongFunctionFactory();
    }
}
