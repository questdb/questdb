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

package io.questdb.griffin.engine.functions.date;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.cast.ToTimestampLongFunctionFactory;
import io.questdb.std.Numbers;
import org.junit.Test;

public class ToTimestampLongFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNotNull() throws SqlException {
        call(0L).andAssertTimestamp(0L);
    }

    @Test
    public void testNull() throws SqlException {
        call(Numbers.LONG_NaN).andAssertTimestamp(Numbers.LONG_NaN);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ToTimestampLongFunctionFactory();
    }
}