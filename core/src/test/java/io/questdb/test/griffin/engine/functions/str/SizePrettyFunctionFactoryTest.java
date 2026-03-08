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

package io.questdb.test.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class SizePrettyFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testSizes() throws SqlException {
        call(0L).andAssert("0.0 B");
        call(Numbers.LONG_NULL).andAssert(null);
        call(1L).andAssert("1.0 B");
        call(1024L).andAssert("1.0 KiB");
        call(1024L * 1024).andAssert("1.0 MiB");
        call(1024L * 1024 * 1024).andAssert("1.0 GiB");
        call(1024L * 1024 * 1024 * 1024).andAssert("1.0 TiB");
        call(1024L * 1024 * 1024 * 1024 * 1024).andAssert("1.0 PiB");
        call(1024L * 1024 * 1024 * 1024 * 1024 * 1024).andAssert("1.0 EiB");
        call((long) (8.657 * 1024L * 1024 * 1024 * 1024 * 1024 * 1024)).andAssert("8.0 EiB");
        call((long) (8.657 * 1024L * 1024 * 1024 * 1024 * 1024)).andAssert("8.7 PiB");
        call((long) (8.657 * 1024L * 1024 * 1024 * 1024)).andAssert("8.7 TiB");
        call((long) (8.657 * 1024L * 1024 * 1024)).andAssert("8.7 GiB");
        call((long) (8.657 * 1024L * 1024)).andAssert("8.7 MiB");
        call((long) (8.657 * 1024L)).andAssert("8.7 KiB");
        call((long) 8.657).andAssert("8.0 B");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new SizePrettyFunctionFactory();
    }
}
