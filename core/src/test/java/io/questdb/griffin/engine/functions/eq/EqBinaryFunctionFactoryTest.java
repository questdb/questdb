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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class EqBinaryFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testAll() throws SqlException {
        byte [] bin1 = "dabale arroz a la zorra el abad".getBytes(StandardCharsets.UTF_8);
        byte [] bin2 = "the lazy fox jumped over the brown dog".getBytes(StandardCharsets.UTF_8);
        call(bin1, bin1).andAssert(true);
        call(bin1, bin2).andAssert(false);
        call(null, null).andAssert(true);
        call(null, "".getBytes(StandardCharsets.UTF_8)).andAssert(false);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new EqBinaryFunctionFactory();
    }
}