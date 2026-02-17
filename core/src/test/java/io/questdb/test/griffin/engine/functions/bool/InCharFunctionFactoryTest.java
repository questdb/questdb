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

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.bool.InCharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class InCharFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testBadConstant() {
        assertFailure(7, "CHAR constant expected", 'i', "an", 10);
    }

    @Test
    public void testNoMatch() throws SqlException {
        call('x', 'y', 'z').andAssert(false);
    }

    @Test
    public void testNullConstant() throws SqlException {
        call('c', 'c', null).andAssert(true);
    }

    @Test
    public void testTwoArgs() throws SqlException {
        call('x', 'y', 'x').andAssert(true);
    }

    @Test
    public void testZeroArgs() {
        try {
            call('y').andAssert(false);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[3] too few arguments for 'in'", e.getMessage());
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new InCharFunctionFactory();
    }
}
