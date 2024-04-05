/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.griffin.engine.functions.bool.InVarcharFunctionFactory;
import io.questdb.std.str.Utf8String;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Assert;
import org.junit.Test;

public class InVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testBadConstant() {
        assertFailure(12, "VARCHAR constant expected", new Utf8String("xv"), new Utf8String("an"), 10);
    }

    @Test
    public void testNoMatch() throws SqlException {
        call(new Utf8String("xc"), new Utf8String("ae"), new Utf8String("bn")).andAssert(false);
    }

    @Test
    public void testNullConstant() throws SqlException {
        call(new Utf8String("xp"), new Utf8String("aq"), null).andAssert(false);
    }

    @Test
    public void testTwoArgs() throws SqlException {
        call(new Utf8String("xy"), new Utf8String("xy"), new Utf8String("yz")).andAssert(true);
    }

    @Test
    public void testTwoArgsOneChar() throws SqlException {
        call(new Utf8String("xy"), new Utf8String("xy"), new Utf8String("yz"), new Utf8String("l")).andAssert(true);
    }

    @Test
    public void testZeroArgs() {
        try {
            call(new Utf8String("xx")).andAssert(false);
            Assert.fail();
        } catch (SqlException e) {
            Assert.assertEquals("[3] too few arguments for 'in'", e.getMessage());
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new InVarcharFunctionFactory();
    }
}
