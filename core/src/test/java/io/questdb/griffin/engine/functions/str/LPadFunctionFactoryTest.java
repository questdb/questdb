/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

public class LPadFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testLPad() throws SqlException {
        call("abc", 6).andAssert("   abc");
        call("xyz", 10).andAssert("       xyz");
        call("pqrs", 7).andAssert("   pqrs");
    }

    @Test
    public void testTrimRight() throws SqlException {
        call("abcdefgh", 5).andAssert("abcde");
        call("photosynthesis", 10).andAssert("photosynth");
    }

    @Test
    public void testZeroLength() throws SqlException {
        call("abc", 0).andAssert("");
        call("pqrs", 0).andAssert("");
    }

    @Test
    public void testNegativeLength() throws SqlException {
        call("abc", -1).andAssert(null);
        call("pqrs", -4).andAssert(null);
    }

    @Test
    public void testNullStr() throws SqlException {
        call(null, 3).andAssert(null);
        call(null, 4).andAssert(null);
    }

    @Test
    public void testNaNLength() throws SqlException {
        call("abc", Numbers.INT_NaN).andAssert(null);
        call("pqrs", Numbers.INT_NaN).andAssert(null);
    }

    @Test
    public void testFailsOnBufferLengthAboveLimit() throws SqlException {
        try {
            call("foo", Integer.MAX_VALUE).andAssert(null);
            Assert.fail();
        } catch (CairoException e) {
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("breached memory limit set for lpad(SI)"));
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LPadFunctionFactory();
    }
}