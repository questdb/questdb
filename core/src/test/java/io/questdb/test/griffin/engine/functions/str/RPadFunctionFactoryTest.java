/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.RPadVarcharFunctionFactory;
import io.questdb.std.str.Utf8String;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.griffin.engine.functions.str.RPadFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class RPadFunctionFactoryTest extends AbstractFunctionFactoryTest {
    static final String SIGNATURE_UTF16 = "rpad(SI)";
    static final String SIGNATURE_UTF8 = "rpad(ØI)";

    private final boolean utf8;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    public RPadFunctionFactoryTest(boolean utf8) {
        this.utf8 = utf8;
    }

    @Test
    public void testFailsOnBufferLengthAboveLimit() throws SqlException {
        try {
            callFn("foo", Integer.MAX_VALUE).andAssert(null);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "breached memory limit set for " + (utf8 ? SIGNATURE_UTF8 : SIGNATURE_UTF16));
        }
    }

    @Test
    public void testNaNLength() throws SqlException {
        callFn("abc", Numbers.INT_NaN).andAssert(null);
        callFn("pqrs", Numbers.INT_NaN).andAssert(null);
    }

    @Test
    public void testNegativeLength() throws SqlException {
        callFn("abc", -1).andAssert(null);
        callFn("pqrs", -4).andAssert(null);
    }

    @Test
    public void testNullStr() throws SqlException {
        callFn(null, 3).andAssert(null);
        callFn(null, 4).andAssert(null);
    }

    @Test
    public void testRPad() throws SqlException {
        callFn("abc", 5).andAssert("abc  ");
        callFn("xyz", 10).andAssert("xyz       ");
        callFn("pqrs", 7).andAssert("pqrs   ");
        callFn("пєрєувєлічєніє", 16).andAssert("пєрєувєлічєніє  ");
    }

    @Test
    public void testTrimLeft() throws SqlException {
        callFn("abcdefgh", 5).andAssert("defgh");
        callFn("photosynthesis", 10).andAssert("osynthesis");
    }

    @Test
    public void testZeroLength() throws SqlException {
        callFn("abc", 0).andAssert("");
        callFn("pqrs", 0).andAssert("");
    }

    protected Invocation callFn(String str, int len) throws SqlException {
        if (utf8) {
            Utf8String utf8 = str == null ? null : new Utf8String(str);
            return callBySignature(SIGNATURE_UTF8, utf8, len);
        }
        return callBySignature(SIGNATURE_UTF16, str, len);
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        if (utf8) {
            return new RPadVarcharFunctionFactory();
        }
        return new RPadFunctionFactory();
    }
}