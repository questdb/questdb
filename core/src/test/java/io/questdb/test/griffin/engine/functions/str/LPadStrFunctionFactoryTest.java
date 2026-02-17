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

import io.questdb.cairo.CairoException;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.LPadStrFunctionFactory;
import io.questdb.std.Numbers;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LPadStrFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testEmptyFillText() throws SqlException {
        call("abc", 4, "").andAssert(null);
        call("pqrs", 10, "").andAssert(null);
    }

    @Test
    public void testFailsOnBufferLengthAboveLimit() throws SqlException {
        try {
            call("foo", Integer.MAX_VALUE, "bar").andAssert(null);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "breached memory limit set for lpad(SIS)");
        }
    }

    @Test
    public void testLPadStr() throws SqlException {
        call("abc", 5, "x").andAssert("xxabc");
        call("xyz", 10, "hello").andAssert("hellohexyz");
        call("pqrs", 7, "abc").andAssert("abcpqrs");
    }

    @Test
    public void testNaNLength() throws SqlException {
        call("abc", Numbers.INT_NULL, "xyz").andAssert(null);
        call("pqrs", Numbers.INT_NULL, "xyz").andAssert(null);
    }

    @Test
    public void testNegativeLength() throws SqlException {
        call("abc", -1, "hello").andAssert(null);
        call("pqrs", -4, "hello").andAssert(null);
    }

    @Test
    public void testNullFillText() throws SqlException {
        call("abc", 4, null).andAssert(null);
        call("pqrs", 10, null).andAssert(null);
    }

    @Test
    public void testNullStr() throws SqlException {
        call(null, 3, "hello").andAssert(null);
        call(null, 4, "hello").andAssert(null);
    }

    @Test
    public void testTrimRight() throws SqlException {
        call("abcdefgh", 5, "abc").andAssert("abcde");
        call("photosynthesis", 10, "light").andAssert("photosynth");
    }

    @Test
    public void testZeroLength() throws SqlException {
        call("abc", 0, "hello").andAssert("");
        call("pqrs", 0, "hello").andAssert("");
    }

    @Test
    public void testABProtocol() throws SqlException {
        execute("create table x as (select rnd_str(1, 40, 0) s from long_sequence(100))");
        assertSql(
                "count\n" +
                        "100\n",
                "select count (*) from x where lpad(s, 20, '.') = lpad(s, 20, '.')"
        );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LPadStrFunctionFactory();
    }
}