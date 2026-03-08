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
import io.questdb.griffin.engine.functions.str.ReplaceStrFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ReplaceStrFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNullReplacement() throws Exception {
        assertMemoryLeak(() -> call("abbbc", "bbb", null).andAssert(null));
    }

    @Test
    public void testNullTerm() throws Exception {
        assertMemoryLeak(() -> call("abc", null, "x").andAssert(null));
    }

    @Test
    public void testNullValue() throws Exception {
        assertMemoryLeak(() -> call(null, "bbb", "x").andAssert(null));
    }

    @Test
    public void testReplaceMoreThanOneTermOccurrence() throws Exception {
        assertMemoryLeak(() -> {
            call("", "a", "b").andAssert("");
            call("", "a", "").andAssert("");
            call("", "", "").andAssert("");
            call("", "", "b").andAssert("");

            call("aa", "a", "b").andAssert("bb");
            call("aac", "a", "b").andAssert("bbc");
            call("aac", "a", "bb").andAssert("bbbbc");
            call("aac", "c", "ddd").andAssert("aaddd");

            call("aac", "a", "").andAssert("c");
            call("aac", "c", "").andAssert("aa");
            call("bbb", "b", "").andAssert("");
            call("bbb", "X", "").andAssert("bbb");

            call("bbb", "", "").andAssert("bbb");
            call("bbb", "", "aaa").andAssert("bbb");
            call("bbb", "bbbc", "aaa").andAssert("bbb");
        });
    }

    @Test
    public void testReplaceSingleTermOccurrence() throws Exception {
        assertMemoryLeak(() -> {
            call("motorhead", "x", "y").andAssert("motorhead");
            call("motorhead", "head", "bike").andAssert("motorbike");
            call("motorhead", "head", "h").andAssert("motorh");
            call("motorhead", "motor", "m").andAssert("mhead");
        });
    }

    @Test
    public void testReplaceWithAnyNullArgReturnsNull() throws Exception {
        assertMemoryLeak(() -> {
            call(null, null, null).andAssert(null);
            call(null, "b", "c").andAssert(null);
            call(null, "b", null).andAssert(null);
            call(null, null, "c").andAssert(null);
            call("a", null, "c").andAssert(null);
            call("a", "b", null).andAssert(null);
            call("a", null, null).andAssert(null);
        });
    }

    @Test
    public void testReplaceWithSymbols() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tab as (select 'sym'::symbol sym from long_sequence(1))");

            assertSql("replace\nSym\n", "select replace(sym, 's', 'S') from tab");
            assertSql("replace\nS\n", "select replace(sym, sym, 'S') from tab");
            assertSql("replace\nsym\n", "select replace(sym, sym, sym) from tab");
        });
    }

    @Test
    public void testReplacementEnd() throws Exception {
        assertMemoryLeak(() -> call("hello xx ok", "ok", "better").andAssert("hello xx better"));
    }

    @Test
    public void testReplacementIsLonger() throws Exception {
        assertMemoryLeak(() -> call("hello xx ok", "xx", "ooooo").andAssert("hello ooooo ok"));
    }

    @Test
    public void testReplacementIsSameLength() throws Exception {
        assertMemoryLeak(() -> call("hello xx ok", "xx", "yy").andAssert("hello yy ok"));
    }

    @Test
    public void testReplacementIsShorter() throws Exception {
        assertMemoryLeak(() -> call("hello xx ok", "xx", "u").andAssert("hello u ok"));
    }

    @Test
    public void testReplacementStart() throws Exception {
        assertMemoryLeak(() -> call("hello xx ok", "hello", "bye").andAssert("bye xx ok"));
    }

    @Test
    public void testWhenChainedCallsExceedsMaxLengthExceptionIsThrown() {
        assertFailure("[-1] breached memory limit set for replace(SSS) [maxLength=1048576, requiredLength=1048580]",
                "select replace(replace(replace(replace( 'aaaaaaaaaaaaaaaaaaaa', 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa')");
    }

    @Test
    public void testWhenSingleCallExceedsMaxLengthExceptionIsThrown() throws Exception {
        assertMemoryLeak(() -> {
            try {
                call(multiply("a", 100), "a", multiply("a", 100000)).andAssert(null);
                Assert.fail();
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "breached memory limit set for replace(SSS)");
            }
        });
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ReplaceStrFunctionFactory();
    }

    StringBuilder multiply(String s, int times) {
        StringBuilder sb = new StringBuilder(s.length() * times);
        for (int i = 0; i < times; i++) {
            sb.append(s);
        }
        return sb;
    }
}
