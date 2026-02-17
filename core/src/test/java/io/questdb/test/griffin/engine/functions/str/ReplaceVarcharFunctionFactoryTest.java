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
import io.questdb.griffin.engine.functions.str.ReplaceVarcharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ReplaceVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNullReplacement() throws SqlException {
        call(utf8("abbbc"), utf8("bbb"), utf8(null)).andAssertUtf8(null);
    }

    @Test
    public void testNullTerm() throws SqlException {
        call(utf8("abc"), utf8(null), utf8("x")).andAssertUtf8(null);
    }

    @Test
    public void testNullValue() throws SqlException {
        call(utf8(null), utf8("bbb"), utf8("x")).andAssertUtf8(null);
    }

    @Test
    public void testReplaceMoreThanOneTermOccurrence() throws SqlException {
        call(utf8(""), utf8("a"), utf8("b")).andAssertUtf8("");
        call(utf8(""), utf8("a"), utf8("")).andAssertUtf8("");
        call(utf8(""), utf8(""), utf8("")).andAssertUtf8("");
        call(utf8(""), utf8(""), utf8("b")).andAssertUtf8("");

        call(utf8("aa"), utf8("a"), utf8("b")).andAssertUtf8("bb");
        call(utf8("aac"), utf8("a"), utf8("b")).andAssertUtf8("bbc");
        call(utf8("aac"), utf8("a"), utf8("bb")).andAssertUtf8("bbbbc");
        call(utf8("aac"), utf8("c"), utf8("ddd")).andAssertUtf8("aaddd");

        call(utf8("aac"), utf8("a"), utf8("")).andAssertUtf8("c");
        call(utf8("aac"), utf8("c"), utf8("")).andAssertUtf8("aa");
        call(utf8("bbb"), utf8("b"), utf8("")).andAssertUtf8("");
        call(utf8("bbb"), utf8("X"), utf8("")).andAssertUtf8("bbb");

        call(utf8("bbb"), utf8(""), utf8("")).andAssertUtf8("bbb");
        call(utf8("bbb"), utf8(""), utf8("aaa")).andAssertUtf8("bbb");
        call(utf8("bbb"), utf8("bbbc"), utf8("aaa")).andAssertUtf8("bbb");
    }

    @Test
    public void testReplaceSingleTermOccurrence() throws SqlException {
        call(utf8("motorhead"), utf8("x"), utf8("y")).andAssertUtf8("motorhead");
        call(utf8("motorhead"), utf8("head"), utf8("bike")).andAssertUtf8("motorbike");
        call(utf8("motorhead"), utf8("head"), utf8("h")).andAssertUtf8("motorh");
        call(utf8("motorhead"), utf8("motor"), utf8("m")).andAssertUtf8("mhead");
    }

    @Test
    public void testReplaceWithAnyNullArgReturnsNull() throws SqlException {
        call(utf8(null), utf8(null), utf8(null)).andAssertUtf8(null);
        call(utf8(null), utf8("b"), utf8("c")).andAssertUtf8(null);
        call(utf8(null), utf8("b"), utf8(null)).andAssertUtf8(null);
        call(utf8(null), utf8(null), utf8("c")).andAssertUtf8(null);
        call(utf8("a"), utf8(null), utf8("c")).andAssertUtf8(null);
        call(utf8("a"), utf8("b"), utf8(null)).andAssertUtf8(null);
        call(utf8("a"), utf8(null), utf8(null)).andAssertUtf8(null);
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
    public void testReplacementEnd() throws SqlException {
        call(utf8("hello xx ok"), utf8("ok"), utf8("better")).andAssertUtf8("hello xx better");
    }

    @Test
    public void testReplacementEndSingleChar() throws SqlException {
        call(utf8("hello xx ok"), utf8("k"), utf8("gg")).andAssertUtf8("hello xx ogg");
    }

    @Test
    public void testReplacementIsLonger() throws SqlException {
        call(utf8("hello xx ok"), utf8("xx"), utf8("ooooo")).andAssertUtf8("hello ooooo ok");
    }

    @Test
    public void testReplacementIsSameLength() throws SqlException {
        call(utf8("hello xx ok"), utf8("xx"), utf8("yy")).andAssertUtf8("hello yy ok");
    }

    @Test
    public void testReplacementIsShorter() throws SqlException {
        call(utf8("hello xx ok"), utf8("xx"), utf8("u")).andAssertUtf8("hello u ok");
    }

    @Test
    public void testReplacementStart() throws SqlException {
        call(utf8("hello xx ok"), utf8("hello"), utf8("bye")).andAssertUtf8("bye xx ok");
    }

    @Test
    public void testWhenChainedCallsExceedsMaxLengthExceptionIsThrown() {
        assertFailure("[-1] breached memory limit set for replace(SSS) [maxLength=1048576, requiredLength=1048580]",
                "select replace(replace(replace(replace( 'aaaaaaaaaaaaaaaaaaaa', 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa')");
    }

    @Test
    public void testWhenSingleCallExceedsMaxLengthExceptionIsThrown() throws SqlException {
        try {
            call(
                    utf8(multiply("a", 100)),
                    utf8("a"),
                    utf8(multiply("a", 100000))
            ).andAssertUtf8(null);
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "breached memory limit set for replace(ØØØ)");
        }
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ReplaceVarcharFunctionFactory();
    }

    StringBuilder multiply(String s, int times) {
        StringBuilder sb = new StringBuilder(s.length() * times);
        for (int i = 0; i < times; i++) {
            sb.append(s);
        }
        return sb;
    }
}
