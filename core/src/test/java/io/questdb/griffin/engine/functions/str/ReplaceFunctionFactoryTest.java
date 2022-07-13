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
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

public class ReplaceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testNullTerm() throws SqlException {
        call("abc", null, "x").andAssert(null);
    }

    @Test
    public void testNullReplacement() throws SqlException {
        call("abbbc", "bbb", null).andAssert(null);
    }

    @Test
    public void testNullValue() throws SqlException {
        call(null, "bbb", "x").andAssert(null);
    }

    @Test
    public void testReplacementIsSameLength() throws SqlException {
        call("hello xx ok", "xx", "yy").andAssert("hello yy ok");
    }

    @Test
    public void testReplacementIsLonger() throws SqlException {
        call("hello xx ok", "xx", "ooooo").andAssert("hello ooooo ok");
    }

    @Test
    public void testReplacementIsShorter() throws SqlException {
        call("hello xx ok", "xx", "u").andAssert("hello u ok");
    }

    @Test
    public void testReplacementStart() throws SqlException {
        call("hello xx ok", "hello", "bye").andAssert("bye xx ok");
    }

    @Test
    public void testReplacementEnd() throws SqlException {
        call("hello xx ok", "ok", "better").andAssert("hello xx better");
    }

    @Test
    public void testReplaceSingleTermOccurrence() throws SqlException {
        call("motorhead", "x", "y").andAssert("motorhead");
        call("motorhead", "head", "bike").andAssert("motorbike");
        call("motorhead", "head", "h").andAssert("motorh");
        call("motorhead", "motor", "m").andAssert("mhead");
    }

    @Test
    public void testReplaceMoreThanOneTermOccurrence() throws SqlException {
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
    }

    @Test
    public void testReplaceWithAnyNullArgReturnsNull() throws SqlException {
        call(null, null, null).andAssert(null);
        call(null, "b", "c").andAssert(null);
        call(null, "b", null).andAssert(null);
        call(null, null, "c").andAssert(null);
        call("a", null, "c").andAssert(null);
        call("a", "b", null).andAssert(null);
        call("a", null, null).andAssert(null);
    }

    @Test
    public void testWhenSingleCallExceedsMaxLengthExceptionIsThrown() throws SqlException {
        try {
            call(multiply("a", 100), "a", multiply("a", 100000)).andAssert(null);
            Assert.fail();
        } catch (CairoException e) {
            MatcherAssert.assertThat(e.getMessage(), CoreMatchers.containsString("breached memory limit set for replace(SSS)"));
        }
    }

    @Test
    public void testWhenChainedCallsExceedsMaxLengthExceptionIsThrown() {
        assertFailure("[-1] breached memory limit set for replace(SSS) [maxLength=1048576, requiredLength=1048580]",
                "select replace(replace(replace(replace( 'aaaaaaaaaaaaaaaaaaaa', 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa'), 'a', 'aaaaaaaaaaaaaaaaaaaa')");
    }

    StringBuilder multiply(String s, int times) {
        StringBuilder sb = new StringBuilder(s.length() * times);
        for (int i = 0; i < times; i++) {
            sb.append(s);
        }
        return sb;
    }


    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ReplaceStrFunctionFactory();
    }
}
