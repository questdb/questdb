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

package io.questdb.griffin.engine.functions.str;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.AbstractFunctionFactoryTest;
import io.questdb.std.Numbers;
import org.junit.Test;

public class ReplaceFunctionFactoryTest extends AbstractFunctionFactoryTest {
    @Test
    public void testNullTerm() throws SqlException {
        call("abc", null, "x").andAssert("abc");
    }

    @Test
    public void testNullReplacement() throws SqlException {
        call("abbbc", "bbb", null).andAssert("ac");
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

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new ReplaceStrFunctionFactory();
    }
}