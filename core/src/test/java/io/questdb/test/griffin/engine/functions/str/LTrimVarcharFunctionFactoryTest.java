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

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.str.LTrimVarcharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class LTrimVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testEmptyLTrimSpace() throws SqlException {
        call(utf8("")).andAssertUtf8("");
        call(utf8(" ")).andAssertUtf8("");
        call(utf8("    ")).andAssertUtf8("");
        call(utf8(null)).andAssertUtf8(null);
    }

    @Test
    public void testLTrimSpace() throws SqlException {
        call(utf8("    abc     ")).andAssertUtf8("abc     ");
        call(utf8("     abc")).andAssertUtf8("abc");
        call(utf8(" a b c ")).andAssertUtf8("a b c ");
    }

    @Test
    public void testNoLTrimSpace() throws SqlException {
        call(utf8("abc     ")).andAssertUtf8("abc     ");
        call(utf8("a b c")).andAssertUtf8("a b c");
        call(utf8("zzz")).andAssertUtf8("zzz");
        call(utf8("()  /  {}")).andAssertUtf8("()  /  {}");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new LTrimVarcharFunctionFactory();
    }
}
