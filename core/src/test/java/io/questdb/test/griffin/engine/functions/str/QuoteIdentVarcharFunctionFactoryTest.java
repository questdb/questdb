/*+*****************************************************************************
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
import io.questdb.griffin.engine.functions.str.QuoteIdentVarcharFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class QuoteIdentVarcharFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void test() throws SqlException {
        call(utf8("")).andAssert("");
        call(utf8(null)).andAssert(null);
        call(utf8("test")).andAssert("test");
        call(utf8("TEST")).andAssert("TEST");

        call(utf8("a b")).andAssert("\"a b\"");
        call(utf8("a\tb")).andAssert("\"a\tb\"");
        call(utf8("a^b")).andAssert("\"a^b\"");
        call(utf8("a\"b")).andAssert("\"a\"\"b\"");
    }

    @Test
    public void testMultiByte() throws Exception {
        assertQuery("select quote_ident('héllo'::varchar) a, quote_ident('héllo wörld'::varchar) b, " +
                "quote_ident('ää-'::varchar) c, quote_ident('東京 タワー'::varchar) d")
                .expectSize()
                .returns("a\tb\tc\td\nhéllo\t\"héllo wörld\"\t\"ää-\"\t\"東京 タワー\"\n");
    }

    @Test(timeout = 60_000)
    public void testOutsideBmp() throws Exception {
        // a 4-byte UTF-8 character must not stall the scan; it is quoted like the STRING overload does
        assertQuery("select quote_ident('a😀'::varchar) a, quote_ident('a😀') b, quote_ident('😀\"x'::varchar) c")
                .expectSize()
                .returns("a\tb\tc\n\"a😀\"\t\"a😀\"\t\"😀\"\"x\"\n");
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new QuoteIdentVarcharFunctionFactory();
    }
}
