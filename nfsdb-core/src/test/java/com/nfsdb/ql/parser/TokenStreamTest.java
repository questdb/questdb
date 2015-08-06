/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.ql.parser;

import com.nfsdb.io.sink.StringSink;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class TokenStreamTest {
    @Test
    public void testEdgeSymbol() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol(" ");
        ts.defineSymbol("+");
        ts.defineSymbol("(");
        ts.defineSymbol(")");
        ts.defineSymbol(",");

        CharSequence content;
        ts.setContent(content = "create journal xyz(a int, b int)");
        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }
        TestUtils.assertEquals(content, sink);
    }

    @Test
    public void testNullContent() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol(" ");
        ts.setContent(null);
        Assert.assertFalse(ts.iterator().hasNext());
    }

    @Test
    public void testQuotedToken() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        ts.setContent("a+\"b\"*abc");

        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }

        TestUtils.assertEquals("a+\"b\"*abc", sink);
    }

    @Test
    public void testSingleQuotedToken() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        ts.setContent("a+'b'*abc");

        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }

        TestUtils.assertEquals("a+'b'*abc", sink);
    }

    @Test
    public void testSymbolLookup() throws Exception {
        TokenStream ts = new TokenStream();
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        CharSequence content;
        ts.setContent(content = "+*a+b++blah-");

        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }
        TestUtils.assertEquals(content, sink);
    }

    @Test
    public void testUnparse() {
        TokenStream ts = new TokenStream();
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");
        ts.setContent("+*a+b++blah-");

        Iterator<CharSequence> it = ts.iterator();

        while (it.hasNext()) {
            CharSequence e = it.next();
            ts.unparse();
            CharSequence a = it.next();
            TestUtils.assertEquals(e, a);
        }
    }
}
