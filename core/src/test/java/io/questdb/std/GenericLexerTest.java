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

package io.questdb.std;

import io.questdb.griffin.SqlUtil;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class GenericLexerTest {

    @Test
    public void testBlockComments() {
        GenericLexer lex = new GenericLexer(64);
        lex.defineSymbol("+");
        lex.defineSymbol("++");
        lex.defineSymbol("*");
        lex.defineSymbol("/*");
        lex.defineSymbol("*/");

        lex.of("a + /* ok, this /* is a */ comment */ 'b' * abc");

        StringSink sink = new StringSink();
        CharSequence token;
        while ((token = SqlUtil.fetchNext(lex)) != null) {
            sink.put(token);
        }

        TestUtils.assertEquals("a+'b'*abc", sink);
    }

    @Test
    public void testDoubleEscapedQuote() {
        GenericLexer lex = new GenericLexer(64);

        lex.defineSymbol("(");
        lex.defineSymbol(";");
        lex.defineSymbol(")");
        lex.defineSymbol(",");
        lex.defineSymbol("/*");
        lex.defineSymbol("*/");
        lex.defineSymbol("--");

        lex.of("insert into data values ('{ title: \\\"Title\\\"}');");

        CharSequence tok;
        final StringSink sink = new StringSink();
        while ((tok = SqlUtil.fetchNext(lex)) != null) {
            sink.put(tok).put('\n');
        }
        TestUtils.assertEquals("insert\n" +
                        "into\n" +
                        "data\n" +
                        "values\n" +
                        "(\n" +
                        "'{ title: \\\"Title\\\"}'\n" +
                        ")\n" +
                        ";\n",
                sink
        );
    }

    @Test
    public void testEdgeSymbol() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol(" ");
        ts.defineSymbol("+");
        ts.defineSymbol("(");
        ts.defineSymbol(")");
        ts.defineSymbol(",");

        CharSequence content;
        ts.of(content = "create journal xyz(a int, b int)");
        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }
        TestUtils.assertEquals(content, sink);
    }

    @Test
    public void testLineComment() {
        GenericLexer lex = new GenericLexer(64);
        lex.defineSymbol("+");
        lex.defineSymbol("++");
        lex.defineSymbol("*");
        lex.defineSymbol("/*");
        lex.defineSymbol("*/");
        lex.defineSymbol("--");

        lex.of("a + -- ok, this is a comment \n 'b' * abc");

        StringSink sink = new StringSink();
        CharSequence token;
        while ((token = SqlUtil.fetchNext(lex)) != null) {
            sink.put(token);
        }

        TestUtils.assertEquals("a+'b'*abc", sink);
    }

    @Test
    public void testNullContent() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol(" ");
        ts.of(null);
        Assert.assertFalse(ts.iterator().hasNext());
    }

    @Test
    public void testQuotedToken() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        ts.of("a+\"b\"*abc");

        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }

        TestUtils.assertEquals("a+\"b\"*abc", sink);
    }

    @Test
    public void testSingleQuotedToken() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        ts.of("a+'b'*abc");

        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }

        TestUtils.assertEquals("a+'b'*abc", sink);
    }

    @Test
    public void testSymbolLookup() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");

        CharSequence content;
        ts.of(content = "+*a+b++blah-");

        StringSink sink = new StringSink();
        for (CharSequence cs : ts) {
            sink.put(cs);
        }
        TestUtils.assertEquals(content, sink);
    }

    @Test
    public void testUnparse() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol("+");
        ts.defineSymbol("++");
        ts.defineSymbol("*");
        ts.of("+*a+b++blah-");

        Iterator<CharSequence> it = ts.iterator();

        while (it.hasNext()) {
            CharSequence e = it.next();
            ts.unparse();
            CharSequence a = it.next();
            TestUtils.assertEquals(e, a);
        }
    }

    @Test
    public void testPeek1() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol(",");
        ts.defineSymbol(" ");
        ts.of("Day-o, day-o");

        Assert.assertEquals("Day-o", ts.next().toString());
        Assert.assertEquals(",", ts.peek().toString());
        Assert.assertEquals(",", ts.next().toString());
        Assert.assertNull(ts.peek());
        Assert.assertEquals(" ", ts.next().toString());
        Assert.assertNull(ts.peek());
        Assert.assertEquals("day-o", ts.next().toString());
        Assert.assertNull(ts.peek());
    }

    @Test
    public void testPeek2() {
        GenericLexer ts = new GenericLexer(64);
        String fortune = "Daylight come and we want go home";
        ts.of(fortune);

        Iterator<CharSequence> it = ts.iterator();
        String [] parts = fortune.split("[ ]");
        for (int i=0; i < parts.length; i++) {
            CharSequence e = it.next();
            Assert.assertEquals(parts[i], e.toString());
            if (i < parts.length - 1) {
                Assert.assertEquals(" ", ts.peek().toString());
                it.next();
            }
        }
        Assert.assertNull(ts.peek());
    }
}
