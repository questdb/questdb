/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;

public class LexerTest {

    @Test
    public void testBlockComments() {
        Lexer lex = new Lexer();
        lex.defineSymbol("+");
        lex.defineSymbol("++");
        lex.defineSymbol("*");
        lex.defineSymbol("/*");
        lex.defineSymbol("*/");

        lex.setContent("a + /* ok, this /* is a */ comment */ 'b' * abc");

        StringSink sink = new StringSink();
        while (lex.hasNext()) {
            sink.put(lex.optionTok());
        }

        TestUtils.assertEquals("a+'b'*abc", sink);
    }

    @Test
    public void testEdgeSymbol() {
        Lexer ts = new Lexer();
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
    public void testLineComment() {
        Lexer lex = new Lexer();
        lex.defineSymbol("+");
        lex.defineSymbol("++");
        lex.defineSymbol("*");
        lex.defineSymbol("/*");
        lex.defineSymbol("*/");
        lex.defineSymbol("--");

        lex.setContent("a + -- ok, this is a comment \n 'b' * abc");

        StringSink sink = new StringSink();
        while (lex.hasNext()) {
            sink.put(lex.optionTok());
        }

        TestUtils.assertEquals("a+'b'*abc", sink);
    }

    @Test
    public void testNullContent() {
        Lexer ts = new Lexer();
        ts.defineSymbol(" ");
        ts.setContent(null);
        Assert.assertFalse(ts.iterator().hasNext());
    }

    @Test
    public void testQuotedToken() {
        Lexer ts = new Lexer();
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
    public void testSingleQuotedToken() {
        Lexer ts = new Lexer();
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
    public void testSymbolLookup() {
        Lexer ts = new Lexer();
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
    @Ignore
    public void testUnicode() throws Exception {
        Lexer lex = new Lexer();
        lex.defineSymbol("+");
        lex.defineSymbol("++");
        lex.defineSymbol("*");

        String s = "'авг'";
        byte[] bb = s.getBytes("UTF8");
        System.out.println(new String(bb));
        long mem = Unsafe.malloc(bb.length);
        for (int i = 0; i < bb.length; i++) {
            Unsafe.getUnsafe().putByte(mem + i, bb[i]);
        }
        DirectByteCharSequence cs = new DirectByteCharSequence();
        cs.of(mem, mem + bb.length);

        lex.setContent(cs);

        StringSink sink = new StringSink();
        while (lex.hasNext()) {
            sink.put(lex.optionTok());
        }

        TestUtils.assertEquals("a+'b'*abc", sink);

    }

    @Test
    public void testUnparse() {
        Lexer ts = new Lexer();
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
