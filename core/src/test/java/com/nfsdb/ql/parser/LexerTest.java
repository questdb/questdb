/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.parser;

import com.nfsdb.io.sink.StringSink;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

public class LexerTest {

    @Test
    public void testBlockComments() throws Exception {
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
    public void testEdgeSymbol() throws Exception {
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
    public void testLineComment() throws Exception {
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
    public void testNullContent() throws Exception {
        Lexer ts = new Lexer();
        ts.defineSymbol(" ");
        ts.setContent(null);
        Assert.assertFalse(ts.iterator().hasNext());
    }

    @Test
    public void testQuotedToken() throws Exception {
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
    public void testSingleQuotedToken() throws Exception {
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
    public void testSymbolLookup() throws Exception {
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
