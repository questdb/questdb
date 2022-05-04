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

package io.questdb.std;

import io.questdb.griffin.SqlException;
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
    public void testBrokenSingleQuotedToken1() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("#1234'");
        Assert.assertEquals("#1234", ts.next().toString());
        Assert.assertEquals("'", ts.next().toString());

        ts.of("#1234\"");
        Assert.assertEquals("#1234", ts.next().toString());
        Assert.assertEquals("\"", ts.next().toString());

        ts.of("#1234`");
        Assert.assertEquals("#1234", ts.next().toString());
        Assert.assertEquals("`", ts.next().toString());
    }

    @Test
    public void testBrokenSingleQuotedToken2() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("'#1234");
        Assert.assertEquals("'", ts.next().toString());
        Assert.assertEquals("#1234", ts.next().toString());

        ts.of("\"#1234");
        Assert.assertEquals("\"", ts.next().toString());
        Assert.assertEquals("#1234", ts.next().toString());

        ts.of("`#1234");
        Assert.assertEquals("`", ts.next().toString());
        Assert.assertEquals("#1234", ts.next().toString());
    }

    @Test
    public void testBrokenSingleQuotedToken3() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("#12'34");
        Assert.assertEquals("#12", ts.next().toString());
        Assert.assertEquals("'", ts.next().toString());
        Assert.assertEquals("34", ts.next().toString());

        ts.of("#12\"34");
        Assert.assertEquals("#12", ts.next().toString());
        Assert.assertEquals("\"", ts.next().toString());
        Assert.assertEquals("34", ts.next().toString());

        ts.of("#12`34");
        Assert.assertEquals("#12", ts.next().toString());
        Assert.assertEquals("`", ts.next().toString());
        Assert.assertEquals("34", ts.next().toString());
    }

    @Test
    public void testBrokenSingleQuotedToken4() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("'");
        Assert.assertEquals("'", ts.next().toString());

        ts.of("\"");
        Assert.assertEquals("\"", ts.next().toString());

        ts.of("`");
        Assert.assertEquals("`", ts.next().toString());
    }

    @Test
    public void testSingleQuotedToken4() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("''");
        Assert.assertEquals("''", ts.next().toString());

        ts.of("\"\"");
        Assert.assertEquals("\"\"", ts.next().toString());

        ts.of("``");
        Assert.assertEquals("``", ts.next().toString());
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
            ts.unparseLast();
            CharSequence a = it.next();
            TestUtils.assertEquals(e, a);
        }
    }

    @Test
    public void testPeek1() {
        GenericLexer ts = new GenericLexer(64);
        ts.defineSymbol(",");
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
        String[] parts = fortune.split("[ ]");
        for (int i = 0; i < parts.length; i++) {
            CharSequence e = it.next();
            Assert.assertEquals(parts[i], e.toString());
            if (i < parts.length - 1) {
                Assert.assertEquals(" ", ts.peek().toString());
                it.next();
            }
        }
        Assert.assertNull(ts.peek());
    }

    @Test
    public void testImmutableOf() {
        Assert.assertTrue(GenericLexer.immutableOf("immutable") instanceof String);
        GenericLexer ts = new GenericLexer(64);
        ts.of("cantaloupe");
        CharSequence tok = ts.next();
        Assert.assertTrue(tok instanceof GenericLexer.InternalFloatingSequence);
        Assert.assertTrue(GenericLexer.immutableOf(tok) instanceof GenericLexer.FloatingSequence);
    }

    @Test(expected = SqlException.class)
    public void testAssertNoDot1() throws SqlException {
        GenericLexer.assertNoDots(".", 0);
    }

    @Test(expected = SqlException.class)
    public void testAssertNoDot2() throws SqlException {
        GenericLexer.assertNoDots("..", 0);
    }

    @Test
    public void testAssertNoDot3() throws SqlException {
        Assert.assertEquals(",", GenericLexer.assertNoDots(",", 0));
    }

    @Test(expected = SqlException.class)
    public void testAssertNoDotAndSlashes1() throws SqlException {
        GenericLexer.assertNoDotsAndSlashes(".", 0);
    }

    @Test(expected = SqlException.class)
    public void testAssertNoDotAndSlashes2() throws SqlException {
        GenericLexer.assertNoDotsAndSlashes("/.", 0);
    }

    @Test
    public void testAssertNoDotAndSlashes3() throws SqlException {
        Assert.assertEquals(",", GenericLexer.assertNoDotsAndSlashes(",", 0));
    }

    @Test
    public void testUnquote() {
        Assert.assertEquals(GenericLexer.unquote("QuestDB"), GenericLexer.unquote("'QuestDB'"));
    }

    @Test
    public void testImmutablePairOf1() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("orange");
        CharSequence cs = ts.next();
        ts.immutablePairOf(GenericLexer.immutableOf(cs), cs);
        Assert.assertEquals("orange", ts.getContent());
        Assert.assertFalse(ts.hasUnparsed());
        Assert.assertEquals(6, ts.getPosition());
        Assert.assertEquals(6, ts.getTokenHi());
    }

    @Test
    public void testStashUnStash() {
        GenericLexer lexer = new GenericLexer(64);
        lexer.of("orange blue yellow green");
        TestUtils.assertEquals("orange", lexer.next());

        CharSequence blue = GenericLexer.immutableOf(SqlUtil.fetchNext(lexer));
        int blueLast = lexer.lastTokenPosition();
        int bluePos = lexer.getPosition();

        TestUtils.assertEquals("blue", blue);
        Assert.assertEquals(7, blueLast);
        Assert.assertEquals(12, bluePos);

        CharSequence yellow = SqlUtil.fetchNext(lexer);
        int yellowLast = lexer.lastTokenPosition();
        int yellowPos = lexer.getPosition();

        TestUtils.assertEquals("yellow", yellow);
        Assert.assertEquals(12, yellowLast);
        Assert.assertEquals(19, yellowPos);

        lexer.unparse(blue, blueLast, bluePos);
        lexer.unparseLast();

        lexer.stash();

        CharSequence green = SqlUtil.fetchNext(lexer);
        TestUtils.assertEquals("green", green);

        Assert.assertNull(SqlUtil.fetchNext(lexer));

        lexer.unstash();

        blue = SqlUtil.fetchNext(lexer);
        blueLast = lexer.lastTokenPosition();
        bluePos = lexer.getPosition();

        TestUtils.assertEquals("blue", blue);
        Assert.assertEquals(7, blueLast);
        Assert.assertEquals(12, bluePos);

        yellow = SqlUtil.fetchNext(lexer);
        yellowLast = lexer.lastTokenPosition();
        yellowPos = lexer.getPosition();

        TestUtils.assertEquals("yellow", yellow);
        Assert.assertEquals(12, yellowLast);
        Assert.assertEquals(19, yellowPos);


        green = SqlUtil.fetchNext(lexer);
        TestUtils.assertEquals("green", green);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testImmutablePairOf2() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("orange");
        CharSequence cs = ts.next();
        ts.immutablePairOf(cs, cs);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testImmutablePairOf3() {
        GenericLexer ts = new GenericLexer(64);
        ts.immutablePairOf("", "");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testImmutablePairOf4() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("geohash 31b");
        ts.immutablePairOf(GenericLexer.immutableOf(ts.next()), ts.next());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testImmutablePairOf5() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("geohash 31b");
        CharSequence tok0 = GenericLexer.immutableOf(ts.next());
        ts.next();
        CharSequence pair = ts.immutablePairOf(tok0, ts.next());
        pair.subSequence(0, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testImmutablePairOf6() {
        GenericLexer ts = new GenericLexer(64);
        ts.of("geohash 31b");
        CharSequence tok0 = GenericLexer.immutableOf(ts.next());
        ts.next();
        GenericLexer.FloatingSequencePair pair = (GenericLexer.FloatingSequencePair) ts.immutablePairOf(tok0, ts.next());
        pair.charAt(17_000);
    }

    @Test
    public void testImmutablePairOf7() {
        GenericLexer ts = new GenericLexer(64);
        String culprit = "geohash 31b";
        ts.of(culprit);
        CharSequence tok0 = GenericLexer.immutableOf(ts.next());
        ts.next();
        CharSequence tok1 = ts.next();
        GenericLexer.FloatingSequencePair pair = (GenericLexer.FloatingSequencePair) ts.immutablePairOf(tok0, tok1);
        Assert.assertEquals(culprit.length() - 1, pair.length());
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int i = 0; i < pair.length(); i++) {
            sink.put(pair.charAt(i));
        }
        pair.clear();
        Assert.assertEquals(pair.toString(), sink.toString());
    }

    @Test
    public void testImmutablePairOf8() {
        GenericLexer lex = new GenericLexer(64);
        lex.defineSymbol("/");
        String culprit = "#sp052w92p1p8/7";
        lex.of(culprit);
        CharSequence geohashTok = GenericLexer.immutableOf(lex.next());
        lex.next(); // slash
        CharSequence bitsTok = lex.next();
        GenericLexer.FloatingSequencePair pair = (GenericLexer.FloatingSequencePair)
                lex.immutablePairOf(geohashTok, '/', bitsTok);
        Assert.assertEquals(culprit, pair.toString());
        StringSink sink = Misc.getThreadLocalBuilder();
        for (int i = 0; i < pair.length(); i++) {
            sink.put(pair.charAt(i));
        }
        pair.clear();
        Assert.assertEquals(pair.toString(), sink.toString());
    }
}
