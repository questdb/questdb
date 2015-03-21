/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.lang;

import com.nfsdb.lang.ast.ExprNode;
import com.nfsdb.lang.parser.NQLParser;
import com.nfsdb.lang.parser.ParserException;
import org.junit.Assert;
import org.junit.Test;

public class NQLParserTest {

    private NQLParser parser = new NQLParser();

    @Test
    public void testCommaExit() throws Exception {
        parser.setContent("a + b * c(b(x,y),z),");
        final String expected = "                                  \n" +
                "                 +                 \n" +
                "         /                 \\       \n" +
                "        a                  *       \n" +
                "                        /      \\   \n" +
                "                       b       c   \n" +
                "                              /  \\ \n" +
                "                             b   z \n" +
                "                             /\\    \n" +
                "                            x y       ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testComplexUnary1() throws Exception {
        parser.setContent("4^-c(x,y)");
        final String expected = "                                  \n" +
                "                 ^                 \n" +
                "         /                 \\       \n" +
                "        4                  -       \n" +
                "                               \\   \n" +
                "                               c   \n" +
                "                              /  \\ \n" +
                "                             x   y \n" +
                "                                   \n" +
                "                                      ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testComplexUnary2() throws Exception {
        parser.setContent("-a^b");
        final String expected = "                                  \n" +
                "                 -                 \n" +
                "                           \\       \n" +
                "                           ^       \n" +
                "                        /      \\   \n" +
                "                       a       b   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                      ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testEqualPrecedence() throws Exception {
        parser.setContent("a^b^c");
        final String expected = "                                  \n" +
                "                 ^                 \n" +
                "         /                 \\       \n" +
                "        a                  ^       \n" +
                "                        /      \\   \n" +
                "                       b       c   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                      ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testLiteralExit() throws Exception {
        parser.setContent("a + b * c(b(x,y),z) lit");
        final String expected = "                                  \n" +
                "                 +                 \n" +
                "         /                 \\       \n" +
                "        a                  *       \n" +
                "                        /      \\   \n" +
                "                       b       c   \n" +
                "                              /  \\ \n" +
                "                             b   z \n" +
                "                             /\\    \n" +
                "                            x y       ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testMissingArgAtBraceError() throws Exception {
        parser.setContent("x * 4 + c(x,y,)");
        try {
            render(parser.parseExpr());
            Assert.fail("Expected syntax exception");
        } catch (ParserException e) {
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testMissingArgError() throws Exception {
        parser.setContent("x * 4 + c(x,,y)");
        try {
            render(parser.parseExpr());
            Assert.fail("Expected syntax exception");
        } catch (ParserException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testNestedFunctions() throws Exception {
        parser.setContent("z(4, r(f(8,y)))");
        final String expected = "                                  \n" +
                "                 z                 \n" +
                "         /                 \\       \n" +
                "        4                  r       \n" +
                "                               \\   \n" +
                "                               f   \n" +
                "                              /  \\ \n" +
                "                             8   y \n" +
                "                                   \n" +
                "                                      ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testNestedOperator() throws Exception {
        parser.setContent("a + b( c * 4, d)");
        final String expected = "                                  \n" +
                "                 +                 \n" +
                "         /                 \\       \n" +
                "        a                  b       \n" +
                "                        /      \\   \n" +
                "                       *       d   \n" +
                "                      /  \\         \n" +
                "                     c   4         \n" +
                "                                   \n" +
                "                                      ";

        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testNoArgFunction() throws Exception {
        parser.setContent("a+b()*4");
        final String expected = "                                  \n" +
                "                 +                 \n" +
                "         /                 \\       \n" +
                "        a                  *       \n" +
                "                        /      \\   \n" +
                "                       b       4   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                      ";

        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testSimple() throws Exception {
        parser.setContent("a + b * c(x,y)/2");
        final String expected = "                                  \n" +
                "                 +                 \n" +
                "         /                 \\       \n" +
                "        a                  /       \n" +
                "                        /      \\   \n" +
                "                       *       2   \n" +
                "                      /  \\         \n" +
                "                     b   c         \n" +
                "                         /\\        \n" +
                "                        x y           ";

        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testUnary() throws Exception {
        parser.setContent("4 * -c");
        final String expected = "                                  \n" +
                "                 *                 \n" +
                "         /                 \\       \n" +
                "        4                  -       \n" +
                "                               \\   \n" +
                "                               c   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                   \n" +
                "                                      ";
        Assert.assertEquals(expected, render(parser.parseExpr()));
    }

    @Test
    public void testUnbalancedLeftBrace() throws Exception {
        parser.setContent("a+b(5,c(x,y)");
        try {
            render(parser.parseExpr());
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(e.getPosition(), 2);
        }
    }

    @Test
    public void testUnbalancedRightBrace() throws Exception {
        parser.setContent("a+b(5,c(x,y)))");
        try {
            System.out.println(render(parser.parseExpr()));
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(e.getPosition(), 13);
        }
    }

    private String render(ExprNode root) {
        final int height = 5, width = 36;

        int len = width * height * 2 + 2;
        StringBuilder sb = new StringBuilder(len);
        for (int i = 1; i <= len; i++)
            sb.append(i < len - 2 && i % width == 0 ? "\n" : ' ');

        render0(sb, width / 2, 1, width / 4, width, root, " ");
        return sb.toString();
    }

    private void render0(StringBuilder sb, int c, int r, int d, int w, ExprNode n, String edge) {
        if (n != null) {
            render0(sb, c - d, r + 2, d / 2, w, n.lhs, " /");

            String s = n.token;
            int idx1 = r * w + c - (s.length() + 1) / 2;
            int idx2 = idx1 + s.length();
            int idx3 = idx1 - w;
            if (idx2 < sb.length())
                sb.replace(idx1, idx2, s).replace(idx3, idx3 + 2, edge);

            render0(sb, c + d, r + 2, d / 2, w, n.rhs, "\\ ");
        }
    }
}
