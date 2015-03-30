/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.lang.parser;

import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ExprParserTest {

    private final ExprParser parser = new ExprParser();

    @Test
    public void testCommaExit() throws Exception {
        x("abxybzc*+", "a + b * c(b(x,y),z),");
    }

    @Test
    public void testComplexUnary1() throws Exception {
        x("4xyc-^", "4^-c(x,y)");
    }

    @Test
    public void testComplexUnary2() throws Exception {
        x("ab^-", "-a^b");
    }

    @Test
    public void testEqualPrecedence() throws Exception {
        x("abc^^", "a^b^c");
    }

    @Test
    public void testExprCompiler() throws Exception {
        x("ac098dzf+1234x+", "a+f(c,d(0,9,8),z)+x(1,2,3,4)");
    }

    @Test
    public void testLiteralExit() throws Exception {
        x("abxybzc*+", "a + b * c(b(x,y),z) lit");
    }

    @Test
    public void testMissingArgAtBraceError() throws Exception {
        try {
            x("", "x * 4 + c(x,y,)");
            Assert.fail("Expected syntax exception");
        } catch (ParserException e) {
            Assert.assertEquals(14, e.getPosition());
        }
    }

    @Test
    public void testMissingArgError() throws Exception {
        try {
            x("", "x * 4 + c(x,,y)");
            Assert.fail("Expected syntax exception");
        } catch (ParserException e) {
            Assert.assertEquals(12, e.getPosition());
        }
    }

    @Test
    public void testNestedFunctions() throws Exception {
        x("48yfrz", "z(4, r(f(8,y)))");
    }

    @Test
    public void testNestedOperator() throws Exception {
        x("ac4*db+", "a + b( c * 4, d)");
    }

    @Test
    public void testNoArgFunction() throws Exception {
        x("ab4*+", "a+b()*4");
    }

    @Test
    public void testSimple() throws Exception {
        x("abxyc*2/+", "a + b * c(x,y)/2");
    }

    @Test
    public void testUnary() throws Exception {
        x("4c-*", "4 * -c");
    }

    @Test
    public void testUnbalancedLeftBrace() throws Exception {
        try {
            x("", "a+b(5,c(x,y)");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(e.getPosition(), 2);
        }
    }

    @Test
    public void testUnbalancedRightBrace() throws Exception {
        try {
            x("", "a+b(5,c(x,y)))");
            Assert.fail("Expected exception");
        } catch (ParserException e) {
            Assert.assertEquals(e.getPosition(), 13);
        }
    }

    private void x(CharSequence expectedRpn, String content) throws ParserException {
        RpnBuilder r = new RpnBuilder();
        parser.parseExpr(content, r);
        TestUtils.assertEquals(expectedRpn, r.rpn());
    }
}
