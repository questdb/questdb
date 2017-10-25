/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.parser.sql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.model.ExprNode;
import com.questdb.std.Lexer;
import com.questdb.std.ObjectPool;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExprParserTest {

    private final ObjectPool<ExprNode> exprNodeObjectPool = new ObjectPool<>(ExprNode.FACTORY, 128);
    private final Lexer lexer = new Lexer();
    private final ExprParser parser = new ExprParser(exprNodeObjectPool);

    @Before
    public void setUp() {
        exprNodeObjectPool.clear();
        ExprParser.configureLexer(lexer);
    }

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
    public void testIn() throws Exception {
        x("abcin", "a in (b,c)");
    }

    @Test
    public void testInOperator() throws Exception {
        x("a10=bxyinand", "a = 10 and b in (x,y)");
    }

    @Test
    public void testLambda() throws Exception {
        x("a`blah blah`inyand", "a in (`blah blah`) and y");
    }

    @Test
    public void testLiteralAndConstant() throws Exception {
        x("'a b'x", "x 'a b'");
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
            Assert.assertEquals(14, QueryError.getPosition());
        }
    }

    @Test
    public void testMissingArgError() throws Exception {
        try {
            x("", "x * 4 + c(x,,y)");
            Assert.fail("Expected syntax exception");
        } catch (ParserException e) {
            Assert.assertEquals(12, QueryError.getPosition());
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
    public void testSimpleLiteralExit() throws Exception {
        x("a", "a lit");
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
            Assert.assertEquals(3, QueryError.getPosition());
        }
    }

    @Test
    public void testUnbalancedRightBraceExit() throws Exception {
        x("a5xycb+", "a+b(5,c(x,y)))");
    }

    private void x(CharSequence expectedRpn, String content) throws ParserException {
        RpnBuilder r = new RpnBuilder();
        lexer.setContent(content);
        parser.parseExpr(lexer, r);
        TestUtils.assertEquals(expectedRpn, r.rpn());
    }
}
