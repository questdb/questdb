/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.CairoEngine;
import com.questdb.std.Chars;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class ExpressionParserTest extends AbstractCairoTest {
    private final static SqlCompiler compiler = new SqlCompiler(new CairoEngine(configuration));
    private final static RpnBuilder rpnBuilder = new RpnBuilder();

    @Test
    public void testBinaryMinus() throws Exception {
        x("4c-", "4-c");
    }

    @Test
    public void testCaseInArithmetic() throws SqlException {
        x("w11+10='th1'w23*1>'th2'0case5*1+",
                "case" +
                        " when w1+1=10" +
                        " then 'th1'" +
                        " when w2*3>1" +
                        " then 'th2'" +
                        " else 0" +
                        " end * 5 + 1");
    }

    @Test
    public void testCaseInFunction() throws SqlException {
        x(
                "xyab+10>'a'ab-3<'b'0case10+zf*",
                "x*f(y,case when (a+b) > 10 then 'a' when (a-b)<3 then 'b' else 0 end + 10,z)");
    }

    @Test
    public void testCaseMisplacedElse() {
        assertFail(
                "case when x else y end",
                12,
                "'then' expected"
        );
    }

    @Test
    @Ignore
    //todo: fix sql parser for PG OPERATOR
    public void testPGOperator() throws SqlException {
        x("", "c.relname ~= E'^(movies\\\\.csv)$'");
    }

    @Test
    public void testCaseMissingElseArg() {
        assertFail(
                "case when a > b then 1 else end",
                28,
                "missing arguments"
        );
    }

    @Test
    public void testCaseMissingThen() {
        assertFail(
                "case when x when y end",
                12,
                "'then' expected"
        );
    }

    @Test
    public void testCaseMissingThenArg() {
        assertFail(
                "case when a > b then else 2 end",
                21,
                "missing arguments"
        );
    }

    @Test
    public void testCaseMissingWhen() {
        assertFail(
                "case then x end",
                5,
                "'when' expected"
        );
    }

    @Test
    public void testCaseMissingWhenArg() {
        assertFail(
                "case when then 1 else 2 end",
                10,
                "missing arguments"
        );
    }

    @Test
    public void testCaseNested() throws SqlException {
        x("x0>y1='a''b'casex0<'c'case",
                "case when x > 0 then case when y = 1 then 'a' else 'b' end when x < 0 then 'c' end");
    }

    @Test
    public void testCaseUnbalanced() {
        assertFail(
                "case when x then y",
                0,
                "unbalanced 'case'"
        );
    }

    @Test
    public void testCaseWhenOutsideOfCase() throws SqlException {
        x("when1+10>",
                "when + 1 > 10"
        );
    }

    @Test
    public void testCaseWithArithmetic() throws SqlException {
        x("w11+10='th1'w23*1>'th2'0case",
                "case" +
                        " when w1+1=10" +
                        " then 'th1'" +
                        " when w2*3>1" +
                        " then 'th2'" +
                        " else 0" +
                        " end");
    }

    @Test
    public void testCaseWithBraces() throws SqlException {
        x("10w11+5*10='th1'1-w23*1>'th2'0case1+*",
                "10*(case" +
                        " when (w1+1)*5=10" +
                        " then 'th1'-1" +
                        " when w2*3>1" +
                        " then 'th2'" +
                        " else 0" +
                        " end + 1)");
    }

    @Test
    public void testCaseWithOuterBraces() throws SqlException {
        x("10w11+10='th1'w23*1>'th2'0case1+*",
                "10*(case" +
                        " when w1+1=10" +
                        " then 'th1'" +
                        " when w2*3>1" +
                        " then 'th2'" +
                        " else 0" +
                        " end + 1)");
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
        // expect that expression parser will stop after literal, because litral followed by constant does not
        // make sense
        x("x", "x 'a b'");
    }

    @Test
    public void testLiteralExit() throws Exception {
        x("abxybzc*+", "a + b * c(b(x,y),z) lit");
    }

    @Test
    public void testMissingArgAtBraceError() {
        assertFail("x * 4 + c(x,y,)", 14, "missing arguments");
    }

    @Test
    public void testMissingArgError() {
        assertFail("x * 4 + c(x,,y)", 12, "missing arguments");
    }

    @Test
    public void testMissingArgError2() {
        assertFail("x * 4 + c(,x,y)", 10, "missing arguments");
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
    public void testNewLambda() throws SqlException {
        x("x(select-choose a from (tab))in", "x in (select a from tab)");
    }

    @Test
    public void testNewLambdaMultipleExpressions() throws SqlException {
        x("x(select-choose a from (tab))iny(select-choose * column from (X))inandk10>and", "x in (select a from tab) and y in (select * from X) and k > 10");
    }

    @Test
    public void testNewLambdaNested() throws SqlException {
        x("x(select-choose a, b from (T where c in (select-choose * column from (Y)) and a = b))in", "x in (select a,b from T where c in (select * from Y) and a=b)");
    }

    @Test
    public void testNewLambdaQuerySyntax() {
        assertFail("x in (select a,b, from T)", 23, "column name expected");
    }

    @Test
    public void testNewLambdaUnbalancedBrace() {
        assertFail("x in (select a,b from T", 23, "')' expected");
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
    public void testSimpleCase() throws SqlException {
        x("w1th1w2th2elscase", "case when w1 then th1 when w2 then th2 else els end");
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
    public void testUnbalancedLeftBrace() {
        assertFail("a+b(5,c(x,y)", 3, "unbalanced");
    }

    @Test
    public void testUnbalancedRightBraceExit() throws Exception {
        x("a5xycb+", "a+b(5,c(x,y)))");
    }

    @Test
    public void testWhacky() throws Exception {
        x("ab^-", "a-^b");
    }

    private void assertFail(String content, int pos, String contains) {
        try {
            compiler.testParseExpression(content, rpnBuilder);
            Assert.fail("expected exception");
        } catch (SqlException e) {
            Assert.assertEquals(pos, e.getPosition());
            if (!Chars.contains(e.getFlyweightMessage(), contains)) {
                Assert.fail(e.getMessage() + " does not contain '" + contains + '\'');
            }
        }
    }

    private void x(CharSequence expectedRpn, String content) throws SqlException {
        rpnBuilder.reset();
        compiler.testParseExpression(content, rpnBuilder);
        TestUtils.assertEquals(expectedRpn, rpnBuilder.rpn());
    }

}