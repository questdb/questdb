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

package io.questdb.griffin;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.CairoEngine;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionParserTest extends AbstractCairoTest {
    private final static SqlCompiler compiler = new SqlCompiler(new CairoEngine(configuration));
    private final static RpnBuilder rpnBuilder = new RpnBuilder();

    @Test
    public void testAllNotEqual() throws SqlException {
        x("ab<>all", "a <> all(b)");
        x("ab<>all", "a != all(b)");
    }

    @Test
    public void testIsGeoHashBitsConstantValid() {
        Assert.assertTrue(ExpressionParser.isGeoHashBitsConstant("##0"));
        Assert.assertTrue(ExpressionParser.isGeoHashBitsConstant("##1"));
        Assert.assertTrue(ExpressionParser.isGeoHashBitsConstant("##111111111100000000001111111111000000000011111111110000000000"));
    }

    @Test
    public void testIsGeoHashBitsConstantNotValid() {
        Assert.assertFalse(ExpressionParser.isGeoHashBitsConstant("#00110")); // missing '#'
        Assert.assertFalse(ExpressionParser.isGeoHashBitsConstant("#0")); // missing '#'
    }

    @Test
    public void testIsGeoHashCharsConstantValid() {
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#0"));
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#1"));
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#sp"));
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#sp052w92p1p8"));
    }

    @Test
    public void testIsGeoHashCharsConstantNotValid() {
        Assert.assertFalse(ExpressionParser.isGeoHashCharsConstant("##"));
    }

    @Test
    public void testExtractGeoHashBitsSuffixZero() {
        Assert.assertThrows("", SqlException.class, () -> ExpressionParser.extractGeoHashSuffix(0, "#/0"));
        Assert.assertThrows("", SqlException.class, () -> ExpressionParser.extractGeoHashSuffix(0, "#/00"));
    }

    @Test
    public void testExtractGeoHashBitsSuffixValid() throws SqlException {
        for (int bits = 1; bits < 10; bits++) {
            Assert.assertEquals(
                    Numbers.encodeLowHighShorts((short) 2, (short) bits),
                    ExpressionParser.extractGeoHashSuffix(0, "#/" + bits)); // '/d'
        }
        for (int bits = 1; bits < 10; bits++) {
            Assert.assertEquals(
                    Numbers.encodeLowHighShorts((short) 3, (short) bits),
                    ExpressionParser.extractGeoHashSuffix(0, "#/0" + bits)); // '/0d'
        }
        for (int bits = 10; bits <= 60; bits++) {
            Assert.assertEquals(
                    Numbers.encodeLowHighShorts((short) 3, (short) bits),
                    ExpressionParser.extractGeoHashSuffix(0, "#/" + bits)); // '/dd'
        }
    }

    @Test
    public void testExtractGeoHashBitsSuffixNoSuffix() throws SqlException {
        for (String tok : new String[]{"#", "#/", "#p", "#pp", "#ppp", "#0", "#01", "#001"}) {
            Assert.assertEquals(
                    Numbers.encodeLowHighShorts((short) 0, (short) (5 * (tok.length() - 1))),
                    ExpressionParser.extractGeoHashSuffix(0, tok));
        }
        for (String tok : new String[]{"#/x", "#/1x", "#/x1", "#/xx", "#/-1",}) {
            Assert.assertThrows("[0] invalid bits size for GEOHASH constant",
                    SqlException.class, () -> ExpressionParser.extractGeoHashSuffix(0, tok));
        }
    }

    @Test
    public void testAllInvalidOperator() {
        assertFail(
                "a || all(b)",
                2,
                "unexpected operator"
        );
    }

    @Test
    public void testUnquotedRegexFail() {
        assertFail(
                "s ~ '.*TDF",
                4,
                "unclosed quoted string?"
        );
    }

    @Test
    public void testUnquotedStrFail() {
        assertFail(
                "s ~ 'TDF",
                4,
                "unclosed quoted string?"
        );
    }

    @Test
    public void testAllNoOperator() {
        assertFail(
                "a all(b)",
                2,
                "missing operator"
        );
    }

    @Test
    public void testArrayDereferenceExpr() throws SqlException {
        x("ai10+[]", "a[i+10]");
    }

    @Test
    public void testArrayDereferenceMissingIndex() {
        assertFail(
                "a[]",
                2,
                "missing array index"
        );
    }

    @Test
    public void testArrayDereferenceNotClosedEndOfExpression() {
        assertFail(
                "a[",
                1,
                "unbalanced ]"
        );
    }

    @Test
    public void testArrayDereferenceNotClosedEndOfFunctionCall() {
        assertFail(
                "f(a[)",
                3,
                "unbalanced ]"
        );
    }

    @Test
    public void testArrayDereferenceNotClosedFunctionArg() {
        assertFail(
                "f(b,a[,c)",
                5,
                "unbalanced ]"
        );
    }

    @Test
    public void testArrayDereferencePriority() throws SqlException {
        x(
                "nspname'pg_toast'<>nspname'^pg_temp_'!~nspnametruepg_catalog.current_schemas1[]=orand",
                "nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1])"
        );
    }

    @Test
    public void testBetweenConstantAndSelect() {
        assertFail(
                "x between select and 10",
                10,
                "constant expected"
        );
    }

    @Test
    public void testBinaryMinus() throws Exception {
        x("4c-", "4-c");
    }

    @Test
    public void testNotIn() throws Exception {
        x("x'a''b'innot", "x not in ('a','b')");
    }

    @Test
    public void testNotInReverseContext() throws Exception {
        x("ax'a''b'innotand", "a and not x in ('a','b')");
    }

    @Test
    public void testIsNull() throws SqlException {
        x("aNULL=", "a IS NULL");
        x("tab.aNULL=", "tab.a IS NULL");
        x("3NULL=", "3 IS NULL");
        x("nullNULL=", "null IS NULL");
        x("NULLNULL=", "NULL IS NULL");
        x("'null'NULL=", "'null' IS NULL");
        x("''null|NULL=", "('' | null) IS NULL");
        assertFail("column is 3", 7, "IS must be followed by NULL");
        assertFail(". is great", 2, "IS [NOT] not allowed here");
        assertFail("column is $1", 7, "IS must be followed by NULL");
        assertFail("column is", 7, "IS must be followed by [NOT] NULL");
    }

    @Test
    public void testIsNotNull() throws SqlException {
        x("aNULL!=", "a IS NOT NULL");
        x("tab.aNULL!=", "tab.a IS NOT NULL");
        x("3NULL!=", "3 IS NOT NULL");
        x("nullNULL!=", "null IS NOT NULL");
        x("NULLNULL!=", "NULL IS NOT NULL");
        x("'null'NULL!=", "'null' IS NOT NULL");
        x("''null||NULL!=", "('' || null) IS NOT NULL");
        assertFail("column is not 3", 7, "IS NOT must be followed by NULL");
        assertFail(". is not great", 2, "IS [NOT] not allowed here");
        assertFail("column is not $1", 7, "IS NOT must be followed by NULL");
        assertFail("column is not", 7, "IS NOT must be followed by NULL");
    }

    @Test
    public void testBug1() throws SqlException {
        x("2022.yyyy", "'2022'.'yyyy'");
    }

    @Test
    public void testCannotConsumeArgumentOutsideOfBrace() {
        assertFail("a+(*b)", 3, "too few arguments for '*' [found=1,expected=2]");
    }

    @Test
    public void testStringConcat() throws SqlException {
        x("a'b'||c||d||", "a||'b'||c||d");
    }

    @Test
    public void testCaseAsArrayIndex() throws SqlException {
        x(
                "ab134case[]",
                "a[case b when 1 then 3 else 4 end]"
        );
    }

    @Test
    public void testCaseAsArrayIndexAnotherArray() throws SqlException {
        x(
                "ab1b3[]4case[]",
                "a[case b when 1 then b[3] else 4 end]"
        );
    }

    @Test
    public void testCaseDanglingBrace() {
        assertFail(
                "1 + (case x when 1 then 'a') when 2 then 'b' end",
                5,
                "unbalanced 'case'"
        );
    }

    @Test
    public void testCaseDanglingOperatorAfterCase() {
        assertFail(
                "1 + case *x when 1 then 'a' when 2 then 'b' end",
                9,
                "too few arguments for '*' [found=1,expected=2]"
        );
    }

    @Test
    public void testCaseDanglingOperatorAfterElse() {
        assertFail(
                "1 + case x when 1 then 'a' when 2 then 'b' else * end",
                48,
                "too few arguments for '*' [found=0,expected=2]"
        );
    }

    @Test
    public void testCaseDanglingOperatorAfterThen() {
        assertFail(
                "1 + case x when 1 then +'a' when 2 then 'b' end",
                23,
                "too few arguments for '+' [found=1,expected=2]"
        );
    }

    @Test
    public void testCaseDanglingOperatorAfterWhen() {
        assertFail(
                "1 + case x when 1* then 'a' when 2 then 'b' end",
                17,
                "too few arguments for '*' [found=1,expected=2]"
        );
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
                "x*f(y,case when (a+b) > 10 then 'a' when (a-b)<3 then 'b' else 0 end + 10,z)"
        );
    }

    @Test
    public void testTypeQualifier() throws SqlException {
        x("'hello'something::", "'hello'::something");
    }

    @Test
    public void testTextArrayQualifier() throws SqlException {
        x("'{hello}'text[]::", "'{hello}'::text[]");
    }

    @Test
    public void testFloatQualifier() throws SqlException {
        x("'NaN'float::", "'NaN'::float");
    }

    @Test
    public void testCaseLikeSwitch() throws SqlException {
        x("x1'a'2'b'case", "case x when 1 then 'a' when 2 then 'b' end");
    }

    @Test
    public void testCaseLowercase() throws SqlException {
        x("10w11+5*10='th1'1-w23*1>'th2'0case1+*",
                "10*(CASE" +
                        " WHEN (w1+1)*5=10" +
                        " THEN 'th1'-1" +
                        " WHEN w2*3>1" +
                        " THEN 'th2'" +
                        " ELSE 0" +
                        " END + 1)");
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
    public void testCaseWithDanglingCast() {
        assertFail(
                "case (cast 1 as int)",
                11,
                "dangling expression"
        );
    }

    @Test
    public void testCaseWithCast() throws SqlException {
        x("1intcast1'th1'2'th2'0case5*1+",
                "case (cast(1 as int))" +
                        " when 1" +
                        " then 'th1'" +
                        " when 2" +
                        " then 'th2'" +
                        " else 0" +
                        " end * 5 + 1");
    }

    @Test
    public void testCastFunctionCallMultiSpace() throws SqlException {
        x("1102030f+shortcast", "cast\t --- this is a comment\n\n(1+f(10,20,30) as short\n)");
    }

    @Test
    public void testCastFunctionWithLambdaMultiSpaceNewlineAndComment() throws SqlException {
        x("(select-choose a, b, c from (x))flongcast", "cast    --- this is a comment\n\n(f(select a,b,c from x) as long\n)");
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
    public void testCastFunctionCall() throws SqlException {
        x("1102030f+shortcast", "cast(1+f(10,20,30) as short)");
    }

    @Test
    public void testCastFunctionWithLambda() throws SqlException {
        x("(select-choose a, b, c from (x))flongcast", "cast(f(select a,b,c from x) as long)");
    }

    @Test
    public void testCastGeoHashCastStrWithCharsPrecision() throws SqlException {
        x("'sp052w92'geohash6ccast", "cast('sp052w92' as geohash(6c))");
    }

    @Test
    public void testCastGeoHashCastStrWithBitsPrecision() throws SqlException {
        x("'sp052w92'geohash60bcast", "cast('sp052w92' as geohash(60b))");
    }

    @Test
    public void testCastGeoHashCastMissingSize1() {
        assertFail("cast('sp052w92' as geohash())",
                27,
                "invalid GEOHASH, invalid type precision");
    }

    @Test
    public void testCastGeoHashCastMissingSize2() {
        assertFail("cast('sp052w92' as geohash)",
                19,
                "invalid type");
    }

    @Test
    public void testCastGeoHashCastMissingSize3() {
        assertFail("cast('sp052w92' as geohash(21b)",
                4,
                "unbalanced (");
    }

    @Test
    public void testCastGeoHashCastMissingSize6() {
        assertFail("cast('sp052w92' as geohash(8c",
                27,
                "invalid GEOHASH, missing ')'");
    }

    @Test
    public void testGeoHash1() throws SqlException {
        x("geohash6c", "geohash(6c)");
    }

    @Test
    public void testGeoHash2() throws SqlException {
        x("geohash31b", "geohash(31b)");
    }

    @Test
    public void testGeoHash3() throws SqlException {
        x("GEOHASH", "GEOHASH");
    }

    @Test
    public void testGeoHash4() throws SqlException {
        x("geohash6c", "geohash ( 6c" +
                "-- this is a comment, as you can see" +
                "\n\n\r)");
    }

    @Test
    public void testGeoHash5() throws SqlException {
        x("geohash6c", " geohash\r\n  (\n 6c\n" +
                "-- this is a comment, as you can see" +
                "\n\n\r)-- my tralala");
    }

    @Test
    public void testGeoHashFail1() {
        assertFail("GEOHASH(",
                7,
                "invalid GEOHASH, invalid type precision");
    }

    @Test
    public void testGeoHashFail2() {
        assertFail("GEOHASH()",
                8,
                "invalid GEOHASH, invalid type precision");
    }

    @Test
    public void testGeoHashConstantValid() throws SqlException {
        x("#sp052w92p1p8/7", " #sp052w92p1p8\r\n  / 7\n 6c\n" +
                "-- this is a comment, as you can see" +
                "\n\n\r-- my tralala");
        x("#sp052w92p1p8/7", "#sp052w92p1p8 / 7");
        x("#sp052w92p1p8", "#sp052w92p1p8");
        x("#sp052w92p1p8/0", "#sp052w92p1p8 / 0"); // valid at the expression level
        x("#sp052w92p1p8/61", "#sp052w92p1p8 / 61"); // valid at the expression level
    }

    @Test
    public void testGeoHashConstantNotValid() {
        assertFail("#sp052w92p1p8/", 13, "missing bits size for GEOHASH constant");
        assertFail("#sp052w92p1p8/x", 14, "missing bits size for GEOHASH constant");
        assertFail("#sp052w92p1p8/xx", 14, "missing bits size for GEOHASH constant");
        assertFail("#sp052w92p1p8/1x", 14, "missing bits size for GEOHASH constant");
        assertFail("#sp052w92p1p8/x1", 14, "missing bits size for GEOHASH constant");
    }

    @Test
    public void testCastLambda() throws SqlException {
        x("(select-choose a, b, c from (x))1+longcast", "cast((select a,b,c from x)+1 as long)");
    }

    @Test
    public void testCastLambda2() throws SqlException {
        x("(select-choose a, b, c from (x))longcast", "cast((select a,b,c from x) as long)");
    }

    @Test
    public void testCastLambdaWithAs() throws SqlException {
        x("(select-choose x y from (tab))funshortcast", "cast(fun(select x as y from tab order by x) as short)");
    }

    @Test
    public void testCastLowercase() throws SqlException {
        x("10shortcast", "CAST(10 AS short)");
    }

    @Test
    public void testCastMissingExpression() {
        assertFail("cast(as short)",
                0,
                "too few arguments for 'cast'");
    }

    @Test
    public void testCastMissingType() {
        assertFail("cast(1 as)",
                0,
                "too few arguments for 'cast'"
        );
    }

    @Test
    public void testCastMissingType2() {
        assertFail("cast(1 as 1)",
                10,
                "invalid type"
        );
    }

    @Test
    public void testCastMissingType3() {
        assertFail("cast(1 as binary)",
                10,
                "invalid type"
        );
    }

    @Test
    public void testCastNested() throws SqlException {
        x("1longcastshortcast", "cast(cast(1 as long) as short)");
    }

    @Test
    public void testCastNoAs() {
        assertFail("cast(1) + 1",
                6,
                "'as' missing");
    }

    @Test
    public void testCastNoAs2() {
        assertFail("cast(1)",
                6,
                "'as' missing");
    }

    @Test
    public void testCastNoAs3() {
        assertFail("cast(cast(1 as short))",
                21,
                "'as' missing");
    }

    @Test
    public void testCastNoAs4() {
        assertFail("cast(cast(1) as short)",
                11,
                "'as' missing");
    }

    @Test
    public void testCastSimple() throws SqlException {
        x("10shortcast", "cast(10 as short)");
    }

    @Test
    public void testCastTooManyArgs() {
        assertFail(
                "cast(10,20 as short)",
                7,
                "',' is not expected here"
        );
    }

    @Test
    public void testCastTooManyArgs2() {
        assertFail(
                "cast(10 as short, double)",
                16,
                "',' is not expected here"
        );
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
        x("a-b^", "-a^b");
    }

    @Test
    public void testCountStar() throws SqlException {
        x("*count", "count(*)");
    }

    @Test
    public void testDanglingExpression() {
        assertFail(
                "(.*10)",
                3,
                "dangling expression"
        );
    }

    @Test
    public void testDigitDigit() throws SqlException {
        x("4", "4  5");
    }

    @Test
    public void testDigitDotSpaceDigit() throws SqlException {
        x("4.", "4. 5");
    }

    @Test
    public void testDotDereference() throws SqlException {
        x("a.bn.", "(a.b).n");
    }

    @Test
    public void testDotDereferenceExpression() throws SqlException {
        x("ua.zc.=", "u = (a.z).c");
    }

    @Test
    public void testDotDereferenceFunction() throws SqlException {
        x("a.b123f.", "(a.b).f(1,2,3)");
    }

    @Test
    public void testDotDereferenceLiteralCase() {
        assertFail("(a.b).case",
                6,
                "'case' is not allowed here"
        );
    }

    @Test
    public void testDotDereferenceLiteralCast() {
        assertFail("(a.b).cast(s as int)",
                6,
                "'cast' is not allowed here"
        );
    }

    @Test
    public void testDotDereferenceLiteralConst() {
        assertFail("(a.b).true",
                6,
                "constant is not allowed here"
        );
    }

    @Test
    public void testDotDereferenceNumericConst() {
        assertFail("(a.b).10.1",
                6,
                "constant is not allowed here"
        );
    }

    @Test
    public void testDotDereferenceStrConst() {
        assertFail("(a.b).'ok'",
                6,
                "constant is not allowed here"
        );
    }

    @Test
    public void testDotDereferenceTooMany() {
        assertFail("(a.b)..",
                6,
                "too many dots"
        );
    }

    @Test
    public void testDotLiterals() throws SqlException {
        x("x.y", "\"x\".\"y\"");
    }

    @Test
    public void testDotLiteralsQuotedUnquoted() throws SqlException {
        x("x.y", "\"x\".y");
    }

    @Test
    public void testDotLiteralsUnquoted() throws SqlException {
        x("x.y", "x.y");
    }

    @Test
    public void testDotLiteralsUnquotedQuoted() throws SqlException {
        x("x.y", "x.\"y\"");
    }

    @Test
    public void testDotSpaceStar() {
        assertFail("a. *", 3, "too few arguments");
    }

    @Test
    public void testDotSpaceStarExpression() throws SqlException {
        x("a.3*", "a. * 3");
    }

    @Test
    public void testDotStar() throws SqlException {
        x("a.*", "a.*");
    }

    @Test
    public void testDoubleArrayDereference() throws SqlException {
        x("faj[][]", "f()[a[j]]");
    }

    @Test
    public void testDoubleParenthesis() {
        assertFail(
                "a(i)(o)",
                4,
                "not a method call"
        );
    }

    @Test
    public void testEqualPrecedence() throws Exception {
        x("abc^^", "a^b^c");
    }

    @Test
    public void testExpectQualifier() {
        assertFail(".a",
                1,
                "qualifier expected"
        );
    }

    @Test
    public void testExprCompiler() throws Exception {
        x("ac098dzf+1234x+", "a+f(c,d(0,9,8),z)+x(1,2,3,4)");
    }

    @Test
    public void testFloatingPointNumber() throws SqlException {
        x("5.90", "5.90");
    }

    @Test
    public void testFloatingPointNumberLeadingDot() throws SqlException {
        x(".199093", ".199093");
    }

    @Test
    public void testFunctionArrayArg() throws SqlException {
        x("abi[]ci[]funcj[]", "func(a,b[i],c[i])[j]");
    }

    @Test
    public void testFunctionArrayDereference() throws SqlException {
        x("funci[]", "func()[i]");
    }

    @Test
    public void testFunctionCallOnFloatingPointValues() throws SqlException {
        x("1.2.0921990.f", "f(1.2,.092,1990.)");
    }

    @Test
    public void testFunctionCallOnFloatingPointValues2() throws SqlException {
        x(".25.0921990.f", "f(.25,.092,1990.)");
    }

    @Test
    public void testFunctionStar() {
        assertFail("fun(*)", 4, "too few arguments");
    }

    @Test
    public void testFunctionWithArgsArrayDereference() throws SqlException {
        x("1abfunci[]", "func(1,a,b)[i]");
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
        x("ablah blahinyand", "a in (`blah blah`) and y");
    }

    @Test
    public void testLambdaInOperator() throws SqlException {
        x("1(select-choose a, b, c from (x))+", "1 + (select a,b,c from x)");
    }

    @Test
    public void testLambdaInOperator2() throws SqlException {
        x("1(select-choose a, b, c from (x))+", "1 + select a,b,c from x");
    }

    @Test
    public void testLambdaInOperator3() throws SqlException {
        x("(select-choose a, b, c from (x))1+", "(select a,b,c from x) + 1");
    }

    @Test
    public void testLambdaMiddleParameter() {
        assertFail(
                "f(1,2,select a,b,c,d from z, 4)",
                29,
                "dangling expression"
        );
    }

    @Test
    public void testLambdaMiddleParameterBraced() throws SqlException {
        x("12(select-choose a, b, c, d from (z))4f", "f(1,2,(select a,b,c,d from z), 4)");
    }

    @Test
    public void testListOfValues() throws SqlException {
        x("a.b", "a.b, c");
    }

    @Test
    public void testLiteralAndConstant() throws Exception {
        // expect that expression parser will stop after literal, because literal followed by constant does not
        // make sense
        x("x", "x \"a b\"");
    }

    @Test
    public void testLiteralDotSpaceLiteral() throws SqlException {
        x("a.", "a. b");
    }

    @Test
    public void testLiteralExit() throws Exception {
        x("abxybzc*+", "a + b * c(b(x,y),z) lit");
    }

    @Test
    public void testLiteralSpaceDot() throws SqlException {
        x("a", "a .b");
    }

    @Test
    public void testLiteralSpaceDotQuoted() throws SqlException {
        x("a", "a .\"b\"");
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
        x("x(select-choose a from (tab))iny(select-choose * from (X))inandk10>and", "x in (select a from tab) and y in (select * from X) and k > 10");
    }

    @Test
    public void testNewLambdaNested() throws SqlException {
        x("x(select-choose a, b from (T where c in (select-choose * from (Y)) and a = b))in", "x in (select a,b from T where c in (select * from Y) and a=b)");
    }

    @Test
    public void testNewLambdaQuerySyntax() {
        assertFail("x in (select a,b, from T)", 23, "column name expected");
    }

    @Test
    public void testNewLambdaUnbalancedBrace() {
        assertFail("x in (select a,b from T", 5, "unbalanced (");
    }

    @Test
    public void testNoArgFunction() throws Exception {
        x("ab4*+", "a+b()*4");
    }

    @Test
    public void testNoFunctionNameForListOfArgs() {
        assertFail(
                "cast((10,20) as short)",
                11,
                "no function or operator?"
        );
    }

    @Test
    public void testOverlappedBraceBracket() {
        assertFail(
                "a([i)]",
                2,
                "unbalanced ]"
        );
    }

    @Test
    public void testOverlappedBracketBrace() {
        assertFail(
                "a[f(i])",
                3,
                "unbalanced ("
        );
    }

    @Test
    public void testPGOperator() throws SqlException {
        x("c.relnameE'^(movies\\\\.csv)$'~", "c.relname ~ E'^(movies\\\\.csv)$'");
    }

    @Test
    public void testSimple() throws Exception {
        x("abxyc*2/+", "a + b * c(x,y)/2");
    }

    @Test
    public void testSimpleArrayDereference() throws SqlException {
        x("ai[]", "a[i]");
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
    public void testTooManyDots() {
        assertFail("a..b",
                2,
                "too many dots"
        );
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
    public void testWhacky() {
        assertFail("a-^b", 1, "too few arguments for '-' [found=1,expected=2]");
    }

    @Test
    public void testFloatLiteralScientific() throws Exception {
        x("1.234e-10", "1.234e-10");
        x("1.234E-10", "1.234E-10");
        x("1.234e+10", "1.234e+10");
        x("1.234E+10", "1.234E+10");
        x("1.234e10", "1.234e10");
        x("1.234E10", "1.234E10");
        x(".234e-10", ".234e-10");
        x(".234E-10", ".234E-10");
        x(".234e+10", ".234e+10");
        x(".234E+10", ".234E+10");
        x(".234e10", ".234e10");
        x(".234E10", ".234E10");
        x("i.1e-3<90100case", "case when i < .1e-3 then 90 else 100 end");
        x("i.1e+3<90100case", "case when i < .1e+3 then 90 else 100 end");
        x("i0.1e-3<90100case", "case when i < 0.1e-3 then 90 else 100 end");
        x("i0.1e+3<90100case", "case when i < 0.1e+3 then 90 else 100 end");

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
