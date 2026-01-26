/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.test.griffin;

import io.questdb.griffin.ExpressionParser;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExpressionParserTest extends AbstractCairoTest {
    private final static RpnBuilder rpnBuilder = new RpnBuilder();

    @Test
    public void testAllInvalidOperator() {
        assertFail(
                "a || all(b)",
                2,
                "unexpected operator"
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
    public void testAllNotEqual() throws SqlException {
        x("a b <>all", "a <> all(b)");
        x("a b <>all", "a != all(b)");
    }

    @Test
    public void testArrayCast() throws SqlException {
        x("'{1, 2, 3, 4}' double[] cast", "cast('{1, 2, 3, 4}' as double[])");
    }

    @Test
    public void testArrayConstruct() throws SqlException {
        x("1 2 3 ARRAY", "ARRAY[1, 2, 3]");
        x("1 2 ARRAY 3 ARRAY", "ARRAY[1, [2], 3]");
        x("1 2 3 4 ARRAY ARRAY 5 ARRAY", "ARRAY[1, [2, [3, 4]], 5]");
        x("x 1 []", "x[1]");
        x("x 1 2 []", "x[1, 2]");
        x("x.y 1 2 []", "x.y[1, 2]");
        x("b i [] c i [] func", "func(b[i], c[i])");
        x("1 2 func 3 ARRAY", "ARRAY[1, func(2), 3]");
        x("1 func 2 []", "func(1)[2]");
        x("1 2 func 3 [] 4 ARRAY", "ARRAY[1, func(2)[3], 4]");
        x("1 2 func ARRAY 3 ARRAY", "ARRAY[1, [func(2)], 3]");
        x("1 2 ARRAY 3 ARRAY", "ARRAY[1, ARRAY[2], 3]");
        x("1 ARRAY 2 []", "ARRAY[1][2]");
        x("1 2 ARRAY 3 [] 4 ARRAY", "ARRAY[1, ARRAY[2][3], 4]");
    }

    @Test
    public void testArrayConstructInvalid() {
        assertFail("ARRAY[1", 5, "unbalanced [");
        assertFail("ARRAY[1, [1]", 5, "unbalanced [");
        assertFail("ARRAY[1 2]", 8, "dangling expression");
    }

    @Test
    public void testArrayDereferenceExpr() throws SqlException {
        x("a i 10 + []", "a[i+10]");
    }

    @Test
    public void testArrayDereferenceMissingIndex() {
        assertFail(
                "a[]",
                2,
                "empty brackets"
        );
    }

    @Test
    public void testArrayDereferenceNotClosedEndOfExpression() {
        assertFail(
                "a[",
                1,
                "unbalanced ["
        );
    }

    @Test
    public void testArrayDereferenceNotClosedEndOfFunctionCall() {
        assertFail(
                "f(a[)",
                3,
                "unbalanced ["
        );
    }

    @Test
    public void testArrayDereferenceNotClosedFunctionArg() {
        assertFail(
                "f(b,a[,c)",
                6,
                "missing arguments"
        );
    }

    @Test
    public void testArrayDereferencePriority() throws SqlException {
        x(
                "nspname 'pg_toast' <> nspname '^pg_temp_' !~ nspname true pg_catalog.current_schemas 1 [] = or and",
                "nspname <> 'pg_toast' AND (nspname !~ '^pg_temp_'  OR nspname = (pg_catalog.current_schemas(true))[1])"
        );
    }

    @Test
    public void testArrayTypeErrorRecovery() throws Exception {
        // Test that parser can handle multiple errors in sequence
        assertException("select null::double [], null::int []", 20, "array type requires no whitespace");

        // Test error recovery with different constructs
        assertException("select x, null::double [], y from table", 23, "array type requires no whitespace");

        // Test complex expressions with array errors
        assertException("select (select null::double [] from dual)", 28, "array type requires no whitespace");
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
        x("4 c -", "4-c");
    }

    @Test
    public void testBooleanLogicPrecedence() throws Exception {
        x("x y not =", "x = NOT y");
    }

    @Test
    public void testBrutalArraySyntaxErrors() throws Exception {
        // Multiple consecutive brackets
        assertException("select null::[][][]", 13, "type definition is expected");
        assertException("select null::][", 13, "syntax error");

        // Brackets with numbers (common user mistake)
        assertException("select null::double[1]", 20, "']' expected");
        assertException("select null::int[0]", 17, "']' expected");
        assertException("select null::varchar[255]", 21, "']' expected");

        // Brackets with expressions
        assertException("select null::double[x+1]", 20, "']' expected");
        assertException("select null::int[null]", 17, "']' expected");

        // Weird spacing patterns in brackets
        assertException("select null::double[ ]", 21, "expected 'double[]' but found 'double[ ]'");
        assertException("select null::int[\t]", 18, "expected 'int[]' but found 'int[\t]'");
        assertException("select null::varchar[\n]", 22, "expected 'varchar[]' but found 'varchar[\n]'");

        // Mixed bracket types
        assertException("select null::double(]", 20, "syntax error");
        assertException("select null::int[)", 17, "']' expected");

        // Unicode brackets (if parser somehow accepts them)
        assertException("select null::double【】", 13, "invalid constant: double【】");
        assertException("select null::int〔〕", 13, "invalid constant: int〔〕");

        // Extreme whitespace variations
        assertException("select null::double\t\t\t[]", 22, "array type requires no whitespace: expected 'double[]' but found 'double\t\t\t []'");

        // NBSP is NOT picked up by Character.isWhitespace()
        assertException("select null::int\u00A0[]", 13, "invalid constant: int []"); // Non-breaking space
        assertException("select null::varchar\u2003[]", 21, "array type requires no whitespace: expected 'varchar[]' but found 'varchar  []'"); // Em space

        // Extremely long type names with spaces
        assertException("select null::doubleprecision []", 29, "array type requires no whitespace: expected 'doubleprecision[]' but found 'doubleprecision  []'");

        // Case sensitivity issues
        assertException("select null::DOUBLE []", 20, "array type requires no whitespace");
        assertException("select null::Double []", 20, "array type requires no whitespace");
        assertException("select null::dOuBlE []", 20, "array type requires no whitespace");

        // Multiple spaces of different types
        assertException("select null::double \t []", 22, "array type requires no whitespace");
        assertException("select null::int  \t  []", 21, "array type requires no whitespace");

        // Nested cast errors
        assertException("select cast(cast(null as double []) as int)", 32, "array type requires no whitespace");

        // Array in function parameters
        assertException("select abs(null::double [])", 24, "array type requires no whitespace");

        // Array in complex expressions
        assertException("select (1 + null::int []) * 2", 22, "array type requires no whitespace");
        assertException("select case when true then null::double [] else null end", 40, "array type requires no whitespace");
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
    public void testCaseAsArrayIndex() throws SqlException {
        x(
                "a b 1 3 4 case []",
                "a[case b when 1 then 3 else 4 end]"
        );
    }

    @Test
    public void testCaseAsArrayIndexAnotherArray() throws SqlException {
        x(
                "a b 1 b 3 [] 4 case []",
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
        x(" w1 1 + 10 = 'th1' w2 3 * 1 > 'th2' 0 case 5 * 1 +",
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
                "x y  a b + 10 > 'a' a b - 3 < 'b' 0 case 10 + z f *",
                "x*f(y,case when (a+b) > 10 then 'a' when (a-b)<3 then 'b' else 0 end + 10,z)"
        );
    }

    @Test
    public void testCaseLikeSwitch() throws SqlException {
        x("x 1 'a' 2 'b' case", "case x when 1 then 'a' when 2 then 'b' end");
    }

    @Test
    public void testCaseLowercase() throws SqlException {
        x("10  w1 1 + 5 * 10 = 'th1' 1 - w2 3 * 1 > 'th2' 0 case 1 + *",
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
        x(" x 0 >  y 1 = 'a' 'b' case x 0 < 'c' case",
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
        x("when 1 + 10 >",
                "when + 1 > 10"
        );
    }

    @Test
    public void testCaseWithArithmetic() throws SqlException {
        x(" w1 1 + 10 = 'th1' w2 3 * 1 > 'th2' 0 case",
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
        x("10  w1 1 + 5 * 10 = 'th1' 1 - w2 3 * 1 > 'th2' 0 case 1 + *",
                "10*(case" +
                        " when (w1+1)*5=10" +
                        " then 'th1'-1" +
                        " when w2*3>1" +
                        " then 'th2'" +
                        " else 0" +
                        " end + 1)");
    }

    @Test
    public void testCaseWithCast() throws SqlException {
        x("1 int cast  1 'th1' 2 'th2' 0 case 5 * 1 +",
                "case (cast(1 as int))" +
                        " when 1" +
                        " then 'th1'" +
                        " when 2" +
                        " then 'th2'" +
                        " else 0" +
                        " end * 5 + 1");
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
    public void testCaseWithOuterBraces() throws SqlException {
        x("10  w1 1 + 10 = 'th1' w2 3 * 1 > 'th2' 0 case 1 + *",
                "10*(case" +
                        " when w1+1=10" +
                        " then 'th1'" +
                        " when w2*3>1" +
                        " then 'th2'" +
                        " else 0" +
                        " end + 1)");
    }

    @Test
    public void testCastDecimalInvalidPrecision() {
        assertFail("cast('123.45' as DECIMAL(abc))",
                25,
                "Invalid decimal type. The precision ('abc') must be a number");
    }

    @Test
    public void testCastDecimalMissingPrecision() {
        assertFail("cast('123.45' as DECIMAL())",
                25,
                "Invalid decimal type. The precision is missing");
    }

    @Test
    public void testCastDecimalWithPrecision() throws SqlException {
        x("'123.45' DECIMAL_10 cast", "cast('123.45' as DECIMAL(10))");
    }

    @Test
    public void testCastDecimalWithPrecisionAndScale() throws SqlException {
        x("'123.45' DECIMAL_10_2 cast", "cast('123.45' as DECIMAL(10,2))");
    }

    @Test
    public void testCastDecimalWithSpaces() throws SqlException {
        x("'123.45' DECIMAL_10_2 cast", "cast('123.45' as DECIMAL ( 10 , 2 ))");
    }

    @Test
    public void testCastFunctionCall() throws SqlException {
        x("1 10 20 30 f + short cast", "cast(1+f(10,20,30) as short)");
    }

    @Test
    public void testCastFunctionCallMultiSpace() throws SqlException {
        x("1 10 20 30 f + short cast", "cast\t --- this is a comment\n\n(1+f(10,20,30) as short\n)");
    }

    @Test
    public void testCastFunctionWithLambda() throws SqlException {
        x(" (select-choose a, b, c from (x)) f long cast", "cast(f(select a,b,c from x) as long)");
    }

    @Test
    public void testCastFunctionWithLambdaMultiSpaceNewlineAndComment() throws SqlException {
        x(" (select-choose a, b, c from (x)) f long cast", "cast    --- this is a comment\n\n(f(select a,b,c from x) as long\n)");
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
                "unsupported cast");
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
    public void testCastGeoHashCastStrWithBitsPrecision() throws SqlException {
        x("'sp052w92' geohash60b cast", "cast('sp052w92' as geohash(60b))");
    }

    @Test
    public void testCastGeoHashCastStrWithCharsPrecision() throws SqlException {
        x("'sp052w92' geohash6c cast", "cast('sp052w92' as geohash(6c))");
    }

    @Test
    public void testCastLambda() throws SqlException {
        x(" (select-choose a, b, c from (x)) 1 + long cast", "cast((select a,b,c from x)+1 as long)");
    }

    @Test
    public void testCastLambda2() throws SqlException {
        x(" (select-choose a, b, c from (x)) long cast", "cast((select a,b,c from x) as long)");
    }

    @Test
    public void testCastLambdaWithAs() throws SqlException {
        x(" (select-choose x y from (tab)) fun short cast", "cast(fun(select x as y from tab order by x) as short)");
    }

    @Test
    public void testCastLowercase() throws SqlException {
        x("10 short cast", "CAST(10 AS short)");
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
                "unsupported cast"
        );
    }

    @Test
    public void testCastMissingType3() {
        assertFail("cast(1 as binary)",
                10,
                "unsupported cast"
        );
    }

    @Test
    public void testCastNested() throws SqlException {
        x("1 long cast short cast", "cast(cast(1 as long) as short)");
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
        x("10 short cast", "cast(10 as short)");
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
        x("a b x y b z c * +", "a + b * c(b(x,y),z),");
    }

    @Test
    public void testComplexUnary1() throws Exception {
        x("4 x y c - ^", "4^-c(x,y)");
    }

    @Test
    public void testComplexUnary2() throws Exception {
        x("a - b ^", "-a^b");
    }

    @Test
    public void testCorrectPrecedenceOfBasicOps() throws Exception {
        x("a ~ b ^ c d & |", "~a^b|c&d");
        x("1 2 4 & |", "1|2&4");
        x("1 - 1 in not", "-1 not in (1)");
        x("'1' '2' || '12' in not", "'1' || '2' not in ('12')");
        x("'1' '2' || '12' in not", "not '1' || '2' in ('12')");
        x("true true false and or", "true or true and false");
        x("1 2 | 3 in", "1 | 2 IN 3");
        x("1 1 in not true =", "1 not in (1) = true");
        x("a b ^ c ^", "a^b^c");
    }

    @Test
    public void testCountDistinctRewrites() throws SqlException {
        x("foo count_distinct", "count_distinct(foo)");
        x("foo count_distinct", "count(distinct foo)");

        x("1 1 + count_distinct", "count_distinct(1+1)");
        x("1 1 + count_distinct", "count(distinct 1+1)");

        x("bar foo count_distinct", "count_distinct(foo(bar))");
        x("bar foo count_distinct", "count(distinct foo(bar))");

        x("bar foo count_distinct varchar cast", "cast (count_distinct(foo(bar)) as varchar)");
        x("bar foo count_distinct varchar cast", "cast (count(distinct foo(bar)) as varchar)");

        // check distinct works as a column name when quoted
        x("distinct count varchar cast", "cast (count(\"distinct\") as varchar)");
        // it works with multiple arguments too
        x("distinct foo count", "count(\"distinct\", foo)");
        // not written when count is not a function
        x("count distinct foo", "foo(count, distinct)");
        x("distinctnot count", "count(distinctnot)");
        x("bar count distinct foo", "foo(bar, count, distinct)");
        x("count bar distinct foo", "foo(bar(count), distinct)");

        assertFail("count(distinct *)", 6, "count(distinct *) is not supported");
        assertFail("count(distinct ", 6, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\"");
        assertFail("notcount(distinct foo)", 18, "dangling literal");
        assertFail("count(distinct(foo, bar))", 23, "count distinct aggregation supports a single column only");
        assertFail("count(DISTINCT(foo, bar))", 23, "count distinct aggregation supports a single column only");
        assertFail("count(distinct (foo, bar))", 24, "count distinct aggregation supports a single column only"); // with an extra space
        assertFail("count(DiSTiNcT (foo, bar))", 24, "count distinct aggregation supports a single column only"); // with an extra space
        assertFail("count(distinct(foo, bar, foobar))", 31, "count distinct aggregation supports a single column only");
        assertFail("count(distinct(foo, bar, foobar, 1+1))", 36, "count distinct aggregation supports a single column only");
        assertFail("count(Distinct(foo, bar, foobar, 1+1))", 36, "count distinct aggregation supports a single column only");
        assertFail("count(distinct))", 6, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\"");
    }

    @Test
    public void testCountStar() throws SqlException {
        x("* count", "count(*)");
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
    public void testDecimalCast() throws SqlException {
        x("'123.456' decimal cast", "cast('123.456' as decimal)");
    }

    @Test
    public void testDecimalDifferentPrecision() throws SqlException {
        x("DECIMAL_15", "DECIMAL(15)");
    }

    @Test
    public void testDecimalEmptyParentheses() {
        assertFail("DECIMAL()",
                8,
                "Invalid decimal type. The precision is missing");
    }

    @Test
    public void testDecimalEmptyScale() {
        assertFail("DECIMAL(10,)",
                11,
                "Invalid decimal type. The scale (')') must be a number");
    }

    @Test
    public void testDecimalFailInvalidPrecision() {
        assertFail("DECIMAL(abc)",
                8,
                "Invalid decimal type. The precision ('abc') must be a number");
    }

    @Test
    public void testDecimalFailInvalidScale() {
        assertFail("DECIMAL(10,xyz)",
                11,
                "Invalid decimal type. The scale ('xyz') must be a number");
    }

    @Test
    public void testDecimalMaxPrecision() throws SqlException {
        x("DECIMAL_76", "DECIMAL(76)");
    }

    @Test
    public void testDecimalMaxPrecisionAndScale() throws SqlException {
        x("DECIMAL_76_76", "DECIMAL(76,76)");
    }

    @Test
    public void testDecimalMissingPrecision() {
        assertFail("DECIMAL(",
                7,
                "Invalid decimal type. The precision is missing");
    }

    @Test
    public void testDecimalMissingScale() {
        assertFail("DECIMAL(10,",
                10,
                "Invalid decimal type. The scale is missing");
    }

    @Test
    public void testDecimalPrecisionAndScale() throws SqlException {
        x("DECIMAL_10_2", "DECIMAL(10,2)");
    }

    @Test
    public void testDecimalPrecisionAndScaleWithSpaces() throws SqlException {
        x("DECIMAL_10_2", "DECIMAL(10, 2)");
    }

    @Test
    public void testDecimalPrecisionOnly() throws SqlException {
        x("DECIMAL_10", "DECIMAL(10)");
    }

    @Test
    public void testDecimalUnbalancedParentheses() {
        assertFail("DECIMAL(10",
                8,
                "Invalid decimal type. Missing ')'");
    }

    @Test
    public void testDecimalWithExtraSpaces() throws SqlException {
        x("DECIMAL_10_2", "DECIMAL ( 10 , 2 )");
    }

    @Test
    public void testDecimalWithNewlines() throws SqlException {
        x("DECIMAL_10_2", """
                 DECIMAL\r
                  (
                 10
                ,
                 2
                       ) \s""");
    }

    @Test
    public void testDecimalZeroScale() throws SqlException {
        x("DECIMAL_10_0", "DECIMAL(10,0)");
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
        x("a.b n .", "(a.b).n");
    }

    @Test
    public void testDotDereferenceExpression() throws SqlException {
        x("u a.z c . =", "u = (a.z).c");
    }

    @Test
    public void testDotDereferenceFunction() throws SqlException {
        x("a.b 1 2 3 f .", "(a.b).f(1,2,3)");
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
    public void testDotLiteralsUnquotedQuotedWithEscapedQuote() throws SqlException {
        x("x.y\"\"z", "x.\"y\"\"z\"");
    }

    @Test
    public void testDotSpaceStarExpression() throws SqlException {
        x("a. *", "a. * 3");
    }

    @Test
    public void testDotStar() throws SqlException {
        x("a.*", "a.*");
    }

    @Test
    public void testDoubleArrayDereference() throws SqlException {
        x("f a j [] []", "f()[a[j]]");
    }

    @Test
    public void testDoubleParenthesis() {
        assertFail(
                "a(i)(o)",
                4,
                "not a function call"
        );
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
        x("a c 0 9 8 d z f + 1 2 3 4 x +", "a+f(c,d(0,9,8),z)+x(1,2,3,4)");
    }

    @Test
    public void testExtractGeoHashBitsSuffixNoSuffix() throws SqlException {
        for (String tok : new String[]{"#", "#/", "#p", "#pp", "#ppp", "#0", "#01", "#001"}) {
            assertEquals(
                    Numbers.encodeLowHighShorts((short) 0, (short) (5 * (tok.length() - 1))),
                    ExpressionParser.extractGeoHashSuffix(0, tok));
        }
        for (String tok : new String[]{"#/x", "#/1x", "#/x1", "#/xx", "#/-1",}) {
            Assert.assertThrows("[0] invalid bits size for GEOHASH constant",
                    SqlException.class, () -> ExpressionParser.extractGeoHashSuffix(0, tok));
        }
    }

    @Test
    public void testExtractGeoHashBitsSuffixValid() throws SqlException {
        for (int bits = 1; bits < 10; bits++) {
            assertEquals(
                    Numbers.encodeLowHighShorts((short) 2, (short) bits),
                    ExpressionParser.extractGeoHashSuffix(0, "#/" + bits)); // '/d'
        }
        for (int bits = 1; bits < 10; bits++) {
            assertEquals(
                    Numbers.encodeLowHighShorts((short) 3, (short) bits),
                    ExpressionParser.extractGeoHashSuffix(0, "#/0" + bits)); // '/0d'
        }
        for (int bits = 10; bits <= 60; bits++) {
            assertEquals(
                    Numbers.encodeLowHighShorts((short) 3, (short) bits),
                    ExpressionParser.extractGeoHashSuffix(0, "#/" + bits)); // '/dd'
        }
    }

    @Test
    public void testExtractGeoHashBitsSuffixZero() {
        Assert.assertThrows("", SqlException.class, () -> ExpressionParser.extractGeoHashSuffix(0, "#/0"));
        Assert.assertThrows("", SqlException.class, () -> ExpressionParser.extractGeoHashSuffix(0, "#/00"));
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
        x(" i .1e-3 < 90 100 case", "case when i < .1e-3 then 90 else 100 end");
        x(" i .1e+3 < 90 100 case", "case when i < .1e+3 then 90 else 100 end");
        x(" i 0.1e-3 < 90 100 case", "case when i < 0.1e-3 then 90 else 100 end");
        x(" i 0.1e+3 < 90 100 case", "case when i < 0.1e+3 then 90 else 100 end");
    }

    @Test
    public void testFloatQualifier() throws SqlException {
        x("'NaN' float ::", "'NaN'::float");
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
        x("a b i [] c i [] func j []", "func(a,b[i],c[i])[j]");
    }

    @Test
    public void testFunctionArrayDereference() throws SqlException {
        x("func i []", "func()[i]");
    }

    @Test
    public void testFunctionCallOnFloatingPointValues() throws SqlException {
        x("1.2 .092 1990. f", "f(1.2,.092,1990.)");
    }

    @Test
    public void testFunctionCallOnFloatingPointValues2() throws SqlException {
        x(".25 .092 1990. f", "f(.25,.092,1990.)");
    }

    @Test
    public void testFunctionStar() {
        assertFail("fun(*)", 4, "too few arguments");
    }

    @Test
    public void testFunctionWithArgsArrayDereference() throws SqlException {
        x("1 a b func i []", "func(1,a,b)[i]");
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
        x("geohash6c", """
                geohash ( 6c\
                -- this is a comment, as you can see\
                
                
                \r)""");
    }

    @Test
    public void testGeoHash5() throws SqlException {
        x("geohash6c", """
                 geohash\r
                  (
                 6c
                -- this is a comment, as you can see\
                
                
                \r)-- my tralala""");
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
    public void testGeoHashConstantValid() throws SqlException {
        x("#sp052w92p1p8/7", """
                 #sp052w92p1p8\r
                  / 7
                 6c
                -- this is a comment, as you can see\
                
                
                \r-- my tralala""");
        x("#sp052w92p1p8/7", "#sp052w92p1p8 / 7");
        x("#sp052w92p1p8", "#sp052w92p1p8");
        x("#sp052w92p1p8/0", "#sp052w92p1p8 / 0"); // valid at the expression level
        x("#sp052w92p1p8/61", "#sp052w92p1p8 / 61"); // valid at the expression level
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
    public void testIn() throws Exception {
        x("a b c in", "a in (b,c)");
    }

    @Test
    public void testInOperator() throws Exception {
        x("a 10 = b x y in and", "a = 10 and b in (x,y)");
    }

    @Test
    public void testInvalidArraySyntaxInCast() throws Exception {
        assertException("select null::[]double;", 13, "did you mean 'double[]'?");
        assertException("select null::[]float;", 13, "did you mean 'float[]'?");
        assertException("select null::[];", 13, "type definition is expected");
        assertException("select null::double []", 20, "array type requires no whitespace: expected 'double[]' but found 'double  []'");
    }

    @Test
    public void testInvalidArraySyntaxInCreateTable() throws Exception {
        assertException("create table x(a []double)", 17, "did you mean 'double[]'?");
        assertException("create table x(a []int)", 17, "did you mean 'int[]'?");
        assertException("create table x(a [])", 17, "column type is expected here");
        assertException("create table x(a [], b double)", 17, "column type is expected here");
    }

    @Test
    public void testIsGeoHashBitsConstantNotValid() {
        Assert.assertFalse(ExpressionParser.isGeoHashBitsConstant("#00110")); // missing '#'
        Assert.assertFalse(ExpressionParser.isGeoHashBitsConstant("#0")); // missing '#'
    }

    @Test
    public void testIsGeoHashBitsConstantValid() {
        Assert.assertTrue(ExpressionParser.isGeoHashBitsConstant("##0"));
        Assert.assertTrue(ExpressionParser.isGeoHashBitsConstant("##1"));
        Assert.assertTrue(ExpressionParser.isGeoHashBitsConstant("##111111111100000000001111111111000000000011111111110000000000"));
    }

    @Test
    public void testIsGeoHashCharsConstantNotValid() {
        Assert.assertFalse(ExpressionParser.isGeoHashCharsConstant("##"));
    }

    @Test
    public void testIsGeoHashCharsConstantValid() {
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#0"));
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#1"));
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#sp"));
        Assert.assertTrue(ExpressionParser.isGeoHashCharsConstant("#sp052w92p1p8"));
    }

    @Test
    public void testIsNotFalse() throws SqlException {
        x("a False !=", "a IS not False");
        x("tab.a False !=", "tab.a IS NOT False");
        x("'False' False !=", "'False' IS not False");
    }

    @Test
    public void testIsNotNull() throws SqlException {
        x("a NULL !=", "a IS NOT NULL");
        x("tab.a NULL !=", "tab.a IS NOT NULL");
        x("3 NULL !=", "3 IS NOT NULL");
        x("null NULL !=", "null IS NOT NULL");
        x("NULL NULL !=", "NULL IS NOT NULL");
        x("'null' NULL !=", "'null' IS NOT NULL");
        x("'' null || NULL !=", "('' || null) IS NOT NULL");
        assertFail("column is not 3", 7, "IS NOT must be followed by NULL, TRUE or FALSE");
        assertFail(". is not great", 2, "IS [NOT] not allowed here");
        assertFail("column is not $1", 7, "IS NOT must be followed by NULL, TRUE or FALSE");
        assertFail("column is not", 7, "IS NOT must be followed by NULL, TRUE or FALSE");
    }

    @Test
    public void testIsNotTrue() throws SqlException {
        x("a True !=", "a IS not True");
        x("tab.a True !=", "tab.a IS NOT True");
        x("'true' true !=", "'true' IS not true");
    }

    @Test
    public void testIsNull() throws SqlException {
        x("a NULL =", "a IS NULL");
        x("tab.a NULL =", "tab.a IS NULL");
        x("3 NULL =", "3 IS NULL");
        x("null NULL =", "null IS NULL");
        x("NULL NULL =", "NULL IS NULL");
        x("'null' NULL =", "'null' IS NULL");
        x("'' null | NULL =", "('' | null) IS NULL");
        assertFail("column is 3", 7, "IS must be followed by NULL, TRUE or FALSE");
        assertFail(". is great", 2, "IS [NOT] not allowed here");
        assertFail("column is $1", 7, "IS must be followed by NULL, TRUE or FALSE");
        assertFail("column is", 7, "IS must be followed by [NOT] NULL");
    }

    @Test
    public void testIsTrue() throws SqlException {
        x("a True =", "a IS True");
        x("tab.a True =", "tab.a IS True");
        x("'true' true =", "'true' IS true");
        x("'' null | NULL =", "('' | null) IS NULL");
    }

    @Test
    public void testLambda() throws Exception {
        x("a blah blah in y and", "a in (`blah blah`) and y");
    }

    @Test
    public void testLambdaInOperator() throws SqlException {
        x("1  (select-choose a, b, c from (x)) +", "1 + (select a,b,c from x)");
    }

    @Test
    public void testLambdaInOperator2() throws SqlException {
        x("1  (select-choose a, b, c from (x)) +", "1 + select a,b,c from x");
    }

    @Test
    public void testLambdaInOperator3() throws SqlException {
        x(" (select-choose a, b, c from (x)) 1 +", "(select a,b,c from x) + 1");
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
        x("1 2  (select-choose a, b, c, d from (z)) 4 f", "f(1,2,(select a,b,c,d from z), 4)");
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
        x("a b x y b z c * +", "a + b * c(b(x,y),z) lit");
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
        x("4 8 y f r z", "z(4, r(f(8,y)))");
    }

    @Test
    public void testNestedOperator() throws Exception {
        x("a c 4 * d b +", "a + b( c * 4, d)");
    }

    @Test
    public void testNewLambda() throws SqlException {
        x("x  (select-choose a from (tab)) in", "x in (select a from tab)");
    }

    @Test
    public void testNewLambdaMultipleExpressions() throws SqlException {
        x("x  (select-choose a from (tab)) in y  (select-choose * from (X)) in and k 10 > and", "x in (select a from tab) and y in (select * from X) and k > 10");
    }

    @Test
    public void testNewLambdaNested() throws SqlException {
        x("x  (select-choose a, b from (T where c in (select-choose * from (Y)) and a = b)) in", "x in (select a,b from T where c in (select * from Y) and a=b)");
    }

    @Test
    public void testNewLambdaQuerySyntax() throws SqlException {
        x("x  (select-choose a, b from (T)) in", "x in (select a,b, from T)");
    }

    @Test
    public void testNewLambdaUnbalancedBrace() {
        assertFail("x in (select a,b from T", 5, "unbalanced (");
    }

    @Test
    public void testNoArgFunction() throws Exception {
        x("a b 4 * +", "a+b()*4");
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
    public void testNotIn() throws Exception {
        x("x 'a' 'b' in not", "x not in ('a','b')");
    }

    @Test
    public void testNotInReverseContext() throws Exception {
        x("a x 'a' 'b' in not and", "a and not x in ('a','b')");
    }

    @Test
    public void testNotInTimestamp() throws Exception {
        x("x '2022-01-01' in not", "x not in '2022-01-01'");
    }

    @Test
    public void testNotOperator() throws SqlException {
        x("aboolean true = aboolean false not = not or", "aboolean = true or not aboolean = not false");
    }

    @Test
    public void testOverlappedBraceBracket() {
        assertFail(
                "a([i)]",
                2,
                "'[' is unexpected here"
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
        x("c.relname E'^(movies\\\\.csv)$' ~", "c.relname ~ E'^(movies\\\\.csv)$'");
    }

    @Test
    public void testSimple() throws Exception {
        x("a b x y c * 2 / +", "a + b * c(x,y)/2");
    }

    @Test
    public void testSimpleArrayDereference() throws SqlException {
        x("a i []", "a[i]");
    }

    @Test
    public void testSimpleCase() throws SqlException {
        x(" w1 th1 w2 th2 els case", "case when w1 then th1 when w2 then th2 else els end");
    }

    @Test
    public void testSimpleLiteralExit() throws Exception {
        x("a", "a lit");
    }

    @Test
    public void testStringAggDistinctRewrites() throws SqlException {
        x("foo ',' string_distinct_agg", "string_distinct_agg(foo, ',')");
        x("foo ',' string_distinct_agg", "string_agg(distinct foo, ',')");

        x("1 1 + ',' string_distinct_agg", "string_distinct_agg(1+1, ',')");
        x("1 1 + ',' string_distinct_agg", "string_agg(distinct 1+1, ',')");

        x("bar foo ',' string_distinct_agg", "string_distinct_agg(foo(bar), ',')");
        x("bar foo ',' string_distinct_agg", "string_agg(distinct foo(bar), ',')");

        x("bar foo ',' string_distinct_agg varchar cast", "cast (string_distinct_agg(foo(bar), ',') as varchar)");
        x("bar foo ',' string_distinct_agg varchar cast", "cast (string_agg(distinct foo(bar), ',') as varchar)");

        // check 'distinct' as a column name works when quoted
        x("distinct ',' string_agg varchar cast", "cast (string_agg(\"distinct\", ',') as varchar)");
        // not re-written when string_agg is not a function
        x("string_agg distinct foo", "foo(string_agg, distinct)");
        x("distinctnot string_agg", "string_agg(distinctnot)");
        x("bar string_agg distinct foo", "foo(bar, string_agg, distinct)");
        x("string_agg bar distinct foo", "foo(bar(string_agg), distinct)");

        assertFail("string_agg(distinct)", 11, "table and column names that are SQL keywords have to be enclosed in double quotes, such as \"distinct\"");
        assertFail("notcount(distinct foo, ',')", 18, "dangling literal");
        assertFail("notcount(distinct foo)", 18, "dangling literal");
    }

    @Test
    public void testStringAggDistinct_orderByNotSupported() {
        assertFail("string_agg(distinct foo, ',' order by bar)", 29, "ORDER BY not supported for string_distinct_agg");
    }

    @Test
    public void testStringConcat() throws SqlException {
        x("a 'b' || c || d ||", "a||'b'||c||d");
    }

    @Test
    public void testTextArrayQualifier() throws SqlException {
        x("'{hello}' text[] ::", "'{hello}'::text[]");
    }

    @Test
    public void testTimestampWithTimezone() throws SqlException {
        x("'2021-12-31 15:15:51.663+00:00' timestamp cast",
                "cast('2021-12-31 15:15:51.663+00:00' as timestamp with time zone)");
    }

    @Test
    public void testTooManyDots() {
        assertFail("a..b",
                2,
                "too many dots"
        );
    }

    @Test
    public void testTypeQualifier() throws SqlException {
        x("'hello' something ::", "'hello'::something");
    }

    @Test
    public void testUnary() throws Exception {
        x("4 c - *", "4 * -c");
    }

    @Test
    public void testUnaryComplement() throws Exception {
        x("1 ~ 1 >", "~1 > 1");
        x("1 ~ - ~ - 1 - ~ >", "-~-~1 > ~-1");
    }

    @Test
    public void testUnbalancedLeftBrace() {
        assertFail("a+b(5,c(x,y)", 3, "unbalanced");
    }

    @Test
    public void testUnbalancedRightBraceExit() throws Exception {
        x("a 5 x y c b +", "a+b(5,c(x,y)))");
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
    public void testUnquotedStrFail2() {
        assertFail(
                "s ~ 'TDF''",
                4,
                "unclosed quoted string?"
        );
    }

    @Test
    public void testUnquotedStrFail3() {
        assertFail(
                "s ~ 'TDF''A''",
                4,
                "unclosed quoted string?"
        );
    }

    @Test
    public void testWhacky() {
        assertFail("a-^b", 1, "too few arguments for '-' [found=1,expected=2]");
    }

    @Test
    public void testWindowFrameCurrentRowWithPrecedingEnd() {
        // Frame starting from CURRENT ROW cannot have PRECEDING end
        assertFail(
                "f(c) over (partition by b order by ts rows between current row and 2 preceding)",
                69,
                "frame starting from CURRENT ROW must end with CURRENT ROW or FOLLOWING"
        );
    }

    @Test
    public void testWindowFrameExcludeGroup() throws SqlException {
        x("c avg over (partition by b order by ts rows between unbounded preceding and current row exclude group)",
                "avg(c) over (partition by b order by ts rows between UNBOUNDED PRECEDING and current row EXCLUDE GROUP)");
    }

    @Test
    public void testWindowFrameExcludeTies() throws SqlException {
        x("c avg over (partition by b order by ts rows between unbounded preceding and current row exclude ties)",
                "avg(c) over (partition by b order by ts rows between UNBOUNDED PRECEDING and current row EXCLUDE TIES)");
    }

    @Test
    public void testWindowFrameExpressionWithExcludeTies() throws SqlException {
        // Expression in frame bound with EXCLUDE TIES
        x("c f over (partition by b order by ts rows between 1 1 / preceding and unbounded following exclude ties)",
                "f(c) over (partition by b order by ts rows between 1/1 preceding and unbounded following exclude ties)");
    }

    @Test
    public void testWindowFrameFollowingWithPrecedingEnd() {
        // Frame starting from FOLLOWING cannot have PRECEDING end
        assertFail(
                "f(c) over (partition by b order by ts rows between 1 following and 2 preceding)",
                69,
                "frame starting from FOLLOWING must end with FOLLOWING"
        );
    }

    @Test
    public void testWindowFrameMissingLowerBoundValue() {
        // Missing value before PRECEDING in lower bound
        assertFail(
                "avg(a) over(partition by b order by ts rows between preceding and current row)",
                52,
                "frame bound value expected before 'preceding'"
        );
    }

    @Test
    public void testWindowFrameMissingUpperBoundValue() {
        // Missing value before PRECEDING in upper bound
        assertFail(
                "avg(a) over(partition by b order by ts rows between 10 preceding and preceding)",
                69,
                "frame bound value expected before 'preceding'"
        );
    }

    @Test
    public void testWindowFrameSingleBoundFollowing() {
        // Single-bound mode only allows PRECEDING, not FOLLOWING
        assertFail(
                "f(c) over (partition by b order by ts rows 12 following)",
                46,
                "single-bound frame specification requires PRECEDING, use BETWEEN for FOLLOWING"
        );
    }

    @Test
    public void testWindowFrameTimeUnitWithGroups() {
        // Time units are only valid with RANGE, not GROUPS
        assertFail(
                "f(c) over (partition by b order by a groups 10 day preceding)",
                47,
                "time units are only valid with RANGE frames, not ROWS or GROUPS"
        );
    }

    @Test
    public void testWindowFrameTimeUnitWithRows() {
        // Time units are only valid with RANGE, not ROWS
        assertFail(
                "f(c) over (partition by b order by a rows 10 day preceding)",
                45,
                "time units are only valid with RANGE frames, not ROWS or GROUPS"
        );
    }

    @Test
    public void testWindowFrameUnboundedFollowingAsStart() {
        // UNBOUNDED FOLLOWING cannot be frame start
        assertFail(
                "avg(a) over(partition by b order by ts rows between unbounded following and current row)",
                62,
                "frame start cannot be UNBOUNDED FOLLOWING, use UNBOUNDED PRECEDING"
        );
    }

    @Test
    public void testWindowFrameUnboundedPrecedingAsEnd() {
        // UNBOUNDED PRECEDING cannot be frame end
        assertFail(
                "avg(a) over(partition by b order by ts rows between current row and unbounded preceding)",
                78,
                "frame end cannot be UNBOUNDED PRECEDING, use UNBOUNDED FOLLOWING"
        );
    }

    @Test
    public void testWindowFunctionArithmetic() throws SqlException {
        // RPN: left operand, right operand, operator
        x("row_number over () 1 +", "row_number() over () + 1");
    }

    @Test
    public void testWindowFunctionArithmeticWithOrderBy() throws SqlException {
        // Window function with ORDER BY plus arithmetic
        x("row_number over (order by ts) 1 +", "row_number() OVER (ORDER BY ts) + 1");
    }

    @Test
    public void testWindowFunctionAsArgumentToWindowFunction() throws SqlException {
        // Window function as argument to another window function
        // sum(row_number() OVER ()) OVER ()
        // Expected RPN: the inner window function is parsed first, then becomes argument to outer
        x("row_number over () sum over ()",
                "sum(row_number() OVER ()) OVER ()");
    }

    @Test
    public void testWindowFunctionCumulative() throws SqlException {
        // CUMULATIVE is shorthand for ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        x("x sum over (order by ts rows between unbounded preceding and current row)",
                "sum(x) over (order by ts cumulative)");
    }

    @Test
    public void testWindowFunctionCumulativeCaseInsensitive() throws SqlException {
        // CUMULATIVE is case-insensitive
        x("x sum over (order by ts rows between unbounded preceding and current row)",
                "sum(x) over (order by ts CUMULATIVE)");
    }

    @Test
    public void testWindowFunctionCumulativeVwap() throws SqlException {
        // VWAP: cumulative sum of (price * volume) / cumulative sum of volume, partitioned by symbol
        // ORDER BY timestamp is explicit but will be ignored by optimizer for time-series tables
        x("price volume * sum over (partition by symbol order by timestamp rows between unbounded preceding and current row) " +
                        "volume sum over (partition by symbol order by timestamp rows between unbounded preceding and current row) /",
                "sum(price * volume) over (partition by symbol order by timestamp cumulative) / " +
                        "sum(volume) over (partition by symbol order by timestamp cumulative)");
    }

    @Test
    public void testWindowFunctionCumulativeWithPartitionBy() throws SqlException {
        // CUMULATIVE with PARTITION BY
        x("x sum over (partition by symbol order by ts rows between unbounded preceding and current row)",
                "sum(x) over (partition by symbol order by ts cumulative)");
    }

    @Test
    public void testWindowFunctionCumulativeWithoutOrderBy() {
        // CUMULATIVE requires ORDER BY - error points to CUMULATIVE keyword
        assertFail(
                "sum(x) over (partition by y cumulative)",
                28,
                "CUMULATIVE requires an ORDER BY clause"
        );
    }

    @Test
    public void testWindowFunctionCumulativeWithoutOrderByNoPartition() {
        // CUMULATIVE requires ORDER BY even without PARTITION BY
        assertFail(
                "sum(x) over (cumulative)",
                13,
                "CUMULATIVE requires an ORDER BY clause"
        );
    }

    @Test
    public void testWindowFunctionDivision() throws SqlException {
        // Two window functions divided by each other
        x("x sum over (partition by y) x count over (partition by y) /",
                "sum(x) over (partition by y) / count(x) over (partition by y)");
    }

    @Test
    public void testWindowFunctionDivisionWithCastInPartitionBy() throws SqlException {
        // VWAP-style: two window functions with :: cast in partition by
        // RPN for ts::date is "ts date ::" (operand, type, cast operator)
        x("price volume * sum over (partition by symbol, ts date :: order by ts) volume sum over (partition by symbol, ts date :: order by ts) /",
                "sum(price * volume) over (partition by symbol, ts::date order by ts) / sum(volume) over (partition by symbol, ts::date order by ts)");
    }

    @Test
    public void testWindowFunctionFrameBoundAbruptEnd() {
        assertFail(
                "sum(x) over (order by ts rows between",
                30,
                "'unbounded', 'current' or expression expected"
        );
    }

    @Test
    public void testWindowFunctionFrameBoundExprAbruptEnd() {
        assertFail(
                "sum(x) over (order by ts rows",
                25,
                "'between', 'unbounded', 'current' or expression expected"
        );
    }

    // Tests for window function argument ordering
    // For regular functions: f(a, b, c) -> RPN: "a b c f"
    // Window functions should follow the same pattern

    @Test
    public void testWindowFunctionFrameBoundInvalidOperator() {
        assertFail(
                "sum(x) over (order by ts rows between + preceding and current row)",
                38,
                "too few arguments for '+'"
        );
    }

    @Test
    public void testWindowFunctionIgnoreNullTypo() {
        // Common typo: "null" instead of "nulls"
        assertFail(
                "lag(d, 1) ignore null over () from tab",
                17,
                "'nulls' expected, not 'null'"
        );
    }

    @Test
    public void testWindowFunctionIgnoreNulls() throws SqlException {
        x("i first_value ignore nulls over (order by ts)", "first_value(i) ignore nulls over (order by ts)");
    }

    @Test
    public void testWindowFunctionIgnoreNullsWithExclude() throws SqlException {
        x("j first_value ignore nulls over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row)",
                "first_value(j) ignore nulls over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row)");
    }

    @Test
    public void testWindowFunctionIgnoreNullsWithoutOver() {
        // IGNORE NULLS requires OVER clause
        assertFail(
                "lag(1.0) ignore nulls from trades",
                22,
                "'over' expected after 'nulls'"
        );
    }

    @Test
    public void testWindowFunctionIgnoreWithoutNulls() {
        // IGNORE must be followed by NULLS
        assertFail(
                "lag(d, 1) ignore over () from tab",
                17,
                "'nulls' expected after 'ignore'"
        );
    }

    @Test
    public void testWindowFunctionInBothThenAndElse() throws SqlException {
        // Window function in both THEN and ELSE (arguments appear before function names in RPN)
        x(" x 0 > x sum over (partition by a) x avg over (order by ts) case",
                "case when x > 0 then sum(x) over (partition by a) else avg(x) over (order by ts) end");
    }

    @Test
    public void testWindowFunctionInCaseDifferentOverSpecs() throws SqlException {
        // Window functions with different OVER specs in THEN vs ELSE
        x(" x 0 > x sum over (partition by a order by ts) x sum over (order by ts) case",
                "CASE WHEN x > 0 THEN sum(x) OVER (PARTITION BY a ORDER BY ts) ELSE sum(x) OVER (ORDER BY ts) END");
    }

    @Test
    public void testWindowFunctionInCaseExpression() throws SqlException {
        // Window function inside CASE WHEN expression - two occurrences
        // Both window functions should be parsed with their OVER clauses
        x(" row_number over (order by ts) 1 = 'first' row_number over (order by ts) 3 = 'last' 'middle' case",
                "CASE WHEN row_number() OVER (ORDER BY ts) = 1 THEN 'first' WHEN row_number() OVER (ORDER BY ts) = 3 THEN 'last' ELSE 'middle' END");
    }

    @Test
    public void testWindowFunctionInCaseThenClause() throws SqlException {
        // Window function in THEN clause (x argument appears before sum in RPN)
        x(" x 0 > x sum over (partition by y) 0 case", "case when x > 0 then sum(x) over (partition by y) else 0 end");
    }

    @Test
    public void testWindowFunctionInCaseWhenCondition() throws SqlException {
        // Window function in WHEN condition (comparing with a value)
        x(" row_number over (partition by x) 5 > 1 0 case", "case when row_number() over (partition by x) > 5 then 1 else 0 end");
    }

    @Test
    public void testWindowFunctionInCaseWithCast() throws SqlException {
        // Window function with cast inside CASE
        x(" x 0 > row_number over (order by ts) string :: 'N/A' case",
                "CASE WHEN x > 0 THEN row_number() OVER (ORDER BY ts)::string ELSE 'N/A' END");
    }

    @Test
    public void testWindowFunctionInMultipleCaseBranches() throws SqlException {
        // Window functions in multiple CASE branches (arguments appear before function names in RPN)
        x(" a 1 = x sum over (order by ts) a 2 = x avg over (partition by b) 0 case",
                "case when a = 1 then sum(x) over (order by ts) when a = 2 then avg(x) over (partition by b) else 0 end");
    }

    @Test
    public void testWindowFunctionInSimpleCase() throws SqlException {
        // Simple CASE expression with window function
        x("x 1 row_number over (partition by y) 0 case",
                "case x when 1 then row_number() over (partition by y) else 0 end");
    }

    @Test
    public void testWindowFunctionInsideCase() throws SqlException {
        // Note: leading space is due to how CASE expressions are serialized in RPN
        x(" x 0 > 1 row_number over (partition by x) case", "case when x > 0 then 1 else row_number() over (partition by x) end");
    }

    @Test
    public void testWindowFunctionInvalidFrameKeyword() {
        // Invalid frame keyword should give a helpful error message listing valid options
        assertFail(
                "sum(x) over (order by ts invalid_keyword)",
                25,
                "'rows', 'range', 'groups', 'cumulative' or ')' expected"
        );
    }

    @Test
    public void testWindowFunctionLeadThreeArgs() throws SqlException {
        // lead(x, 2, -1) should produce RPN: "x 2 -1 lead over (...)"
        x("x 2 1 - lead over (order by ts)", "lead(x, 2, -1) over (order by ts)");
    }

    @Test
    public void testWindowFunctionMultipleArithmetic() throws SqlException {
        // Multiple window functions in arithmetic expression
        x("x sum over (order by ts) x lag over (order by ts) - x avg over (order by ts) /",
                "(sum(x) over (order by ts) - lag(x) over (order by ts)) / avg(x) over (order by ts)");
    }

    @Test
    public void testWindowFunctionNestedParenthesesWithCast() throws SqlException {
        // Deeply nested parentheses with cast
        x("row_number over (order by ts) int ::", "(((row_number() OVER (ORDER BY ts))))::int");
    }

    @Test
    public void testWindowFunctionNthValueTwoArgs() throws SqlException {
        // nth_value(x, 2) should produce RPN: "x 2 nth_value over (...)"
        x("x 2 nth_value over (order by ts)", "nth_value(x, 2) over (order by ts)");
    }

    @Test
    public void testWindowFunctionOrderByAbruptEnd() {
        // ORDER BY has explicit null check
        assertFail(
                "sum(x) over (order by",
                19,
                "Expression expected"
        );
    }

    @Test
    public void testWindowFunctionOrderByEmptyList() {
        // Empty ORDER BY list should fail - "order by" followed immediately by ')'
        assertFail(
                "sum(x) over (order by )",
                22,
                "Expression expected"
        );
    }

    @Test
    public void testWindowFunctionOrderByFourArgFunction() throws SqlException {
        // ORDER BY with a 4-argument function
        x("x sum over (order by d c b a func)", "sum(x) over (order by func(a, b, c, d))");
    }

    @Test
    public void testWindowFunctionOrderByMultipleThreeArgFunctions() throws SqlException {
        // ORDER BY with multiple 3-argument functions
        x("x sum over (order by c b a f1, f e d f2 desc)",
                "sum(x) over (order by f1(a, b, c), f2(d, e, f) desc)");
    }

    @Test
    public void testWindowFunctionOrderByNestedThreeArgFunction() throws SqlException {
        // ORDER BY with nested function calls
        x("x sum over (order by a b inner c outer)",
                "sum(x) over (order by outer(inner(a, b), c))");
    }

    @Test
    public void testWindowFunctionOrderByOnlyComma() {
        // Just a comma after ORDER BY should fail
        assertFail(
                "sum(x) over (order by ,)",
                22,
                "Expression expected"
        );
    }

    @Test
    public void testWindowFunctionOrderByThreeArgFunction() throws SqlException {
        // ORDER BY with a 3-argument function - args should be in correct order
        x("x sum over (order by c b a func)", "sum(x) over (order by func(a, b, c))");
    }

    @Test
    public void testWindowFunctionOrderByThreeArgFunctionDesc() throws SqlException {
        // ORDER BY with a 3-argument function and DESC
        x("x sum over (order by c b a func desc)", "sum(x) over (order by func(a, b, c) desc)");
    }

    @Test
    public void testWindowFunctionOrderByThreeArgFunctionWithExpressions() throws SqlException {
        // ORDER BY with 3-arg function containing expressions
        x("x sum over (order by c 3 - b 2 * a 1 + func)",
                "sum(x) over (order by func(a + 1, b * 2, c - 3))");
    }

    @Test
    public void testWindowFunctionOrderByTrailingComma() {
        // Trailing comma in ORDER BY should fail - "order by a," followed by ')'
        assertFail(
                "sum(x) over (order by a,)",
                24,
                "Expression expected"
        );
    }

    @Test
    public void testWindowFunctionOrderByTrailingCommaBeforeRows() {
        // Trailing comma in ORDER BY should fail - "order by a," followed by ROWS
        // Note: "rows" gets parsed as an identifier/expression, then fails on "between"
        assertFail(
                "sum(x) over (order by a, rows between unbounded preceding and current row)",
                30,
                "too few arguments for 'between'"
        );
    }

    // Tests to prove PARTITION BY parsing bug - empty lists and trailing commas should be rejected
    // See https://github.com/questdb/questdb/pull/6626

    @Test
    public void testWindowFunctionOrderByTwoArgFunction() throws SqlException {
        // ORDER BY with a 2-argument function
        x("x sum over (order by a b func)", "sum(x) over (order by func(a, b))");
    }

    @Test
    public void testWindowFunctionPartitionByAbruptEnd() {
        assertFail(
                "sum(x) over (partition by",
                23,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionByEmptyList() {
        // Empty PARTITION BY list should fail - "partition by" followed immediately by ORDER
        assertFail(
                "sum(x) over (partition by order by ts)",
                26,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionByEmptyListWithCloseParen() {
        // Empty PARTITION BY list should fail - "partition by" followed immediately by ')'
        assertFail(
                "sum(x) over (partition by )",
                26,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionByExpression() throws SqlException {
        x("i avg over (partition by sym '%aaa%' like order by ts)", "avg(i) over (partition by sym like '%aaa%' order by ts)");
    }

    @Test
    public void testWindowFunctionPartitionByFourArgFunction() throws SqlException {
        // PARTITION BY with a 4-argument function
        x("x sum over (partition by d c b a func)", "sum(x) over (partition by func(a, b, c, d))");
    }

    // Tests to check if ORDER BY has the same bug as PARTITION BY
    // (empty lists and trailing commas)

    @Test
    public void testWindowFunctionPartitionByFunctionCall() throws SqlException {
        x("row_number over (partition by 'y' ts timestamp_floor order by temp desc)",
                "row_number() over (partition by timestamp_floor('y', ts) order by temp desc)");
    }

    @Test
    public void testWindowFunctionPartitionByInvalidOperator() {
        assertFail(
                "sum(x) over (partition by +)",
                26,
                "too few arguments for '+'"
        );
    }

    @Test
    public void testWindowFunctionPartitionByInvalidOperator2() {
        assertFail(
                "sum(x) over (partition by *)",
                26,
                "too few arguments for '*'"
        );
    }

    @Test
    public void testWindowFunctionPartitionByKeywordAsExpr() {
        assertFail(
                "sum(x) over (partition by from)",
                26,
                "table and column names that are SQL keywords have to be enclosed in double quotes"
        );
    }

    // Tests for ORDER BY with multi-argument function expressions

    @Test
    public void testWindowFunctionPartitionByMultipleTrailingCommas() {
        // Multiple trailing commas should fail
        assertFail(
                "sum(x) over (partition by a,,)",
                29,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionByOnlyComma() {
        // Just a comma after PARTITION BY should fail
        assertFail(
                "sum(x) over (partition by ,)",
                27,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionBySelectAsExpr() {
        assertFail(
                "sum(x) over (partition by select)",
                33,
                "[distinct] column expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionByThreeArgFunction() throws SqlException {
        // PARTITION BY with a 3-argument function - args should be in correct order
        x("x sum over (partition by c b a func)", "sum(x) over (partition by func(a, b, c))");
    }

    @Test
    public void testWindowFunctionPartitionByTrailingComma() {
        // Trailing comma in PARTITION BY should fail - "partition by a," followed by ')'
        assertFail(
                "sum(x) over (partition by a,)",
                28,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionPartitionByTrailingCommaBeforeOrder() {
        // Trailing comma in PARTITION BY should fail - "partition by a," followed by ORDER
        assertFail(
                "sum(x) over (partition by a, order by ts)",
                29,
                "column name expected"
        );
    }

    @Test
    public void testWindowFunctionRangeFrameWithTimeUnit() throws SqlException {
        x("ts max over (order by ts range between '12' hour preceding and current row)",
                "max(ts) over (order by ts range between '12' hour preceding and current row)");
    }

    // Tests for PARTITION BY with multi-argument function expressions

    @Test
    public void testWindowFunctionRespectWithoutNulls() {
        // RESPECT must be followed by NULLS
        assertFail(
                "lag(d, 1) respect over () from tab",
                18,
                "'nulls' expected after 'respect'"
        );
    }

    @Test
    public void testWindowFunctionRowsFrame() throws SqlException {
        x("ts max over (order by ts rows between unbounded preceding and current row)",
                "max(ts) over (order by ts rows between unbounded preceding and current row)");
    }

    @Test
    public void testWindowFunctionRowsFrameExcludeCurrentRow() throws SqlException {
        x("j avg over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row)",
                "avg(j) over (partition by i order by ts rows between 2 preceding and 1 preceding exclude current row)");
    }

    // Tests to verify parseWindowExpr error handling
    // All these produce proper SqlException errors (no NPEs)

    // Window function tests
    @Test
    public void testWindowFunctionSimple() throws SqlException {
        x("row_number over ()", "row_number() over ()");
    }

    @Test
    public void testWindowFunctionThreeArgs() throws SqlException {
        // lag(x, 1, 0) should produce RPN: "x 1 0 lag over (...)"
        // This tests if 3+ argument window functions have correct arg order
        x("x 1 0 lag over (order by ts)", "lag(x, 1, 0) over (order by ts)");
    }

    @Test
    public void testWindowFunctionThreeArgsWithExpressions() throws SqlException {
        // lag(a + b, 1, c * 2) - more complex expressions as arguments
        x("a b + 1 c 2 * lag over (order by ts)", "lag(a + b, 1, c * 2) over (order by ts)");
    }

    @Test
    public void testWindowFunctionTwoArgs() throws SqlException {
        // lag(x, 1) should produce RPN: "x 1 lag over (...)"
        x("x 1 lag over (order by ts)", "lag(x, 1) over (order by ts)");
    }

    @Test
    public void testWindowFunctionUnterminatedOverClause() {
        // Missing closing parenthesis for OVER clause
        assertFail(
                "f(c) over (partition by b order by ts",
                35,
                "')' expected to close OVER clause"
        );
    }

    @Test
    public void testWindowFunctionWithArithmeticInCaseBranches() throws SqlException {
        // Window function with arithmetic operations in CASE branches
        x(" x 0 > row_number over (order by ts) 1 + row_number over (order by ts) 1 - case",
                "CASE WHEN x > 0 THEN row_number() OVER (ORDER BY ts) + 1 ELSE row_number() OVER (ORDER BY ts) - 1 END");
    }

    @Test
    public void testWindowFunctionWithCast() throws SqlException {
        // Window function with :: cast operator - no parentheses
        // RPN: window_func, type, cast_operator
        x("row_number over (order by ts) string ::",
                "row_number() over (order by ts)::string");
    }

    @Test
    public void testWindowFunctionWithCastInParens() throws SqlException {
        // Window function with :: cast operator - with outer parentheses
        x("row_number over (order by ts) string ::",
                "(row_number() over (order by ts))::string");
    }

    @Test
    public void testWindowFunctionWithOrderBy() throws SqlException {
        x("ts max over (order by ts)", "max(ts) over (order by ts)");
    }

    // Tests for window function with cast operator
    // ExpressionParser correctly parses these - the bug is in SqlParser

    @Test
    public void testWindowFunctionWithOrderByDesc() throws SqlException {
        x("ts max over (order by ts desc)", "max(ts) over (order by ts desc)");
    }

    @Test
    public void testWindowFunctionWithPartitionAndOrder() throws SqlException {
        x("x row_number over (partition by sym order by ts)", "row_number(x) over (partition by sym order by ts)");
    }

    @Test
    public void testWindowFunctionWithPartitionBy() throws SqlException {
        x("x row_number over (partition by x)", "row_number(x) over (partition by x)");
    }

    private void assertFail(String content, int pos, String contains) {
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.testParseExpression(content, rpnBuilder);
            Assert.fail("expected exception");
        } catch (SqlException e) {
            // Empty contains means "show me what error I get" - always fail with details
            if (contains.isEmpty()) {
                Assert.fail("Position: " + e.getPosition() + ", Message: " + e.getFlyweightMessage());
            }
            assertEquals(pos, e.getPosition());
            if (!Chars.contains(e.getFlyweightMessage(), contains)) {
                Assert.fail(e.getMessage() + " does not contain '" + contains + '\'');
            }
        }
    }

    private void x(CharSequence expectedRpn, String content) throws SqlException {
        rpnBuilder.reset();
        try (SqlCompiler compiler = engine.getSqlCompiler()) {
            compiler.testParseExpression(content, rpnBuilder);
        }
        TestUtils.assertEquals(expectedRpn, rpnBuilder.rpn());
    }
}
