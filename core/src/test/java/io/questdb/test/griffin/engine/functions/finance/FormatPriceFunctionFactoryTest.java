/*+*****************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
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
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.finance.FormatPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class FormatPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    @Test
    public void testHalfPoint() throws Exception {
        assertQuery("select format_price(100.5, 32)").expectSize(true).returns("format_price\n100-16\n");
    }

    @Test
    public void testWholeNumber() throws Exception {
        assertQuery("select format_price(101.0, 32)").expectSize(true).returns("format_price\n101-00\n");
    }

    @Test
    public void testIntegerInput() throws Exception {
        assertQuery("select format_price(101, 32)").expectSize(true).returns("format_price\n101-00\n");
    }

    @Test
    public void testThreeEighths() throws Exception {
        assertQuery("select format_price(99.375, 32)").expectSize(true).returns("format_price\n99-12\n");
    }

    @Test
    public void testNineThirtySeconds() throws Exception {
        assertQuery("select format_price(102.28125, 32)").expectSize(true).returns("format_price\n102-09\n");
    }

    @Test
    public void testMaxFractionalTick32() throws Exception {
        assertQuery("select format_price(99.96875, 32)").expectSize(true).returns("format_price\n99-31\n");
    }

    @Test
    public void testSingleDigitWholePart() throws Exception {
        assertQuery("select format_price(1.5, 32)").expectSize(true).returns("format_price\n1-16\n");
    }

    @Test
    public void testZeroPrice() throws Exception {
        assertQuery("select format_price(0.0, 32)").expectSize(true).returns("format_price\n0-00\n");
    }

    @Test
    public void testTickSize2() throws Exception {
        assertQuery("select format_price(100.5, 2)").expectSize(true).returns("format_price\n100-1\n");
    }

    @Test
    public void testTickSize2ZeroFractional() throws Exception {
        assertQuery("select format_price(100.0, 2)").expectSize(true).returns("format_price\n100-0\n");
    }

    @Test
    public void testTickSize4() throws Exception {
        assertQuery("select format_price(100.25, 4)").expectSize(true).returns("format_price\n100-1\n");
    }

    @Test
    public void testTickSize8() throws Exception {
        assertQuery("select format_price(100.5, 8)").expectSize(true).returns("format_price\n100-4\n");
    }

    @Test
    public void testTickSize16() throws Exception {
        assertQuery("select format_price(100.5, 16)").expectSize(true).returns("format_price\n100-08\n");
    }

    @Test
    public void testTickSize16SingleDigitPadded() throws Exception {
        assertQuery("select format_price(100.0625, 16)").expectSize(true).returns("format_price\n100-01\n");
    }

    @Test
    public void testTickSize64() throws Exception {
        assertQuery("select format_price(100.5, 64)").expectSize(true).returns("format_price\n100-32\n");
    }

    @Test
    public void testTickSize64SingleDigitPadded() throws Exception {
        assertQuery("select format_price(100.015625, 64)").expectSize(true).returns("format_price\n100-01\n");
    }

    @Test
    public void testTickSize128() throws Exception {
        assertQuery("select format_price(100.5, 128)").expectSize(true).returns("format_price\n100-064\n");
    }

    @Test
    public void testTickSize128SingleDigitPadded() throws Exception {
        assertQuery("select format_price(100.0078125, 128)").expectSize(true).returns("format_price\n100-001\n");
    }

    @Test
    public void testTickSize1000() throws Exception {
        assertQuery("select format_price(100.5, 1000)").expectSize(true).returns("format_price\n100-500\n");
    }

    @Test
    public void testTickSize1000SingleDigitPadded() throws Exception {
        assertQuery("select format_price(100.001, 1000)").expectSize(true).returns("format_price\n100-001\n");
    }

    @Test
    public void testTickSize1() throws Exception {
        assertQuery("select format_price(100.0, 1)").expectSize(true).returns("format_price\n100-0\n");
    }

    @Test
    public void testNegativeHalfPoint() throws Exception {
        assertQuery("select format_price(-100.5, 32)").expectSize(true).returns("format_price\n-100-16\n");
    }

    @Test
    public void testNegativeWholeNumber() throws Exception {
        assertQuery("select format_price(-101.0, 32)").expectSize(true).returns("format_price\n-101-00\n");
    }

    @Test
    public void testNegativePriceTickSize16() throws Exception {
        assertQuery("select format_price(-50.5, 16)").expectSize(true).returns("format_price\n-50-08\n");
    }

    @Test
    public void testNegativePriceTickSize128() throws Exception {
        assertQuery("select format_price(-100.5, 128)").expectSize(true).returns("format_price\n-100-064\n");
    }

    @Test
    public void testNegativeZeroWholePart() throws Exception {
        assertQuery("select format_price(-0.5, 32)").expectSize(true).returns("format_price\n-0-16\n");
    }

    @Test
    public void testNegativePriceSmallTickSize() throws Exception {
        assertQuery("select format_price(-5.5, 2)").expectSize(true).returns("format_price\n-5-1\n");
    }

    @Test
    public void testNullTickSize() throws Exception {
        assertQuery("select format_price(1.0, NULL) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNullTickSizeWithFractional() throws Exception {
        assertQuery("select format_price(1.5, NULL) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testBothNull() throws Exception {
        assertQuery("select format_price(NULL, NULL) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNullPrice() throws Exception {
        assertQuery("select format_price(NULL, 32) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNullPriceNonNullTick() throws Exception {
        assertQuery("select format_price(NULL, 16) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNegativeTickSize() throws Exception {
        assertQuery("select format_price(1.0, -2) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNegativeTickSizeMinusOne() throws Exception {
        assertQuery("select format_price(1.5, -1) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testZeroTickSize() throws Exception {
        assertQuery("select format_price(100.5, 0) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNegativePriceNullTick() throws Exception {
        assertQuery("select format_price(-100.5, NULL) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testNegativePriceZeroTick() throws Exception {
        assertQuery("select format_price(-100.5, 0) is null is_null").expectSize(true).returns("is_null\ntrue\n");
    }

    @Test
    public void testRoundingUp() throws Exception {
        assertQuery("select format_price(100.484375, 32)").expectSize(true).returns("format_price\n100-16\n");
    }

    @Test
    public void testRoundingDown() throws Exception {
        assertQuery("select format_price(100.46875, 32)").expectSize(true).returns("format_price\n100-15\n");
    }

    @Test
    public void testExactMidpointRoundsUp() throws Exception {
        assertQuery("select format_price(100.015625, 32)").expectSize(true).returns("format_price\n100-01\n");
    }

    @Test
    public void testMultiRowConstantTickSize32() throws Exception {
        assertQuery("select format_price(x * 10.0 + 0.5, 32) from long_sequence(5) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "10-16\n" +
                        "20-16\n" +
                        "30-16\n" +
                        "40-16\n" +
                        "50-16\n"
        );
    }

    @Test
    public void testMultiRowConstantTickSize16() throws Exception {
        assertQuery("select format_price(x * 10.0 + 0.5, 16) from long_sequence(5) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "10-08\n" +
                        "20-08\n" +
                        "30-08\n" +
                        "40-08\n" +
                        "50-08\n"
        );
    }

    @Test
    public void testMultiRowMixedFractionals() throws Exception {
        assertQuery("select format_price(1.0 + (x-1) * 0.125, 8) from long_sequence(8) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "1-0\n" +
                        "1-1\n" +
                        "1-2\n" +
                        "1-3\n" +
                        "1-4\n" +
                        "1-5\n" +
                        "1-6\n" +
                        "1-7\n"
        );
    }

    @Test
    public void testMultiRowOrderedResultsTickSize32() throws Exception {
        assertQuery("select format_price(100.0 + (x-1)/32.0, 32) from long_sequence(32) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "100-00\n" +
                        "100-01\n" +
                        "100-02\n" +
                        "100-03\n" +
                        "100-04\n" +
                        "100-05\n" +
                        "100-06\n" +
                        "100-07\n" +
                        "100-08\n" +
                        "100-09\n" +
                        "100-10\n" +
                        "100-11\n" +
                        "100-12\n" +
                        "100-13\n" +
                        "100-14\n" +
                        "100-15\n" +
                        "100-16\n" +
                        "100-17\n" +
                        "100-18\n" +
                        "100-19\n" +
                        "100-20\n" +
                        "100-21\n" +
                        "100-22\n" +
                        "100-23\n" +
                        "100-24\n" +
                        "100-25\n" +
                        "100-26\n" +
                        "100-27\n" +
                        "100-28\n" +
                        "100-29\n" +
                        "100-30\n" +
                        "100-31\n"
        );
    }

    @Test
    public void testMultiRowNullsAndValidMixed() throws Exception {
        assertQuery("select format_price(x * 1.0, case when x % 2 = 0 then NULL else 32 end) from long_sequence(5) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "\n" +
                        "\n" +
                        "1-00\n" +
                        "3-00\n" +
                        "5-00\n"
        );
    }

    @Test
    public void testMultiRowNegativePrices() throws Exception {
        assertQuery("select format_price(-x * 1.0, 32) from long_sequence(5) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "-1-00\n" +
                        "-2-00\n" +
                        "-3-00\n" +
                        "-4-00\n" +
                        "-5-00\n"
        );
    }

    @Test
    public void testMultiRowDifferentTickSizes() throws Exception {
        assertQuery("select format_price(x * 1.0, case when x % 2 = 0 then 32 else 2 end) from long_sequence(5) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "1-0\n" +
                        "2-00\n" +
                        "3-0\n" +
                        "4-00\n" +
                        "5-0\n"
        );
    }

    @Test
    public void testNonConstantTickFromColumn() throws Exception {
        assertQuery("select format_price(100.5, tick) from (select 2 as tick union all select 16 union all select 32) order by 1").expectSize(true).returns(
                "format_price\n" +
                        "100-08\n" +
                        "100-1\n" +
                        "100-16\n"
        );
    }

    @Test
    public void testConstantTickWidthBoundaryAt10() throws Exception {
        assertQuery("select format_price(100.5, 10)").expectSize(true).returns("format_price\n100-5\n");
    }

    @Test
    public void testConstantTickWidthBoundaryAt11() throws Exception {
        assertQuery("select format_price(100.5, 11)").expectSize(true).returns("format_price\n100-06\n");
    }

    @Test
    public void testConstantTickWidthBoundaryAt100() throws Exception {
        assertQuery("select format_price(100.5, 100)").expectSize(true).returns("format_price\n100-50\n");
    }

    @Test
    public void testConstantTickWidthBoundaryAt101() throws Exception {
        assertQuery("select format_price(100.5, 101)").expectSize(true).returns("format_price\n100-051\n");
    }

    @Test
    public void testLargePriceValue() throws Exception {
        assertQuery("select format_price(999999.5, 32)").expectSize(true).returns("format_price\n999999-16\n");
    }

    @Test
    public void testLargeNegativePrice() throws Exception {
        assertQuery("select format_price(-999999.5, 32)").expectSize(true).returns("format_price\n-999999-16\n");
    }

    @Test
    public void testOneThirtySecond() throws Exception {
        assertQuery("select format_price(100.03125, 32)").expectSize(true).returns("format_price\n100-01\n");
    }

    @Test
    public void testThirtyOneThirtySeconds() throws Exception {
        assertQuery("select format_price(100.96875, 32)").expectSize(true).returns("format_price\n100-31\n");
    }

    @Test
    public void testMultipleSinksInSameRow() throws Exception {
        assertQuery("select format_price(100.5, 32) as p1, format_price(50.25, 32) as p2")
                .expectSize(true)
                .returns(
                        "p1\tp2\n" +
                                "100-16\t50-08\n"
                );
    }

    @Test
    public void testStringConcatenationSinks() throws Exception {
        assertQuery("select concat(format_price(1.5, 32), ' vs ', format_price(2.5, 32)) as matchup")
                .expectSize(true)
                .returns(
                        "matchup\n" +
                                "1-16 vs 2-16\n"
                );
    }

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new FormatPriceFunctionFactory();
    }
}
