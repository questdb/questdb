/*+*****************************************************************************
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

package io.questdb.test.griffin.engine.functions.finance;

import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.engine.functions.finance.FormatPriceFunctionFactory;
import io.questdb.test.griffin.engine.AbstractFunctionFactoryTest;
import org.junit.Test;

public class FormatPriceFunctionFactoryTest extends AbstractFunctionFactoryTest {

    // =========================================================================
    // 1. BASIC HAPPY PATH — tick size 32 (width = 2, pads to "00".."31")
    // =========================================================================

    @Test
    public void testHalfPoint() throws Exception {
        // 100.5 → fractional = 0.5, ticks = round(0.5 * 32) = 16 → "100-16"
        assertQuery("format_price\n100-16\n", "select format_price(100.5, 32)");
    }

    @Test
    public void testWholeNumber() throws Exception {
        // 101.0 → fractional = 0.0, ticks = 0, zero-padded to width 2 → "101-00"
        assertQuery("format_price\n101-00\n", "select format_price(101.0, 32)");
    }

    @Test
    public void testIntegerInput() throws Exception {
        // Integer literal cast to double → same as 101.0
        assertQuery("format_price\n101-00\n", "select format_price(101, 32)");
    }

    @Test
    public void testThreeEighths() throws Exception {
        // 99.375 → fractional = 0.375, ticks = round(0.375 * 32) = 12 → "99-12"
        assertQuery("format_price\n99-12\n", "select format_price(99.375, 32)");
    }

    @Test
    public void testNineThirtySeconds() throws Exception {
        // 102.28125 → fractional = 0.28125, ticks = round(0.28125 * 32) = 9 → "102-09"
        assertQuery("format_price\n102-09\n", "select format_price(102.28125, 32)");
    }

    @Test
    public void testMaxFractionalTick32() throws Exception {
        // 99.96875 → 0.96875 * 32 = 31 → "99-31"
        assertQuery("format_price\n99-31\n", "select format_price(99.96875, 32)");
    }

    @Test
    public void testSingleDigitWholePart() throws Exception {
        // 1.5, tick=32 → ticks = 16 → "1-16"
        assertQuery("format_price\n1-16\n", "select format_price(1.5, 32)");
    }

    @Test
    public void testZeroPrice() throws Exception {
        // 0.0, tick=32 → "0-00"
        assertQuery("format_price\n0-00\n", "select format_price(0.0, 32)");
    }

    // =========================================================================
    // 2. DIFFERENT TICK SIZES — reviewer explicitly requested this
    // =========================================================================

    @Test
    public void testTickSize2() throws Exception {
        // tick=2, width = digits(2-1) = digits(1) = 1
        // 100.5 → ticks = round(0.5 * 2) = 1 → "100-1"
        assertQuery("format_price\n100-1\n", "select format_price(100.5, 2)");
    }

    @Test
    public void testTickSize2ZeroFractional() throws Exception {
        // 100.0, tick=2 → ticks = 0, width=1 → "100-0"
        assertQuery("format_price\n100-0\n", "select format_price(100.0, 2)");
    }

    @Test
    public void testTickSize4() throws Exception {
        // tick=4, width = digits(3) = 1
        // 100.25 → ticks = round(0.25 * 4) = 1 → "100-1"
        assertQuery("format_price\n100-1\n", "select format_price(100.25, 4)");
    }

    @Test
    public void testTickSize8() throws Exception {
        // tick=8, width = digits(7) = 1
        // 100.5 → ticks = round(0.5 * 8) = 4 → "100-4"
        assertQuery("format_price\n100-4\n", "select format_price(100.5, 8)");
    }

    @Test
    public void testTickSize16() throws Exception {
        // tick=16, width = digits(15) = 2
        // 100.5 → ticks = round(0.5 * 16) = 8 → "100-08"
        assertQuery("format_price\n100-08\n", "select format_price(100.5, 16)");
    }

    @Test
    public void testTickSize16SingleDigitPadded() throws Exception {
        // 100.0625, tick=16 → ticks = round(0.0625 * 16) = 1 → "100-01"
        assertQuery("format_price\n100-01\n", "select format_price(100.0625, 16)");
    }

    @Test
    public void testTickSize64() throws Exception {
        // tick=64, width = digits(63) = 2
        // 100.5 → ticks = round(0.5 * 64) = 32 → "100-32"
        assertQuery("format_price\n100-32\n", "select format_price(100.5, 64)");
    }

    @Test
    public void testTickSize64SingleDigitPadded() throws Exception {
        // 100.015625, tick=64 → ticks = round(0.015625 * 64) = 1 → "100-01"
        assertQuery("format_price\n100-01\n", "select format_price(100.015625, 64)");
    }

    @Test
    public void testTickSize128() throws Exception {
        // tick=128, width = digits(127) = 3
        // 100.5 → ticks = round(0.5 * 128) = 64 → "100-064"
        assertQuery("format_price\n100-064\n", "select format_price(100.5, 128)");
    }

    @Test
    public void testTickSize128SingleDigitPadded() throws Exception {
        // 100.0078125, tick=128 → ticks = round(0.0078125 * 128) = 1 → "100-001"
        assertQuery("format_price\n100-001\n", "select format_price(100.0078125, 128)");
    }

    @Test
    public void testTickSize1000() throws Exception {
        // tick=1000, width = digits(999) = 3
        // 100.5 → ticks = round(0.5 * 1000) = 500 → "100-500"
        assertQuery("format_price\n100-500\n", "select format_price(100.5, 1000)");
    }

    @Test
    public void testTickSize1000SingleDigitPadded() throws Exception {
        // 100.001, tick=1000 → ticks = round(0.001 * 1000) = 1 → "100-001"
        assertQuery("format_price\n100-001\n", "select format_price(100.001, 1000)");
    }

    @Test
    public void testTickSize1() throws Exception {
        // tick=1: calculateWidth(1) → temp=0, loop runs once → width=1
        // 100.0 → ticks = 0 → "100-0"
        assertQuery("format_price\n100-0\n", "select format_price(100.0, 1)");
    }

    // =========================================================================
    // 3. NEGATIVE PRICE — new behavior added in updated version
    // =========================================================================

    @Test
    public void testNegativeHalfPoint() throws Exception {
        // -100.5, tick=32 → abs=100.5, ticks=16 → "-100-16"
        assertQuery("format_price\n-100-16\n", "select format_price(-100.5, 32)");
    }

    @Test
    public void testNegativeWholeNumber() throws Exception {
        // -101.0, tick=32 → "-101-00"
        assertQuery("format_price\n-101-00\n", "select format_price(-101.0, 32)");
    }

    @Test
    public void testNegativePriceTickSize16() throws Exception {
        // -50.5, tick=16 → ticks=8 → "-50-08"
        assertQuery("format_price\n-50-08\n", "select format_price(-50.5, 16)");
    }

    @Test
    public void testNegativePriceTickSize128() throws Exception {
        // -100.5, tick=128 → ticks=64 → "-100-064"
        assertQuery("format_price\n-100-064\n", "select format_price(-100.5, 128)");
    }

    @Test
    public void testNegativeZeroWholePart() throws Exception {
        // -0.5, tick=32 → whole=0, ticks=16 → "-0-16"
        assertQuery("format_price\n-0-16\n", "select format_price(-0.5, 32)");
    }

    @Test
    public void testNegativePriceSmallTickSize() throws Exception {
        // -5.5, tick=2 → ticks=1, width=1 → "-5-1"
        assertQuery("format_price\n-5-1\n", "select format_price(-5.5, 2)");
    }

    // =========================================================================
    // 4. NULL / INVALID INPUTS — returns empty varchar (NOT "NULL" string)
    //    Key fix from reviewer: use empty Utf8StringSink, not String.format "NULL"
    // =========================================================================

    @Test
    public void testNullTickSize() throws Exception {
        assertQuery("format_price\n\n", "select format_price(1.0, NULL)");
    }

    @Test
    public void testNullTickSizeWithFractional() throws Exception {
        assertQuery("format_price\n\n", "select format_price(1.5, NULL)");
    }

    @Test
    public void testBothNull() throws Exception {
        assertQuery("format_price\n\n", "select format_price(NULL, NULL)");
    }

    @Test
    public void testNullPrice() throws Exception {
        assertQuery("format_price\n\n", "select format_price(NULL, 32)");
    }

    @Test
    public void testNullPriceNonNullTick() throws Exception {
        assertQuery("format_price\n\n", "select format_price(NULL, 16)");
    }

    @Test
    public void testNegativeTickSize() throws Exception {
        assertQuery("format_price\n\n", "select format_price(1.0, -2)");
    }

    @Test
    public void testNegativeTickSizeMinusOne() throws Exception {
        assertQuery("format_price\n\n", "select format_price(1.5, -1)");
    }

    @Test
    public void testZeroTickSize() throws Exception {
        // tick=0 is invalid (tick <= 0 guard)
        assertQuery("format_price\n\n", "select format_price(100.5, 0)");
    }

    @Test
    public void testNegativePriceNullTick() throws Exception {
        assertQuery("format_price\n\n", "select format_price(-100.5, NULL)");
    }

    @Test
    public void testNegativePriceZeroTick() throws Exception {
        assertQuery("format_price\n\n", "select format_price(-100.5, 0)");
    }

    // =========================================================================
    // 5. ROUNDING BEHAVIOR — Math.round on fractional ticks
    // =========================================================================

    @Test
    public void testRoundingUp() throws Exception {
        // 0.484375 * 32 = 15.5 → rounds to 16 → "100-16"
        assertQuery("format_price\n100-16\n", "select format_price(100.484375, 32)");
    }

    @Test
    public void testRoundingDown() throws Exception {
        // 0.46875 * 32 = 15.0 → "100-15"
        assertQuery("format_price\n100-15\n", "select format_price(100.46875, 32)");
    }

    @Test
    public void testExactMidpointRoundsUp() throws Exception {
        // 0.015625 * 32 = 0.5 → rounds to 1 → "100-01"
        assertQuery("format_price\n100-01\n", "select format_price(100.015625, 32)");
    }

    // =========================================================================
    // 6. MULTI-ROW RESULT SETS — explicitly requested by reviewer:
    //    "select format_price(rnd_double() * 100, 16) from long_sequence(50) order by 1"
    //
    //    NOTE: ORDER BY on a varchar column sorts lexicographically, not numerically.
    //    All expected values below reflect actual string sort order.
    // =========================================================================

    @Test
    public void testMultiRowConstantTickSize32() throws Exception {
        // x * 10.0 + 0.5 for x=1..5 → all have ticks=16 with tick=32
        // Results: "10-16","20-16","30-16","40-16","50-16" — already in lex order
        assertQuery(
                "format_price\n" +
                        "10-16\n" +
                        "20-16\n" +
                        "30-16\n" +
                        "40-16\n" +
                        "50-16\n",
                "select format_price(x * 10.0 + 0.5, 32) from long_sequence(5) order by 1"
        );
    }

    @Test
    public void testMultiRowConstantTickSize16() throws Exception {
        // x * 10.0 + 0.5 for x=1..5 → ticks = round(0.5 * 16) = 8 with tick=16
        // Results: "10-08","20-08","30-08","40-08","50-08"
        assertQuery(
                "format_price\n" +
                        "10-08\n" +
                        "20-08\n" +
                        "30-08\n" +
                        "40-08\n" +
                        "50-08\n",
                "select format_price(x * 10.0 + 0.5, 16) from long_sequence(5) order by 1"
        );
    }

    @Test
    public void testMultiRowMixedFractionals() throws Exception {
        // 1.0 + (x-1) * 0.125 for x=1..8, tick=8 → covers all 8 fractional tick values 0-7
        assertQuery(
                "format_price\n" +
                        "1-0\n" +
                        "1-1\n" +
                        "1-2\n" +
                        "1-3\n" +
                        "1-4\n" +
                        "1-5\n" +
                        "1-6\n" +
                        "1-7\n",
                "select format_price(1.0 + (x-1) * 0.125, 8) " +
                        "from long_sequence(8) order by 1"
        );
    }

    @Test
    public void testMultiRowOrderedResultsTickSize32() throws Exception {
        // Full sweep 0-31: 100.0 + (x-1)/32.0 for x=1..32, tick=32
        // Lex sort matches numeric sort here because all whole parts are equal
        // and fractional parts are zero-padded to the same width
        assertQuery(
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
                        "100-31\n",
                "select format_price(100.0 + (x-1)/32.0, 32) " +
                        "from long_sequence(32) order by 1"
        );
    }

    @Test
    public void testMultiRowNullsAndValidMixed() throws Exception {
        // x%2=0 → NULL tick → empty string; x%2!=0 → tick=32 → "x-00"
        // Odd x in 1..5: prices 1,3,5 → "1-00","3-00","5-00"
        // Empty strings sort before non-empty strings
        assertQuery(
                "format_price\n" +
                        "\n" +
                        "\n" +
                        "1-00\n" +
                        "3-00\n" +
                        "5-00\n",
                "select format_price(x * 1.0, case when x % 2 = 0 then NULL else 32 end) " +
                        "from long_sequence(5) order by 1"
        );
    }

    @Test
    public void testMultiRowNegativePrices() throws Exception {
        // -x*1.0 for x=1..5, tick=32 → "-1-00".."-5-00"
        // Lexicographic sort: "-1-00" < "-2-00" < ... < "-5-00"
        // (because '-' then '1' < '2' < ... < '5')
        assertQuery(
                "format_price\n" +
                        "-1-00\n" +
                        "-2-00\n" +
                        "-3-00\n" +
                        "-4-00\n" +
                        "-5-00\n",
                "select format_price(-x * 1.0, 32) from long_sequence(5) order by 1"
        );
    }

    @Test
    public void testMultiRowDifferentTickSizes() throws Exception {
        // Even x gets tick=32 (width=2), odd x gets tick=2 (width=1)
        // x=1→"1-0", x=2→"2-00", x=3→"3-0", x=4→"4-00", x=5→"5-0"
        // Lex sort: "1-0" < "2-00" < "3-0" < "4-00" < "5-0"
        assertQuery(
                "format_price\n" +
                        "1-0\n" +
                        "2-00\n" +
                        "3-0\n" +
                        "4-00\n" +
                        "5-0\n",
                "select format_price(x * 1.0, case when x % 2 = 0 then 32 else 2 end) " +
                        "from long_sequence(5) order by 1"
        );
    }

    // =========================================================================
    // 7. CONSTANT vs. NON-CONSTANT TICK — exercises both code paths
    //    constantTick=true: width pre-computed once in init()
    //    constantTick=false: calculateWidth called per row
    // =========================================================================

    @Test
    public void testNonConstantTickFromColumn() throws Exception {
        // Tick from a column → constantTick=false, calculateWidth() called per row
        // format_price(100.5, 2)  → "100-1"  (width=1)
        // format_price(100.5, 16) → "100-08" (width=2)
        // format_price(100.5, 32) → "100-16" (width=2)
        // Lex sort: "100-08" < "100-1" < "100-16"
        // ('0' < '1' at index 4)
        assertQuery(
                "format_price\n" +
                        "100-08\n" +
                        "100-1\n" +
                        "100-16\n",
                "select format_price(100.5, tick) " +
                        "from (select 2 as tick union all select 16 union all select 32) " +
                        "order by 1"
        );
    }

    @Test
    public void testConstantTickWidthBoundaryAt10() throws Exception {
        // tick=10 → width = digits(9) = 1 — no zero-padding
        // 100.5 → ticks = round(0.5 * 10) = 5 → "100-5"
        assertQuery("format_price\n100-5\n", "select format_price(100.5, 10)");
    }

    @Test
    public void testConstantTickWidthBoundaryAt11() throws Exception {
        // tick=11 → width = digits(10) = 2 — zero-padding kicks in
        // 100.5 → ticks = round(0.5 * 11) = 6 (5.5 rounds up) → "100-06"
        assertQuery("format_price\n100-06\n", "select format_price(100.5, 11)");
    }

    @Test
    public void testConstantTickWidthBoundaryAt100() throws Exception {
        // tick=100 → width = digits(99) = 2
        // 100.5 → ticks = round(0.5 * 100) = 50 → "100-50"
        assertQuery("format_price\n100-50\n", "select format_price(100.5, 100)");
    }

    @Test
    public void testConstantTickWidthBoundaryAt101() throws Exception {
        // tick=101 → width = digits(100) = 3
        // 100.5 → ticks = round(0.5 * 101) = 51 (50.5 rounds up) → "100-051"
        assertQuery("format_price\n100-051\n", "select format_price(100.5, 101)");
    }

    // =========================================================================
    // 8. LARGE PRICE VALUES
    // =========================================================================

    @Test
    public void testLargePriceValue() throws Exception {
        assertQuery("format_price\n999999-16\n", "select format_price(999999.5, 32)");
    }

    @Test
    public void testLargeNegativePrice() throws Exception {
        assertQuery("format_price\n-999999-16\n", "select format_price(-999999.5, 32)");
    }

    // =========================================================================
    // 9. FLOATING-POINT PRECISION — exact fractional values
    // =========================================================================

    @Test
    public void testOneThirtySecond() throws Exception {
        // 100 + 1/32 = 100.03125 → "100-01"
        assertQuery("format_price\n100-01\n", "select format_price(100.03125, 32)");
    }

    @Test
    public void testThirtyOneThirtySeconds() throws Exception {
        // 100 + 31/32 = 100.96875 → "100-31"
        assertQuery("format_price\n100-31\n", "select format_price(100.96875, 32)");
    }

    // =========================================================================
    // REQUIRED: FunctionFactory provider
    // =========================================================================

    @Override
    protected FunctionFactory getFunctionFactory() {
        return new FormatPriceFunctionFactory();
    }
}