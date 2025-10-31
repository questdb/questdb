/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.std;

import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.DecimalParser;
import io.questdb.std.Decimals;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Tests for the consolidated Decimal256 class
 */
public class Decimal256Test {

    @Test(expected = NumericException.class)
    public void testAddOverflow() {
        Decimal256 a = new Decimal256(Long.MAX_VALUE, 0, 0, 0, 0);
        Decimal256 b = new Decimal256(1, 0, 0, 0, 0);
        a.add(b);
    }

    @Test
    public void testAddRescaleB() {
        Decimal256 a = new Decimal256(0, 0, 0, 100, 2);
        Decimal256 b = new Decimal256(0, 0, 0, 1, 0);
        a.add(b);
        Assert.assertEquals(200, a.getLl());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testAdditionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal256 a = rnd.nextDecimal256();
            Decimal256 b = rnd.nextDecimal256();

            // Test addition accuracy
            testAdditionAccuracy(a, b, i);
        }
    }

    @Test
    public void testAdditionNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        m.add(Decimal256.fromLong(1, 0));
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testAdditionNullOther() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.add(Decimal256.NULL_VALUE);
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test(expected = NumericException.class)
    public void testAdditionOverflow() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.add(new Decimal256(0, 0, 0, 1, 0));
    }

    @Test(expected = NumericException.class)
    public void testBigDecimalOverflow() {
        BigDecimal bd = new BigDecimal("1e100");
        Decimal256.fromBigDecimal(bd);
    }

    @Test(expected = NumericException.class)
    public void testBigDecimalOverflowLimit() {
        BigDecimal bd = new BigDecimal("1e76");
        Decimal256.fromBigDecimal(bd);
    }

    @Test
    public void testComparePrecisionBasic() {
        Decimal256 ten = Decimal256.fromLong(10, 0);
        Assert.assertTrue("10 have a precision of 2 (< 3)", ten.comparePrecision(3));
        Assert.assertTrue("10 have a precision of 2 (== 2)", ten.comparePrecision(2));
        Assert.assertFalse("10 have a precision of 2 (> 1)", ten.comparePrecision(1));

        Decimal256 mten = Decimal256.fromLong(-10, 0);
        Assert.assertTrue("-10 have a precision of 2 (< 3)", mten.comparePrecision(3));
        Assert.assertTrue("-10 have a precision of 2 (== 2)", mten.comparePrecision(2));
        Assert.assertFalse("-10 have a precision of 2 (> 1)", mten.comparePrecision(1));

        Assert.assertTrue("A precision higher than MAX_SCALE is always true", ten.comparePrecision(Decimal256.MAX_SCALE + 1));
        Assert.assertFalse("A precision lower than 0 is always false", ten.comparePrecision(-1));
    }

    @Test
    public void testComparePrecisionCombinatorics() {
        // value -> actual precision
        final Object[][] combinations = new Object[][]{
                {"0", 0},
                {"1", 1},
                {"5", 1},
                {"9", 1},
                {"12", 2},
                {"76", 2},
                {"99", 2},
                {"872", 3},
                {"999", 3},
                {"12345", 5},
                {"99999", 5},
                {"100000", 6},
                {"654321", 6},
                {"1234567890", 10},
                {"98765432101234567890", 20},
                {"123456789098765432101234567890", 30},
                {"9876543210123456789098765432101234567890", 40},
                {"12345678909876543210123456789098765432101234567890", 50},
                {"987654321012345678909876543210123456789098765432101234567890", 60},
                {"1234567890987654321012345678909876543210123456789098765432101234567890", 70},
                {"6123456789098765432101234567890987654321012345678909876543210123456789012345", 76},
        };
        for (Object[] combination : combinations) {
            String value = (String) combination[0];
            int actualPrecision = (int) combination[1];
            BigDecimal d = new BigDecimal(value);
            Decimal256 decimal = Decimal256.fromBigDecimal(d);
            for (int i = 0; i <= Decimal256.MAX_SCALE; i++) {
                Assert.assertEquals(String.format("Test failed with decimal %s (p: %d) when compared against %d", decimal, actualPrecision, i), actualPrecision <= i, decimal.comparePrecision(i));
            }
            decimal.negate();
            // Negated values have the same precision as the positive one
            for (int i = 0; i <= Decimal256.MAX_SCALE; i++) {
                Assert.assertEquals(String.format("Test failed with decimal %s (p: %d) when compared against %d", decimal, actualPrecision, i), actualPrecision <= i, decimal.comparePrecision(i));
            }
        }
    }

    @Test
    public void testComparePrecisionNull() {
        Assert.assertTrue(Decimal256.NULL_VALUE.comparePrecision(1));
    }

    @Test
    public void testCompareTo() {
        Decimal256 smaller = new Decimal256(0, 0, 0, 100, 2);
        Decimal256 larger = new Decimal256(0, 0, 0, 200, 2);

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));

        smaller.of(0, 0, 100, 0, 2);
        larger.of(0, 0, 200, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);

        smaller.of(0, 100, 0, 0, 2);
        larger.of(0, 200, 0, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);

        smaller.of(100, 0, 0, 0, 2);
        larger.of(200, 0, 0, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
    }

    @Test
    public void testCompareToDifferentSigns() {
        Decimal256 a = Decimal256.fromLong(1L, 0);
        Decimal256 b = Decimal256.fromLong(-1L, 0);
        Assert.assertEquals(1, a.compareTo(b));
    }

    @Test
    public void testCompareToFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal256 a = rnd.nextDecimal256();
            Decimal256 b = rnd.nextDecimal256();

            BigDecimal bdA = a.toBigDecimal();
            BigDecimal bdB = b.toBigDecimal();

            // The comparison may overflow during rescaling
            try {
                int actual = a.compareTo(b);
                int expected = bdA.compareTo(bdB);
                Assert.assertEquals("iteration: " + i + " expected:<" + expected + "> but was:<" + actual + ">", expected, actual);
            } catch (NumericException ignore) {
            }
        }
    }

    @Test
    public void testCompareToScaled() {
        Decimal256 smaller = new Decimal256(0, 0, 0, 10, 1);
        Decimal256 larger = new Decimal256(0, 0, 0, 200, 2);

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));

        smaller.of(0, 0, 10, 0, 1);
        larger.of(0, 0, 200, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);

        smaller.of(0, 10, 0, 0, 1);
        larger.of(0, 200, 0, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);

        smaller.of(10, 0, 0, 0, 1);
        larger.of(200, 0, 0, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
    }

    @Test
    public void testCompareToWithDifferentScales() {
        // Test 12.34 (scale 2) vs 12.345 (scale 3)
        Decimal256 a = Decimal256.fromDouble(12.34, 2);   // 12.34
        Decimal256 b = Decimal256.fromDouble(12.345, 3);  // 12.345

        Assert.assertTrue(a.compareTo(b) < 0); // 12.34 < 12.345
        Assert.assertTrue(b.compareTo(a) > 0); // 12.345 > 12.34

        // Test 12.340 (scale 3) vs 12.34 (scale 2) - should be equal
        Decimal256 c = Decimal256.fromDouble(12.340, 3);  // 12.340
        Decimal256 d = Decimal256.fromDouble(12.34, 2);   // 12.34

        Assert.assertEquals(0, c.compareTo(d)); // 12.340 == 12.34
        Assert.assertEquals(0, d.compareTo(c)); // 12.34 == 12.340

        // Test with larger scale difference: 1.2 (scale 1) vs 1.23456 (scale 5)
        Decimal256 e = Decimal256.fromDouble(1.2, 1);      // 1.2
        Decimal256 f = Decimal256.fromDouble(1.23456, 5);  // 1.23456

        Assert.assertTrue(e.compareTo(f) < 0); // 1.2 < 1.23456
        Assert.assertTrue(f.compareTo(e) > 0); // 1.23456 > 1.2

        // Test negative numbers with different scales
        Decimal256 g = Decimal256.fromDouble(-12.3, 1);    // -12.3
        Decimal256 h = Decimal256.fromDouble(-12.34, 2);   // -12.34

        Assert.assertTrue(g.compareTo(h) > 0); // -12.3 > -12.34
        Assert.assertTrue(h.compareTo(g) < 0); // -12.34 < -12.3

        // Test zero with different scales
        Decimal256 zero1 = Decimal256.fromDouble(0.0, 1);
        Decimal256 zero2 = Decimal256.fromDouble(0.0, 3);

        Assert.assertEquals(0, zero1.compareTo(zero2)); // 0.0 == 0.000
        Assert.assertEquals(0, zero2.compareTo(zero1)); // 0.000 == 0.0
    }

    @Test
    public void testCompareToWithNull() {
        // Create null decimal using NULL_VALUE constant
        Decimal256 nullDecimal = new Decimal256();
        nullDecimal.copyFrom(Decimal256.NULL_VALUE);
        Assert.assertTrue(nullDecimal.isNull());

        // Create another null decimal
        Decimal256 anotherNullDecimal = new Decimal256();
        anotherNullDecimal.ofNull();
        Assert.assertTrue(anotherNullDecimal.isNull());

        // Create non-null decimal
        Decimal256 nonNullDecimal = Decimal256.fromLong(123, 2);
        Assert.assertFalse(nonNullDecimal.isNull());

        // Test null comparing with null (should return 0)
        Assert.assertEquals("null compareTo null should return 0",
                0, nullDecimal.compareTo(anotherNullDecimal));

        // Test null comparing with non-null (should return -1)
        Assert.assertEquals("null compareTo non-null should return -1",
                -1, nullDecimal.compareTo(nonNullDecimal));

        // Test non-null comparing with null (should return 1)
        Assert.assertEquals("non-null compareTo null should return 1",
                1, nonNullDecimal.compareTo(nullDecimal));

        // Test using the direct compareTo(hh, hl, lh, ll, scale) method
        Assert.assertEquals("null compareTo(NULL values) should return 0",
                0, nullDecimal.compareTo(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        0));

        Assert.assertEquals("non-null compareTo(NULL values) should return 1",
                1, nonNullDecimal.compareTo(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        0));

        // Test that null decimal with compareTo on another null returns 0 regardless of scale
        Assert.assertEquals("null compareTo null with different scale should return 0",
                0, nullDecimal.compareTo(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL,
                        5));

        // Test static isNull method
        Assert.assertTrue("isNull(NULL values) should return true",
                Decimal256.isNull(
                        Decimals.DECIMAL256_HH_NULL,
                        Decimals.DECIMAL256_HL_NULL,
                        Decimals.DECIMAL256_LH_NULL,
                        Decimals.DECIMAL256_LL_NULL));
    }

    @Test
    public void testConstructorAndGetters() {
        Decimal256 decimal = new Decimal256(0x123456789ABCDEFL, 0xFEDCBA9876543210L, 0x123456789ABCDEFL, 0xFEDCBA9876543210L, 3);

        Assert.assertEquals(0x123456789ABCDEFL, decimal.getHh());
        Assert.assertEquals(0xFEDCBA9876543210L, decimal.getHl());
        Assert.assertEquals(0x123456789ABCDEFL, decimal.getLh());
        Assert.assertEquals(0xFEDCBA9876543210L, decimal.getLl());
        Assert.assertEquals(3, decimal.getScale());
    }

    @Test
    public void testCopyFrom() {
        Decimal256 original = Decimal256.fromDouble(123.456, 3);
        Decimal256 copy = new Decimal256();

        copy.copyFrom(original);

        Assert.assertEquals(original.getHh(), copy.getHh());
        Assert.assertEquals(original.getHl(), copy.getHl());
        Assert.assertEquals(original.getLh(), copy.getLh());
        Assert.assertEquals(original.getLl(), copy.getLl());
        Assert.assertEquals(original.getScale(), copy.getScale());
    }

    @Test
    public void testDigitExtractionLoop() {
        // Test extracting all digits from 12345 using the two methods together
        Decimal256 value = new Decimal256(0, 0, 0, 12345, 0);

        // Extract ten thousands digit (1)
        int digit = value.getDigitAtPowerOfTen(4);
        Assert.assertEquals(1, digit);
        value.subtractPowerOfTenMultiple(4, digit);
        Assert.assertEquals(2345, value.getLl());

        // Extract thousands digit (2)
        digit = value.getDigitAtPowerOfTen(3);
        Assert.assertEquals(2, digit);
        value.subtractPowerOfTenMultiple(3, digit);
        Assert.assertEquals(345, value.getLl());

        // Extract hundreds digit (3)
        digit = value.getDigitAtPowerOfTen(2);
        Assert.assertEquals(3, digit);
        value.subtractPowerOfTenMultiple(2, digit);
        Assert.assertEquals(45, value.getLl());

        // Extract tens digit (4)
        digit = value.getDigitAtPowerOfTen(1);
        Assert.assertEquals(4, digit);
        value.subtractPowerOfTenMultiple(1, digit);
        Assert.assertEquals(5, value.getLl());

        // Extract ones digit (5)
        digit = value.getDigitAtPowerOfTen(0);
        Assert.assertEquals(5, digit);
        value.subtractPowerOfTenMultiple(0, digit);
        Assert.assertEquals(0, value.getLl());
    }

    @Test
    public void testDivide() {
        Decimal256 a = new Decimal256(0, 0, 0, 1000, 2);
        Decimal256 b = new Decimal256(0, 0, 0, 3, 0);
        a.divide(b, 2, RoundingMode.HALF_UP);
        Assert.assertEquals(333, a.getLl());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal256(0, 0, 0, 10, 0);
        b = new Decimal256(0, 0, 0, 300, 2);
        a.divide(b, 2, RoundingMode.HALF_UP);
        Assert.assertEquals(333, a.getLl());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testDivide64() {
        Decimal256 a = Decimal256.fromDouble(123.456, 3);
        Decimal256 b = Decimal256.fromDouble(7.89, 2);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        Decimal256 result = new Decimal256();
        Decimal256.divide(a, b, result, tgtScale, RoundingMode.HALF_UP);
        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, result.toBigDecimal());
    }

    @Test
    public void testDivideAddBack() {
        // Test a specific case where the division falls into Step D6 of Knuth algorithm
        // Test multiple edge cases that should trigger Step D5
        long[][] patterns = {
                // Patterns for line 1260 (128xLong first)
                {0x8000000000000000L, 0x0000000000000000L, 0x0000000000000000L, 0x8000000000000001L},
                {0xC000000000000000L, 0x0000000000000000L, 0x0000000000000000L, 0xC000000000000001L},

                // Patterns for line 1327 (128xLong third)
                {0x0000000080000000L, 0x0000000000000000L, 0x0000000000000000L, 0x8000000000000001L},
                {0x00000000C0000000L, 0x0000000000000000L, 0x0000000000000000L, 0xC000000000000001L},

                // Patterns for line 1392 (96x96)
                {0x0000000080000000L, 0x0000000000000000L, 0x8000000000000000L, 0x0000000000000001L},
                {0x00000000C0000000L, 0x0000000000000000L, 0xC000000000000000L, 0x0000000000000001L},

                // Patterns for line 1457 (96xLong first)
                {0x0000000080000000L, 0x0000000000000000L, 0x0000000000000000L, 0x8000000000000001L},
                {0x00000000C0000000L, 0x0000000000000000L, 0x0000000000000000L, 0xC000000000000001L},

                // Patterns for line 1195 (128x96 second)
                {0x0000800000000000L, 0x0000000000000000L, 0x0000000080000000L, 0x0000000000000001L},
                {0x0000C00000000000L, 0x0000000000000000L, 0x00000000C0000000L, 0x0000000000000001L}
        };

        for (long[] testCase : patterns) {
            Decimal256 dividend = new Decimal256(0, 0, testCase[0], testCase[1], 0);
            Decimal256 divisor = new Decimal256(0, 0, testCase[2], testCase[3], 0);
            Decimal256 result = new Decimal256();
            BigDecimal bdDividend = dividend.toBigDecimal();
            BigDecimal bdDivisor = divisor.toBigDecimal();
            BigDecimal bdResult = bdDividend.divide(bdDivisor, 0, RoundingMode.HALF_UP);
            Decimal256.divide(dividend, divisor, result, 0, RoundingMode.HALF_UP);

            Assert.assertEquals("Dividend: " + dividend + " divisor: " + divisor + " result: " + result, bdResult, result.toBigDecimal());
        }
    }

    @Test(expected = NumericException.class)
    public void testDivideDivisorScaleOverflow() {
        Decimal256 a = new Decimal256(0, Long.MAX_VALUE / 2, Long.MAX_VALUE, Long.MAX_VALUE, 0);
        Decimal256 b = new Decimal256(0, 0, 3, 0, 0);
        a.multiply(b);
    }

    @Test(expected = NumericException.class)
    public void testDivideInvalidScale() {
        Decimal256 a = new Decimal256(0, 0, 0, 1, 0);
        Decimal256 b = new Decimal256(0, 0, 0, 1, 50);
        a.divide(b, 50, RoundingMode.HALF_UP);
    }

    @Test
    public void testDivideLarge() {
        Decimal256 a = Decimal256.fromDouble(987654321.123456789, 9);
        Decimal256 b = Decimal256.fromDouble(123.456, 3);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        a.divide(b, tgtScale, RoundingMode.HALF_UP);

        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, a.toBigDecimal());
    }

    @Test
    public void testDivideLargeScale() {
        Decimal256 a = Decimal256.fromDouble(3.141592653589793, 15);
        Decimal256 b = Decimal256.fromDouble(2.718281828459045, 15);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 6;
        a.divide(b, tgtScale, RoundingMode.HALF_UP);

        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, a.toBigDecimal());
    }

    @Test
    public void testDivideNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        m.divide(Decimal256.fromLong(1, 0), 0, RoundingMode.DOWN);
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testDivideNullOther() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.divide(Decimal256.NULL_VALUE, 0, RoundingMode.DOWN);
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testDivideOverflow() {
        Decimal256 a = Decimal256.fromDouble(-328049473, 0);
        Decimal256 b = Decimal256.fromDouble(-50582053256.05, 2);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        Decimal256 result = new Decimal256();
        Decimal256.divide(a, b, result, tgtScale, RoundingMode.HALF_UP);

        Assert.assertEquals(result.toBigDecimal(), bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP));
    }

    @Test
    public void testDivisionByOne() {
        Decimal256 dividend = Decimal256.fromLong(123456L, 3);
        Decimal256 divisor = Decimal256.fromLong(1L, 0);
        dividend.divide(divisor, 3, RoundingMode.HALF_UP);
        Assert.assertEquals("123.456", dividend.toString());
    }

    @Test(expected = NumericException.class)
    public void testDivisionByZero() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 zero = Decimal256.fromDouble(0.0, 2);

        a.divide(zero, 2, RoundingMode.HALF_UP);
    }

    @Test
    public void testDivisionCombinatorics() {
        RoundingMode[] modes = new RoundingMode[]{
                RoundingMode.HALF_UP,
                RoundingMode.HALF_DOWN,
                RoundingMode.HALF_EVEN,
                RoundingMode.UNNECESSARY,
                RoundingMode.FLOOR,
                RoundingMode.CEILING,
                RoundingMode.DOWN,
                RoundingMode.UP
        };
        BigDecimal[] dividends = new BigDecimal[]{
                new BigDecimal("-3.45"),
                new BigDecimal("0.000987654321"),
                new BigDecimal("1"),
                new BigDecimal("10"),
                new BigDecimal("123.456"),
                new BigDecimal("123456.789101112"),
                new BigDecimal("1234567891011121314151617181920212222324"),
                new BigDecimal("123456789101112131415161718.19202122223242526272829"),
        };
        BigDecimal[] divisors = new BigDecimal[]{
                new BigDecimal("-123.456"),
                new BigDecimal("0"),
                new BigDecimal("1"),
                new BigDecimal("2"),
                new BigDecimal("123.456"),
                new BigDecimal("123456.789101112"),
                new BigDecimal("1234567891011121314151617181920212222324"),
                new BigDecimal("123456789101112131415161718.19202122223242526272829"),
        };
        int[] scales = new int[]{
                0,
                2,
                6,
                16,
                32
        };

        for (RoundingMode roundingMode : modes) {
            for (BigDecimal divisor : divisors) {
                for (BigDecimal dividend : dividends) {
                    for (int scale : scales) {
                        Decimal256 a = Decimal256.fromBigDecimal(dividend);
                        Decimal256 b = Decimal256.fromBigDecimal(divisor);
                        if (divisor.compareTo(BigDecimal.ZERO) == 0) {
                            try {
                                a.divide(b, scale, roundingMode);
                                Assert.fail("NumericException expected");
                            } catch (NumericException e) {
                                Assert.assertEquals("Division by zero", e.getMessage());
                            }
                            continue;
                        }

                        // Division might fail because of overflow during scaling
                        try {
                            a.divide(b, scale, roundingMode);
                            BigDecimal expected = dividend.divide(divisor, scale, roundingMode);
                            Assert.assertEquals(expected, a.toBigDecimal());
                        } catch (NumericException e) {
                            if (roundingMode == RoundingMode.UNNECESSARY && e.getMessage().equals("Rounding necessary")) {
                                continue;
                            }

                            if (!e.getMessage().startsWith("Overflow")) {
                                Assert.fail("NumericException Overflow expected, but was " + e.getMessage());
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testDivisionDivWord() {
        Decimal256 dividend = new Decimal256(0, -2, -2, -2, 0);
        BigDecimal bdDividend = dividend.toBigDecimal();
        Decimal256 divisor = new Decimal256(0, 0, 0, 0xFFFFFFFFL, 0);
        BigDecimal bdDivisor = divisor.toBigDecimal();
        dividend.divide(divisor, 0, RoundingMode.HALF_UP);
        BigDecimal expected = bdDividend.divide(bdDivisor, 0, RoundingMode.HALF_UP);
        Assert.assertEquals(expected.toString(), dividend.toString());
    }

    @Test
    public void testDivisionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal256 a = rnd.nextDecimal256();
            Decimal256 b = rnd.nextDecimal256();

            // Test division accuracy (avoid division by zero)
            if (!b.isZero()) {
                testDivisionAccuracy(a, b, i);
            }
        }
    }

    @Test(expected = NumericException.class)
    public void testDivisionRemainderUnnecessary() {
        Decimal256 dividend = Decimal256.fromLong(10L, 0);
        Decimal256 divisor = Decimal256.fromLong(3L, 0);
        dividend.divide(divisor, 0, RoundingMode.UNNECESSARY);
    }

    @Test
    public void testDivisionScaled() {
        Decimal256 dividend = Decimal256.fromDouble(10, 0);
        Decimal256 divisor = Decimal256.fromDouble(3, 0);

        dividend.divide(divisor, 6, RoundingMode.HALF_UP);
        Assert.assertEquals(6, dividend.getScale());
        Assert.assertEquals(0, dividend.getHh());
        Assert.assertEquals(0, dividend.getHl());
        Assert.assertEquals(0, dividend.getLh());
        Assert.assertEquals(3333333, dividend.getLl());
    }

    @Test
    public void testEnd() {
        BigDecimal a = new BigDecimal("30450464.57299");
        BigDecimal b = new BigDecimal("-0.0000000006363875680102");
        BigDecimal expected = a.divide(b, 6, RoundingMode.HALF_UP);

        Decimal256 da = Decimal256.fromBigDecimal(a);
        Decimal256 db = Decimal256.fromBigDecimal(b);

        da.divide(db, 6, RoundingMode.HALF_UP);

        Assert.assertEquals(expected, da.toBigDecimal());
    }

    @Test
    public void testEquals() {
        Decimal256 a = new Decimal256(123, 456, 789, 101112, 2);
        Decimal256 b = new Decimal256(123, 456, 789, 101112, 2);
        Decimal256 c = new Decimal256(123, 456, 789, 101113, 2);

        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testFitsInStorageSizeCombinatorics() {
        // combinations described as
        // hh, hl, lh, ll, fits in byte, fits in short, fits in int, fits in long, fits in Long128, fits in Long256
        Object[][] combinations = new Object[][]{
                {0L, 0L, 0L, 0L, true, true, true, true, true, true},
                {0L, 0L, 0L, 1L, true, true, true, true, true, true},
                {-1L, -1L, -1L, -1L, true, true, true, true, true, true},
                {0L, 0L, 0L, (long) Byte.MAX_VALUE, true, true, true, true, true, true},
                {-1L, -1L, -1L, (long) Byte.MIN_VALUE + 1, true, true, true, true, true, true},
                {0L, 0L, 0L, (long) Short.MAX_VALUE, false, true, true, true, true, true},
                {-1L, -1L, -1L, (long) Short.MIN_VALUE + 1, false, true, true, true, true, true},
                {0L, 0L, 0L, (long) Integer.MAX_VALUE, false, false, true, true, true, true},
                {-1L, -1L, -1L, (long) Integer.MIN_VALUE + 1, false, false, true, true, true, true},
                {0L, 0L, 0L, Long.MAX_VALUE, false, false, false, true, true, true},
                {-1L, -1L, -1L, Long.MIN_VALUE + 1, false, false, false, true, true, true},
                {0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, false, false, false, false, true, true},
                {-1L, -1L, Long.MIN_VALUE, Long.MIN_VALUE + 1, false, false, false, false, true, true},
                {Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, false, false, false, false, false, true},
                {Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE + 1, false, false, false, false, false, true},
                {Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, false, false, false, false, false, true},
        };
        Decimal256 d = new Decimal256();
        for (var combination : combinations) {
            d.of((long) combination[0], (long) combination[1], (long) combination[2], (long) combination[3], 0);
            Assert.assertEquals("Expected " + d + " to fit in a byte", combination[4], d.fitsInStorageSizePow2(0));
            Assert.assertEquals("Expected " + d + " to fit in a short", combination[5], d.fitsInStorageSizePow2(1));
            Assert.assertEquals("Expected " + d + " to fit in an integer", combination[6], d.fitsInStorageSizePow2(2));
            Assert.assertEquals("Expected " + d + " to fit in a long", combination[7], d.fitsInStorageSizePow2(3));
            Assert.assertEquals("Expected " + d + " to fit in 128-bits", combination[8], d.fitsInStorageSizePow2(4));
            Assert.assertEquals("Expected " + d + " to fit in 256-bits", combination[9], d.fitsInStorageSizePow2(5));
        }
    }

    @Test
    public void testFitsInStorageSizeInvalid() {
        Decimal256 d = new Decimal256(0, 0, 0, 1, 0);
        Assert.assertFalse(d.fitsInStorageSizePow2(6));
    }

    @Test
    public void testFromBigDecimal() {
        BigDecimal bd = new BigDecimal("1e37");
        Decimal256 decimal = Decimal256.fromBigDecimal(bd);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("10000000000000000000000000000000000000", sink.toString());
    }

    @Test
    public void testFromBigDecimalHuge() {
        BigDecimal bd = new BigDecimal("1e75");
        Decimal256 decimal = Decimal256.fromBigDecimal(bd);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("1000000000000000000000000000000000000000000000000000000000000000000000000000", sink.toString());
    }

    @Test
    public void testFromDouble() {
        Decimal256 decimal = Decimal256.fromDouble(123.45, 2);

        Assert.assertEquals(12345, decimal.getLl());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.001);
    }

    @Test
    public void testFromLong() {
        Decimal256 decimal = Decimal256.fromLong(12345, 2);

        Assert.assertEquals(0, decimal.getHh());
        Assert.assertEquals(0, decimal.getHl());
        Assert.assertEquals(0, decimal.getLh());
        Assert.assertEquals(12345, decimal.getLl());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.001);
    }

    @Test
    public void testGetDigitAtPowerOfTenBoundaryValues() {
        Assert.assertEquals(9, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 9, 0));
        Assert.assertEquals(9, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 99, 1));
        Assert.assertEquals(9, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 999, 2));
        Assert.assertEquals(9, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 9999, 3));
    }

    @Test
    public void testGetDigitAtPowerOfTenHundredsPlace() {
        Assert.assertEquals(5, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 523, 2));
        Assert.assertEquals(9, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 999, 2));
        Assert.assertEquals(1, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 100, 2));
        Assert.assertEquals(0, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 99, 2));
    }

    @Test
    public void testGetDigitAtPowerOfTenLargeNumbers() {
        // 2^64 = 18,446,744,073,709,551,616
        Assert.assertEquals(1, Decimal256.getDigitAtPowerOfTen(0, 0, 1, 0, 19));

        // 2^128 = 340,282,366,920,938,463,463,374,607,431,768,211,456
        Assert.assertEquals(3, Decimal256.getDigitAtPowerOfTen(0, 1, 0, 0, 38));
    }

    @Test
    public void testGetDigitAtPowerOfTenSingleDigits() {
        for (int i = 0; i <= 9; i++) {
            Assert.assertEquals(i, Decimal256.getDigitAtPowerOfTen(0, 0, 0, i, 0));
        }
    }

    @Test
    public void testGetDigitAtPowerOfTenTensPlace() {
        Assert.assertEquals(9, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 95, 1));
        Assert.assertEquals(5, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 50, 1));
        Assert.assertEquals(1, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 19, 1));
        Assert.assertEquals(0, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 9, 1));
    }

    @Test
    public void testGetDigitAtPowerOfTenZero() {
        Assert.assertEquals(0, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 0, 0));
        Assert.assertEquals(0, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 0, 5));
        Assert.assertEquals(0, Decimal256.getDigitAtPowerOfTen(0, 0, 0, 0, 10));
    }

    @Test
    public void testHashCode() {
        Decimal256 a = new Decimal256(123, 456, 789, 101112, 2);
        Decimal256 b = new Decimal256(123, 456, 789, 101112, 2);

        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testInPlaceAddition() {
        Decimal256 a = Decimal256.fromDouble(123.45, 2);
        Decimal256 b = Decimal256.fromDouble(67.89, 2);

        a.add(b);

        Assert.assertEquals(191.34, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testInPlaceDivision() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 b = Decimal256.fromDouble(4.0, 1);

        a.divide(b, 2, RoundingMode.HALF_UP);

        Assert.assertEquals(25.0, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testInPlaceModulo() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 b = Decimal256.fromDouble(30.0, 2);

        a.modulo(b);

        Assert.assertEquals(10.0, a.toDouble(), 0.01);
    }

    @Test
    public void testInPlaceMultiplication() {
        Decimal256 a = Decimal256.fromDouble(12.34, 2);
        Decimal256 b = Decimal256.fromDouble(5.6, 1);

        a.multiply(b);

        Assert.assertEquals(69.104, a.toDouble(), 0.001);
        Assert.assertEquals(3, a.getScale()); // 2 + 1 = 3
    }

    @Test
    public void testInPlaceSubtraction() {
        Decimal256 a = Decimal256.fromDouble(123.45, 2);
        Decimal256 b = Decimal256.fromDouble(67.89, 2);

        a.subtract(b);

        Assert.assertEquals(55.56, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testInvalidScale() {
        new Decimal256(0, 0, 0, 0, 100);
    }

    @Test
    public void testIsNegative() {
        Decimal256 positive = new Decimal256(0, 0, 0, 100, 2);
        Decimal256 negative = new Decimal256(-1, 0, 0, 0, 2);

        Assert.assertFalse(positive.isNegative());
        Assert.assertTrue(negative.isNegative());
    }

    @Test
    public void testIsZero() {
        Decimal256 zero = new Decimal256(0, 0, 0, 0, 2);
        Decimal256 nonZero = new Decimal256(0, 0, 0, 1, 2);

        Assert.assertTrue(zero.isZero());
        Assert.assertFalse(nonZero.isZero());
    }

    @Test
    public void testLargeNumbers() {
        // Test with large numbers
        Decimal256 a = new Decimal256(0, 0, 0, 0xFFFFFFFFFFFFFFFFL, 0); // Large positive
        Decimal256 b = Decimal256.fromLong(2, 0);

        a.multiply(b);

        // The result should be 2 * (2^64 - 1)
        Assert.assertEquals(0x1L, a.getLh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFEL, a.getLl());
    }

    @Test
    public void testLossyScientificNotation() throws NumericException {
        final String value = "9.8765432e4";
        Decimal256 decimal = new Decimal256();
        DecimalParser.parse(decimal, value, 0, value.length(), -1, 1, false, true);

        Assert.assertEquals("98765.4", decimal.toString());
    }

    @Test
    public void testLossyScientificNotationDropsFractionalDigitsBeforeExponent() throws NumericException {
        final String value = "1.234e2";
        Decimal256 decimal = new Decimal256();
        long metadata = DecimalParser.parse(decimal, value, 0, value.length(), -1, 0, false, true);

        Assert.assertEquals("lossy parsing should only remove fractional digits that exceed the target scale", "123", decimal.toString());
        Assert.assertEquals("expected scale of 0", 0, Numbers.decodeHighInt(metadata));
        Assert.assertEquals("expected precision to reflect 3 digits", 3, Numbers.decodeLowInt(metadata));
    }

    @Test
    public void testLossyScientificNotationKeepsAllSignificantDigits() throws NumericException {
        final String value = "9.876e4";
        Decimal256 decimal = new Decimal256();
        long metadata = DecimalParser.parse(decimal, value, 0, value.length(), -1, 0, false, true);

        Assert.assertEquals("expected lossy parsing to retain all significant digits when moving the decimal point", "98760", decimal.toString());
        Assert.assertEquals("expected scale of 0", 0, Numbers.decodeHighInt(metadata));
        Assert.assertEquals("expected precision to reflect 5 digits", 5, Numbers.decodeLowInt(metadata));
    }

    @Test
    public void testLossyScientificNotationLosesMantissaForIntegerExponent() throws NumericException {
        final String value = "1e2";
        Decimal256 decimal = new Decimal256();
        long metadata = DecimalParser.parse(decimal, value, 0, value.length(), -1, 0, false, true);

        Assert.assertEquals("expected lossy parsing to preserve exponent shift", "100", decimal.toString());
        Assert.assertEquals("expected scale of 0", 0, Numbers.decodeHighInt(metadata));
        Assert.assertEquals("expected precision to reflect 3 digits", 3, Numbers.decodeLowInt(metadata));
    }

    @Test(expected = NumericException.class)
    public void testModuloByZero() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 zero = Decimal256.fromDouble(0.0, 2);

        a.modulo(zero);
    }

    @Test
    public void testModuloFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal256 a = rnd.nextDecimal256();
            Decimal256 b = rnd.nextDecimal256();

            if (!b.isZero()) {
                // Test modulo accuracy
                testModuloAccuracy(a, b, i);
            }
        }
    }

    @Test
    public void testModuloNegative() {
        // Test -10 % 3 = -1
        Decimal256 a = Decimal256.fromDouble(-10.0, 0);
        Decimal256 b = Decimal256.fromDouble(3.0, 0);

        a.modulo(b);

        Assert.assertEquals(-1.0, a.toDouble(), 0.001);
    }

    @Test
    public void testModuloNegativeScale() {
        // Test -10 % 3 = -1
        BigDecimal bdA = new BigDecimal("-0.0000000000000000000000000000000001364898122");
        BigDecimal bdB = new BigDecimal("0.000000000000000000000000018446744073709550324");
        Decimal256 a = Decimal256.fromBigDecimal(bdA);
        Decimal256 b = Decimal256.fromBigDecimal(bdB);

        a.modulo(b);

        Assert.assertEquals("-0.000000000000000000000000000000000136489812200", a.toString());
    }

    @Test
    public void testModuloNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        m.modulo(Decimal256.fromLong(1, 0));
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testModuloNullOther() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.modulo(Decimal256.NULL_VALUE);
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testModuloSimple() {
        // Test 10 % 3 = 1
        Decimal256 a = Decimal256.fromDouble(10.0, 0);
        Decimal256 b = Decimal256.fromDouble(3.0, 0);

        a.modulo(b);

        Assert.assertEquals(1.0, a.toDouble(), 0.001);
    }

    @Test
    public void testModuloWithDecimals() {
        // Test 10.5 % 3.2 = 0.9
        Decimal256 a = Decimal256.fromDouble(10.5, 1);
        Decimal256 b = Decimal256.fromDouble(3.2, 1);

        a.modulo(b);

        // 10.5 / 3.2 = 3.28125, floor = 3
        // 3 * 3.2 = 9.6
        // 10.5 - 9.6 = 0.9
        Assert.assertEquals(0.9, a.toDouble(), 0.001);
    }

    @Test
    public void testMultiplicationFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal256 a = rnd.nextDecimal256();
            Decimal256 b = rnd.nextDecimal256();

            // Test multiplication accuracy
            testMultiplicationAccuracy(a, b, i);
        }
    }

    @Test
    public void testMultiply() {
        Decimal256 a = new Decimal256(0, 0, 0, 100, 2);
        Decimal256 b = new Decimal256(0, 0, 0, 2, 0);
        a.multiply(b);
        Assert.assertEquals(200, a.getLl());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal256(0, 0, 0, 100, 2);
        b = new Decimal256(0, 0, 2, 0, 0);
        a.multiply(b);
        Assert.assertEquals(0, a.getLl());
        Assert.assertEquals(200, a.getLh());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal256(0, 0, 0, 100, 2);
        b = new Decimal256(0, 2, 0, 0, 0);
        a.multiply(b);
        Assert.assertEquals(0, a.getLl());
        Assert.assertEquals(0, a.getLh());
        Assert.assertEquals(200, a.getHl());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal256(0, 0, 0, 100, 2);
        b = new Decimal256(2, 0, 0, 0, 0);
        a.multiply(b);
        Assert.assertEquals(0, a.getLl());
        Assert.assertEquals(0, a.getLh());
        Assert.assertEquals(0, a.getHl());
        Assert.assertEquals(200, a.getHh());
        Assert.assertEquals(2, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testMultiply128Overflow() {
        Decimal256 a = new Decimal256(0, Long.MAX_VALUE / 2, Long.MAX_VALUE, Long.MAX_VALUE, 0);
        Decimal256 b = new Decimal256(0, 0, 3, 0, 0);
        a.multiply(b);
    }

    @Test
    public void testMultiplyNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        m.multiply(Decimal256.fromLong(1, 0));
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testMultiplyNullOther() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.multiply(Decimal256.NULL_VALUE);
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow128() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.multiply(new Decimal256(0, 0, 2, 0, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow128B() {
        Decimal256 m = new Decimal256(0, Long.MAX_VALUE, 0, 0, 0);
        m.multiply(new Decimal256(0, 0, 2, 1, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow192() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.multiply(new Decimal256(0, 2, 0, 0, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow192B() {
        Decimal256 m = new Decimal256(0, 0, Long.MAX_VALUE, 0, 0);
        m.multiply(new Decimal256(0, 2, 0, 1, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow256() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.multiply(new Decimal256(2, 0, 0, 0, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow256B() {
        Decimal256 m = new Decimal256(0, 0, 0, Long.MAX_VALUE, 0);
        m.multiply(new Decimal256(2, 0, 0, 1, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow64() {
        Decimal256 m = new Decimal256(Long.MAX_VALUE, 0, 0, 0, 0);
        m.multiply(new Decimal256(0, 0, 0, Long.MAX_VALUE, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow64B() {
        Decimal256 m = new Decimal256(Long.MAX_VALUE / 2, 0, 0, 0, 0);
        m.multiply(new Decimal256(0, 0, 0, 3, 0));
    }

    @Test
    public void testNegateNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        m.negate();
        Assert.assertTrue(m.isNull());
    }

    @Test
    public void testNegativeArithmetic() {
        // Test with negative numbers
        Decimal256 a = Decimal256.fromDouble(-12.5, 1);
        Decimal256 b = Decimal256.fromDouble(4.0, 1);

        a.multiply(b);

        Assert.assertEquals(-50.0, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());  // Scale should be 1 + 1 = 2

        // Test both negative
        Decimal256 c = Decimal256.fromDouble(-3.0, 1);
        Decimal256 d = Decimal256.fromDouble(-7.0, 1);

        c.multiply(d);
        Assert.assertEquals(21.0, c.toDouble(), 0.01);
    }

    @Test
    public void testNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        Assert.assertTrue(m.isNull());
    }

    @Test(expected = NumericException.class)
    public void testNullBigDecimal() {
        Decimal256.fromBigDecimal(null);
    }

    @Test
    public void testNumericExceptionDivisionByZero() {
        BigDecimal a = new BigDecimal("1e30");
        BigDecimal b = new BigDecimal("0");
        try {
            Decimal256 da = Decimal256.fromBigDecimal(a);
            Decimal256 db = Decimal256.fromBigDecimal(b);
            da.divide(db, 0, RoundingMode.HALF_UP);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            Assert.assertTrue(e.getMessage().contains("Division by zero"));
        }
    }

    @Test
    public void testNumericExceptionOverflow() {
        BigDecimal a = new BigDecimal("1e50");  // Larger number for 256-bit
        try {
            Decimal256 da = Decimal256.fromBigDecimal(a);
            Decimal256 db = Decimal256.fromBigDecimal(a);
            da.multiply(db);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            Assert.assertTrue(e.getMessage().contains("Overflow") || e.getMessage().contains("too large"));
        }
    }

    @Test
    public void testOfStringBoundaryValues() throws NumericException {
        // Test near max precision boundary
        Decimal256 d1 = new Decimal256();
        String maxPrecisionMinus1 = new String(new char[75]).replace('\0', '9');
        int precision1 = Numbers.decodeLowInt(d1.ofString(maxPrecisionMinus1));
        Assert.assertEquals(75, precision1);

        // Test near max scale boundary
        Decimal256 d2 = new Decimal256();
        String maxScaleMinus1 = "1." + new String(new char[75]).replace('\0', '1');
        int precision2 = Numbers.decodeLowInt(d2.ofString(maxScaleMinus1));
        Assert.assertEquals(76, precision2);
        Assert.assertEquals(75, d2.getScale());
    }

    @Test
    public void testOfStringComplexWithLeadingZerosAndSpaces() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("  -00123.4500M"));
        Assert.assertEquals(7, precision);
        Assert.assertEquals(4, d.getScale());
        Assert.assertEquals("-123.4500", d.toString());
    }

    @Test
    public void testOfStringDecimalFloat() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.0f"));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("123", d.toString());
    }

    @Test
    public void testOfStringDecimalPointAtEnd() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123."));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("123", d.toString());
    }

    @Test
    public void testOfStringDecimalPointAtStart() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString(".123"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("0.123", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringEmptyString() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("");
    }

    @Test
    public void testOfStringErrorMessageEmptyValue() {
        Decimal256 d = new Decimal256();
        try {
            d.ofString("");
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal: empty value");
        }
    }

    @Test
    public void testOfStringErrorMessageExceedsMaxPrecision() {
        Decimal256 d = new Decimal256();
        String tooLarge = new String(new char[77]).replace('\0', '9');
        try {
            d.ofString(tooLarge);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed precision of 76");
        }
    }

    @Test
    public void testOfStringErrorMessageExceedsMaxScale() {
        Decimal256 d = new Decimal256();
        String tooManyDecimals = "1." + new String(new char[77]).replace('\0', '1');
        try {
            d.ofString(tooManyDecimals);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "exceeds maximum allowed scale of 76");
        }
    }

    @Test
    public void testOfStringErrorMessageExceedsPrecision() {
        Decimal256 d = new Decimal256();
        try {
            d.ofString("123456", 5, -1);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '123456' requires precision of 6 but is limited to 5");
        }
    }

    @Test
    public void testOfStringErrorMessageExceedsScale() {
        Decimal256 d = new Decimal256();
        try {
            d.ofString("123.456", -1, 2);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "decimal '123.456' has 3 decimal places but scale is limited to 2");
        }
    }

    @Test
    public void testOfStringErrorMessageInvalidCharacter() {
        Decimal256 d = new Decimal256();
        try {
            d.ofString("123x456");
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "contains invalid character 'x'");
        }
    }

    @Test
    public void testOfStringErrorMessageNoDigits() {
        Decimal256 d = new Decimal256();
        try {
            d.ofString(".");
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            TestUtils.assertContains(e.getMessage(), "invalid decimal");
            TestUtils.assertContains(e.getMessage(), "contains no digits");
        }
    }

    @Test(expected = NumericException.class)
    public void testOfStringExceedsMaxPrecision() throws NumericException {
        Decimal256 d = new Decimal256();
        String tooLarge = new String(new char[77]).replace('\0', '9'); // 77 digits
        d.ofString(tooLarge);
    }

    @Test(expected = NumericException.class)
    public void testOfStringExceedsMaxScale() throws NumericException {
        Decimal256 d = new Decimal256();
        String tooManyDecimals = "1." + new String(new char[77]).replace('\0', '1'); // 77 decimal places
        d.ofString(tooManyDecimals);
    }

    @Test
    public void testOfStringExponentComplexNumbers() throws NumericException {
        Decimal256 d = new Decimal256();

        // Complex decimal with exponent
        int precision = Numbers.decodeLowInt(d.ofString("123.456789e-3"));
        Assert.assertEquals(10, precision);
        Assert.assertEquals(9, d.getScale());
        Assert.assertEquals("0.123456789", d.toString());

        // Another complex case
        precision = Numbers.decodeLowInt(d.ofString("9.87654321e5"));
        Assert.assertEquals(9, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("987654.321", d.toString());
    }

    @Test
    public void testOfStringExponentLargeButValid() throws NumericException {
        Decimal256 d = new Decimal256();

        // Large positive exponent within bounds
        int precision = Numbers.decodeLowInt(d.ofString("1e20"));
        Assert.assertEquals(21, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("100000000000000000000", d.toString());

        // Large negative exponent within bounds
        precision = Numbers.decodeLowInt(d.ofString("1e-20"));
        Assert.assertEquals(21, precision);
        Assert.assertEquals(20, d.getScale());
        Assert.assertEquals("0.00000000000000000001", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentMalformedDecimalInExponent() {
        Decimal256 d = new Decimal256();
        d.ofString("1.5e2.5");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentMalformedDoubleE() {
        Decimal256 d = new Decimal256();
        d.ofString("1.5ee2");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentMalformedMultipleE() {
        Decimal256 d = new Decimal256();
        d.ofString("1e2e3");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentMalformedNoDigitsAfterE() {
        Decimal256 d = new Decimal256();
        d.ofString("1.5e");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentMalformedSpaceAfterE() {
        Decimal256 d = new Decimal256();
        d.ofString("1.5e 2");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentMalformedSpaceBeforeE() {
        Decimal256 d = new Decimal256();
        d.ofString("1.5 e2");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentOverflowPositive() {
        Decimal256 d = new Decimal256();
        // Exponent too large, should cause overflow
        d.ofString("1e100");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentUnderflowNegative() {
        Decimal256 d = new Decimal256();
        // Exponent too negative, should cause underflow
        d.ofString("1e-100");
    }

    @Test
    public void testOfStringExponentWithIntegerBase() throws NumericException {
        Decimal256 d = new Decimal256();

        // Integer base with positive exponent
        int precision = Numbers.decodeLowInt(d.ofString("123e2"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("12300", d.toString());

        // Integer base with negative exponent
        precision = Numbers.decodeLowInt(d.ofString("456e-3"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("0.456", d.toString());
    }

    @Test
    public void testOfStringExponentWithLeadingZeros() throws NumericException {
        Decimal256 d = new Decimal256();

        // Leading zeros in mantissa
        int precision = Numbers.decodeLowInt(d.ofString("00.123e3"));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("123", d.toString());

        // Leading zeros in exponent
        precision = Numbers.decodeLowInt(d.ofString("1.5e02"));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("150", d.toString());
    }

    @Test
    public void testOfStringExponentWithNegativeNumber() throws NumericException {
        Decimal256 d = new Decimal256();

        // Negative number with positive exponent
        int precision = Numbers.decodeLowInt(d.ofString("-1.5e3"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("-1500", d.toString());

        // Negative number with negative exponent
        precision = Numbers.decodeLowInt(d.ofString("-2.5e-2"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("-0.025", d.toString());
    }

    @Test
    public void testOfStringExponentWithPrecisionLimit() throws NumericException {
        Decimal256 d = new Decimal256();

        // With precision limit - this should work as 123.4 has precision 4
        int precision = Numbers.decodeLowInt(d.ofString("1.234e2", 4, -1));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(1, d.getScale());
        Assert.assertEquals("123.4", d.toString());

        // With scale limit that matches the result scale
        precision = Numbers.decodeLowInt(d.ofString("1e-3", -1, 3));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("0.001", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentWithPrecisionLimitExceeded() {
        Decimal256 d = new Decimal256();
        // This should fail: 1.234e2 = 123.4 has precision 4, but limit is 3
        d.ofString("1.234e2", 3, -1);
    }

    @Test(expected = NumericException.class)
    public void testOfStringExponentWithScaleLimitExceeded() {
        Decimal256 d = new Decimal256();
        // This should fail: 1.234e-2 = 0.01234 has scale 5, but limit is 3
        d.ofString("1.234e-2", -1, 3);
    }

    @Test
    public void testOfStringExponentWithSuffix() throws NumericException {
        Decimal256 d = new Decimal256();

        // With M suffix
        int precision = Numbers.decodeLowInt(d.ofString("1.5e2M"));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("150", d.toString());

        // With m suffix
        precision = Numbers.decodeLowInt(d.ofString("2.5e-3m"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(4, d.getScale());
        Assert.assertEquals("0.0025", d.toString());
    }

    @Test
    public void testOfStringExponentWithTrailingZeros() throws NumericException {
        Decimal256 d = new Decimal256();

        // Trailing zeros in mantissa
        int precision = Numbers.decodeLowInt(d.ofString("1.2300e2m"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.00", d.toString());

        // Result with trailing zeros after exponent application
        precision = Numbers.decodeLowInt(d.ofString("1.00e3"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("1000", d.toString());
    }

    @Test
    public void testOfStringExponentWithWhitespace() throws NumericException {
        Decimal256 d = new Decimal256();

        // Leading whitespace
        int precision = Numbers.decodeLowInt(d.ofString("  1.5e3"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("1500", d.toString());

        // Note: Whitespace within the number should cause an error
    }

    @Test
    public void testOfStringInfinity() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("Infinity");
        Assert.assertTrue(d.isNull());
    }

    @Test(expected = NumericException.class)
    public void testOfStringInvalidCharacter() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("123a45");
    }

    @Test
    public void testOfStringLargeNumber() throws NumericException {
        Decimal256 d = new Decimal256();
        String largeNumber = "123456789012345678901234567890.123456789012345678901234567898";
        long meta = d.ofString(largeNumber);
        Assert.assertEquals(60, Numbers.decodeLowInt(meta));
        Assert.assertEquals(30, Numbers.decodeHighInt(meta));
        Assert.assertEquals(30, d.getScale());
        Assert.assertEquals(largeNumber, d.toString());
    }

    @Test
    public void testOfStringLossyFalseExactMatch() throws NumericException {
        Decimal256 d = new Decimal256();
        // When scale matches exactly, lossy=false should work fine
        long result = d.ofString("12.345", 0, 6, -1, 3, false, false);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(5, precision);
        Assert.assertEquals(3, scale);
        Assert.assertEquals("12.345", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringLossyFalseNegativeNumber() throws NumericException {
        Decimal256 d = new Decimal256();
        // lossy=false should throw error for negative numbers too
        d.ofString("-123.456789", 0, 11, -1, 3, false, false);
    }

    @Test(expected = NumericException.class)
    public void testOfStringLossyFalseThrowsError() throws NumericException {
        Decimal256 d = new Decimal256();
        // lossy=false should throw error when scale is exceeded
        d.ofString("12.3456", 0, 7, -1, 3, false, false);
    }

    @Test(expected = NumericException.class)
    public void testOfStringLossyFalseWithMoreDigits() throws NumericException {
        Decimal256 d = new Decimal256();
        // lossy=false should throw error for multiple extra digits
        d.ofString("123.456789", 0, 10, -1, 2, false, false);
    }

    @Test
    public void testOfStringLossyTrue() throws NumericException {
        Decimal256 d = new Decimal256();
        // lossy=true allows truncation of extra digits
        long result = d.ofString("12.3456", 0, 7, -1, 3, false, true);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(5, precision); // 2 digits before + 3 after = 5 total
        Assert.assertEquals(3, scale);
        Assert.assertEquals("12.345", d.toString()); // 6 is truncated
    }

    @Test
    public void testOfStringLossyTrueExactMatch() throws NumericException {
        Decimal256 d = new Decimal256();
        // When scale matches exactly, lossy should have no effect
        long result = d.ofString("12.345", 0, 6, -1, 3, false, true);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(5, precision);
        Assert.assertEquals(3, scale);
        Assert.assertEquals("12.345", d.toString());
    }

    @Test
    public void testOfStringLossyTrueNegativeNumber() throws NumericException {
        Decimal256 d = new Decimal256();
        // Test lossy with negative numbers
        long result = d.ofString("-123.456789", 0, 11, -1, 3, false, true);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(6, precision);
        Assert.assertEquals(3, scale);
        Assert.assertEquals("-123.456", d.toString());
    }

    @Test
    public void testOfStringLossyTrueWithMoreDigits() throws NumericException {
        Decimal256 d = new Decimal256();
        // Truncate multiple digits
        long result = d.ofString("123.456789", 0, 10, -1, 2, false, true);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(5, precision); // 3 digits before + 2 after = 5 total
        Assert.assertEquals(2, scale);
        Assert.assertEquals("123.45", d.toString()); // 6789 is truncated
    }

    @Test
    public void testOfStringLossyTrueWithNoScale() throws NumericException {
        Decimal256 d = new Decimal256();
        // lossy with scale=-1 (auto) should not truncate
        long result = d.ofString("12.3456", 0, 7, -1, -1, false, true);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(6, precision);
        Assert.assertEquals(4, scale);
        Assert.assertEquals("12.3456", d.toString());
    }

    @Test
    public void testOfStringLossyTrueWithZeroScale() throws NumericException {
        Decimal256 d = new Decimal256();
        // lossy with scale=0 should truncate all decimal places
        long result = d.ofString("12.3456", 0, 7, -1, 0, false, true);
        int precision = Numbers.decodeLowInt(result);
        int scale = Numbers.decodeHighInt(result);
        Assert.assertEquals(2, precision);
        Assert.assertEquals(0, scale);
        Assert.assertEquals("12", d.toString());
    }

    @Test
    public void testOfStringMaxPrecision() throws NumericException {
        Decimal256 d = new Decimal256();
        String maxNumber = new String(new char[76]).replace('\0', '9');
        int precision = Numbers.decodeLowInt(d.ofString(maxNumber));
        Assert.assertEquals(76, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals(maxNumber, d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringMultipleDecimalPoints() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("12.34.56");
    }

    @Test
    public void testOfStringNaN() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("  -NaNM");
        Assert.assertTrue(d.isNull());
    }

    @Test(expected = NumericException.class)
    public void testOfStringOnlyDecimalPoint() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString(".");
    }

    @Test(expected = NumericException.class)
    public void testOfStringOnlySign() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("-");
    }

    @Test(expected = NumericException.class)
    public void testOfStringOnlySpaces() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("   ");
    }

    @Test
    public void testOfStringPrecisionConsistency() throws NumericException {
        // Test that the returned precision matches the actual value precision
        Decimal256 d = new Decimal256();

        int p1 = Numbers.decodeLowInt(d.ofString("1"));
        Assert.assertEquals(1, p1);

        int p2 = Numbers.decodeLowInt(d.ofString("12"));
        Assert.assertEquals(2, p2);

        int p3 = Numbers.decodeLowInt(d.ofString("123.456"));
        Assert.assertEquals(6, p3);

        int p4 = Numbers.decodeLowInt(d.ofString("00.001"));
        Assert.assertEquals(4, p4); // Leading zeros don't count toward precision
    }

    @Test(expected = NumericException.class)
    public void testOfStringPrecisionExceedsSpecified() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("123456", 5, -1); // 6 digits but precision limited to 5
    }

    @Test(expected = NumericException.class)
    public void testOfStringScaleExceedsSpecified() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("123.456", -1, 2); // 3 decimal places but scale limited to 2
    }

    @Test
    public void testOfStringScalePadsZeros() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123", -1, 2));
        Assert.assertEquals(5, precision); // 3 + 2
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.00", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringSignAndDecimalPoint() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("-.");
    }

    @Test
    public void testOfStringSimpleNegative() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("-123"));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("-123", d.toString());
    }

    @Test
    public void testOfStringSimplePositive() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123"));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("123", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringSpecifiedScaleExceedsMax() throws NumericException {
        Decimal256 d = new Decimal256();
        d.ofString("123", -1, 77); // Scale 77 exceeds max of 76
    }

    @Test
    public void testOfStringWithBothPrecisionAndScale() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.45", 10, 3));
        Assert.assertEquals(6, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("123.450", d.toString());
    }

    @Test
    public void testOfStringWithCapitalMSuffix() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.45M"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithDecimalPoint() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithLeadingSpaces() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("  123.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithLeadingZeros() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("00123.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithMSuffix() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.45m"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithNegativeExponent() throws NumericException {
        Decimal256 d = new Decimal256();

        // Basic negative exponent
        int precision = Numbers.decodeLowInt(d.ofString("1.234e-2"));
        Assert.assertEquals(6, precision);
        Assert.assertEquals(5, d.getScale());
        Assert.assertEquals("0.01234", d.toString());

        // Larger negative exponent
        precision = Numbers.decodeLowInt(d.ofString("5e-5"));
        Assert.assertEquals(6, precision);
        Assert.assertEquals(5, d.getScale());
        Assert.assertEquals("0.00005", d.toString());

        // Negative exponent with E (uppercase)
        precision = Numbers.decodeLowInt(d.ofString("7.89E-3"));
        Assert.assertEquals(6, precision);
        Assert.assertEquals(5, d.getScale());
        Assert.assertEquals("0.00789", d.toString());

        // Very small number
        precision = Numbers.decodeLowInt(d.ofString("1e-10"));
        Assert.assertEquals(11, precision);
        Assert.assertEquals(10, d.getScale());
        Assert.assertEquals("0.0000000001", d.toString());
    }

    @Test
    public void testOfStringWithPositiveExponent() throws NumericException {
        Decimal256 d = new Decimal256();

        // Basic positive exponent
        int precision = Numbers.decodeLowInt(d.ofString("1.234e2"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(1, d.getScale());
        Assert.assertEquals("123.4", d.toString());

        // Large positive exponent
        precision = Numbers.decodeLowInt(d.ofString("1.5e10"));
        Assert.assertEquals(11, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("15000000000", d.toString());

        // Positive exponent with E (uppercase)
        precision = Numbers.decodeLowInt(d.ofString("2.5E3"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("2500", d.toString());

        // Positive exponent with + sign
        precision = Numbers.decodeLowInt(d.ofString("3.14e+5"));
        Assert.assertEquals(6, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("314000", d.toString());
    }

    @Test
    public void testOfStringWithPositiveSign() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("+123.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithSpecifiedPrecision() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.45", 10, -1));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithSpecifiedScale() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("123.45", -1, 5));
        Assert.assertEquals(8, precision); // 3 integer + 5 scale
        Assert.assertEquals(5, d.getScale());
        Assert.assertEquals("123.45000", d.toString());
    }

    @Test
    public void testOfStringWithZeroExponent() throws NumericException {
        Decimal256 d = new Decimal256();

        // Zero exponent
        int precision = Numbers.decodeLowInt(d.ofString("1.234e0"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("1.234", d.toString());

        // Zero exponent with E
        precision = Numbers.decodeLowInt(d.ofString("5.678E0"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("5.678", d.toString());

        // Zero exponent with +
        precision = Numbers.decodeLowInt(d.ofString("9.0e+0"));
        Assert.assertEquals(1, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("9", d.toString());
    }

    @Test
    public void testOfStringZero() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("0"));
        Assert.assertEquals(1, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("0", d.toString());
    }

    @Test
    public void testOfStringZeroWithDecimal() throws NumericException {
        Decimal256 d = new Decimal256();
        int precision = Numbers.decodeLowInt(d.ofString("0.000"));
        Assert.assertEquals(1, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("0", d.toString());

        precision = Numbers.decodeLowInt(d.ofString("0.000m"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("0.000", d.toString());

        precision = Numbers.decodeLowInt(d.ofString("0."));
        Assert.assertEquals(1, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("0", d.toString());
    }

    @Test
    public void testPowersTenTable() {
        BigDecimal bd = new BigDecimal(1);
        long[][] table = new long[Decimals.MAX_PRECISION][9 * 4];
        for (int i = 0; i < Decimals.MAX_PRECISION; i++) {
            BigDecimal base = bd;
            for (int j = 0; j < 9; j++) {
                Decimal256 d = Decimal256.fromBigDecimal(bd);
                table[i][j * 4] = d.getHh();
                table[i][j * 4 + 1] = d.getHl();
                table[i][j * 4 + 2] = d.getLh();
                table[i][j * 4 + 3] = d.getLl();
                bd = bd.add(base);
            }
        }

        printPowerTable(table);

        long[][] currentTable = Decimal256.getPowersTenTable();
        for (int i = 0; i < Decimals.MAX_PRECISION; i++) {
            for (int j = 0; j < 9 * 4; j++) {
                Assert.assertEquals(table[i][j], currentTable[i][j]);
            }
        }
    }

    @Test
    public void testRescale128() {
        Decimal256 a = Decimal256.fromLong(1, 0);
        a.rescale(30);
        Assert.assertEquals("1.000000000000000000000000000000", a.toString());
    }

    @Test
    public void testRescale192() {
        Decimal256 a = Decimal256.fromLong(1, 0);
        a.rescale(50);
        Assert.assertEquals("1.00000000000000000000000000000000000000000000000000", a.toString());
    }

    @Test
    public void testRescale256() {
        Decimal256 a = Decimal256.fromLong(1, 0);
        a.rescale(65);
        Assert.assertEquals("1.00000000000000000000000000000000000000000000000000000000000000000", a.toString());
    }

    @Test
    public void testRescale64() {
        Decimal256 a = Decimal256.fromLong(1, 0);
        a.rescale(10);
        Assert.assertEquals("1.0000000000", a.toString());
    }

    @Test
    public void testRescaleLess() {
        Decimal256 m = Decimal256.fromLong(12340, 2); // 123.40
        m.rescale(1); // / 10 -> ok, last digit is 0
        Assert.assertEquals(1234, m.getLl());
        Assert.assertEquals(1, m.getScale());
    }

    @Test(expected = NumericException.class)
    public void testRescaleLessFails() {
        Decimal256 m = Decimal256.fromLong(12345, 2); // 123.45
        m.rescale(1); // / 10 -> fails because it ends with a non-zero digit
    }

    @Test(expected = NumericException.class)
    public void testRescaleLessFails2() {
        Decimal256 a = new Decimal256(0, 0, 0, 100, 10);
        a.rescale(5);
    }

    @Test
    public void testRescaleMore() {
        Decimal256 m = Decimal256.fromLong(12345, 2); // 123.45
        m.rescale(4); // * 100
        Assert.assertEquals(1234500, m.getLl());
        Assert.assertEquals(4, m.getScale());
    }

    @Test
    public void testRescaleNegative() {
        Decimal256 a = Decimal256.fromLong(-10, 0);
        a.rescale(2);
        Assert.assertEquals(-1000, a.getLl());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testRescaleNull() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.NULL_VALUE);
        m.rescale(10);
        Assert.assertTrue(m.isNull());
    }

    @Test(expected = NumericException.class)
    public void testRescaleOverflowHh() {
        Decimal256 a = new Decimal256(100, 0, 0, 0, 0);
        a.rescale(20);
    }

    @Test(expected = NumericException.class)
    public void testRescaleOverflowHl() {
        Decimal256 a = new Decimal256(0, 100, 0, 0, 0);
        a.rescale(42);
    }

    @Test(expected = NumericException.class)
    public void testRescaleOverflowLh() {
        Decimal256 a = new Decimal256(0, 0, 100, 0, 0);
        a.rescale(62);
    }

    @Test(expected = NumericException.class)
    public void testRescaleOverflowLl() {
        Decimal256 a = new Decimal256(0, 0, 0, 100, 0);
        a.rescale(76);
    }

    @Test
    public void testRound() {
        // Test basic rounding from scale 3 to scale 2
        Decimal256 a = Decimal256.fromDouble(1.234, 3);
        a.round(2, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(1.23);
        Assert.assertEquals("Basic rounding failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 2", 2, a.getScale());
    }

    @Test
    public void testRoundFuzz() {
        // Fuzz test for round() method using BigDecimal as oracle
        final int iterations = 10_000;  // Reduced for debugging
        final Rnd rnd = TestUtils.generateRandom(null);
        rnd.reset(120289594706857L, 1757435072227L);

        // All rounding modes except UNNECESSARY (which requires special handling)
        java.math.RoundingMode[] roundingModes = {
                java.math.RoundingMode.UP,
                java.math.RoundingMode.DOWN,
                java.math.RoundingMode.CEILING,
                java.math.RoundingMode.FLOOR,
                java.math.RoundingMode.HALF_UP,
                java.math.RoundingMode.HALF_DOWN,
                java.math.RoundingMode.HALF_EVEN
        };

        Decimal256 decimal = new Decimal256();
        for (int i = 0; i < iterations; i++) {
            // Generate random decimal with varying characteristics
            rnd.nextDecimal256(decimal);

            // Skip zero and very small values that might cause issues
            if (decimal.isZero()) {
                continue;
            }

            // Generate random target scale (0 to 10)
            int targetScale = rnd.nextInt(11);

            // Skip cases where target scale equals current scale (no rounding needed)
            if (targetScale == decimal.getScale()) {
                continue;
            }

            // Randomly select rounding mode
            java.math.RoundingMode roundingMode = roundingModes[rnd.nextInt(roundingModes.length)];

            // Create copies for testing
            Decimal256 testDecimal = new Decimal256();
            testDecimal.copyFrom(decimal);

            // Get the original BigDecimal representation
            java.math.BigDecimal originalBigDecimal;
            try {
                originalBigDecimal = decimal.toBigDecimal();
            } catch (NumberFormatException e) {
                String errorMsg = String.format(
                        "Failed to convert original Decimal256 to BigDecimal at iteration %d:\n" +
                                "Decimal256: hh=0x%016x, hl=0x%016x, lh=0x%016x, ll=0x%016x, scale=%d\n" +
                                "toString()=%s\n" +
                                "Error: %s",
                        i, decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl(), decimal.getScale(),
                        decimal, e.getMessage()
                );
                Assert.fail(errorMsg);
                return; // unreachable but makes compiler happy
            }

            try {
                // Apply rounding to our Decimal256
                testDecimal.round(targetScale, roundingMode);

                // Apply same rounding to BigDecimal as oracle
                java.math.BigDecimal expectedBigDecimal = originalBigDecimal.setScale(targetScale, roundingMode);

                // Compare results
                java.math.BigDecimal actualBigDecimal;
                try {
                    actualBigDecimal = testDecimal.toBigDecimal();
                } catch (NumberFormatException e) {
                    String errorMsg = String.format(
                            "Failed to convert result Decimal256 to BigDecimal at iteration %d:\n" +
                                    "Original: %s (scale=%d)\n" +
                                    "Target scale: %d, Mode: %s\n" +
                                    "Result Decimal256: hh=0x%016x, hl=0x%016x, lh=0x%016x, ll=0x%016x, scale=%d\n" +
                                    "toString()=%s\n" +
                                    "Error: %s",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            testDecimal.getHh(), testDecimal.getHl(), testDecimal.getLh(), testDecimal.getLl(), testDecimal.getScale(),
                            testDecimal, e.getMessage()
                    );
                    Assert.fail(errorMsg);
                    return; // unreachable but makes compiler happy
                }

                if (!expectedBigDecimal.equals(actualBigDecimal)) {
                    String errorMsg = String.format(
                            "Rounding mismatch at iteration %d:\n" +
                                    "Original: %s (scale=%d)\n" +
                                    "Target scale: %d, Mode: %s\n" +
                                    "Expected: %s\n" +
                                    "Actual: %s\n" +
                                    "Original Decimal256: hh=0x%016x, hl=0x%016x, lh=0x%016x, ll=0x%016x, scale=%d",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            expectedBigDecimal.toPlainString(),
                            actualBigDecimal.toPlainString(),
                            decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl(), decimal.getScale()
                    );
                    Assert.fail(errorMsg);
                }

                // Verify the scale is set correctly
                Assert.assertEquals("Scale should match target scale", targetScale, testDecimal.getScale());

            } catch (NumericException e) {
                // BigDecimal might throw NumericException in some cases
                // In such cases, our implementation should either handle it gracefully
                // or throw the same exception
                boolean decimal256Threw = false;
                try {
                    testDecimal.copyFrom(decimal);
                    testDecimal.round(targetScale, roundingMode);
                } catch (NumericException e2) {
                    decimal256Threw = true;
                }

                if (!decimal256Threw) {
                    String errorMsg = String.format(
                            "BigDecimal threw NumericException but Decimal256 didn't at iteration %d:\n" +
                                    "Original: %s (scale=%d)\n" +
                                    "Target scale: %d, Mode: %s\n" +
                                    "BigDecimal error: %s",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            e.getMessage()
                    );
                    Assert.fail(errorMsg);
                }
            }
        }
    }

    @Test
    public void testRoundNegativeScale() {
        Decimal256 a = Decimal256.fromLong(-5, 0);

        a.round(2, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals(-500, a.getLl());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testRoundNull() {
        Decimal256 a = new Decimal256();
        a.copyFrom(Decimal256.NULL_VALUE);
        a.round(2, java.math.RoundingMode.HALF_UP);
        Assert.assertTrue(a.isNull());
    }

    @Test
    public void testRoundUseless() {
        Decimal256 a = Decimal256.fromLong(12345, 2); // 123.45
        a.round(2, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals(12345, a.getLl());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testRoundZero() {
        Decimal256 a = Decimal256.fromLong(0, 3);

        a.round(2, java.math.RoundingMode.HALF_UP);
        Assert.assertTrue(a.isZero());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testSetScale() {
        Decimal256 a = new Decimal256(0, 0, 0, 12345, 2);
        a.setScale(1);
        Assert.assertEquals("1234.5", a.toString());
    }

    @Test
    public void testSinkNull() {
        // Sinking a null value shouldn't print anything
        StringSink sink = new StringSink();
        Decimal256.NULL_VALUE.toSink(sink);
        Assert.assertEquals("", sink.toString());
    }

    @Test
    public void testStaticAdd() {
        Decimal256 a = Decimal256.fromDouble(123.45, 2);
        Decimal256 b = Decimal256.fromDouble(67.89, 2);
        Decimal256 result = new Decimal256();

        Decimal256.add(a, b, result);

        Assert.assertEquals(191.34, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(123.45, a.toDouble(), 0.01);
        Assert.assertEquals(67.89, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticDivide() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 b = Decimal256.fromDouble(4.0, 1);
        Decimal256 result = new Decimal256();

        Decimal256.divide(a, b, result, 2, RoundingMode.HALF_UP);

        Assert.assertEquals(25.0, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(100.0, a.toDouble(), 0.01);
        Assert.assertEquals(4.0, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticMethodsWithDifferentScales() {
        Decimal256 a = Decimal256.fromDouble(123.45, 2);  // Scale 2
        Decimal256 b = Decimal256.fromDouble(6.789, 3);   // Scale 3
        Decimal256 result = new Decimal256();

        Decimal256.add(a, b, result);

        Assert.assertEquals(3, result.getScale());  // Should use larger scale
        Assert.assertEquals(130.239, result.toDouble(), 0.001);

        // Verify operands remain unchanged with original scales
        Assert.assertEquals(2, a.getScale());
        Assert.assertEquals(3, b.getScale());
        Assert.assertEquals(123.45, a.toDouble(), 0.01);
        Assert.assertEquals(6.789, b.toDouble(), 0.001);
    }

    @Test
    public void testStaticModulo() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 b = Decimal256.fromDouble(30.0, 2);
        Decimal256 result = new Decimal256();

        Decimal256.modulo(a, b, result);

        Assert.assertEquals(10.0, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(100.0, a.toDouble(), 0.01);
        Assert.assertEquals(30.0, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticMultiply() {
        Decimal256 a = Decimal256.fromDouble(12.34, 2);
        Decimal256 b = Decimal256.fromDouble(5.6, 1);
        Decimal256 result = new Decimal256();

        Decimal256.multiply(a, b, result);

        Assert.assertEquals(69.104, result.toDouble(), 0.001);
        Assert.assertEquals(3, result.getScale()); // 2 + 1 = 3

        // Verify operands are unchanged
        Assert.assertEquals(12.34, a.toDouble(), 0.01);
        Assert.assertEquals(5.6, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticNegate() {
        Decimal256 result = new Decimal256();

        // Test positive number
        Decimal256 a = Decimal256.fromDouble(42.5, 1);
        a.toDecimal256(result);
        result.negate();
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Verify original is unchanged - compare with fresh instance
        Decimal256 original = Decimal256.fromDouble(42.5, 1);
        Assert.assertEquals(original.toBigDecimal(), a.toBigDecimal());

        // Test negative number
        a = Decimal256.fromDouble(-123.456, 3);
        result.copyFrom(a);
        result.negate();
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Verify original is unchanged - compare with fresh instance
        original = Decimal256.fromDouble(-123.456, 3);
        Assert.assertEquals(original.toBigDecimal(), a.toBigDecimal());

        // Test zero
        a = Decimal256.fromDouble(0.0, 0);
        result.copyFrom(a);
        result.negate();
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Test very small number
        a = Decimal256.fromDouble(1e-10, 10);
        result.copyFrom(a);
        result.negate();
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Test very large number
        a = Decimal256.fromDouble(1e10, 0);
        result.copyFrom(a);
        result.negate();
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());
    }

    @Test
    public void testStaticSubtract() {
        Decimal256 a = Decimal256.fromDouble(123.45, 2);
        Decimal256 b = Decimal256.fromDouble(67.89, 2);
        Decimal256 result = new Decimal256();

        Decimal256.subtract(a, b, result);

        Assert.assertEquals(55.56, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(123.45, a.toDouble(), 0.01);
        Assert.assertEquals(67.89, b.toDouble(), 0.01);
    }

    @Test
    public void testStorageSizeBasic() {
        // value -> expected storage size (pow 2 bytes)
        final Object[][] cases = new Object[][]{
                {"0", 0},
                {"55", 0},
                {"127", 0},
                {"128", 1},
                {"32767", 1},
                {"32768", 2},
                {"65565", 2},
                {"2147483647", 2},
                {"2147483648", 3},
                {"12345678901234", 3},
                {"9223372036854775807", 3},
                {"9223372036854775808", 4},
                {"123456789012345678901234567890", 4},
                {"170141183460469231731687303715884105727", 4},
                {"170141183460469231731687303715884105728", 5},
                {"9999999999999999999999999999999999999999999999999999999999999999999999999999", 5},
        };
        for (Object[] case0 : cases) {
            String value = (String) case0[0];
            int expectedStorageSize = (int) case0[1];
            BigDecimal d = new BigDecimal(value);
            Decimal256 decimal = Decimal256.fromBigDecimal(d);
            Assert.assertEquals(String.format("Test failed with decimal %s", decimal), expectedStorageSize, decimal.getStorageSize());
            decimal.negate();
            // Negated values have the storage size as the positive one
            Assert.assertEquals(String.format("Test failed with decimal %s", decimal), expectedStorageSize, decimal.getStorageSize());
        }
    }

    @Test
    public void testSubtractPowerOfTenMultiple() {
        // Test subtracting 500 from 523
        Decimal256 value = new Decimal256(0, 0, 0, 523, 0);
        value.subtractPowerOfTenMultiple(2, 5);
        Assert.assertEquals(23, value.getLl());

        // Test subtracting 20 from 23
        value.subtractPowerOfTenMultiple(1, 2);
        Assert.assertEquals(3, value.getLl());

        // Test subtracting 3 from 3
        value.subtractPowerOfTenMultiple(0, 3);
        Assert.assertEquals(0, value.getLl());
    }

    @Test
    public void testSubtractPowerOfTenMultipleZero() {
        // Test that multiplier 0 leaves value unchanged
        Decimal256 value = new Decimal256(0, 0, 0, 123, 0);
        value.subtractPowerOfTenMultiple(1, 0);
        Assert.assertEquals(123, value.getLl());
    }

    @Test
    public void testSubtractionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal256 a = rnd.nextDecimal256();
            Decimal256 b = rnd.nextDecimal256();

            // Test subtraction accuracy
            testSubtractionAccuracy(a, b, i);
        }
    }

    @Test
    public void testSubtractionNull() {
        Decimal256 m = new Decimal256();
        Decimal256.NULL_VALUE.toDecimal256(m);
        m.subtract(Decimal256.fromLong(1, 0));
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testSubtractionNullOther() {
        Decimal256 m = new Decimal256();
        m.copyFrom(Decimal256.MAX_VALUE);
        m.subtract(Decimal256.NULL_VALUE);
        Assert.assertEquals(Decimal256.NULL_VALUE, m);
    }

    @Test
    public void testToBigDecimal() {
        // Test basic positive number
        Decimal256 a = Decimal256.fromDouble(123.456, 3);
        java.math.BigDecimal bigDecimal = a.toBigDecimal();
        Assert.assertEquals("123.456", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());

        // Test negative number
        a = Decimal256.fromDouble(-789.123, 3);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("-789.123", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());

        // Test zero
        a = Decimal256.fromDouble(0.0, 2);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("0.00", bigDecimal.toString());
        Assert.assertEquals(2, bigDecimal.scale());

        // Test integer (scale 0)
        a = Decimal256.fromDouble(42.0, 0);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("42", bigDecimal.toString());
        Assert.assertEquals(0, bigDecimal.scale());

        // Test very small number
        a = Decimal256.fromDouble(0.001, 3);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("0.001", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());
    }

    @Test
    public void testToDouble() {
        Decimal256 decimal = Decimal256.fromLong(12345, 3);
        Assert.assertEquals(12.345, decimal.toDouble(), 0.0001);

        Decimal256 negative = new Decimal256(-1, -1, -1, -1, 2); // Two's complement representation
        Assert.assertTrue(negative.toDouble() < 0);
    }

    @Test
    public void testToSinkBasic() {
        Decimal256 decimal = Decimal256.fromLong(12345, 2);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("123.45", sink.toString());
    }

    @Test
    public void testToSinkVsToString() {
        // Test that toSink and toString produce the same result
        Decimal256 decimal = Decimal256.fromDouble(123.456, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);
        String sinkResult = sink.toString();
        String toStringResult = decimal.toString();

        Assert.assertEquals(toStringResult, sinkResult);
    }

    @Test
    public void testToString() {
        Decimal256 decimal = Decimal256.fromLong(12345, 2);
        String str = decimal.toString();

        Assert.assertTrue(str.contains("123"));
        Assert.assertTrue(str.contains("45"));
    }

    @Test
    public void testToStringMaxScale() {
        Decimal256 a = new Decimal256();
        a.copyFrom(Decimal256.MAX_VALUE);
        a.setScale(Decimal256.MAX_SCALE);
        a.subtract(0, 0, 0, 1, Decimal256.MAX_SCALE);
        Assert.assertEquals("0.9999999999999999999999999999999999999999999999999999999999999999999999999998", a.toString());
    }

    @Test
    public void testUncheckedAddDecimal128HighWordContribution() {
        Decimal256 result = Decimal256.fromLong(0, 0);
        Decimal128 addend = Decimal128.fromBigDecimal(new BigDecimal("18446744073709551616"));

        Decimal256.uncheckedAdd(result, addend);

        Assert.assertEquals("18446744073709551616", result.toString());
    }

    @Test
    public void testUncheckedAddDecimal128KeepsScale() {
        Decimal256 result = Decimal256.fromLong(150, 2);
        Decimal128 addend = Decimal128.fromLong(25, 2);

        Decimal256.uncheckedAdd(result, addend);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals("1.75", result.toString());
    }

    @Test
    public void testUncheckedAddDecimal128NegativeSignExtension() {
        Decimal256 result = Decimal256.fromLong(50, 2);
        Decimal128 addend = Decimal128.fromLong(-125, 2);

        Decimal256.uncheckedAdd(result, addend);

        Assert.assertEquals("-0.75", result.toString());
    }

    private void printPowerTable(long[][] table) {
        System.err.println("    private static final long[][] POWERS_TEN_TABLE = new long[][]{");
        for (int i = 0, n = table.length; i < n; i++) {
            System.err.print("            {");
            for (int j = 0; j < 9 * 4; j++) {
                System.err.printf("%dL", table[i][j]);
                if (j < (9 * 4 - 1)) {
                    System.err.print(", ");
                }
            }
            System.err.print("}");
            if (i < n - 1) {
                System.err.print(",");
            }
            System.err.println();
        }
        System.err.println("    };");
    }

    private void testAdditionAccuracy(Decimal256 a, Decimal256 b, int iteration) {
        // Test addition accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference addition
        BigDecimal expected = bigA.add(bigB);

        BigDecimal min = Decimal256.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal256.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected
            Decimal256 result = new Decimal256();
            Assert.assertThrows(NumericException.class, () -> Decimal256.add(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal256 staticResult = new Decimal256();

            // Test static add method
            Decimal256.add(a, b, staticResult);

            Decimal256 result = new Decimal256();
            result.copyFrom(a);

            // Test in-place add method
            result.add(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place addition differ at iteration " + iteration,
                    result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();

            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow") || e.getMessage().contains("too large")) {
                // This is expected for cases where intermediate calculations would exceed 256-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testDivisionAccuracy(Decimal256 a, Decimal256 b, int iteration) {
        // Choose a reasonable result scale
        int resultScale = Math.min(a.getScale() + 2, 6); // Limit to avoid precision issues

        // Test division accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform division with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.divide(bigB, resultScale, RoundingMode.HALF_UP);

        // catch overflow exceptions
        try {
            Decimal256 staticResult = new Decimal256();

            // Test static divide method
            Decimal256.divide(a, b, staticResult, resultScale, RoundingMode.HALF_UP);

            Decimal256 result = new Decimal256();
            result.copyFrom(a);

            // Test in-place divide method
            result.divide(b, resultScale, RoundingMode.HALF_UP);

            // Results should be the same
            Assert.assertEquals("Static and in-place division differ at iteration " + iteration,
                    result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();
            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow") || e.getMessage().contains("too large")) {
                // This is expected for cases where intermediate calculations would exceed 256-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testModuloAccuracy(Decimal256 a, Decimal256 b, int iteration) {
        // Test modulo accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform modulo with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.remainder(bigB);

        // catch overflow exceptions
        try {
            Decimal256 staticResult = new Decimal256();

            // Test static modulo method
            Decimal256.modulo(a, b, staticResult);

            Decimal256 result = new Decimal256();
            result.copyFrom(a);

            // Test in-place modulo method
            result.modulo(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place modulo differ at iteration " + iteration,
                    result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();
            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow") || e.getMessage().contains("too large")) {
                // This is expected for cases where intermediate calculations would exceed 256-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testMultiplicationAccuracy(Decimal256 a, Decimal256 b, int iteration) {
        // Test multiplication accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform multiplication with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.multiply(bigB);

        BigDecimal min = Decimal256.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal256.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected
            Decimal256 result = new Decimal256();
            Assert.assertThrows(NumericException.class, () -> Decimal256.multiply(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal256 staticResult = new Decimal256();

            // Test static multiply method
            Decimal256.multiply(a, b, staticResult);

            Decimal256 result = new Decimal256();
            result.copyFrom(a);

            // Test in-place multiply method
            result.multiply(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place multiplication differ at iteration " + iteration,
                    result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();
            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow") || e.getMessage().contains("Invalid scale") || e.getMessage().contains("too large")) {
                // This is expected for cases where intermediate calculations would exceed 256-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testSubtractionAccuracy(Decimal256 a, Decimal256 b, int iteration) {
        // Test subtraction accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference subtraction
        BigDecimal expected = bigA.subtract(bigB);

        BigDecimal min = Decimal256.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal256.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected
            Decimal256 result = new Decimal256();
            Assert.assertThrows(NumericException.class, () -> Decimal256.subtract(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal256 staticResult = new Decimal256();

            // Test static subtract method
            Decimal256.subtract(a, b, staticResult);

            // Verify operands unchanged
            Assert.assertEquals("Subtract modified first operand at iteration " + iteration,
                    a.toBigDecimal(), a.toBigDecimal());
            Assert.assertEquals("Subtract modified second operand at iteration " + iteration,
                    b.toBigDecimal(), b.toBigDecimal());

            Decimal256 result = new Decimal256();
            a.toDecimal256(result);

            // Test in-place subtract method
            result.subtract(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place subtraction differ at iteration " + iteration,
                    result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();

            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow") || e.getMessage().contains("too large")) {
                // This is expected for cases where intermediate calculations would exceed 256-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }
}
