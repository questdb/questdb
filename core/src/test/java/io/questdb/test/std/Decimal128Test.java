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
 * Tests for the consolidated Decimal128 class
 */
public class Decimal128Test {

    @Test(expected = NumericException.class)
    public void testAddOverflow() {
        Decimal128 a = new Decimal128(Long.MAX_VALUE, 0, 0);
        Decimal128 b = new Decimal128(1, 0, 0);
        a.add(b);
    }

    @Test
    public void testAddZeroMaxValue() {
        Decimal128 b = new Decimal128(0, 0, 0);
        b.add(Decimal128.MAX_VALUE);
        Assert.assertEquals(b, Decimal128.MAX_VALUE);
    }

    @Test
    public void testAddRescaleB() {
        Decimal128 a = new Decimal128(0, 100, 2);
        Decimal128 b = new Decimal128(0, 1, 0);
        a.add(b);
        Assert.assertEquals(200, a.getLow());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testAdditionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal128 a = rnd.nextDecimal128();
            Decimal128 b = rnd.nextDecimal128();

            // Test addition accuracy
            testAdditionAccuracy(a, b, i);
        }
    }

    @Test(expected = NumericException.class)
    public void testBigDecimalOverflow() {
        BigDecimal bd = new BigDecimal("1e100");
        Decimal128.fromBigDecimal(bd);
    }

    @Test
    public void testCompareTo() {
        Decimal128 smaller = new Decimal128(0, 100, 2);
        Decimal128 larger = new Decimal128(0, 200, 2);

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));

        smaller.of(100, 0, 2);
        larger.of(200, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
    }

    @Test
    public void testCompareToDifferentSigns() {
        Decimal128 a = Decimal128.fromLong(1L, 0);
        Decimal128 b = Decimal128.fromLong(-1L, 0);
        Assert.assertEquals(1, a.compareTo(b));
    }

    @Test
    public void testCompareToFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal128 a = rnd.nextDecimal128();
            Decimal128 b = rnd.nextDecimal128();

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
    public void testCompareToMinValue() {
        Decimal128 a = new Decimal128();
        a.copyFrom(Decimal128.MIN_VALUE);
        a.add(Decimal128.fromLong(1, 0));
        Assert.assertEquals(1, a.compareTo(Decimal128.MIN_VALUE));
    }

    @Test
    public void testCompareToScaled() {
        Decimal128 smaller = new Decimal128(0, 10, 1);
        Decimal128 larger = new Decimal128(0, 200, 2);

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));

        smaller.of(10, 0, 1);
        larger.of(200, 0, 2);
        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
    }

    @Test
    public void testCompareToWithDifferentScales() {
        // Test 12.34 (scale 2) vs 12.345 (scale 3)
        Decimal128 a = Decimal128.fromDouble(12.34, 2);   // 12.34
        Decimal128 b = Decimal128.fromDouble(12.345, 3);  // 12.345

        Assert.assertTrue(a.compareTo(b) < 0); // 12.34 < 12.345
        Assert.assertTrue(b.compareTo(a) > 0); // 12.345 > 12.34

        // Test 12.340 (scale 3) vs 12.34 (scale 2) - should be equal
        Decimal128 c = Decimal128.fromDouble(12.340, 3);  // 12.340
        Decimal128 d = Decimal128.fromDouble(12.34, 2);   // 12.34

        Assert.assertEquals(0, c.compareTo(d)); // 12.340 == 12.34
        Assert.assertEquals(0, d.compareTo(c)); // 12.34 == 12.340

        // Test with larger scale difference: 1.2 (scale 1) vs 1.23456 (scale 5)
        Decimal128 e = Decimal128.fromDouble(1.2, 1);      // 1.2
        Decimal128 f = Decimal128.fromDouble(1.23456, 5);  // 1.23456

        Assert.assertTrue(e.compareTo(f) < 0); // 1.2 < 1.23456
        Assert.assertTrue(f.compareTo(e) > 0); // 1.23456 > 1.2

        // Test negative numbers with different scales
        Decimal128 g = Decimal128.fromDouble(-12.3, 1);    // -12.3
        Decimal128 h = Decimal128.fromDouble(-12.34, 2);   // -12.34

        Assert.assertTrue(g.compareTo(h) > 0); // -12.3 > -12.34
        Assert.assertTrue(h.compareTo(g) < 0); // -12.34 < -12.3

        // Test zero with different scales
        Decimal128 zero1 = Decimal128.fromDouble(0.0, 1);
        Decimal128 zero2 = Decimal128.fromDouble(0.0, 3);

        Assert.assertEquals(0, zero1.compareTo(zero2)); // 0.0 == 0.000
        Assert.assertEquals(0, zero2.compareTo(zero1)); // 0.000 == 0.0
    }

    @Test
    public void testCompareToWithNull() {
        // Create null decimal
        Decimal128 nullDecimal = new Decimal128();
        nullDecimal.ofNull();
        Assert.assertTrue(nullDecimal.isNull());

        // Create another null decimal
        Decimal128 anotherNullDecimal = new Decimal128();
        anotherNullDecimal.ofNull();

        // Create non-null decimal
        Decimal128 nonNullDecimal = Decimal128.fromLong(123, 2);
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

        // Test using the direct compareTo(high, low, scale) method
        Assert.assertEquals("null compareTo(DECIMAL128_HI_NULL, DECIMAL128_LO_NULL, 0) should return 0",
                0, nullDecimal.compareTo(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0));

        Assert.assertEquals("non-null compareTo(DECIMAL128_HI_NULL, DECIMAL128_LO_NULL, 0) should return 1",
                1, nonNullDecimal.compareTo(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 0));

        // Test that null decimal with compareTo on another null returns 0 regardless of scale
        Assert.assertEquals("null compareTo null with different scale should return 0",
                0, nullDecimal.compareTo(Decimals.DECIMAL128_HI_NULL, Decimals.DECIMAL128_LO_NULL, 5));
    }

    @Test
    public void testConstructorAndGetters() {
        Decimal128 decimal = new Decimal128(0x123456789ABCDEFL, 0xFEDCBA9876543210L, 3);

        Assert.assertEquals(0x123456789ABCDEFL, decimal.getHigh());
        Assert.assertEquals(0xFEDCBA9876543210L, decimal.getLow());
        Assert.assertEquals(3, decimal.getScale());
    }

    @Test
    public void testCopyFrom() {
        Decimal128 original = Decimal128.fromDouble(123.456, 3);
        Decimal128 copy = new Decimal128();

        copy.copyFrom(original);

        Assert.assertEquals(original.getHigh(), copy.getHigh());
        Assert.assertEquals(original.getLow(), copy.getLow());
        Assert.assertEquals(original.getScale(), copy.getScale());
    }

    @Test
    public void testDigitExtractionLoop() {
        // Test extracting all digits from 12345 using the two methods together
        Decimal128 value = new Decimal128(0, 12345, 0);

        // Extract ten thousands digit (1)
        int digit = value.getDigitAtPowerOfTen(4);
        Assert.assertEquals(1, digit);
        value.subtract(0, 10000L * digit, 0);
        Assert.assertEquals(2345, value.getLow());

        // Extract thousands digit (2)
        digit = value.getDigitAtPowerOfTen(3);
        Assert.assertEquals(2, digit);
        value.subtract(0, 1000L * digit, 0);
        Assert.assertEquals(345, value.getLow());

        // Extract hundreds digit (3)
        digit = value.getDigitAtPowerOfTen(2);
        Assert.assertEquals(3, digit);
        value.subtract(0, 100L * digit, 0);
        Assert.assertEquals(45, value.getLow());

        // Extract tens digit (4)
        digit = value.getDigitAtPowerOfTen(1);
        Assert.assertEquals(4, digit);
        value.subtract(0, 10L * digit, 0);
        Assert.assertEquals(5, value.getLow());

        // Extract ones digit (5)
        digit = value.getDigitAtPowerOfTen(0);
        Assert.assertEquals(5, digit);
        value.subtract(0, digit, 0);
        Assert.assertEquals(0, value.getLow());
    }

    @Test
    public void testDivide() {
        Decimal128 a = new Decimal128(0, 1000, 2);
        Decimal128 b = new Decimal128(0, 3, 0);
        a.divide(b, 2, RoundingMode.HALF_UP);
        Assert.assertEquals(333, a.getLow());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal128(0, 10, 0);
        b = new Decimal128(0, 300, 2);
        a.divide(b, 2, RoundingMode.HALF_UP);
        Assert.assertEquals(333, a.getLow());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal128(10, 0, 0);
        b = new Decimal128(3, 0, 0);
        a.divide(b, 0, RoundingMode.HALF_UP);
        Assert.assertEquals(3, a.getLow());
        Assert.assertEquals(0, a.getScale());
    }

    @Test
    public void testDivide64() {
        Decimal128 a = Decimal128.fromDouble(123.456, 3);
        Decimal128 b = Decimal128.fromDouble(7.89, 2);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        Decimal128 result = new Decimal128();
        Decimal128.divide(a, b, result, tgtScale, RoundingMode.HALF_UP);
        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, result.toBigDecimal());
    }

    @Test(expected = NumericException.class)
    public void testDivideInvalidScale() {
        Decimal128 a = new Decimal128(0, 1, 0);
        Decimal128 b = new Decimal128(0, 1, 30);
        a.divide(b, 30, RoundingMode.HALF_UP);
    }

    @Test
    public void testDivideLarge() {
        Decimal128 a = Decimal128.fromDouble(987654321.123456789, 9);
        Decimal128 b = Decimal128.fromDouble(123.456, 3);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        a.divide(b, tgtScale, RoundingMode.HALF_UP);

        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, a.toBigDecimal());

    }

    @Test
    public void testDivideLargeScale() {
        Decimal128 a = Decimal128.fromDouble(3.141592653589793, 15);
        Decimal128 b = Decimal128.fromDouble(2.718281828459045, 15);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 6;
        a.divide(b, tgtScale, RoundingMode.HALF_UP);

        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, a.toBigDecimal());
    }

    @Test
    public void testDivideOverflow() {
        Decimal128 a = Decimal128.fromDouble(-328049473, 0);
        Decimal128 b = Decimal128.fromDouble(-50582053256.05, 2);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        Decimal128 result = new Decimal128();
        Decimal128.divide(a, b, result, tgtScale, RoundingMode.HALF_UP);

        Assert.assertEquals(result.toBigDecimal(), bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP));
    }

    @Test
    public void testDivisionByOne() {
        Decimal128 dividend = Decimal128.fromLong(123456L, 3);
        Decimal128 divisor = Decimal128.fromLong(1L, 0);
        dividend.divide(divisor, 3, RoundingMode.HALF_UP);
        Assert.assertEquals("123.456", dividend.toString());
    }

    @Test(expected = NumericException.class)
    public void testDivisionByZero() {
        Decimal128 a = Decimal128.fromDouble(100.0, 2);
        Decimal128 zero = Decimal128.fromDouble(0.0, 2);

        a.divide(zero, 2, RoundingMode.HALF_UP);
    }

    @Test
    public void testDivisionCombinatorics() {
        RoundingMode[] modes = new RoundingMode[]{RoundingMode.HALF_UP, RoundingMode.HALF_DOWN, RoundingMode.HALF_EVEN, RoundingMode.UNNECESSARY, RoundingMode.FLOOR, RoundingMode.CEILING, RoundingMode.DOWN, RoundingMode.UP};
        BigDecimal[] dividends = new BigDecimal[]{new BigDecimal("-3.45"), new BigDecimal("0.000987654321"), new BigDecimal("1"), new BigDecimal("10"), new BigDecimal("123.456"), new BigDecimal("123456.789101112"), new BigDecimal("1234567891011121314"),};
        BigDecimal[] divisors = new BigDecimal[]{new BigDecimal("-123.456"), new BigDecimal("0"), new BigDecimal("1"), new BigDecimal("2"), new BigDecimal("123.456"), new BigDecimal("123456.789101112"), new BigDecimal("12345678910111213141516171819202"),};
        int[] scales = new int[]{0, 2, 6, 16};

        for (RoundingMode roundingMode : modes) {
            for (BigDecimal divisor : divisors) {
                for (BigDecimal dividend : dividends) {
                    for (int scale : scales) {
                        Decimal128 a = Decimal128.fromBigDecimal(dividend);
                        Decimal128 b = Decimal128.fromBigDecimal(divisor);
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
        Decimal128 dividend = new Decimal128(0, -2, 0);
        BigDecimal bdDividend = dividend.toBigDecimal();
        Decimal128 divisor = new Decimal128(0, 0xFFFFFFFFL, 0);
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
            Decimal128 a = rnd.nextDecimal128();
            Decimal128 b = rnd.nextDecimal128();

            // Test division accuracy (avoid division by zero)
            if (!b.isZero()) {
                testDivisionAccuracy(a, b, i);
            }
        }
    }

    @Test(expected = NumericException.class)
    public void testDivisionRemainderUnnecessary() {
        Decimal128 dividend = Decimal128.fromLong(10L, 0);
        Decimal128 divisor = Decimal128.fromLong(3L, 0);
        dividend.divide(divisor, 0, RoundingMode.UNNECESSARY);
    }

    @Test
    public void testDivisionScaled() {
        Decimal128 dividend = Decimal128.fromDouble(10, 0);
        Decimal128 divisor = Decimal128.fromDouble(3, 0);

        dividend.divide(divisor, 6, RoundingMode.HALF_UP);
        Assert.assertEquals(6, dividend.getScale());
        Assert.assertEquals(0, dividend.getHigh());
        Assert.assertEquals(3333333, dividend.getLow());
    }

    @Test
    public void testEnd() {
        BigDecimal a = new BigDecimal("30450464.57299");
        BigDecimal b = new BigDecimal("-0.0000000006363875680102");
        BigDecimal expected = a.divide(b, 6, RoundingMode.HALF_UP);

        Decimal128 da = Decimal128.fromBigDecimal(a);
        Decimal128 db = Decimal128.fromBigDecimal(b);

        da.divide(db, 6, RoundingMode.HALF_UP);

        Assert.assertEquals(expected, da.toBigDecimal());
    }

    @Test
    public void testEquals() {
        Decimal128 a = new Decimal128(123, 456, 2);
        Decimal128 b = new Decimal128(123, 456, 2);
        Decimal128 c = new Decimal128(123, 457, 2);

        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testFromBigDecimal() {
        BigDecimal bd = new BigDecimal("1e37");
        Decimal128 decimal = Decimal128.fromBigDecimal(bd);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("10000000000000000000000000000000000000", sink.toString());
    }

    @Test
    public void testFromDouble() {
        Decimal128 decimal = Decimal128.fromDouble(123.45, 2);

        Assert.assertEquals(12345, decimal.getLow());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.001);
    }

    @Test
    public void testFromLong() {
        Decimal128 decimal = Decimal128.fromLong(12345, 2);

        Assert.assertEquals(0, decimal.getHigh());
        Assert.assertEquals(12345, decimal.getLow());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.001);
    }

    @Test
    public void testGetDigitAtPowerOfTenBoundaryValues() {
        Assert.assertEquals(9, Decimal128.getDigitAtPowerOfTen(0, 9, 0));
        Assert.assertEquals(9, Decimal128.getDigitAtPowerOfTen(0, 99, 1));
        Assert.assertEquals(9, Decimal128.getDigitAtPowerOfTen(0, 999, 2));
        Assert.assertEquals(9, Decimal128.getDigitAtPowerOfTen(0, 9999, 3));
    }

    @Test
    public void testGetDigitAtPowerOfTenHundredsPlace() {
        Assert.assertEquals(5, Decimal128.getDigitAtPowerOfTen(0, 523, 2));
        Assert.assertEquals(9, Decimal128.getDigitAtPowerOfTen(0, 999, 2));
        Assert.assertEquals(1, Decimal128.getDigitAtPowerOfTen(0, 100, 2));
        Assert.assertEquals(0, Decimal128.getDigitAtPowerOfTen(0, 99, 2));
    }

    @Test
    public void testGetDigitAtPowerOfTenSingleDigits() {
        for (int i = 0; i <= 9; i++) {
            Assert.assertEquals(i, Decimal128.getDigitAtPowerOfTen(0, i, 0));
        }
    }

    @Test
    public void testGetDigitAtPowerOfTenTensPlace() {
        Assert.assertEquals(9, Decimal128.getDigitAtPowerOfTen(0, 95, 1));
        Assert.assertEquals(5, Decimal128.getDigitAtPowerOfTen(0, 50, 1));
        Assert.assertEquals(1, Decimal128.getDigitAtPowerOfTen(0, 19, 1));
        Assert.assertEquals(0, Decimal128.getDigitAtPowerOfTen(0, 9, 1));
    }

    @Test
    public void testGetDigitAtPowerOfTenZero() {
        Assert.assertEquals(0, Decimal128.getDigitAtPowerOfTen(0, 0, 0));
        Assert.assertEquals(0, Decimal128.getDigitAtPowerOfTen(0, 0, 5));
        Assert.assertEquals(0, Decimal128.getDigitAtPowerOfTen(0, 0, 10));
    }

    @Test
    public void testHashCode() {
        Decimal128 a = new Decimal128(123, 456, 2);
        Decimal128 b = new Decimal128(123, 456, 2);

        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testInPlaceAddition() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(67.89, 2);

        a.add(b);

        Assert.assertEquals(191.34, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testInPlaceDivision() {
        Decimal128 a = Decimal128.fromDouble(100.0, 2);
        Decimal128 b = Decimal128.fromDouble(4.0, 1);

        a.divide(b, 2, RoundingMode.HALF_UP);

        Assert.assertEquals(25.0, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testInPlaceModulo() {
        Decimal128 a = Decimal128.fromDouble(100.0, 2);
        Decimal128 b = Decimal128.fromDouble(30.0, 2);

        a.modulo(b);

        Assert.assertEquals(10.0, a.toDouble(), 0.01);
    }

    @Test
    public void testInPlaceMultiplication() {
        Decimal128 a = Decimal128.fromDouble(12.34, 2);
        Decimal128 b = Decimal128.fromDouble(5.6, 1);

        a.multiply(b);

        Assert.assertEquals(69.104, a.toDouble(), 0.001);
        Assert.assertEquals(3, a.getScale()); // 2 + 1 = 3
    }

    @Test
    public void testInPlaceSubtraction() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(67.89, 2);

        a.subtract(b);

        Assert.assertEquals(55.56, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testInvalidScale() {
        new Decimal128(0, 0, 100);
    }

    @Test
    public void testIsNegative() {
        Decimal128 positive = new Decimal128(0, 100, 2);
        Decimal128 negative = new Decimal128(-1, 0, 2);

        Assert.assertFalse(positive.isNegative());
        Assert.assertTrue(negative.isNegative());
    }

    @Test
    public void testIsZero() {
        Decimal128 zero = new Decimal128(0, 0, 2);
        Decimal128 nonZero = new Decimal128(0, 1, 2);

        Assert.assertTrue(zero.isZero());
        Assert.assertFalse(nonZero.isZero());
    }

    @Test
    public void testLargeNumbers() {
        // Test with large numbers
        Decimal128 a = new Decimal128(0x0L, 0xFFFFFFFFFFFFFFFFL, 0); // Large positive
        Decimal128 b = Decimal128.fromLong(2, 0);

        a.multiply(b);

        // The result should be 2 * (2^64 - 1)
        Assert.assertEquals(0x1L, a.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFEL, a.getLow());
    }

    @Test(expected = NumericException.class)
    public void testModuloByZero() {
        Decimal128 a = Decimal128.fromDouble(100.0, 2);
        Decimal128 zero = Decimal128.fromDouble(0.0, 2);

        a.modulo(zero);
    }

    @Test
    public void testModuloFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal128 a = rnd.nextDecimal128();
            Decimal128 b = rnd.nextDecimal128();

            if (!b.isZero()) {
                // Test modulo accuracy
                testModuloAccuracy(a, b, i);
            }
        }
    }

    @Test
    public void testModuloNegative() {
        // Test -10 % 3 = -1
        Decimal128 a = Decimal128.fromDouble(-10.0, 0);
        Decimal128 b = Decimal128.fromDouble(3.0, 0);

        a.modulo(b);

        Assert.assertEquals(-1.0, a.toDouble(), 0.001);
    }

    @Test
    public void testModuloNegativeScale() {
        // Test -10 % 3 = -1
        BigDecimal bdA = new BigDecimal("-0.0000000000000000001364898122");
        BigDecimal bdB = new BigDecimal("0.000000000018446744073709550324");
        Decimal128 a = Decimal128.fromBigDecimal(bdA);
        Decimal128 b = Decimal128.fromBigDecimal(bdB);

        a.modulo(b);

        Assert.assertEquals("-0.000000000000000000136489812200", a.toString());
    }

    @Test
    public void testModuloSimple() {
        // Test 10 % 3 = 1
        Decimal128 a = Decimal128.fromDouble(10.0, 0);
        Decimal128 b = Decimal128.fromDouble(3.0, 0);

        a.modulo(b);

        Assert.assertEquals(1.0, a.toDouble(), 0.001);
    }

    @Test
    public void testModuloWithDecimals() {
        // Test 10.5 % 3.2 = 0.9
        Decimal128 a = Decimal128.fromDouble(10.5, 1);
        Decimal128 b = Decimal128.fromDouble(3.2, 1);

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
            Decimal128 a = rnd.nextDecimal128();
            Decimal128 b = rnd.nextDecimal128();

            // Test multiplication accuracy
            testMultiplicationAccuracy(a, b, i);
        }
    }

    @Test
    public void testMultiply() {
        Decimal128 a = new Decimal128(0, 100, 2);
        Decimal128 b = new Decimal128(0, 2, 0);
        a.multiply(b);
        Assert.assertEquals(200, a.getLow());
        Assert.assertEquals(2, a.getScale());

        a = new Decimal128(0, 100, 2);
        b = new Decimal128(2, 0, 0);
        a.multiply(b);
        Assert.assertEquals(0, a.getLow());
        Assert.assertEquals(200, a.getHigh());
        Assert.assertEquals(2, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow128() {
        Decimal128 m = new Decimal128();
        m.copyFrom(Decimal128.MAX_VALUE);
        m.multiply(new Decimal128(2, 0, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow128B() {
        Decimal128 m = new Decimal128(Long.MAX_VALUE, 0, 0);
        m.multiply(new Decimal128(0, 2, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow64() {
        Decimal128 m = new Decimal128(Long.MAX_VALUE, 0, 0);
        m.multiply(new Decimal128(0, Long.MAX_VALUE, 0));
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflow64B() {
        Decimal128 m = new Decimal128(Long.MAX_VALUE / 2, 0, 0);
        m.multiply(new Decimal128(0, 3, 0));
    }

    @Test
    public void testNegateNull() {
        Decimal128 a = new Decimal128();
        a.ofNull();
        a.negate();
        Assert.assertTrue(a.isNull());
    }

    @Test
    public void testNegativeArithmetic() {
        // Test with negative numbers
        Decimal128 a = Decimal128.fromDouble(-12.5, 1);
        Decimal128 b = Decimal128.fromDouble(4.0, 1);

        a.multiply(b);

        Assert.assertEquals(-50.0, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());  // Scale should be 1 + 1 = 2

        // Test both negative
        Decimal128 c = Decimal128.fromDouble(-3.0, 1);
        Decimal128 d = Decimal128.fromDouble(-7.0, 1);

        c.multiply(d);
        Assert.assertEquals(21.0, c.toDouble(), 0.01);
    }

    @Test(expected = NumericException.class)
    public void testNullBigDecimal() {
        Decimal128.fromBigDecimal(null);
    }

    @Test
    public void testNumericExceptionDivisionByZero() {
        BigDecimal a = new BigDecimal("1e30");
        BigDecimal b = new BigDecimal("0");
        try {
            Decimal128 da = Decimal128.fromBigDecimal(a);
            Decimal128 db = Decimal128.fromBigDecimal(b);
            da.divide(db, 0, RoundingMode.HALF_UP);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            Assert.assertTrue(e.getMessage().contains("Division by zero"));
        }
    }

    @Test
    public void testNumericExceptionOverflow() {
        BigDecimal a = new BigDecimal("1e30");
        try {
            Decimal128 da = Decimal128.fromBigDecimal(a);
            Decimal128 db = Decimal128.fromBigDecimal(a);
            da.multiply(db);
            Assert.fail("Expected NumericException");
        } catch (NumericException e) {
            Assert.assertTrue(e.getMessage().contains("Overflow"));
        }
    }

    @Test
    public void testOfStringDecimalPointAtEnd() throws NumericException {
        Decimal128 d = new Decimal128();
        int precision = Numbers.decodeLowInt(d.ofString("123."));
        Assert.assertEquals(3, precision);
        Assert.assertEquals(0, d.getScale());
        Assert.assertEquals("123", d.toString());
    }

    @Test
    public void testOfStringDecimalPointAtStart() throws NumericException {
        Decimal128 d = new Decimal128();
        int precision = Numbers.decodeLowInt(d.ofString(".123"));
        Assert.assertEquals(4, precision);
        Assert.assertEquals(3, d.getScale());
        Assert.assertEquals("0.123", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOfStringEmptyString() throws NumericException {
        Decimal128 d = new Decimal128();
        d.ofString("");
    }

    @Test(expected = NumericException.class)
    public void testOfStringExceedsMaxPrecision() throws NumericException {
        Decimal128 d = new Decimal128();
        String tooLarge = new String(new char[39]).replace('\0', '9'); // 39 digits
        d.ofString(tooLarge);
    }

    @Test(expected = NumericException.class)
    public void testOfStringExceedsMaxScale() throws NumericException {
        Decimal128 d = new Decimal128();
        String tooManyDecimals = "1." + new String(new char[39]).replace('\0', '1'); // 39 decimal places
        d.ofString(tooManyDecimals);
    }

    @Test
    public void testOfStringExponentWithIntegerBase() throws NumericException {
        Decimal128 d = new Decimal128();

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
    public void testOfStringExponentWithNegativeNumber() throws NumericException {
        Decimal128 d = new Decimal128();

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
    public void testOfStringInfinity() throws NumericException {
        Decimal128 d = new Decimal128();
        d.ofString("Infinity");
        Assert.assertTrue(d.isNull());
    }

    @Test(expected = NumericException.class)
    public void testOfStringInvalidCharacter() throws NumericException {
        Decimal128 d = new Decimal128();
        d.ofString("123a45");
    }

    @Test
    public void testOfStringNaN() throws NumericException {
        Decimal128 d = new Decimal128();
        d.ofString("NaN");
        Assert.assertTrue(d.isNull());
    }

    @Test
    public void testOfStringSimpleNegative() throws NumericException {
        Decimal128 d = new Decimal128();
        int precision = Numbers.decodeLowInt(d.ofString("-123.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("-123.45", d.toString());
    }

    @Test
    public void testOfStringSimplePositive() throws NumericException {
        Decimal128 d = new Decimal128();
        int precision = Numbers.decodeLowInt(d.ofString("103.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("103.45", d.toString());
    }

    @Test
    public void testOfStringWithLeadingZeros() throws NumericException {
        Decimal128 d = new Decimal128();
        int precision = Numbers.decodeLowInt(d.ofString("00123.45"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test
    public void testOfStringWithMSuffix() throws NumericException {
        Decimal128 d = new Decimal128();
        int precision = Numbers.decodeLowInt(d.ofString("123.45m"));
        Assert.assertEquals(5, precision);
        Assert.assertEquals(2, d.getScale());
        Assert.assertEquals("123.45", d.toString());
    }

    @Test(expected = NumericException.class)
    public void testOverflowCtor() {
        new Decimal128(Long.MAX_VALUE, -1L, 0);
    }

    @Test
    public void testPowersTenTable() {
        BigDecimal bd = new BigDecimal(1);
        long[][] table = new long[Decimal128.MAX_PRECISION][9 * 4];
        for (int i = 0; i < Decimal128.MAX_PRECISION; i++) {
            BigDecimal base = bd;
            for (int j = 0; j < 9; j++) {
                Decimal128 d = Decimal128.fromBigDecimal(bd);
                table[i][j * 2] = d.getHigh();
                table[i][j * 2 + 1] = d.getLow();
                bd = bd.add(base);
            }
        }

        printPowerTable(table);

        long[][] currentTable = Decimal128.getPowersTenTable();
        for (int i = 0; i < Decimal128.MAX_PRECISION; i++) {
            for (int j = 0; j < 9 * 2; j++) {
                Assert.assertEquals(table[i][j], currentTable[i][j]);
            }
        }
    }

    @Test
    public void testRescale128() {
        Decimal128 a = Decimal128.fromLong(1, 0);
        a.rescale(30);
        Assert.assertEquals("1.000000000000000000000000000000", a.toString());
    }

    @Test
    public void testRescale64() {
        Decimal128 a = Decimal128.fromLong(1, 0);
        a.rescale(10);
        Assert.assertEquals("1.0000000000", a.toString());
    }

    @Test(expected = NumericException.class)
    public void testRescaleLess() {
        Decimal128 a = new Decimal128(0, 100, 10);
        a.rescale(5);
    }

    @Test
    public void testRescaleNegative() {
        Decimal128 a = Decimal128.fromLong(-10, 0);
        a.rescale(2);
        Assert.assertEquals(-1000, a.getLow());
        Assert.assertEquals(2, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testRescaleOverflowHigh() {
        Decimal128 a = new Decimal128(100, 0, 0);
        a.rescale(20);
    }

    @Test(expected = NumericException.class)
    public void testRescaleOverflowLow() {
        Decimal128 a = new Decimal128(0, 100, 0);
        a.rescale(38);
    }

    @Test
    public void testRound() {
        // Test basic rounding from scale 3 to scale 2
        Decimal128 a = Decimal128.fromDouble(1.234, 3);
        a.round(2, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(1.23);
        Assert.assertEquals("Basic rounding failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 2", 2, a.getScale());
    }

    @Test
    public void testRoundAllModes() {
        double testValue = 1.235;
        int originalScale = 3;
        int targetScale = 2;

        // Test all rounding modes
        java.math.RoundingMode[] modes = {java.math.RoundingMode.UP, java.math.RoundingMode.DOWN, java.math.RoundingMode.CEILING, java.math.RoundingMode.FLOOR, java.math.RoundingMode.HALF_UP, java.math.RoundingMode.HALF_DOWN, java.math.RoundingMode.HALF_EVEN};

        for (java.math.RoundingMode mode : modes) {
            Decimal128 a = Decimal128.fromDouble(testValue, originalScale);
            a.round(targetScale, mode);

            // Compare with BigDecimal reference
            java.math.BigDecimal reference = java.math.BigDecimal.valueOf(testValue).setScale(originalScale, java.math.RoundingMode.HALF_UP).setScale(targetScale, mode);

            Assert.assertEquals("Rounding mode " + mode + " failed for " + testValue, reference, a.toBigDecimal());
            Assert.assertEquals("Scale should be " + targetScale, targetScale, a.getScale());
        }
    }

    @Test
    public void testRoundFuzz() {
        // Fuzz test for round() method using BigDecimal as oracle
        final int iterations = 10_000;  // Reduced for debugging
        final Rnd rnd = TestUtils.generateRandom(null);

        // All rounding modes except UNNECESSARY (which requires special handling)
        java.math.RoundingMode[] roundingModes = {java.math.RoundingMode.UP, java.math.RoundingMode.DOWN, java.math.RoundingMode.CEILING, java.math.RoundingMode.FLOOR, java.math.RoundingMode.HALF_UP, java.math.RoundingMode.HALF_DOWN, java.math.RoundingMode.HALF_EVEN};

        Decimal128 decimal = new Decimal128();
        for (int i = 0; i < iterations; i++) {
            // Generate random decimal with varying characteristics
            rnd.nextDecimal128(decimal);

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
            Decimal128 testDecimal = new Decimal128();
            testDecimal.copyFrom(decimal);

            // Get the original BigDecimal representation
            java.math.BigDecimal originalBigDecimal;
            try {
                originalBigDecimal = decimal.toBigDecimal();
            } catch (NumberFormatException e) {
                String errorMsg = String.format("Failed to convert original Decimal128 to BigDecimal at iteration %d:\n" + "Decimal128: high=0x%016x, low=0x%016x, scale=%d\n" + "toString()=%s\n" + "Error: %s", i, decimal.getHigh(), decimal.getLow(), decimal.getScale(), decimal, e.getMessage());
                Assert.fail(errorMsg);
                return; // unreachable but makes compiler happy
            }

            try {
                // Apply rounding to our Decimal128
                testDecimal.round(targetScale, roundingMode);

                // Apply same rounding to BigDecimal as oracle
                java.math.BigDecimal expectedBigDecimal = originalBigDecimal.setScale(targetScale, roundingMode);

                // Compare results
                java.math.BigDecimal actualBigDecimal;
                try {
                    actualBigDecimal = testDecimal.toBigDecimal();
                } catch (NumberFormatException e) {
                    String errorMsg = String.format("Failed to convert result Decimal128 to BigDecimal at iteration %d:\n" + "Original: %s (scale=%d)\n" + "Target scale: %d, Mode: %s\n" + "Result Decimal128: high=0x%016x, low=0x%016x, scale=%d\n" + "toString()=%s\n" + "Error: %s", i, originalBigDecimal.toPlainString(), decimal.getScale(), targetScale, roundingMode, testDecimal.getHigh(), testDecimal.getLow(), testDecimal.getScale(), testDecimal, e.getMessage());
                    Assert.fail(errorMsg);
                    return; // unreachable but makes compiler happy
                }

                if (!expectedBigDecimal.equals(actualBigDecimal)) {
                    String errorMsg = String.format("Rounding mismatch at iteration %d:\n" + "Original: %s (scale=%d)\n" + "Target scale: %d, Mode: %s\n" + "Expected: %s\n" + "Actual: %s\n" + "Original Decimal128: high=0x%016x, low=0x%016x, scale=%d", i, originalBigDecimal.toPlainString(), decimal.getScale(), targetScale, roundingMode, expectedBigDecimal.toPlainString(), actualBigDecimal.toPlainString(), decimal.getHigh(), decimal.getLow(), decimal.getScale());
                    Assert.fail(errorMsg);
                }

                // Verify the scale is set correctly
                Assert.assertEquals("Scale should match target scale", targetScale, testDecimal.getScale());

            } catch (NumericException e) {
                // BigDecimal might throw NumericException in some cases
                // In such cases, our implementation should either handle it gracefully
                // or throw the same exception
                boolean decimal128Threw = false;
                try {
                    testDecimal.round(targetScale, roundingMode);
                } catch (NumericException e2) {
                    decimal128Threw = true;
                }

                if (!decimal128Threw) {
                    String errorMsg = String.format("BigDecimal threw NumericException but Decimal128 didn't at iteration %d:\n" + "Original: %s (scale=%d)\n" + "Target scale: %d, Mode: %s\n" + "BigDecimal error: %s", i, originalBigDecimal.toPlainString(), decimal.getScale(), targetScale, roundingMode, e.getMessage());
                    Assert.fail(errorMsg);
                }
            }
        }
    }

    @Test
    public void testRoundFuzzWithUnnecessaryMode() {
        // Separate fuzz test for UNNECESSARY mode which has special semantics
        final int iterations = 1000;
        final Rnd rnd = TestUtils.generateRandom(null);
        final Decimal128 decimal = new Decimal128();

        for (int i = 0; i < iterations; i++) {
            // Generate random decimal
            rnd.nextDecimal128(decimal);

            // For UNNECESSARY mode, we need to ensure no rounding is actually needed
            // So we'll create a decimal that already has the target scale
            int currentScale = decimal.getScale();

            // Test with same scale (no rounding needed) - should be no-op
            Decimal128 testDecimal = new Decimal128();
            testDecimal.copyFrom(decimal);

            java.math.BigDecimal originalBigDecimal = decimal.toBigDecimal();

            // Apply UNNECESSARY rounding with same scale
            testDecimal.round(currentScale, java.math.RoundingMode.UNNECESSARY);

            // Should be unchanged
            java.math.BigDecimal resultBigDecimal = testDecimal.toBigDecimal();

            if (!originalBigDecimal.equals(resultBigDecimal)) {
                String errorMsg = String.format("UNNECESSARY mode changed value when no rounding needed at iteration %d:\n" + "Original: %s (scale=%d)\n" + "Result: %s (scale=%d)", i, originalBigDecimal.toPlainString(), currentScale, resultBigDecimal.toPlainString(), testDecimal.getScale());
                Assert.fail(errorMsg);
            }

            // Verify scale unchanged
            Assert.assertEquals("Scale should remain unchanged with UNNECESSARY mode", currentScale, testDecimal.getScale());
        }
    }

    @Test
    public void testRoundHalfEvenTieBreaking() {
        // Test HALF_EVEN tie-breaking specifically
        double[] testValues = {1.125, 1.135, 2.125, 2.135}; // Tie cases
        java.math.BigDecimal[] expectedResults = {java.math.BigDecimal.valueOf(1.12), // Round to even (2)
                java.math.BigDecimal.valueOf(1.14), // Round to even (4)
                java.math.BigDecimal.valueOf(2.12), // Round to even (2)
                java.math.BigDecimal.valueOf(2.14)  // Round to even (4)
        };

        for (int i = 0; i < testValues.length; i++) {
            Decimal128 a = Decimal128.fromDouble(testValues[i], 3);
            a.round(2, java.math.RoundingMode.HALF_EVEN);

            Assert.assertEquals("HALF_EVEN tie-breaking failed for " + testValues[i], expectedResults[i], a.toBigDecimal());
        }
    }

    @Test
    public void testRoundNegative() {
        Decimal128 a = new Decimal128(-1, -123, 0);
        a.round(1, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals("-123.0", a.toString());
    }

    @Test
    public void testRoundNegativeNumbers() {
        double testValue = -1.235;
        int originalScale = 3;
        int targetScale = 2;

        java.math.RoundingMode[] modes = {java.math.RoundingMode.UP, java.math.RoundingMode.DOWN, java.math.RoundingMode.CEILING, java.math.RoundingMode.FLOOR, java.math.RoundingMode.HALF_UP, java.math.RoundingMode.HALF_DOWN, java.math.RoundingMode.HALF_EVEN};

        for (java.math.RoundingMode mode : modes) {
            Decimal128 a = Decimal128.fromDouble(testValue, originalScale);
            a.round(targetScale, mode);

            // Compare with BigDecimal reference
            java.math.BigDecimal reference = java.math.BigDecimal.valueOf(testValue).setScale(originalScale, java.math.RoundingMode.HALF_UP).setScale(targetScale, mode);

            Assert.assertEquals("Rounding mode " + mode + " failed for negative " + testValue, reference, a.toBigDecimal());
            Assert.assertEquals("Scale should be " + targetScale, targetScale, a.getScale());
        }
    }

    @Test(expected = NumericException.class)
    public void testRoundNegativeScale() {
        Decimal128 a = Decimal128.fromDouble(1.23, 2);
        a.round(-1, java.math.RoundingMode.HALF_UP);
    }

    @Test
    public void testRoundNoChange() {
        // Test when target scale equals current scale (no-op)
        Decimal128 a = Decimal128.fromDouble(1.234, 3);
        Decimal128 original = new Decimal128();
        original.copyFrom(a);

        a.round(3, java.math.RoundingMode.HALF_UP);

        Assert.assertEquals("No-op rounding should not change value", original, a);
    }

    @Test
    public void testRoundNull() {
        Decimal128 d = new Decimal128();
        d.ofNull();
        d.round(2, RoundingMode.HALF_UP);
        Assert.assertTrue(d.isNull());
    }

    @Test
    public void testRoundScaleIncrease() {
        // Test increasing scale (should add trailing zeros)
        Decimal128 a = Decimal128.fromDouble(1.23, 2);
        a.round(4, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(1.23).setScale(4, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals("Scale increase failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 4", 4, a.getScale());
    }

    @Test
    public void testRoundZero() {
        // Test rounding zero
        Decimal128 a = Decimal128.fromDouble(0.0, 5);
        a.round(2, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.ZERO.setScale(2, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals("Rounding zero failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 2", 2, a.getScale());
    }

    @Test
    public void testScaleHandling() {
        // Test addition with different scales
        Decimal128 a = Decimal128.fromDouble(123.45, 2);  // Scale 2
        Decimal128 b = Decimal128.fromDouble(6.789, 3);   // Scale 3

        a.add(b);

        Assert.assertEquals(3, a.getScale());  // Should use larger scale
        Assert.assertEquals(130.239, a.toDouble(), 0.001);
    }

    @Test
    public void testSetScale() {
        Decimal128 a = new Decimal128(0, 12345, 2);
        a.setScale(1);
        Assert.assertEquals("1234.5", a.toString());
    }

    @Test
    public void testSinkNull() {
        // Sinking a null value shouldn't print anything
        StringSink sink = new StringSink();
        Decimal128.NULL_VALUE.toSink(sink);
        Assert.assertEquals("", sink.toString());
    }

    @Test
    public void testSinkZero() {
        StringSink sink = new StringSink();
        Decimal128.ZERO.toSink(sink);
        Assert.assertEquals("0", sink.toString());
    }

    @Test
    public void testSinkableInterface() {
        // Test that Decimal128 can be used as a Sinkable
        Decimal128 decimal = Decimal128.fromDouble(42.99, 2);
        StringSink sink = new StringSink();

        // Use the put(Sinkable) method from CharSink
        sink.put(decimal);

        Assert.assertEquals("42.99", sink.toString());
    }

    @Test
    public void testStaticAdd() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(67.89, 2);
        Decimal128 result = new Decimal128();

        Decimal128.add(a, b, result);

        Assert.assertEquals(191.34, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(123.45, a.toDouble(), 0.01);
        Assert.assertEquals(67.89, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticDivide() {
        Decimal128 a = Decimal128.fromDouble(100.0, 2);
        Decimal128 b = Decimal128.fromDouble(4.0, 1);
        Decimal128 result = new Decimal128();

        Decimal128.divide(a, b, result, 2, RoundingMode.HALF_UP);

        Assert.assertEquals(25.0, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(100.0, a.toDouble(), 0.01);
        Assert.assertEquals(4.0, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticMethodsWithDifferentScales() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);  // Scale 2
        Decimal128 b = Decimal128.fromDouble(6.789, 3);   // Scale 3
        Decimal128 result = new Decimal128();

        Decimal128.add(a, b, result);

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
        Decimal128 a = Decimal128.fromDouble(100.0, 2);
        Decimal128 b = Decimal128.fromDouble(30.0, 2);
        Decimal128 result = new Decimal128();

        Decimal128.modulo(a, b, result);

        Assert.assertEquals(10.0, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(100.0, a.toDouble(), 0.01);
        Assert.assertEquals(30.0, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticMultiply() {
        Decimal128 a = Decimal128.fromDouble(12.34, 2);
        Decimal128 b = Decimal128.fromDouble(5.6, 1);
        Decimal128 result = new Decimal128();

        Decimal128.multiply(a, b, result);

        Assert.assertEquals(69.104, result.toDouble(), 0.001);
        Assert.assertEquals(3, result.getScale()); // 2 + 1 = 3

        // Verify operands are unchanged
        Assert.assertEquals(12.34, a.toDouble(), 0.01);
        Assert.assertEquals(5.6, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticNegate() {
        Decimal128 result = new Decimal128();

        // Test positive number
        Decimal128 a = Decimal128.fromDouble(42.5, 1);
        Decimal128.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Verify original is unchanged - compare with fresh instance
        Decimal128 original = Decimal128.fromDouble(42.5, 1);
        Assert.assertEquals(original.toBigDecimal(), a.toBigDecimal());

        // Test negative number
        a = Decimal128.fromDouble(-123.456, 3);
        Decimal128.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Verify original is unchanged - compare with fresh instance
        original = Decimal128.fromDouble(-123.456, 3);
        Assert.assertEquals(original.toBigDecimal(), a.toBigDecimal());

        // Test zero
        a = Decimal128.fromDouble(0.0, 0);
        Decimal128.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Test very small number
        a = Decimal128.fromDouble(1e-10, 10);
        Decimal128.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Test very large number
        a = Decimal128.fromDouble(1e10, 0);
        Decimal128.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());
    }

    @Test
    public void testStaticNegateConsistentWithInPlace() {
        Decimal128 staticResult = new Decimal128();
        Decimal128 inPlaceResult = new Decimal128();

        // Test various values to ensure static and in-place methods are consistent
        double[] testValues = {0.0, 1.0, -1.0, 123.456, -789.123, 1e-5, -1e-5, 1e8, -1e8};

        for (double value : testValues) {
            Decimal128 a = Decimal128.fromDouble(value, 3);

            // Test static method
            Decimal128.negate(a, staticResult);

            // Test in-place method
            inPlaceResult.copyFrom(a);
            inPlaceResult.negate();

            // Results should be identical
            Assert.assertEquals("Static and in-place negate differ for value " + value, staticResult.toBigDecimal(), inPlaceResult.toBigDecimal());

            // Verify original is unchanged by static method - compare with fresh instance
            Decimal128 originalFresh = Decimal128.fromDouble(value, 3);
            Assert.assertEquals("Static negate modified original for value " + value, originalFresh.toBigDecimal(), a.toBigDecimal());
        }
    }

    @Test
    public void testStaticSubtract() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(67.89, 2);
        Decimal128 result = new Decimal128();

        Decimal128.subtract(a, b, result);

        Assert.assertEquals(55.56, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(123.45, a.toDouble(), 0.01);
        Assert.assertEquals(67.89, b.toDouble(), 0.01);
    }

    @Test
    public void testSubstractionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal128 a = rnd.nextDecimal128();
            Decimal128 b = rnd.nextDecimal128();

            // Test subtraction accuracy
            testSubtractionAccuracy(a, b, i);
        }
    }

    @Test
    public void testToBigDecimal() {
        // Test basic positive number
        Decimal128 a = Decimal128.fromDouble(123.456, 3);
        java.math.BigDecimal bigDecimal = a.toBigDecimal();
        Assert.assertEquals("123.456", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());

        // Test negative number
        a = Decimal128.fromDouble(-789.123, 3);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("-789.123", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());

        // Test zero
        a = Decimal128.fromDouble(0.0, 2);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("0.00", bigDecimal.toString());
        Assert.assertEquals(2, bigDecimal.scale());

        // Test integer (scale 0)
        a = Decimal128.fromDouble(42.0, 0);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("42", bigDecimal.toString());
        Assert.assertEquals(0, bigDecimal.scale());

        // Test very small number
        a = Decimal128.fromDouble(0.001, 3);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("0.001", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());
    }

    @Test
    public void testToBigDecimalConsistentWithToString() {
        // toBigDecimal should produce the same string representation as toString
        double[] testValues = {0.0, 1.0, -1.0, 123.456, -789.123, 0.001, -0.001};

        for (double value : testValues) {
            Decimal128 a = Decimal128.fromDouble(value, 3);
            String stringRep = a.toString();
            java.math.BigDecimal bigDecimal = a.toBigDecimal();

            Assert.assertEquals("toBigDecimal and toString should be consistent for " + value, stringRep, bigDecimal.toString());
        }
    }

    @Test
    public void testToBigDecimalPrecision() {
        // Test that toBigDecimal preserves precision better than toDouble
        Decimal128 a = Decimal128.fromDouble(123.456789, 6);
        java.math.BigDecimal bigDecimal = a.toBigDecimal();

        // BigDecimal should preserve the exact decimal representation
        Assert.assertTrue("BigDecimal should preserve precision", bigDecimal.toString().contains("123.456789"));

        // Convert back and forth should be consistent
        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(123.456789).setScale(6, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals(expected.doubleValue(), bigDecimal.doubleValue(), 1e-15);
    }

    @Test
    public void testToDecimal256Basic() {
        Decimal128 d128 = Decimal128.fromDouble(123.45, 2);
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(2, d256.getScale());
        Assert.assertEquals("123.45", d256.toString());
    }

    @Test
    public void testToDecimal256LargeNumber() {
        // Test with a large 128-bit number
        Decimal128 d128 = Decimal128.fromBigDecimal(new BigDecimal("12345678901234567890.123456"));
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(d128.getScale(), d256.getScale());
        Assert.assertEquals(d128.toString(), d256.toString());
    }

    @Test
    public void testToDecimal256MaxValue() {
        Decimal128 d128 = new Decimal128();
        d128.copyFrom(Decimal128.MAX_VALUE);
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(0, d256.getScale());
        Assert.assertEquals(d128.toString(), d256.toString());
    }

    @Test
    public void testToDecimal256MinValue() {
        Decimal128 d128 = new Decimal128();
        d128.copyFrom(Decimal128.MIN_VALUE);
        d128.add(Decimal128.fromLong(1, 0));  // Avoid NULL value
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(0, d256.getScale());
        Assert.assertEquals(d128.toString(), d256.toString());
    }

    @Test
    public void testToDecimal256NegativeValue() {
        Decimal128 d128 = Decimal128.fromDouble(-987.654321, 6);
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(6, d256.getScale());
        Assert.assertEquals("-987.654321", d256.toString());
    }

    @Test
    public void testToDecimal256Null() {
        Decimal128 d128 = new Decimal128();
        d128.ofNull();
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertTrue(d256.isNull());
    }

    @Test
    public void testToDecimal256WithHighScale() {
        Decimal128 d128 = Decimal128.fromDouble(1.23456789012345678, 17);
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(17, d256.getScale());
        Assert.assertEquals(d128.toString(), d256.toString());
    }

    @Test
    public void testToDecimal256Zero() {
        Decimal128 d128 = Decimal128.fromLong(0, 10);
        Decimal256 d256 = new Decimal256();
        d128.toDecimal256(d256);

        Assert.assertEquals(10, d256.getScale());
        Assert.assertEquals("0.0000000000", d256.toString());
    }

    @Test
    public void testToDouble() {
        Decimal128 decimal = Decimal128.fromLong(12345, 3);
        Assert.assertEquals(12.345, decimal.toDouble(), 0.0001);

        Decimal128 negative = new Decimal128(-1, -1, 2); // Two's complement representation
        Assert.assertTrue(negative.toDouble() < 0);
    }

    // ofString tests

    @Test
    public void testToSinkBasic() {
        Decimal128 decimal = Decimal128.fromLong(12345, 2);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("123.45", sink.toString());
    }

    @Test
    public void testToSinkComplex128Bit() {
        // Test complex 128-bit number conversion to decimal
        Decimal128 decimal = new Decimal128(0x123456789ABCDEFL, 0xFEDCBA9876543210L, 2);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        String result = sink.toString();
        // Should convert large 128-bit number to proper decimal representation
        Assert.assertEquals("15123660752041709473323553696831370.40", result);
    }

    @Test
    public void testToSinkLargeLong() {
        Decimal128 decimal = new Decimal128(0, -1, 0);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("18446744073709551615", sink.toString());
    }

    @Test
    public void testToSinkLargeLongLargeScale() {
        Decimal128 decimal = new Decimal128(0, -1, 25);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("0.0000018446744073709551615", sink.toString());
    }

    @Test
    public void testToSinkLargeLongScale() {
        Decimal128 decimal = new Decimal128(0, -1, 5);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("184467440737095.51615", sink.toString());
    }

    @Test
    public void testToSinkLargeNumber() {
        Decimal128 decimal = Decimal128.fromLong(9876543210L, 4);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("987654.3210", sink.toString());
    }

    @Test
    public void testToSinkMultipleDecimals() {
        // Test multiple decimals being written to the same sink
        StringSink sink = new StringSink();

        Decimal128 a = Decimal128.fromDouble(12.34, 2);
        Decimal128 b = Decimal128.fromDouble(56.78, 2);

        a.toSink(sink);
        sink.putAscii(" + ");
        b.toSink(sink);
        sink.putAscii(" = ");

        Decimal128 result = new Decimal128();
        Decimal128.add(a, b, result);
        result.toSink(sink);

        Assert.assertEquals("12.34 + 56.78 = 69.12", sink.toString());
    }

    @Test
    public void testToSinkNegativeNumber() {
        Decimal128 decimal = Decimal128.fromLong(-12345, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("-12.345", sink.toString());
    }

    @Test
    public void testToSinkSmallNumber() {
        Decimal128 decimal = Decimal128.fromLong(123, 5);  // 0.00123
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("0.00123", sink.toString());
    }

    @Test
    public void testToSinkVsToString() {
        // Test that toSink and toString produce the same result
        Decimal128 decimal = Decimal128.fromDouble(123.456, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);
        String sinkResult = sink.toString();
        String toStringResult = decimal.toString();

        Assert.assertEquals(toStringResult, sinkResult);
    }

    @Test
    public void testToSinkZero() {
        Decimal128 decimal = Decimal128.fromLong(0, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("0.000", sink.toString());
    }

    @Test
    public void testToSinkZeroScale() {
        Decimal128 decimal = Decimal128.fromLong(123, 0);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("123", sink.toString());
    }

    @Test
    public void testToString() {
        Decimal128 decimal = Decimal128.fromLong(12345, 2);
        String str = decimal.toString();

        Assert.assertTrue(str.contains("123"));
        Assert.assertTrue(str.contains("45"));
    }

    @Test
    public void testUncheckedAddDecimal64CarryIntoHigh() {
        Decimal128 result = new Decimal128(0, -2L, 0);

        Decimal128.uncheckedAdd(result, 3L);

        Assert.assertEquals(1L, result.getHigh());
        Assert.assertEquals(1L, result.getLow());
        Assert.assertEquals(0, result.toBigDecimal().compareTo(new BigDecimal("18446744073709551617")));
    }

    // toDecimal256 tests

    @Test
    public void testUncheckedAddDecimal64KeepsScale() {
        Decimal128 result = Decimal128.fromLong(150, 2);

        Decimal128.uncheckedAdd(result, 25);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals(0, result.toBigDecimal().compareTo(new BigDecimal("1.75")));
    }

    @Test
    public void testUncheckedAddDecimal64NegativeSignExtension() {
        Decimal128 result = Decimal128.fromLong(50, 2);

        Decimal128.uncheckedAdd(result, -125);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals(0, result.toBigDecimal().compareTo(new BigDecimal("-0.75")));
        Assert.assertEquals(-1L, result.getHigh());
    }

    @Test
    public void testZeroAllocationArithmetic() {
        // Demonstrate truly allocation-free arithmetic chain
        Decimal128 accumulator = new Decimal128();

        // Start with 100
        accumulator.ofLong(10000, 2); // 100.00 with scale 2

        // Add 50 -> 150
        Decimal128 increment = Decimal128.fromLong(5000, 2); // 50.00 with scale 2
        accumulator.add(increment);
        Assert.assertEquals(150.0, accumulator.toDouble(), 0.01);

        // Multiply by 2 -> 300
        Decimal128 multiplier = Decimal128.fromLong(2, 0);
        accumulator.multiply(multiplier);
        Assert.assertEquals(300.0, accumulator.toDouble(), 0.01);

        // Subtract 75 -> 225
        Decimal128 subtrahend = Decimal128.fromLong(7500, 2); // 75.00 with scale 2
        accumulator.subtract(subtrahend);
        Assert.assertEquals(225.0, accumulator.toDouble(), 0.01);

        // Divide by 5 -> 45
        Decimal128 divisor = Decimal128.fromLong(5, 0);
        accumulator.divide(divisor, 2, RoundingMode.HALF_UP);
        Assert.assertEquals(45.0, accumulator.toDouble(), 0.01);

        // Modulo 10 -> 5
        Decimal128 mod = Decimal128.fromLong(1000, 2); // 10.00 with scale 2
        accumulator.modulo(mod);
        Assert.assertEquals(5.0, accumulator.toDouble(), 0.01);
    }

    private void printPowerTable(long[][] table) {
        System.err.println("    private static final long[][] POWERS_TEN_TABLE = new long[][]{");
        for (int i = 0, n = table.length; i < n; i++) {
            System.err.print("            {");
            for (int j = 0; j < 9 * 2; j++) {
                System.err.printf("%dL", table[i][j]);
                if (j < (9 * 2 - 1)) {
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

    private void testAdditionAccuracy(Decimal128 a, Decimal128 b, int iteration) {
        // Test addition accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference addition
        BigDecimal expected = bigA.add(bigB);

        BigDecimal min = Decimal128.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal128.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected

            Decimal128 result = new Decimal128();

            Assert.assertThrows(NumericException.class, () -> Decimal128.add(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal128 staticResult = new Decimal128();

            // Test static add method
            Decimal128.add(a, b, staticResult);

            Decimal128 result = new Decimal128();
            result.copyFrom(a);

            // Test in-place add method
            result.add(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place addition differ at iteration " + iteration, result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();

            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testDivisionAccuracy(Decimal128 a, Decimal128 b, int iteration) {
        // Choose a reasonable result scale
        int resultScale = Math.min(a.getScale() + 2, 6); // Limit to avoid precision issues

        // Test division accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform division with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.divide(bigB, resultScale, RoundingMode.HALF_UP);

        // catch overflow exceptions
        try {
            Decimal128 staticResult = new Decimal128();

            // Test static divide method
            Decimal128.divide(a, b, staticResult, resultScale, RoundingMode.HALF_UP);

            Decimal128 result = new Decimal128();
            result.copyFrom(a);

            // Test in-place divide method
            result.divide(b, resultScale, RoundingMode.HALF_UP);

            // Results should be the same
            Assert.assertEquals("Static and in-place division differ at iteration " + iteration, result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();
            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testModuloAccuracy(Decimal128 a, Decimal128 b, int iteration) {
        // Test modulo accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform modulo with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.remainder(bigB);

        // catch overflow exceptions
        try {
            Decimal128 staticResult = new Decimal128();

            // Test static modulo method
            Decimal128.modulo(a, b, staticResult);

            Decimal128 result = new Decimal128();
            result.copyFrom(a);

            // Test in-place modulo method
            result.modulo(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place modulo differ at iteration " + iteration, result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();
            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testMultiplicationAccuracy(Decimal128 a, Decimal128 b, int iteration) {
        // Test multiplication accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform multiplication with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.multiply(bigB);

        BigDecimal min = Decimal128.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal128.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected

            Decimal128 result = new Decimal128();

            Assert.assertThrows(NumericException.class, () -> Decimal128.multiply(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal128 staticResult = new Decimal128();

            // Test static multiply method
            Decimal128.multiply(a, b, staticResult);

            Decimal128 result = new Decimal128();
            result.copyFrom(a);

            // Test in-place multiply method
            result.multiply(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place multiplication differ at iteration " + iteration, result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();
            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testSubtractionAccuracy(Decimal128 a, Decimal128 b, int iteration) {
        // Test subtraction accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference subtraction
        BigDecimal expected = bigA.subtract(bigB);

        BigDecimal min = Decimal128.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal128.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected

            Decimal128 result = new Decimal128();

            Assert.assertThrows(NumericException.class, () -> Decimal128.subtract(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal128 staticResult = new Decimal128();

            // Test static subtract method
            Decimal128.subtract(a, b, staticResult);

            // Verify operands unchanged
            // Verify operands unchanged
            Assert.assertEquals("Subtract modified first operand at iteration " + iteration, bigA, a.toBigDecimal());
            Assert.assertEquals("Subtract modified second operand at iteration " + iteration, bigB, b.toBigDecimal());

            Decimal128 result = new Decimal128();
            result.copyFrom(a);

            // Test in-place subtract method
            result.subtract(b);

            // Results should be the same
            Assert.assertEquals("Static and in-place subtraction differ at iteration " + iteration, result.toBigDecimal(), staticResult.toBigDecimal());

            BigDecimal actual = result.toBigDecimal();

            if (expected.compareTo(actual) != 0) {
                BigDecimal difference = expected.subtract(actual).abs();
                Assert.fail("iteration: " + iteration + " expected:<" + expected + "> but was:<" + result + "> (difference: " + difference + ")");
            }
        } catch (NumericException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }
}
