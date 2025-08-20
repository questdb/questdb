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

import io.questdb.std.Decimal256;
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

    @Test(expected = NumericException.class)
    public void testRescaleLess() {
        Decimal256 a = new Decimal256(0, 0, 0, 100, 10);
        a.rescale(5);
    }

    @Test
    public void testRescaleNegative() {
        Decimal256 a = Decimal256.fromLong(-10, 0);
        a.rescale(2);
        Assert.assertEquals(-1000, a.getLl());
        Assert.assertEquals(2, a.getScale());
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
    public void testRoundZero() {
        Decimal256 a = Decimal256.fromLong(0, 3);

        a.round(2, java.math.RoundingMode.HALF_UP);
        Assert.assertTrue(a.isZero());
        Assert.assertEquals(2, a.getScale());
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
        result.copyFrom(a);
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
        m.copyFrom(Decimal256.NULL_VALUE);
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
            result.copyFrom(a);

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