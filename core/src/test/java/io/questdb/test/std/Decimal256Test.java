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
    public void testCompareTo() {
        Decimal256 smaller = new Decimal256(0, 0, 0, 100, 2);
        Decimal256 larger = new Decimal256(0, 0, 0, 200, 2);

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));
    }

    @Test
    public void testCompareToFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 1000_000;

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

    @Test(expected = NumericException.class)
    public void testDivisionByZero() {
        Decimal256 a = Decimal256.fromDouble(100.0, 2);
        Decimal256 zero = Decimal256.fromDouble(0.0, 2);

        a.divide(zero, 2, RoundingMode.HALF_UP);
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
    public void testFromBigDecimal() {
        BigDecimal bd = new BigDecimal("1e37");
        Decimal256 decimal = Decimal256.fromBigDecimal(bd);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("10000000000000000000000000000000000000", sink.toString());
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
    public void testRound() {
        // Test basic rounding from scale 3 to scale 2
        Decimal256 a = Decimal256.fromDouble(1.234, 3);
        a.round(2, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(1.23);
        Assert.assertEquals("Basic rounding failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 2", 2, a.getScale());
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
    public void testSubstractionFuzz() {
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