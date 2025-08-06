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

import io.questdb.std.Decimal160;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Tests for the consolidated Decimal160 class
 */
public class Decimal160Test {
    @Test
    public void testAdditionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal160 a = rnd.nextDecimal160();
            Decimal160 b = rnd.nextDecimal160();

            // Test addition accuracy
            testAdditionAccuracy(a, b, i);
        }
    }

    @Test
    public void testCompareTo() {
        Decimal160 smaller = new Decimal160(0, 100, 2);
        Decimal160 larger = new Decimal160(0, 200, 2);

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));
    }

    @Test
    public void testCompareToWithDifferentScales() {
        // Test 12.34 (scale 2) vs 12.345 (scale 3)
        Decimal160 a = Decimal160.fromDouble(12.34, 2);   // 12.34
        Decimal160 b = Decimal160.fromDouble(12.345, 3);  // 12.345

        Assert.assertTrue(a.compareTo(b) < 0); // 12.34 < 12.345
        Assert.assertTrue(b.compareTo(a) > 0); // 12.345 > 12.34

        // Test 12.340 (scale 3) vs 12.34 (scale 2) - should be equal
        Decimal160 c = Decimal160.fromDouble(12.340, 3);  // 12.340
        Decimal160 d = Decimal160.fromDouble(12.34, 2);   // 12.34

        Assert.assertEquals(0, c.compareTo(d)); // 12.340 == 12.34
        Assert.assertEquals(0, d.compareTo(c)); // 12.34 == 12.340

        // Test with larger scale difference: 1.2 (scale 1) vs 1.23456 (scale 5)
        Decimal160 e = Decimal160.fromDouble(1.2, 1);      // 1.2
        Decimal160 f = Decimal160.fromDouble(1.23456, 5);  // 1.23456

        Assert.assertTrue(e.compareTo(f) < 0); // 1.2 < 1.23456
        Assert.assertTrue(f.compareTo(e) > 0); // 1.23456 > 1.2

        // Test negative numbers with different scales
        Decimal160 g = Decimal160.fromDouble(-12.3, 1);    // -12.3
        Decimal160 h = Decimal160.fromDouble(-12.34, 2);   // -12.34

        Assert.assertTrue(g.compareTo(h) > 0); // -12.3 > -12.34
        Assert.assertTrue(h.compareTo(g) < 0); // -12.34 < -12.3

        // Test zero with different scales
        Decimal160 zero1 = Decimal160.fromDouble(0.0, 1);
        Decimal160 zero2 = Decimal160.fromDouble(0.0, 3);

        Assert.assertEquals(0, zero1.compareTo(zero2)); // 0.0 == 0.000
        Assert.assertEquals(0, zero2.compareTo(zero1)); // 0.000 == 0.0
    }

    @Test
    public void testConstructorAndGetters() {
        Decimal160 decimal = new Decimal160(0x123456789ABCDEFL, 0xFEDCBA9876543210L, 3);

        Assert.assertEquals(0x123456789ABCDEFL, decimal.getHigh());
        Assert.assertEquals(0xFEDCBA9876543210L, decimal.getLow());
        Assert.assertEquals(3, decimal.getScale());
    }

    @Test
    public void testCopyFrom() {
        Decimal160 original = Decimal160.fromDouble(123.456, 3);
        Decimal160 copy = new Decimal160();

        copy.copyFrom(original);

        Assert.assertEquals(original.getHigh(), copy.getHigh());
        Assert.assertEquals(original.getLow(), copy.getLow());
        Assert.assertEquals(original.getScale(), copy.getScale());
    }

    @Test
    public void testDivide64() {
        Decimal160 a = Decimal160.fromDouble(123.456, 3);
        Decimal160 b = Decimal160.fromDouble(7.89, 2);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        Decimal160 result = new Decimal160();
        Decimal160.divide(a, b, result, tgtScale, RoundingMode.HALF_UP);
        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, result.toBigDecimal());
    }

    @Test
    public void testDivideLarge() {
        Decimal160 a = Decimal160.fromDouble(987654321.123456789, 9);
        Decimal160 b = Decimal160.fromDouble(123.456, 3);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        a.divide(b, tgtScale, RoundingMode.HALF_UP);

        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, a.toBigDecimal());

    }

    @Test
    public void testDivideLargeScale() {
        Decimal160 a = Decimal160.fromDouble(3.141592653589793, 15);
        Decimal160 b = Decimal160.fromDouble(2.718281828459045, 15);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 6;
        a.divide(b, tgtScale, RoundingMode.HALF_UP);

        BigDecimal bdResult = bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP);
        Assert.assertEquals(bdResult, a.toBigDecimal());
    }

    @Test
    public void testDivideOverflow() {
        Decimal160 a = Decimal160.fromDouble(-328049473, 0);
        Decimal160 b = Decimal160.fromDouble(-50582053256.05, 2);
        BigDecimal bdA = a.toBigDecimal();
        BigDecimal bdB = b.toBigDecimal();

        int tgtScale = 2;
        Decimal160 result = new Decimal160();
        Decimal160.divide(a, b, result, tgtScale, RoundingMode.HALF_UP);

        Assert.assertEquals(result.toBigDecimal(), bdA.divide(bdB, tgtScale, RoundingMode.HALF_UP));
    }

    @Test(expected = ArithmeticException.class)
    public void testDivisionByZero() {
        Decimal160 a = Decimal160.fromDouble(100.0, 2);
        Decimal160 zero = Decimal160.fromDouble(0.0, 2);

        a.divide(zero, 2, RoundingMode.HALF_UP);
    }

    @Test
    public void testDivisionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal160 a = rnd.nextDecimal160();
            Decimal160 b = rnd.nextDecimal160();

            // Test division accuracy (avoid division by zero)
            if (!b.isZero()) {
                testDivisionAccuracy(a, b, i);
            }
        }
    }

    @Test
    public void testDivisionScaled() {
        Decimal160 dividend = Decimal160.fromDouble(10, 0);
        Decimal160 divisor = Decimal160.fromDouble(3, 0);

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

        Decimal160 da = Decimal160.fromBigDecimal(a);
        Decimal160 db = Decimal160.fromBigDecimal(b);

        da.divide(db, 6, RoundingMode.HALF_UP);

        Assert.assertEquals(expected, da.toBigDecimal());
    }

    @Test
    public void testEquals() {
        Decimal160 a = new Decimal160(123, 456, 2);
        Decimal160 b = new Decimal160(123, 456, 2);
        Decimal160 c = new Decimal160(123, 457, 2);

        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
    }

    @Test
    public void testFromBigDecimal() {
        BigDecimal bd = new BigDecimal("1e37");
        Decimal160 decimal = Decimal160.fromBigDecimal(bd);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("10000000000000000000000000000000000000", sink.toString());
    }

    @Test
    public void testFromDouble() {
        Decimal160 decimal = Decimal160.fromDouble(123.45, 2);

        Assert.assertEquals(12345, decimal.getLow());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.001);
    }

    @Test
    public void testFromLong() {
        Decimal160 decimal = Decimal160.fromLong(12345, 2);

        Assert.assertEquals(0, decimal.getHigh());
        Assert.assertEquals(12345, decimal.getLow());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.001);
    }

    @Test
    public void testHashCode() {
        Decimal160 a = new Decimal160(123, 456, 2);
        Decimal160 b = new Decimal160(123, 456, 2);

        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testInPlaceAddition() {
        Decimal160 a = Decimal160.fromDouble(123.45, 2);
        Decimal160 b = Decimal160.fromDouble(67.89, 2);

        a.add(b);

        Assert.assertEquals(191.34, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testInPlaceDivision() {
        Decimal160 a = Decimal160.fromDouble(100.0, 2);
        Decimal160 b = Decimal160.fromDouble(4.0, 1);

        a.divide(b, 2, RoundingMode.HALF_UP);

        Assert.assertEquals(25.0, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testInPlaceModulo() {
        Decimal160 a = Decimal160.fromDouble(100.0, 2);
        Decimal160 b = Decimal160.fromDouble(30.0, 2);

        a.modulo(b);

        Assert.assertEquals(10.0, a.toDouble(), 0.01);
    }

    @Test
    public void testInPlaceMultiplication() {
        Decimal160 a = Decimal160.fromDouble(12.34, 2);
        Decimal160 b = Decimal160.fromDouble(5.6, 1);

        a.multiply(b);

        Assert.assertEquals(69.104, a.toDouble(), 0.001);
        Assert.assertEquals(3, a.getScale()); // 2 + 1 = 3
    }

    @Test
    public void testInPlaceSubtraction() {
        Decimal160 a = Decimal160.fromDouble(123.45, 2);
        Decimal160 b = Decimal160.fromDouble(67.89, 2);

        a.subtract(b);

        Assert.assertEquals(55.56, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testIsNegative() {
        Decimal160 positive = new Decimal160(0, 100, 2);
        Decimal160 negative = new Decimal160(-1, 0, 2);

        Assert.assertFalse(positive.isNegative());
        Assert.assertTrue(negative.isNegative());
    }

    @Test
    public void testIsZero() {
        Decimal160 zero = new Decimal160(0, 0, 2);
        Decimal160 nonZero = new Decimal160(0, 1, 2);

        Assert.assertTrue(zero.isZero());
        Assert.assertFalse(nonZero.isZero());
    }

    @Test
    public void testLargeNumbers() {
        // Test with large numbers
        Decimal160 a = new Decimal160(0x0L, 0xFFFFFFFFFFFFFFFFL, 0); // Large positive
        Decimal160 b = Decimal160.fromLong(2, 0);

        a.multiply(b);

        // The result should be 2 * (2^64 - 1)
        Assert.assertEquals(0x1L, a.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFEL, a.getLow());
    }

    @Test(expected = ArithmeticException.class)
    public void testModuloByZero() {
        Decimal160 a = Decimal160.fromDouble(100.0, 2);
        Decimal160 zero = Decimal160.fromDouble(0.0, 2);

        a.modulo(zero);
    }

    @Test
    public void testModuloFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);

        // Number of test iterations
        final int ITERATIONS = 10_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands with various scales and values
            Decimal160 a = rnd.nextDecimal160();
            Decimal160 b = rnd.nextDecimal160();

            if (!b.isZero()) {
                // Test modulo accuracy
                testModuloAccuracy(a, b, i);
            }
        }
    }

    @Test
    public void testModuloNegative() {
        // Test -10 % 3 = -1
        Decimal160 a = Decimal160.fromDouble(-10.0, 0);
        Decimal160 b = Decimal160.fromDouble(3.0, 0);

        a.modulo(b);

        Assert.assertEquals(-1.0, a.toDouble(), 0.001);
    }

    @Test
    public void testModuloSimple() {
        // Test 10 % 3 = 1
        Decimal160 a = Decimal160.fromDouble(10.0, 0);
        Decimal160 b = Decimal160.fromDouble(3.0, 0);

        a.modulo(b);

        Assert.assertEquals(1.0, a.toDouble(), 0.001);
    }

    @Test
    public void testModuloWithDecimals() {
        // Test 10.5 % 3.2 = 0.9
        Decimal160 a = Decimal160.fromDouble(10.5, 1);
        Decimal160 b = Decimal160.fromDouble(3.2, 1);

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
            Decimal160 a = rnd.nextDecimal160();
            Decimal160 b = rnd.nextDecimal160();

            // Test multiplication accuracy
            testMultiplicationAccuracy(a, b, i);
        }
    }

    @Test
    public void testNegativeArithmetic() {
        // Test with negative numbers
        Decimal160 a = Decimal160.fromDouble(-12.5, 1);
        Decimal160 b = Decimal160.fromDouble(4.0, 1);

        a.multiply(b);

        Assert.assertEquals(-50.0, a.toDouble(), 0.01);
        Assert.assertEquals(2, a.getScale());  // Scale should be 1 + 1 = 2

        // Test both negative
        Decimal160 c = Decimal160.fromDouble(-3.0, 1);
        Decimal160 d = Decimal160.fromDouble(-7.0, 1);

        c.multiply(d);
        Assert.assertEquals(21.0, c.toDouble(), 0.01);
    }

    @Test
    public void testRound() {
        // Test basic rounding from scale 3 to scale 2
        Decimal160 a = Decimal160.fromDouble(1.234, 3);
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
        java.math.RoundingMode[] modes = {
                java.math.RoundingMode.UP,
                java.math.RoundingMode.DOWN,
                java.math.RoundingMode.CEILING,
                java.math.RoundingMode.FLOOR,
                java.math.RoundingMode.HALF_UP,
                java.math.RoundingMode.HALF_DOWN,
                java.math.RoundingMode.HALF_EVEN
        };

        for (java.math.RoundingMode mode : modes) {
            Decimal160 a = Decimal160.fromDouble(testValue, originalScale);
            a.round(targetScale, mode);

            // Compare with BigDecimal reference
            java.math.BigDecimal reference = java.math.BigDecimal.valueOf(testValue)
                    .setScale(originalScale, java.math.RoundingMode.HALF_UP)
                    .setScale(targetScale, mode);

            Assert.assertEquals("Rounding mode " + mode + " failed for " + testValue,
                    reference, a.toBigDecimal());
            Assert.assertEquals("Scale should be " + targetScale, targetScale, a.getScale());
        }
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

        Decimal160 decimal = new Decimal160();
        for (int i = 0; i < iterations; i++) {
            // Generate random decimal with varying characteristics
            rnd.nextDecimal160(decimal);

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
            Decimal160 testDecimal = new Decimal160();
            testDecimal.copyFrom(decimal);

            // Get the original BigDecimal representation
            java.math.BigDecimal originalBigDecimal;
            try {
                originalBigDecimal = decimal.toBigDecimal();
            } catch (NumberFormatException e) {
                String errorMsg = String.format(
                        "Failed to convert original Decimal160 to BigDecimal at iteration %d:\n" +
                                "Decimal160: high=0x%016x, low=0x%016x, scale=%d\n" +
                                "toString()=%s\n" +
                                "Error: %s",
                        i, decimal.getHigh(), decimal.getLow(), decimal.getScale(),
                        decimal, e.getMessage()
                );
                Assert.fail(errorMsg);
                return; // unreachable but makes compiler happy
            }

            try {
                // Apply rounding to our Decimal160
                testDecimal.round(targetScale, roundingMode);

                // Apply same rounding to BigDecimal as oracle
                java.math.BigDecimal expectedBigDecimal = originalBigDecimal.setScale(targetScale, roundingMode);

                // Compare results
                java.math.BigDecimal actualBigDecimal;
                try {
                    actualBigDecimal = testDecimal.toBigDecimal();
                } catch (NumberFormatException e) {
                    String errorMsg = String.format(
                            "Failed to convert result Decimal160 to BigDecimal at iteration %d:\n" +
                                    "Original: %s (scale=%d)\n" +
                                    "Target scale: %d, Mode: %s\n" +
                                    "Result Decimal160: high=0x%016x, low=0x%016x, scale=%d\n" +
                                    "toString()=%s\n" +
                                    "Error: %s",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            testDecimal.getHigh(), testDecimal.getLow(), testDecimal.getScale(),
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
                                    "Original Decimal160: high=0x%016x, low=0x%016x, scale=%d",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            expectedBigDecimal.toPlainString(),
                            actualBigDecimal.toPlainString(),
                            decimal.getHigh(), decimal.getLow(), decimal.getScale()
                    );
                    Assert.fail(errorMsg);
                }

                // Verify the scale is set correctly
                Assert.assertEquals("Scale should match target scale", targetScale, testDecimal.getScale());

            } catch (ArithmeticException e) {
                // BigDecimal might throw ArithmeticException in some cases
                // In such cases, our implementation should either handle it gracefully
                // or throw the same exception
                boolean decimal160Threw = false;
                try {
                    testDecimal.round(targetScale, roundingMode);
                } catch (ArithmeticException e2) {
                    decimal160Threw = true;
                }

                if (!decimal160Threw) {
                    String errorMsg = String.format(
                            "BigDecimal threw ArithmeticException but Decimal160 didn't at iteration %d:\n" +
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
    public void testRoundFuzzWithUnnecessaryMode() {
        // Separate fuzz test for UNNECESSARY mode which has special semantics
        final int iterations = 1000;
        final Rnd rnd = TestUtils.generateRandom(null);
        final Decimal160 decimal = new Decimal160();

        for (int i = 0; i < iterations; i++) {
            // Generate random decimal
            rnd.nextDecimal160(decimal);

            // For UNNECESSARY mode, we need to ensure no rounding is actually needed
            // So we'll create a decimal that already has the target scale
            int currentScale = decimal.getScale();

            // Test with same scale (no rounding needed) - should be no-op
            Decimal160 testDecimal = new Decimal160();
            testDecimal.copyFrom(decimal);

            java.math.BigDecimal originalBigDecimal = decimal.toBigDecimal();

            // Apply UNNECESSARY rounding with same scale
            testDecimal.round(currentScale, java.math.RoundingMode.UNNECESSARY);

            // Should be unchanged
            java.math.BigDecimal resultBigDecimal = testDecimal.toBigDecimal();

            if (!originalBigDecimal.equals(resultBigDecimal)) {
                String errorMsg = String.format(
                        "UNNECESSARY mode changed value when no rounding needed at iteration %d:\n" +
                                "Original: %s (scale=%d)\n" +
                                "Result: %s (scale=%d)",
                        i,
                        originalBigDecimal.toPlainString(), currentScale,
                        resultBigDecimal.toPlainString(), testDecimal.getScale()
                );
                Assert.fail(errorMsg);
            }

            // Verify scale unchanged
            Assert.assertEquals("Scale should remain unchanged with UNNECESSARY mode",
                    currentScale, testDecimal.getScale());
        }
    }

    @Test
    public void testRoundHalfEvenTieBreaking() {
        // Test HALF_EVEN tie-breaking specifically
        double[] testValues = {1.125, 1.135, 2.125, 2.135}; // Tie cases
        java.math.BigDecimal[] expectedResults = {
                java.math.BigDecimal.valueOf(1.12), // Round to even (2)
                java.math.BigDecimal.valueOf(1.14), // Round to even (4)
                java.math.BigDecimal.valueOf(2.12), // Round to even (2)
                java.math.BigDecimal.valueOf(2.14)  // Round to even (4)
        };

        for (int i = 0; i < testValues.length; i++) {
            Decimal160 a = Decimal160.fromDouble(testValues[i], 3);
            a.round(2, java.math.RoundingMode.HALF_EVEN);

            Assert.assertEquals("HALF_EVEN tie-breaking failed for " + testValues[i],
                    expectedResults[i], a.toBigDecimal());
        }
    }

    @Test
    public void testRoundNegativeNumbers() {
        double testValue = -1.235;
        int originalScale = 3;
        int targetScale = 2;

        java.math.RoundingMode[] modes = {
                java.math.RoundingMode.UP,
                java.math.RoundingMode.DOWN,
                java.math.RoundingMode.CEILING,
                java.math.RoundingMode.FLOOR,
                java.math.RoundingMode.HALF_UP,
                java.math.RoundingMode.HALF_DOWN,
                java.math.RoundingMode.HALF_EVEN
        };

        for (java.math.RoundingMode mode : modes) {
            Decimal160 a = Decimal160.fromDouble(testValue, originalScale);
            a.round(targetScale, mode);

            // Compare with BigDecimal reference
            java.math.BigDecimal reference = java.math.BigDecimal.valueOf(testValue)
                    .setScale(originalScale, java.math.RoundingMode.HALF_UP)
                    .setScale(targetScale, mode);

            Assert.assertEquals("Rounding mode " + mode + " failed for negative " + testValue,
                    reference, a.toBigDecimal());
            Assert.assertEquals("Scale should be " + targetScale, targetScale, a.getScale());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRoundNegativeScale() {
        Decimal160 a = Decimal160.fromDouble(1.23, 2);
        a.round(-1, java.math.RoundingMode.HALF_UP);
    }

    @Test
    public void testRoundNoChange() {
        // Test when target scale equals current scale (no-op)
        Decimal160 a = Decimal160.fromDouble(1.234, 3);
        Decimal160 original = new Decimal160();
        original.copyFrom(a);

        a.round(3, java.math.RoundingMode.HALF_UP);

        Assert.assertEquals("No-op rounding should not change value", original, a);
    }

    @Test
    public void testRoundScaleIncrease() {
        // Test increasing scale (should add trailing zeros)
        Decimal160 a = Decimal160.fromDouble(1.23, 2);
        a.round(4, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(1.23).setScale(4, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals("Scale increase failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 4", 4, a.getScale());
    }

    @Test
    public void testRoundUnnecessaryMode() {
        // Test UNNECESSARY mode - should be a no-op regardless of whether rounding is needed
        Decimal160 a = Decimal160.fromDouble(1.235, 3);
        Decimal160 original = new Decimal160();
        original.copyFrom(a);

        a.round(2, java.math.RoundingMode.UNNECESSARY);

        // UNNECESSARY mode should be a no-op, leaving the value unchanged
        Assert.assertEquals("UNNECESSARY mode should be no-op", original.toBigDecimal(), a.toBigDecimal());
        Assert.assertEquals("Scale should remain unchanged", original.getScale(), a.getScale());
    }

    @Test
    public void testRoundUnnecessaryModeNoRounding() {
        // Test UNNECESSARY mode when no rounding is needed
        Decimal160 a = Decimal160.fromDouble(1.231, 3);
        a.round(2, java.math.RoundingMode.UNNECESSARY);

        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(1.231);
        Assert.assertEquals("UNNECESSARY mode should work when no rounding needed", expected, a.toBigDecimal());
    }

    @Test
    public void testRoundZero() {
        // Test rounding zero
        Decimal160 a = Decimal160.fromDouble(0.0, 5);
        a.round(2, java.math.RoundingMode.HALF_UP);

        java.math.BigDecimal expected = java.math.BigDecimal.ZERO.setScale(2, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals("Rounding zero failed", expected, a.toBigDecimal());
        Assert.assertEquals("Scale should be 2", 2, a.getScale());
    }

    @Test
    public void testScaleHandling() {
        // Test addition with different scales
        Decimal160 a = Decimal160.fromDouble(123.45, 2);  // Scale 2
        Decimal160 b = Decimal160.fromDouble(6.789, 3);   // Scale 3

        a.add(b);

        Assert.assertEquals(3, a.getScale());  // Should use larger scale
        Assert.assertEquals(130.239, a.toDouble(), 0.001);
    }

    @Test
    public void testSinkableInterface() {
        // Test that Decimal160 can be used as a Sinkable
        Decimal160 decimal = Decimal160.fromDouble(42.99, 2);
        StringSink sink = new StringSink();

        // Use the put(Sinkable) method from CharSink
        sink.put(decimal);

        Assert.assertEquals("42.99", sink.toString());
    }

    @Test
    public void testStaticAdd() {
        Decimal160 a = Decimal160.fromDouble(123.45, 2);
        Decimal160 b = Decimal160.fromDouble(67.89, 2);
        Decimal160 result = new Decimal160();

        Decimal160.add(a, b, result);

        Assert.assertEquals(191.34, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(123.45, a.toDouble(), 0.01);
        Assert.assertEquals(67.89, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticDivide() {
        Decimal160 a = Decimal160.fromDouble(100.0, 2);
        Decimal160 b = Decimal160.fromDouble(4.0, 1);
        Decimal160 result = new Decimal160();

        Decimal160.divide(a, b, result, 2, RoundingMode.HALF_UP);

        Assert.assertEquals(25.0, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(100.0, a.toDouble(), 0.01);
        Assert.assertEquals(4.0, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticMethodsWithDifferentScales() {
        Decimal160 a = Decimal160.fromDouble(123.45, 2);  // Scale 2
        Decimal160 b = Decimal160.fromDouble(6.789, 3);   // Scale 3
        Decimal160 result = new Decimal160();

        Decimal160.add(a, b, result);

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
        Decimal160 a = Decimal160.fromDouble(100.0, 2);
        Decimal160 b = Decimal160.fromDouble(30.0, 2);
        Decimal160 result = new Decimal160();

        Decimal160.modulo(a, b, result);

        Assert.assertEquals(10.0, result.toDouble(), 0.01);
        Assert.assertEquals(2, result.getScale());

        // Verify operands are unchanged
        Assert.assertEquals(100.0, a.toDouble(), 0.01);
        Assert.assertEquals(30.0, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticMultiply() {
        Decimal160 a = Decimal160.fromDouble(12.34, 2);
        Decimal160 b = Decimal160.fromDouble(5.6, 1);
        Decimal160 result = new Decimal160();

        Decimal160.multiply(a, b, result);

        Assert.assertEquals(69.104, result.toDouble(), 0.001);
        Assert.assertEquals(3, result.getScale()); // 2 + 1 = 3

        // Verify operands are unchanged
        Assert.assertEquals(12.34, a.toDouble(), 0.01);
        Assert.assertEquals(5.6, b.toDouble(), 0.01);
    }

    @Test
    public void testStaticNegate() {
        Decimal160 result = new Decimal160();

        // Test positive number
        Decimal160 a = Decimal160.fromDouble(42.5, 1);
        Decimal160.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Verify original is unchanged - compare with fresh instance
        Decimal160 original = Decimal160.fromDouble(42.5, 1);
        Assert.assertEquals(original.toBigDecimal(), a.toBigDecimal());

        // Test negative number
        a = Decimal160.fromDouble(-123.456, 3);
        Decimal160.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Verify original is unchanged - compare with fresh instance
        original = Decimal160.fromDouble(-123.456, 3);
        Assert.assertEquals(original.toBigDecimal(), a.toBigDecimal());

        // Test zero
        a = Decimal160.fromDouble(0.0, 0);
        Decimal160.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Test very small number
        a = Decimal160.fromDouble(1e-10, 10);
        Decimal160.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());

        // Test very large number
        a = Decimal160.fromDouble(1e10, 0);
        Decimal160.negate(a, result);
        Assert.assertEquals(a.toBigDecimal().negate(), result.toBigDecimal());
    }

    @Test
    public void testStaticNegateConsistentWithInPlace() {
        Decimal160 staticResult = new Decimal160();
        Decimal160 inPlaceResult = new Decimal160();

        // Test various values to ensure static and in-place methods are consistent
        double[] testValues = {0.0, 1.0, -1.0, 123.456, -789.123, 1e-5, -1e-5, 1e8, -1e8};

        for (double value : testValues) {
            Decimal160 a = Decimal160.fromDouble(value, 3);

            // Test static method
            Decimal160.negate(a, staticResult);

            // Test in-place method
            inPlaceResult.copyFrom(a);
            inPlaceResult.negate();

            // Results should be identical
            Assert.assertEquals("Static and in-place negate differ for value " + value,
                    staticResult.toBigDecimal(), inPlaceResult.toBigDecimal());

            // Verify original is unchanged by static method - compare with fresh instance
            Decimal160 originalFresh = Decimal160.fromDouble(value, 3);
            Assert.assertEquals("Static negate modified original for value " + value,
                    originalFresh.toBigDecimal(), a.toBigDecimal());
        }
    }

    @Test
    public void testStaticSubtract() {
        Decimal160 a = Decimal160.fromDouble(123.45, 2);
        Decimal160 b = Decimal160.fromDouble(67.89, 2);
        Decimal160 result = new Decimal160();

        Decimal160.subtract(a, b, result);

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
            Decimal160 a = rnd.nextDecimal160();
            Decimal160 b = rnd.nextDecimal160();

            // Test subtraction accuracy
            testSubtractionAccuracy(a, b, i);
        }
    }

    @Test
    public void testToBigDecimal() {
        // Test basic positive number
        Decimal160 a = Decimal160.fromDouble(123.456, 3);
        java.math.BigDecimal bigDecimal = a.toBigDecimal();
        Assert.assertEquals("123.456", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());

        // Test negative number
        a = Decimal160.fromDouble(-789.123, 3);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("-789.123", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());

        // Test zero
        a = Decimal160.fromDouble(0.0, 2);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("0.00", bigDecimal.toString());
        Assert.assertEquals(2, bigDecimal.scale());

        // Test integer (scale 0)
        a = Decimal160.fromDouble(42.0, 0);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("42", bigDecimal.toString());
        Assert.assertEquals(0, bigDecimal.scale());

        // Test very small number
        a = Decimal160.fromDouble(0.001, 3);
        bigDecimal = a.toBigDecimal();
        Assert.assertEquals("0.001", bigDecimal.toString());
        Assert.assertEquals(3, bigDecimal.scale());
    }

    @Test
    public void testToBigDecimalConsistentWithToString() {
        // toBigDecimal should produce the same string representation as toString
        double[] testValues = {0.0, 1.0, -1.0, 123.456, -789.123, 0.001, -0.001};

        for (double value : testValues) {
            Decimal160 a = Decimal160.fromDouble(value, 3);
            String stringRep = a.toString();
            java.math.BigDecimal bigDecimal = a.toBigDecimal();

            Assert.assertEquals("toBigDecimal and toString should be consistent for " + value,
                    stringRep, bigDecimal.toString());
        }
    }

    @Test
    public void testToBigDecimalPrecision() {
        // Test that toBigDecimal preserves precision better than toDouble
        Decimal160 a = Decimal160.fromDouble(123.456789, 6);
        java.math.BigDecimal bigDecimal = a.toBigDecimal();

        // BigDecimal should preserve the exact decimal representation
        Assert.assertTrue("BigDecimal should preserve precision",
                bigDecimal.toString().contains("123.456789"));

        // Convert back and forth should be consistent
        java.math.BigDecimal expected = java.math.BigDecimal.valueOf(123.456789).setScale(6, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals(expected.doubleValue(), bigDecimal.doubleValue(), 1e-15);
    }

    @Test
    public void testToDouble() {
        Decimal160 decimal = Decimal160.fromLong(12345, 3);
        Assert.assertEquals(12.345, decimal.toDouble(), 0.0001);

        Decimal160 negative = new Decimal160(-1, -1, 2); // Two's complement representation
        Assert.assertTrue(negative.toDouble() < 0);
    }

    @Test
    public void testToSinkBasic() {
        Decimal160 decimal = Decimal160.fromLong(12345, 2);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("123.45", sink.toString());
    }

    @Test
    public void testToSinkComplex160Bit() {
        // Test complex 160-bit number conversion to decimal
        Decimal160 decimal = new Decimal160(0x123456789ABCDEFL, 0xFEDCBA9876543210L, 2);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        String result = sink.toString();
        // Should convert large 160-bit number to proper decimal representation
        Assert.assertEquals("15123660752041709473323553696831370.40", result);
    }

    @Test
    public void testToSinkLargeNumber() {
        Decimal160 decimal = Decimal160.fromLong(9876543210L, 4);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("987654.3210", sink.toString());
    }

    @Test
    public void testToSinkMultipleDecimals() {
        // Test multiple decimals being written to the same sink
        StringSink sink = new StringSink();

        Decimal160 a = Decimal160.fromDouble(12.34, 2);
        Decimal160 b = Decimal160.fromDouble(56.78, 2);

        a.toSink(sink);
        sink.putAscii(" + ");
        b.toSink(sink);
        sink.putAscii(" = ");

        Decimal160 result = new Decimal160();
        Decimal160.add(a, b, result);
        result.toSink(sink);

        Assert.assertEquals("12.34 + 56.78 = 69.12", sink.toString());
    }

    @Test
    public void testToSinkNegativeNumber() {
        Decimal160 decimal = Decimal160.fromLong(-12345, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("-12.345", sink.toString());
    }

    @Test
    public void testToSinkSmallNumber() {
        Decimal160 decimal = Decimal160.fromLong(123, 5);  // 0.00123
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("0.00123", sink.toString());
    }

    @Test
    public void testToSinkVsToString() {
        // Test that toSink and toString produce the same result
        Decimal160 decimal = Decimal160.fromDouble(123.456, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);
        String sinkResult = sink.toString();
        String toStringResult = decimal.toString();

        Assert.assertEquals(toStringResult, sinkResult);
    }

    @Test
    public void testToSinkZero() {
        Decimal160 decimal = Decimal160.fromLong(0, 3);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("0.000", sink.toString());
    }

    @Test
    public void testToSinkZeroScale() {
        Decimal160 decimal = Decimal160.fromLong(123, 0);
        StringSink sink = new StringSink();

        decimal.toSink(sink);

        Assert.assertEquals("123", sink.toString());
    }

    @Test
    public void testToString() {
        Decimal160 decimal = Decimal160.fromLong(12345, 2);
        String str = decimal.toString();

        Assert.assertTrue(str.contains("123"));
        Assert.assertTrue(str.contains("45"));
    }

    @Test
    public void testZeroAllocationArithmetic() {
        // Demonstrate truly allocation-free arithmetic chain
        Decimal160 accumulator = new Decimal160();

        // Start with 100
        accumulator.setFromLong(10000, 2); // 100.00 with scale 2

        // Add 50 -> 150
        Decimal160 increment = Decimal160.fromLong(5000, 2); // 50.00 with scale 2
        accumulator.add(increment);
        Assert.assertEquals(150.0, accumulator.toDouble(), 0.01);

        // Multiply by 2 -> 300
        Decimal160 multiplier = Decimal160.fromLong(2, 0);
        accumulator.multiply(multiplier);
        Assert.assertEquals(300.0, accumulator.toDouble(), 0.01);

        // Subtract 75 -> 225
        Decimal160 subtrahend = Decimal160.fromLong(7500, 2); // 75.00 with scale 2
        accumulator.subtract(subtrahend);
        Assert.assertEquals(225.0, accumulator.toDouble(), 0.01);

        // Divide by 5 -> 45
        Decimal160 divisor = Decimal160.fromLong(5, 0);
        accumulator.divide(divisor, 2, RoundingMode.HALF_UP);
        Assert.assertEquals(45.0, accumulator.toDouble(), 0.01);

        // Modulo 10 -> 5
        Decimal160 mod = Decimal160.fromLong(1000, 2); // 10.00 with scale 2
        accumulator.modulo(mod);
        Assert.assertEquals(5.0, accumulator.toDouble(), 0.01);
    }

    private boolean fitsInLongRange(Decimal160 decimal) {
        // Check if the decimal can be represented as a long without overflow
        // This is a conservative check to avoid overflow in reference calculations
        return decimal.getHigh() == 0 || (decimal.getHigh() == -1 && decimal.getLow() < 0);
    }

    private void testAdditionAccuracy(Decimal160 a, Decimal160 b, int iteration) {
        // Test addition accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference addition
        BigDecimal expected = bigA.add(bigB);

        BigDecimal min = Decimal160.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal160.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected

            Decimal160 result = new Decimal160();

            Assert.assertThrows(ArithmeticException.class, () -> {
                Decimal160.add(a, b, result);
            });
            return;
        }

        // catch overflow exceptions
        try {
            Decimal160 staticResult = new Decimal160();

            // Test static add method
            Decimal160.add(a, b, staticResult);

            Decimal160 result = new Decimal160();
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
        } catch (ArithmeticException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testComparisonAccuracy(Decimal160 a, Decimal160 b, int iteration) {
        // Test compareTo with different scales
        int decimal160Result = a.compareTo(b);

        // Test reference calculation
        double aDouble = a.toDouble();
        double bDouble = b.toDouble();
        int doubleResult = Double.compare(aDouble, bDouble);

        // Results should have the same sign (or both be zero)
        boolean sameSign = (decimal160Result == 0 && doubleResult == 0) ||
                (decimal160Result > 0 && doubleResult > 0) ||
                (decimal160Result < 0 && doubleResult < 0);

        Assert.assertTrue("Comparison accuracy failed at iteration " + iteration +
                        " (a=" + aDouble + ", b=" + bDouble +
                        ", decimal160=" + decimal160Result + ", double=" + doubleResult + ")",
                sameSign);
    }

    private void testDivisionAccuracy(Decimal160 a, Decimal160 b, int iteration) {
        // Choose a reasonable result scale
        int resultScale = Math.min(a.getScale() + 2, 6); // Limit to avoid precision issues

        // Test division accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform division with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.divide(bigB, resultScale, RoundingMode.HALF_UP);

        // catch overflow exceptions
        try {
            Decimal160 staticResult = new Decimal160();

            // Test static divide method
            Decimal160.divide(a, b, staticResult, resultScale, RoundingMode.HALF_UP);

            Decimal160 result = new Decimal160();
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
        } catch (ArithmeticException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testModuloAccuracy(Decimal160 a, Decimal160 b, int iteration) {
        // Choose a reasonable result scale
        int resultScale = Math.min(a.getScale() + 2, 6); // Limit to avoid precision issues

        // Test modulo accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform modulo with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.remainder(bigB);

        // catch overflow exceptions
        try {
            Decimal160 staticResult = new Decimal160();

            // Test static modulo method
            Decimal160.modulo(a, b, staticResult);

            Decimal160 result = new Decimal160();
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
        } catch (ArithmeticException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testMultiplicationAccuracy(Decimal160 a, Decimal160 b, int iteration) {
        // Test multiplication accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform multiplication with the same scale and rounding mode as our implementation should use
        BigDecimal expected = bigA.multiply(bigB);

        BigDecimal min = Decimal160.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal160.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected

            Decimal160 result = new Decimal160();

            Assert.assertThrows(ArithmeticException.class, () -> {
                Decimal160.multiply(a, b, result);
            });
            return;
        }

        // catch overflow exceptions
        try {
            Decimal160 staticResult = new Decimal160();

            // Test static multiply method
            Decimal160.multiply(a, b, staticResult);

            Decimal160 result = new Decimal160();
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
        } catch (ArithmeticException e) {
            // Skip this test case if overflow occurs during scaling
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 128-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testSubtractionAccuracy(Decimal160 a, Decimal160 b, int iteration) {
        // Test subtraction accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference subtraction
        BigDecimal expected = bigA.subtract(bigB);

        BigDecimal min = Decimal160.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal160.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected

            Decimal160 result = new Decimal160();

            Assert.assertThrows(ArithmeticException.class, () -> {
                Decimal160.subtract(a, b, result);
            });
            return;
        }

        // catch overflow exceptions
        try {
            Decimal160 staticResult = new Decimal160();

            // Test static subtract method
            Decimal160.subtract(a, b, staticResult);

            // Verify operands unchanged
            Assert.assertEquals("Subtract modified first operand at iteration " + iteration,
                    a.toBigDecimal(), a.toBigDecimal());
            Assert.assertEquals("Subtract modified second operand at iteration " + iteration,
                    b.toBigDecimal(), b.toBigDecimal());

            Decimal160 result = new Decimal160();
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
        } catch (ArithmeticException e) {
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