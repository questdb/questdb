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

import io.questdb.std.Decimal64;
import io.questdb.std.NumericException;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Tests for the Decimal64 class
 */
public class Decimal64Test {

    @Test
    public void testAddition() {
        // Same scale addition
        Decimal64 a = new Decimal64(12345, 2); // 123.45
        Decimal64 b = new Decimal64(6789, 2);  // 67.89
        a.add(b);
        Assert.assertEquals("191.34", a.toString());
        Assert.assertEquals(2, a.getScale());

        // Different scale addition
        Decimal64 c = new Decimal64(123, 1);   // 12.3
        Decimal64 d = new Decimal64(4567, 3);  // 4.567
        c.add(d);
        Assert.assertEquals("16.867", c.toString());
        Assert.assertEquals(3, c.getScale());
    }

    @Test
    public void testAdditionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int ITERATIONS = 5_000; // Limited by 64-bit range

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands using nextDecimal64
            Decimal64 a = rnd.nextDecimal64();
            Decimal64 b = rnd.nextDecimal64();

            // Test addition accuracy
            testAdditionAccuracy(a, b, i);
        }
    }

    @Test
    public void testModuloStatic() {
        Decimal64 a = Decimal64.fromLong(10, 0);
        Decimal64 b = Decimal64.fromLong(3, 0);
        Decimal64 result = new Decimal64();
        Decimal64.modulo(a, b, result);
        Assert.assertEquals(1, result.getValue());
        Assert.assertEquals(10, a.getValue());
        Assert.assertEquals(3, b.getValue());
    }

    @Test
    public void testAdditionScaleB() {
        Decimal64 a = Decimal64.fromLong(10, 1);
        Decimal64 b = Decimal64.fromLong(1, 0);
        a.add(b);
        Assert.assertEquals(20, a.getValue());
        Assert.assertEquals(1, a.getScale());
    }

    @Test
    public void testDivideNegative() {
        Decimal64 a = Decimal64.fromLong(-10, 0);
        Decimal64 b = Decimal64.fromLong(-2, 0);
        a.divide(b, 0, RoundingMode.HALF_UP);
        Assert.assertEquals(5, a.getValue());
        Assert.assertEquals(0, a.getScale());
    }

    @Test
    public void testDivideNegativeResult() {
        Decimal64 a = Decimal64.fromLong(-10, 0);
        Decimal64 b = Decimal64.fromLong(2, 0);
        a.divide(b, 0, RoundingMode.HALF_UP);
        Assert.assertEquals(-5, a.getValue());
        Assert.assertEquals(0, a.getScale());
    }

    @Test
    public void testModuloScale() {
        Decimal64 a = Decimal64.fromLong(10, 0);
        Decimal64 b = Decimal64.fromLong(30, 1);
        a.modulo(b);
        Assert.assertEquals(10, a.getValue());
        Assert.assertEquals(1, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testMultiplyOverflowScale() {
        Decimal64 a = Decimal64.fromLong(2, 10);
        Decimal64 b = Decimal64.fromLong(2, 10);
        a.multiply(b);
    }

    @Test
    public void testRoundScale() {
        Decimal64 a = Decimal64.fromLong(-10, 1);
        a.round(2, RoundingMode.HALF_UP);
        Assert.assertEquals(-100, a.getValue());
        Assert.assertEquals(2, a.getScale());
    }

    @Test
    public void testSubtractScaleB() {
        Decimal64 a = Decimal64.fromLong(10, 1);
        Decimal64 b = Decimal64.fromLong(1, 0);
        a.subtract(b);
        Assert.assertEquals(0, a.getValue());
        Assert.assertEquals(1, a.getScale());
    }

    @Test(expected = NumericException.class)
    public void testDivisionOverflowScale() {
        Decimal64 a = Decimal64.fromLong(1, 0);
        Decimal64 b = Decimal64.fromLong(1, 10);
        a.divide(b, 10, RoundingMode.HALF_UP);
    }

    @Test(expected = NumericException.class)
    public void testDivisionOverflow() {
        Decimal64 a = Decimal64.fromLong(10000000, 0);
        Decimal64 b = Decimal64.fromLong(1, 10);
        a.divide(b, 5, RoundingMode.HALF_UP);
    }

    @Test(expected = NumericException.class)
    public void testAdditionOverflowScale() {
        Decimal64 a = Decimal64.fromLong(10000000, 0);
        Decimal64 b = Decimal64.fromLong(1, 15);
        a.add(b);
    }

    @Test(expected = NumericException.class)
    public void testAdditionOverflowNegativeScale() {
        Decimal64 a = Decimal64.fromLong(-10000000, 0);
        Decimal64 b = Decimal64.fromLong(1, 15);
        a.add(b);
    }

    @Test
    public void testAllModes() {
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
            Decimal64 a = Decimal64.fromDouble(testValue, originalScale);
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
    public void testBasicConstruction() {
        Decimal64 zero = new Decimal64();
        Assert.assertEquals(0, zero.getValue());
        Assert.assertEquals(0, zero.getScale());
        Assert.assertTrue(zero.isZero());
        Assert.assertFalse(zero.isNegative());

        Decimal64 value = new Decimal64(12345, 2);
        Assert.assertEquals(12345, value.getValue());
        Assert.assertEquals(2, value.getScale());
        Assert.assertFalse(value.isZero());
        Assert.assertFalse(value.isNegative());

        Decimal64 negative = new Decimal64(-12345, 2);
        Assert.assertEquals(-12345, negative.getValue());
        Assert.assertEquals(2, negative.getScale());
        Assert.assertFalse(negative.isZero());
        Assert.assertTrue(negative.isNegative());
    }

    @Test(expected = NumericException.class)
    public void testBigDecimalOverflow() {
        BigDecimal bd = new BigDecimal("1e100");
        Decimal64.fromBigDecimal(bd);
    }

    @Test
    public void testCompareTo() {
        Decimal64 smaller = new Decimal64(100, 2);  // 1.00
        Decimal64 larger = new Decimal64(200, 2);   // 2.00

        Assert.assertTrue(smaller.compareTo(larger) < 0);
        Assert.assertTrue(larger.compareTo(smaller) > 0);
        Assert.assertEquals(0, smaller.compareTo(smaller));
    }

    @Test
    public void testCompareToFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int ITERATIONS = 5_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands using nextDecimal64
            Decimal64 a = rnd.nextDecimal64();
            Decimal64 b = rnd.nextDecimal64();

            BigDecimal bdA = a.toBigDecimal();
            BigDecimal bdB = b.toBigDecimal();

            // The comparison may overflow during rescaling
            try {
                int actual = a.compareTo(b);
                int expected = bdA.compareTo(bdB);
                Assert.assertEquals("iteration: " + i + " expected:<" + expected + "> but was:<" + actual + ">", expected, actual);
            } catch (NumericException ignore) {
                // Overflow is acceptable during scaling operations
            }
        }
    }

    @Test
    public void testCompareToWithDifferentScales() {
        // Test 12.34 (scale 2) vs 12.345 (scale 3)
        Decimal64 a = Decimal64.fromDouble(12.34, 2);   // 12.34
        Decimal64 b = Decimal64.fromDouble(12.345, 3);  // 12.345

        Assert.assertTrue(a.compareTo(b) < 0); // 12.34 < 12.345
        Assert.assertTrue(b.compareTo(a) > 0); // 12.345 > 12.34

        // Test equal values with different scales
        Decimal64 c = new Decimal64(1230, 2);  // 12.30
        Decimal64 d = new Decimal64(123, 1);   // 12.3
        Assert.assertEquals(0, c.compareTo(d)); // Should be equal
    }

    @Test
    public void testConstants() {
        Assert.assertEquals(0, Decimal64.ZERO.getValue());
        Assert.assertEquals(0, Decimal64.ZERO.getScale());

        Assert.assertEquals(1, Decimal64.ONE.getValue());
        Assert.assertEquals(0, Decimal64.ONE.getScale());

        Assert.assertEquals(Long.MAX_VALUE, Decimal64.MAX_VALUE.getValue());
        Assert.assertEquals(Long.MIN_VALUE, Decimal64.MIN_VALUE.getValue());
    }

    @Test
    public void testConversions() {
        Decimal64 value = new Decimal64(12345, 2);

        // Test toBigDecimal
        BigDecimal bd = value.toBigDecimal();
        Assert.assertEquals("123.45", bd.toString());
        Assert.assertEquals(2, bd.scale());

        // Test toDouble
        double d = value.toDouble();
        Assert.assertEquals(123.45, d, 0.0001);

        // Test toString
        String str = value.toString();
        Assert.assertEquals("123.45", str);
    }

    @Test
    public void testCopyFrom() {
        Decimal64 source = new Decimal64(12345, 3);
        Decimal64 target = new Decimal64();

        target.copyFrom(source);
        Assert.assertEquals(source.getValue(), target.getValue());
        Assert.assertEquals(source.getScale(), target.getScale());
        Assert.assertEquals(source.toString(), target.toString());
    }

    @Test
    public void testDivision() {
        Decimal64 a = new Decimal64(12345, 2); // 123.45
        Decimal64 b = new Decimal64(5, 0);     // 5
        a.divide(b, 2, RoundingMode.HALF_UP);
        Assert.assertEquals("24.69", a.toString());
        Assert.assertEquals(2, a.getScale());

        // Test division with rounding
        Decimal64 c = new Decimal64(10, 0);    // 10
        Decimal64 d = new Decimal64(3, 0);     // 3
        c.divide(d, 3, RoundingMode.HALF_UP);
        Assert.assertEquals("3.333", c.toString());
    }

    @Test
    public void testDivisionByOne() {
        Decimal64 dividend = Decimal64.fromLong(123456L, 3);
        Decimal64 divisor = Decimal64.fromLong(1L, 0);
        dividend.divide(divisor, 3, RoundingMode.HALF_UP);
        Assert.assertEquals("123.456", dividend.toString());
    }

    @Test
    public void testDivisionByZero() {
        Decimal64 dividend = new Decimal64(123, 0);
        Decimal64 zero = new Decimal64(0, 0);

        try {
            dividend.divide(zero, 2, RoundingMode.HALF_UP);
            Assert.fail("Should have thrown NumericException");
        } catch (NumericException e) {
            Assert.assertTrue(e.getMessage().contains("Division by zero"));
        }

        try {
            dividend.modulo(zero);
            Assert.fail("Should have thrown NumericException");
        } catch (NumericException e) {
            Assert.assertTrue(e.getMessage().contains("Division by zero"));
        }
    }

    @Test
    public void testDivisionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int ITERATIONS = 3_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands
            Decimal64 a = rnd.nextDecimal64();
            Decimal64 b = rnd.nextDecimal64();

            // Skip zero divisor
            if (b.isZero()) {
                continue;
            }

            // Test division accuracy
            testDivisionAccuracy(a, b, i);
        }
    }

    @Test(expected = NumericException.class)
    public void testDivisionRemainderUnnecessary() {
        Decimal64 dividend = Decimal64.fromLong(10L, 0);
        Decimal64 divisor = Decimal64.fromLong(3L, 0);
        dividend.divide(divisor, 0, RoundingMode.UNNECESSARY);
    }

    @Test
    public void testEquals() {
        Decimal64 a = new Decimal64(1230, 2);  // 12.30
        Decimal64 b = new Decimal64(123, 1);   // 12.3
        Decimal64 c = new Decimal64(1230, 2);  // 12.30
        Decimal64 d = new Decimal64(1234, 2);  // 12.34

        Assert.assertEquals(a, b);  // Same value, different scale
        Assert.assertEquals(a, c);  // Same value, same scale
        Assert.assertNotEquals(a, d); // Different value
        Assert.assertNotEquals(null, a);
        Assert.assertNotEquals("string", a);
    }

    @Test
    public void testFromBigDecimal() {
        BigDecimal bd = new BigDecimal("123.45");
        Decimal64 value = Decimal64.fromBigDecimal(bd);
        Assert.assertEquals("123.45", value.toString());
        Assert.assertEquals(2, value.getScale());

        // Test negative scale handling
        BigDecimal negScale = new BigDecimal("123E2"); // 123 with scale -2
        Decimal64 transformed = Decimal64.fromBigDecimal(negScale);
        Assert.assertEquals("12300", transformed.toString());
        Assert.assertEquals(0, transformed.getScale());
    }

    @Test
    public void testFromDouble() {
        Decimal64 value = Decimal64.fromDouble(123.45, 2);
        Assert.assertEquals("123.45", value.toString());
        Assert.assertEquals(2, value.getScale());

        Decimal64 rounded = Decimal64.fromDouble(123.456, 2);
        Assert.assertEquals("123.46", rounded.toString()); // HALF_UP rounding
    }

    @Test
    public void testFromLong() {
        Decimal64 value = Decimal64.fromLong(123, 2);
        Assert.assertEquals(123, value.getValue());
        Assert.assertEquals(2, value.getScale());

        // Test caching for small values
        Decimal64 small1 = Decimal64.fromLong(5, 0);
        Decimal64 small2 = Decimal64.fromLong(5, 0);
        Assert.assertEquals(small1.getValue(), small2.getValue());
        Assert.assertEquals(small1.getScale(), small2.getScale());
    }

    @Test
    public void testHashCode() {
        Decimal64 a = new Decimal64(1230, 2);  // 12.30
        Decimal64 b = new Decimal64(1230, 2);  // 12.30 (same value and scale)
        Decimal64 c = new Decimal64(123, 1);   // 12.3 (same mathematical value, different scale)

        // Same value and scale should have same hash code
        Assert.assertEquals(a.hashCode(), b.hashCode());

        // Different scale means different hash code (like Decimal128)
        Assert.assertNotEquals(a.hashCode(), c.hashCode());
    }

    @Test
    public void testLargeNumbers() {
        // Test with numbers close to Long.MAX_VALUE
        Decimal64 large = new Decimal64(Long.MAX_VALUE / 1000, 0);
        Decimal64 small = new Decimal64(1, 0);

        large.add(small);
        Assert.assertEquals(Long.MAX_VALUE / 1000 + 1, large.getValue());

        // Test with very small scale
        Decimal64 smallScale = new Decimal64(123456789012345678L, 0);
        Assert.assertEquals("123456789012345678", smallScale.toString());
    }

    @Test
    public void testModulo() {
        Decimal64 a = new Decimal64(1234, 1); // 123.4
        Decimal64 b = new Decimal64(50, 1);   // 5.0
        a.modulo(b);
        Assert.assertEquals("3.4", a.toString());
    }

    @Test
    public void testModuloFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int ITERATIONS = 2_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands
            Decimal64 a = rnd.nextDecimal64();
            Decimal64 b = rnd.nextDecimal64();

            // Skip zero divisor
            if (b.isZero()) {
                continue;
            }

            // Test modulo accuracy
            testModuloAccuracy(a, b, i);
        }
    }

    @Test
    public void testModuloNegativeScale() {
        // Test -10 % 3 = -1
        BigDecimal bdA = new BigDecimal("-0.000001364898122");
        BigDecimal bdB = new BigDecimal("0.4073709550324");
        Decimal64 a = Decimal64.fromBigDecimal(bdA);
        Decimal64 b = Decimal64.fromBigDecimal(bdB);

        a.modulo(b);

        Assert.assertEquals("-0.000001364898122", a.toString());
    }

    @Test
    public void testMultiplication() {
        Decimal64 a = new Decimal64(123, 1);  // 12.3
        Decimal64 b = new Decimal64(456, 2);  // 4.56
        a.multiply(b);
        Assert.assertEquals("56.088", a.toString());
        Assert.assertEquals(3, a.getScale()); // 1 + 2 = 3
    }

    @Test
    public void testMultiplicationFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int ITERATIONS = 3_000; // Lower due to higher overflow risk

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands
            Decimal64 a = rnd.nextDecimal64();
            Decimal64 b = rnd.nextDecimal64();

            // Test multiplication accuracy
            testMultiplicationAccuracy(a, b, i);
        }
    }

    @Test
    public void testNegation() {
        Decimal64 positive = new Decimal64(12345, 2);
        positive.negate();
        Assert.assertEquals(-12345, positive.getValue());
        Assert.assertTrue(positive.isNegative());

        positive.negate();
        Assert.assertEquals(12345, positive.getValue());
        Assert.assertFalse(positive.isNegative());
    }

    @Test
    public void testNegativeNumbers() {
        Decimal64 negative = new Decimal64(-12345, 2);
        Assert.assertEquals("-123.45", negative.toString());
        Assert.assertTrue(negative.isNegative());

        Decimal64 positive = new Decimal64(6789, 2);
        negative.add(positive);
        Assert.assertEquals("-55.56", negative.toString());
        Assert.assertTrue(negative.isNegative());
    }

    @Test(expected = NumericException.class)
    public void testNullBigDecimal() {
        Decimal64.fromBigDecimal(null);
    }

    @Test
    public void testOverflowHandling() {
        // Test addition overflow
        Decimal64 max = new Decimal64(Long.MAX_VALUE, 0);
        Decimal64 one = new Decimal64(1, 0);

        try {
            max.add(one);
            Assert.fail("Should have thrown NumericException");
        } catch (NumericException e) {
            // Expected overflow
        }

        // Test multiplication overflow
        Decimal64 large = new Decimal64(Long.MAX_VALUE / 2 + 1, 0);
        Decimal64 two = new Decimal64(2, 0);

        try {
            large.multiply(two);
            Assert.fail("Should have thrown NumericException");
        } catch (NumericException e) {
            // Expected overflow
        }
    }

    @Test
    public void testPrecisionLimits() {
        // Test maximum scale
        Decimal64 maxScale = new Decimal64(1, Decimal64.MAX_SCALE);
        Assert.assertEquals(Decimal64.MAX_SCALE, maxScale.getScale());

        // Test that operations maintain precision within limits
        Decimal64 a = new Decimal64(1, 9);   // 0.000000001 (scale 9)
        Decimal64 b = new Decimal64(1, 9);   // 0.000000001 (scale 9)
        a.add(b);
        Assert.assertEquals(9, a.getScale());
        Assert.assertEquals("2E-9", a.toString()); // BigDecimal uses scientific notation for very small numbers
    }

    @Test
    public void testRound() {
        // Test basic rounding from scale 3 to scale 2
        Decimal64 a = Decimal64.fromDouble(1.234, 3);
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

        Decimal64 decimal = new Decimal64();
        for (int i = 0; i < iterations; i++) {
            // Generate random decimal with varying characteristics
            rnd.nextDecimal64(decimal);

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
            Decimal64 testDecimal = new Decimal64();
            testDecimal.copyFrom(decimal);

            // Get the original BigDecimal representation
            java.math.BigDecimal originalBigDecimal;
            try {
                originalBigDecimal = decimal.toBigDecimal();
            } catch (NumberFormatException e) {
                String errorMsg = String.format(
                        "Failed to convert original Decimal64 to BigDecimal at iteration %d:\n" +
                                "Decimal64: value=0x%016x, scale=%d\n" +
                                "toString()=%s\n" +
                                "Error: %s",
                        i, decimal.getValue(), decimal.getScale(),
                        decimal, e.getMessage()
                );
                Assert.fail(errorMsg);
                return; // unreachable but makes compiler happy
            }

            try {
                // Apply rounding to our Decimal64
                testDecimal.round(targetScale, roundingMode);

                // Apply same rounding to BigDecimal as oracle
                java.math.BigDecimal expectedBigDecimal = originalBigDecimal.setScale(targetScale, roundingMode);

                // Compare results
                java.math.BigDecimal actualBigDecimal;
                try {
                    actualBigDecimal = testDecimal.toBigDecimal();
                } catch (NumberFormatException e) {
                    String errorMsg = String.format(
                            "Failed to convert result Decimal64 to BigDecimal at iteration %d:\n" +
                                    "Original: %s (scale=%d)\n" +
                                    "Target scale: %d, Mode: %s\n" +
                                    "Result Decimal64: value=0x%016x, scale=%d\n" +
                                    "toString()=%s\n" +
                                    "Error: %s",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            testDecimal.getValue(), testDecimal.getScale(),
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
                                    "Original Decimal64: value=0x%016x, scale=%d",
                            i,
                            originalBigDecimal.toPlainString(), decimal.getScale(),
                            targetScale, roundingMode,
                            expectedBigDecimal.toPlainString(),
                            actualBigDecimal.toPlainString(),
                            decimal.getValue(), decimal.getScale()
                    );
                    Assert.fail(errorMsg);
                }

                // Verify the scale is set correctly
                Assert.assertEquals("Scale should match target scale", targetScale, testDecimal.getScale());

            } catch (NumericException e) {
                // BigDecimal might throw NumericException in some cases
                // In such cases, our implementation should either handle it gracefully
                // or throw the same exception
                boolean decimal64Threw = false;
                try {
                    testDecimal.round(targetScale, roundingMode);
                } catch (NumericException e2) {
                    decimal64Threw = true;
                }

                if (!decimal64Threw) {
                    String errorMsg = String.format(
                            "BigDecimal threw NumericException but Decimal64 didn't at iteration %d:\n" +
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
    public void testRoundZero() {
        Decimal64 a = Decimal64.fromDouble(0, 3);
        a.round(0, java.math.RoundingMode.HALF_UP);
        Assert.assertEquals(0, a.getValue());
    }

    @Test
    public void testScaleValidation() {
        try {
            new Decimal64(123, -1);
            Assert.fail("Should have thrown NumericException");
        } catch (NumericException e) {
            // Expected
        }

        try {
            new Decimal64(123, Decimal64.MAX_SCALE + 1);
            Assert.fail("Should have thrown NumericException");
        } catch (NumericException e) {
            // Expected
        }
    }

    @Test
    public void testStaticOperations() {
        Decimal64 a = new Decimal64(123, 1);
        Decimal64 b = new Decimal64(456, 2);
        Decimal64 result = new Decimal64();

        // Test static add
        Decimal64.add(a, b, result);
        Assert.assertEquals("16.86", result.toString());

        // Test static subtract
        Decimal64.subtract(a, b, result);
        Assert.assertEquals("7.74", result.toString());

        // Test static multiply
        Decimal64.multiply(a, b, result);
        Assert.assertEquals("56.088", result.toString());

        // Test static divide
        Decimal64.divide(a, b, result, 2, RoundingMode.HALF_UP);
        Assert.assertEquals("2.70", result.toString());

        // Test static negate
        Decimal64.negate(a, result);
        Assert.assertEquals("-12.3", result.toString());
    }

    @Test(expected = NumericException.class)
    public void testSubtractOverflow() {
        Decimal64 a = new Decimal64(Long.MAX_VALUE, 0);
        Decimal64 b = new Decimal64(Long.MIN_VALUE + 1, 0);
        a.subtract(b);
    }

    @Test
    public void testSubtraction() {
        // Same scale subtraction
        Decimal64 a = new Decimal64(12345, 2); // 123.45
        Decimal64 b = new Decimal64(6789, 2);  // 67.89
        a.subtract(b);
        Assert.assertEquals("55.56", a.toString());

        // Different scale subtraction
        Decimal64 c = new Decimal64(123, 1);   // 12.3
        Decimal64 d = new Decimal64(4567, 3);  // 4.567
        c.subtract(d);
        Assert.assertEquals("7.733", c.toString());
        Assert.assertEquals(3, c.getScale());
    }

    @Test
    public void testSubtractionFuzz() {
        Rnd rnd = TestUtils.generateRandom(null);
        final int ITERATIONS = 5_000;

        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random operands
            Decimal64 a = rnd.nextDecimal64();
            Decimal64 b = rnd.nextDecimal64();

            // Test subtraction accuracy
            testSubtractionAccuracy(a, b, i);
        }
    }

    @Test
    public void testToSink() {
        Decimal64 value = new Decimal64(12345, 2);
        StringSink sink = new StringSink();
        value.toSink(sink);
        Assert.assertEquals("123.45", sink.toString());
    }

    // Helper methods for fuzz testing accuracy

    private void testAdditionAccuracy(Decimal64 a, Decimal64 b, int iteration) {
        // Test addition accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference addition
        BigDecimal expected = bigA.add(bigB);

        BigDecimal min = Decimal64.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal64.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing, check that we are throwing an error as expected
            Decimal64 result = new Decimal64();

            Assert.assertThrows(NumericException.class, () -> Decimal64.add(a, b, result));
            return;
        }

        // catch overflow exceptions
        try {
            Decimal64 staticResult = new Decimal64();

            // Test static add method
            Decimal64.add(a, b, staticResult);

            Decimal64 result = new Decimal64();
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
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                // This is expected for cases where intermediate calculations would exceed 64-bit capacity
                return;
            }
            // Re-throw other arithmetic exceptions
            throw e;
        }
    }

    private void testDivisionAccuracy(Decimal64 a, Decimal64 b, int iteration) {
        // Choose a reasonable result scale - use the maximum of the two operand scales + 2
        final int resultScale = Math.min(Math.max(a.getScale(), b.getScale()) + 2, Decimal64.MAX_SCALE);

        // Test division accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        try {
            // Perform reference division
            BigDecimal expected = bigA.divide(bigB, resultScale, RoundingMode.HALF_UP);

            BigDecimal min = Decimal64.MIN_VALUE.toBigDecimal();
            BigDecimal max = Decimal64.MAX_VALUE.toBigDecimal();
            if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
                // We must be overflowing
                Decimal64 result = new Decimal64();
                Assert.assertThrows(NumericException.class, () -> Decimal64.divide(a, b, result, resultScale, RoundingMode.HALF_UP));
                return;
            }

            Decimal64 staticResult = new Decimal64();
            Decimal64.divide(a, b, staticResult, resultScale, RoundingMode.HALF_UP);

            Decimal64 result = new Decimal64();
            result.copyFrom(a);
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
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow") ||
                    e.getMessage().contains("Scale adjustment too large")) {
                return;
            }
            throw e;
        }
    }

    private void testModuloAccuracy(Decimal64 a, Decimal64 b, int iteration) {
        // Test modulo accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        try {
            // Perform reference modulo
            BigDecimal expected = bigA.remainder(bigB);

            BigDecimal min = Decimal64.MIN_VALUE.toBigDecimal();
            BigDecimal max = Decimal64.MAX_VALUE.toBigDecimal();
            if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
                // We must be overflowing
                Decimal64 result = new Decimal64();
                Assert.assertThrows(NumericException.class, () -> Decimal64.modulo(a, b, result));
                return;
            }

            Decimal64 staticResult = new Decimal64();
            Decimal64.modulo(a, b, staticResult);

            Decimal64 result = new Decimal64();
            result.copyFrom(a);
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
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                return;
            }
            throw e;
        }
    }

    private void testMultiplicationAccuracy(Decimal64 a, Decimal64 b, int iteration) {
        // Test multiplication accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference multiplication
        BigDecimal expected = bigA.multiply(bigB);

        BigDecimal min = Decimal64.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal64.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing
            Decimal64 result = new Decimal64();
            Assert.assertThrows(NumericException.class, () -> Decimal64.multiply(a, b, result));
            return;
        }

        try {
            Decimal64 staticResult = new Decimal64();
            Decimal64.multiply(a, b, staticResult);

            Decimal64 result = new Decimal64();
            result.copyFrom(a);
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
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                return;
            }
            throw e;
        }
    }

    private void testSubtractionAccuracy(Decimal64 a, Decimal64 b, int iteration) {
        // Test subtraction accuracy with BigDecimal
        BigDecimal bigA = a.toBigDecimal();
        BigDecimal bigB = b.toBigDecimal();

        // Perform reference subtraction
        BigDecimal expected = bigA.subtract(bigB);

        BigDecimal min = Decimal64.MIN_VALUE.toBigDecimal();
        BigDecimal max = Decimal64.MAX_VALUE.toBigDecimal();
        if (expected.compareTo(min) < 0 || expected.compareTo(max) > 0) {
            // We must be overflowing
            Decimal64 result = new Decimal64();
            Assert.assertThrows(NumericException.class, () -> Decimal64.subtract(a, b, result));
            return;
        }

        try {
            Decimal64 staticResult = new Decimal64();
            Decimal64.subtract(a, b, staticResult);

            Decimal64 result = new Decimal64();
            result.copyFrom(a);
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
            if (e.getMessage().contains("overflow") || e.getMessage().contains("Overflow")) {
                return;
            }
            throw e;
        }
    }
}