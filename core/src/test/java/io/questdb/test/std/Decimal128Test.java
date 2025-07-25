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
import org.junit.Assert;
import org.junit.Test;

public class Decimal128Test {

    @Test
    public void testAddDifferentScales() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(6.789, 3);
        Decimal128 result = a.add(b);

        Assert.assertEquals(3, result.getScale());
        Assert.assertEquals(130.239, result.toDouble(), 0.001);
    }

    @Test
    public void testAddLargeNumbers() {
        Decimal128 a = Decimal128.fromLong(Long.MAX_VALUE - 1000, 0);
        Decimal128 b = Decimal128.fromLong(999, 0);
        Decimal128 result = a.add(b);

        Assert.assertEquals((double) (Long.MAX_VALUE - 1), result.toDouble(), 1.0);
    }

    @Test
    public void testAddNegativeNumbers() {
        Decimal128 a = Decimal128.fromDouble(-50.25, 2);
        Decimal128 b = Decimal128.fromDouble(-30.75, 2);
        Decimal128 result = a.add(b);

        Assert.assertEquals(-81.0, result.toDouble(), 0.01);
    }

    @Test
    public void testAddPositiveAndNegative() {
        Decimal128 positive = Decimal128.fromDouble(100.00, 2);
        Decimal128 negative = Decimal128.fromDouble(-30.50, 2);
        Decimal128 result = positive.add(negative);

        Assert.assertEquals(69.50, result.toDouble(), 0.01);
    }

    @Test
    public void testAddSameScale() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(67.89, 2);
        Decimal128 result = a.add(b);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals(191.34, result.toDouble(), 0.01);
    }

    @Test
    public void testAddWithCarry() {
        Decimal128 a = new Decimal128(0L, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 b = Decimal128.fromLong(1, 0);
        Decimal128 result = a.add(b);

        Assert.assertEquals(1L, result.getHigh());
        Assert.assertEquals(0L, result.getLow());
    }

    @Test
    public void testAddWithZero() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 zero = Decimal128.fromLong(0, 2);
        Decimal128 result = a.add(zero);

        Assert.assertEquals(123.45, result.toDouble(), 0.01);
    }

    @Test
    public void testAlternatingAddSubtract() {
        Decimal128 base = Decimal128.fromLong(1000, 0);
        Decimal128 delta = Decimal128.fromLong(1, 0);

        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0) {
                base = base.add(delta);
            } else {
                base = base.subtract(delta);
            }
        }

        Assert.assertEquals(1000.0, base.toDouble(), 0.1);
    }

    @Test
    public void testArithmeticOverflowSafety() {
        Decimal128 large = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 small = Decimal128.fromLong(1, 0);

        Decimal128 overflowResult = large.add(small);
        Assert.assertNotNull(overflowResult);

        Decimal128 underflowResult = large.negate().subtract(small);
        Assert.assertNotNull(underflowResult);
    }

    @Test
    public void testBitOperations() {
        Decimal128 value = Decimal128.fromLong(0xAAAAAAAAAAAAAAAAL, 0);
        Decimal128 shifted = value.rescale(1);

        Assert.assertNotEquals(value.getLow(), shifted.getLow());
        Assert.assertEquals(1, shifted.getScale());
    }

    @Test
    public void testBitPatternPreservation() {
        long[] testHighs = {
                0x0000000000000000L,
                0x5555555555555555L,
                0xAAAAAAAAAAAAAAAAL,
                0xFFFFFFFFFFFFFFFFL,
                0x8000000000000000L,
                0x7FFFFFFFFFFFFFFFL
        };

        long[] testLows = {
                0x0000000000000000L,
                0x3333333333333333L,
                0xCCCCCCCCCCCCCCCCL,
                0xFFFFFFFFFFFFFFFFL,
                0x8000000000000000L,
                0x7FFFFFFFFFFFFFFFL
        };

        for (long high : testHighs) {
            for (long low : testLows) {
                Decimal128 decimal = new Decimal128(high, low, 5);
                Assert.assertEquals(high, decimal.getHigh());
                Assert.assertEquals(low, decimal.getLow());
                Assert.assertEquals(5, decimal.getScale());
            }
        }
    }

    @Test
    public void testBitShiftingAccuracy() {
        Decimal128 value = Decimal128.fromLong(0xAAAAAAAAAAAAAAAAL, 0);

        for (int i = 1; i <= 10; i++) {
            Decimal128 scaled = value.rescale(i);
            Assert.assertEquals(i, scaled.getScale());
        }
    }

    @Test
    public void testBorrowPropagation() {
        Decimal128 small = new Decimal128(0x1L, 0x0L, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = small.subtract(one);

        Assert.assertEquals(0x0L, result.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getLow());
    }

    @Test
    public void testBoundaryAddition() {
        Decimal128 almostMax = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = almostMax.add(one);

        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, result.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getLow());
    }

    @Test
    public void testCarryPropagationChain() {
        Decimal128 base = new Decimal128(0x0L, 0xFFFFFFFFFFFFFFFEL, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 two = Decimal128.fromLong(2, 0);

        // Test adding 1 twice
        Decimal128 step1 = base.add(one);
        Assert.assertEquals(0x0L, step1.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, step1.getLow());

        Decimal128 step2 = step1.add(one);
        Assert.assertEquals(0x1L, step2.getHigh());
        Assert.assertEquals(0x0L, step2.getLow());

        // Test adding 2 directly to base - should give same result
        Decimal128 directAdd = base.add(two);
        Assert.assertEquals(0x1L, directAdd.getHigh());
        Assert.assertEquals(0x0L, directAdd.getLow());

        // Verify both approaches give same result
        Assert.assertEquals(step2.getHigh(), directAdd.getHigh());
        Assert.assertEquals(step2.getLow(), directAdd.getLow());
    }

    @Test
    public void testChainedBorrow() {
        Decimal128 value = new Decimal128(0x1L, 0x0L, 0);
        Decimal128 large = new Decimal128(0x0L, 0x1L, 0);
        Decimal128 result = value.subtract(large);

        Assert.assertEquals(0x0L, result.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getLow());
    }

    @Test
    public void testChainedNegations() {
        Decimal128 original = Decimal128.fromDouble(123.456, 3);
        Decimal128 current = original;

        for (int i = 0; i < 100; i++) {
            current = current.negate();
        }

        Assert.assertEquals(original.getHigh(), current.getHigh());
        Assert.assertEquals(original.getLow(), current.getLow());
        Assert.assertEquals(original.getScale(), current.getScale());
    }

    @Test
    public void testCompareTo() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(123.46, 2);
        Decimal128 c = Decimal128.fromDouble(123.45, 2);

        Assert.assertTrue(a.compareTo(b) < 0);
        Assert.assertTrue(b.compareTo(a) > 0);
        Assert.assertEquals(0, a.compareTo(c));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareToDifferentScales() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(123.45, 3);
        a.compareTo(b);
    }

    @Test
    public void testCompareToNegative() {
        Decimal128 negative = Decimal128.fromDouble(-50.25, 2);
        Decimal128 positive = Decimal128.fromDouble(50.25, 2);

        Assert.assertTrue(negative.compareTo(positive) < 0);
        Assert.assertTrue(positive.compareTo(negative) > 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareToWithDifferentScales() {
        Decimal128 a = Decimal128.fromLong(100, 2);
        Decimal128 b = Decimal128.fromLong(100, 3);
        a.compareTo(b);
    }

    @Test
    public void testComplexArithmetic() {
        Decimal128 a = Decimal128.fromDouble(10.5, 1);
        Decimal128 b = Decimal128.fromDouble(2.25, 2);
        Decimal128 c = Decimal128.fromDouble(1.5, 1);

        Decimal128 result = a.add(b).multiply(c).subtract(Decimal128.fromDouble(5.0, 1));

        Assert.assertEquals(3, result.getScale());
        double expected = (10.5 + 2.25) * 1.5 - 5.0;
        Assert.assertEquals(expected, result.toDouble(), 0.001);
    }

    @Test
    public void testComplexNegation() {
        Decimal128 complex = new Decimal128(0xDEADBEEFCAFEBABEL, 0x1337C0DEDEADFACEL, 7);
        Decimal128 negated = complex.negate();
        Decimal128 doubleNegated = negated.negate();

        Assert.assertEquals(complex.getHigh(), doubleNegated.getHigh());
        Assert.assertEquals(complex.getLow(), doubleNegated.getLow());
        Assert.assertEquals(complex.getScale(), doubleNegated.getScale());
    }

    @Test
    public void testCompoundOperations() {
        Decimal128 base = Decimal128.fromDouble(2.5, 1);

        Decimal128 squared = base.multiply(base);
        Decimal128 cubed = squared.multiply(base);

        Assert.assertEquals(15.625, cubed.toDouble(), 0.001);
    }

    @Test
    public void testConsecutiveOperations() {
        Decimal128 value = Decimal128.fromLong(1, 0);

        for (int i = 0; i < 100; i++) {
            value = value.add(Decimal128.fromLong(1, 0));
        }

        Assert.assertEquals(101.0, value.toDouble(), 0.1);
    }

    @Test
    public void testConstructor() {
        Decimal128 decimal = new Decimal128(0x1234567890ABCDEFL, 0xFEDCBA0987654321L, 3);
        Assert.assertEquals(0x1234567890ABCDEFL, decimal.getHigh());
        Assert.assertEquals(0xFEDCBA0987654321L, decimal.getLow());
        Assert.assertEquals(3, decimal.getScale());
    }

    @Test
    public void testDivide() {
        Decimal128 dividend = Decimal128.fromDouble(100.0, 2);
        Decimal128 divisor = Decimal128.fromDouble(4.0, 1);
        Decimal128 result = dividend.divide(divisor, 2);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals(25.0, result.toDouble(), 0.01);
    }

    @Test
    public void testDivideByOne() {
        Decimal128 dividend = Decimal128.fromDouble(123.45, 2);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = dividend.divide(one, 2);

        Assert.assertEquals(123.45, result.toDouble(), 0.01);
    }

    @Test(expected = ArithmeticException.class)
    public void testDivideByZero() {
        Decimal128 dividend = Decimal128.fromDouble(100.0, 2);
        Decimal128 zero = Decimal128.fromLong(0, 1);
        dividend.divide(zero, 2);
    }

    @Test(expected = ArithmeticException.class)
    public void testDivideByZeroThrowsException() {
        Decimal128 dividend = Decimal128.fromLong(100, 2);
        Decimal128 zero = Decimal128.fromLong(0, 2);
        dividend.divide(zero, 2);
    }

    @Test
    public void testDivideNegative() {
        Decimal128 dividend = Decimal128.fromDouble(-100.0, 2);
        Decimal128 divisor = Decimal128.fromDouble(4.0, 1);
        Decimal128 result = dividend.divide(divisor, 2);

        Assert.assertEquals(-25.0, result.toDouble(), 0.01);
    }

    @Test
    public void testDivideTwoNegatives() {
        Decimal128 dividend = Decimal128.fromDouble(-100.0, 2);
        Decimal128 divisor = Decimal128.fromDouble(-4.0, 1);
        Decimal128 result = dividend.divide(divisor, 2);

        Assert.assertEquals(25.0, result.toDouble(), 0.01);
    }

    @Test
    public void testDivideWithRemainder() {
        Decimal128 dividend = Decimal128.fromDouble(100.0, 2);
        Decimal128 divisor = Decimal128.fromDouble(3.0, 1);
        Decimal128 result = dividend.divide(divisor, 4);

        Assert.assertEquals(4, result.getScale());
        Assert.assertEquals(33.3333, result.toDouble(), 0.0001);
    }

    @Test
    public void testDivisionByVerySmallNumber() {
        Decimal128 dividend = Decimal128.fromLong(1000000, 0);
        Decimal128 divisor = Decimal128.fromLong(1, 10);
        Decimal128 result = dividend.divide(divisor, 5);

        Assert.assertTrue(result.toDouble() > 1e15);
    }

    @Test
    public void testDivisionPrecisionLoss() {
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 three = Decimal128.fromLong(3, 0);

        for (int precision = 1; precision <= 15; precision++) {
            Decimal128 result = one.divide(three, precision);
            Assert.assertEquals(precision, result.getScale());

            double expected = 1.0 / 3.0;
            double actual = result.toDouble();
            Assert.assertEquals(expected, actual, Math.pow(10, -precision));
        }
    }

    @Test
    public void testDivisionResultingInZero() {
        Decimal128 tiny = Decimal128.fromLong(1, 20);
        Decimal128 huge = Decimal128.fromLong(1000000000L, 0);
        Decimal128 result = tiny.divide(huge, 2);

        Assert.assertTrue(result.isZero() || Math.abs(result.toDouble()) < 1e-10);
    }

    @Test
    public void testDoubleCarryScenario() {
        Decimal128 value1 = new Decimal128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFEL, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = value1.add(one);

        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getLow());
    }

    @Test
    public void testEdgeCaseRounding() {
        double value = 0.9999999999999995;
        Decimal128 decimal = Decimal128.fromDouble(value, 15);

        double result = decimal.toDouble();
        Assert.assertEquals(value, result, 0.000000000000001);
    }

    @Test
    public void testEquals() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(123.45, 2);
        Decimal128 c = Decimal128.fromDouble(123.46, 2);
        Decimal128 d = Decimal128.fromDouble(123.45, 3);

        Assert.assertEquals(a, b);
        Assert.assertNotEquals(a, c);
        Assert.assertNotEquals(a, d);
        Assert.assertNotEquals(null, a);
        Assert.assertNotEquals("123.45", a);
    }

    @Test
    public void testEqualsContractViolations() {
        Decimal128 a = Decimal128.fromLong(100, 2);

        Assert.assertNotEquals(null, a);
        Assert.assertNotEquals("100.00", a);
        Assert.assertNotEquals(100, a);
    }

    @Test
    public void testExtremeCasesToString() {
        Decimal128[] extremeCases = {
                new Decimal128(0, 0, 0),
                new Decimal128(-1, -1, 20),
                new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0),
                new Decimal128(0x8000000000000000L, 0x0000000000000000L, 15)
        };

        for (Decimal128 decimal : extremeCases) {
            String str = decimal.toString();
            Assert.assertNotNull(str);
            Assert.assertFalse(str.isEmpty());
        }
    }

    @Test
    public void testExtremeDivision() {
        Decimal128 tiny = Decimal128.fromLong(1, 10);
        Decimal128 huge = Decimal128.fromLong(1000000000L, 0);

        Decimal128 result = huge.divide(tiny, 5);
        Assert.assertEquals(1e19, result.toDouble(), 1e14);
    }

    @Test
    public void testExtremeMultiplication() {
        Decimal128 large = new Decimal128(0x0000000000000001L, 0x0000000000000000L, 0);
        Decimal128 factor = Decimal128.fromLong(2, 0);
        Decimal128 result = large.multiply(factor);

        Assert.assertEquals(0x0000000000000002L, result.getHigh());
        Assert.assertEquals(0x0000000000000000L, result.getLow());
    }

    @Test
    public void testExtremeNegativeNumbers() {
        Decimal128 extremeNeg = new Decimal128(0x8000000000000000L, 0x0000000000000001L, 0);
        Assert.assertTrue(extremeNeg.isNegative());

        Decimal128 negated = extremeNeg.negate();
        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, negated.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, negated.getLow());
    }

    @Test
    public void testFloatingPointBoundaries() {
        double[] testValues = {
                1e-5,
                1e5,
                -1e5,
                123.456,
                -987.654
        };

        for (double val : testValues) {
            if (Double.isFinite(val) && Math.abs(val) < 1e15) {
                Decimal128 decimal = Decimal128.fromDouble(val, 8);
                double recovered = decimal.toDouble();
                Assert.assertEquals(val, recovered, Math.abs(val) * 1e-3);
            }
        }
    }

    @Test
    public void testFromDoubleNegative() {
        Decimal128 decimal = Decimal128.fromDouble(-98.76, 2);
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(-98.76, decimal.toDouble(), 0.01);
    }

    @Test
    public void testFromDoublePositive() {
        Decimal128 decimal = Decimal128.fromDouble(123.456, 3);
        Assert.assertEquals(3, decimal.getScale());
        Assert.assertEquals(123.456, decimal.toDouble(), 0.001);
    }

    @Test
    public void testFromDoubleVeryLarge() {
        double largeValue = 1e15;
        Decimal128 decimal = Decimal128.fromDouble(largeValue, 0);
        Assert.assertEquals(largeValue, decimal.toDouble(), largeValue * 0.000001);
    }

    @Test
    public void testFromDoubleVerySmall() {
        double smallValue = 0.000001;
        Decimal128 decimal = Decimal128.fromDouble(smallValue, 6);
        Assert.assertEquals(smallValue, decimal.toDouble(), 0.0000001);
    }

    @Test
    public void testFromDoubleZero() {
        Decimal128 decimal = Decimal128.fromDouble(0.0, 4);
        Assert.assertTrue(decimal.isZero());
        Assert.assertEquals(0.0, decimal.toDouble(), 0.0001);
    }

    @Test
    public void testFromLongMaxValue() {
        Decimal128 decimal = Decimal128.fromLong(Long.MAX_VALUE, 0);
        Assert.assertEquals(0L, decimal.getHigh());
        Assert.assertEquals(Long.MAX_VALUE, decimal.getLow());
        Assert.assertEquals(0, decimal.getScale());
        Assert.assertEquals((double) Long.MAX_VALUE, decimal.toDouble(), 1.0);
    }

    @Test
    public void testFromLongMinValue() {
        Decimal128 decimal = Decimal128.fromLong(Long.MIN_VALUE, 0);
        Assert.assertEquals(-1L, decimal.getHigh());
        Assert.assertEquals(Long.MIN_VALUE, decimal.getLow());
        Assert.assertEquals(0, decimal.getScale());
        Assert.assertEquals((double) Long.MIN_VALUE, decimal.toDouble(), 1.0);
    }

    @Test
    public void testFromLongNegative() {
        Decimal128 decimal = Decimal128.fromLong(-12345L, 2);
        Assert.assertEquals(-1L, decimal.getHigh());
        Assert.assertEquals(-12345L, decimal.getLow());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(-123.45, decimal.toDouble(), 0.000001);
    }

    @Test
    public void testFromLongPositive() {
        Decimal128 decimal = Decimal128.fromLong(12345L, 2);
        Assert.assertEquals(0L, decimal.getHigh());
        Assert.assertEquals(12345L, decimal.getLow());
        Assert.assertEquals(2, decimal.getScale());
        Assert.assertEquals(123.45, decimal.toDouble(), 0.000001);
    }

    @Test
    public void testFromLongZero() {
        Decimal128 decimal = Decimal128.fromLong(0L, 5);
        Assert.assertEquals(0L, decimal.getHigh());
        Assert.assertEquals(0L, decimal.getLow());
        Assert.assertEquals(5, decimal.getScale());
        Assert.assertEquals(0.0, decimal.toDouble(), 0.000001);
    }

    @Test
    public void testFuzzyArithmetic() {
        java.util.Random random = new java.util.Random(54321);

        for (int i = 0; i < 500; i++) {
            long a_high = random.nextLong() >>> 1;
            long a_low = random.nextLong();
            int a_scale = random.nextInt(10);

            long b_high = random.nextLong() >>> 1;
            long b_low = random.nextLong();
            int b_scale = random.nextInt(10);

            Decimal128 a = new Decimal128(a_high, a_low, a_scale);
            Decimal128 b = new Decimal128(b_high, b_low, b_scale);

            if (!b.isZero()) {
                Decimal128 sum = a.add(b);
                Assert.assertNotNull(sum);

                Decimal128 product = a.multiply(b);
                Assert.assertNotNull(product);

                Decimal128 quotient = a.divide(b, Math.max(a_scale, b_scale));
                Assert.assertNotNull(quotient);

            }
        }
    }

    @Test
    public void testHashCode() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(123.45, 2);

        Assert.assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testHashCodeConsistency() {
        Decimal128 a = Decimal128.fromLong(12345, 3);
        Decimal128 b = new Decimal128(a.getHigh(), a.getLow(), a.getScale());

        Assert.assertEquals(a.hashCode(), b.hashCode());
        Assert.assertEquals(a, b);

        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(a.hashCode(), a.hashCode());
        }
    }

    @Test
    public void testHighPrecisionCalculation() {
        Decimal128 a = Decimal128.fromDouble(1.0 / 3.0, 15);
        Decimal128 three = Decimal128.fromLong(3, 0);
        Decimal128 result = a.multiply(three);

        Assert.assertEquals(1.0, result.toDouble(), 0.000000000000001);
    }

    @Test
    public void testInfiniteScaling() {
        Decimal128 value = Decimal128.fromLong(1, 0);

        for (int i = 1; i <= 10; i++) {
            value = value.rescale(i);
            Assert.assertEquals(i, value.getScale());
            Assert.assertEquals(1.0, value.toDouble(), 0.0000000001);
        }
    }

    @Test
    public void testIntegerBoundaryConditions() {
        Decimal128 maxInt = Decimal128.fromLong(Integer.MAX_VALUE, 0);
        Decimal128 minInt = Decimal128.fromLong(Integer.MIN_VALUE, 0);

        Assert.assertEquals(Integer.MAX_VALUE, maxInt.toDouble(), 1.0);
        Assert.assertEquals(Integer.MIN_VALUE, minInt.toDouble(), 1.0);

        Decimal128 sum = maxInt.add(minInt);
        Assert.assertEquals(-1.0, sum.toDouble(), 1.0);
    }

    @Test
    public void testInvalidDoubleInputs() {
        Decimal128 fromNaN = Decimal128.fromDouble(Double.NaN, 2);
        Assert.assertNotNull(fromNaN);

        Decimal128 fromInfinity = Decimal128.fromDouble(Double.POSITIVE_INFINITY, 2);
        Assert.assertNotNull(fromInfinity);

        Decimal128 fromNegInfinity = Decimal128.fromDouble(Double.NEGATIVE_INFINITY, 2);
        Assert.assertNotNull(fromNegInfinity);
    }

    @Test
    public void testIsNegative() {
        Assert.assertTrue(Decimal128.fromLong(-1, 2).isNegative());
        Assert.assertTrue(Decimal128.fromDouble(-0.01, 2).isNegative());
        Assert.assertFalse(Decimal128.fromLong(0, 2).isNegative());
        Assert.assertFalse(Decimal128.fromLong(1, 2).isNegative());
        Assert.assertFalse(Decimal128.fromDouble(0.01, 2).isNegative());
    }

    @Test
    public void testIsZero() {
        Assert.assertTrue(Decimal128.fromLong(0, 5).isZero());
        Assert.assertTrue(new Decimal128(0, 0, 10).isZero());
        Assert.assertFalse(Decimal128.fromLong(1, 5).isZero());
        Assert.assertFalse(Decimal128.fromLong(-1, 5).isZero());
    }

    @Test
    public void testMalformedOperations() {
        Decimal128 value = Decimal128.fromLong(100, 2);

        Decimal128 result1 = value.add(value);
        Assert.assertNotNull(result1);

        Decimal128 result2 = value.multiply(value);
        Assert.assertNotNull(result2);

        Decimal128 result3 = value.subtract(value);
        Assert.assertTrue(result3.isZero());
    }

    @Test
    public void testMaximumNegativeValue() {
        Decimal128 maxNeg = new Decimal128(0x8000000000000000L, 0x0000000000000000L, 0);
        Assert.assertTrue(maxNeg.isNegative());
        Assert.assertFalse(maxNeg.isZero());
        Assert.assertTrue(maxNeg.toDouble() < 0);
    }

    @Test
    public void testMaximumPositiveValue() {
        Decimal128 maxPos = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0);
        Assert.assertFalse(maxPos.isNegative());
        Assert.assertFalse(maxPos.isZero());
        Assert.assertTrue(maxPos.toDouble() > 0);
    }

    @Test
    public void testMemoryLayout() {
        long testHigh = 0x123456789ABCDEF0L;
        long testLow = 0xFEDCBA0987654321L;
        int testScale = 42;

        Decimal128 decimal = new Decimal128(testHigh, testLow, testScale);

        Assert.assertEquals(testHigh, decimal.getHigh());
        Assert.assertEquals(testLow, decimal.getLow());
        Assert.assertEquals(testScale, decimal.getScale());
    }

    @Test
    public void testModulo() {
        Decimal128 dividend = Decimal128.fromDouble(100.5, 1);
        Decimal128 divisor = Decimal128.fromDouble(30.0, 1);
        Decimal128 result = dividend.modulo(divisor);

        Assert.assertEquals(10.5, result.toDouble(), 0.1);
    }

    @Test(expected = ArithmeticException.class)
    public void testModuloByZero() {
        Decimal128 dividend = Decimal128.fromDouble(100.0, 2);
        Decimal128 zero = Decimal128.fromLong(0, 1);
        dividend.modulo(zero);
    }

    @Test(expected = ArithmeticException.class)
    public void testModuloByZeroThrowsException() {
        Decimal128 value = Decimal128.fromLong(100, 2);
        Decimal128 zero = Decimal128.fromLong(0, 2);
        value.modulo(zero);
    }

    @Test
    public void testModuloExactDivision() {
        Decimal128 dividend = Decimal128.fromDouble(100.0, 1);
        Decimal128 divisor = Decimal128.fromDouble(25.0, 1);
        Decimal128 result = dividend.modulo(divisor);

        Assert.assertTrue(result.isZero());
    }

    @Test
    public void testModuloNegativeDividend() {
        Decimal128 dividend = Decimal128.fromDouble(-100.5, 1);
        Decimal128 divisor = Decimal128.fromDouble(30.0, 1);
        Decimal128 result = dividend.modulo(divisor);

        Assert.assertEquals(-10.5, result.toDouble(), 0.1);
    }

    @Test
    public void testModuloNegativeDivisor() {
        Decimal128 dividend = Decimal128.fromDouble(100.5, 1);
        Decimal128 divisor = Decimal128.fromDouble(-30.0, 1);
        Decimal128 result = dividend.modulo(divisor);

        Assert.assertEquals(10.5, result.toDouble(), 0.1);
    }

    @Test
    public void testModuloWithNegatives() {
        Decimal128 negDividend = Decimal128.fromLong(-100, 1);
        Decimal128 posDivisor = Decimal128.fromLong(30, 1);
        Decimal128 result1 = negDividend.modulo(posDivisor);
        Assert.assertTrue(result1.isNegative() || result1.isZero());

        Decimal128 posDividend = Decimal128.fromLong(100, 1);
        Decimal128 negDivisor = Decimal128.fromLong(-30, 1);
        Decimal128 result2 = posDividend.modulo(negDivisor);
        Assert.assertFalse(result2.isNegative());

        Decimal128 negBoth1 = Decimal128.fromLong(-100, 1);
        Decimal128 negBoth2 = Decimal128.fromLong(-30, 1);
        Decimal128 result3 = negBoth1.modulo(negBoth2);
        Assert.assertTrue(result3.isNegative() || result3.isZero());
    }

    @Test
    public void testModuloWithTinyRemainder() {
        Decimal128 dividend = Decimal128.fromLong(1000001, 6);
        Decimal128 divisor = Decimal128.fromLong(1, 0);
        Decimal128 result = dividend.modulo(divisor);

        Assert.assertEquals(0.000001, result.toDouble(), 0.0000001);
    }

    @Test
    public void testMultipleRescaling() {
        Decimal128 original = Decimal128.fromLong(5, 0);
        Decimal128 rescaled = original.rescale(5);

        Assert.assertEquals(500000, rescaled.getLow());
        Assert.assertEquals(5, rescaled.getScale());
        Assert.assertEquals(5.0, rescaled.toDouble(), 0.00001);
    }

    @Test
    public void testMultiplicationByZeroEdgeCases() {
        Decimal128[] testValues = {
                new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 5),
                new Decimal128(0x8000000000000000L, 0x0000000000000000L, 10),
                Decimal128.fromDouble(Math.PI, 15),
                Decimal128.fromLong(-987654321, 8)
        };

        Decimal128 zero = Decimal128.fromLong(0, 3);

        for (Decimal128 value : testValues) {
            Decimal128 result = value.multiply(zero);
            Assert.assertNotNull(result);
        }
    }

    @Test
    public void testMultiplicationOverflow() {
        Decimal128 large1 = new Decimal128(0x1000000000000000L, 0x0000000000000000L, 0);
        Decimal128 large2 = new Decimal128(0x0000000000000010L, 0x0000000000000000L, 0);
        Decimal128 result = large1.multiply(large2);

        Assert.assertEquals(0x0000000000000000L, result.getHigh());
        Assert.assertEquals(0x0000000000000000L, result.getLow());
    }

    @Test
    public void testMultiplyBy10() {
        Decimal128 original = Decimal128.fromLong(123, 0);
        Decimal128 rescaled = original.rescale(1);

        Assert.assertEquals(1230, rescaled.getLow());
        Assert.assertEquals(1, rescaled.getScale());
    }

    @Test
    public void testMultiplyByOne() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = a.multiply(one);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals(123.45, result.toDouble(), 0.01);
    }

    @Test
    public void testMultiplyByZero() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 zero = Decimal128.fromLong(0, 1);
        Decimal128 result = a.multiply(zero);

        Assert.assertTrue(result.isZero());
    }

    @Test
    public void testMultiplyDifferentScales() {
        Decimal128 a = Decimal128.fromDouble(12.34, 2);
        Decimal128 b = Decimal128.fromDouble(5.678, 3);
        Decimal128 result = a.multiply(b);

        Assert.assertEquals(5, result.getScale());
        Assert.assertEquals(70.06652, result.toDouble(), 0.00001);
    }

    @Test
    public void testMultiplyNegative() {
        Decimal128 positive = Decimal128.fromDouble(25.5, 1);
        Decimal128 negative = Decimal128.fromDouble(-4.0, 1);
        Decimal128 result = positive.multiply(negative);

        Assert.assertEquals(-102.0, result.toDouble(), 0.01);
    }

    @Test
    public void testMultiplySameScale() {
        Decimal128 a = Decimal128.fromDouble(12.5, 1);
        Decimal128 b = Decimal128.fromDouble(4.0, 1);
        Decimal128 result = a.multiply(b);

        Assert.assertEquals(2, result.getScale());
        Assert.assertEquals(50.0, result.toDouble(), 0.01);
    }

    @Test
    public void testMultiplyTwoNegatives() {
        Decimal128 a = Decimal128.fromDouble(-12.5, 1);
        Decimal128 b = Decimal128.fromDouble(-4.0, 1);
        Decimal128 result = a.multiply(b);

        Assert.assertEquals(50.0, result.toDouble(), 0.01);
    }

    @Test
    public void testNegate() {
        Decimal128 positive = Decimal128.fromDouble(123.45, 2);
        Decimal128 negative = positive.negate();

        Assert.assertEquals(-123.45, negative.toDouble(), 0.01);
        Assert.assertTrue(negative.isNegative());
    }

    @Test
    public void testNegateMaxValue() {
        Decimal128 maxValue = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 negated = maxValue.negate();

        Assert.assertTrue(negated.isNegative());
        Assert.assertEquals(0x8000000000000000L, negated.getHigh());
        Assert.assertEquals(0x0000000000000001L, negated.getLow());
    }

    @Test
    public void testNegateNegative() {
        Decimal128 negative = Decimal128.fromDouble(-123.45, 2);
        Decimal128 positive = negative.negate();

        Assert.assertEquals(123.45, positive.toDouble(), 0.01);
        Assert.assertFalse(positive.isNegative());
    }

    @Test
    public void testNegateZero() {
        Decimal128 zero = Decimal128.fromLong(0, 3);
        Decimal128 negatedZero = zero.negate();

        Assert.assertEquals(-1L, negatedZero.getHigh());
        Assert.assertEquals(0L, negatedZero.getLow());
        Assert.assertEquals(3, negatedZero.getScale());
    }

    @Test
    public void testNegationEdgeCases() {
        Decimal128 minValue = new Decimal128(0x8000000000000000L, 0x0000000000000000L, 0);
        Decimal128 negated = minValue.negate();

        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, negated.getHigh());
        Assert.assertEquals(0x0000000000000000L, negated.getLow());
    }

    @Test
    public void testNegativeCarryPropagation() {
        Decimal128 a = new Decimal128(-1L, 1L, 0);
        Decimal128 b = new Decimal128(0L, -1L, 0);
        Decimal128 result = a.add(b);

        Assert.assertEquals(0L, result.getHigh());
        Assert.assertEquals(0L, result.getLow());
    }

    @Test
    public void testNegativeScales() {
        Decimal128 invalid = new Decimal128(0, 100, -5);
        Assert.assertEquals(-5, invalid.getScale());
    }

    @Test
    public void testOperationChaining() {
        Decimal128 base = Decimal128.fromLong(10000, 2);

        Decimal128 result = base
                .add(Decimal128.fromLong(5000, 2))
                .multiply(Decimal128.fromLong(2, 0))
                .divide(Decimal128.fromLong(3, 0), 2)
                .subtract(Decimal128.fromLong(2500, 2));

        double expected = (((100.0 + 50.0) * 2.0) / 3.0) - 25.0;
        Assert.assertEquals(expected, result.toDouble(), 0.01);
    }

    @Test
    public void testOperationSymmetry() {
        Decimal128 a = Decimal128.fromDouble(123.45, 2);
        Decimal128 b = Decimal128.fromDouble(67.89, 2);

        Decimal128 sum1 = a.add(b);
        Decimal128 sum2 = b.add(a);
        Assert.assertEquals(sum1.toDouble(), sum2.toDouble(), 0.001);

        Decimal128 product1 = a.multiply(b);
        Decimal128 product2 = b.multiply(a);
        Assert.assertEquals(product1.toDouble(), product2.toDouble(), 0.001);
    }

    @Test
    public void testOperationsWithDifferentSigns() {
        Decimal128 positive = Decimal128.fromLong(1000, 2);
        Decimal128 negative = Decimal128.fromLong(-1000, 2);

        Decimal128 sum = positive.add(negative);
        Assert.assertTrue(sum.isZero());

        Decimal128 product = positive.multiply(negative);
        Assert.assertTrue(product.isNegative());
        Assert.assertEquals(-100.0, product.toDouble(), 0.01);
    }

    @Test
    public void testOverflowAddition() {
        Decimal128 max = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = max.add(one);

        Assert.assertEquals(0x8000000000000000L, result.getHigh());
        Assert.assertEquals(0x0000000000000000L, result.getLow());
        Assert.assertTrue(result.isNegative());
    }

    @Test
    public void testOverflowBehavior() {
        Decimal128 maxHigh = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);

        Decimal128 result = maxHigh.add(one);

        Assert.assertEquals(0x8000000000000000L, result.getHigh());
        Assert.assertEquals(0x0000000000000000L, result.getLow());
    }

    @Test
    public void testPrecisionLossInConversion() {
        double original = Math.PI;
        Decimal128 decimal = Decimal128.fromDouble(original, 15);
        double recovered = decimal.toDouble();

        Assert.assertEquals(original, recovered, 0.000000000000001);
    }

    @Test
    public void testRandomizedStressTest() {
        java.util.Random random = new java.util.Random(12345);

        for (int i = 0; i < 1000; i++) {
            long high = random.nextLong();
            long low = random.nextLong();
            int scale = random.nextInt(20);

            Decimal128 decimal = new Decimal128(high, low, scale);

            Assert.assertEquals(high, decimal.getHigh());
            Assert.assertEquals(low, decimal.getLow());
            Assert.assertEquals(scale, decimal.getScale());

            Decimal128 negated = decimal.negate();
            Decimal128 doubleNegated = negated.negate();

            Assert.assertEquals(decimal.getHigh(), doubleNegated.getHigh());
            Assert.assertEquals(decimal.getLow(), doubleNegated.getLow());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRescaleDown() {
        Decimal128 original = Decimal128.fromDouble(123.45, 3);
        original.rescale(2);
    }

    @Test
    public void testRescaleSame() {
        Decimal128 original = Decimal128.fromDouble(123.45, 2);
        Decimal128 rescaled = original.rescale(2);

        Assert.assertSame(original, rescaled);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRescaleToSmallerScale() {
        Decimal128 value = Decimal128.fromLong(123, 5);
        value.rescale(3);
    }

    @Test
    public void testRescaleUp() {
        Decimal128 original = Decimal128.fromDouble(123.45, 2);
        Decimal128 rescaled = original.rescale(4);

        Assert.assertEquals(4, rescaled.getScale());
        Assert.assertEquals(123.45, rescaled.toDouble(), 0.0001);
    }

    @Test
    public void testRescalingLimits() {
        Decimal128 current = Decimal128.fromLong(1, 0);
        for (int scale = 1; scale <= 50; scale++) {
            current = current.rescale(scale);
            if (current.isZero()) {
                break;
            }
        }
        Assert.assertNotNull(current);
    }

    @Test
    public void testScaleOverflow() {
        Decimal128 value = Decimal128.fromLong(Long.MAX_VALUE / 10, 0);
        Decimal128 rescaled = value.rescale(1);

        Assert.assertEquals(1, rescaled.getScale());
        Assert.assertNotNull(rescaled);
    }

    @Test
    public void testScalingStressTest() {
        Decimal128 value = Decimal128.fromLong(123456789, 0);

        for (int scale = 1; scale <= 15; scale++) {
            Decimal128 scaled = value.rescale(scale);
            Assert.assertEquals(scale, scaled.getScale());
            Assert.assertEquals(123456789.0, scaled.toDouble(), Math.pow(10, -scale));
        }
    }

    @Test
    public void testSignedArithmeticEdgeCases() {
        Decimal128 positive = new Decimal128(0x0L, 0x8000000000000000L, 0);
        Decimal128 negative = new Decimal128(0xFFFFFFFFFFFFFFFFL, 0x8000000000000000L, 0);

        Assert.assertFalse(positive.isNegative());
        Assert.assertTrue(negative.isNegative());

        Decimal128 sum = positive.add(negative);
        Assert.assertEquals(0x0L, sum.getHigh());
        Assert.assertEquals(0x0L, sum.getLow());
        Assert.assertTrue(sum.isZero());
    }

    @Test
    public void testSignedUnsignedComparison() {
        Decimal128 positive = new Decimal128(0L, 0x8000000000000000L, 0);
        Decimal128 negative = new Decimal128(-1L, 0x7FFFFFFFFFFFFFFFL, 0);

        Assert.assertFalse(positive.isNegative());
        Assert.assertTrue(negative.isNegative());
    }

    @Test
    public void testSubtract() {
        Decimal128 a = Decimal128.fromDouble(100.50, 2);
        Decimal128 b = Decimal128.fromDouble(30.25, 2);
        Decimal128 result = a.subtract(b);

        Assert.assertEquals(70.25, result.toDouble(), 0.01);
    }

    @Test
    public void testSubtractFromZero() {
        Decimal128 zero = Decimal128.fromLong(0, 2);
        Decimal128 value = Decimal128.fromDouble(50.25, 2);
        Decimal128 result = zero.subtract(value);

        Assert.assertEquals(-50.25, result.toDouble(), 0.01);
    }

    @Test
    public void testSubtractNegativeResult() {
        Decimal128 a = Decimal128.fromDouble(30.25, 2);
        Decimal128 b = Decimal128.fromDouble(100.50, 2);
        Decimal128 result = a.subtract(b);

        Assert.assertEquals(-70.25, result.toDouble(), 0.01);
    }

    @Test
    public void testSubtractionUnderflow() {
        Decimal128 minVal = new Decimal128(0x8000000000000000L, 0x0000000000000000L, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);

        Decimal128 result = minVal.subtract(one);
        Assert.assertFalse(result.isNegative());
        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, result.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getLow());
    }

    @Test
    public void testToDouble() {
        Assert.assertEquals(0.0, Decimal128.fromLong(0, 5).toDouble(), 0.0);
        Assert.assertEquals(123.45, Decimal128.fromDouble(123.45, 2).toDouble(), 0.01);
        Assert.assertEquals(-67.89, Decimal128.fromDouble(-67.89, 2).toDouble(), 0.01);
        Assert.assertEquals(0.001, Decimal128.fromLong(1, 3).toDouble(), 0.0001);
        Assert.assertEquals(1000.0, Decimal128.fromLong(1000, 0).toDouble(), 0.1);
    }

    @Test
    public void testToString() {
        Assert.assertEquals("123", Decimal128.fromLong(123, 0).toString());
        Assert.assertEquals("12.34", Decimal128.fromLong(1234, 2).toString());
        Assert.assertEquals("0.001", Decimal128.fromLong(1, 3).toString());
        Assert.assertEquals("-123", Decimal128.fromLong(-123, 0).toString());
        Assert.assertEquals("-12.34", Decimal128.fromLong(-1234, 2).toString());
    }

    @Test
    public void testToStringEdgeCases() {
        Assert.assertTrue(Decimal128.fromLong(0, 0).toString().contains("0"));
        Assert.assertTrue(Decimal128.fromLong(-1, 0).toString().contains("-1"));

        String largeString = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0).toString();
        Assert.assertNotNull(largeString);
        Assert.assertFalse(largeString.isEmpty());
    }

    @Test
    public void testTripleCarryScenario() {
        Decimal128 value1 = new Decimal128(0xFFFFFFFFFFFFFFFFL, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = value1.add(one);

        Assert.assertEquals(0x0L, result.getHigh());
        Assert.assertEquals(0x0L, result.getLow());
        Assert.assertTrue(result.isZero());
    }

    @Test
    public void testUnderflowSubtraction() {
        Decimal128 minNeg = new Decimal128(0x8000000000000000L, 0x0000000000000000L, 0);
        Decimal128 one = Decimal128.fromLong(1, 0);
        Decimal128 result = minNeg.subtract(one);

        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, result.getHigh());
        Assert.assertEquals(0xFFFFFFFFFFFFFFFFL, result.getLow());
        Assert.assertFalse(result.isNegative());
    }

    @Test
    public void testUnsignedComparisons() {
        Decimal128 largePos = new Decimal128(0x0L, 0xFFFFFFFFFFFFFFFFL, 0);
        Decimal128 smallNeg = new Decimal128(0xFFFFFFFFFFFFFFFFL, 0x0000000000000001L, 0);

        Assert.assertFalse(largePos.isNegative());
        Assert.assertTrue(smallNeg.isNegative());
    }

    @Test
    public void testVeryLargeDivision() {
        Decimal128 dividend = new Decimal128(0x0L, 0x7FFFFFFFFFFFFFFFL, 0);
        Decimal128 divisor = Decimal128.fromLong(2, 0);
        Decimal128 result = dividend.divide(divisor, 0);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.toDouble() > 0);
    }

    @Test
    public void testVeryLargeNumbers() {
        Decimal128 large1 = new Decimal128(0x7FFFFFFFFFFFFFFFL, 0x0L, 0);
        Decimal128 large2 = new Decimal128(0x0L, 0x7FFFFFFFFFFFFFFFL, 0);

        Decimal128 sum = large1.add(large2);
        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, sum.getHigh());
        Assert.assertEquals(0x7FFFFFFFFFFFFFFFL, sum.getLow());
    }

    @Test
    public void testVeryLargeScales() {
        Decimal128 value = Decimal128.fromLong(1, 0);

        for (int scale = 1; scale <= 100; scale++) {
            value = value.rescale(scale);
            Assert.assertEquals(scale, value.getScale());
        }
    }

    @Test
    public void testZeroDivision() {
        Decimal128 zero = Decimal128.fromLong(0, 2);
        Decimal128 nonZero = Decimal128.fromLong(100, 1);

        Decimal128 result = zero.divide(nonZero, 2);
        Assert.assertTrue(result.isZero());
    }

    @Test
    public void testZeroDivisionByZero() {
        Decimal128 zero1 = Decimal128.fromLong(0, 2);
        Decimal128 zero2 = Decimal128.fromLong(0, 3);

        try {
            zero1.divide(zero2, 2);
            Assert.fail("Should have thrown ArithmeticException");
        } catch (ArithmeticException e) {
            Assert.assertTrue(e.getMessage().contains("zero"));
        }
    }
}