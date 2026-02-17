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

package io.questdb.test.std.fastdouble;

import io.questdb.std.Unsafe;
import io.questdb.std.fastdouble.FastDoubleMath;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

import static io.questdb.std.fastdouble.FastDoubleMath.MANTISSA_128;
import static io.questdb.std.fastdouble.FastDoubleMath.MANTISSA_64;
import static java.lang.Long.toUnsignedString;

/**
 * Tests class {@link FastDoubleMath}.
 */
public class FastDoubleMathTest {
    @Test
    public void dynamicTestsTryDecFloatToDouble() {
        testTryDecFloatToDouble("Inside Clinger fast path \"1000000000000000000e-340\")", false, 1000000000000000000L, -325, 1000000000000000000e-325);
        testTryDecFloatToDouble("Inside Clinger fast path (max_clinger_significand, max_clinger_exponent)", false, 9007199254740991L, 22, 9007199254740991e22);
        testTryDecFloatToDouble("Outside Clinger fast path (max_clinger_significand, max_clinger_exponent + 1)", false, 9007199254740991L, 23, 9007199254740991e23);
        testTryDecFloatToDouble("Outside Clinger fast path (max_clinger_significand + 1, max_clinger_exponent)", false, 9007199254740992L, 22, 9007199254740992e22);
        testTryDecFloatToDouble("Inside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent)", false, 1L, -22, 1e-22);
        testTryDecFloatToDouble("Outside Clinger fast path (min_clinger_significand + 1, min_clinger_exponent - 1)", false, 1L, -23, 1e-23);
        testTryDecFloatToDouble("Outside Clinger fast path, bail out in semi-fast path, -8446744073709551617", false, -8446744073709551617L, 0, Double.NaN);
        testTryDecFloatToDouble("Outside Clinger fast path, semi-fast path, -9223372036854775808e7", false, -9223372036854775808L, 7, 9.223372036854776E25);
        testTryDecFloatToDouble("Outside Clinger fast path, semi-fast path, exponent out of range, -9223372036854775808e-325", false, -9223372036854775808L, -325, 9.223372036854776E-307);
        testTryDecFloatToDouble("Outside Clinger fast path, bail-out in semi-fast path, 1e23", false, 1L, 23, Double.NaN);
        testTryDecFloatToDouble("Outside Clinger fast path, mantissa overflows in semi-fast path, 7.2057594037927933e+16", false, 72057594037927933L, 0, 7.205759403792794E16);
        testTryDecFloatToDouble("Outside Clinger fast path, bail-out in semi-fast path, 7.3177701707893310e+15", false, 73177701707893310L, -1, Double.NaN);
    }

    @Test
    public void testFullMultiplication() {
        //before Java 18
        long x01 = 0x123456789ABCDEF0L & 0xffffffffL, x11 = 0x123456789ABCDEF0L >>> 32;
        long y01 = 0x10L & 0xffffffffL;
        long p111 = 0;
        long p101 = x11 * y01, p001 = x01 * y01;

        // 64-bit product + two 32-bit values
        long middle1 = p101 + (p001 >>> 32);
        Assert.assertEquals(1L, p111 + (middle1 >>> 32));
        Assert.assertEquals(0x23456789abcdef00L, (middle1 << 32) | (p001 & 0xffffffffL));

        //before Java 18
        long x0 = 0x123456789ABCDEF0L & 0xffffffffL, x1 = 0x123456789ABCDEF0L >>> 32;
        long y0 = -0x10L & 0xffffffffL, y1 = -0x10L >>> 32;
        long p11 = x1 * y1, p01 = x0 * y1;
        long p10 = x1 * y0, p00 = x0 * y0;

        // 64-bit product + two 32-bit values
        long middle = p10 + (p00 >>> 32) + (p01 & 0xffffffffL);
        Assert.assertEquals(0x123456789abcdeeeL, p11 + (middle >>> 32) + (p01 >>> 32));
        Assert.assertEquals(0xdcba987654321100L, (middle << 32) | (p00 & 0xffffffffL));
    }

    /**
     * Tests if the values in {@link FastDoubleMath#MANTISSA_64} and
     * {@link FastDoubleMath#MANTISSA_128} are correct in the range
     * [{@value FastDoubleMath#DOUBLE_MIN_EXPONENT_POWER_OF_TEN},0].
     */
    @Test
    public void testNegativePowersOf10() {
        // Note: We compute powers of 5 here.
        // Since {@literal 10 == 5<<1}, we obtain powers of 10 after we have
        // shifted the value to obtain the 128 most significant bits.
        BigInteger five = new BigInteger(new byte[]{5});
        BigInteger value = BigInteger.ONE;

        // We need enough bits, so that we have a precision of 128 bits for
        // the inverse computed bigOne/Double.DOUBLE_MIN_EXPONENT_POWER_OF_TEN
        BigInteger bigOne = BigInteger.ONE.shiftLeft(-Double.MIN_EXPONENT - 140);


        for (int p = 0; p >= FastDoubleMath.DOUBLE_MIN_EXPONENT_POWER_OF_TEN; p--) {
            long expectedHigh = MANTISSA_64[p - FastDoubleMath.DOUBLE_MIN_EXPONENT_POWER_OF_TEN];
            long expectedLow = MANTISSA_128[p - FastDoubleMath.DOUBLE_MIN_EXPONENT_POWER_OF_TEN];

            byte[] expectedBytes = new byte[17];
            setBigEndian(expectedBytes, expectedHigh, expectedLow);
            BigInteger expectedShifted = new BigInteger(expectedBytes);

            BigInteger inverse = bigOne.divide(value);
            int bitLength = inverse.bitLength();
            Assert.assertTrue("we need at least 128 bits of precision: " + bitLength, bitLength >= 128);
            BigInteger actualShifted = inverse.shiftRight(bitLength - 128);
            byte[] actualBytes = actualShifted.toByteArray();
            long actualHigh = getLongFromBigEndianArray(actualBytes, 1);
            long actualLow = getLongFromBigEndianArray(actualBytes, 1 + 8);

            Assert.assertEquals("p=" + p, expectedShifted, actualShifted);
            Assert.assertEquals("(high) " + p + ":" + toUnsignedString(expectedHigh) + "," + toUnsignedString(expectedLow)
                    + " <> " + toUnsignedString(actualHigh) + "," + toUnsignedString(actualLow), expectedHigh, actualHigh);
            Assert.assertEquals("(low) " + p + ":" + toUnsignedString(expectedHigh) + "," + toUnsignedString(expectedLow)
                    + " <> " + toUnsignedString(actualHigh) + "," + toUnsignedString(actualLow), expectedLow, actualLow);

            value = value.multiply(five);
        }
    }

    /**
     * Tests if the values in {@link FastDoubleMath#MANTISSA_64} and
     * {@link FastDoubleMath#MANTISSA_128} are correct in the range
     * [0,{@value FastDoubleMath#DOUBLE_MAX_EXPONENT_POWER_OF_TEN}].
     */
    @Test
    public void testPositivePowersOf10() {
        // Note: We compute powers of 5 here.
        // Since {@literal 10 == 5<<1}, we obtain powers of 10 after we have
        // shifted the value to obtain the 128 most significant bits.
        BigInteger five = new BigInteger(new byte[]{5});
        BigInteger value = BigInteger.ONE;


        for (int p = 0; p <= FastDoubleMath.DOUBLE_MAX_EXPONENT_POWER_OF_TEN; p++) {
            long expectedHigh = MANTISSA_64[p - FastDoubleMath.DOUBLE_MIN_EXPONENT_POWER_OF_TEN];
            long expectedLow = MANTISSA_128[p - FastDoubleMath.DOUBLE_MIN_EXPONENT_POWER_OF_TEN];

            byte[] expectedBytes = new byte[17];
            setBigEndian(expectedBytes, expectedHigh, expectedLow);
            BigInteger expectedShifted = new BigInteger(expectedBytes);

            int bitLength = value.bitLength();
            BigInteger actualShifted = bitLength <= 128 ? value.shiftLeft(128 - bitLength) : value.shiftRight(bitLength - 128);
            byte[] actualBytes = actualShifted.toByteArray();
            long actualHigh = getLongFromBigEndianArray(actualBytes, 1);
            long actualLow = getLongFromBigEndianArray(actualBytes, 1 + 8);

            Assert.assertEquals("p=" + p, expectedShifted, actualShifted);
            Assert.assertEquals("(high) " + p + ":" + toUnsignedString(expectedHigh) + "," + toUnsignedString(expectedLow)
                    + " <> " + toUnsignedString(actualHigh) + "," + toUnsignedString(actualLow), expectedHigh, actualHigh);
            Assert.assertEquals("(low) " + p + ":" + toUnsignedString(expectedHigh) + "," + toUnsignedString(expectedLow)
                    + " <> " + toUnsignedString(actualHigh) + "," + toUnsignedString(actualLow), expectedLow, actualLow);


            value = value.multiply(five);
        }
    }

    public void testTryDecFloatToDouble(String testName, boolean isNegative, long significand, int power, double expected) {
        double actual = FastDoubleMath.tryDecFloatToDouble(isNegative, significand, power);
        Assert.assertEquals(testName, expected, actual, 0.001);
    }

    private static long getLongFromBigEndianArray(byte[] array, int offset) {
        return Long.reverseBytes(Unsafe.getUnsafe().getLong(array, Unsafe.BYTE_OFFSET + offset));
    }

    private static void setBigEndian(byte[] array, long lo, long hi) {
        Unsafe.getUnsafe().putLong(array, Unsafe.BYTE_OFFSET + 1, Long.reverseBytes(lo));
        Unsafe.getUnsafe().putLong(array, Unsafe.BYTE_OFFSET + 1 + 8, Long.reverseBytes(hi));
    }
}
