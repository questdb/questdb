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

package io.questdb.std.fastdouble;

import static io.questdb.std.fastdouble.FastDoubleMath.*;

/**
 * This class complements {@link FastDoubleMath} with methods for
 * converting {@code FloatingPointLiteral} productions to floats.
 * <p>
 * See {@link io.questdb.std.fastdouble} for a description of
 * {@code FloatingPointLiteral}.
 */
public class FastFloatMath {
    /**
     * Bias used in the exponent of a float.
     */
    private static final int FLOAT_EXPONENT_BIAS = 127;
    private final static int FLOAT_MAX_EXPONENT_POWER_OF_TEN = 38;
    private final static int FLOAT_MAX_EXPONENT_POWER_OF_TWO = Float.MAX_EXPONENT;
    private final static int FLOAT_MIN_EXPONENT_POWER_OF_TEN = -45;
    private final static int FLOAT_MIN_EXPONENT_POWER_OF_TWO = Float.MIN_EXPONENT;
    /**
     * Precomputed powers of ten from 10^0 to 10^10. These
     * can be represented exactly using the float type.
     */
    private static final float[] FLOAT_POWER_OF_TEN = {
            1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};
    /**
     * The number of bits in the significand, including the implicit bit.
     */
    private static final int FLOAT_SIGNIFICAND_WIDTH = 24;

    /**
     * Don't let anyone instantiate this class.
     */
    private FastFloatMath() {

    }

    /**
     * Attempts to compute {@literal digits * 10^(power)} exactly;
     * and if "negative" is true, negate the result.
     * <p>
     * This function will only work in some cases, when it does not work it
     * returns null. This should work *most of the time* (like 99% of the time).
     * We assume that power is in the
     * [{@value FastDoubleMath#DOUBLE_MIN_EXPONENT_POWER_OF_TEN},
     * {@value FastDoubleMath#DOUBLE_MAX_EXPONENT_POWER_OF_TEN}]
     * interval: the caller is responsible for this check.
     *
     * @param isNegative whether the number is negative
     * @param digits     uint64 the digits of the number
     * @param power      int32 the exponent of the number
     * @return the computed double on success, {@link Double#NaN} on failure
     */
    public static float tryDecToFloatWithFastAlgorithm(boolean isNegative, long digits, int power) {

        // we start with a fast path
        if (-10 <= power && power <= 10 && Long.compareUnsigned(digits, (1L << FLOAT_SIGNIFICAND_WIDTH) - 1L) <= 0) {
            // convert the integer into a float. This is lossless since
            // 0 <= i <= 2^24 - 1.
            float d = (float) digits;
            //
            // The general idea is as follows.
            // If 0 <= s < 2^24 and if 10^0 <= p <= 10^10 then
            // 1) Both s and p can be represented exactly as 32-bit floating-point values
            // 2) Because s and p can be represented exactly as floating-point values,
            // then s * p and s / p will produce correctly rounded values.
            //
            if (power < 0) {
                d = d / FLOAT_POWER_OF_TEN[-power];
            } else {
                d = d * FLOAT_POWER_OF_TEN[power];
            }
            return (isNegative) ? -d : d;
        }


        // The fast path has now failed, so we are falling back on the slower path.

        // We are going to need to do some 64-bit arithmetic to get a more precise product.
        // We use a table lookup approach.
        // It is safe because
        // power >= DOUBLE_MIN_EXPONENT_POWER_OF_TEN
        // and power <= DOUBLE_MAX_EXPONENT_POWER_OF_TEN
        // We recover the mantissa of the power, it has a leading 1. It is always
        // rounded down.
        long factorMantissa = MANTISSA_64[power - DOUBLE_MIN_EXPONENT_POWER_OF_TEN];


        // The exponent is 127 + 64 + power
        //     + floor(log(5**power)/log(2)).
        // The 127 is the exponent bias.
        // The 64 comes from the fact that we use a 64-bit word.
        //
        // Computing floor(log(5**power)/log(2)) could be
        // slow. Instead ,we use a fast function.
        //
        // For power in (-400,350), we have that
        // (((152170 + 65536) * power ) >> 16);
        // is equal to
        //  floor(log(5**power)/log(2)) + power when power >= 0
        // and it is equal to
        //  ceil(log(5**-power)/log(2)) + power when power < 0
        //
        //
        // The 65536 is (1<<16) and corresponds to
        // (65536 * power) >> 16 ---> power
        //
        // ((152170 * power ) >> 16) is equal to
        // floor(log(5**power)/log(2))
        //
        // Note that this is not magic: 152170/(1<<16) is
        // approximately equal to log(5)/log(2).
        // The 1<<16 value is a power of two; we could use a
        // larger power of 2 if we wanted to.
        //
        long exponent = (((152170L + 65536L) * power) >> 16) + FLOAT_EXPONENT_BIAS + 64;
        // We want the most significant bit of digits to be 1. Shift if needed.
        int lz = Long.numberOfLeadingZeros(digits);
        digits <<= lz;
        // We want the most significant 64 bits of the product. We know
        // this will be non-zero because the most significant bit of i is
        // 1.
        //before Java 18
        long x01 = digits & 0xffffffffL, x11 = digits >>> 32;
        long y01 = factorMantissa & 0xffffffffL, y11 = factorMantissa >>> 32;
        long p111 = x11 * y11, p011 = x01 * y11;
        long p101 = x11 * y01, p001 = x01 * y01;

        // 64-bit product + two 32-bit values
        long middle1 = p101 + (p001 >>> 32) + (p011 & 0xffffffffL);
        long lower = (middle1 << 32) | (p001 & 0xffffffffL);
        long upper = p111 + (middle1 >>> 32) + (p011 >>> 32);
        // We know that upper has at most one leading zero because
        // both i and factor_mantissa have a leading one. This means
        // that the result is at least as large as ((1<<63)*(1<<63))/(1<<64).

        // As long as the first 39 bits of "upper" are not "1", then we
        // know that we have an exact computed value for the leading
        // 25 bits because any imprecision would play out as a +1, in
        // the worst case.
        // Having 25 bits is necessary because
        // we need 24 bits for the mantissa, but we have to have one rounding bit, and
        // we can waste a bit if the most significant bit of the product is zero.
        // We expect this next branch to be rarely taken (say 1% of the time).
        // When (upper &0x3FFFFFFFFF) == 0x3FFFFFFFFF, it can be common for
        // lower + i < lower to be true (proba. much higher than 1%).
        if ((upper & 0x3_FFFFF_FFFFL) == 0x3_FFFFF_FFFFL && Long.compareUnsigned(lower + digits, lower) < 0) {
            long factor_mantissa_low =
                    MANTISSA_128[power - DOUBLE_MIN_EXPONENT_POWER_OF_TEN];
            // next, we compute the 64-bit x 128-bit multiplication, getting a 192-bit
            // result (three 64-bit values)
            //before Java 18
            long x0 = digits & 0xffffffffL, x1 = digits >>> 32;
            long y0 = factor_mantissa_low & 0xffffffffL, y1 = factor_mantissa_low >>> 32;
            long p11 = x1 * y1, p01 = x0 * y1;
            long p10 = x1 * y0, p00 = x0 * y0;

            // 64-bit product + two 32-bit values
            long middle = p10 + (p00 >>> 32) + (p01 & 0xffffffffL);
            long product_low = (middle << 32) | (p00 & 0xffffffffL);
            long product_middle2 = p11 + (middle >>> 32) + (p01 >>> 32);
            long product_high = upper;
            long product_middle = lower + product_middle2;
            if (Long.compareUnsigned(product_middle, lower) < 0) {
                product_high++; // overflow carry
            }


            // we want to check whether mantissa *i + i would affect our result
            // This does happen, e.g. with 7.3177701707893310e+15 ????
            if (((product_middle + 1 == 0) && ((product_high & 0x7_FFFFF_FFFFL) == 0x7_FFFFF_FFFFL) &&
                    (product_low + Long.compareUnsigned(digits, product_low) < 0))) { // let us be prudent and bail out.
                return Float.NaN;
            }
            upper = product_high;
            //lower = product_middle;
        }

        // The final mantissa should be 24 bits with a leading 1.
        // We shift it so that it occupies 25 bits with a leading 1.
        long upperbit = upper >>> 63;
        long mantissa = upper >>> (upperbit + 38);
        lz += (int) (1 ^ upperbit);
        // Here we have mantissa < (1<<25).
        //assert mantissa < (1<<25);

        // We have to round to even. The "to even" part
        // is only a problem when we are right in between two floating-point values
        // which we guard against.
        // If we have lots of trailing zeros, we may fall right between two
        // floating-point values.
        if (((upper & 0x3_FFFFF_FFFFL) == 0x3_FFFFF_FFFFL)
                || ((upper & 0x3_FFFFF_FFFFL) == 0) && (mantissa & 3) == 1) {
            // if mantissa & 1 == 1 we might need to round up.
            //
            // Scenarios:
            // 1. We are not in the middle. Then we should round up.
            //
            // 2. We are right in the middle. Whether we round up depends
            // on the last significant bit: if it is "one" then we round
            // up (round to even) otherwise, we do not.
            //
            // So if the last significant bit is 1, we can safely round up.
            // Hence, we only need to bail out if (mantissa & 3) == 1.
            // Otherwise, we may need more accuracy or analysis to determine whether
            // we are exactly between two floating-point numbers.
            // It can be triggered with 1e23. ??
            // Note: because the factor_mantissa and factor_mantissa_low are
            // almost always rounded down (except for small positive powers),
            // almost always should round up.
            return Float.NaN;
        }

        mantissa += 1;
        mantissa >>>= 1;

        // Here we have mantissa < (1<<24), unless there was an overflow
        if (mantissa >= (1L << FLOAT_SIGNIFICAND_WIDTH)) {
            // This will happen when parsing values such as 7.2057594037927933e+16 ??
            mantissa = (1L << (FLOAT_SIGNIFICAND_WIDTH - 1));
            lz--; // undo previous addition
        }

        mantissa &= ~(1L << (FLOAT_SIGNIFICAND_WIDTH - 1));


        long real_exponent = exponent - lz;
        // we have to check that real_exponent is in range, otherwise we bail out
        if ((real_exponent < 1) || (real_exponent > FLOAT_MAX_EXPONENT_POWER_OF_TWO + FLOAT_EXPONENT_BIAS)) {
            return Float.NaN;
        }

        int bits = (int) (mantissa | real_exponent << (FLOAT_SIGNIFICAND_WIDTH - 1)
                | (isNegative ? 1L << 31 : 0));
        return Float.intBitsToFloat(bits);
    }

    static float decFloatLiteralToFloat(boolean isNegative, long significand, int exponent,
                                        boolean isSignificandTruncated,
                                        int exponentOfTruncatedSignificand) {
        if (significand == 0) {
            return isNegative ? -0.0f : 0.0f;
        }

        final float result;
        if (isSignificandTruncated) {

            // We have too many digits. We may have to round up.
            // To know whether rounding up is needed, we may have to examine up to 768 digits.

            // There are cases, in which rounding has no effect.
            if (FLOAT_MIN_EXPONENT_POWER_OF_TEN <= exponentOfTruncatedSignificand
                    && exponentOfTruncatedSignificand <= FLOAT_MAX_EXPONENT_POWER_OF_TEN) {
                float withoutRounding = tryDecToFloatWithFastAlgorithm(isNegative, significand, exponentOfTruncatedSignificand);
                float roundedUp = tryDecToFloatWithFastAlgorithm(isNegative, significand + 1, exponentOfTruncatedSignificand);
                if (!Float.isNaN(withoutRounding) && roundedUp == withoutRounding) {
                    return withoutRounding;
                }
            }

            // We have to take a slow path.
            //return Double.parseDouble(str.toString());
            result = Float.NaN;


        } else if (FLOAT_MIN_EXPONENT_POWER_OF_TEN <= exponent && exponent <= FLOAT_MAX_EXPONENT_POWER_OF_TEN) {
            result = tryDecToFloatWithFastAlgorithm(isNegative, significand, exponent);
        } else {
            result = Float.NaN;
        }
        return result;
    }

    static float hexFloatLiteralToFloat(boolean isNegative, long significand, int exponent,
                                        boolean isSignificandTruncated,
                                        int exponentOfTruncatedSignificand) {
        if (significand == 0) {
            return isNegative ? -0.0f : 0.0f;
        }
        final float result;
        if (isSignificandTruncated) {

            // We have too many digits. We may have to round up.
            // To know whether rounding up is needed, we may have to examine up to 768 digits.

            // There are cases, in which rounding has no effect.
            if (FLOAT_MIN_EXPONENT_POWER_OF_TWO <= exponentOfTruncatedSignificand && exponentOfTruncatedSignificand <= FLOAT_MAX_EXPONENT_POWER_OF_TWO) {
                float withoutRounding = tryHexToFloatWithFastAlgorithm(isNegative, significand, exponentOfTruncatedSignificand);
                float roundedUp = tryHexToFloatWithFastAlgorithm(isNegative, significand + 1, exponentOfTruncatedSignificand);
                if (!Double.isNaN(withoutRounding) && roundedUp == withoutRounding) {
                    return withoutRounding;
                }
            }

            // We have to take a slow path.
            result = Float.NaN;

        } else if (FLOAT_MIN_EXPONENT_POWER_OF_TWO <= exponent && exponent <= FLOAT_MAX_EXPONENT_POWER_OF_TWO) {
            result = tryHexToFloatWithFastAlgorithm(isNegative, significand, exponent);
        } else {
            result = Float.NaN;
        }
        return result;
    }

    static float tryHexToFloatWithFastAlgorithm(boolean isNegative, long digits, int power) {
        if (digits == 0 || power < Float.MIN_EXPONENT - 54) {
            return isNegative ? -0.0f : 0.0f;
        }
        if (power > Float.MAX_EXPONENT) {
            return isNegative ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        }

        // we start with a fast path
        // We try to mimic the fast described by Clinger WD for decimal
        // float number literals. How to read floating point numbers accurately.
        // ACM SIGPLAN Notices. 1990
        if (Long.compareUnsigned(digits, 0x1fffffffffffffL) <= 0) {
            // convert the integer into a double. This is lossless since
            // 0 <= i <= 2^53 - 1.
            float d = (float) digits;
            //
            // The general idea is as follows.
            // If 0 <= s < 2^53  then
            // 1) Both s and p can be represented exactly as 64-bit floating-point
            // values (binary64).
            // 2) Because s and p can be represented exactly as floating-point values,
            // then s * p will produce correctly rounded values.
            //
            d = d * Math.scalb(1f, power);
            if (isNegative) {
                d = -d;
            }
            return d;
        }

        // The fast path has failed
        return Float.NaN;
    }


}
