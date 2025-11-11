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

package io.questdb.std;

import java.math.RoundingMode;
import java.util.Arrays;

/**
 * Thread-safe implementation of Knuth's division algorithm (Algorithm D from TAOCP Vol. 2, §4.3.1).
 * <p>
 * This class provides high-precision division for 256-bit decimal numbers using the classical
 * long-division algorithm optimized for multi-precision arithmetic.
 * <p>
 * The algorithm divides a dividend {@code u} by a divisor {@code v}, producing quotient {@code q}
 * and remainder {@code r} such that:
 * {@code u = q * v + r}, where {@code 0 <= r &lt; v}.
 */
public class DecimalKnuthDivider {
    private final int[] q = new int[8];
    private final int[] u = new int[9];
    private final int[] v = new int[8];
    private int m = 0;
    private int n = 0;

    public void clear() {
        m = 0;
        n = 0;
        // We don't need to clear u and v as they are bounded by m and n.
        Arrays.fill(q, 0);
    }

    /**
     * Performs the division operation and round depending on the remainder value and the isNegative.
     * Note: if isNegative is true, the quotient won't reflect that, you will need to negate the result.
     */
    public void divide(boolean isNegative, RoundingMode roundingMode) {
        // If the dividend is smaller than the divisor, the result is zero.
        if (m < n) {
            if (m == 0) {
                return;
            }

            round(isNegative, roundingMode, m - 1);
            return;
        }

        // If the divisor is a word, we can use the optimized algorithm.
        if (n == 1) {
            divideByWord();
            round(isNegative, roundingMode, 0);
            return;
        }

        // Standard Knuth division algorithm, with inspiration from Devil's delight
        // implementation.
        normalize();

        long divisor = v[n - 1] & 0xFFFFFFFFL;

        // Main loop.
        for (int j = m - n; j >= 0; j--) {
            //region Step D3
            long dividend = ((long) u[j + n] << Integer.SIZE) | (u[j + n - 1] & 0xFFFFFFFFL);
            long qhat;
            long rhat;
            {
                long q = (dividend >>> 1) / divisor << 1;
                long r = dividend - q * divisor;
                long i = ~(r - divisor);
                qhat = q + ((r | i) >>> 63);
                rhat = r - (i >> 63 & divisor);
            }

            // Now correct qhat, algorithm from MutableBigInteger::divideMagnitude
            long vnm1Long = v[n - 1] & 0xFFFFFFFFL;
            long nl = u[j + n - 2] & 0xFFFFFFFFL;
            long rs = ((rhat & 0xFFFFFFFFL) << Integer.SIZE) | nl;
            long estProduct = (qhat & 0xFFFFFFFFL) * (v[n - 2] & 0xFFFFFFFFL);
            if (unsignedLongCompare(estProduct, rs)) {
                qhat--;
                rhat = (rhat + vnm1Long) & 0xFFFFFFFFL;
                if (rhat >= vnm1Long) {
                    estProduct -= v[n - 2] & 0xFFFFFFFFL;
                    rs = ((rhat & 0xFFFFFFFFL) << 32) | nl;
                    if (unsignedLongCompare(estProduct, rs)) {
                        qhat--;
                    }
                }
            }
            //endregion

            //region Step D4
            long k = 0;
            for (int i = 0; i < n; i++) {
                long p = qhat * (v[i] & 0xFFFFFFFFL);
                long t = (u[i + j] & 0xFFFFFFFFL) - k - (p & 0xFFFFFFFFL);
                u[i + j] = (int) t;
                k = (p >>> 32) - (t >> 32);
            }
            long t = (u[j + n] & 0xFFFFFFFFL) - k;
            u[j + n] = (int) t;

            //endregion

            //region Step D5
            // Store quotient digit
            q[j] = (int) qhat;
            //endregion StepD5

            if (t < 0) {
                //region Step D6
                // If we subtracted too much, add back.
                q[j] = q[j] - 1;
                k = 0;
                for (int i = 0; i < n; i++) {
                    t = u[i + j] + k + (v[i] & 0xFFFFFFFFL);
                    u[i + j] = (int) t;
                    k = (t >> 32);
                }
                //endregion Step D6
            }
        }
        round(isNegative, roundingMode);
    }

    /**
     * Initializes the dividend for the division operation.
     * Converts the 128-bit positive value into an array of 32-bit integers
     * stored in little-endian order for the Knuth division algorithm.
     *
     * @param high the high 64 bits of the dividend
     * @param low  the low 64 bits of the dividend
     */
    public void ofDividend(long high, long low) {
        u[3] = (int) (high >>> 32);
        u[2] = (int) high;
        u[1] = (int) (low >>> 32);
        u[0] = (int) low;
        if (u[3] != 0) {
            m = 4;
        } else if (u[2] != 0) {
            m = 3;
        } else if (u[1] != 0) {
            m = 2;
        } else {
            m = 1;
        }
    }

    /**
     * Initializes the dividend for the division operation.
     * Converts the 256-bit positive value into an array of 32-bit integers
     * stored in little-endian order for the Knuth division algorithm.
     *
     * @param hh the most significant 64 bits of the dividend
     * @param hl the high-middle 64 bits of the dividend
     * @param lh the low-middle 64 bits of the dividend
     * @param ll the least significant 64 bits of the dividend
     */
    public void ofDividend(long hh, long hl, long lh, long ll) {
        u[7] = (int) (hh >>> 32);
        u[6] = (int) hh;
        u[5] = (int) (hl >>> 32);
        u[4] = (int) hl;
        u[3] = (int) (lh >>> 32);
        u[2] = (int) lh;
        u[1] = (int) (ll >>> 32);
        u[0] = (int) ll;
        m = countSignificantInts(u);
    }

    /**
     * Initializes the divisor for the division operation.
     * Converts the 128-bit positive value into an array of 32-bit integers
     * stored in little-endian order for the Knuth division algorithm.
     *
     * @param high the high 64 bits of the divisor
     * @param low  the low 64 bits of the divisor
     */
    public void ofDivisor(long high, long low) {
        v[3] = (int) (high >>> 32);
        v[2] = (int) high;
        v[1] = (int) (low >>> 32);
        v[0] = (int) low;
        if (v[3] != 0) {
            n = 4;
        } else if (v[2] != 0) {
            n = 3;
        } else if (v[1] != 0) {
            n = 2;
        } else {
            n = 1;
        }
    }

    /**
     * Initializes the divisor for the division operation.
     * Converts the 256-bit positive value into an array of 32-bit integers
     * stored in little-endian order for the Knuth division algorithm.
     *
     * @param hh the most significant 64 bits of the divisor
     * @param hl the high-middle 64 bits of the divisor
     * @param lh the low-middle 64 bits of the divisor
     * @param ll the least significant 64 bits of the divisor
     */
    public void ofDivisor(long hh, long hl, long lh, long ll) {
        v[7] = (int) (hh >>> 32);
        v[6] = (int) hh;
        v[5] = (int) (hl >>> 32);
        v[4] = (int) hl;
        v[3] = (int) (lh >>> 32);
        v[2] = (int) lh;
        v[1] = (int) (ll >>> 32);
        v[0] = (int) ll;
        n = countSignificantInts(v);
    }

    /**
     * Store the result from the division operation into a Decimal128.
     *
     * @param quotient the Decimal128 to store the result in
     */
    public void sink(Decimal128 quotient, int scale) {
        long high = (q[2] & 0xFFFFFFFFL) | ((long) q[3] << 32);
        long low = (q[0] & 0xFFFFFFFFL) | ((long) q[1] << 32);
        quotient.of(high, low, scale);
    }

    /**
     * Store the result from the division operation into a Decimal256.
     *
     * @param quotient the Decimal256 to store the result in
     */
    public void sink(Decimal256 quotient, int scale) {
        long hh = (q[6] & 0xFFFFFFFFL) | ((long) q[7] << 32);
        long hl = (q[4] & 0xFFFFFFFFL) | ((long) q[5] << 32);
        long lh = (q[2] & 0xFFFFFFFFL) | ((long) q[3] << 32);
        long ll = (q[0] & 0xFFFFFFFFL) | ((long) q[1] << 32);
        quotient.of(hh, hl, lh, ll, scale);
    }

    /**
     * Counts the number of significant (non-zero) integers in the array from most to least significant.
     * This determines the actual precision needed for arithmetic operations by finding the highest
     * non-zero element.
     *
     * @param arr the integer array representing a multi-precision number
     * @return the count of significant integers (minimum 1, maximum 8)
     */
    private static int countSignificantInts(int[] arr) {
        if (arr[7] != 0) {
            return 8;
        }
        if (arr[6] != 0) {
            return 7;
        }
        if (arr[5] != 0) {
            return 6;
        }
        if (arr[4] != 0) {
            return 5;
        }
        if (arr[3] != 0) {
            return 4;
        }
        if (arr[2] != 0) {
            return 3;
        }
        if (arr[1] != 0) {
            return 2;
        }
        return 1;
    }

    /**
     * Compare the remainder of the division with the half of the divisor.
     *
     * @return -1 if the remainder is less than the half of the divisor, 0 if they are equal, 1 otherwise.
     */
    private int compareDivisorHalfRemainder() {
        int h = 0;
        for (int i = n - 1; i >= 0; i--) {
            int cmp = Integer.compareUnsigned(i >= m ? 0 : u[i], (h << 31) | (v[i] >>> 1));
            if (cmp != 0) {
                return cmp;
            }
            h = (v[i] & 0x1);
        }
        return h == 0 ? 0 : -1;
    }

    private void divideByWord() {
        long divisor = (v[0] & 0xFFFFFFFFL);
        if (divisor == 0) {
            throw NumericException.instance().put("Division by zero");
        }

        long r = 0;
        long rLong = 0L;
        for (int j = m - 1; j >= 0; j--) {
            long qhat = rLong << 32 | (u[j] & 0xFFFFFFFFL);
            long quo;
            if (qhat >= 0L) {
                quo = (qhat / divisor);
                r = qhat % divisor;
            } else {
                quo = divWord(qhat, divisor);
                r = (quo >>> 32);
            }
            rLong = r;
            q[j] = (int) quo;
            u[j] = 0;
        }
        u[0] = (int) r;
    }

    /**
     * Step D1: Normalize
     * Shift the dividend/divisor so that the most significant bit of the divisor is set to 1.
     */
    private void normalize() {
        int s = Integer.numberOfLeadingZeros(v[n - 1]);
        if (s == 0) {
            return;
        }

        // Shift the divisor
        for (int i = n - 1; i > 0; i--) {
            v[i] = (v[i] << s) | (int) ((v[i - 1] & 0xFFFFFFFFL) >>> (31 & -s));
        }
        v[0] <<= s;

        // Shift the dividend
        u[m] = (int) ((u[m - 1] & 0xFFFFFFFFL) >>> (31 & -s));
        for (int i = m - 1; i > 0; i--) {
            u[i] = (u[i] << s) | (int) ((u[i - 1] & 0xFFFFFFFFL) >>> (31 & -s));
        }
        u[0] <<= s;
    }

    /**
     * Round the quotient based on the unnormalized remainder/division.
     */
    private void round(boolean isNegative, RoundingMode roundingMode) {
        int mostSignificantDigit = -1;
        for (int i = m - 1; i >= 0; i--) {
            if (u[i] != 0) {
                mostSignificantDigit = i;
                break;
            }
        }
        if (mostSignificantDigit == -1) {
            return;
        }

        round(isNegative, roundingMode, mostSignificantDigit);
    }

    /**
     * Round the quotient based on the unnormalized remainder/division.
     */
    private void round(boolean isNegative, RoundingMode roundingMode, int mostSignificantDigit) {
        if (u[mostSignificantDigit] == 0) {
            return;
        }

        boolean increment = false;

        switch (roundingMode) {
            case UNNECESSARY:
                throw NumericException.instance().put("Rounding necessary");
            case UP: // Away from zero
                increment = true;
                break;

            case DOWN: // Towards zero
                break;

            case CEILING: // Towards +infinity
                increment = !isNegative;
                break;

            case FLOOR: // Towards -infinity
                increment = isNegative;
                break;

            default: // Some kind of half-way rounding
                final int cmp = compareDivisorHalfRemainder();
                if (cmp > -1) {
                    increment = cmp > 0 || roundingMode == RoundingMode.HALF_UP || (roundingMode == RoundingMode.HALF_EVEN && (q[0] & 0x1) == 1);
                }
        }

        if (increment) {
            for (int i = 0; i < n; i++) {
                if (++q[i] != 0) {
                    break;
                }
            }
        }
    }

    /**
     * Compare two longs as if they were unsigned.
     * Returns true iff one is bigger than two.
     */
    private boolean unsignedLongCompare(long one, long two) {
        return (one + Long.MIN_VALUE) > (two + Long.MIN_VALUE);
    }

    static long divWord(long dividend, long divisor) {
        if (divisor >= 0L) {
            long q = (dividend >>> 1) / divisor << 1;
            long r = dividend - q * divisor;
            long d = ~(r - divisor);
            return (r - (d >> 63 & divisor)) << 32 | (q + ((r | d) >>> 63)) & 0xFFFFFFFFL;
        } else {
            long d = dividend & ~(dividend - divisor);
            return dividend - (d >> 63 & divisor) << 32 | (d >>> 63) & 0xFFFFFFFFL;
        }
    }
}
