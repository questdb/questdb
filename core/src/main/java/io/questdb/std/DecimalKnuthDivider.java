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
 * Thread-safe implementation of Knuth's division algorithm (Algorithm D from TAOCP Vol 2, Section 4.3.1).
 * This class provides high-precision division for 256-bit decimal numbers using the classical
 * long division algorithm optimized for multi-precision arithmetic.
 * 
 * The algorithm divides a dividend u by a divisor v, producing quotient q and remainder r
 * such that u = q * v + r, where 0 <= r < v.
 */
class DecimalKnuthDivider {
    private static final ThreadLocal<DecimalKnuthDivider> tl = new ThreadLocal<>(DecimalKnuthDivider::new);
    private final int[] v = new int[8];
    private final int[] q = new int[8];
    private final int[] r = new int[8];
    private final int[] u = new int[9];
    private int m = 0;
    private int n = 0;

    public static DecimalKnuthDivider instance() {
        DecimalKnuthDivider instance = tl.get();
        instance.clear();
        return instance;
    }

    private void clear() {
        m = 0;
        n = 0;
        // We don't need to clear u and v as they are bounded by m and n.
        Arrays.fill(q, 0);
        Arrays.fill(r, 0);
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
     * Initializes the divisor for the division operation.
     * Converts the 256-bit positive value into an array of 32-bit integers
     * stored in little-endian order for the Knuth division algorithm.
     *
     * @param hh the most significant 64 bits of the dividend
     * @param hl the high-middle 64 bits of the dividend
     * @param lh the low-middle 64 bits of the dividend
     * @param ll the least significant 64 bits of the dividend
     */
    public void ofDivisor(long hh, long hl, long lh, long ll) {
        v[7] = (int)(hh >>> 32);
        v[6] = (int)hh;
        v[5] = (int)(hl >>> 32);
        v[4] = (int)hl;
        v[3] = (int)(lh >>> 32);
        v[2] = (int)lh;
        v[1] = (int)(ll >>> 32);
        v[0] = (int)ll;
        m = countSignificantInts(v);
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
        u[7] = (int)(hh >>> 32);
        u[6] = (int)hh;
        u[5] = (int)(hl >>> 32);
        u[4] = (int)hl;
        u[3] = (int)(lh >>> 32);
        u[2] = (int)lh;
        u[1] = (int)(ll >>> 32);
        u[0] = (int)ll;
        n = countSignificantInts(u);
    }

    /**
     * Store the result from the division operation into a Decimal256.
     * @param quotient the Decimal256 to store the result in
     */
    public void sink(Decimal256 quotient, int scale) {
        long hh = (q[7] & 0xFFFFFFFFL) | ((long)q[6] << 32);
        long hl = (q[5] & 0xFFFFFFFFL) | ((long)q[4] << 32);
        long lh = (q[3] & 0xFFFFFFFFL) | ((long)q[2] << 32);
        long ll = (q[1] & 0xFFFFFFFFL) | ((long)q[0] << 32);
        quotient.of(hh, hl, lh, ll, scale);
    }

    /**
     * Performs the division operation and round depending on the remainder value and the isNegative.
     * Note: if isNegative is true, the quotient won't reflect that, you will need to negate the result.
     */
    public void divide(boolean isNegative, RoundingMode roundingMode) {
        // If the dividend is smaller than the divisor, the result is zero.
        if (n < m) {
            System.arraycopy(u, 0, r, 0, n);
            return;
        }

        // If the divisor is a word, we can use the optimized algorithm.
        if (m == 1) {
            divideByWord();
        }

        // Standard Knuth division algorithm, with inspiration from Devil's delight
        // implementation.
        int s = normalize();

        long divisor = v[n-1] & 0xFFFFFFFFL;

        // Main loop.
        for (int j = m - n; j >= 0; j--) {
            //region Step D3
            long dividend = ((long) u[j+n] << Integer.SIZE) | (u[j+n-1] & 0xFFFFFFFFL);
            long qhat;
            long rhat;
            {
                // TODO: improve
                long q = (dividend >>> 1) / divisor << 1;
                long r = dividend - q * divisor;
                long i = ~(r - divisor);
                qhat = q + ((r | i) >>> 63);
                rhat = r - (i >> 63 & divisor);
            }

            // Now correct qhat, algorithm from MutableBigInteger::divideMagnitude
            long vnm1Long = v[n-1] & 0xFFFFFFFFL;
            long nl = u[j+n-2] & 0xFFFFFFFFL;
            long rs = ((rhat & 0xFFFFFFFFL) << Integer.SIZE) | nl;
            long estProduct = (qhat & 0xFFFFFFFFL) * (v[n-2] & 0xFFFFFFFFL);
            if (unsignedLongCompare(estProduct, rs)) {
                qhat--;
                rhat = (rhat + vnm1Long) & 0xFFFFFFFFL;
                if (rhat >= vnm1Long) {
                    estProduct -= v[n-2] & 0xFFFFFFFFL;
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
                long t = (u[i+j] & 0xFFFFFFFFL) - k - (p & 0xFFFFFFFFL);
                u[i+j] = (int) t;
                k = (p >>> 32) - (t >> 32);
            }
            long t = (u[j+n] & 0xFFFFFFFFL) - k;
            u[j+n] = (int) t;

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
                    t = u[i+j] + k + (v[i] & 0xFFFFFFFFL);
                    u[i+j] = (int) t;
                    k = (t >> 32);
                }
                //endregion Step D6
            }
        }
        unnormalize(s);
    }

    /**
     * Compare two longs as if they were unsigned.
     * Returns true iff one is bigger than two.
     */
    private boolean unsignedLongCompare(long one, long two) {
        return (one+Long.MIN_VALUE) > (two+Long.MIN_VALUE);
    }

    /**
     * Step D1: Normalize
     * Shift the dividend/divisor so that the most significant bit of the divisor is set to 1.
     * @return the number of leading zeros in the divisor
     */
    private int normalize() {
        int s = Integer.numberOfLeadingZeros(v[n-1]);
        if (s == 0) {
            return 0;
        }

        // Shift the divisor
        for (int i = n-1; i > 0; i--) {
            v[i] = (v[i] << s) | (int)((v[i-1] & 0xFFFFFFFFL) >> (31 & -s));
        }
        v[0] <<= s;

        // Shift the dividend
        u[m] = (int)((u[m-1] & 0xFFFFFFFFL) >> (31 & -s));
        for (int i = m-1; i > 0; i--) {
            u[i] = (u[i] << s) | (int)((u[i-1] & 0xFFFFFFFFL) >> (31 & -s));
        }
        u[0] <<= s;

        return s;
    }

    /**
     * Step D8: Unnormalize
     * Build the reminder unnormalized.
     */
    private void unnormalize(int s) {
        if (s == 0) {
            // No need to unnormalize, just copy the remainder.
            System.arraycopy(u, 0, r, 0, n);
            return;
        }
        for (int i = 0; i < n - 1; i++) {
            r[i] = (u[i] >>> s) | (u[i+1] << (32 & -s));
        }
        r[n-1] = u[n-1] >>> s;
    }

    private void divideByWord() {
        int divisor = v[0];
        if (divisor == 0) {
            throw NumericException.instance().put("Division by zero");
        }
    }
}
