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

import io.questdb.std.str.CharSink;

public final class Decimals {
    public static final int MAX_PRECISION = 76;
    public static final int MAX_SCALE = MAX_PRECISION;
    private static final long[] POW10_PRECISION = { // maps the maximum positive value to the precision
            9L,
            99L,
            999L,
            9999L,
            99999L,
            999999L,
            9999999L,
            99999999L,
            999999999L,
            9999999999L,
            99999999999L,
            999999999999L,
            9999999999999L,
            99999999999999L,
            999999999999999L,
            9999999999999999L,
            99999999999999999L,
            999999999999999999L,
    };
    private static final int[] PRECISION_SIZE_POW2 = {
            0, 0, 0, // precision 0-2 -> 1 byte
            1, 1, // precision 3-4 -> 2 byte
            2, 2, 2, 2, 2, // precision 5-9 -> 4 bytes
            3, 3, 3, 3, 3, 3, 3, 3, 3, // precision 10-18 -> 8 bytes
            4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
            4, 4, 4, 4, 4, 4, 4, 4, // precision 19-38 -> 16 bytes
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
            5, 5, // precision 39-76 -> 32 bytes
    };
    public static long DECIMAL128_HI_NULL = Long.MIN_VALUE;
    public static long DECIMAL128_LO_NULL = 0L;
    public static short DECIMAL16_NULL = Short.MIN_VALUE;
    public static long DECIMAL256_HH_NULL = Long.MIN_VALUE;
    public static long DECIMAL256_HL_NULL = 0L;
    public static long DECIMAL256_LH_NULL = 0L;
    public static long DECIMAL256_LL_NULL = 0L;
    public static int DECIMAL32_NULL = Integer.MIN_VALUE;
    public static long DECIMAL64_NULL = Long.MIN_VALUE;
    public static byte DECIMAL8_NULL = Byte.MIN_VALUE;

    /**
     * Prints the long decimal to a sink
     *
     * @param value to print
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void append(long value, int precision, int scale, CharSink<?> sink) {
        if (value == Decimals.DECIMAL64_NULL) {
            sink.put("null");
        } else {
            long s = value < 0 ? -1 : 0;
            Decimal256.toSink(sink, s, s, s, value, scale, precision);
        }
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void append(long hi, long lo, int precision, int scale, CharSink<?> sink) {
        if (Decimal128.isNull(hi, lo)) {
            sink.put("null");
        } else {
            long s = hi < 0 ? -1 : 0;
            Decimal256.toSink(sink, s, s, hi, lo, scale, precision);
        }
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void append(long hh, long hl, long lh, long ll, int precision, int scale, CharSink<?> sink) {
        if (Decimal256.isNull(hh, hl, lh, ll)) {
            sink.put("null");
        } else {
            Decimal256.toSink(sink, hh, hl, lh, ll, scale, precision);
        }
    }

    /**
     * Returns the precision for a specific int decimal.
     *
     * @param value of the decimal
     * @return the required precision to store the long.
     */
    public static int getIntPrecision(int value) {
        if (value == Numbers.INT_NULL) return 0;
        if (value < 0) {
            value = -value;
        }
        for (int i = 0; i < 9; i++) {
            if (value <= POW10_PRECISION[i]) {
                return i + 1;
            }
        }
        return 10;
    }

    /**
     * Returns the precision for a specific long decimal.
     *
     * @param value of the decimal
     * @return the required precision to store the long.
     */
    public static int getLongPrecision(long value) {
        if (value == Numbers.LONG_NULL) return 0;
        if (value < 0) {
            value = -value;
        }
        for (int i = 0, n = POW10_PRECISION.length; i < n; i++) {
            if (value <= POW10_PRECISION[i]) {
                return i + 1;
            }
        }
        return 19;
    }

    /**
     * Returns the maximum byte value for a specific precision.
     *
     * @param precision to be used as reference
     */
    public static byte getMaxByte(int precision) {
        assert precision > 0;
        if (precision >= 3) {
            return Byte.MAX_VALUE;
        }
        return (byte) POW10_PRECISION[precision - 1];
    }

    /**
     * Returns the maximum int value for a specific precision.
     *
     * @param precision to be used as reference
     */
    public static int getMaxInt(int precision) {
        assert precision > 0;
        if (precision >= 10) {
            return Integer.MAX_VALUE;
        }
        return (int) POW10_PRECISION[precision - 1];
    }

    /**
     * Returns the maximum long value for a specific precision.
     *
     * @param precision to be used as reference
     */
    public static long getMaxLong(int precision) {
        assert precision > 0;
        if (precision >= 19) {
            return Long.MAX_VALUE;
        }
        return POW10_PRECISION[precision - 1];
    }

    /**
     * Returns the maximum short value for a specific precision.
     *
     * @param precision to be used as reference
     */
    public static short getMaxShort(int precision) {
        assert precision > 0;
        if (precision >= 5) {
            return Short.MAX_VALUE;
        }
        return (short) POW10_PRECISION[precision - 1];
    }

    /**
     * Returns the required storage size in bytes (pow 2) for a decimal with given precision.
     * <p>
     * Storage sizes:
     * - Precision 1-2:   1 byte  (7 bits for magnitude + 1 sign bit)
     * - Precision 3-4:   2 bytes (15 bits for magnitude + 1 sign bit)
     * - Precision 5-9:   4 bytes (31 bits for magnitude + 1 sign bit)
     * - Precision 10-18: 8 bytes (63 bits for magnitude + 1 sign bit)
     * - Precision 19-38: 16 bytes (128-bit storage)
     * - Precision 39-76: 32 bytes (256-bit storage)
     *
     * @param precision the number of significant digits
     * @return the required storage size in bytes (pow 2)
     * @throws IllegalArgumentException if precision is invalid
     */
    public static int getStorageSizePow2(int precision) {
        if (precision < 1 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid decimal precision: " + precision +
                    ". Must be between 1 and " + MAX_PRECISION);
        }
        return PRECISION_SIZE_POW2[precision];
    }
}
