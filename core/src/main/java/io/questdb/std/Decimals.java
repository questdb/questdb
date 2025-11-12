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

import io.questdb.cairo.ColumnType;
import io.questdb.std.str.CharSink;

public final class Decimals {
    public static final int MAX_PRECISION = 76;
    // Maximum scale of a decimal value, must be kept below Byte.MAX_VALUE for ILP.
    public static final int MAX_SCALE = MAX_PRECISION;
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
    private static final int[] TYPE_PRECISION = {
            2, // DECIMAL 8
            4, // DECIMAL 16
            9, // DECIMAL 32
            18, // DECIMAL 64
            38, // DECIMAL 128
            MAX_PRECISION // DECIMAL 256
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
            appendNonNull(value, precision, scale, sink);
        }
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void append(Decimal128 decimal128, int precision, int scale, CharSink<?> sink) {
        if (decimal128.isNull()) {
            sink.put("null");
        } else {
            appendNonNull(decimal128, precision, scale, sink);
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
            appendNonNull(hi, lo, precision, scale, sink);
        }
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void append(Decimal256 decimal256, int precision, int scale, CharSink<?> sink) {
        if (decimal256.isNull()) {
            sink.put("null");
        } else {
            appendNonNull(decimal256, precision, scale, sink);
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
            appendNonNull(hh, hl, lh, ll, precision, scale, sink);
        }
    }

    /**
     * Prints the long decimal to a sink
     *
     * @param value to print
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void appendNonNull(long value, int precision, int scale, CharSink<?> sink) {
        Decimal64.toSink(sink, value, scale, precision);
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void appendNonNull(long hi, long lo, int precision, int scale, CharSink<?> sink) {
        Decimal128.toSink(sink, hi, lo, scale, precision);
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void appendNonNull(Decimal256 decimal256, int precision, int scale, CharSink<?> sink) {
        Decimal256.toSink(
                sink,
                decimal256.getHh(),
                decimal256.getHl(),
                decimal256.getLh(),
                decimal256.getLl(),
                scale,
                precision
        );
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void appendNonNull(long hh, long hl, long lh, long ll, int precision, int scale, CharSink<?> sink) {
        Decimal256.toSink(
                sink,
                hh,
                hl,
                lh,
                ll,
                scale,
                precision
        );
    }

    /**
     * Prints the decimal to a sink
     *
     * @param scale defines the place of the dot
     * @param sink  to write the value to
     */
    public static void appendNonNull(Decimal128 decimal128, int precision, int scale, CharSink<?> sink) {
        Decimal128.toSink(sink, decimal128.getHigh(), decimal128.getLow(), scale, precision);
    }

    public static int getDecimalTagPrecision(int tag) {
        return TYPE_PRECISION[tag - ColumnType.DECIMAL8];
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
