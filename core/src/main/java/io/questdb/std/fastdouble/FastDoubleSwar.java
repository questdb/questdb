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

import io.questdb.std.Unsafe;

/**
 * This class provides methods for parsing multiple characters at once using
 * the "SIMD with a register" (SWAR) technique.
 * <p>
 * References:
 * <dl>
 *     <dt>Leslie Lamport, Multiple Byte Processing with Full-Word Instructions</dt>
 *     <dd><a href="https://lamport.azurewebsites.net/pubs/multiple-byte.pdf">azurewebsites.net</a></dd>
 *
 *     <dt>Daniel Lemire, fast_double_parser, 4x faster than strtod.
 *     Apache License 2.0 or Boost Software License.</dt>
 *     <dd><a href="https://github.com/lemire/fast_double_parser">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, fast_float number parsing library: 4x faster than strtod.
 *     Apache License 2.0.</dt>
 *     <dd><a href="https://github.com/fastfloat/fast_float">github.com</a></dd>
 *
 *     <dt>Daniel Lemire, Number Parsing at a Gigabyte per Second,
 *     Software: Practice and Experience 51 (8), 2021.
 *     arXiv.2101.11408v3 [cs.DS] 24 Feb 2021</dt>
 *     <dd><a href="https://arxiv.org/pdf/2101.11408.pdf">arxiv.org</a></dd>
 * </dl>
 */
public class FastDoubleSwar {
    public static long getChunk(byte[] a, int offset) {
        return Unsafe.getUnsafe().getLong(a, Unsafe.BYTE_OFFSET + offset);
    }

    /**
     * Tries to parse eight decimal digits at once using the
     * 'SIMD within a register technique' (SWAR).
     *
     * <pre>{@literal
     * char[] chars = ...;
     * long first  = chars[0]|(chars[1]<<16)|(chars[2]<<32)|(chars[3]<<48);
     * long second = chars[4]|(chars[5]<<16)|(chars[6]<<32)|(chars[7]<<48);
     * }</pre>
     *
     * @param first  the first four characters in big endian order
     * @param second the second four characters in big endian order
     * @return the parsed digits or -1
     */
    public static int tryToParseEightDigitsUtf16(long first, long second) {//since Java 18
        long fval = first - 0x0030_0030_0030_0030L;
        long sval = second - 0x0030_0030_0030_0030L;

        // Create a predicate for all bytes which are smaller than '0' (0x0030)
        // or greater than '9' (0x0039).
        // We have 0x007f - 0x0039 = 0x0046.
        // The predicate is true if the hsb of a byte is set: (predicate & 0xff80) != 0.
        long fpre = first + 0x0046_0046_0046_0046L | fval;
        long spre = second + 0x0046_0046_0046_0046L | sval;
        if (((fpre | spre) & 0xff80_ff80_ff80_ff80L) != 0L) {
            return -1;
        }

        return (int) (sval * 0x03e8_0064_000a_0001L >>> 48)
                + (int) (fval * 0x03e8_0064_000a_0001L >>> 48) * 10000;
    }

    /**
     * Tries to parse eight decimal digits from a char array using the
     * 'SIMD within a register technique' (SWAR).
     *
     * @param a      contains 8 utf-16 characters starting at offset
     * @param offset the offset into the array
     * @return the parsed number,
     * returns a negative value if {@code value} does not contain 8 hex digits
     */

    public static int tryToParseEightDigitsUtf16(char[] a, int offset) {
        long first = a[offset]
                | (long) a[offset + 1] << 16
                | (long) a[offset + 2] << 32
                | (long) a[offset + 3] << 48;
        long second = a[offset + 4]
                | (long) a[offset + 5] << 16
                | (long) a[offset + 6] << 32
                | (long) a[offset + 7] << 48;
        return FastDoubleSwar.tryToParseEightDigitsUtf16(first, second);
    }

    /**
     * Tries to parse eight decimal digits from a byte array using the
     * 'SIMD within a register technique' (SWAR).
     *
     * @param a      contains 8 ascii characters
     * @param offset the offset of the first character in {@code a}
     * @return the parsed number,
     * returns a negative value if {@code value} does not contain 8 digits
     */
    public static int tryToParseEightDigitsUtf8(byte[] a, int offset) {
        return tryToParseEightDigitsUtf8(getChunk(a, offset));
    }

    /**
     * Tries to parse eight hex digits from two longs using the
     * 'SIMD within a register technique' (SWAR).
     *
     * <pre>{@code
     * char[] chars = ...;
     * long first  = (long) chars[0] << 48
     *             | (long) chars[1] << 32
     *             | (long) chars[2] << 16
     *             | (long) chars[3];
     *
     * long second = (long) chars[4] << 48
     *             | (long) chars[5] << 32
     *             | (long) chars[6] << 16
     *             | (long) chars[7];
     * }</pre>
     *
     * @param first  contains 4 utf-16 characters in big endian order
     * @param second contains 4 utf-16 characters in big endian order
     * @return the parsed number,
     * returns a negative value if the two longs do not contain 8 hex digits
     */
    public static long tryToParseEightHexDigitsUtf16(long first, long second) {
        long lfirst = tryToParseFourHexDigitsUtf16(first);
        long lsecond = tryToParseFourHexDigitsUtf16(second);
        return (lfirst << 16) | lsecond;
    }

    /**
     * Tries to parse eight hex digits from a char array using the
     * 'SIMD within a register technique' (SWAR).
     *
     * @param a      contains 8 utf-16 characters starting at offset
     * @param offset the offset into the array
     * @return the parsed number,
     * returns a negative value if {@code value} does not contain 8 hex digits
     */
    public static long tryToParseEightHexDigitsUtf16(char[] a, int offset) {
        // Performance: We extract the chars in two steps so that we
        //              can benefit from out of order execution in the CPU.
        long first = (long) a[offset] << 48
                | (long) a[offset + 1] << 32
                | (long) a[offset + 2] << 16
                | (long) a[offset + 3];

        long second = (long) a[offset + 4] << 48
                | (long) a[offset + 5] << 32
                | (long) a[offset + 6] << 16
                | (long) a[offset + 7];

        return FastDoubleSwar.tryToParseEightHexDigitsUtf16(first, second);
    }

    /**
     * Tries to parse eight hex digits from a byte array using the
     * 'SIMD within a register technique' (SWAR).
     *
     * @param a      contains 8 ascii characters
     * @param offset the offset of the first character in {@code a}
     *               returns a negative value if {@code value} does not contain 8 digits
     */
    public static long tryToParseEightHexDigitsUtf8(byte[] a, int offset) {
        return tryToParseEightHexDigitsUtf8(
                Long.reverseBytes(getChunk(a, offset))
        );
    }

    /**
     * Tries to parse eight digits from a long using the
     * 'SIMD within a register technique' (SWAR).
     *
     * <pre>{@literal
     * byte[] bytes = ...;
     * long value  = ((bytes[7]&0xffL)<<56)
     *             | ((bytes[6]&0xffL)<<48)
     *             | ((bytes[5]&0xffL)<<40)
     *             | ((bytes[4]&0xffL)<<32)
     *             | ((bytes[3]&0xffL)<<24)
     *             | ((bytes[2]&0xffL)<<16)
     *             | ((bytes[1]&0xffL)<< 8)
     *             |  (bytes[0]&0xffL);
     * }</pre>
     *
     * @param chunk contains 8 ascii characters in little endian order
     * @return the parsed number,
     * returns a negative value if {@code value} does not contain 8 digits
     */
    static int tryToParseEightDigitsUtf8(long chunk) {
        // Create a predicate for all bytes which are greater than '0' (0x30).
        // The predicate is true if the hsb of a byte is set: (predicate & 0x80) != 0.
        long val = chunk - 0x3030303030303030L;
        long predicate = ((chunk + 0x4646464646464646L) | val) & 0x8080808080808080L;
        if (predicate != 0L) {
            return -1;
        }

        // The last 2 multiplications are independent of each other.
        val = val * (1 + (10 << 8)) >>> 8;
        val = (val & 0xff_000000ffL) * (100 + (100_0000L << 32))
                + (val >>> 16 & 0xff_000000ffL) * (1 + (1_0000L << 32)) >>> 32;
        return (int) val;
    }

    /**
     * Tries to parse eight digits from a long using the
     * 'SIMD within a register technique' (SWAR).
     *
     * @param chunk contains 8 ascii characters in big endian order
     * @return the parsed number,
     * returns a negative value if {@code value} does not contain 8 digits
     */
    static long tryToParseEightHexDigitsUtf8(long chunk) {
        // The following code is based on the technique presented in the paper
        // by Leslie Lamport.


        // Subtract character '0' (0x30) from each of the eight characters
        long vec = chunk - 0x30_30_30_30_30_30_30_30L;

        // Create a predicate for all bytes which are greater than '9'-'0' (0x09).
        // The predicate is true if the hsb of a byte is set: (predicate & 0x80) != 0.
        long gt_09 = vec + (0x09_09_09_09_09_09_09_09L ^ 0x7f_7f_7f_7f_7f_7f_7f_7fL);
        gt_09 &= 0x80_80_80_80_80_80_80_80L;
        // Create a predicate for all bytes which are greater or equal 'a'-'0' (0x30).
        // The predicate is true if the hsb of a byte is set.
        long ge_30 = vec + (0x30303030_30303030L ^ 0x7f_7f_7f_7f_7f_7f_7f_7fL);
        ge_30 &= 0x80_80_80_80_80_80_80_80L;

        // Create a predicate for all bytes which are smaller equal than 'f'-'0' (0x37).
        long le_37 = 0x37_37_37_37_37_37_37_37L + (vec ^ 0x7f_7f_7f_7f_7f_7f_7f_7fL);
        // we don't need to 'and' with 0x80…L here, because we 'and' this with ge_30 anyway.
        //le_37 &= 0x80_80_80_80_80_80_80_80L;


        // If a character is greater than '9' then it must be greater equal 'a'
        // and smaller  'f'.
        if (gt_09 != (ge_30 & le_37)) {
            return -1;
        }

        // Expand the predicate to a byte mask
        long gt_09mask = (gt_09 >>> 7) * 0xffL;

        // Subtract 'a'-'0'+10 (0x27) from all bytes that are greater than 0x09.
        long v = vec & ~gt_09mask | vec - (0x27272727_27272727L & gt_09mask);

        // Compact all nibbles
        long v2 = v | v >>> 4;
        long v3 = v2 & 0x00ff00ff_00ff00ffL;
        long v4 = v3 | v3 >>> 8;
        return ((v4 >>> 16) & 0xffff_0000L) | v4 & 0xffffL;
    }

    /**
     * Tries to parse four hex digits from a long using the
     * 'SIMD within a register technique' (SWAR).
     *
     * @param chunk contains 4 utf-16 characters in big endian order
     * @return the parsed number,
     * returns a negative value if {@code value} does not contain 8 digits
     */
    static long tryToParseFourHexDigitsUtf16(long chunk) {
        // The following code is based on the technique presented in the paper
        // by Leslie Lamport.


        // Subtract character '0' (0x0030) from each of the four characters
        long vec = chunk - 0x0030_0030_0030_0030L;

        // Create a predicate for all bytes which are greater than '9'-'0' (0x0009).
        // The predicate is true if the hsb of a byte is set: (predicate & 0xa000) != 0.
        long gt_09 = vec + (0x0009_0009_0009_0009L ^ 0x7fff_7fff_7fff_7fffL);
        gt_09 = gt_09 & 0x8000_8000_8000_8000L;
        // Create a predicate for all bytes which are greater or equal 'a'-'0' (0x0030).
        // The predicate is true if the hsb of a byte is set.
        long ge_30 = vec + (0x0030_0030_0030_0030L ^ 0x7fff_7fff_7fff_7fffL);
        ge_30 = ge_30 & 0x8000_8000_8000_8000L;

        // Create a predicate for all bytes which are smaller equal than 'f'-'0' (0x0037).
        long le_37 = 0x0037_0037_0037_0037L + (vec ^ 0x7fff_7fff_7fff_7fffL);
        // Not needed, because we are going to and this value with ge_30 anyway.
        //le_37 = le_37 & 0x8000_8000_8000_8000L;


        // If a character is greater than '9' then it must be greater equal 'a'
        // and smaller equal 'f'.
        if (gt_09 != (ge_30 & le_37)) {
            return -1;
        }

        // Expand the predicate to a char mask
        long gt_09mask = (gt_09 >>> 15) * 0xffffL;

        // Subtract 'a'-'0'+10 (0x0027) from all bytes that are greater than 0x09.
        long v = vec & ~gt_09mask | vec - (0x0027_0027_0027_0027L & gt_09mask);

        // Compact all nibbles
        long v2 = v | v >>> 12;
        return (v2 | v2 >>> 24) & 0xffffL;
    }
}
