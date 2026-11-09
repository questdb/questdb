/*+*****************************************************************************
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

package io.questdb.std;

public class Long256Util {

    public static void add(Long256 dst, long v0, long v1, long v2, long v3) {
        boolean isNull = v0 == Numbers.LONG_NULL
                && v1 == Numbers.LONG_NULL
                && v2 == Numbers.LONG_NULL
                && v3 == Numbers.LONG_NULL;

        if (isNull) {
            dst.setAll(
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL,
                    Numbers.LONG_NULL
            );
        } else {
            // The sum will overflow if both top bits are set (x & y) or if one of them
            // is (x | y), and a carry from the lower place happened. If such a carry
            // happens, the top bit will be 1 + 0 + 1 = 0 (& ~sum).
            long carry = 0;
            final long l0 = v0 + dst.getLong0() + carry;
            carry = ((v0 & dst.getLong0()) | ((v0 | dst.getLong0()) & ~l0)) >>> 63;

            final long l1 = v1 + dst.getLong1() + carry;
            carry = ((v1 & dst.getLong1()) | ((v1 | dst.getLong1()) & ~l1)) >>> 63;

            final long l2 = v2 + dst.getLong2() + carry;
            carry = ((v2 & dst.getLong2()) | ((v2 | dst.getLong2()) & ~l2)) >>> 63;

            final long l3 = v3 + dst.getLong3() + carry;
            //carry = ((v3 & dst.getLong3()) | ((v3 | dst.getLong3()) & ~l3)) >>> 63;

            dst.setAll(l0, l1, l2, l3);
        }
    }

    public static void add(Long256 acc, Long256 incr) {
        add(acc, incr.getLong0(), incr.getLong1(), incr.getLong2(), incr.getLong3());
    }

    // this method is used by byte-code generator
    public static int compare(Long256 a, Long256 b) {

        // Special handling for NULLs. Q: Why is it needed?
        // A: We use 4xLong.MIN_VALUEs to represent Long256 null value.
        // However, Long256 is unsigned and Long.MIN_VALUE has the highest bit is 1, all others bits are 0.
        // Without this special handling some values would be considered higher than NULL and some lower.
        // Having nulls values in the middle of the range would be confusing. INT and LONG types consider NULL
        // to be the lowest possible value so we follow the same convention here.
        if (Long256Impl.isNull(a)) {
            return Long256Impl.isNull(b) ? 0 : -1;
        } else if (Long256Impl.isNull(b)) {
            return 1;
        }

        int cmp = Long.compareUnsigned(a.getLong3(), b.getLong3());
        if (cmp != 0) {
            return cmp;
        }

        cmp = Long.compareUnsigned(a.getLong2(), b.getLong2());
        if (cmp != 0) {
            return cmp;
        }

        cmp = Long.compareUnsigned(a.getLong1(), b.getLong1());
        if (cmp != 0) {
            return cmp;
        }

        return Long.compareUnsigned(a.getLong0(), b.getLong0());
    }

    /**
     * The unsigned value of a LONG256 that is not NULL, rounded to the nearest double, ties to even.
     */
    public static double toDouble(Long256 value) {
        final long top = topBits(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
        final double scaled = top < 0 ? (double) (top >>> 1 | top & 1) * 2.0 : (double) top;
        return Math.scalb(scaled, topScale(value.getLong1(), value.getLong2(), value.getLong3()));
    }

    /**
     * The unsigned value of a LONG256 that is not NULL, rounded to the nearest float, ties to even, and infinity past
     * the largest float.
     */
    public static float toFloat(Long256 value) {
        final long top = topBits(value.getLong0(), value.getLong1(), value.getLong2(), value.getLong3());
        final float scaled = top < 0 ? (float) (top >>> 1 | top & 1) * 2.0f : (float) top;
        return Math.scalb(scaled, topScale(value.getLong1(), value.getLong2(), value.getLong3()));
    }

    /**
     * The 64 bits of {@code hi:lo:rest} from the highest set bit of {@code hi} down, with bit 0 also set when a lower
     * bit is, so that rounding the bits to a double or a float rounds the whole value.
     */
    private static long normalize(long hi, long lo, long rest) {
        final int shift = Long.numberOfLeadingZeros(hi);
        final long sticky = (shift == 0 ? lo : lo << shift) != 0 || rest != 0 ? 1 : 0;
        return shift == 0 ? hi | sticky : hi << shift | lo >>> (64 - shift) | sticky;
    }

    /**
     * The value scaled down by {@code 2^topScale}: its highest 64 bits, see {@link #normalize}, or the value itself
     * when it fits.
     */
    private static long topBits(long l0, long l1, long l2, long l3) {
        if (l3 != 0) {
            return normalize(l3, l2, l1 | l0);
        }
        if (l2 != 0) {
            return normalize(l2, l1, l0);
        }
        return l1 != 0 ? normalize(l1, l0, 0) : l0;
    }

    private static int topScale(long l1, long l2, long l3) {
        if (l3 != 0) {
            return 192 - Long.numberOfLeadingZeros(l3);
        }
        if (l2 != 0) {
            return 128 - Long.numberOfLeadingZeros(l2);
        }
        return l1 != 0 ? 64 - Long.numberOfLeadingZeros(l1) : 0;
    }
}
