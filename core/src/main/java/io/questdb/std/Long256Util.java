/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.ImplicitCastException;

public class Long256Util {

    public static void add(Long256 dst, long v0, long v1, long v2, long v3) {
        boolean isNull = v0 == Numbers.LONG_NaN &&
                v1 == Numbers.LONG_NaN &&
                v2 == Numbers.LONG_NaN &&
                v3 == Numbers.LONG_NaN;

        if (isNull) {
            dst.setAll(Numbers.LONG_NaN,
                    Numbers.LONG_NaN,
                    Numbers.LONG_NaN,
                    Numbers.LONG_NaN);
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

        if (a.getLong3() < b.getLong3()) {
            return -1;
        }

        if (a.getLong3() > b.getLong3()) {
            return 1;
        }

        if (a.getLong2() < b.getLong2()) {
            return -1;
        }

        if (a.getLong2() > b.getLong2()) {
            return 1;
        }

        if (a.getLong1() < b.getLong1()) {
            return -1;
        }

        if (a.getLong1() > b.getLong1()) {
            return 1;
        }

        return Long.compare(a.getLong0(), b.getLong0());
    }

    public static void decodeDec(
            final CharSequence decString,
            final int startPos,
            final int endPos,
            final Long256 long256
    ) {
        final int len = endPos - startPos;
        if (len > 78) {
            throw ImplicitCastException.inconvertibleValue(decString, ColumnType.STRING, ColumnType.LONG256);
        }
        long256.setAll(0, 0, 0, 0);
        //todo: check for overflow
        for (int i = 0; i < len; i++) {
            try {
                long l = Numbers.parseLong(decString, startPos + i, startPos + i + 1);
                Long256Util.multipleBy10(long256);
                Long256Util.add(long256, l, 0, 0, 0);
            } catch (NumericException e) {
                throw ImplicitCastException.inconvertibleValue(decString, ColumnType.STRING, ColumnType.LONG256);
            }
        }
    }

    /**
     * Divide Long256 by a given positive integer and return remainder.
     *
     * @param l Long256 to divide
     * @param i integer to divide by
     * @return remainder
     */
    public static int divideByInt(Long256 l, int i) {
        assert i > 0;

        long r = 0;
        long l0 = l.getLong0();
        long l1 = l.getLong1();
        long l2 = l.getLong2();
        long l3 = l.getLong3();

        if (l3 != 0) {
            r = Long.remainderUnsigned(l3, i);
            l3 = Long.divideUnsigned(l3, i);
        }

        // todo: remove remainderUnsigned() - we can replace it with multiplication and subtraction, that should be faster 
        if (l2 != 0 || r != 0) {
            long hi = l2 >>> 32;
            long lo = l2 & 0xFFFFFFFFL;
            long wrk = r << 32 | hi;
            long newHi = Long.divideUnsigned(wrk, i);
            r = Long.remainderUnsigned(wrk, i);
            wrk = r << 32 | lo;
            long newLo = Long.divideUnsigned(wrk, i);
            r = Long.remainderUnsigned(wrk, i);
            l2 = newHi << 32 | newLo;
        }

        if (l1 != 0 || r != 0) {
            long hi = l1 >>> 32;
            long lo = l1 & 0xFFFFFFFFL;
            long wrk = r << 32 | hi;
            long newHi = Long.divideUnsigned(wrk, i);
            r = Long.remainderUnsigned(wrk, i);
            wrk = r << 32 | lo;
            long newLo = Long.divideUnsigned(wrk, i);
            r = Long.remainderUnsigned(wrk, i);
            l1 = newHi << 32 | newLo;
        }

        if (l0 != 0 || r != 0) {
            long hi = l0 >>> 32;
            long lo = l0 & 0xFFFFFFFFL;
            long wrk = r << 32 | hi;
            long newHi = Long.divideUnsigned(wrk, i);
            r = Long.remainderUnsigned(wrk, i);
            wrk = r << 32 | lo;
            long newLo = Long.divideUnsigned(wrk, i);
            r = Long.remainderUnsigned(wrk, i);
            l0 = newHi << 32 | newLo;
        }

        l.setAll(l0, l1, l2, l3);
        return (int) (r & 0xFFFFFFFFL);
    }

    public static boolean isZero(Long256 l) {
        return l.getLong0() == 0 && l.getLong1() == 0 && l.getLong2() == 0 && l.getLong3() == 0;
    }

    public static void leftShift(Long256 l, int bits) {
        assert bits >= 0;
        if (bits == 0) {
            return;
        }
        if (bits > 255) {
            l.setAll(0, 0, 0, 0);
            return;
        }

        // a long value shifted by 64 bits is not 0 as one could expect, but it's the original value.
        // thus we need special cases when shifting by multiple of 64 bits: 64, 128, 192
        if (bits < 64) {
            l.setAll(
                    l.getLong0() << bits,
                    (l.getLong0() >>> (64 - bits)) | (l.getLong1() << bits),
                    (l.getLong1() >>> (64 - bits)) | (l.getLong2() << bits),
                    (l.getLong2() >>> (64 - bits)) | (l.getLong3() << bits)
            );
        } else if (bits == 64) {
            l.setAll(
                    0L,
                    l.getLong0(),
                    l.getLong1(),
                    l.getLong2()
            );
        } else if (bits < 128) {
            l.setAll(
                    0,
                    l.getLong0() << (bits - 64),
                    (l.getLong0() >>> (128 - bits)) | (l.getLong1() << (bits - 64)),
                    (l.getLong1() >>> (128 - bits)) | (l.getLong2() << (bits - 64))
            );
        } else if (bits == 128) {
            l.setAll(
                    0,
                    0,
                    l.getLong0(),
                    l.getLong1()
            );
        } else if (bits < 192) {
            l.setAll(
                    0,
                    0,
                    l.getLong0() << (bits - 128),
                    (l.getLong0() >>> (192 - bits)) | (l.getLong1() << (bits - 128))
            );
        } else if (bits == 192) {
            l.setAll(
                    0,
                    0,
                    0,
                    l.getLong0()
            );
        } else {
            l.setAll(
                    0,
                    0,
                    0,
                    l.getLong0() << (bits - 192)
            );
        }
    }

    public static void multipleBy10(Long256 l) {
        long l0 = l.getLong0();
        long l1 = l.getLong1();
        long l2 = l.getLong2();
        long l3 = l.getLong3();

        // (8 * x) + x + x == 10 * x

        // shift left by 3 bits is the same as multiply by 8
        leftShift(l, 3);

        // add the original value
        add(l, l0, l1, l2, l3);
        // add the original value again
        add(l, l0, l1, l2, l3);
    }

    public static void multipleBy10000(Long256 l) {
        if (isZero(l)) {
            return;
        }
        multipleBy10(l);
        multipleBy10(l);
        multipleBy10(l);
        multipleBy10(l);
    }

    public static void multipleByInt(Long256 l, int i) {
        if (i == 0) {
            l.setAll(0, 0, 0, 0);
            return;
        }
        if (i == 1) {
            return;
        }
        if (i < 0) {
            throw new IllegalArgumentException("i must be positive");
        }
        long l0 = l.getLong0();
        long l1 = l.getLong1();
        long l2 = l.getLong2();
        long l3 = l.getLong3();

        long carry;
        long wrk = (l0 & 0xFFFFFFFFL) * i;
        long ni0 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l0 >>> 32) * i + carry;
        long ni1 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l1 & 0xFFFFFFFFL) * i + carry;
        long ni2 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l1 >>> 32) * i + carry;
        long ni3 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l2 & 0xFFFFFFFFL) * i + carry;
        long ni4 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l2 >>> 32) * i + carry;
        long ni5 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l3 & 0xFFFFFFFFL) * i + carry;
        long ni6 = wrk & 0xFFFFFFFFL;
        carry = wrk >>> 32;

        wrk = (l3 >>> 32) * i + carry;
        long ni7 = wrk & 0xFFFFFFFFL;
//        curry = wrk >>> 32;

        long nl0 = ni1 << 32 | ni0 & 0xFFFFFFFFL;
        long nl1 = ni3 << 32 | ni2 & 0xFFFFFFFFL;
        long nl2 = ni5 << 32 | ni4 & 0xFFFFFFFFL;
        long nl3 = ni7 << 32 | ni6 & 0xFFFFFFFFL;

        l.setAll(nl0, nl1, nl2, nl3);
    }
}
