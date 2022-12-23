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

import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.std.str.DirectByteCharSequence;

public final class Hash {

    private static final int M2 = 0x7a646e4d;
    private static final int SPREAD_HASH_BITS = 0x7fffffff;

    private Hash() {
    }

    /**
     * Restricts hashCode() of the underlying char sequence to be no greater than max.
     *
     * @param s   char sequence
     * @param max max value of hashCode()
     * @return power of 2 integer
     */
    public static int boundedHash(CharSequence s, int max) {
        return s == null ? -1 : (Chars.hashCode(s) & 0xFFFFFFF) & max;
    }

    /**
     * Same as {@link #hashMem32(long, long)}, but with direct UTF8 char sequence
     * instead of direct unsafe access.
     */
    public static int hashMem32(DirectByteCharSequence charSequence) {
        return hashMem32(charSequence.getLo(), charSequence.length());
    }

    /**
     * 32-bit variant of {@link #hashMem64(long, long)}.
     */
    public static int hashMem32(long p, long len) {
        long h = 0;
        int i = 0;
        for (; i + 3 < len; i += 4) {
            h = h * M2 + Unsafe.getUnsafe().getInt(p + i);
        }
        for (; i < len; i++) {
            h = h * M2 + Unsafe.getUnsafe().getByte(p + i);
        }
        h *= M2;
        return (int) h ^ (int) (h >>> 25);
    }

    /**
     * Same as {@link #hashMem32(long, long)}, but with a UTF8 re-encoded string
     * instead of direct unsafe access.
     * <p>
     * Important note:
     * The string is interpreted as a sequence of UTF8 bytes. It means that each
     * string's char is trimmed to a byte when calculating the hash code.
     */
    public static int hashMem32(String utf8String) {
        final int len = utf8String.length();
        long h = 0;
        int i = 0;
        for (; i + 3 < len; i += 4) {
            h = h * M2 + intFromUtf8String(utf8String, i);
        }
        for (; i < len; i++) {
            h = h * M2 + byteFromUtf8String(utf8String, i);
        }
        h *= M2;
        return (int) h ^ (int) (h >>> 25);
    }

    /**
     * Calculates positive integer hash of memory pointer using a polynomial
     * hash function.
     * <p>
     * The function is a modified version of the function from
     * <a href="https://vanilla-java.github.io/2018/08/15/Looking-at-randomness-and-performance-for-hash-codes.html">this article</a>
     * by Peter Lawrey.
     *
     * @param p   memory pointer
     * @param len memory length in bytes
     * @return hash code
     */
    public static long hashMem64(long p, long len) {
        long h = 0;
        int i = 0;
        for (; i + 3 < len; i += 4) {
            h = h * M2 + Unsafe.getUnsafe().getInt(p + i);
        }
        for (; i < len; i++) {
            h = h * M2 + Unsafe.getUnsafe().getByte(p + i);
        }
        h *= M2;
        return h ^ (h >>> 25);
    }

    /**
     * Same as {@link #hashMem64(long, long)}, but with MemoryR instead of direct
     * unsafe access.
     */
    public static long hashMem64(long p, long len, MemoryR memory) {
        long h = 0;
        int i = 0;
        for (; i + 3 < len; i += 4) {
            h = h * M2 + memory.getInt(p + i);
        }
        for (; i < len; i++) {
            h = h * M2 + memory.getByte(p + i);
        }
        h *= M2;
        return h ^ (h >>> 25);
    }

    /**
     * (copied from ConcurrentHashMap)
     * Spreads (XORs) higher bits of hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a trade-off between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     *
     * @param h hash code
     * @return adjusted hash code
     */
    public static int spread(int h) {
        return (h ^ (h >>> 16)) & SPREAD_HASH_BITS;
    }

    private static int byteAsUnsignedInt(String utf8String, int index) {
        return ((byte) utf8String.charAt(index)) & 0xff;
    }

    private static byte byteFromUtf8String(String utf8String, int index) {
        return (byte) utf8String.charAt(index);
    }

    private static int intFromUtf8String(String utf8String, int index) {
        final int n = byteAsUnsignedInt(utf8String, index)
                | (byteAsUnsignedInt(utf8String, index + 1) << 8)
                | (byteAsUnsignedInt(utf8String, index + 2) << 16)
                | (byteAsUnsignedInt(utf8String, index + 3) << 24);
        return Unsafe.isLittleEndian() ? n : Integer.reverseBytes(n);
    }
}
