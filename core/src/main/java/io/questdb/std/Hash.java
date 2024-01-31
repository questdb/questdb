/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Utf8String;

public final class Hash {

    // Constant from FxHasher.
    private static final long M = 0x517cc1b727220a95L;
    private static final int R_DIST = 17;
    private static final int SPREAD_HASH_BITS = 0x7fffffff;

    private Hash() {
    }

    /**
     * Restricts hashCode() of the underlying char sequence to be no greater than max.
     *
     * @param seq char sequence
     * @param max max value of hashCode()
     * @return power of 2 integer
     */
    public static int boundedHash(CharSequence seq, int max) {
        return seq == null ? -1 : (Chars.hashCode(seq) & 0xFFFFFFF) & max;
    }

    public static int hash32Long(long k) {
        return Numbers.decodeLowInt(hash64Long(k));
    }

    public static int hash32Long128(long key1, long key2) {
        return Numbers.decodeLowInt(hash64Long128(key1, key2));
    }

    /**
     * Same as {@link #hash64Mem(long, long)}, but with on-heap char sequence
     * instead of direct unsafe access.
     */
    public static int hash32Utf8(Utf8String us) {
        final int len = us.size();
        long h = 0;
        int i = 0;
        for (; i + 7 < len; i += 8) {
            h = Long.rotateLeft(h * M, R_DIST);
            h ^= us.longAt(i);
        }
        if (i + 3 < len) {
            h = Long.rotateLeft(h * M, R_DIST);
            h ^= us.intAt(i);
            i += 4;
        }
        for (; i < len; i++) {
            h = Long.rotateLeft(h * M, R_DIST);
            h ^= us.byteAt(i);
        }
        return Numbers.decodeLowInt(Long.rotateLeft(h * M, R_DIST));
    }

    /**
     * Same as {@link #hash64Mem(long, long)}, but with direct UTF8 string
     * instead of direct unsafe access.
     */
    public static int hash32Utf8(DirectUtf8Sequence seq) {
        return Numbers.decodeLowInt(hash64Mem(seq.lo(), seq.size()));
    }

    public static long hash64Long(long k) {
        return Long.rotateLeft(k * M, R_DIST);
    }

    public static long hash64Long128(long k1, long k2) {
        long h = Long.rotateLeft(k1 * M, R_DIST);
        h ^= k2;
        return Long.rotateLeft(h * M, R_DIST);
    }

    public static long hash64Long256(long k1, long k2, long k3, long k4) {
        long h = Long.rotateLeft(k1 * M, R_DIST);
        h ^= k2;
        h = Long.rotateLeft(h * M, R_DIST);
        h ^= k3;
        h = Long.rotateLeft(h * M, R_DIST);
        h ^= k4;
        return Long.rotateLeft(h, R_DIST);
    }

    /**
     * Calculates positive integer hash of memory pointer using a polynomial
     * hash function.
     * <p>
     * The function is a heavily modified version of the function from
     * <a href="https://vanilla-java.github.io/2018/08/15/Looking-at-randomness-and-performance-for-hash-codes.html">this article</a>
     * by Peter Lawrey. It also borrows ideas from Rust compiler's FxHash.
     *
     * @param p   memory pointer
     * @param len memory length in bytes
     * @return hash code
     */
    public static long hash64Mem(long p, long len) {
        long h = 0;
        int i = 0;
        for (; i + 7 < len; i += 8) {
            h = Long.rotateLeft(h * M, R_DIST);
            h ^= Unsafe.getUnsafe().getLong(p + i);
        }
        if (i + 3 < len) {
            h = Long.rotateLeft(h * M, R_DIST);
            h ^= Unsafe.getUnsafe().getInt(p + i);
            i += 4;
        }
        for (; i < len; i++) {
            h = Long.rotateLeft(h * M, R_DIST);
            h ^= Unsafe.getUnsafe().getByte(p + i);
        }
        return Long.rotateLeft(h * M, R_DIST);
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
     * cheapest possible way to reduce systematic loss, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     *
     * @param h hash code
     * @return adjusted hash code
     */
    public static int spread(int h) {
        return (h ^ (h >>> 16)) & SPREAD_HASH_BITS;
    }
}
