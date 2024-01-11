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

    private static final long M2 = 0x7a646e4d;
    private static final int SPREAD_HASH_BITS = 0x7fffffff;
    private static final long MURMUR3_X64_128_C1 = 0x87c37b91114253d5L;
    private static final long MURMUR3_X64_128_C2 = 0x4cf5ad432745937fL;
    private static final int MURMUR3_SEED = 95967;

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

    /**
     * Hash function based on Knuth's multiplicative method.
     * It provides good distribution even for a sequence of keys.
     *
     * @param k the long for which the hash will be calculated
     * @return the hash
     */
    public static long fastLongMix(long k) {
        long h = k * M2;
        return h ^ h >>> 32;
    }

    public static int hashLong(long k) {
        return (int) fastLongMix(k);
    }

    public static int hashLong128(long key1, long key2) {
        return (int) fastLongMix(fastLongMix(key1) + key2);
    }

    /**
     * Same as {@link #hashMem32(long, long)}, but with on-heap char sequence
     * instead of direct unsafe access.
     */
    public static int hashMem32(Utf8String us) {
        final int len = us.size();
        long h = 0;
        int i = 0;
        for (; i + 7 < len; i += 8) {
            h = h * M2 + us.longAt(i);
        }
        if (i + 3 < len) {
            h = h * M2 + us.intAt(i);
            i += 4;
        }
        for (; i < len; i++) {
            h = h * M2 + us.byteAt(i);
        }
        h *= M2;
        return (int) (h ^ h >>> 32);
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
    public static int hashMem32(long p, long len) {
        long h = 0;
        int i = 0;
        for (; i + 7 < len; i += 8) {
            h = h * M2 + Unsafe.getUnsafe().getLong(p + i);
        }
        if (i + 3 < len) {
            h = h * M2 + Unsafe.getUnsafe().getInt(p + i);
            i += 4;
        }
        for (; i < len; i++) {
            h = h * M2 + Unsafe.getUnsafe().getByte(p + i);
        }
        h *= M2;
        return (int) (h ^ h >>> 32);
    }

    /**
     * Same as {@link #hashMem32(long, long)}, but with direct UTF8 string
     * instead of direct unsafe access.
     */
    public static int hashMem32(DirectUtf8Sequence seq) {
        return hashMem32(seq.lo(), seq.size());
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

    /**
     * The Murmur3 128-bit variant. Returns the 8 most significant bytes of the hash.
     */
    public static long murmur3ToLong(long key) {
        return murmur3ToLong(key, Long.BYTES);
    }

    /**
     * The Murmur3 128-bit variant. Returns the 8 most significant bytes of the hash.
     */
    public static long murmur3ToLong(int key) {
        return murmur3ToLong(key & 0xffffffffL, Integer.BYTES);
    }

    /**
     * This is an implementation of the Murmur3 128-bit variant, based on the MurmurHash3_x64_128
     * function from the original MurmurHash3 C++ code
     * (<a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp">SMHasher</a>).
     * <p>
     * Returns the 8 most significant bytes of the hash.
     */
    private static long murmur3ToLong(long key, int keySizeInBytes) {
        long k1 = key;
        long h1 = MURMUR3_SEED;
        long h2 = MURMUR3_SEED;
        k1 *= MURMUR3_X64_128_C1;
        k1 = Long.rotateLeft(k1, 31);
        k1 *= MURMUR3_X64_128_C2;
        h1 ^= k1;

        // finalization
        h1 ^= keySizeInBytes;
        h2 ^= keySizeInBytes;
        h1 += h2;
        h2 += h1;
        h1 = fmix64(h1);
        h2 = fmix64(h2);
        h1 += h2;
        return h1;
    }

    private static long fmix64(long h) {
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }
}
