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

public final class Hash {

    private static final long XXH_PRIME64_1 = -7046029288634856825L; /* 0b1001111000110111011110011011000110000101111010111100101010000111 */
    private static final long XXH_PRIME64_2 = -4417276706812531889L; /* 0b1100001010110010101011100011110100100111110101001110101101001111 */
    private static final long XXH_PRIME64_3 = 1609587929392839161L;  /* 0b0001011001010110011001111011000110011110001101110111100111111001 */
    private static final long XXH_PRIME64_4 = -8796714831421723037L; /* 0b1000010111101011110010100111011111000010101100101010111001100011 */
    private static final long XXH_PRIME64_5 = 2870177450012600261L;  /* 0b0010011111010100111010110010111100010110010101100110011111000101 */

    private static final int SPREAD_HASH_BITS = 0x7fffffff;

    private static final MemoryAccessor unsafeAccessor = new MemoryAccessor() {
        @Override
        public long getLong(long offset) {
            return Unsafe.getUnsafe().getLong(offset);
        }

        @Override
        public int getInt(long offset) {
            return Unsafe.getUnsafe().getInt(offset);
        }

        @Override
        public byte getByte(long offset) {
            return Unsafe.getUnsafe().getByte(offset);
        }
    };

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
     * Calculates positive integer hash of memory pointer using 32-bit variant of xxHash hash algorithm.
     *
     * @param p   memory pointer
     * @param len memory length in bytes
     * @return hash code
     */
    public static long hashMem(long p, long len) {
        return xxHash64(p, len, 0, unsafeAccessor);
    }

    /**
     * Calculates positive integer hash of memory pointer using 64-bit variant of xxHash hash algorithm.
     *
     * @param p        memory pointer
     * @param len      memory length in bytes
     * @param seed     seed value
     * @param accessor memory accessor that provides access to memory
     * @return hash code
     */
    public static long xxHash64(long p, long len, long seed, MemoryAccessor accessor) {
        long h64;
        final long end = p + len;

        if (len >= 32) {
            final long lim = end - 32;
            long v1 = seed + XXH_PRIME64_1 + XXH_PRIME64_2;
            long v2 = seed + XXH_PRIME64_2;
            long v3 = seed;
            long v4 = seed - XXH_PRIME64_1;
            do {
                v1 += accessor.getLong(p) * XXH_PRIME64_2;
                v1 = Long.rotateLeft(v1, 31);
                v1 *= XXH_PRIME64_1;
                p += 8;

                v2 += accessor.getLong(p) * XXH_PRIME64_2;
                v2 = Long.rotateLeft(v2, 31);
                v2 *= XXH_PRIME64_1;
                p += 8;

                v3 += accessor.getLong(p) * XXH_PRIME64_2;
                v3 = Long.rotateLeft(v3, 31);
                v3 *= XXH_PRIME64_1;
                p += 8;

                v4 += accessor.getLong(p) * XXH_PRIME64_2;
                v4 = Long.rotateLeft(v4, 31);
                v4 *= XXH_PRIME64_1;
                p += 8;
            } while (p <= lim);

            h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7)
                    + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

            v1 *= XXH_PRIME64_2;
            v1 = Long.rotateLeft(v1, 31);
            v1 *= XXH_PRIME64_1;
            h64 ^= v1;
            h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;

            v2 *= XXH_PRIME64_2;
            v2 = Long.rotateLeft(v2, 31);
            v2 *= XXH_PRIME64_1;
            h64 ^= v2;
            h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;

            v3 *= XXH_PRIME64_2;
            v3 = Long.rotateLeft(v3, 31);
            v3 *= XXH_PRIME64_1;
            h64 ^= v3;
            h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;

            v4 *= XXH_PRIME64_2;
            v4 = Long.rotateLeft(v4, 31);
            v4 *= XXH_PRIME64_1;
            h64 ^= v4;
            h64 = h64 * XXH_PRIME64_1 + XXH_PRIME64_4;
        } else {
            h64 = seed + XXH_PRIME64_5;
        }

        h64 += len;

        while (p <= end - 8) {
            long k1 = accessor.getLong(p);
            k1 *= XXH_PRIME64_2;
            k1 = Long.rotateLeft(k1, 31);
            k1 *= XXH_PRIME64_1;
            h64 ^= k1;
            h64 = Long.rotateLeft(h64, 27) * XXH_PRIME64_1 + XXH_PRIME64_4;
            p += 8;
        }

        if (p <= end - 4) {
            h64 ^= (accessor.getInt(p) & 0xFFFFFFFFL) * XXH_PRIME64_1;
            h64 = Long.rotateLeft(h64, 23) * XXH_PRIME64_2 + XXH_PRIME64_3;
            p += 4;
        }

        while (p < end) {
            h64 ^= (accessor.getByte(p) & 0xFF) * XXH_PRIME64_5;
            h64 = Long.rotateLeft(h64, 11) * XXH_PRIME64_1;
            ++p;
        }

        // Mix all bits to finalize the hash.
        h64 ^= h64 >>> 33;
        h64 *= XXH_PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= XXH_PRIME64_3;
        h64 ^= h64 >>> 32;

        return h64;
    }

    /**
     * (copied from ConcurrentHashMap)
     * Spreads (XORs) higher bits of hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a trade off between speed, utility, and
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

    public interface MemoryAccessor {
        long getLong(long offset);

        int getInt(long offset);

        byte getByte(long offset);
    }
}
