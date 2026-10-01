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

package io.questdb.cairo.idx;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Split Block Bloom Filter (SBBF) operating on native memory.
 * <p>
 * Implements the Apache Parquet SBBF algorithm: 32-byte blocks (one cache line),
 * 8 salt values for hash probes, standard xxHash64 (seed=0) for key hashing.
 * SWAR-accelerated probes — each probe reads 4 longs and performs bitwise
 * AND-checks with zero JNI overhead.
 * <p>
 * Sizing formula: m = -8 * ndv / ln(1 - fpp^(1/8)), rounded up to next multiple
 * of 32 bytes.
 */
public final class SplitBlockBloomFilter {
    public static final int BLOCK_SIZE = 32; // bytes, one cache line
    public static final int BLOCK_SIZE_SHIFT = 5; // log2(32)

    // 8 Parquet SBBF salt constants
    private static final int SALT_0 = 0x47b6137b;
    private static final int SALT_1 = 0x44974d91;
    private static final int SALT_2 = 0x8824ad5b;
    private static final int SALT_3 = 0xa2b7289d;
    private static final int SALT_4 = 0x705495c7;
    private static final int SALT_5 = 0x2df1424b;
    private static final int SALT_6 = 0x9efc4947;
    private static final int SALT_7 = 0x5c6bfb31;

    // xxHash64 PRIME constants.
    private static final long XXH_P1 = 0x9E3779B185EBCA87L;
    private static final long XXH_P2 = 0xC2B2AE3D27D4EB4FL;
    private static final long XXH_P3 = 0x165667B19E3779F9L;
    private static final long XXH_P5 = 0x27D4EB2F165667C5L;

    private SplitBlockBloomFilter() {
    }

    public static long allocate(int filterSize) {
        long addr = Unsafe.malloc(filterSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.setMemory(addr, filterSize, (byte) 0);
        return addr;
    }

    public static int computeSize(int numDistinctValues, double fpp) {
        assert fpp > 0.0 && fpp < 1.0 : "fpp must be in (0, 1), got " + fpp;
        if (numDistinctValues <= 0) {
            return BLOCK_SIZE;
        }
        // m = -8 * ndv / ln(1 - fpp^(1/8))
        double m = -8.0 * numDistinctValues / Math.log(1.0 - Math.pow(fpp, 1.0 / 8.0));
        long bytes = (long) Math.ceil(m / 8.0);
        bytes = Math.min(bytes, 128 * 1024 * 1024L);
        int intBytes = (int) bytes;
        intBytes = Math.max(BLOCK_SIZE, ((intBytes + BLOCK_SIZE - 1) >> BLOCK_SIZE_SHIFT) << BLOCK_SIZE_SHIFT);
        return intBytes;
    }

    public static void free(long addr, int filterSize) {
        if (addr != 0) {
            Unsafe.free(addr, filterSize, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    public static long hashKey(int key) {
        long h = XXH_P5 + 4L;
        h ^= (key & 0xFFFFFFFFL) * XXH_P1;
        h = Long.rotateLeft(h, 23) * XXH_P2 + XXH_P3;
        h ^= h >>> 33;
        h *= XXH_P2;
        h ^= h >>> 29;
        h *= XXH_P3;
        h ^= h >>> 32;
        return h;
    }

    public static void insert(long filterAddr, int filterSize, long hash) {
        int numBlocks = filterSize >> BLOCK_SIZE_SHIFT;
        int blockIndex = (int) ((hash >>> 32) * numBlocks >>> 32);
        long blockAddr = filterAddr + ((long) blockIndex << BLOCK_SIZE_SHIFT);

        int key = (int) hash;

        // Load block as 4 longs
        long w01 = Unsafe.getLong(blockAddr);
        long w23 = Unsafe.getLong(blockAddr + 8);
        long w45 = Unsafe.getLong(blockAddr + 16);
        long w67 = Unsafe.getLong(blockAddr + 24);

        // Compute 8 masks: 1 << ((key * SALT[i]) >>> 27), packed pairwise into longs
        w01 |= mask01(key);
        w23 |= mask23(key);
        w45 |= mask45(key);
        w67 |= mask67(key);

        Unsafe.putLong(blockAddr, w01);
        Unsafe.putLong(blockAddr + 8, w23);
        Unsafe.putLong(blockAddr + 16, w45);
        Unsafe.putLong(blockAddr + 24, w67);
    }

    public static boolean mightContain(long filterAddr, int filterSize, long hash) {
        int numBlocks = filterSize >> BLOCK_SIZE_SHIFT;
        int blockIndex = (int) ((hash >>> 32) * numBlocks >>> 32);
        long blockAddr = filterAddr + ((long) blockIndex << BLOCK_SIZE_SHIFT);

        int key = (int) hash;

        long w01 = Unsafe.getLong(blockAddr);
        long w23 = Unsafe.getLong(blockAddr + 8);
        long w45 = Unsafe.getLong(blockAddr + 16);
        long w67 = Unsafe.getLong(blockAddr + 24);

        long m01 = mask01(key);
        long m23 = mask23(key);
        long m45 = mask45(key);
        long m67 = mask67(key);

        return (w01 & m01) == m01
                && (w23 & m23) == m23
                && (w45 & m45) == m45
                && (w67 & m67) == m67;
    }

    private static long mask01(int key) {
        long m0 = 1L << ((key * SALT_0) >>> 27);
        long m1 = 1L << (((key * SALT_1) >>> 27) + 32);
        return m0 | m1;
    }

    private static long mask23(int key) {
        long m2 = 1L << ((key * SALT_2) >>> 27);
        long m3 = 1L << (((key * SALT_3) >>> 27) + 32);
        return m2 | m3;
    }

    private static long mask45(int key) {
        long m4 = 1L << ((key * SALT_4) >>> 27);
        long m5 = 1L << (((key * SALT_5) >>> 27) + 32);
        return m4 | m5;
    }

    private static long mask67(int key) {
        long m6 = 1L << ((key * SALT_6) >>> 27);
        long m7 = 1L << (((key * SALT_7) >>> 27) + 32);
        return m6 | m7;
    }
}
