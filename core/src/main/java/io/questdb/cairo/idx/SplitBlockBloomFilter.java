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

package io.questdb.cairo.idx;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Split Block Bloom Filter (SBBF) operating on native memory.
 * <p>
 * Uses the same constants as Apache Parquet's SBBF implementation:
 * 32-byte blocks (one cache line), 8 salt values for hash probes.
 * SWAR-accelerated probes — each probe reads 4 longs and performs
 * bitwise AND-checks with zero JNI overhead.
 * <p>
 * Sizing formula: m = -8 * ndv / ln(1 - fpp^(1/8)), rounded up to
 * next multiple of 32 bytes.
 */
final class SplitBlockBloomFilter {
    static final int BLOCK_SIZE = 32; // bytes, one cache line
    static final int BLOCK_SIZE_SHIFT = 5; // log2(32)

    // 8 Parquet salt constants for hash probes
    private static final int SALT_0 = 0x47b6137b;
    private static final int SALT_1 = 0x44974d91;
    private static final int SALT_2 = 0x8824ad5b;
    private static final int SALT_3 = 0xa2b7289d;
    private static final int SALT_4 = 0x705495c7;
    private static final int SALT_5 = 0x2df1424b;
    private static final int SALT_6 = 0x9efc4947;
    private static final int SALT_7 = 0x5c6bfb31;

    private SplitBlockBloomFilter() {
    }

    static long allocate(int filterSize) {
        long addr = Unsafe.malloc(filterSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.getUnsafe().setMemory(addr, filterSize, (byte) 0);
        return addr;
    }

    static int computeSize(int numDistinctValues, double fpp) {
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

    static void free(long addr, int filterSize) {
        if (addr != 0) {
            Unsafe.free(addr, filterSize, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    static long hashKey(int key) {
        // xxHash64 with seed=0, single 32-bit input
        long h = 0x9E3779B97F4A7C15L; // PRIME64_5
        h += 4; // length
        long k1 = (key & 0xFFFFFFFFL) * 0xC2B2AE3D27D4EB4FL; // PRIME64_1 * input
        k1 = Long.rotateLeft(k1, 23) * 0x9E3779B97F4A7C15L;
        h ^= k1;
        h = Long.rotateLeft(h, 37) * 0x9E3779B97F4A7C15L + 0x85EBCA77C2B2AE63L;

        // Avalanche
        h ^= h >>> 33;
        h *= 0xFF51AFD7ED558CCDL;
        h ^= h >>> 33;
        h *= 0xC4CEB9FE1A85EC53L;
        h ^= h >>> 33;
        return h;
    }

    static void insert(long filterAddr, int filterSize, long hash) {
        int numBlocks = filterSize >> BLOCK_SIZE_SHIFT;
        int blockIndex = (int) (((hash >>> 32) & 0xFFFFFFFFL) * numBlocks >>> 32);
        long blockAddr = filterAddr + ((long) blockIndex << BLOCK_SIZE_SHIFT);

        int key = (int) hash;

        // Load block as 4 longs
        long w01 = Unsafe.getUnsafe().getLong(blockAddr);
        long w23 = Unsafe.getUnsafe().getLong(blockAddr + 8);
        long w45 = Unsafe.getUnsafe().getLong(blockAddr + 16);
        long w67 = Unsafe.getUnsafe().getLong(blockAddr + 24);

        // Compute 8 masks: 1 << ((key * SALT[i]) >>> 27), packed pairwise into longs
        w01 |= mask01(key);
        w23 |= mask23(key);
        w45 |= mask45(key);
        w67 |= mask67(key);

        Unsafe.getUnsafe().putLong(blockAddr, w01);
        Unsafe.getUnsafe().putLong(blockAddr + 8, w23);
        Unsafe.getUnsafe().putLong(blockAddr + 16, w45);
        Unsafe.getUnsafe().putLong(blockAddr + 24, w67);
    }

    static boolean mightContain(long filterAddr, int filterSize, long hash) {
        int numBlocks = filterSize >> BLOCK_SIZE_SHIFT;
        int blockIndex = (int) (((hash >>> 32) & 0xFFFFFFFFL) * numBlocks >>> 32);
        long blockAddr = filterAddr + ((long) blockIndex << BLOCK_SIZE_SHIFT);

        int key = (int) hash;

        long w01 = Unsafe.getUnsafe().getLong(blockAddr);
        long w23 = Unsafe.getUnsafe().getLong(blockAddr + 8);
        long w45 = Unsafe.getUnsafe().getLong(blockAddr + 16);
        long w67 = Unsafe.getUnsafe().getLong(blockAddr + 24);

        long m01 = mask01(key);
        long m23 = mask23(key);
        long m45 = mask45(key);
        long m67 = mask67(key);

        return (w01 & m01) == m01
                && (w23 & m23) == m23
                && (w45 & m45) == m45
                && (w67 & m67) == m67;
    }

    // Pack two 32-bit masks into one 64-bit long for SWAR checks
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
