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

import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Shared memory-efficient generation lookup for BP bitmap index readers.
 * Uses a tiered strategy based on key count and a configurable memory budget.
 * <p>
 * Tier 1 — Per-key off-heap CSR index (small keyCount, within budget):
 * O(hitGens) per key lookup, zero binary searches.
 * <p>
 * Tier 2 — Per-gen SBBF (large keyCount, budget-bounded):
 * Hash the key, probe each gen's SBBF. On "no", skip the gen entirely.
 * <p>
 * Tier 3 — No index (fallback):
 * Pure binary search + min/max bounds check.
 * <p>
 * Gen metadata cache (all tiers): genFileOffsets, genDataSizes, genKeyCounts,
 * genMinKeys, genMaxKeys — avoids re-reading key file gen dir entries.
 */
public class PostingGenLookup implements Closeable {
    static final int TIER_NONE = 0;
    static final int TIER_PER_KEY = 1;
    static final int TIER_SBBF = 2;

    private static final long DEFAULT_MEMORY_BUDGET = 256L * 1024 * 1024; // 256MB
    private static final double DEFAULT_TARGET_FPP = 0.01; // 1% FPR
    private static final double MAX_FPP = 0.5; // don't degrade SBBF beyond 50%
    // State
    private int builtForGenCount;
    private long[] genDataSizes;
    // Shared: gen metadata cache
    private long[] genFileOffsets;
    private long genIndicesAddr;   // totalEntries × 4B native
    private int[] genKeyCounts;    // negative = sparse
    private int[] genMaxKeys;
    private int[] genMinKeys;
    private int[] genSidecarOffsets;
    private int keyCount;
    // Tier 1: per-key off-heap CSR arrays
    private long keyOffsetsAddr;   // (keyCount+1) × 4B native
    private long memoryBudget = DEFAULT_MEMORY_BUDGET;
    private long posInGenAddr;     // totalEntries × 4B native
    // Tier 2: per-gen SBBF on native memory
    private long[] sbbfAddrs;      // [genCount] native address per gen's SBBF
    private int sbbfGenCount;      // number of SBBFs allocated
    private int sbbfSizePerGen;    // uniform size per gen
    // Tier selection
    private int tier;
    private long tier1EntriesSize;    // actual alloc size for genIndicesAddr/posInGenAddr
    private long tier1KeyOffsetsSize; // actual alloc size for keyOffsetsAddr

    @Override
    public void close() {
        freeTier1();
        freeTier2();
        genFileOffsets = null;
        genDataSizes = null;
        genKeyCounts = null;
        genMinKeys = null;
        genMaxKeys = null;
        builtForGenCount = 0;
        keyCount = 0;
        tier = TIER_NONE;
    }

    private void buildLookupIndex(MemoryMR valueMem, int keyCount, int genCount) {
        // Count sparse gen entries to estimate memory
        int sparseGenCount = 0;
        long totalSparseEntries = 0;
        for (int g = 0; g < genCount; g++) {
            if (genKeyCounts[g] < 0) {
                sparseGenCount++;
                totalSparseEntries -= genKeyCounts[g];
            }
        }

        if (sparseGenCount == 0) {
            tier = TIER_NONE;
            return;
        }

        // Single sparse gen: use stored prefix-sum for O(1) lookup, skip CSR build
        if (sparseGenCount == 1) {
            tier = TIER_NONE;
            return;
        }

        // Estimate Tier 1 memory: (keyCount+1)*4 + totalEntries*4*2
        long tier1Mem = (long) (keyCount + 1) * Integer.BYTES + totalSparseEntries * 2L * Integer.BYTES;

        if (tier1Mem <= memoryBudget) {
            buildTier1(valueMem, keyCount, genCount);
        } else if (genCount > 2) {
            buildTier2(valueMem, genCount, sparseGenCount);
        } else {
            tier = TIER_NONE;
        }
    }

    private void buildTier1(MemoryMR valueMem, int kc, int genCount) {
        // Pass 1: count entries per key across all sparse gens (native scratch buffer)
        long countsSize = (long) kc * Integer.BYTES;
        long countsAddr = Unsafe.malloc(countsSize, MemoryTag.NATIVE_INDEX_READER);
        Unsafe.getUnsafe().setMemory(countsAddr, countsSize, (byte) 0);
        try {
            for (int g = 0; g < genCount; g++) {
                if (genKeyCounts[g] >= 0) {
                    continue; // dense gen
                }
                int activeKeyCount = -genKeyCounts[g];
                long genFileOffset = genFileOffsets[g];
                long genDataSize = genDataSizes[g];
                valueMem.extend(genFileOffset + genDataSize);
                long genAddr = valueMem.addressOf(genFileOffset);
                for (int i = 0; i < activeKeyCount; i++) {
                    int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                    if (key < kc) {
                        int prev = Unsafe.getUnsafe().getInt(countsAddr + (long) key * Integer.BYTES);
                        Unsafe.getUnsafe().putInt(countsAddr + (long) key * Integer.BYTES, prev + 1);
                    }
                }
            }

            // Build prefix sums
            int total = 0;
            for (int k = 0; k < kc; k++) {
                int c = Unsafe.getUnsafe().getInt(countsAddr + (long) k * Integer.BYTES);
                Unsafe.getUnsafe().putInt(countsAddr + (long) k * Integer.BYTES, total);
                total += c;
            }

            if (total == 0) {
                tier = TIER_NONE;
                return;
            }

            // Allocate off-heap CSR arrays with safe partial-failure handling
            long keyOffsetsSize = (long) (kc + 1) * Integer.BYTES;
            long entriesSize = (long) total * Integer.BYTES;
            keyOffsetsAddr = Unsafe.malloc(keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
            try {
                genIndicesAddr = Unsafe.malloc(entriesSize, MemoryTag.NATIVE_INDEX_READER);
                try {
                    posInGenAddr = Unsafe.malloc(entriesSize, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(genIndicesAddr, entriesSize, MemoryTag.NATIVE_INDEX_READER);
                    genIndicesAddr = 0;
                    throw e;
                }
            } catch (Throwable e) {
                Unsafe.free(keyOffsetsAddr, keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
                keyOffsetsAddr = 0;
                throw e;
            }
            tier1KeyOffsetsSize = keyOffsetsSize;
            tier1EntriesSize = entriesSize;

            // Write prefix sums to off-heap
            for (int k = 0; k < kc; k++) {
                Unsafe.getUnsafe().putInt(keyOffsetsAddr + (long) k * Integer.BYTES,
                        Unsafe.getUnsafe().getInt(countsAddr + (long) k * Integer.BYTES));
            }
            Unsafe.getUnsafe().putInt(keyOffsetsAddr + (long) kc * Integer.BYTES, total);

            // Pass 2: fill entries (reuse countsAddr as write position tracker)
            for (int g = 0; g < genCount; g++) {
                if (genKeyCounts[g] >= 0) {
                    continue;
                }
                int activeKeyCount = -genKeyCounts[g];
                long genAddr = valueMem.addressOf(genFileOffsets[g]);
                for (int i = 0; i < activeKeyCount; i++) {
                    int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                    if (key < kc) {
                        int pos = Unsafe.getUnsafe().getInt(countsAddr + (long) key * Integer.BYTES);
                        Unsafe.getUnsafe().putInt(countsAddr + (long) key * Integer.BYTES, pos + 1);
                        Unsafe.getUnsafe().putInt(genIndicesAddr + (long) pos * Integer.BYTES, g);
                        Unsafe.getUnsafe().putInt(posInGenAddr + (long) pos * Integer.BYTES, i);
                    }
                }
            }

            tier = TIER_PER_KEY;
        } finally {
            Unsafe.free(countsAddr, countsSize, MemoryTag.NATIVE_INDEX_READER);
        }
    }

    private void buildTier2(MemoryMR valueMem, int genCount, int sparseGenCount) {
        // Estimate per-gen SBBF size at default FPP
        int maxActiveKeys = 0;
        for (int g = 0; g < genCount; g++) {
            if (genKeyCounts[g] < 0) {
                int ak = -genKeyCounts[g];
                if (ak > maxActiveKeys) {
                    maxActiveKeys = ak;
                }
            }
        }

        double fpp = DEFAULT_TARGET_FPP;
        sbbfSizePerGen = SplitBlockBloomFilter.computeSize(maxActiveKeys, fpp);
        long totalSbbfMem = (long) sparseGenCount * sbbfSizePerGen;

        // If over budget, increase FPP until it fits
        while (totalSbbfMem > memoryBudget && fpp < MAX_FPP) {
            fpp *= 2.0;
            sbbfSizePerGen = SplitBlockBloomFilter.computeSize(maxActiveKeys, fpp);
            totalSbbfMem = (long) sparseGenCount * sbbfSizePerGen;
        }

        if (totalSbbfMem > memoryBudget) {
            tier = TIER_NONE;
            return;
        }

        sbbfAddrs = new long[genCount];
        sbbfGenCount = genCount;

        for (int g = 0; g < genCount; g++) {
            if (genKeyCounts[g] >= 0) {
                sbbfAddrs[g] = 0; // no SBBF for dense gens
                continue;
            }
            int activeKeyCount = -genKeyCounts[g];
            long genAddr = valueMem.addressOf(genFileOffsets[g]);

            sbbfAddrs[g] = SplitBlockBloomFilter.allocate(sbbfSizePerGen);
            sbbfGenCount = g + 1; // track for cleanup if later allocations fail
            for (int i = 0; i < activeKeyCount; i++) {
                int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                long hash = SplitBlockBloomFilter.hashKey(key);
                SplitBlockBloomFilter.insert(sbbfAddrs[g], sbbfSizePerGen, hash);
            }
        }

        tier = TIER_SBBF;
    }

    private void freeTier1() {
        if (keyOffsetsAddr != 0) {
            Unsafe.free(keyOffsetsAddr, tier1KeyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.free(genIndicesAddr, tier1EntriesSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.free(posInGenAddr, tier1EntriesSize, MemoryTag.NATIVE_INDEX_READER);
            keyOffsetsAddr = 0;
            genIndicesAddr = 0;
            posInGenAddr = 0;
            tier1KeyOffsetsSize = 0;
            tier1EntriesSize = 0;
        }
    }

    private void freeTier2() {
        if (sbbfAddrs != null) {
            for (int g = 0; g < sbbfGenCount; g++) {
                SplitBlockBloomFilter.free(sbbfAddrs[g], sbbfSizePerGen);
            }
            sbbfAddrs = null;
            sbbfGenCount = 0;
            sbbfSizePerGen = 0;
        }
    }

    /**
     * Builds the lookup index (tier1/tier2) if the gen dir metadata has been
     * snapshotted but the index hasn't been built yet for the current genCount.
     * Called from ensureGenLookup() after snapshotMetadata() has populated the arrays.
     */
    void buildLookupIfNeeded(MemoryMR valueMem, int keyCount, int genCount) {
        if (genCount <= builtForGenCount && this.keyCount == keyCount) {
            return;
        }
        this.keyCount = keyCount;
        if (genCount == 0 || keyCount == 0 || genFileOffsets == null) {
            this.builtForGenCount = genCount;
            this.tier = TIER_NONE;
            return;
        }
        // Metadata is already in the arrays from snapshotMetadata — just build the index
        freeTier1();
        freeTier2();
        buildLookupIndex(valueMem, keyCount, genCount);
        this.builtForGenCount = genCount;
    }

    // Tier 1 accessors
    int getEntryEnd(int key) {
        return Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) (key + 1) * Integer.BYTES);
    }

    int getEntryStart(int key) {
        return Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) key * Integer.BYTES);
    }

    long getGenDataSize(int gen) {
        return genDataSizes[gen];
    }

    long getGenFileOffset(int gen) {
        return genFileOffsets[gen];
    }

    int getGenIndex(int entryPos) {
        return Unsafe.getUnsafe().getInt(genIndicesAddr + (long) entryPos * Integer.BYTES);
    }

    int getGenKeyCount(int gen) {
        return genKeyCounts[gen];
    }

    int getGenMaxKey(int gen) {
        return genMaxKeys[gen];
    }

    int getGenMinKey(int gen) {
        return genMinKeys[gen];
    }

    long getGenPrefixSumOffset(int gen) {
        int minKey = genMinKeys[gen];
        int maxKey = genMaxKeys[gen];
        int keyRange = maxKey - minKey + 1;
        return genFileOffsets[gen] + genDataSizes[gen] - (long) (keyRange + 2) * Integer.BYTES;
    }

    int getGenSidecarOffset(int gen) {
        return genSidecarOffsets != null ? genSidecarOffsets[gen] : 0;
    }

    int getKeyCount() {
        return keyCount;
    }

    int getPosInGen(int entryPos) {
        return Unsafe.getUnsafe().getInt(posInGenAddr + (long) entryPos * Integer.BYTES);
    }

    int getTier() {
        return tier;
    }

    /**
     * Invalidates the cached lookup index (tier1/tier2). Called by the reader
     * AFTER {@link #snapshotMetadata} has been validated via post-snapshot seq
     * recheck, so that a subsequent query rebuilds the index from the freshly
     * captured metadata arrays.
     */
    void invalidateLookupIndex() {
        freeTier1();
        freeTier2();
        builtForGenCount = 0;
    }

    boolean isPerKeyMode() {
        return tier == TIER_PER_KEY;
    }

    // Tier 2 accessor
    boolean mightContainKey(int gen, int key) {
        if (gen >= sbbfGenCount || sbbfAddrs[gen] == 0) {
            return true; // no SBBF for this gen (dense gen), assume present
        }
        return SplitBlockBloomFilter.mightContain(sbbfAddrs[gen], sbbfSizePerGen, SplitBlockBloomFilter.hashKey(key));
    }

    void setMemoryBudget(long budget) {
        this.memoryBudget = budget;
    }

    /**
     * Snapshots gen dir entries from keyMem into local arrays. Called by the reader
     * inside the seq_start/seq_end validation window so the snapshot is consistent.
     * Does NOT build the lookup index (tier1/tier2) — call buildLookupIfNeeded after.
     */
    void snapshotMetadata(MemoryMR keyMem, int genCount, long pageOffset) {
        if (genCount == 0) {
            return;
        }
        // Ensure arrays are large enough
        if (genFileOffsets == null || genFileOffsets.length < genCount) {
            int newSize = Math.max(genCount, 16);
            genFileOffsets = new long[newSize];
            genDataSizes = new long[newSize];
            genKeyCounts = new int[newSize];
            genMinKeys = new int[newSize];
            genMaxKeys = new int[newSize];
            genSidecarOffsets = new int[newSize];
        }
        for (int g = 0; g < genCount; g++) {
            long dirOffset = PostingIndexUtils.getGenDirOffset(pageOffset, g);
            genFileOffsets[g] = keyMem.getLong(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
            genDataSizes[g] = keyMem.getLong(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_SIZE);
            genKeyCounts[g] = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            genMinKeys[g] = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY);
            genMaxKeys[g] = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY);
            genSidecarOffsets[g] = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_SIDECAR_OFFSET);
        }
    }
}
