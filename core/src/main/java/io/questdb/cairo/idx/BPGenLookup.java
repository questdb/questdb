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
class BPGenLookup implements Closeable {
    static final int TIER_NONE = 0;
    static final int TIER_PER_KEY = 1;
    static final int TIER_SBBF = 2;

    private static final long DEFAULT_MEMORY_BUDGET = 256L * 1024 * 1024; // 256MB
    private static final double DEFAULT_TARGET_FPP = 0.01; // 1% FPR
    private static final double MAX_FPP = 0.5; // don't degrade SBBF beyond 50%

    // Tier selection
    private int tier;

    // Tier 1: per-key off-heap CSR arrays
    private long keyOffsetsAddr;   // (keyCount+1) × 4B native
    private long genIndicesAddr;   // totalEntries × 4B native
    private long posInGenAddr;     // totalEntries × 4B native
    private int totalEntries;
    private long tier1AllocSize;   // total bytes allocated for tier 1

    // Tier 2: per-gen SBBF on native memory
    private long[] sbbfAddrs;      // [genCount] native address per gen's SBBF
    private int sbbfSizePerGen;    // uniform size per gen
    private int sbbfGenCount;      // number of SBBFs allocated

    // Shared: gen metadata cache
    private long[] genFileOffsets;
    private int[] genDataSizes;
    private int[] genKeyCounts;    // negative = sparse
    private int[] genMinKeys;
    private int[] genMaxKeys;

    // State
    private int builtForGenCount;
    private int keyCount;
    private long memoryBudget = DEFAULT_MEMORY_BUDGET;

    void build(MemoryMR keyMem, MemoryMR valueMem, int keyCount, int genCount) {
        close();
        this.keyCount = keyCount;
        this.builtForGenCount = 0;

        if (keyCount == 0 || genCount == 0) {
            this.builtForGenCount = genCount;
            this.tier = TIER_NONE;
            return;
        }

        buildMetadata(keyMem, 0, genCount);
        buildLookupIndex(keyMem, valueMem, keyCount, genCount, 0);
        this.builtForGenCount = genCount;
    }

    void buildIncremental(MemoryMR keyMem, MemoryMR valueMem, int keyCount, int newGenCount) {
        if (newGenCount <= builtForGenCount && this.keyCount == keyCount) {
            return;
        }

        if (this.keyCount != keyCount || builtForGenCount == 0 || genFileOffsets == null) {
            // Key count changed or no prior build — full rebuild
            build(keyMem, valueMem, keyCount, newGenCount);
            return;
        }

        int oldGenCount = builtForGenCount;

        // Extend metadata arrays
        buildMetadata(keyMem, oldGenCount, newGenCount);

        // Extend lookup index
        if (tier == TIER_PER_KEY) {
            incrementalBuildTier1(keyMem, valueMem, keyCount, oldGenCount, newGenCount);
        } else if (tier == TIER_SBBF) {
            incrementalBuildTier2(valueMem, oldGenCount, newGenCount);
        }
        // TIER_NONE: nothing to do

        this.builtForGenCount = newGenCount;
    }

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

    int getBuiltForGenCount() {
        return builtForGenCount;
    }

    // Tier 1 accessors
    int getEntryEnd(int key) {
        return Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) (key + 1) * Integer.BYTES);
    }

    int getEntryStart(int key) {
        return Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) key * Integer.BYTES);
    }

    int getGenDataSize(int gen) {
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

    int getKeyCount() {
        return keyCount;
    }

    int getPosInGen(int entryPos) {
        return Unsafe.getUnsafe().getInt(posInGenAddr + (long) entryPos * Integer.BYTES);
    }

    int getTier() {
        return tier;
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

    private void buildLookupIndex(MemoryMR keyMem, MemoryMR valueMem, int keyCount, int genCount, int fromGen) {
        // Count sparse gen entries to estimate memory
        int sparseGenCount = 0;
        long totalSparseEntries = 0;
        for (int g = fromGen; g < genCount; g++) {
            if (genKeyCounts[g] < 0) {
                sparseGenCount++;
                totalSparseEntries += -genKeyCounts[g];
            }
        }

        if (sparseGenCount == 0 && fromGen == 0) {
            tier = TIER_NONE;
            return;
        }

        // Estimate Tier 1 memory: (keyCount+1)*4 + totalEntries*4*2
        long tier1Mem = (long) (keyCount + 1) * Integer.BYTES + totalSparseEntries * 2L * Integer.BYTES;

        if (tier1Mem <= memoryBudget) {
            buildTier1(keyMem, valueMem, keyCount, genCount);
        } else if (genCount > 2) {
            buildTier2(valueMem, genCount, sparseGenCount);
        } else {
            tier = TIER_NONE;
        }
    }

    private void buildMetadata(MemoryMR keyMem, int fromGen, int toGenCount) {
        if (genFileOffsets == null || genFileOffsets.length < toGenCount) {
            int newSize = Math.max(toGenCount, 16);
            long[] newOffsets = new long[newSize];
            int[] newSizes = new int[newSize];
            int[] newKeyCounts = new int[newSize];
            int[] newMinKeys = new int[newSize];
            int[] newMaxKeys = new int[newSize];

            if (genFileOffsets != null && fromGen > 0) {
                System.arraycopy(genFileOffsets, 0, newOffsets, 0, fromGen);
                System.arraycopy(genDataSizes, 0, newSizes, 0, fromGen);
                System.arraycopy(genKeyCounts, 0, newKeyCounts, 0, fromGen);
                System.arraycopy(genMinKeys, 0, newMinKeys, 0, fromGen);
                System.arraycopy(genMaxKeys, 0, newMaxKeys, 0, fromGen);
            }

            genFileOffsets = newOffsets;
            genDataSizes = newSizes;
            genKeyCounts = newKeyCounts;
            genMinKeys = newMinKeys;
            genMaxKeys = newMaxKeys;
        }

        for (int g = fromGen; g < toGenCount; g++) {
            long dirOffset = BPBitmapIndexUtils.getGenDirOffset(g);
            genFileOffsets[g] = keyMem.getLong(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
            genDataSizes[g] = keyMem.getInt(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_SIZE);
            genKeyCounts[g] = keyMem.getInt(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            genMinKeys[g] = keyMem.getInt(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_MIN_KEY);
            genMaxKeys[g] = keyMem.getInt(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_MAX_KEY);
        }
    }

    private void buildTier1(MemoryMR keyMem, MemoryMR valueMem, int kc, int genCount) {
        // Pass 1: count entries per key across all sparse gens
        int[] counts = new int[kc];
        for (int g = 0; g < genCount; g++) {
            if (genKeyCounts[g] >= 0) {
                continue; // dense gen
            }
            int activeKeyCount = -genKeyCounts[g];
            long genFileOffset = genFileOffsets[g];
            int genDataSize = genDataSizes[g];
            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);
            for (int i = 0; i < activeKeyCount; i++) {
                int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                if (key < kc) {
                    counts[key]++;
                }
            }
        }

        // Build prefix sums
        int total = 0;
        for (int k = 0; k < kc; k++) {
            int c = counts[k];
            counts[k] = total;
            total += c;
        }

        if (total == 0) {
            tier = TIER_NONE;
            return;
        }

        // Allocate off-heap CSR arrays
        long keyOffsetsSize = (long) (kc + 1) * Integer.BYTES;
        long entriesSize = (long) total * Integer.BYTES;
        keyOffsetsAddr = Unsafe.malloc(keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
        genIndicesAddr = Unsafe.malloc(entriesSize, MemoryTag.NATIVE_INDEX_READER);
        posInGenAddr = Unsafe.malloc(entriesSize, MemoryTag.NATIVE_INDEX_READER);
        totalEntries = total;
        tier1AllocSize = keyOffsetsSize + entriesSize * 2;

        // Write prefix sums to off-heap
        for (int k = 0; k < kc; k++) {
            Unsafe.getUnsafe().putInt(keyOffsetsAddr + (long) k * Integer.BYTES, counts[k]);
        }
        Unsafe.getUnsafe().putInt(keyOffsetsAddr + (long) kc * Integer.BYTES, total);

        // Pass 2: fill entries
        int[] writePos = counts; // reuse as write position tracker (already holds prefix sums)
        for (int g = 0; g < genCount; g++) {
            if (genKeyCounts[g] >= 0) {
                continue;
            }
            int activeKeyCount = -genKeyCounts[g];
            long genAddr = valueMem.addressOf(genFileOffsets[g]);
            for (int i = 0; i < activeKeyCount; i++) {
                int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                if (key < kc) {
                    int pos = writePos[key]++;
                    Unsafe.getUnsafe().putInt(genIndicesAddr + (long) pos * Integer.BYTES, g);
                    Unsafe.getUnsafe().putInt(posInGenAddr + (long) pos * Integer.BYTES, i);
                }
            }
        }

        tier = TIER_PER_KEY;
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
            long keyOffsetsSize = (long) (keyCount + 1) * Integer.BYTES;
            long entriesSize = (long) totalEntries * Integer.BYTES;
            Unsafe.free(keyOffsetsAddr, keyOffsetsSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.free(genIndicesAddr, entriesSize, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.free(posInGenAddr, entriesSize, MemoryTag.NATIVE_INDEX_READER);
            keyOffsetsAddr = 0;
            genIndicesAddr = 0;
            posInGenAddr = 0;
            totalEntries = 0;
            tier1AllocSize = 0;
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

    private void incrementalBuildTier1(MemoryMR keyMem, MemoryMR valueMem, int kc, int oldGenCount, int newGenCount) {
        // Count new entries per key for new gens only
        int newEntries = 0;
        for (int g = oldGenCount; g < newGenCount; g++) {
            if (genKeyCounts[g] < 0) {
                newEntries += -genKeyCounts[g];
            }
        }

        if (newEntries == 0) {
            return;
        }

        // Full rebuild for simplicity — incremental CSR append is complex
        // (requires realloc + prefix sum rebuild). The cost is O(totalEntries)
        // which is already the cost of the original build.
        freeTier1();
        buildTier1(keyMem, valueMem, kc, newGenCount);
    }

    private void incrementalBuildTier2(MemoryMR valueMem, int oldGenCount, int newGenCount) {
        // Grow SBBF array if needed
        if (sbbfAddrs == null || sbbfAddrs.length < newGenCount) {
            long[] newAddrs = new long[newGenCount];
            if (sbbfAddrs != null) {
                System.arraycopy(sbbfAddrs, 0, newAddrs, 0, sbbfGenCount);
            }
            sbbfAddrs = newAddrs;
        }

        // Allocate SBBFs for new sparse gens only
        for (int g = oldGenCount; g < newGenCount; g++) {
            if (genKeyCounts[g] >= 0) {
                sbbfAddrs[g] = 0;
                continue;
            }
            int activeKeyCount = -genKeyCounts[g];
            long genAddr = valueMem.addressOf(genFileOffsets[g]);

            // Recompute size for this gen if it has more keys than prior gens
            int genSbbfSize = SplitBlockBloomFilter.computeSize(activeKeyCount, DEFAULT_TARGET_FPP);
            if (genSbbfSize > sbbfSizePerGen) {
                genSbbfSize = sbbfSizePerGen; // keep uniform size for simplicity
            }

            sbbfAddrs[g] = SplitBlockBloomFilter.allocate(sbbfSizePerGen);
            for (int i = 0; i < activeKeyCount; i++) {
                int key = Unsafe.getUnsafe().getInt(genAddr + (long) i * Integer.BYTES);
                SplitBlockBloomFilter.insert(sbbfAddrs[g], sbbfSizePerGen, SplitBlockBloomFilter.hashKey(key));
            }
        }
        sbbfGenCount = newGenCount;
    }
}
