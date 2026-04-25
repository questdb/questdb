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

import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.DirectIntLongHashMap;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Unsafe;

import java.io.Closeable;

/**
 * Per-reader gen metadata snapshot plus a lazy "key -> hit gens" cursor cache.
 * <p>
 * Cache slot encoding: high 32 bits = entryStart, low 32 bits = entryCount.
 * {@link #CACHE_NOT_PRESENT} (-1L) is the unmapped-key sentinel; 0L means "key
 * cached but matches no gen" and is a valid hit.
 * <p>
 * {@link #invalidateCache} bumps {@link #getCacheVersion()}; cursors snapshot
 * the version at {@code of()} time and bail if it changes mid-iteration to
 * avoid reading into recycled cache memory.
 */
public class PostingGenLookup implements Closeable {
    static final long CACHE_NOT_PRESENT = -1L;
    private static final long CACHE_ENTRIES_INITIAL_CAPACITY = 16;
    private static final long DEFAULT_CACHE_BUDGET = 16L * 1024 * 1024;
    private static final int KEY_TO_SLOT_INITIAL_CAPACITY = 16;
    private static final double KEY_TO_SLOT_LOAD_FACTOR = 0.5;
    // Column keys are always >= 0, so Integer.MIN_VALUE is a safe sentinel.
    private static final int NO_ENTRY_KEY = Integer.MIN_VALUE;
    // Double-buffered snapshot. {@code snapshotMetadata} fills the staging
    // copy in-place; {@code commitSnapshot} swaps the active/staging
    // pointers only after the seqlock reader has verified the source bytes
    // were consistent. Until then the active copy still reflects the
    // previous successful snapshot, so a torn read can never corrupt
    // observable state.
    private final Snapshot bufA = new Snapshot();
    private final Snapshot bufB = new Snapshot();
    private final DirectLongList cacheEntries = new DirectLongList(CACHE_ENTRIES_INITIAL_CAPACITY, MemoryTag.NATIVE_INDEX_READER);
    private final DirectIntLongHashMap keyToCacheSlot = new DirectIntLongHashMap(
            KEY_TO_SLOT_INITIAL_CAPACITY,
            KEY_TO_SLOT_LOAD_FACTOR,
            NO_ENTRY_KEY,
            CACHE_NOT_PRESENT,
            MemoryTag.NATIVE_INDEX_READER
    );
    private Snapshot active = bufA;
    private long cacheBudget = DEFAULT_CACHE_BUDGET;
    private long cacheUsedBytes;
    private long cacheVersion;
    private Snapshot staging = bufB;

    public static long packCacheEntry(int gen, int posInGen) {
        return ((long) gen << 32) | (posInGen & 0xFFFFFFFFL);
    }

    public static int unpackCacheGen(long entry) {
        return (int) (entry >>> 32);
    }

    public static int unpackCachePosInGen(long entry) {
        return (int) entry;
    }

    public static int unpackEntryCount(long packedSlot) {
        return (int) packedSlot;
    }

    public static int unpackEntryStart(long packedSlot) {
        return (int) (packedSlot >>> 32);
    }

    public long cacheEntryAt(int idx) {
        return cacheEntries.get(idx);
    }

    public long cacheLookup(int key) {
        assert key != NO_ENTRY_KEY : "column key must not equal hash map sentinel";
        if (!active.anySparseGen) {
            return CACHE_NOT_PRESENT;
        }
        return keyToCacheSlot.get(key);
    }

    @Override
    public void close() {
        Misc.free(keyToCacheSlot);
        Misc.free(cacheEntries);
        cacheUsedBytes = 0;
        bufA.clear();
        bufB.clear();
    }

    /**
     * Promote the staging snapshot (last fill from {@link #snapshotMetadata})
     * to active. The previous active becomes the new staging buffer. Must
     * only be called after the seqlock reader has verified the source bytes
     * were consistent during the fill.
     */
    public void commitSnapshot() {
        Snapshot tmp = active;
        active = staging;
        staging = tmp;
    }

    public long getCacheVersion() {
        return cacheVersion;
    }

    public long getGenDataSize(int gen) {
        return active.genDataSizes.getQuick(gen);
    }

    public long getGenFileOffset(int gen) {
        return active.genFileOffsets.getQuick(gen);
    }

    public int getGenKeyCount(int gen) {
        return active.genKeyCounts.getQuick(gen);
    }

    public int getGenMaxKey(int gen) {
        return active.genMaxKeys.getQuick(gen);
    }

    public int getGenMinKey(int gen) {
        return active.genMinKeys.getQuick(gen);
    }

    // prefix-sum sits between encoded data and the SBBF+footer trailer, so
    // we walk back from the gen tail.
    public long getGenPrefixSumOffset(int gen, MemoryMR valueMem) {
        long genFileOffset = active.genFileOffsets.getQuick(gen);
        long genDataSize = active.genDataSizes.getQuick(gen);
        int sbbfNumBlocks = readSbbfNumBlocks(valueMem, genFileOffset, genDataSize);
        int minKey = active.genMinKeys.getQuick(gen);
        int maxKey = active.genMaxKeys.getQuick(gen);
        int keyRange = maxKey - minKey + 1;
        return genFileOffset + genDataSize
                - PostingIndexUtils.SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE
                - (long) sbbfNumBlocks * SplitBlockBloomFilter.BLOCK_SIZE
                - (long) (keyRange + 2) * Integer.BYTES;
    }

    public void invalidateCache() {
        keyToCacheSlot.clear();
        cacheEntries.clear();
        cacheUsedBytes = 0;
        cacheVersion++;
    }

    /**
     * Returns true when the SBBF proves K is absent from the gen. Returns false
     * for "maybe present" or when the gen has no SBBF (numBlocks == 0); in both
     * cases the caller falls back to the prefix-sum verification path.
     */
    public boolean notContainKey(MemoryMR valueMem, int gen, int key) {
        long genFileOffset = active.genFileOffsets.getQuick(gen);
        long genDataSize = active.genDataSizes.getQuick(gen);
        int numBlocks = readSbbfNumBlocks(valueMem, genFileOffset, genDataSize);
        if (numBlocks == 0) {
            return false;
        }
        int sbbfSize = numBlocks << SplitBlockBloomFilter.BLOCK_SIZE_SHIFT;
        long baseAddr = valueMem.addressOf(0);
        long sbbfAddr = baseAddr + genFileOffset + genDataSize
                - PostingIndexUtils.SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE - sbbfSize;
        return !SplitBlockBloomFilter.mightContain(sbbfAddr, sbbfSize, SplitBlockBloomFilter.hashKey(key));
    }

    /**
     * Idempotent commit: a no-op if {@code key} is already cached. Drops silently
     * when adding {@code builderEntries} would exceed the cache budget; correctness
     * is preserved because uncached keys keep using the SBBF-only path.
     */
    public void putCacheEntries(int key, LongList builderEntries) {
        assert key != NO_ENTRY_KEY : "column key must not equal hash map sentinel";
        if (cacheBudget <= 0 || !active.anySparseGen || keyToCacheSlot.get(key) != CACHE_NOT_PRESENT) {
            return;
        }
        int count = builderEntries.size();
        long bytesNeeded = (long) count * Long.BYTES;
        if (cacheUsedBytes + bytesNeeded > cacheBudget) {
            return;
        }
        long newSize = cacheEntries.size() + count;
        assert newSize <= Integer.MAX_VALUE : "cache pool overflow: " + newSize;
        int startIdx = (int) cacheEntries.size();
        cacheEntries.ensureCapacity(count);
        for (int i = 0; i < count; i++) {
            cacheEntries.add(builderEntries.getQuick(i));
        }
        cacheUsedBytes += bytesNeeded;
        keyToCacheSlot.put(key, ((long) startIdx << 32) | (count & 0xFFFFFFFFL));
    }

    public void reopen() {
        keyToCacheSlot.reopen();
        cacheEntries.reopen();
        cacheUsedBytes = 0;
        cacheVersion++;
    }

    public void setCacheMemoryBudget(long budget) {
        this.cacheBudget = budget;
        // budget == 0 disables caching; clear residual empty-hit slots too so the
        // disabled state is fully clean.
        if (budget <= 0 || cacheUsedBytes > budget) {
            invalidateCache();
        }
    }

    /**
     * Fill the staging snapshot from the writer's metadata page. Caller
     * must validate the seqlock and then call {@link #commitSnapshot} to
     * make the snapshot observable. Until the swap, the previous active
     * snapshot is unchanged — torn reads here cannot corrupt anything.
     */
    public void snapshotMetadata(MemoryMR keyMem, int genCount, long pageOffset) {
        Snapshot s = staging;
        s.genFileOffsets.clear();
        s.genDataSizes.clear();
        s.genKeyCounts.clear();
        s.genMinKeys.clear();
        s.genMaxKeys.clear();
        s.anySparseGen = false;
        for (int i = 0; i < genCount; i++) {
            long dirOffset = PostingIndexUtils.getGenDirOffset(pageOffset, i);
            s.genFileOffsets.add(keyMem.getLong(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET));
            s.genDataSizes.add(keyMem.getLong(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_SIZE));
            int kc = keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_KEY_COUNT);
            s.genKeyCounts.add(kc);
            if (kc < 0) {
                s.anySparseGen = true;
            }
            s.genMinKeys.add(keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MIN_KEY));
            s.genMaxKeys.add(keyMem.getInt(dirOffset + PostingIndexUtils.GEN_DIR_OFFSET_MAX_KEY));
        }
    }

    private static int readSbbfNumBlocks(MemoryMR valueMem, long genFileOffset, long genDataSize) {
        if (genDataSize < PostingIndexUtils.SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE) {
            return 0;
        }
        long off = genFileOffset + genDataSize - PostingIndexUtils.SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE;
        int numBlocks = Unsafe.getInt(valueMem.addressOf(off));
        if (numBlocks <= 0
                || (long) numBlocks << SplitBlockBloomFilter.BLOCK_SIZE_SHIFT
                > genDataSize - PostingIndexUtils.SPARSE_SBBF_NUM_BLOCKS_FOOTER_SIZE) {
            return 0;
        }
        return numBlocks;
    }

    /**
     * One side of the active/staging double buffer used for torn-read
     * safety. The reader fills the staging side, validates the seqlock,
     * then swaps via {@link #commitSnapshot}. The previous active side
     * becomes the next staging buffer — its existing storage is reused so
     * the swap is allocation-free.
     */
    private static final class Snapshot {
        final LongList genDataSizes = new LongList();
        final LongList genFileOffsets = new LongList();
        final IntList genKeyCounts = new IntList(); // negative = sparse
        final IntList genMaxKeys = new IntList();
        final IntList genMinKeys = new IntList();
        boolean anySparseGen;

        void clear() {
            genFileOffsets.clear();
            genDataSizes.clear();
            genKeyCounts.clear();
            genMinKeys.clear();
            genMaxKeys.clear();
            anySparseGen = false;
        }
    }
}
