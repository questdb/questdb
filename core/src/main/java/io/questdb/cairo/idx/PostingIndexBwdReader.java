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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class PostingIndexBwdReader extends AbstractPostingIndexReader {
    private static final int MIN_BUFFER_CAPACITY = 4;
    private final ObjList<Cursor> freeCursors = new ObjList<>();
    private final ObjList<NullCursor> freeNullCursors = new ObjList<>();

    public PostingIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp);
    }

    public PostingIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long pinnedTableTxn
    ) {
        setPinnedTableTxn(pinnedTableTxn);
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp);
    }

    @Override
    public void close() {
        super.close();
        for (int i = 0, n = freeCursors.size(); i < n; i++) {
            freeCursors.getQuick(i).releaseResources();
        }
        Misc.clear(freeCursors);
        for (int i = 0, n = freeNullCursors.size(); i < n; i++) {
            freeNullCursors.getQuick(i).releaseResources();
        }
        Misc.clear(freeNullCursors);
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        return getCursor(key, minValue, maxValue, null);
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        stampOperatingThread();
        reloadConditionally();

        // See PostingIndexFwdReader.getCursor: clamp the index-walked
        // upper bound to the picked chain entry's MAX_VALUE so dirty
        // (key, rowId) entries in .pv past the entry's coverage are
        // not surfaced. Implicit nulls (rows before columnTop) stay
        // clamped by columnTop only.
        long indexMaxValue = entryMaxValue >= 0 ? Math.min(maxValue, entryMaxValue) : maxValue;

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            NullCursor nc;
            if (freeNullCursors.size() > 0) {
                nc = freeNullCursors.popLast();
                nc.isPooled = false;
            } else {
                nc = new NullCursor();
            }
            // of() can throw (e.g. OOM growing the block buffer). The cursor has
            // been popped from the pool (or freshly created) but is not yet owned
            // by the caller, so release its retained native buffers on failure;
            // the reader's close() only drains freeNullCursors and would never
            // reclaim a cursor stranded mid-of().
            try {
                nc.of(key, minValue, indexMaxValue);
            } catch (Throwable th) {
                nc.releaseResources();
                throw th;
            }
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nc.nullCount = Math.min(columnTop, hi);
            nc.nullPos = nc.nullCount;
            return nc;
        }

        if (key < keyCount) {
            openRequiredSidecars(requiredCoverColumns);
            Cursor c;
            if (freeCursors.size() > 0) {
                c = freeCursors.popLast();
                c.isPooled = false;
            } else {
                c = new Cursor();
            }
            // See the NullCursor branch above: release the cursor's native buffers
            // if of() throws so a mid-of() failure cannot strand them.
            try {
                c.of(key, minValue, indexMaxValue);
            } catch (Throwable th) {
                c.releaseResources();
                throw th;
            }
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    /**
     * Backward-iteration peer of
     * {@link PostingIndexFwdReader#getDetachedCursor(int, long, long, int[])}.
     * Constructs a fresh, single-worker-owned cursor that never draws from or
     * returns to the shared freeCursors pool; its {@link Cursor#close()} frees
     * its own native scratch directly. Positioning is identical to
     * {@link #getCursor(int, long, long, int[])}; only the construct/close
     * lifecycle differs. The reader's shared state must have been made
     * read-only first via
     * {@link AbstractPostingIndexReader#warmForKeys} for concurrent use to be
     * safe. Does NOT stamp the operating-thread tripwire (detached cursors run
     * off the reader's owning thread by design).
     * <p>
     * Provided for API symmetry with the forward reader. The covered parallel-decode
     * pipeline reads each frame forward (even DESC frames are decoded ascending; the
     * cheap selectKthMatch partitioning is forward-only), so this backward variant is
     * not currently exercised by that pipeline — it is internally correct but untested
     * in the concurrent path.
     */
    public RowCursor getDetachedCursor(int key, long minValue, long maxValue, int[] requiredCoverColumns) {
        reloadConditionally();

        // Mirror getCursor's clamp of the index-walked upper bound to the
        // picked chain entry's MAX_VALUE.
        long indexMaxValue = entryMaxValue >= 0 ? Math.min(maxValue, entryMaxValue) : maxValue;

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            NullCursor nc = new NullCursor();
            nc.isDetached = true;
            // of() can throw (e.g. OOM growing the block buffer). A detached cursor is
            // never in the reader's free list, so nothing else would reclaim it; release
            // its native scratch on a mid-of() failure (mirrors getCursor).
            try {
                nc.of(key, minValue, indexMaxValue);
            } catch (Throwable th) {
                nc.releaseResources();
                throw th;
            }
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nc.nullCount = Math.min(columnTop, hi);
            nc.nullPos = nc.nullCount;
            return nc;
        }

        if (key < keyCount) {
            openRequiredSidecars(requiredCoverColumns);
            Cursor c = new Cursor();
            c.isDetached = true;
            try {
                c.of(key, minValue, indexMaxValue);
            } catch (Throwable th) {
                c.releaseResources();
                throw th;
            }
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor extends AbstractCoveringCursor {
        private final LongList builderEntries = new LongList();
        protected long efRankDirAddr;
        protected int efRankDirCapacity;
        protected long maxValue;
        protected long minValue;
        protected long next;
        // Set for cursors handed out by getDetachedCursor: a single worker owns
        // this cursor and it was never drawn from freeCursors, so close() must
        // free its native scratch directly and never push it back to the pool
        // (which is racy under the concurrent same-reader decode this enables).
        boolean isDetached;
        private long blockBufferAddr = 0;
        private int blockBufferCapacity = 0;
        private int blockBufferPos;
        private boolean bufferRangeChecked;
        private int cacheReplayEnd;
        private int cacheReplayPos;
        private long cacheVersionAtOf;
        private int constantDeltaRemaining;
        private long constantDeltaStep;
        private long constantDeltaValue;
        private int currentBlock;
        private int currentGen;
        private long efHighOffset;
        private int efHighWordIdx;
        private int efL;
        private long efLowMask;
        private long efLowOffset;
        private int encodedBlockCount;
        private long flatBaseValue;
        private int flatBitWidth;
        private long flatDataOffset;
        private int flatRemaining;
        private int flatStartIdx;
        private boolean isCacheReplayMode;
        private boolean isEFMode;
        private boolean isFlatMode;
        private int minBlock;
        private long packedDataStartOffset;
        private int sparseGenLoadedIdx;
        private long srcBitWidthsOffset;
        private long srcFirstValuesOffset;
        private long srcMinDeltasOffset;
        private long srcPackedOffsetsOffset;
        private long srcValueCountsOffset;

        @Override
        public void close() {
            // Detached cursors are owned by a single worker thread that is, by
            // design, NOT the reader's owning thread; they never touch the
            // shared freeCursors pool. releaseResources() frees the block buffer,
            // the EF rank directory, and all covering scratch -- the same native
            // state the pool branch frees -- so skip both the operating-thread
            // gate and the pool-push.
            if (isDetached) {
                releaseResources();
                return;
            }
            // Re-pool only while the owning reader is still open (the pool retains
            // blockBufferAddr, NATIVE_INDEX_READER, and only the reader's close()
            // drains freeCursors to reclaim it) and on the reader's operating
            // thread; off-thread closes fall through to releaseResources(), which
            // frees only cursor-local buffers and is safe from any thread. See
            // AbstractPostingIndexReader.isOperatingThread() for the full
            // rationale and the gate's limits.
            if (canRepool(freeCursors.size())) {
                isPooled = true;
                closeCoveringResources();
                if (efRankDirAddr != 0) {
                    Unsafe.free(efRankDirAddr, (long) efRankDirCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    efRankDirAddr = 0;
                    efRankDirCapacity = 0;
                }
                resetCoveringState();
                freeCursors.add(this);
                return;
            }
            releaseResources();
        }

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from constant-delta stream (bitWidth=0 block)
                if (constantDeltaRemaining > 0) {
                    long value = constantDeltaValue;
                    constantDeltaValue += constantDeltaStep;
                    constantDeltaRemaining--;
                    if (value < minValue) {
                        constantDeltaRemaining = 0;
                        return false;
                    }
                    next = value;
                    if (coverCount > 0) {
                        sidecarOrdinal--;
                        cachedSidecarIdx = isCurrentGenDense
                                ? sidecarStrideKeyStart + sidecarOrdinal
                                : sidecarOrdinal;
                    }
                    return true;
                }

                // Serve from block buffer in reverse
                if (bufferRangeChecked) {
                    if (blockBufferPos >= 0) {
                        this.next = Unsafe.getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                        blockBufferPos--;
                        if (coverCount > 0) {
                            sidecarOrdinal--;
                            cachedSidecarIdx = isCurrentGenDense
                                    ? sidecarStrideKeyStart + sidecarOrdinal
                                    : sidecarOrdinal;
                        }
                        return true;
                    }
                } else {
                    while (blockBufferPos >= 0) {
                        long value = Unsafe.getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                        if (value < minValue) {
                            blockBufferPos = -1;
                            return false;
                        }
                        blockBufferPos--;
                        if (value <= maxValue) {
                            this.next = value;
                            if (coverCount > 0) {
                                sidecarOrdinal--;
                                cachedSidecarIdx = isCurrentGenDense
                                        ? sidecarStrideKeyStart + sidecarOrdinal
                                        : sidecarOrdinal;
                            }
                            return true;
                        }
                        if (coverCount > 0) sidecarOrdinal--;
                    }
                }

                // Decode previous block in current generation
                if (currentBlock >= minBlock) {
                    decodeBlock(currentBlock);
                    currentBlock--;
                    continue;
                }

                if (isEFMode && efHighWordIdx >= 0) {
                    decodeNextEFChunkReverse();
                    continue;
                }

                // Flat mode: decode previous batch
                if (isFlatMode && flatRemaining > 0) {
                    decodeNextFlatBatchReverse();
                    continue;
                }

                // Advance to previous generation
                if (advanceToPrevRelevantGen()) {
                    return false;
                }
            }
        }

        @Override
        public long next() {
            return next - minValue;
        }

        @Override
        public long seekToLast() {
            if (hasNext()) {
                return next();
            }
            return -1;
        }

        private boolean advanceToPrevRelevantGen() {
            if (isCacheReplayMode && cacheVersionAtOf != genLookup.getCacheVersion()) {
                return true;
            }
            currentGen--;
            while (currentGen >= 0) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return false;
                }
                if (isCacheReplayMode) {
                    if (cacheReplayEnd <= cacheReplayPos) {
                        currentGen--;
                        continue;
                    }
                    long entry = genLookup.cacheEntryAt(cacheReplayEnd - 1);
                    int hitGen = PostingGenLookup.unpackCacheGen(entry);
                    if (currentGen > hitGen) {
                        currentGen--;
                        continue;
                    }
                    cacheReplayEnd--;
                    loadSparseGenDirect(currentGen, PostingGenLookup.unpackCachePosInGen(entry));
                    return false;
                }
                if (requestedKey < genLookup.getGenMinKey(currentGen)
                        || requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen--;
                    continue;
                }
                if (genLookup.notContainKey(valueMem, currentGen, requestedKey)) {
                    currentGen--;
                    continue;
                }
                loadSparseGenByPrefixSum(currentGen);
                if (encodedBlockCount > 0 || isFlatMode || isEFMode) {
                    builderEntries.add(PostingGenLookup.packCacheEntry(currentGen, sparseGenLoadedIdx));
                    return false;
                }
                currentGen--;
            }
            // A detached (per-worker) cursor must NEVER mutate the shared reader's genLookup cache
            // (see PostingIndexFwdReader): the dispatch-thread warm populates it before freeze; a
            // detached cursor that reaches here re-walked read-only and must not race on the write.
            if (!isCacheReplayMode && requestedKey >= 0 && !isDetached) {
                builderEntries.reverse();
                genLookup.putCacheEntries(requestedKey, builderEntries);
            }
            return true;
        }

        private void clearBlockState() {
            this.encodedBlockCount = 0;
            this.currentBlock = -1;
            this.minBlock = 0;
            this.blockBufferPos = -1;
            this.constantDeltaRemaining = 0;
            this.isEFMode = false;
            this.efHighWordIdx = -1;
            this.isFlatMode = false;
            this.flatRemaining = 0;
            this.bufferRangeChecked = false;
        }

        private void decodeBlock(int b) {
            long baseAddr = valueMem.addressOf(0);
            int count = Unsafe.getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
            int bitWidth = Unsafe.getByte(baseAddr + srcBitWidthsOffset + b) & 0xFF;
            int numDeltas = count - 1;

            long firstValue = Unsafe.getLong(baseAddr + srcFirstValuesOffset + (long) b * Long.BYTES);

            if (bitWidth == 0) {
                long minD = numDeltas > 0
                        ? Unsafe.getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES)
                        : 0;
                long startValue = firstValue + (long) numDeltas * minD;
                int remaining = count;
                if (maxValue < startValue) {
                    if (minD > 0) {
                        long over = (startValue - maxValue + minD - 1) / minD;
                        if (over >= remaining) {
                            if (coverCount > 0) sidecarOrdinal -= remaining;
                            remaining = 0;
                        } else {
                            startValue -= over * minD;
                            if (coverCount > 0) sidecarOrdinal -= (int) over;
                            remaining -= (int) over;
                        }
                    } else {
                        if (coverCount > 0) sidecarOrdinal -= remaining;
                        remaining = 0;
                    }
                }
                constantDeltaValue = startValue;
                constantDeltaStep = -minD;
                constantDeltaRemaining = remaining;
                blockBufferPos = -1;
                return;
            } else {
                // Variable-delta: decode to buffer
                ensureBuffer(count);
                Unsafe.putLong(blockBufferAddr, firstValue);
                if (numDeltas > 0) {
                    long minD = Unsafe.getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES);
                    long blockPackedAddr = srcPackedOffsetsOffset != 0
                            ? baseAddr + packedDataStartOffset + Unsafe.getLong(baseAddr + srcPackedOffsetsOffset + (long) b * Long.BYTES)
                            : baseAddr + packedDataStartOffset;
                    long scratchAddr = blockBufferAddr + Long.BYTES;
                    BitpackUtils.unpackAllValues(blockPackedAddr, numDeltas, bitWidth, minD, scratchAddr);
                    long cumulative = firstValue;
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getLong(scratchAddr + (long) i * Long.BYTES);
                        Unsafe.putLong(scratchAddr + (long) i * Long.BYTES, cumulative);
                    }
                }
            }
            blockBufferPos = count - 1;
            bufferRangeChecked = false;
        }

        private void decodeNextEFChunkReverse() {
            ensureBuffer(PostingIndexUtils.PACKED_BATCH_SIZE);
            long baseAddr = valueMem.addressOf(0);
            while (efHighWordIdx >= 0) {
                long word = Unsafe.getLong(baseAddr + efHighOffset + (long) efHighWordIdx * 8);
                if (word == 0) {
                    efHighWordIdx--;
                    continue;
                }
                int rankBefore = Unsafe.getInt(efRankDirAddr + (long) efHighWordIdx * Integer.BYTES);
                int bufIdx = 0;
                long w = word;
                while (w != 0) {
                    int trail = Long.numberOfTrailingZeros(w);
                    int globalIdx = rankBefore + bufIdx;
                    long highValue = (long) efHighWordIdx * 64 + trail - globalIdx;
                    long low = PostingIndexUtils.readBitsWord(baseAddr + efLowOffset, (long) globalIdx * efL, efL) & efLowMask;
                    Unsafe.putLong(blockBufferAddr + (long) bufIdx * Long.BYTES, (highValue << efL) | low);
                    bufIdx++;
                    w &= w - 1;
                }
                blockBufferPos = bufIdx - 1;
                bufferRangeChecked = false;
                efHighWordIdx--;
                return;
            }
            blockBufferPos = -1;
            bufferRangeChecked = false;
        }

        private void decodeNextFlatBatchReverse() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            ensureBuffer(batch);
            int batchStart = flatStartIdx - batch;
            long baseAddr = valueMem.addressOf(0);
            BitpackUtils.unpackValuesFrom(baseAddr + flatDataOffset, batchStart, batch, flatBitWidth, flatBaseValue, blockBufferAddr);
            flatStartIdx = batchStart;
            flatRemaining -= batch;
            blockBufferPos = batch - 1;
            bufferRangeChecked = true;
        }

        private void ensureBuffer(int count) {
            if (count <= blockBufferCapacity) return;
            int newCap = Math.max(count, MIN_BUFFER_CAPACITY);
            blockBufferAddr = Unsafe.realloc(
                    blockBufferAddr,
                    (long) blockBufferCapacity * Long.BYTES,
                    (long) newCap * Long.BYTES,
                    MemoryTag.NATIVE_INDEX_READER
            );
            blockBufferCapacity = newCap;
        }

        private void loadDenseGenerationCached(int gen) {
            this.isCurrentGenDense = true;
            this.bufferRangeChecked = false;
            // See loadSparseGenByPrefixSum: clear the EF flag so a previous
            // gen's EF mode cannot leak past this gen's empty early returns.
            this.isEFMode = false;
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            // Invariant: valueMem is pre-extended to its full published size
            // (valueMemSize) by the synchronous read setup (of -> mapValueMem /
            // reloadConditionally -> changeSize) and, for the parallel-decode
            // path, once up front by warmForKeys. No gen load may therefore need
            // to grow valueMem here; if it could, a worker decode would trigger a
            // remap and invalidate raw page addresses held by sibling cursors.
            assert genFileOffset + genDataSize <= valueMem.size()
                    : "covering gen exceeds pre-extended valueMem: off=" + genFileOffset + " len=" + genDataSize + " size=" + valueMem.size();
            if (genFileOffset + genDataSize > valueMem.size()) {
                throw CairoException.critical(0).put("covering gen data exceeds mapped valueMem [off=").put(genFileOffset).put(", len=").put(genDataSize).put(", size=").put(valueMem.size()).put(']');
            }
            Unsafe.loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.isFlatMode = false;
            this.sealedGenKeyCount = genKeyCount;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            cacheSidecarKeyAddrs(stride, localKey);
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            long strideOff = Unsafe.getLong(genAddr + (long) stride * Long.BYTES);
            long nextStrideOff = Unsafe.getLong(genAddr + (long) (stride + 1) * Long.BYTES);
            // Empty stride: writer records strideOff[s] == strideOff[s+1] when
            // stride s contributed no bytes. Reading on would interpret the next
            // stride's bytes here.
            if (nextStrideOff == strideOff) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }
            long strideFileOffset = genFileOffset + siSize + strideOff;
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            byte mode = Unsafe.getByte(strideAddr);
            assert mode == PostingIndexUtils.STRIDE_MODE_FLAT || mode == PostingIndexUtils.STRIDE_MODE_DELTA;

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int startCount = Unsafe.getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int endCount = Unsafe.getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
                int count = endCount - startCount;
                this.denseVarKeyStartCount = startCount;

                if (count == 0) {
                    this.encodedBlockCount = 0;
                    this.currentBlock = -1;
                    this.blockBufferPos = -1;
                    return;
                }

                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                long dataAddr = strideAddr + flatHeaderSize;

                int effectiveStart = startCount;
                int effectiveEnd = endCount;
                if (bitWidth == 0) {
                    if (baseValue < minValue || (maxValue < Long.MAX_VALUE && baseValue > maxValue)) {
                        effectiveEnd = effectiveStart;
                    }
                } else {
                    if (minValue > 0) {
                        int lo = startCount, hi = endCount;
                        while (lo < hi) {
                            int mid = (lo + hi) >>> 1;
                            long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                            if (val < minValue) {
                                lo = mid + 1;
                            } else {
                                hi = mid;
                            }
                        }
                        effectiveStart = lo;
                    }
                    if (maxValue < Long.MAX_VALUE && effectiveStart < effectiveEnd) {
                        int lo = effectiveStart, hi = effectiveEnd;
                        while (lo < hi) {
                            int mid = (lo + hi) >>> 1;
                            long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                            if (val > maxValue) {
                                hi = mid;
                            } else {
                                lo = mid + 1;
                            }
                        }
                        effectiveEnd = lo;
                    }
                }
                int effectiveCount = effectiveEnd - effectiveStart;

                if (effectiveCount == 0) {
                    this.encodedBlockCount = 0;
                    this.currentBlock = -1;
                    this.blockBufferPos = -1;
                    return;
                }

                int flatHeaderSizeForOffset = PostingIndexUtils.strideFlatHeaderSize(ks);
                this.isFlatMode = true;
                this.flatBitWidth = bitWidth;
                this.flatBaseValue = baseValue;
                this.flatDataOffset = strideFileOffset + flatHeaderSizeForOffset;
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.sidecarStrideKeyStart = effectiveStart - startCount;

                int batch = Math.min(effectiveCount, PostingIndexUtils.PACKED_BATCH_SIZE);
                int batchStart = effectiveStart + effectiveCount - batch;
                ensureBuffer(batch);
                BitpackUtils.unpackValuesFrom(dataAddr, batchStart, batch, bitWidth, baseValue, blockBufferAddr);
                this.blockBufferPos = batch - 1;
                this.flatStartIdx = batchStart;
                this.flatRemaining = effectiveCount - batch;
                // Set sidecar ordinal to just past the end so first hasNext decrement lands correctly
                this.sidecarOrdinal = effectiveCount;
                this.bufferRangeChecked = true;
                return;
            }

            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalValueCount = Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
            this.sidecarStrideKeyStart = 0;
            if (coverCount > 0) {
                int deltaKeyStartCount = 0;
                for (int k = 0; k < localKey; k++) {
                    deltaKeyStartCount += Unsafe.getInt(countsAddr + (long) k * Integer.BYTES);
                }
                this.denseVarKeyStartCount = deltaKeyStartCount;
            }
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            long dataOffset = Unsafe.getLong(offsetsBase + (long) localKey * Long.BYTES);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            long encodedOffset = strideFileOffset + deltaHeaderSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            // Set sidecar ordinal to just past the end for reverse iteration
            this.sidecarOrdinal = totalValueCount;

            readDeltaBlockMetadata(encodedOffset, totalValueCount);
        }

        private void loadSparseGenByPrefixSum(int gen) {
            this.isCurrentGenDense = false;
            this.bufferRangeChecked = false;
            // Clear the EF flag up front so a previous gen's EF mode never
            // survives into this gen's load. The early-return "no data for this
            // key" paths below reset encodedBlockCount and isFlatMode but not
            // isEFMode; without this, a stale isEFMode makes the build-mode
            // cache-add guard (hasNext -> advanceToPrevRelevantGen) record a
            // bogus (gen, posInGen) entry for a gen that has no values for the
            // key. Replaying that entry then reads the wrong key's posting list
            // (or an out-of-bounds offset). readDeltaBlockMetadata sets the flag
            // correctly when this gen actually has data.
            this.isEFMode = false;
            computePerColumnSidecarOffsets(gen);
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            // Invariant: valueMem is pre-extended to its full published size
            // (valueMemSize) by the synchronous read setup (of -> mapValueMem /
            // reloadConditionally -> changeSize) and, for the parallel-decode
            // path, once up front by warmForKeys. No gen load may therefore need
            // to grow valueMem here; if it could, a worker decode would trigger a
            // remap and invalidate raw page addresses held by sibling cursors.
            assert genFileOffset + genDataSize <= valueMem.size()
                    : "covering gen exceeds pre-extended valueMem: off=" + genFileOffset + " len=" + genDataSize + " size=" + valueMem.size();
            if (genFileOffset + genDataSize > valueMem.size()) {
                throw CairoException.critical(0).put("covering gen data exceeds mapped valueMem [off=").put(genFileOffset).put(", len=").put(genDataSize).put(", size=").put(valueMem.size()).put(']');
            }
            Unsafe.loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            int minKey = genLookup.getGenMinKey(gen);
            int maxKey = genLookup.getGenMaxKey(gen);
            if (requestedKey < minKey || requestedKey > maxKey) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.isFlatMode = false;
                return;
            }

            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
            int k = requestedKey - minKey;
            int start = Unsafe.getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start == end) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.isFlatMode = false;
                return;
            }

            this.isFlatMode = false;
            this.sparseGenLoadedIdx = start;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalValueCount = Unsafe.getInt(countsBase + (long) start * Integer.BYTES);
            long dataOffset = Unsafe.getLong(offsetsBase + (long) start * Long.BYTES);
            long encodedOffset = genFileOffset + headerSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            // For sparse gens, compute sidecar base and set ordinal past the end
            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < start; i++) {
                    sidecarBase += Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase + totalValueCount;
            } else {
                this.sidecarOrdinal = totalValueCount;
            }

            readDeltaBlockMetadata(encodedOffset, totalValueCount);
        }

        private void loadSparseGenDirect(int gen, int idx) {
            this.isCurrentGenDense = false;
            this.bufferRangeChecked = false;
            // See loadSparseGenByPrefixSum: clear the EF flag so a previous
            // gen's EF mode cannot leak past this gen's empty (totalValueCount
            // == 0) early return and make hasNext re-decode the prior gen.
            this.isEFMode = false;
            computePerColumnSidecarOffsets(gen);
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            // Invariant: valueMem is pre-extended to its full published size
            // (valueMemSize) by the synchronous read setup (of -> mapValueMem /
            // reloadConditionally -> changeSize) and, for the parallel-decode
            // path, once up front by warmForKeys. No gen load may therefore need
            // to grow valueMem here; if it could, a worker decode would trigger a
            // remap and invalidate raw page addresses held by sibling cursors.
            assert genFileOffset + genDataSize <= valueMem.size()
                    : "covering gen exceeds pre-extended valueMem: off=" + genFileOffset + " len=" + genDataSize + " size=" + valueMem.size();
            if (genFileOffset + genDataSize > valueMem.size()) {
                throw CairoException.critical(0).put("covering gen data exceeds mapped valueMem [off=").put(genFileOffset).put(", len=").put(genDataSize).put(", size=").put(valueMem.size()).put(']');
            }
            Unsafe.loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.isFlatMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalValueCount = Unsafe.getInt(countsBase + (long) idx * Integer.BYTES);
            long dataOffset = Unsafe.getLong(offsetsBase + (long) idx * Long.BYTES);
            long encodedOffset = genFileOffset + headerSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            // For sparse gens, compute sidecar base and set ordinal past the end
            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < idx; i++) {
                    sidecarBase += Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase + totalValueCount;
            } else {
                this.sidecarOrdinal = totalValueCount;
            }

            readDeltaBlockMetadata(encodedOffset, totalValueCount);
        }

        private void readDeltaBlockMetadata(long encodedOffset, int totalValueCount) {
            long baseAddr = valueMem.addressOf(0);
            long pos = encodedOffset;
            int firstWord = Unsafe.getInt(baseAddr + pos);
            if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
                pos += 4;
                int efTotalCount = Unsafe.getInt(baseAddr + pos);
                pos += 4;
                efL = Unsafe.getByte(baseAddr + pos) & 0xFF;
                pos += 1;
                long u = Unsafe.getLong(baseAddr + pos);
                pos += 8;
                efLowMask = (efL < 64) ? (1L << efL) - 1 : -1L;
                efLowOffset = pos;
                int lowBytes = PostingIndexUtils.efLowBytesAligned(efTotalCount, efL);
                efHighOffset = pos + lowBytes;
                int efNumHighWords = (int) ((efTotalCount + (u >>> efL) + 63) / 64);
                // Build rank directory for reverse iteration
                if (efNumHighWords > efRankDirCapacity) {
                    int newCap = Math.max(efNumHighWords, efRankDirCapacity * 2);
                    efRankDirAddr = Unsafe.realloc(
                            efRankDirAddr,
                            (long) efRankDirCapacity * Integer.BYTES,
                            (long) newCap * Integer.BYTES,
                            MemoryTag.NATIVE_INDEX_READER
                    );
                    efRankDirCapacity = newCap;
                }
                int cumulative = 0;
                for (int w = 0; w < efNumHighWords; w++) {
                    Unsafe.putInt(efRankDirAddr + (long) w * Integer.BYTES, cumulative);
                    cumulative += Long.bitCount(Unsafe.getLong(baseAddr + efHighOffset + (long) w * 8));
                }
                efHighWordIdx = efNumHighWords - 1;
                isEFMode = true;
                encodedBlockCount = 0;
                isFlatMode = false;
                currentBlock = -1;
                blockBufferPos = -1;
                return;
            }
            isEFMode = false;
            if (firstWord < 0 || firstWord > (totalValueCount + PostingIndexUtils.BLOCK_CAPACITY - 1) / PostingIndexUtils.BLOCK_CAPACITY) {
                throw CairoException.critical(0).put("corrupt posting index: invalid block count [blockCount=")
                        .put(firstWord).put(", totalValues=").put(totalValueCount).put(']');
            }
            pos += 4;

            srcValueCountsOffset = pos;
            pos += firstWord;

            srcFirstValuesOffset = pos;
            pos += (long) firstWord * Long.BYTES;

            srcMinDeltasOffset = pos;
            pos += (long) firstWord * Long.BYTES;

            srcBitWidthsOffset = pos;
            pos += firstWord;

            if (firstWord > 1) {
                srcPackedOffsetsOffset = pos;
                pos += (long) firstWord * Long.BYTES;
            } else {
                srcPackedOffsetsOffset = 0;
            }

            packedDataStartOffset = pos;

            int endBlock = firstWord;
            if (maxValue < Long.MAX_VALUE && firstWord > 1) {
                int lo = 0, hi = firstWord - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getLong(baseAddr + srcFirstValuesOffset + (long) mid * Long.BYTES) <= maxValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                endBlock = lo + 1;
            }

            int startBlock = 0;
            if (minValue > 0 && firstWord > 1) {
                int lo = 0, hi = firstWord - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getLong(baseAddr + srcFirstValuesOffset + (long) mid * Long.BYTES) <= minValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                startBlock = lo;
            }

            // Adjust sidecar ordinal for trimmed trailing blocks — those values
            // are never iterated, so the ordinal must not count them.
            if (endBlock < firstWord && coverCount > 0) {
                int trailingTrimmedCount = 0;
                for (int b = endBlock; b < firstWord; b++) {
                    trailingTrimmedCount += Unsafe.getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
                }
                this.sidecarOrdinal -= trailingTrimmedCount;
            }

            this.encodedBlockCount = endBlock;
            this.currentBlock = endBlock - 1;
            this.minBlock = startBlock;
            this.blockBufferPos = -1;
        }

        void of(int key, long minValue, long maxValue) {
            this.cursorGenCount = genCount;
            clearBlockState();
            resetCoveringState();
            builderEntries.clear();
            isCacheReplayMode = false;
            cacheReplayPos = 0;
            cacheReplayEnd = 0;
            this.minValue = minValue;
            this.maxValue = maxValue;

            if (keyCount == 0 || key < 0 || key >= keyCount || cursorGenCount == 0) {
                this.requestedKey = -1;
                currentGen = -1;
                return;
            }

            this.requestedKey = key;

            // Fast path: sealed single-generation dense index. No advance machinery
            // needed; cache offers no win because there is no SBBF skip to amortize
            // and dense gens never read prefix sums.
            if (cursorGenCount == 1 && genLookup.getGenKeyCount(0) >= 0) {
                this.currentGen = -1;
                loadDenseGenerationCached(0);
                return;
            }

            this.currentGen = cursorGenCount;

            long packedSlot = genLookup.cacheLookup(key);
            if (packedSlot != PostingGenLookup.CACHE_NOT_PRESENT) {
                isCacheReplayMode = true;
                cacheReplayPos = PostingGenLookup.unpackEntryStart(packedSlot);
                cacheReplayEnd = cacheReplayPos + PostingGenLookup.unpackEntryCount(packedSlot);
                cacheVersionAtOf = genLookup.getCacheVersion();
            }

            if (advanceToPrevRelevantGen()) {
                currentGen = -1;
                encodedBlockCount = 0;
                currentBlock = -1;
                blockBufferPos = -1;
            }
        }

        protected void releaseResources() {
            if (blockBufferAddr != 0) {
                Unsafe.free(blockBufferAddr, (long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferAddr = 0;
                blockBufferCapacity = 0;
            }
            if (efRankDirAddr != 0) {
                Unsafe.free(efRankDirAddr, (long) efRankDirCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                efRankDirAddr = 0;
                efRankDirCapacity = 0;
            }
            closeCoveringResources();
        }
    }

    private class NullCursor extends Cursor {
        private long nullCount;
        private long nullPos;

        @Override
        public void close() {
            // See Cursor.close(): detached cursors bypass the operating-thread
            // gate and the pool, freeing their own native scratch directly.
            if (isDetached) {
                releaseResources();
                return;
            }
            // See Cursor.close(): re-pool only while the reader is open and on the
            // reader's operating thread; otherwise release directly.
            if (canRepool(freeNullCursors.size())) {
                isPooled = true;
                closeCoveringResources();
                if (efRankDirAddr != 0) {
                    Unsafe.free(efRankDirAddr, (long) efRankDirCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    efRankDirAddr = 0;
                    efRankDirCapacity = 0;
                }
                resetCoveringState();
                freeNullCursors.add(this);
                return;
            }
            releaseResources();
        }

        @Override
        public boolean hasNext() {
            if (super.hasNext()) {
                return true;
            }
            if (--nullPos >= minValue) {
                next = nullPos;
                return true;
            }
            return false;
        }

        @Override
        public long size() {
            // nullCount is set in getCursor from the unclamped caller maxValue
            // and never mutates during iteration; using it directly avoids the
            // Cursor.maxValue field, which now holds the entryMaxValue-clamped
            // bound and would under-count nulls when entryMaxValue < columnTop.
            long indexSize = super.size();
            return indexSize < 0 ? -1 : indexSize + Math.max(0L, nullCount - minValue);
        }
    }
}
