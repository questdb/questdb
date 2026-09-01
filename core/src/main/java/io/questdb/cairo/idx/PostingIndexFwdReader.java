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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.DirectBinarySequence;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8Sequence;

public class PostingIndexFwdReader extends AbstractPostingIndexReader {
    private static final int MIN_BUFFER_CAPACITY = 4;
    private final ObjList<Cursor> freeCursors = new ObjList<>();
    private final ObjList<NullCursor> freeNullCursors = new ObjList<>();

    public PostingIndexFwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, null, null, 0);
    }

    public PostingIndexFwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            io.questdb.cairo.sql.RecordMetadata metadata,
            io.questdb.cairo.ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp);
    }

    public PostingIndexFwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            io.questdb.cairo.sql.RecordMetadata metadata,
            io.questdb.cairo.ColumnVersionReader columnVersionReader,
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

        // Clamp the index-walked range to the picked chain entry's
        // tracked maxValue. Writers can leave dirty (key, rowId) entries
        // in .pv past the chain entry's coverage (e.g. an O3 split that
        // shrinks the partition before the next reseal evicts them, or
        // a stale generation a sparse-gen append later supersedes); the
        // entry's MAX_VALUE field is the boundary between clean and
        // dirty rows, and the reader is the only place that can skip
        // them without a full reseal. Implicit nulls (rows before
        // columnTop) are independent of the index and stay clamped by
        // columnTop only.
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
            nc.npRequiredColumns = requiredCoverColumns;
            nc.nullPos = minValue;
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nc.nullCount = Math.min(columnTop, hi);
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
     * Returns a cursor that a single worker thread owns outright: it is
     * constructed fresh (never popped from the shared freeCursors pool) and
     * marked detached, so its {@link Cursor#close()} frees its own native
     * scratch directly and never pushes back to the pool. This makes N such
     * cursors safe to iterate concurrently over ONE reader, provided the
     * reader's shared state was made read-only first via
     * {@link AbstractPostingIndexReader#warmForKeys}. Positioning is identical
     * to {@link #getCursor(int, long, long, int[])}; only the construct/close
     * lifecycle differs.
     * <p>
     * Unlike {@link #getCursor}, this does NOT stamp the operating-thread
     * tripwire: detached cursors are deliberately driven off the reader's
     * owning thread, and pooled cursors must not be in flight while detached
     * ones run (the warm/decode split in the async covered-decode pipeline
     * guarantees this).
     */
    @Override
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
            nc.npRequiredColumns = requiredCoverColumns;
            nc.nullPos = minValue;
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nc.nullCount = Math.min(columnTop, hi);
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
        private int blockBufferEnd;
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
        private int efNumHighWords;
        private int efOutputCount;
        private int efTotalCount;
        private int encodedBlockCount;
        private long encodedOffset;
        private long flatBaseValue;
        private int flatBitWidth;
        private long flatDataOffset;
        private int flatRemaining;
        private int flatStartIdx;
        private boolean isCacheReplayMode;
        private boolean isEFMode;
        private boolean isFlatMode;
        private long packedDataOffset;
        private int sparseGenLoadedIdx;
        private long srcBitWidthsOffset;
        private long srcFirstValuesOffset;
        private long srcMinDeltasOffset;
        private long srcValueCountsOffset;
        private int totalValueCount;

        @Override
        public void close() {
            // Detached cursors are owned by a single worker thread that is, by
            // design, NOT the reader's owning thread; they never touch the
            // shared freeCursors pool. Free their native scratch directly and
            // skip both the operating-thread gate and the pool-push.
            if (isDetached) {
                releaseResources();
                return;
            }
            // Re-pool only while the reader is still open (a cursor that re-pools
            // after the reader closed would strand blockBufferAddr,
            // NATIVE_INDEX_READER, in a never-drained pool) and on the reader's
            // operating thread; off-thread closes release the cursor-local
            // buffers directly. See AbstractPostingIndexReader.isOperatingThread()
            // for the full rationale and the gate's limits.
            if (canRepool(freeCursors.size())) {
                isPooled = true;
                closeCoveringResources();
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
                    if (value > maxValue) {
                        constantDeltaRemaining = 0;
                        return false;
                    }
                    next = value;
                    if (coverCount > 0) {
                        cachedSidecarIdx = isCurrentGenDense
                                ? sidecarStrideKeyStart + sidecarOrdinal
                                : sidecarOrdinal;
                        sidecarOrdinal++;
                    }
                    return true;
                }

                // Serve from block buffer
                if (bufferRangeChecked) {
                    if (blockBufferPos < blockBufferEnd) {
                        this.next = Unsafe.getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                        blockBufferPos++;
                        if (coverCount > 0) {
                            cachedSidecarIdx = isCurrentGenDense
                                    ? sidecarStrideKeyStart + sidecarOrdinal
                                    : sidecarOrdinal;
                            sidecarOrdinal++;
                        }
                        return true;
                    }
                } else {
                    while (blockBufferPos < blockBufferEnd) {
                        long value = Unsafe.getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                        blockBufferPos++;
                        if (value > maxValue) {
                            blockBufferPos = blockBufferEnd;
                            return false;
                        }
                        if (value >= minValue) {
                            this.next = value;
                            if (coverCount > 0) {
                                cachedSidecarIdx = isCurrentGenDense
                                        ? sidecarStrideKeyStart + sidecarOrdinal
                                        : sidecarOrdinal;
                                sidecarOrdinal++;
                            }
                            return true;
                        }
                        if (coverCount > 0) sidecarOrdinal++;
                    }
                }

                // Decode next block in current generation
                if (currentBlock < encodedBlockCount) {
                    decodeNextBlock();
                    continue;
                }

                // EF mode: decode next chunk
                if (isEFMode && efOutputCount < efTotalCount) {
                    decodeNextEFChunk();
                    continue;
                }

                // Flat mode: decode next batch
                if (isFlatMode && flatRemaining > 0) {
                    decodeNextFlatBatch();
                    continue;
                }

                // Advance to next generation
                if (!advanceToNextRelevantGen()) {
                    return false;
                }
            }
        }

        @Override
        public long next() {
            return next - minValue;
        }

        private boolean advanceToNextRelevantGen() {
            // Bail if cache was invalidated mid-iteration — replay pos would be stale.
            if (isCacheReplayMode && cacheVersionAtOf != genLookup.getCacheVersion()) {
                return false;
            }
            currentGen++;
            while (currentGen < cursorGenCount) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return true;
                }
                if (isCacheReplayMode) {
                    if (cacheReplayPos >= cacheReplayEnd) {
                        currentGen++;
                        continue;
                    }
                    long entry = genLookup.cacheEntryAt(cacheReplayPos);
                    int hitGen = PostingGenLookup.unpackCacheGen(entry);
                    if (currentGen < hitGen) {
                        currentGen++;
                        continue;
                    }
                    cacheReplayPos++;
                    loadSparseGenDirect(currentGen, PostingGenLookup.unpackCachePosInGen(entry));
                    return true;
                }
                if (requestedKey < genLookup.getGenMinKey(currentGen)
                        || requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen++;
                    continue;
                }
                if (genLookup.notContainKey(valueMem, currentGen, requestedKey)) {
                    currentGen++;
                    continue;
                }
                loadSparseGenByPrefixSum(currentGen);
                if (totalValueCount > 0 || encodedBlockCount > 0 || isFlatMode || isEFMode) {
                    builderEntries.add(PostingGenLookup.packCacheEntry(currentGen, sparseGenLoadedIdx));
                    return true;
                }
                currentGen++;
            }
            // Reached the end naturally. Commit accumulated entries iff we built them ourselves.
            // A detached (per-worker) cursor must NEVER mutate the shared reader's genLookup cache:
            // many workers run concurrently against one frozen reader, so the dispatch-thread warm
            // (populateCacheForKey, before freeze) is the only thing allowed to populate it. A
            // detached cursor that reaches here simply re-walked the gen read-only — correct, just
            // not memoized — so it must not race on putCacheEntries.
            if (!isCacheReplayMode && requestedKey >= 0 && !isDetached) {
                genLookup.putCacheEntries(requestedKey, builderEntries);
            }
            return false;
        }

        private void clearBlockState() {
            this.encodedBlockCount = 0;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
            this.constantDeltaRemaining = 0;
            this.isEFMode = false;
            this.efOutputCount = 0;
            this.efTotalCount = 0;
            this.isFlatMode = false;
            this.flatRemaining = 0;
            this.totalValueCount = 0;
            this.bufferRangeChecked = false;
        }

        private void decodeNextBlock() {
            int b = currentBlock;
            long baseAddr = valueMem.addressOf(0);
            int count = Unsafe.getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
            int bitWidth = Unsafe.getByte(baseAddr + srcBitWidthsOffset + b) & 0xFF;
            int numDeltas = count - 1;

            long firstValue = Unsafe.getLong(baseAddr + srcFirstValuesOffset + (long) b * Long.BYTES);
            currentBlock++;

            if (bitWidth == 0) {
                long minD = numDeltas > 0
                        ? Unsafe.getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES)
                        : 0;
                long startValue = firstValue;
                int remaining = count;
                if (minValue > startValue) {
                    if (minD > 0) {
                        long skip = (minValue - startValue + minD - 1) / minD;
                        if (skip >= remaining) {
                            if (coverCount > 0) sidecarOrdinal += remaining;
                            remaining = 0;
                        } else {
                            startValue += skip * minD;
                            if (coverCount > 0) sidecarOrdinal += (int) skip;
                            remaining -= (int) skip;
                        }
                    } else {
                        if (coverCount > 0) sidecarOrdinal += remaining;
                        remaining = 0;
                    }
                }
                constantDeltaValue = startValue;
                constantDeltaStep = minD;
                constantDeltaRemaining = remaining;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                return;
            } else {
                // Variable-delta: decode to buffer
                ensureBuffer(count);
                Unsafe.putLong(blockBufferAddr, firstValue);
                if (numDeltas > 0) {
                    long minD = Unsafe.getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES);
                    long scratchAddr = blockBufferAddr + Long.BYTES;
                    BitpackUtils.unpackAllValues(baseAddr + packedDataOffset, numDeltas, bitWidth, minD, scratchAddr);
                    packedDataOffset += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    long cumulative = firstValue;
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getLong(scratchAddr + (long) i * Long.BYTES);
                        Unsafe.putLong(scratchAddr + (long) i * Long.BYTES, cumulative);
                    }
                }
            }
            blockBufferPos = 0;
            blockBufferEnd = count;
            bufferRangeChecked = false;
        }

        private void decodeNextEFChunk() {
            // Fused single-pass: extract low bits from a sliding 64-bit window
            // while scanning high bits, writing each value in one store.
            // Accumulates across multiple high-bits words to fill the buffer.
            ensureBuffer(PostingIndexUtils.PACKED_BATCH_SIZE);
            int totalBuf = 0;
            long baseAddr = valueMem.addressOf(0);

            // Load low-bits window at current position
            long lowBitPos = (long) efOutputCount * efL;
            long lowWordAddr = baseAddr + efLowOffset + ((lowBitPos >>> 6) << 3);
            int lowBitOffset = (int) (lowBitPos & 63);

            while (efHighWordIdx < efNumHighWords && efOutputCount < efTotalCount && totalBuf < blockBufferCapacity) {
                long word = Unsafe.getLong(baseAddr + efHighOffset + (long) efHighWordIdx * 8);
                if (word == 0) {
                    efHighWordIdx++;
                    continue;
                }
                int chunkCount = Math.min(Long.bitCount(word), efTotalCount - efOutputCount);
                if (chunkCount > blockBufferCapacity - totalBuf) {
                    break;
                }

                long base = (long) efHighWordIdx * 64 - efOutputCount;
                int bufPos = 0;
                while (word != 0 && bufPos < chunkCount) {
                    int trail = Long.numberOfTrailingZeros(word);

                    // Extract L low bits from the sliding window
                    long low;
                    if (efL == 0) {
                        low = 0;
                    } else {
                        long lowWord = Unsafe.getLong(lowWordAddr);
                        low = (lowWord >>> lowBitOffset) & efLowMask;
                        if (lowBitOffset + efL > 64) {
                            // Spans two words — merge bits from next word
                            low |= (Unsafe.getLong(lowWordAddr + 8) << (64 - lowBitOffset)) & efLowMask;
                        }
                        lowBitOffset += efL;
                        if (lowBitOffset >= 64) {
                            lowWordAddr += 8;
                            lowBitOffset -= 64;
                        }
                    }

                    Unsafe.putLong(
                            blockBufferAddr + (long) (totalBuf + bufPos) * Long.BYTES,
                            ((base + trail) << efL) | low);
                    bufPos++;
                    efOutputCount++;
                    base--;
                    word &= word - 1;
                }

                totalBuf += chunkCount;
                efHighWordIdx++;
            }
            blockBufferPos = 0;
            blockBufferEnd = totalBuf;
            bufferRangeChecked = false;
        }

        private void decodeNextFlatBatch() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            ensureBuffer(batch);
            long baseAddr = valueMem.addressOf(0);
            BitpackUtils.unpackValuesFrom(baseAddr + flatDataOffset, flatStartIdx, batch, flatBitWidth, flatBaseValue, blockBufferAddr);
            flatStartIdx += batch;
            flatRemaining -= batch;
            blockBufferPos = 0;
            blockBufferEnd = batch;
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
            this.sidecarOrdinal = 0;
            this.bufferRangeChecked = false;
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                clearBlockState();
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
                clearBlockState();
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
                    clearBlockState();
                    return;
                }

                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                long dataAddr = strideAddr + flatHeaderSize;

                int effectiveStart = startCount;
                int effectiveCount = count;
                if (bitWidth == 0) {
                    if (baseValue < minValue || (maxValue < Long.MAX_VALUE && baseValue > maxValue)) {
                        effectiveCount = 0;
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
                        effectiveCount = endCount - effectiveStart;
                    }

                    if (maxValue < Long.MAX_VALUE && effectiveCount > 0) {
                        int lo = effectiveStart, hi = effectiveStart + effectiveCount;
                        while (lo < hi) {
                            int mid = (lo + hi) >>> 1;
                            long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                            if (val > maxValue) {
                                hi = mid;
                            } else {
                                lo = mid + 1;
                            }
                        }
                        effectiveCount = lo - effectiveStart;
                    }
                }

                if (effectiveCount == 0) {
                    clearBlockState();
                    return;
                }

                this.isFlatMode = true;
                this.flatBitWidth = bitWidth;
                this.flatBaseValue = baseValue;
                this.flatDataOffset = strideFileOffset + flatHeaderSize;
                this.encodedBlockCount = 0;
                this.currentBlock = 0;
                this.sidecarStrideKeyStart = effectiveStart - startCount;
                this.denseVarKeyStartCount = startCount;
                this.sidecarOrdinal = 0;

                int batch = Math.min(effectiveCount, PostingIndexUtils.PACKED_BATCH_SIZE);
                ensureBuffer(batch);
                BitpackUtils.unpackValuesFrom(dataAddr, effectiveStart, batch, bitWidth, baseValue, blockBufferAddr);
                this.blockBufferPos = 0;
                this.blockBufferEnd = batch;
                this.flatStartIdx = effectiveStart + batch;
                this.flatRemaining = effectiveCount - batch;
                this.bufferRangeChecked = true;
                return;
            }

            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            this.totalValueCount = Unsafe.getInt(countsAddr + (long) localKey * Integer.BYTES);
            this.sidecarStrideKeyStart = 0;
            this.sidecarOrdinal = 0;
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
            this.encodedOffset = strideFileOffset + deltaHeaderSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void loadSparseGenByPrefixSum(int gen) {
            this.isCurrentGenDense = false;
            this.bufferRangeChecked = false;
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
                clearBlockState();
                totalValueCount = 0;
                this.isFlatMode = false;
                return;
            }

            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen, valueMem));
            int k = requestedKey - minKey;
            int start = Unsafe.getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start == end) {
                clearBlockState();
                totalValueCount = 0;
                this.isFlatMode = false;
                return;
            }

            this.isFlatMode = false;
            this.sparseGenLoadedIdx = start;

            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < start; i++) {
                    sidecarBase += Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase;
            } else {
                this.sidecarOrdinal = 0;
            }

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getInt(countsBase + (long) start * Integer.BYTES);
            long dataOffset = Unsafe.getLong(offsetsBase + (long) start * Long.BYTES);
            this.encodedOffset = genFileOffset + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void loadSparseGenDirect(int gen, int idx) {
            this.isCurrentGenDense = false;
            this.bufferRangeChecked = false;
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

            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < idx; i++) {
                    sidecarBase += Unsafe.getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase;
            } else {
                this.sidecarOrdinal = 0;
            }

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getInt(countsBase + (long) idx * Integer.BYTES);
            long dataOffset = Unsafe.getLong(offsetsBase + (long) idx * Long.BYTES);
            this.encodedOffset = genFileOffset + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void readDeltaBlockMetadata() {
            if (totalValueCount == 0) {
                clearBlockState();
                return;
            }

            long baseAddr = valueMem.addressOf(0);
            long pos = encodedOffset;
            int firstWord = Unsafe.getInt(baseAddr + pos);
            if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
                pos += 4;
                efTotalCount = Unsafe.getInt(baseAddr + pos);
                pos += 4;
                efL = Unsafe.getByte(baseAddr + pos) & 0xFF;
                pos += 1;
                long u = Unsafe.getLong(baseAddr + pos);
                pos += 8;
                efLowMask = (efL < 64) ? (1L << efL) - 1 : -1L;
                efLowOffset = pos;
                int lowBytes = PostingIndexUtils.efLowBytesAligned(efTotalCount, efL);
                efHighOffset = pos + lowBytes;
                efNumHighWords = (int) ((efTotalCount + (u >>> efL) + 63) / 64);
                efHighWordIdx = 0;
                efOutputCount = 0;
                isEFMode = true;
                encodedBlockCount = 0;
                isFlatMode = false;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                constantDeltaRemaining = 0;
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

            long srcPackedOffsetsOffset = 0;
            if (firstWord > 1) {
                srcPackedOffsetsOffset = pos;
                pos += (long) firstWord * Long.BYTES;
            }

            long packedDataStartOffset = pos;

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

            int skippedValueCount = 0;
            for (int b = 0; b < startBlock; b++) {
                skippedValueCount += Unsafe.getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
            }
            if (startBlock > 0) {
                packedDataStartOffset += Unsafe.getLong(baseAddr + srcPackedOffsetsOffset + (long) startBlock * Long.BYTES);
            }
            this.sidecarStrideKeyStart += skippedValueCount;
            this.denseVarKeyStartCount += skippedValueCount;
            if (!isCurrentGenDense && coverCount > 0) {
                this.sidecarOrdinal += skippedValueCount;
            }

            int endBlock = firstWord;
            if (maxValue < Long.MAX_VALUE && firstWord > 0) {
                for (int b = startBlock; b < firstWord; b++) {
                    if (Unsafe.getLong(baseAddr + srcFirstValuesOffset + (long) b * Long.BYTES) > maxValue) {
                        endBlock = b;
                        break;
                    }
                }
            }

            this.encodedBlockCount = endBlock;
            this.packedDataOffset = packedDataStartOffset;
            this.currentBlock = startBlock;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
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
                currentGen = cursorGenCount;
                return;
            }

            this.requestedKey = key;

            // Fast path: sealed single-generation dense index. No advance machinery
            // needed; cache offers no win because there is no SBBF skip to amortize
            // and dense gens never read prefix sums.
            if (cursorGenCount == 1 && genLookup.getGenKeyCount(0) >= 0) {
                this.currentGen = cursorGenCount;
                loadDenseGenerationCached(0);
                return;
            }

            this.currentGen = -1;

            long packedSlot = genLookup.cacheLookup(key);
            if (packedSlot != PostingGenLookup.CACHE_NOT_PRESENT) {
                isCacheReplayMode = true;
                cacheReplayPos = PostingGenLookup.unpackEntryStart(packedSlot);
                cacheReplayEnd = cacheReplayPos + PostingGenLookup.unpackEntryCount(packedSlot);
                cacheVersionAtOf = genLookup.getCacheVersion();
            }

            advanceToNextRelevantGen();
        }

        protected void releaseResources() {
            if (blockBufferAddr != 0) {
                Unsafe.free(blockBufferAddr, (long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferAddr = 0;
                blockBufferCapacity = 0;
            }
            closeCoveringResources();
        }
    }

    private class NullCursor extends Cursor {
        private long nullCount;
        private long nullPos;
        // Raw-column fallback for the implicit null-prefix (rows [minValue, columnTop)
        // of the INDEXED column, which the posting chain carries no entries for). An
        // INCLUDEd column can predate the indexed column -- have real, non-null data
        // over that same row range -- so the covered read for these synthetic rows
        // cannot use the sidecar (built only from the chain's real postings): it must
        // read the INCLUDEd column's own .d/.i files directly, honouring THAT column's
        // own column top. Opened lazily, once per checkout; freed in
        // closeCoveringResources() (called on every close, pooled or final -- see
        // Cursor.close()/releaseResources()), so a reused cursor always re-resolves
        // against whatever partition the outer reader is currently bound to.
        private long[] npAuxAddrs;
        private long[] npAuxSizes;
        private long[] npColAddrs;
        private long[] npColSizes;
        private long[] npColTops;
        private boolean npColumnsOpened;
        // The cover-column subset this checkout's caller declared it reads (the query
        // projection), captured at getCursor()/getDetachedCursor().
        // ensureNullPrefixColumnsOpen() maps only these columns, mirroring
        // openRequiredSidecars() on the real-posting branch: a covered getter for a
        // column outside the declared subset breaks the same contract there, and both
        // branches answer it with a miss (a 0 address here, an unmapped sidecar there).
        // null means the caller declared no subset at all, and every cover column is
        // mapped, exactly as it was before this filter.
        private int[] npRequiredColumns;
        // True only while hasNext() is currently serving a synthetic null-prefix row
        // (as opposed to a real posting reached via super.hasNext()). Gates every
        // getCoveredXxx override below: false means behave exactly as inherited.
        private boolean isNullPrefixRow;

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
                resetCoveringState();
                freeNullCursors.add(this);
                return;
            }
            releaseResources();
        }

        @Override
        public ArrayView getCoveredArray(int includeIdx, int columnType) {
            if (isNullPrefixRow) {
                ArrayView v = readRawCoveredArray(includeIdx, next, columnType, arrayView);
                if (v != null) {
                    return v;
                }
            }
            return super.getCoveredArray(includeIdx, columnType);
        }

        @Override
        public BinarySequence getCoveredBin(int includeIdx) {
            if (isNullPrefixRow) {
                BinarySequence v = readRawCoveredBin(includeIdx, next, binView);
                if (v != null) {
                    return v;
                }
            }
            return super.getCoveredBin(includeIdx);
        }

        @Override
        public long getCoveredBinLen(int includeIdx) {
            if (isNullPrefixRow) {
                BinarySequence v = readRawCoveredBin(includeIdx, next, binView);
                if (v != null) {
                    return v.length();
                }
            }
            return super.getCoveredBinLen(includeIdx);
        }

        @Override
        public byte getCoveredByte(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, Byte.BYTES);
            return addr != 0 ? Unsafe.getByte(addr) : super.getCoveredByte(includeIdx);
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, Double.BYTES);
            return addr != 0 ? Unsafe.getDouble(addr) : super.getCoveredDouble(includeIdx);
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, Float.BYTES);
            return addr != 0 ? Unsafe.getFloat(addr) : super.getCoveredFloat(includeIdx);
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, Integer.BYTES);
            return addr != 0 ? Unsafe.getInt(addr) : super.getCoveredInt(includeIdx);
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, Long.BYTES);
            return addr != 0 ? Unsafe.getLong(addr) : super.getCoveredLong(includeIdx);
        }

        @Override
        public long getCoveredLong128Hi(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, 16);
            return addr != 0 ? Unsafe.getLong(addr + 8) : super.getCoveredLong128Hi(includeIdx);
        }

        @Override
        public long getCoveredLong128Lo(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, 16);
            return addr != 0 ? Unsafe.getLong(addr) : super.getCoveredLong128Lo(includeIdx);
        }

        @Override
        public long getCoveredLong256_0(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, 32);
            return addr != 0 ? Unsafe.getLong(addr) : super.getCoveredLong256_0(includeIdx);
        }

        @Override
        public long getCoveredLong256_1(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, 32);
            return addr != 0 ? Unsafe.getLong(addr + 8) : super.getCoveredLong256_1(includeIdx);
        }

        @Override
        public long getCoveredLong256_2(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, 32);
            return addr != 0 ? Unsafe.getLong(addr + 16) : super.getCoveredLong256_2(includeIdx);
        }

        @Override
        public long getCoveredLong256_3(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, 32);
            return addr != 0 ? Unsafe.getLong(addr + 24) : super.getCoveredLong256_3(includeIdx);
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            long addr = resolveRawFixedAddr(includeIdx, next, Short.BYTES);
            return addr != 0 ? Unsafe.getShort(addr) : super.getCoveredShort(includeIdx);
        }

        @Override
        public CharSequence getCoveredStrA(int includeIdx) {
            if (isNullPrefixRow) {
                CharSequence v = readRawCoveredStr(includeIdx, next, stringViewA);
                if (v != null) {
                    return v;
                }
            }
            return super.getCoveredStrA(includeIdx);
        }

        @Override
        public CharSequence getCoveredStrB(int includeIdx) {
            if (isNullPrefixRow) {
                CharSequence v = readRawCoveredStr(includeIdx, next, stringViewB);
                if (v != null) {
                    return v;
                }
            }
            return super.getCoveredStrB(includeIdx);
        }

        @Override
        public Utf8Sequence getCoveredVarcharA(int includeIdx) {
            if (isNullPrefixRow) {
                Utf8Sequence v = readRawCoveredVarchar(includeIdx, next, varcharViewA);
                if (v != null) {
                    return v;
                }
            }
            return super.getCoveredVarcharA(includeIdx);
        }

        @Override
        public Utf8Sequence getCoveredVarcharB(int includeIdx) {
            if (isNullPrefixRow) {
                Utf8Sequence v = readRawCoveredVarchar(includeIdx, next, varcharViewB);
                if (v != null) {
                    return v;
                }
            }
            return super.getCoveredVarcharB(includeIdx);
        }

        @Override
        public boolean hasNext() {
            if (nullPos < nullCount) {
                next = nullPos++;
                isNullPrefixRow = true;
                return true;
            }
            isNullPrefixRow = false;
            return super.hasNext();
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

        /**
         * Unmaps every null-prefix raw-column region this cursor currently owns and
         * drops the arrays. Shared by closeCoveringResources() and by
         * ensureNullPrefixColumnsOpen()'s failure path, which must not leave a
         * half-mapped set behind.
         */
        private void closeNullPrefixColumns() {
            if (npColAddrs != null) {
                for (int i = 0; i < npColAddrs.length; i++) {
                    if (npColAddrs[i] != 0) {
                        ff.munmap(npColAddrs[i], npColSizes[i], MemoryTag.MMAP_INDEX_READER);
                        npColAddrs[i] = 0;
                    }
                    if (npAuxAddrs[i] != 0) {
                        ff.munmap(npAuxAddrs[i], npAuxSizes[i], MemoryTag.MMAP_INDEX_READER);
                        npAuxAddrs[i] = 0;
                    }
                }
            }
            npAuxAddrs = null;
            npAuxSizes = null;
            npColAddrs = null;
            npColSizes = null;
            npColTops = null;
        }

        /**
         * Lazily opens every INCLUDEd column's own raw .d (and .i, for var-size
         * types) file for this partition, read-only, mmapped whole. Runs at most
         * once per checkout (see closeCoveringResources(), which resets
         * npColumnsOpened on every close -- pooled or final -- so a reused cursor
         * always re-resolves against whatever partition the outer reader is
         * currently bound to). Mirrors PostingIndexWriter#mapCoveredColumn /
         * #mapColumnFile, which build the sidecar from these same files.
         *
         * @return true iff at least the per-column top/address arrays were
         * populated (individual columns can still be unavailable -- dropped,
         * or genuinely missing their .d file -- tracked by a 0 address).
         */
        private boolean ensureNullPrefixColumnsOpen() {
            if (npColumnsOpened) {
                return npColAddrs != null;
            }
            npColumnsOpened = true;
            if (coverCount <= 0 || metadata == null || columnVersionReader == null) {
                return false;
            }
            // Map straight into the persistent fields rather than into locals the
            // loop publishes only on success: openRawColumnFile throws when mmap
            // fails, and a var-size column alone issues two mmaps (.d then .i), so
            // buffering in locals orphans every region the loop already mapped --
            // closeCoveringResources() would see a null npColAddrs and free none of
            // them. PostingIndexWriter#mapColumnFile targets its persistent array
            // for the same reason.
            npAuxAddrs = new long[coverCount];
            npAuxSizes = new long[coverCount];
            npColAddrs = new long[coverCount];
            npColSizes = new long[coverCount];
            npColTops = new long[coverCount];
            Path p = Path.getThreadLocal(sidecarBasePath);
            int pLen = p.size();
            try {
                for (int c = 0; c < coverCount; c++) {
                    if (!isCoverColumnRequired(c)) {
                        continue; // the query never reads this INCLUDE column
                    }
                    int writerIdx = sidecarColumnIndices.getQuick(c);
                    int colType = sidecarColumnTypes.getQuick(c);
                    if (writerIdx < 0 || colType < 0) {
                        continue; // column dropped, or absent from this chain entry
                    }
                    int denseIdx = denseIndexFromWriter(metadata, writerIdx);
                    if (denseIdx < 0) {
                        continue;
                    }
                    npColTops[c] = columnVersionReader.getColumnTop(partitionTimestamp, writerIdx);
                    CharSequence name = metadata.getColumnName(denseIdx);
                    long nameTxn = sidecarCovTs.getQuick(c);
                    p.trimTo(pLen);
                    openRawColumnFile(p, name, nameTxn, false, npColAddrs, npColSizes, c);
                    if (ColumnType.isVarSize(colType)) {
                        p.trimTo(pLen);
                        openRawColumnFile(p, name, nameTxn, true, npAuxAddrs, npAuxSizes, c);
                    }
                }
            } catch (Throwable th) {
                // Release now, and keep the all-or-nothing contract this method
                // documents: npColumnsOpened stays true, so a retry on the same
                // cursor short-circuits to false and every covered read falls back
                // to the sidecar, exactly as it did before any column was mapped.
                closeNullPrefixColumns();
                throw th;
            } finally {
                p.trimTo(pLen);
            }
            return true;
        }

        /**
         * True when this checkout's caller declared cover column {@code c} among the
         * ones it reads. A null subset means the caller declared nothing, so every
         * column stays eligible.
         */
        private boolean isCoverColumnRequired(int c) {
            final int[] required = npRequiredColumns;
            if (required == null) {
                return true;
            }
            for (int r : required) {
                if (r == c) {
                    return true;
                }
            }
            return false;
        }

        private Utf8Sequence readRawCoveredVarchar(int includeIdx, long row, DirectUtf8String view) {
            if (!ensureNullPrefixColumnsOpen() || includeIdx < 0 || npColTops == null || includeIdx >= npColTops.length) {
                return null;
            }
            long fileRow = row - npColTops[includeIdx];
            if (fileRow < 0) {
                return null;
            }
            long auxAddr = resolveRawAuxAddr(includeIdx, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES * fileRow, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
            if (auxAddr == 0) {
                return null;
            }
            int header = Unsafe.getInt(auxAddr);
            if ((header & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
                return null;
            }
            if ((header & 1) != 0) {
                // inlined: the payload starts 1 byte into the aux entry
                // (VarcharTypeDriver.FULLY_INLINED_STRING_OFFSET). The size field
                // is 4 bits wide, so the mask admits 15, but the writer inlines at
                // most VARCHAR_MAX_BYTES_FULLY_INLINED (9) bytes.
                int size = (header >>> 4) & 0xF;
                return view.of(auxAddr + 1, auxAddr + 1 + size);
            }
            int size = (header >>> 4) & 0x0FFFFFFF;
            if (size <= 0) {
                return view.of(auxAddr, auxAddr);
            }
            long dataOffset = Unsafe.getLong(auxAddr + 8) >>> 16;
            long dataAddr = resolveRawDataAddr(includeIdx, dataOffset, size);
            if (dataAddr == 0) {
                return null;
            }
            return view.of(dataAddr, dataAddr + size);
        }

        private void openRawColumnFile(Path p, CharSequence name, long nameTxn, boolean isAux, long[] addrs, long[] sizes, int idx) {
            LPSZ fileName = isAux ? TableUtils.iFile(p, name, nameTxn) : TableUtils.dFile(p, name, nameTxn);
            long fd = ff.openRO(fileName);
            if (fd < 0) {
                return; // no data below the indexed column's top for this column -- normal
            }
            try {
                long fileSize = ff.length(fd);
                if (fileSize > 0) {
                    long mapped = ff.mmap(fd, fileSize, 0, Files.MAP_RO, MemoryTag.MMAP_INDEX_READER);
                    if (mapped == FilesFacade.MAP_FAILED) {
                        throw CairoException.critical(ff.errno())
                                .put("could not mmap covering INCLUDE column for null-prefix read [file=").put(fileName)
                                .put(", size=").put(fileSize).put(']');
                    }
                    addrs[idx] = mapped;
                    sizes[idx] = fileSize;
                }
            } finally {
                ff.close(fd);
            }
        }

        private ArrayView readRawCoveredArray(int includeIdx, long row, int columnType, BorrowedArray view) {
            if (!ensureNullPrefixColumnsOpen() || includeIdx < 0 || npColTops == null || includeIdx >= npColTops.length) {
                return null;
            }
            long fileRow = row - npColTops[includeIdx];
            if (fileRow < 0) {
                return null;
            }
            long auxAddr = resolveRawAuxAddr(includeIdx, ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES * fileRow, ArrayTypeDriver.ARRAY_AUX_WIDTH_BYTES);
            if (auxAddr == 0) {
                return null;
            }
            int size = Unsafe.getInt(auxAddr + Long.BYTES);
            if (size <= 0) {
                view.ofNull();
                return view;
            }
            long dataOffset = Unsafe.getLong(auxAddr) & ArrayTypeDriver.OFFSET_MAX;
            long dataAddr = resolveRawDataAddr(includeIdx, dataOffset, size);
            if (dataAddr == 0) {
                return null;
            }
            int dims = ColumnType.decodeArrayDimensionality(columnType);
            short elemType = ColumnType.decodeArrayElementType(columnType);
            int elemSize = ColumnType.sizeOf(elemType);
            int cardinality = 1;
            for (int d = 0; d < dims; d++) {
                cardinality *= Unsafe.getInt(dataAddr + (long) d * Integer.BYTES);
            }
            int valueSize = cardinality * elemSize;
            long valuePtr = dataAddr + size - valueSize;
            return view.of(columnType, dataAddr, valuePtr, valueSize);
        }

        private BinarySequence readRawCoveredBin(int includeIdx, long row, DirectBinarySequence view) {
            if (!ensureNullPrefixColumnsOpen() || includeIdx < 0 || npColTops == null || includeIdx >= npColTops.length) {
                return null;
            }
            long fileRow = row - npColTops[includeIdx];
            if (fileRow < 0) {
                return null;
            }
            long auxAddr = resolveRawAuxAddr(includeIdx, fileRow << 3, Long.BYTES);
            if (auxAddr == 0) {
                return null;
            }
            long dataOffset = Unsafe.getLong(auxAddr);
            long lenAddr = resolveRawDataAddr(includeIdx, dataOffset, Long.BYTES);
            if (lenAddr == 0) {
                return null;
            }
            long len = Unsafe.getLong(lenAddr);
            if (len < 0) {
                return null;
            }
            long dataAddr = resolveRawDataAddr(includeIdx, dataOffset, Long.BYTES + len);
            if (dataAddr == 0) {
                return null;
            }
            return view.of(dataAddr + Long.BYTES, len);
        }

        private CharSequence readRawCoveredStr(int includeIdx, long row, DirectString view) {
            if (!ensureNullPrefixColumnsOpen() || includeIdx < 0 || npColTops == null || includeIdx >= npColTops.length) {
                return null;
            }
            long fileRow = row - npColTops[includeIdx];
            if (fileRow < 0) {
                return null;
            }
            long auxAddr = resolveRawAuxAddr(includeIdx, fileRow << 3, Long.BYTES);
            if (auxAddr == 0) {
                return null;
            }
            long dataOffset = Unsafe.getLong(auxAddr);
            long lenAddr = resolveRawDataAddr(includeIdx, dataOffset, Integer.BYTES);
            if (lenAddr == 0) {
                return null;
            }
            int len = Unsafe.getInt(lenAddr);
            if (len < 0) {
                return null;
            }
            long dataAddr = resolveRawDataAddr(includeIdx, dataOffset, (long) Integer.BYTES + (long) len * Character.BYTES);
            if (dataAddr == 0) {
                return null;
            }
            return view.of(dataAddr + Integer.BYTES, len);
        }

        @Override
        protected void closeCoveringResources() {
            super.closeCoveringResources();
            closeNullPrefixColumns();
            npColumnsOpened = false;
            npRequiredColumns = null;
        }

        private long resolveRawAuxAddr(int includeIdx, long offset, long needed) {
            if (includeIdx < 0 || npAuxAddrs == null || includeIdx >= npAuxAddrs.length) {
                return 0;
            }
            long addr = npAuxAddrs[includeIdx];
            if (addr == 0 || offset < 0 || offset + needed > npAuxSizes[includeIdx]) {
                return 0;
            }
            return addr + offset;
        }

        private long resolveRawDataAddr(int includeIdx, long offset, long needed) {
            if (includeIdx < 0 || npColAddrs == null || includeIdx >= npColAddrs.length) {
                return 0;
            }
            long addr = npColAddrs[includeIdx];
            if (addr == 0 || offset < 0 || offset + needed > npColSizes[includeIdx]) {
                return 0;
            }
            return addr + offset;
        }

        private long resolveRawFixedAddr(int includeIdx, long row, int size) {
            if (!isNullPrefixRow || !ensureNullPrefixColumnsOpen()
                    || includeIdx < 0 || npColTops == null || includeIdx >= npColTops.length) {
                return 0;
            }
            long fileRow = row - npColTops[includeIdx];
            if (fileRow < 0) {
                return 0; // genuinely null (below the INCLUDEd column's own top) -- caller falls back
            }
            return resolveRawDataAddr(includeIdx, fileRow * size, size);
        }
    }
}
