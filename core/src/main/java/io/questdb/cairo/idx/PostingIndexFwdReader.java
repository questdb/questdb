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
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

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
        reloadConditionally();

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            NullCursor nc;
            if (freeNullCursors.size() > 0) {
                nc = freeNullCursors.popLast();
                nc.isPooled = false;
            } else {
                nc = new NullCursor();
            }
            nc.of(key, minValue, maxValue);
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
            c.of(key, minValue, maxValue);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor extends AbstractCoveringCursor {
        private final LongList builderEntries = new LongList();
        protected long maxValue;
        protected long minValue;
        protected long next;
        boolean isPooled;
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
            if (!isPooled && freeCursors.size() < MAX_CACHED_FREE_CURSORS) {
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
                        this.next = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
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
                        long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
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
            if (!isCacheReplayMode && requestedKey >= 0) {
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
            int count = Unsafe.getUnsafe().getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
            int bitWidth = Unsafe.getUnsafe().getByte(baseAddr + srcBitWidthsOffset + b) & 0xFF;
            int numDeltas = count - 1;

            long firstValue = Unsafe.getUnsafe().getLong(baseAddr + srcFirstValuesOffset + (long) b * Long.BYTES);
            currentBlock++;

            if (bitWidth == 0) {
                long minD = numDeltas > 0
                        ? Unsafe.getUnsafe().getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES)
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
                Unsafe.getUnsafe().putLong(blockBufferAddr, firstValue);
                if (numDeltas > 0) {
                    long minD = Unsafe.getUnsafe().getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES);
                    long scratchAddr = blockBufferAddr + Long.BYTES;
                    BitpackUtils.unpackAllValues(baseAddr + packedDataOffset, numDeltas, bitWidth, minD, scratchAddr);
                    packedDataOffset += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    long cumulative = firstValue;
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getUnsafe().getLong(scratchAddr + (long) i * Long.BYTES);
                        Unsafe.getUnsafe().putLong(scratchAddr + (long) i * Long.BYTES, cumulative);
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
                long word = Unsafe.getUnsafe().getLong(baseAddr + efHighOffset + (long) efHighWordIdx * 8);
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
                        long lowWord = Unsafe.getUnsafe().getLong(lowWordAddr);
                        low = (lowWord >>> lowBitOffset) & efLowMask;
                        if (lowBitOffset + efL > 64) {
                            // Spans two words — merge bits from next word
                            low |= (Unsafe.getUnsafe().getLong(lowWordAddr + 8) << (64 - lowBitOffset)) & efLowMask;
                        }
                        lowBitOffset += efL;
                        if (lowBitOffset >= 64) {
                            lowWordAddr += 8;
                            lowBitOffset -= 64;
                        }
                    }

                    Unsafe.getUnsafe().putLong(
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

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.isFlatMode = false;
            this.sealedGenKeyCount = genKeyCount;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            cacheSidecarKeyAddrs(stride, localKey);
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            long strideOff = Unsafe.getUnsafe().getLong(genAddr + (long) stride * Long.BYTES);
            long nextStrideOff = Unsafe.getUnsafe().getLong(genAddr + (long) (stride + 1) * Long.BYTES);
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
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);
            assert mode == PostingIndexUtils.STRIDE_MODE_FLAT || mode == PostingIndexUtils.STRIDE_MODE_DELTA;

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int startCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int endCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
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
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            this.sidecarStrideKeyStart = 0;
            this.sidecarOrdinal = 0;
            if (coverCount > 0) {
                int deltaKeyStartCount = 0;
                for (int k = 0; k < localKey; k++) {
                    deltaKeyStartCount += Unsafe.getUnsafe().getInt(countsAddr + (long) k * Integer.BYTES);
                }
                this.denseVarKeyStartCount = deltaKeyStartCount;
            }
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            long dataOffset = Unsafe.getUnsafe().getLong(offsetsBase + (long) localKey * Long.BYTES);
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

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
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
            int start = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
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
                    sidecarBase += Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase;
            } else {
                this.sidecarOrdinal = 0;
            }

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) start * Integer.BYTES);
            long dataOffset = Unsafe.getUnsafe().getLong(offsetsBase + (long) start * Long.BYTES);
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

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.isFlatMode = false;

            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

            if (coverCount > 0) {
                int sidecarBase = 0;
                for (int i = 0; i < idx; i++) {
                    sidecarBase += Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase;
            } else {
                this.sidecarOrdinal = 0;
            }

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            long dataOffset = Unsafe.getUnsafe().getLong(offsetsBase + (long) idx * Long.BYTES);
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
            int firstWord = Unsafe.getUnsafe().getInt(baseAddr + pos);
            if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
                pos += 4;
                efTotalCount = Unsafe.getUnsafe().getInt(baseAddr + pos);
                pos += 4;
                efL = Unsafe.getUnsafe().getByte(baseAddr + pos) & 0xFF;
                pos += 1;
                long u = Unsafe.getUnsafe().getLong(baseAddr + pos);
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
                    if (Unsafe.getUnsafe().getLong(baseAddr + srcFirstValuesOffset + (long) mid * Long.BYTES) <= minValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                startBlock = lo;
            }

            int skippedValueCount = 0;
            for (int b = 0; b < startBlock; b++) {
                skippedValueCount += Unsafe.getUnsafe().getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
            }
            if (startBlock > 0) {
                packedDataStartOffset += Unsafe.getUnsafe().getLong(baseAddr + srcPackedOffsetsOffset + (long) startBlock * Long.BYTES);
            }
            this.sidecarStrideKeyStart += skippedValueCount;
            this.denseVarKeyStartCount += skippedValueCount;
            if (!isCurrentGenDense && coverCount > 0) {
                this.sidecarOrdinal += skippedValueCount;
            }

            int endBlock = firstWord;
            if (maxValue < Long.MAX_VALUE && firstWord > 0) {
                for (int b = startBlock; b < firstWord; b++) {
                    if (Unsafe.getUnsafe().getLong(baseAddr + srcFirstValuesOffset + (long) b * Long.BYTES) > maxValue) {
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

        @Override
        public void close() {
            if (!isPooled && freeNullCursors.size() < MAX_CACHED_FREE_CURSORS) {
                isPooled = true;
                closeCoveringResources();
                resetCoveringState();
                freeNullCursors.add(this);
                return;
            }
            releaseResources();
        }

        @Override
        public boolean hasNext() {
            if (nullPos < nullCount) {
                next = nullPos++;
                return true;
            }
            return super.hasNext();
        }

        @Override
        public long size() {
            long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            long nullLimit = Math.min(columnTop, hi);
            long nulls = Math.max(0L, nullLimit - minValue);
            return super.size() + nulls;
        }
    }
}
