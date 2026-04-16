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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Backward reader for Delta + FoR64 BitPacking bitmap index.
 * <p>
 * Iterates generations in reverse, blocks within each generation in reverse,
 * and values within each block in reverse — producing values in descending order.
 * Uses PostingGenLookup for tiered gen-to-key mapping with cached metadata.
 */
public class PostingIndexBwdReader extends AbstractPostingIndexReader {
    private static final int MIN_BUFFER_CAPACITY = 4;
    private final ObjList<Cursor> freeCursors = new ObjList<>();

    public PostingIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, null);
    }

    public PostingIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            io.questdb.cairo.sql.RecordMetadata metadata
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, metadata);
    }

    @Override
    public void close() {
        super.close();
        for (int i = 0, n = freeCursors.size(); i < n; i++) {
            freeCursors.getQuick(i).releaseResources();
        }
        Misc.clear(freeCursors);
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
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
        private long blockBufferAddr = 0;
        private int blockBufferCapacity = 0;
        private int blockBufferPos;
        private int constantDeltaRemaining;
        private long constantDeltaStep;
        private long constantDeltaValue;
        private int currentBlock;
        private int currentGen;
        private long cursorReloadGeneration;
        private long efHighOffset;
        private int efHighWordIdx;
        private int efL;
        private long efLowMask;
        private long efLowOffset;
        private long efRankDirAddr;
        private int efRankDirCapacity;
        private int encodedBlockCount;
        private long flatBaseValue;
        private int flatBitWidth;
        private long flatDataOffset;
        private int flatRemaining;
        private int flatStartIdx;
        private boolean isEFMode;
        private boolean isFlatMode;
        private boolean isPooled;
        private int lookupEnd;
        private int lookupPos;
        private long maxValue;
        private int minBlock;
        private long minValue;
        private long next;
        private long packedDataStartOffset;
        // File offsets into mapped value memory — resolved via baseAddr per hot func
        private long srcBitWidthsOffset;
        private long srcFirstValuesOffset;
        private long srcMinDeltasOffset;
        private long srcPackedOffsetsOffset;
        private long srcValueCountsOffset;

        @Override
        public void close() {
            // On pool return, keep only blockBuffer (~2 KB upper bound, size
            // fixed by the block encoding format) and release every buffer
            // whose size scales with data — covering caches (alp/float/
            // decodeWorkspace/fsstDecompBuf) and the EF rank directory.
            // Retaining data-scaled buffers would pin memory proportional to
            // historical peak, multiplied across pool slots × partitions ×
            // concurrent table readers.
            if (!isPooled && freeCursors.size() < MAX_CACHED_FREE_CURSORS) {
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
                while (constantDeltaRemaining > 0) {
                    long value = constantDeltaValue;
                    constantDeltaValue += constantDeltaStep;
                    constantDeltaRemaining--;
                    if (value < minValue) {
                        constantDeltaRemaining = 0;
                        return false;
                    }
                    if (value <= maxValue) {
                        next = value;
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

                // Serve from block buffer in reverse
                while (blockBufferPos >= 0) {
                    long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                    if (value < minValue) {
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
            // For backward cursor, the first yielded row is the last row
            if (hasNext()) {
                return next();
            }
            return -1;
        }

        private boolean advanceToPrevRelevantGen() {
            int lookupTier = genLookup.getTier();

            if (lookupTier == PostingGenLookup.TIER_PER_KEY) {
                return !advanceWithPerKeyLookupReverse();
            } else if (lookupTier == PostingGenLookup.TIER_SBBF) {
                return !advanceWithSbbfLookupReverse();
            } else {
                return !advanceWithLinearScanReverse();
            }
        }

        private boolean advanceWithLinearScanReverse() {
            currentGen--;
            while (currentGen >= 0) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen--;
                    continue;
                }

                loadSparseGenByPrefixSum(currentGen);
                if (encodedBlockCount > 0 || isFlatMode || isEFMode) {
                    return true;
                }
                currentGen--;
            }
            return false;
        }

        private boolean advanceWithPerKeyLookupReverse() {
            if (lookupPos >= lookupEnd) {
                int nextSparseGen = genLookup.getGenIndex(lookupPos);

                currentGen--;
                while (currentGen > nextSparseGen) {
                    if (genLookup.getGenKeyCount(currentGen) >= 0) {
                        loadDenseGenerationCached(currentGen);
                        return true;
                    }
                    currentGen--;
                }

                int posInGen = genLookup.getPosInGen(lookupPos);
                lookupPos--;
                currentGen = nextSparseGen;
                loadSparseGenDirect(currentGen, posInGen);
                return true;
            }

            currentGen--;
            while (currentGen >= 0) {
                if (genLookup.getGenKeyCount(currentGen) >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return true;
                }
                currentGen--;
            }
            return false;
        }

        private boolean advanceWithSbbfLookupReverse() {
            currentGen--;
            while (currentGen >= 0) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen--;
                    continue;
                }

                if (genLookup.mightNotContainKey(currentGen, requestedKey)) {
                    currentGen--;
                    continue;
                }

                loadSparseGenByPrefixSum(currentGen);
                if (encodedBlockCount > 0 || isFlatMode || isEFMode) {
                    return true;
                }
                currentGen--;
            }
            return false;
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
        }

        private void decodeBlock(int b) {
            long baseAddr = valueMem.addressOf(0);
            int count = Unsafe.getUnsafe().getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
            int bitWidth = Unsafe.getUnsafe().getByte(baseAddr + srcBitWidthsOffset + b) & 0xFF;
            int numDeltas = count - 1;

            long firstValue = Unsafe.getUnsafe().getLong(baseAddr + srcFirstValuesOffset + (long) b * Long.BYTES);

            if (bitWidth == 0) {
                long minD = numDeltas > 0
                        ? Unsafe.getUnsafe().getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES)
                        : 0;
                constantDeltaValue = firstValue + (long) numDeltas * minD;
                constantDeltaStep = -minD;
                constantDeltaRemaining = count;
                blockBufferPos = -1;
                return;
            } else {
                // Variable-delta: decode to buffer
                ensureBuffer(count);
                Unsafe.getUnsafe().putLong(blockBufferAddr, firstValue);
                if (numDeltas > 0) {
                    long minD = Unsafe.getUnsafe().getLong(baseAddr + srcMinDeltasOffset + (long) b * Long.BYTES);
                    long blockPackedAddr = srcPackedOffsetsOffset != 0
                            ? baseAddr + packedDataStartOffset + Unsafe.getUnsafe().getInt(baseAddr + srcPackedOffsetsOffset + (long) b * Integer.BYTES)
                            : baseAddr + packedDataStartOffset;
                    long scratchAddr = blockBufferAddr + Long.BYTES;
                    BitpackUtils.unpackAllValues(blockPackedAddr, numDeltas, bitWidth, minD, scratchAddr);
                    long cumulative = firstValue;
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getUnsafe().getLong(scratchAddr + (long) i * Long.BYTES);
                        Unsafe.getUnsafe().putLong(scratchAddr + (long) i * Long.BYTES, cumulative);
                    }
                }
            }
            blockBufferPos = count - 1;
        }

        private void decodeNextEFChunkReverse() {
            ensureBuffer(PostingIndexUtils.PACKED_BATCH_SIZE);
            long baseAddr = valueMem.addressOf(0);
            while (efHighWordIdx >= 0) {
                long word = Unsafe.getUnsafe().getLong(baseAddr + efHighOffset + (long) efHighWordIdx * 8);
                if (word == 0) {
                    efHighWordIdx--;
                    continue;
                }
                int rankBefore = Unsafe.getUnsafe().getInt(efRankDirAddr + (long) efHighWordIdx * Integer.BYTES);
                int bufIdx = 0;
                long w = word;
                while (w != 0) {
                    int trail = Long.numberOfTrailingZeros(w);
                    int globalIdx = rankBefore + bufIdx;
                    long highValue = (long) efHighWordIdx * 64 + trail - globalIdx;
                    long low = PostingIndexUtils.readBitsWord(baseAddr + efLowOffset, (long) globalIdx * efL, efL) & efLowMask;
                    Unsafe.getUnsafe().putLong(blockBufferAddr + (long) bufIdx * Long.BYTES, (highValue << efL) | low);
                    bufIdx++;
                    w &= w - 1;
                }
                blockBufferPos = bufIdx - 1;
                efHighWordIdx--;
                return;
            }
            blockBufferPos = -1;
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
            if (cursorReloadGeneration != reloadGeneration) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.constantDeltaRemaining = 0;
                this.isEFMode = false;
                this.isFlatMode = false;
                this.flatRemaining = 0;
                return;
            }
            this.isCurrentGenDense = true;
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
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
            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
            long strideFileOffset = genFileOffset + siSize + strideOff;
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_FLAT) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_FLAT_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_FLAT_PREFIX_COUNTS_OFFSET;
                int startCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int endCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES);
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

                // Binary search to trim values outside [minValue, maxValue].
                int effectiveStart = startCount;
                int effectiveEnd = endCount;
                if (minValue > 0 && bitWidth > 0 && count > 1) {
                    int lo = startCount, hi = endCount - 1;
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
                    int lo = effectiveStart, hi = effectiveEnd - 1;
                    while (lo < hi) {
                        int mid = (lo + hi + 1) >>> 1;
                        long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                        if (val > maxValue) {
                            hi = mid - 1;
                        } else {
                            lo = mid;
                        }
                    }
                    effectiveEnd = lo + 1;
                }
                int effectiveCount = effectiveEnd - effectiveStart;

                if (effectiveCount <= 0) {
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
                return;
            }

            // Delta mode
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            this.sidecarStrideKeyStart = 0;
            if (coverCount > 0) {
                int deltaKeyStartCount = 0;
                for (int k = 0; k < localKey; k++) {
                    deltaKeyStartCount += Unsafe.getUnsafe().getInt(countsAddr + (long) k * Integer.BYTES);
                }
                this.denseVarKeyStartCount = deltaKeyStartCount;
            }
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
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
            if (cursorReloadGeneration != reloadGeneration) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.constantDeltaRemaining = 0;
                this.isEFMode = false;
                this.isFlatMode = false;
                this.flatRemaining = 0;
                return;
            }
            this.isCurrentGenDense = false;
            computePerColumnSidecarOffsets(gen);
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            // Use stored prefix-sum for O(1) key lookup
            int minKey = genLookup.getGenMinKey(gen);
            int maxKey = genLookup.getGenMaxKey(gen);
            if (requestedKey < minKey || requestedKey > maxKey) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.isFlatMode = false;
                return;
            }

            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen));
            int k = requestedKey - minKey;
            int start = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start == end) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.isFlatMode = false;
                return;
            }

            this.isFlatMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) start * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) start * Integer.BYTES);
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
                    sidecarBase += Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
                }
                this.sidecarOrdinal = sidecarBase + totalValueCount;
            } else {
                this.sidecarOrdinal = totalValueCount;
            }

            readDeltaBlockMetadata(encodedOffset, totalValueCount);
        }

        private void loadSparseGenDirect(int gen, int idx) {
            if (cursorReloadGeneration != reloadGeneration) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.constantDeltaRemaining = 0;
                this.isEFMode = false;
                this.isFlatMode = false;
                this.flatRemaining = 0;
                return;
            }
            this.isCurrentGenDense = false;
            computePerColumnSidecarOffsets(gen);
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.isFlatMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
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
                    sidecarBase += Unsafe.getUnsafe().getInt(countsBase + (long) i * Integer.BYTES);
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
            int firstWord = Unsafe.getUnsafe().getInt(baseAddr + pos);
            if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
                pos += 4;
                int efTotalCount = Unsafe.getUnsafe().getInt(baseAddr + pos);
                pos += 4;
                efL = Unsafe.getUnsafe().getByte(baseAddr + pos) & 0xFF;
                pos += 1;
                long u = Unsafe.getUnsafe().getLong(baseAddr + pos);
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
                    Unsafe.getUnsafe().putInt(efRankDirAddr + (long) w * Integer.BYTES, cumulative);
                    cumulative += Long.bitCount(Unsafe.getUnsafe().getLong(baseAddr + efHighOffset + (long) w * 8));
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

            // File offsets into mapped value memory — resolved lazily per hot func
            srcValueCountsOffset = pos;
            pos += firstWord;

            srcFirstValuesOffset = pos;
            pos += (long) firstWord * Long.BYTES;

            srcMinDeltasOffset = pos;
            pos += (long) firstWord * Long.BYTES;

            srcBitWidthsOffset = pos;
            pos += firstWord;

            // packedOffsets only present for multi-block keys
            if (firstWord > 1) {
                srcPackedOffsetsOffset = pos;
                pos += (long) firstWord * Integer.BYTES;
            } else {
                srcPackedOffsetsOffset = 0;
            }

            packedDataStartOffset = pos;

            // Trim trailing blocks (highest values) above maxValue.
            int endBlock = firstWord;
            if (maxValue < Long.MAX_VALUE && firstWord > 1) {
                int lo = 0, hi = firstWord - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getUnsafe().getLong(baseAddr + srcFirstValuesOffset + (long) mid * Long.BYTES) <= maxValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                endBlock = lo + 1;
            }

            // Trim leading blocks (lowest values) below minValue.
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

            // Adjust sidecar ordinal for trimmed trailing blocks — those values
            // are never iterated, so the ordinal must not count them.
            if (endBlock < firstWord && coverCount > 0) {
                int trailingTrimmedCount = 0;
                for (int b = endBlock; b < firstWord; b++) {
                    trailingTrimmedCount += Unsafe.getUnsafe().getByte(baseAddr + srcValueCountsOffset + b) & 0xFF;
                }
                this.sidecarOrdinal -= trailingTrimmedCount;
            }

            this.encodedBlockCount = endBlock;
            this.currentBlock = endBlock - 1;
            this.minBlock = startBlock;
            this.blockBufferPos = -1;
        }

        private void releaseResources() {
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

        void of(int key, long minValue, long maxValue) {
            this.cursorReloadGeneration = reloadGeneration;
            clearBlockState();
            resetCoveringState();

            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                currentGen = -1;
                return;
            }

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;

            // Fast path: sealed single-generation index with dense gen 0.
            // Load directly without tier lookup or advance machinery.
            // Set currentGen = -1 so advanceToPrevRelevantGen() (called
            // when blocks are exhausted) returns false immediately.
            if (genCount == 1 && genLookup.getGenKeyCount(0) >= 0) {
                this.currentGen = -1;
                this.lookupPos = -1;
                this.lookupEnd = 0;
                loadDenseGenerationCached(0);
                return;
            }

            ensureGenLookup();

            this.currentGen = genCount; // will be decremented

            // Set up inverted index range for this key (Tier 1), reverse order
            if (genLookup.isPerKeyMode() && key < genLookup.getKeyCount()) {
                this.lookupPos = genLookup.getEntryEnd(key) - 1;
                this.lookupEnd = genLookup.getEntryStart(key);
            } else {
                this.lookupPos = -1;
                this.lookupEnd = 0;
            }

            if (advanceToPrevRelevantGen()) {
                currentGen = -1;
                encodedBlockCount = 0;
                currentBlock = -1;
                blockBufferPos = -1;
            }
        }
    }
}
