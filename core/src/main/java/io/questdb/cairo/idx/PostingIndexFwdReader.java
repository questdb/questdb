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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Forward reader for Delta + FoR64 BitPacking bitmap index.
 * <p>
 * Block-buffered decode: unpacks 64 values at a time from FoR64 blocks.
 * Generation iteration uses PostingGenLookup for tiered gen-to-key mapping.
 */
public class PostingIndexFwdReader extends AbstractPostingIndexReader {
    private final Cursor cursor = new Cursor();
    private final ObjList<Cursor> extraCursors = new ObjList<>();
    private int extraCursorIdx;

    public PostingIndexFwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop);
    }

    @Override
    public void close() {
        cursor.close();
        for (int i = 0, n = extraCursors.size(); i < n; i++) {
            extraCursors.getQuick(i).close();
        }
        extraCursors.clear();
        extraCursorIdx = 0;
        super.close();
    }

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            final Cursor c;
            if (cachedInstance) {
                c = cursor;
                shrinkExtraCursors();
            } else {
                c = extraCursor();
            }
            c.of(key, minValue, maxValue);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private Cursor extraCursor() {
        if (extraCursorIdx < extraCursors.size()) {
            return extraCursors.getQuick(extraCursorIdx++);
        }
        Cursor c = new Cursor();
        extraCursors.add(c);
        extraCursorIdx++;
        return c;
    }

    private void shrinkExtraCursors() {
        // Close cursors beyond what the previous round used, then reset the index.
        for (int i = extraCursors.size() - 1; i >= extraCursorIdx; i--) {
            extraCursors.getQuick(i).close();
            extraCursors.setQuick(i, null);
        }
        extraCursors.setPos(extraCursorIdx);
        extraCursorIdx = 0;
    }

    private class Cursor extends AbstractCoveringCursor {
        private long blockBufferAddr = Unsafe.malloc((long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private int blockBufferCapacity = PostingIndexUtils.PACKED_BATCH_SIZE;
        private int blockBufferEnd;
        private int blockBufferPos;
        private int constantDeltaRemaining;
        private long constantDeltaStep;
        private long constantDeltaValue;
        private int currentBlock;
        private int currentGen;
        // Elias-Fano streaming state
        private int efHighWordIdx;
        private long efHighStart;
        private int efL;
        private long efLowMask;
        private long efLowStart;
        private int efNumHighWords;
        private int efOutputCount;
        private int efTotalCount;
        private long encodedAddr;
        private int encodedBlockCount;
        private long flatBaseValue;
        private int flatBitWidth;
        private long flatDataBase;
        private int flatRemaining;
        private int flatStartIdx;
        private boolean isEFMode;
        private boolean isFlatMode;
        private int lookupEnd;
        private int lookupPos;
        private long maxValue;
        private long minValue;
        private long next;
        private long packedDataAddr;
        private long srcBitWidthsAddr;
        private long srcFirstValuesAddr;
        private long srcMinDeltasAddr;
        private long srcValueCountsAddr;
        private int totalValueCount;

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from constant-delta stream (bitWidth=0 block)
                while (constantDeltaRemaining > 0) {
                    long value = constantDeltaValue;
                    constantDeltaValue += constantDeltaStep;
                    constantDeltaRemaining--;
                    if (value > maxValue) {
                        constantDeltaRemaining = 0;
                        return false;
                    }
                    if (value >= minValue) {
                        next = value;
                        sidecarOrdinal++;
                        cachedSidecarIdx = sidecarValueIdx();
                        return true;
                    }
                    if (coverCount > 0) sidecarOrdinal++;
                }

                // Serve from block buffer
                while (blockBufferPos < blockBufferEnd) {
                    long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                    blockBufferPos++;
                    if (value > maxValue) {
                        return false;
                    }
                    if (value >= minValue) {
                        this.next = value;
                        if (coverCount > 0) {
                            sidecarOrdinal++;
                            cachedSidecarIdx = sidecarValueIdx();
                        }
                        return true;
                    }
                    if (coverCount > 0) sidecarOrdinal++;
                }

                // Decode next block in current generation
                if (currentBlock < encodedBlockCount) {
                    decodeNextBlock();
                    continue;
                }

                // EF mode: decode next chunk (one 64-bit word of high bits)
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

        void close() {
            if (blockBufferAddr != 0) {
                Unsafe.free(blockBufferAddr, (long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferAddr = 0;
                blockBufferCapacity = 0;
            }
            closeCoveringResources();
        }

        void of(int key, long minValue, long maxValue) {
            // Re-allocate fixed buffers if freed by close()
            if (blockBufferAddr == 0) {
                blockBufferCapacity = PostingIndexUtils.PACKED_BATCH_SIZE;
                blockBufferAddr = Unsafe.malloc((long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                totalValueCount = 0;
                currentGen = genCount;
                encodedBlockCount = 0;
                currentBlock = 0;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                constantDeltaRemaining = 0;
                isFlatMode = false;
                return;
            }

            ensureGenLookup();

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.currentGen = -1; // will be advanced by first advanceToNextRelevantGen()

            // Set up inverted index range for this key (Tier 1)
            if (genLookup.isPerKeyMode() && key < genLookup.getKeyCount()) {
                this.lookupPos = genLookup.getEntryStart(key);
                this.lookupEnd = genLookup.getEntryEnd(key);
            } else {
                this.lookupPos = 0;
                this.lookupEnd = 0;
            }

            // Kick off iteration
            this.encodedBlockCount = 0;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
            this.constantDeltaRemaining = 0;
            this.isFlatMode = false;
            resetCoveringState();
            advanceToNextRelevantGen();
        }

        private boolean advanceToNextRelevantGen() {
            int lookupTier = genLookup.getTier();

            if (lookupTier == PostingGenLookup.TIER_PER_KEY) {
                return advanceWithPerKeyLookup();
            } else if (lookupTier == PostingGenLookup.TIER_SBBF) {
                return advanceWithSbbfLookup();
            } else {
                return advanceWithBinarySearch();
            }
        }

        private boolean advanceWithBinarySearch() {
            currentGen++;
            while (currentGen < genCount) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                // Min/max bounds check from cached metadata
                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen++;
                    continue;
                }

                loadSparseGenWithBinarySearch(currentGen);
                if (totalValueCount > 0 || encodedBlockCount > 0 || isFlatMode || isEFMode) {
                    return true;
                }
                currentGen++;
            }
            return false;
        }

        private boolean advanceWithPerKeyLookup() {
            while (true) {
                if (lookupPos < lookupEnd) {
                    int nextSparseGen = genLookup.getGenIndex(lookupPos);

                    // Check dense gens before this sparse gen
                    currentGen++;
                    while (currentGen < nextSparseGen) {
                        if (genLookup.getGenKeyCount(currentGen) >= 0) {
                            loadDenseGenerationCached(currentGen);
                            return true;
                        }
                        currentGen++;
                    }

                    // Load sparse gen directly
                    int posInGen = genLookup.getPosInGen(lookupPos);
                    lookupPos++;
                    currentGen = nextSparseGen;
                    loadSparseGenDirect(currentGen, posInGen);
                    return true;
                }

                // No more sparse hits — check remaining dense gens
                currentGen++;
                while (currentGen < genCount) {
                    if (genLookup.getGenKeyCount(currentGen) >= 0) {
                        loadDenseGenerationCached(currentGen);
                        return true;
                    }
                    currentGen++;
                }
                return false;
            }
        }

        private boolean advanceWithSbbfLookup() {
            currentGen++;
            while (currentGen < genCount) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    // Dense gen
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                // Sparse gen: check min/max bounds first
                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen++;
                    continue;
                }

                // SBBF probe
                if (!genLookup.mightContainKey(currentGen, requestedKey)) {
                    currentGen++;
                    continue;
                }

                // SBBF says "maybe" — fall back to binary search within this gen
                loadSparseGenWithBinarySearch(currentGen);
                if (totalValueCount > 0 || encodedBlockCount > 0 || isFlatMode || isEFMode) {
                    return true;
                }
                // False positive — key not actually in this gen
                currentGen++;
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
        }

        private void decodeNextBlock() {
            int b = currentBlock;
            int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
            int bitWidth = Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF;
            int numDeltas = count - 1;

            long firstValue = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
            currentBlock++;

            if (bitWidth == 0) {
                long minD = numDeltas > 0
                        ? Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES)
                        : 0;
                constantDeltaValue = firstValue;
                constantDeltaStep = minD;
                constantDeltaRemaining = count;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                return;
            } else {
                // Variable-delta: decode to buffer
                Unsafe.getUnsafe().putLong(blockBufferAddr, firstValue);
                if (numDeltas > 0) {
                    long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
                    long scratchAddr = blockBufferAddr + Long.BYTES;
                    BitpackUtils.unpackAllValues(packedDataAddr, numDeltas, bitWidth, minD, scratchAddr);
                    packedDataAddr += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    long cumulative = firstValue;
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getUnsafe().getLong(scratchAddr + (long) i * Long.BYTES);
                        Unsafe.getUnsafe().putLong(scratchAddr + (long) i * Long.BYTES, cumulative);
                    }
                }
            }
            blockBufferPos = 0;
            blockBufferEnd = count;
        }

        /**
         * Decodes the next chunk of EF values by processing one 64-bit word of high bits.
         * Typically produces ~20-40 values per call (the popcount of the high-bits word).
         */
        private void decodeNextEFChunk() {
            int bufPos = 0;
            while (efHighWordIdx < efNumHighWords && efOutputCount < efTotalCount) {
                long word = Unsafe.getUnsafe().getLong(efHighStart + (long) efHighWordIdx * 8);
                if (word == 0) {
                    efHighWordIdx++;
                    continue;
                }
                while (word != 0 && efOutputCount < efTotalCount) {
                    int trail = Long.numberOfTrailingZeros(word);
                    long highValue = (long) efHighWordIdx * 64 + trail - efOutputCount;
                    long low = PostingIndexUtils.readBitsWord(efLowStart, (long) efOutputCount * efL, efL) & efLowMask;
                    Unsafe.getUnsafe().putLong(blockBufferAddr + (long) bufPos * Long.BYTES, (highValue << efL) | low);
                    bufPos++;
                    efOutputCount++;
                    word &= word - 1;
                }
                efHighWordIdx++;
                break; // one word per chunk — keeps buffer bounded
            }
            blockBufferPos = 0;
            blockBufferEnd = bufPos;
        }

        private void decodeNextFlatBatch() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            BitpackUtils.unpackValuesFrom(flatDataBase, flatStartIdx, batch, flatBitWidth, flatBaseValue, blockBufferAddr);
            flatStartIdx += batch;
            flatRemaining -= batch;
            blockBufferPos = 0;
            blockBufferEnd = batch;
        }

        private void loadDenseGenerationCached(int gen) {
            this.isCurrentGenDense = true;
            this.sidecarOrdinal = 0;
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
            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
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
                    clearBlockState();
                    return;
                }

                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                long dataAddr = strideAddr + flatHeaderSize;

                // Binary search to skip values below minValue.
                int effectiveStart = startCount;
                int effectiveCount = count;
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
                    effectiveCount = endCount - effectiveStart;
                }

                // Also trim values above maxValue from the end
                if (maxValue < Long.MAX_VALUE && effectiveCount > 1) {
                    int lo = effectiveStart, hi = effectiveStart + effectiveCount - 1;
                    while (lo < hi) {
                        int mid = (lo + hi + 1) >>> 1;
                        long val = BitpackUtils.unpackValue(dataAddr, mid, bitWidth, baseValue);
                        if (val > maxValue) {
                            hi = mid - 1;
                        } else {
                            lo = mid;
                        }
                    }
                    effectiveCount = lo - effectiveStart + 1;
                }

                if (effectiveCount <= 0) {
                    clearBlockState();
                    return;
                }

                this.isFlatMode = true;
                this.flatBitWidth = bitWidth;
                this.flatBaseValue = baseValue;
                this.flatDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = 0;
                this.sidecarStrideKeyStart = effectiveStart - startCount;
                this.denseVarKeyStartCount = startCount;
                this.sidecarOrdinal = 0;

                int batch = Math.min(effectiveCount, PostingIndexUtils.PACKED_BATCH_SIZE);
                BitpackUtils.unpackValuesFrom(dataAddr, effectiveStart, batch, bitWidth, baseValue, blockBufferAddr);
                this.blockBufferPos = 0;
                this.blockBufferEnd = batch;
                this.flatStartIdx = effectiveStart + batch;
                this.flatRemaining = effectiveCount - batch;
                return;
            }

            // Delta mode
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
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            this.encodedAddr = strideAddr + deltaHeaderSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void loadSparseGenDirect(int gen, int idx) {
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
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void loadSparseGenWithBinarySearch(int gen) {
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
                clearBlockState();
                totalValueCount = 0;
                this.isFlatMode = false;
                return;
            }

            long prefixSumAddr = valueMem.addressOf(genLookup.getGenPrefixSumOffset(gen));
            int k = requestedKey - minKey;
            int start = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) k * Integer.BYTES);
            int end = Unsafe.getUnsafe().getInt(prefixSumAddr + (long) (k + 1) * Integer.BYTES);
            if (start == end) {
                clearBlockState();
                totalValueCount = 0;
                this.isFlatMode = false;
                return;
            }
            int idx = start;

            this.isFlatMode = false;

            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;

            if (coverCount > 0) {
                // Prefix-sum gives us the sidecar base directly: sum of counts for keys before idx
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
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        private void readDeltaBlockMetadata() {
            if (totalValueCount == 0) {
                clearBlockState();
                return;
            }

            long pos = encodedAddr;
            int firstWord = Unsafe.getUnsafe().getInt(pos);

            // Elias-Fano format: set up streaming state, no delta blocks to parse
            if (firstWord == PostingIndexUtils.EF_FORMAT_SENTINEL) {
                pos += 4; // skip sentinel
                efTotalCount = Unsafe.getUnsafe().getInt(pos); pos += 4;
                efL = Unsafe.getUnsafe().getByte(pos) & 0xFF; pos += 1;
                long u = Unsafe.getUnsafe().getLong(pos); pos += 8;
                efLowMask = (efL < 64) ? (1L << efL) - 1 : -1L;
                efLowStart = pos;
                int lowBytes = PostingIndexUtils.efLowBytesAligned(efTotalCount, efL);
                efHighStart = pos + lowBytes;
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

            int blockCount = firstWord;
            if (blockCount < 0 || blockCount > (totalValueCount + PostingIndexUtils.BLOCK_CAPACITY - 1) / PostingIndexUtils.BLOCK_CAPACITY) {
                throw CairoException.critical(0).put("corrupt posting index: invalid block count [blockCount=")
                        .put(blockCount).put(", totalValues=").put(totalValueCount).put(']');
            }
            isEFMode = false;
            pos += 4;

            srcValueCountsAddr = pos;
            pos += blockCount;

            srcFirstValuesAddr = pos;
            pos += (long) blockCount * Long.BYTES;

            srcMinDeltasAddr = pos;
            pos += (long) blockCount * Long.BYTES;

            srcBitWidthsAddr = pos;
            pos += blockCount;

            // packedOffsets only present for multi-block keys
            long srcPackedOffsetsAddr = 0;
            if (blockCount > 1) {
                srcPackedOffsetsAddr = pos;
                pos += (long) blockCount * Integer.BYTES;
            }

            long packedDataStart = pos;

            int startBlock = 0;
            if (minValue > 0 && blockCount > 1) {
                int lo = 0, hi = blockCount - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) mid * Long.BYTES) <= minValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                startBlock = lo;
            }

            int skippedValueCount = 0;
            for (int b = 0; b < startBlock; b++) {
                skippedValueCount += Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
            }
            // Use packedOffsets for O(1) jump to startBlock's packed data
            if (startBlock > 0) {
                packedDataStart += Unsafe.getUnsafe().getInt(srcPackedOffsetsAddr + (long) startBlock * Integer.BYTES);
            }
            this.sidecarStrideKeyStart += skippedValueCount;
            this.denseVarKeyStartCount += skippedValueCount;
            if (!isCurrentGenDense && coverCount > 0) {
                this.sidecarOrdinal += skippedValueCount;
            }

            int endBlock = blockCount;
            if (maxValue < Long.MAX_VALUE && blockCount > 0) {
                for (int b = startBlock; b < blockCount; b++) {
                    if (Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES) > maxValue) {
                        endBlock = b;
                        break;
                    }
                }
            }

            this.encodedBlockCount = endBlock;
            this.packedDataAddr = packedDataStart;
            this.currentBlock = startBlock;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }
    }
}
