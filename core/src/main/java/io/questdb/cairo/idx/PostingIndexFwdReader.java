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
    private ObjList<Cursor> extraCursors;

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
        if (extraCursors != null) {
            for (int i = 0, n = extraCursors.size(); i < n; i++) {
                extraCursors.getQuick(i).close();
            }
            extraCursors.clear();
        }
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
            } else {
                // Non-cached: create a fresh cursor for concurrent use (IN-list).
                // Track it for cleanup in close().
                c = new Cursor();
                if (extraCursors == null) {
                    extraCursors = new ObjList<>();
                }
                extraCursors.add(c);
            }
            c.of(key, minValue, maxValue);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor extends AbstractCoveringCursor {
        private long blockBufferAddr = Unsafe.malloc((long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private int blockBufferCapacity = PostingIndexUtils.PACKED_BATCH_SIZE;
        private int blockBufferEnd;
        private int blockBufferPos;
        private long blockDeltasAddr = Unsafe.malloc((long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private int constantDeltaBlock;
        private int constantDeltaBlockCount;
        private int constantDeltaRemaining;
        private long constantDeltaStep;
        private long constantDeltaValue;
        private int currentBlock;
        private int currentGen;
        private long encodedAddr;
        private int encodedBlockCount;
        private long flatBaseValue;
        private int flatBitWidth;
        private long flatDataBase;
        private int flatRemaining;
        private int flatStartIdx;
        private boolean isConstantDeltaMode;
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
            // Fast path: streaming constant-delta (full scan, all bitWidth=0).
            // Tiny method body for JIT inlining — no min/max checks needed
            // because streaming mode is only active when minValue=0 and
            // maxValue=Long.MAX_VALUE (set by decodeAllBlocksBulk).
            if (constantDeltaRemaining > 0) {
                next = constantDeltaValue;
                constantDeltaValue += constantDeltaStep;
                constantDeltaRemaining--;
                sidecarOrdinal++;
                cachedSidecarIdx = sidecarValueIdx();
                return true;
            }
            return hasNextComplex();
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
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockDeltasAddr = 0;
            }
            closeCoveringResources();
        }

        void of(int key, long minValue, long maxValue) {
            // Re-allocate fixed buffers if freed by close()
            if (blockBufferAddr == 0) {
                blockBufferCapacity = PostingIndexUtils.PACKED_BATCH_SIZE;
                blockBufferAddr = Unsafe.malloc((long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (blockDeltasAddr == 0) {
                blockDeltasAddr = Unsafe.malloc((long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                totalValueCount = 0;
                currentGen = genCount;
                encodedBlockCount = 0;
                currentBlock = 0;
                blockBufferPos = 0;
                blockBufferEnd = 0;
                isConstantDeltaMode = false;
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
            this.isConstantDeltaMode = false;
            this.constantDeltaRemaining = 0;
            this.isFlatMode = false;
            resetCoveringState();
            advanceToNextRelevantGen();
        }

        private void advanceConstantDeltaBlock() {
            int b = constantDeltaBlock;
            int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
            long firstValue = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
            long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
            constantDeltaValue = firstValue;
            constantDeltaStep = minD;
            constantDeltaRemaining = count;
            constantDeltaBlock++;
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
                if (totalValueCount > 0 || encodedBlockCount > 0 || isFlatMode) {
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
                if (totalValueCount > 0 || encodedBlockCount > 0 || isFlatMode) {
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
            this.isConstantDeltaMode = false;
            this.constantDeltaRemaining = 0;
        }

        private void decodeAllBlocksBulk(int blockCount, long packedDataStart) {
            // Check if all blocks have constant delta (bitWidth=0).
            boolean allConstantDelta = true;
            for (int b = 0; b < blockCount; b++) {
                if ((Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF) != 0) {
                    allConstantDelta = false;
                    break;
                }
            }

            if (allConstantDelta) {
                // Streaming mode: no buffer writes, compute values in hasNext().
                this.isConstantDeltaMode = true;
                this.constantDeltaBlockCount = blockCount;
                this.constantDeltaBlock = 0;
                this.encodedBlockCount = 0;
                this.currentBlock = blockCount;
                this.blockBufferPos = 0;
                this.blockBufferEnd = 0;
                advanceConstantDeltaBlock();
                return;
            }

            // Variable-delta fallback: decode into buffer
            if (totalValueCount > blockBufferCapacity) {
                Unsafe.free(blockBufferAddr, (long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferCapacity = Math.max(totalValueCount, blockBufferCapacity * 2);
                blockBufferAddr = Unsafe.malloc((long) blockBufferCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }

            long packedPos = packedDataStart;
            int destIdx = 0;

            for (int b = 0; b < blockCount; b++) {
                int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
                int bitWidth = Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF;
                long firstValue = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
                long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
                int numDeltas = count - 1;

                Unsafe.getUnsafe().putLong(blockBufferAddr + (long) destIdx * Long.BYTES, firstValue);
                long cumulative = firstValue;

                if (numDeltas > 0) {
                    if (bitWidth == 0) {
                        for (int i = 0; i < numDeltas; i++) {
                            cumulative += minD;
                            Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (destIdx + 1 + i) * Long.BYTES, cumulative);
                        }
                    } else {
                        BitpackUtils.unpackAllValues(packedPos, numDeltas, bitWidth, minD, blockDeltasAddr);
                        for (int i = 0; i < numDeltas; i++) {
                            cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                            Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (destIdx + 1 + i) * Long.BYTES, cumulative);
                        }
                        packedPos += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    }
                }
                destIdx += count;
            }

            this.blockBufferPos = 0;
            this.blockBufferEnd = totalValueCount;
            this.encodedBlockCount = 0;
            this.currentBlock = blockCount;
        }

        private void decodeNextBlock() {
            int b = currentBlock;
            int count = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
            int bitWidth = Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF;
            int numDeltas = count - 1;

            long cumulative = Unsafe.getUnsafe().getLong(srcFirstValuesAddr + (long) b * Long.BYTES);
            Unsafe.getUnsafe().putLong(blockBufferAddr, cumulative);

            if (numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(srcMinDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += minD;
                        Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (i + 1) * Long.BYTES, cumulative);
                    }
                } else {
                    BitpackUtils.unpackAllValues(packedDataAddr, numDeltas, bitWidth, minD, blockDeltasAddr);
                    packedDataAddr += BitpackUtils.packedDataSize(numDeltas, bitWidth);
                    for (int i = 0; i < numDeltas; i++) {
                        cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                        Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (i + 1) * Long.BYTES, cumulative);
                    }
                }
            }

            blockBufferPos = 0;
            blockBufferEnd = count;
            currentBlock++;
        }

        private void decodeNextFlatBatch() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            BitpackUtils.unpackValuesFrom(flatDataBase, flatStartIdx, batch, flatBitWidth, flatBaseValue, blockBufferAddr);
            flatStartIdx += batch;
            flatRemaining -= batch;
            blockBufferPos = 0;
            blockBufferEnd = batch;
        }

        private boolean hasNextComplex() {
            while (true) {
                // Streaming mode: advance to next block
                if (isConstantDeltaMode) {
                    if (constantDeltaBlock < constantDeltaBlockCount) {
                        advanceConstantDeltaBlock();
                        if (constantDeltaRemaining > 0) {
                            next = constantDeltaValue;
                            constantDeltaValue += constantDeltaStep;
                            constantDeltaRemaining--;
                            sidecarOrdinal++;
                            cachedSidecarIdx = sidecarValueIdx();
                            return true;
                        }
                        continue;
                    }
                    isConstantDeltaMode = false;
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

                // Try to decode next block in current generation
                if (currentBlock < encodedBlockCount) {
                    decodeNextBlock();
                    continue;
                }

                // Flat mode: decode next batch if remaining
                if (isFlatMode && flatRemaining > 0) {
                    decodeNextFlatBatch();
                    continue;
                }

                // Move to next relevant generation
                if (!advanceToNextRelevantGen()) {
                    return false;
                }

                // New gen may have set up streaming mode — serve first value
                if (constantDeltaRemaining > 0) {
                    next = constantDeltaValue;
                    constantDeltaValue += constantDeltaStep;
                    constantDeltaRemaining--;
                    sidecarOrdinal++;
                    cachedSidecarIdx = sidecarValueIdx();
                    return true;
                }
            }
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
            decodeSidecarKey(stride, localKey);
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
                this.denseVarKeyStartCount = effectiveStart; // adjust for skipped values
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

            int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, requestedKey);
            if (idx < 0) {
                clearBlockState();
                totalValueCount = 0;
                this.isFlatMode = false;
                return;
            }

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

        private void readDeltaBlockMetadata() {
            if (totalValueCount == 0) {
                clearBlockState();
                return;
            }

            long pos = encodedAddr;
            int blockCount = Unsafe.getUnsafe().getInt(pos);
            if (blockCount < 0 || blockCount > (totalValueCount + PostingIndexUtils.BLOCK_CAPACITY - 1) / PostingIndexUtils.BLOCK_CAPACITY) {
                throw CairoException.critical(0).put("corrupt posting index: invalid block count [blockCount=")
                        .put(blockCount).put(", totalValues=").put(totalValueCount).put(']');
            }
            pos += 4;

            srcValueCountsAddr = pos;
            pos += blockCount;

            srcFirstValuesAddr = pos;
            pos += (long) blockCount * Long.BYTES;

            srcMinDeltasAddr = pos;
            pos += (long) blockCount * Long.BYTES;

            srcBitWidthsAddr = pos;
            pos += blockCount;

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
                int vc = Unsafe.getUnsafe().getByte(srcValueCountsAddr + b) & 0xFF;
                int numDeltas = vc - 1;
                packedDataStart += BitpackUtils.packedDataSize(numDeltas, Unsafe.getUnsafe().getByte(srcBitWidthsAddr + b) & 0xFF);
                skippedValueCount += vc;
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

            if (startBlock == 0 && endBlock == blockCount && minValue == 0 && maxValue == Long.MAX_VALUE) {
                decodeAllBlocksBulk(blockCount, packedDataStart);
                return;
            }

            this.encodedBlockCount = endBlock;
            this.packedDataAddr = packedDataStart;
            this.currentBlock = startBlock;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }
    }
}
