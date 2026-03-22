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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.api.MemoryMR;
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
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            final Cursor c = cachedInstance ? cursor : new Cursor();
            c.of(key, minValue, maxValue);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor implements CoveringRowCursor {
        private final long[] blockBuffer = new long[PostingIndexUtils.PACKED_BATCH_SIZE];
        private final long[] blockDeltas = new long[PostingIndexUtils.BLOCK_CAPACITY];
        private long next;
        private int blockBufferPos;
        private int blockBufferEnd;
        private int currentGen;
        // Per-generation state for block-buffered decode
        private long encodedAddr;
        private int encodedBlockCount;
        private int currentBlock;
        private long maxValue;
        private long minValue;
        private int requestedKey;
        private int totalValueCount;
        // Block metadata arrays (pre-allocated, grown as needed)
        private int metadataCapacity = 256;
        private int[] valueCounts = new int[256];
        private long[] firstValues = new long[256];
        private long[] minDeltas = new long[256];
        private int[] bitWidths = new int[256];
        private long packedDataAddr;
        // Flat mode batch state (for count > BLOCK_CAPACITY)
        private boolean flatMode;
        private int flatBitWidth;
        private long flatBaseValue;
        private long flatDataBase;
        private int flatStartIdx;
        private int flatRemaining;
        // Inverted index cursor state — jumps directly to relevant sparse gens
        private int lookupPos;
        private int lookupEnd;
        // Covering index sidecar state
        private int sidecarOrdinal;
        private int sidecarStrideKeyStart;
        private int sealedGenKeyCount;
        // Decoded sidecar buffers (one per covered column, lazily allocated)
        private double[][] decodedDoubles;
        private int[][] decodedInts;
        private long[][] decodedLongs;
        private int decodedStride = -1;

        @Override
        public byte getCoveredByte(int includeIdx) {
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return 0;
            }
            return (byte) decodedInts[includeIdx][idx];
        }

        @Override
        public double getCoveredDouble(int includeIdx) {
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedDoubles == null || decodedDoubles[includeIdx] == null) {
                return Double.NaN;
            }
            return decodedDoubles[includeIdx][idx];
        }

        @Override
        public float getCoveredFloat(int includeIdx) {
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return Float.NaN;
            }
            return Float.intBitsToFloat(decodedInts[includeIdx][idx]);
        }

        @Override
        public int getCoveredInt(int includeIdx) {
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return Integer.MIN_VALUE;
            }
            return decodedInts[includeIdx][idx];
        }

        @Override
        public long getCoveredLong(int includeIdx) {
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedLongs == null || decodedLongs[includeIdx] == null) {
                return Long.MIN_VALUE;
            }
            return decodedLongs[includeIdx][idx];
        }

        @Override
        public short getCoveredShort(int includeIdx) {
            int idx = sidecarValueIndex();
            if (idx < 0 || decodedInts == null || decodedInts[includeIdx] == null) {
                return 0;
            }
            return (short) decodedInts[includeIdx][idx];
        }

        @Override
        public boolean hasCovering() {
            return coverCount > 0 && sidecarMems != null && genCount == 1;
        }

        private int sidecarValueIndex() {
            return sidecarStrideKeyStart + sidecarOrdinal - 1;
        }

        private void decodeSidecarStride(int stride) {
            if (stride == decodedStride || sidecarMems == null || coverCount == 0) {
                return;
            }
            if (sealedGenKeyCount <= 0) {
                return;
            }
            int sc = PostingIndexUtils.strideCount(sealedGenKeyCount);
            if (stride >= sc) {
                return;
            }
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);

            if (decodedDoubles == null) {
                decodedDoubles = new double[coverCount][];
                decodedInts = new int[coverCount][];
                decodedLongs = new long[coverCount][];
            }

            for (int c = 0; c < coverCount; c++) {
                MemoryMR mem = sidecarMems[c];
                if (mem == null || mem.size() == 0) {
                    continue;
                }
                long strideIdxOffset = (long) stride * Integer.BYTES;
                if (strideIdxOffset + Integer.BYTES > mem.size()) {
                    continue;
                }
                int strideOff = mem.getInt(strideIdxOffset);
                long dataOffset = siSize + strideOff;
                if (dataOffset >= mem.size()) {
                    continue;
                }
                long addr = mem.addressOf(dataOffset);
                int colType = sidecarColumnTypes[c];

                switch (ColumnType.tagOf(colType)) {
                    case ColumnType.DOUBLE: {
                        int count = Unsafe.getUnsafe().getShort(addr) & 0xFFFF;
                        if (decodedDoubles[c] == null || decodedDoubles[c].length < count) {
                            decodedDoubles[c] = new double[count];
                        }
                        AlpCompression.decompressDoubles(addr, decodedDoubles[c]);
                        break;
                    }
                    case ColumnType.LONG:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.DATE:
                    case ColumnType.GEOLONG: {
                        int count = Unsafe.getUnsafe().getShort(addr) & 0xFFFF;
                        if (decodedLongs[c] == null || decodedLongs[c].length < count) {
                            decodedLongs[c] = new long[count];
                        }
                        AlpCompression.decompressLongs(addr, decodedLongs[c]);
                        break;
                    }
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.GEOINT: {
                        int count = Unsafe.getUnsafe().getShort(addr) & 0xFFFF;
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        AlpCompression.decompressInts(addr, decodedInts[c]);
                        break;
                    }
                    default: {
                        // SHORT, BYTE: small types stored uncompressed
                        // Read raw bytes into int array for uniform access
                        int shift = ColumnType.pow2SizeOf(colType);
                        int ks = PostingIndexUtils.keysInStride(sealedGenKeyCount, stride);
                        // Estimate count from stride key count (conservative)
                        int maxCount = ks * PostingIndexUtils.BLOCK_CAPACITY;
                        if (decodedInts[c] == null || decodedInts[c].length < maxCount) {
                            decodedInts[c] = new int[maxCount];
                        }
                        // Read raw values
                        for (int i = 0; i < maxCount && dataOffset + (long) (i + 1) * (1 << shift) <= mem.size(); i++) {
                            if (shift == 1) {
                                decodedInts[c][i] = mem.getShort(dataOffset + (long) i * Short.BYTES);
                            } else {
                                decodedInts[c][i] = mem.getByte(dataOffset + i);
                            }
                        }
                        break;
                    }
                }
            }
            decodedStride = stride;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from block buffer first
                while (blockBufferPos < blockBufferEnd) {
                    long value = blockBuffer[blockBufferPos];
                    if (value > maxValue) {
                        return false;
                    }
                    blockBufferPos++;

                    if (value >= minValue) {
                        this.next = value;
                        sidecarOrdinal++;
                        return true;
                    }
                    sidecarOrdinal++;
                }

                // Try to decode next block in current generation
                if (currentBlock < encodedBlockCount) {
                    decodeNextBlock();
                    continue;
                }

                // Flat mode: decode next batch if remaining
                if (flatMode && flatRemaining > 0) {
                    decodeNextFlatBatch();
                    continue;
                }

                // Move to next relevant generation
                if (!advanceToNextRelevantGen()) {
                    return false;
                }
            }
        }

        @Override
        public long next() {
            return next;
        }

        void of(int key, long minValue, long maxValue) {
            if (keyCount == 0 || key < 0 || key >= keyCount || genCount == 0) {
                totalValueCount = 0;
                currentGen = genCount;
                encodedBlockCount = 0;
                currentBlock = 0;
                blockBufferPos = 0;
                blockBufferEnd = 0;
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
            this.sidecarOrdinal = 0;
            this.sidecarStrideKeyStart = 0;
            this.decodedStride = -1;
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

        /**
         * Tier 1: Direct jumps via inverted index — O(hitGens) per key.
         */
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

        /**
         * Tier 2: SBBF probe per gen — skip gens where key definitely absent.
         */
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
                if (totalValueCount > 0 || encodedBlockCount > 0 || flatMode) {
                    return true;
                }
                // False positive — key not actually in this gen
                currentGen++;
            }
            return false;
        }

        /**
         * Tier 3: Binary search + min/max bounds check per gen.
         */
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
                if (totalValueCount > 0 || encodedBlockCount > 0 || flatMode) {
                    return true;
                }
                currentGen++;
            }
            return false;
        }

        private void clearBlockState() {
            this.encodedBlockCount = 0;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }

        private void decodeNextBlock() {
            int b = currentBlock;
            int count = valueCounts[b];
            int bitWidth = bitWidths[b];
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        blockDeltas[i] = minDeltas[b];
                    }
                } else {
                    BitpackUtils.unpackAllValues(packedDataAddr, numDeltas, bitWidth, minDeltas[b], blockDeltas);
                }
            }
            packedDataAddr += BitpackUtils.packedDataSize(numDeltas, bitWidth);

            // Cumulative sum from firstValue
            long cumulative = firstValues[b];
            blockBuffer[0] = cumulative;
            for (int i = 0; i < numDeltas; i++) {
                cumulative += blockDeltas[i];
                blockBuffer[i + 1] = cumulative;
            }

            blockBufferPos = 0;
            blockBufferEnd = count;
            currentBlock++;
        }

        private void decodeNextFlatBatch() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            BitpackUtils.unpackValuesFrom(flatDataBase, flatStartIdx, batch, flatBitWidth, flatBaseValue, blockBuffer);
            flatStartIdx += batch;
            flatRemaining -= batch;
            blockBufferPos = 0;
            blockBufferEnd = batch;
        }

        private void ensureMetadataCapacity(int needed) {
            if (needed > metadataCapacity) {
                metadataCapacity = Math.max(needed, metadataCapacity * 2);
                valueCounts = new int[metadataCapacity];
                firstValues = new long[metadataCapacity];
                minDeltas = new long[metadataCapacity];
                bitWidths = new int[metadataCapacity];
            }
        }

        /**
         * Loads a dense generation using cached metadata from PostingGenLookup.
         */
        private void loadDenseGenerationCached(int gen) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                clearBlockState();
                return;
            }

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            this.flatMode = false;
            this.sealedGenKeyCount = genKeyCount;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            decodeSidecarStride(stride);
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
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

                if (count == 0) {
                    clearBlockState();
                    return;
                }

                int flatHeaderSize = PostingIndexUtils.strideFlatHeaderSize(ks);
                long dataAddr = strideAddr + flatHeaderSize;

                // Binary search to skip values below minValue.
                // Flat values are monotonically increasing (value - baseValue),
                // so we can use O(1) random access via unpackValue.
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

                this.flatMode = true;
                this.flatBitWidth = bitWidth;
                this.flatBaseValue = baseValue;
                this.flatDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = 0;
                this.sidecarStrideKeyStart = effectiveStart;
                this.sidecarOrdinal = 0;

                int batch = Math.min(effectiveCount, PostingIndexUtils.PACKED_BATCH_SIZE);
                BitpackUtils.unpackValuesFrom(dataAddr, effectiveStart, batch, bitWidth, baseValue, blockBuffer);
                this.blockBufferPos = 0;
                this.blockBufferEnd = batch;
                this.flatStartIdx = effectiveStart + batch;
                this.flatRemaining = effectiveCount - batch;
                return;
            }

            // Delta mode
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            // Compute sidecar start: sum of counts for keys 0..localKey-1
            int sidecarStart = 0;
            for (int j = 0; j < localKey; j++) {
                sidecarStart += Unsafe.getUnsafe().getInt(countsAddr + (long) j * Integer.BYTES);
            }
            this.sidecarStrideKeyStart = sidecarStart;
            this.sidecarOrdinal = 0;
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            this.encodedAddr = strideAddr + deltaHeaderSize + dataOffset;

            readDeltaBlockMetadata();
        }

        /**
         * Loads a sparse generation using the known position from the inverted index,
         * bypassing binary search entirely.
         */
        private void loadSparseGenDirect(int gen, int idx) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            this.flatMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readDeltaBlockMetadata();
        }

        /**
         * Loads a sparse generation using binary search (Tier 2/3 fallback).
         */
        private void loadSparseGenWithBinarySearch(int gen) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            long genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, requestedKey);
            if (idx < 0) {
                clearBlockState();
                totalValueCount = 0;
                this.flatMode = false;
                return;
            }

            this.flatMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
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
            pos += 4;

            ensureMetadataCapacity(blockCount);

            for (int b = 0; b < blockCount; b++) {
                valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
            }
            pos += blockCount;

            for (int b = 0; b < blockCount; b++) {
                firstValues[b] = Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES);
            }
            pos += (long) blockCount * Long.BYTES;

            for (int b = 0; b < blockCount; b++) {
                minDeltas[b] = Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES);
            }
            pos += (long) blockCount * Long.BYTES;

            for (int b = 0; b < blockCount; b++) {
                bitWidths[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
            }
            pos += blockCount;

            // pos now points to the start of packed data for block 0
            long packedDataStart = pos;

            // Skip blocks whose values are entirely below minValue.
            // firstValues[] stores the first (lowest) absolute value per block.
            // Since values are monotonically increasing, if firstValues[b+1] <= minValue
            // then all values in block b are < minValue and can be skipped.
            int startBlock = 0;
            if (minValue > 0 && blockCount > 1) {
                // Binary search: find the last block whose firstValue <= minValue.
                // That block (or the one before it) is where we need to start decoding.
                int lo = 0, hi = blockCount - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (firstValues[mid] <= minValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                // lo is the last block with firstValues[lo] <= minValue.
                // This block might contain values >= minValue, so start here.
                startBlock = lo;
            }

            // Advance packedDataAddr past skipped blocks and account for
            // skipped values in the sidecar offset calculation
            int skippedValueCount = 0;
            for (int b = 0; b < startBlock; b++) {
                int numDeltas = valueCounts[b] - 1;
                packedDataStart += BitpackUtils.packedDataSize(numDeltas, bitWidths[b]);
                skippedValueCount += valueCounts[b];
            }
            this.sidecarStrideKeyStart += skippedValueCount;

            // Trim trailing blocks that are entirely above maxValue
            int endBlock = blockCount;
            if (maxValue < Long.MAX_VALUE && blockCount > 0) {
                // Find first block whose firstValue > maxValue — all subsequent blocks can be skipped
                for (int b = startBlock; b < blockCount; b++) {
                    if (firstValues[b] > maxValue) {
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
