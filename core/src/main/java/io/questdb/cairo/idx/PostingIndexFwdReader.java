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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
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

    @Override
    public void close() {
        cursor.close();
        super.close();
    }

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
            // Always use cached cursor — posting cursors hold native memory
            // that cannot be freed through the RowCursor interface.
            cursor.of(key, minValue, maxValue);
            return cursor;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor implements CoveringRowCursor {
        private final long blockBufferAddr = Unsafe.malloc((long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private final long blockDeltasAddr = Unsafe.malloc((long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
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
        private long firstValuesAddr = Unsafe.malloc(256L * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long minDeltasAddr = Unsafe.malloc(256L * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
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
        private long decodeWorkspaceAddr; // reusable native workspace for ALP decompress
        private int decodeWorkspaceCapacity;
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

        @Override
        public long seekToLast() {
            long lastRowId = -1;
            while (hasNext()) {
                lastRowId = next();
            }
            return lastRowId;
        }

        private int sidecarValueIndex() {
            // sidecarStrideKeyStart tracks values skipped by block-skip (delta mode)
            // or binary search (flat mode) within this key's decoded sidecar block.
            // sidecarOrdinal counts values iterated (both emitted and filtered).
            return sidecarStrideKeyStart + sidecarOrdinal - 1;
        }

        private void decodeSidecarKey(int stride, int localKey) {
            if (sidecarMems == null || coverCount == 0 || sealedGenKeyCount <= 0) {
                return;
            }
            int sc = PostingIndexUtils.strideCount(sealedGenKeyCount);
            if (stride >= sc) {
                return;
            }
            int siSize = PostingIndexUtils.strideIndexSize(sealedGenKeyCount);
            int ks = PostingIndexUtils.keysInStride(sealedGenKeyCount, stride);
            if (localKey >= ks) {
                return;
            }

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
                // Get stride data offset from stride index
                long strideIdxOffset = (long) stride * Integer.BYTES;
                if (strideIdxOffset + Integer.BYTES > mem.size()) {
                    continue;
                }
                int strideOff = mem.getInt(strideIdxOffset);
                long strideDataStart = siSize + strideOff;
                if (strideDataStart >= mem.size()) {
                    continue;
                }

                // Per-key layout: [key_offsets: ks × 4B][key_0_block][key_1_block]...
                long keyOffsetsAddr = mem.addressOf(strideDataStart);
                int keyBlockOff = Unsafe.getUnsafe().getInt(keyOffsetsAddr + (long) localKey * Integer.BYTES);
                long keyBlockAddr = mem.addressOf(strideDataStart + (long) ks * Integer.BYTES + keyBlockOff);
                int count = Unsafe.getUnsafe().getInt(keyBlockAddr);
                if (count == 0) {
                    continue;
                }
                int colType = sidecarColumnTypes[c];

                switch (ColumnType.tagOf(colType)) {
                    case ColumnType.DOUBLE: {
                        if (decodedDoubles[c] == null || decodedDoubles[c].length < count) {
                            decodedDoubles[c] = new double[count];
                        }
                        ensureDecodeWorkspaceCapacity(count);
                        AlpCompression.decompressDoubles(keyBlockAddr, decodedDoubles[c], decodeWorkspaceAddr);
                        break;
                    }
                    case ColumnType.LONG:
                    case ColumnType.TIMESTAMP:
                    case ColumnType.DATE:
                    case ColumnType.GEOLONG: {
                        if (decodedLongs[c] == null || decodedLongs[c].length < count) {
                            decodedLongs[c] = new long[count];
                        }
                        AlpCompression.decompressLongs(keyBlockAddr, decodedLongs[c], decodeWorkspaceAddr);
                        break;
                    }
                    case ColumnType.INT:
                    case ColumnType.FLOAT:
                    case ColumnType.GEOINT:
                    case ColumnType.SYMBOL: {
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        ensureDecodeWorkspaceCapacity(count);
                        AlpCompression.decompressInts(keyBlockAddr, decodedInts[c], decodeWorkspaceAddr);
                        break;
                    }
                    case ColumnType.SHORT:
                    case ColumnType.GEOSHORT: {
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        long rawAddr = keyBlockAddr + 4; // skip count header
                        for (int i = 0; i < count; i++) {
                            decodedInts[c][i] = Unsafe.getUnsafe().getShort(rawAddr + (long) i * Short.BYTES);
                        }
                        break;
                    }
                    case ColumnType.BYTE:
                    case ColumnType.BOOLEAN:
                    case ColumnType.GEOBYTE: {
                        if (decodedInts[c] == null || decodedInts[c].length < count) {
                            decodedInts[c] = new int[count];
                        }
                        long rawAddr = keyBlockAddr + 4; // skip count header
                        for (int i = 0; i < count; i++) {
                            decodedInts[c][i] = Unsafe.getUnsafe().getByte(rawAddr + i);
                        }
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("unsupported sidecar column type: " + ColumnType.nameOf(colType));
                }
            }
            decodedStride = stride;
        }

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from block buffer first
                while (blockBufferPos < blockBufferEnd) {
                    long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
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
                long minD = Unsafe.getUnsafe().getLong(minDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                    }
                } else {
                    BitpackUtils.unpackAllValues(packedDataAddr, numDeltas, bitWidth, minD, blockDeltasAddr);
                }
            }
            packedDataAddr += BitpackUtils.packedDataSize(numDeltas, bitWidth);

            // Cumulative sum from firstValue
            long cumulative = Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES);
            Unsafe.getUnsafe().putLong(blockBufferAddr, cumulative);
            for (int i = 0; i < numDeltas; i++) {
                cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (i + 1) * Long.BYTES, cumulative);
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

        private void ensureMetadataCapacity(int needed) {
            if (needed > metadataCapacity) {
                int newCapacity = Math.max(needed, metadataCapacity * 2);
                long newFirstAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newMinAddr = Unsafe.malloc((long) newCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                Unsafe.free(firstValuesAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                Unsafe.free(minDeltasAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = newFirstAddr;
                minDeltasAddr = newMinAddr;
                metadataCapacity = newCapacity;
                valueCounts = new int[newCapacity];
                bitWidths = new int[newCapacity];
            }
        }

        private void ensureDecodeWorkspaceCapacity(int count) {
            if (count > decodeWorkspaceCapacity) {
                if (decodeWorkspaceAddr != 0) {
                    Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                decodeWorkspaceCapacity = count;
                decodeWorkspaceAddr = Unsafe.malloc((long) count * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            }
        }

        void close() {
            Unsafe.free(blockBufferAddr, (long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            Unsafe.free(blockDeltasAddr, (long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = 0;
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                minDeltasAddr = 0;
            }
            if (decodeWorkspaceAddr != 0) {
                Unsafe.free(decodeWorkspaceAddr, (long) decodeWorkspaceCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                decodeWorkspaceAddr = 0;
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
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.flatMode = false;
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
                this.sidecarStrideKeyStart = effectiveStart - startCount;
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
            Unsafe.getUnsafe().loadFence();
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
            Unsafe.getUnsafe().loadFence();
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
            if (blockCount < 0 || blockCount > (totalValueCount + PostingIndexUtils.BLOCK_CAPACITY - 1) / PostingIndexUtils.BLOCK_CAPACITY) {
                throw CairoException.critical(0).put("corrupt posting index: invalid block count [blockCount=")
                        .put(blockCount).put(", totalValues=").put(totalValueCount).put(']');
            }
            pos += 4;

            ensureMetadataCapacity(blockCount);

            for (int b = 0; b < blockCount; b++) {
                valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
            }
            pos += blockCount;

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putLong(firstValuesAddr + (long) b * Long.BYTES,
                        Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
            }
            pos += (long) blockCount * Long.BYTES;

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putLong(minDeltasAddr + (long) b * Long.BYTES,
                        Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES));
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
                    if (Unsafe.getUnsafe().getLong(firstValuesAddr + (long) mid * Long.BYTES) <= minValue) {
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
                    if (Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES) > maxValue) {
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
