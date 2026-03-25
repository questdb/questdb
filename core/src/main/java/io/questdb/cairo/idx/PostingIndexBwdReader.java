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
 * Backward reader for Delta + FoR64 BitPacking bitmap index.
 * <p>
 * Iterates generations in reverse, blocks within each generation in reverse,
 * and values within each block in reverse — producing values in descending order.
 * Uses PostingGenLookup for tiered gen-to-key mapping with cached metadata.
 */
public class PostingIndexBwdReader extends AbstractPostingIndexReader {
    private final Cursor cursor = new Cursor();
    private ObjList<Cursor> extraCursors;

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

    public PostingIndexBwdReader(
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
            final Cursor c;
            if (cachedInstance) {
                c = cursor;
            } else {
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

    private class Cursor implements RowCursor {
        private long blockBufferAddr = Unsafe.malloc((long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long blockDeltasAddr = Unsafe.malloc((long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long next;
        private int blockBufferPos;
        private int currentGen;
        private int encodedBlockCount;
        private int minBlock;
        private int currentBlock;
        private long maxValue;
        private long minValue;
        private int requestedKey;
        private int metadataCapacity = 256;
        private long valueCountsAddr = Unsafe.malloc(256L * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long firstValuesAddr = Unsafe.malloc(256L * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long minDeltasAddr = Unsafe.malloc(256L * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long bitWidthsAddr = Unsafe.malloc(256L * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
        private long[] blockPackedAddrs = new long[256];
        private boolean flatMode;
        private int flatBitWidth;
        private long flatBaseValue;
        private long flatDataBase;
        private int flatStartIdx; // start index of remaining values (going backwards)
        private int flatRemaining;
        private int lookupPos;
        private int lookupEnd;

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from block buffer in reverse
                while (blockBufferPos >= 0) {
                    long value = Unsafe.getUnsafe().getLong(blockBufferAddr + (long) blockBufferPos * Long.BYTES);
                    if (value < minValue) {
                        return false;
                    }
                    blockBufferPos--;
                    if (value <= maxValue) {
                        this.next = value;
                        return true;
                    }
                }

                // Try to decode previous block in current generation
                if (currentBlock >= minBlock) {
                    decodeBlock(currentBlock);
                    currentBlock--;
                    continue;
                }

                // Flat mode: decode previous batch if remaining
                if (flatMode && flatRemaining > 0) {
                    decodeNextFlatBatchReverse();
                    continue;
                }

                // Move to previous generation
                if (!advanceToPrevRelevantGen()) {
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
                currentGen = -1;
                encodedBlockCount = 0;
                currentBlock = -1;
                blockBufferPos = -1;
                return;
            }

            ensureGenLookup();

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.currentGen = genCount; // will be decremented

            // Set up inverted index range for this key (Tier 1), reverse order
            if (genLookup.isPerKeyMode() && key < genLookup.getKeyCount()) {
                this.lookupPos = genLookup.getEntryEnd(key) - 1;
                this.lookupEnd = genLookup.getEntryStart(key);
            } else {
                this.lookupPos = -1;
                this.lookupEnd = 0;
            }

            if (!advanceToPrevRelevantGen()) {
                currentGen = -1;
                encodedBlockCount = 0;
                currentBlock = -1;
                blockBufferPos = -1;
            }
        }

        private boolean advanceToPrevRelevantGen() {
            int lookupTier = genLookup.getTier();

            if (lookupTier == PostingGenLookup.TIER_PER_KEY) {
                return advanceWithPerKeyLookupReverse();
            } else if (lookupTier == PostingGenLookup.TIER_SBBF) {
                return advanceWithSbbfLookupReverse();
            } else {
                return advanceWithLinearScanReverse();
            }
        }

        /**
         * Tier 1: Direct jumps via inverted index in reverse — O(hitGens) per key.
         */
        private boolean advanceWithPerKeyLookupReverse() {
            while (true) {
                if (lookupPos >= lookupEnd) {
                    int nextSparseGen = genLookup.getGenIndex(lookupPos);

                    // Check dense gens between currentGen-1 and this sparse gen (in reverse)
                    currentGen--;
                    while (currentGen > nextSparseGen) {
                        if (genLookup.getGenKeyCount(currentGen) >= 0) {
                            loadDenseGenerationCached(currentGen);
                            return true;
                        }
                        currentGen--;
                    }

                    // Load sparse gen directly
                    int posInGen = genLookup.getPosInGen(lookupPos);
                    lookupPos--;
                    currentGen = nextSparseGen;
                    loadSparseGenDirect(currentGen, posInGen);
                    return true;
                }

                // No more sparse hits — check remaining dense gens in reverse
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
        }

        /**
         * Tier 2: SBBF probe per gen in reverse — skip gens where key definitely absent.
         */
        private boolean advanceWithSbbfLookupReverse() {
            currentGen--;
            while (currentGen >= 0) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    // Dense gen
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                // Sparse gen: check min/max bounds first
                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen--;
                    continue;
                }

                // SBBF probe
                if (!genLookup.mightContainKey(currentGen, requestedKey)) {
                    currentGen--;
                    continue;
                }

                // SBBF says "maybe" — fall back to binary search within this gen
                loadSparseGenWithBinarySearch(currentGen);
                if (encodedBlockCount > 0 || flatMode) {
                    return true;
                }
                currentGen--;
            }
            return false;
        }

        /**
         * Tier 3: Binary search + min/max bounds check per gen in reverse.
         */
        private boolean advanceWithLinearScanReverse() {
            currentGen--;
            while (currentGen >= 0) {
                int gkc = genLookup.getGenKeyCount(currentGen);
                if (gkc >= 0) {
                    // Dense gen
                    loadDenseGenerationCached(currentGen);
                    return true;
                }

                // Sparse gen: check min/max bounds from cached metadata
                if (requestedKey < genLookup.getGenMinKey(currentGen) ||
                        requestedKey > genLookup.getGenMaxKey(currentGen)) {
                    currentGen--;
                    continue;
                }

                // Binary search in sparse gen
                loadSparseGenWithBinarySearch(currentGen);
                if (encodedBlockCount > 0 || flatMode) {
                    return true;
                }
                currentGen--;
            }
            return false;
        }

        private void decodeBlock(int b) {
            int count = Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES);
            int bitWidth = Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES);
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                long minD = Unsafe.getUnsafe().getLong(minDeltasAddr + (long) b * Long.BYTES);
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        Unsafe.getUnsafe().putLong(blockDeltasAddr + (long) i * Long.BYTES, minD);
                    }
                } else {
                    BitpackUtils.unpackAllValues(blockPackedAddrs[b], numDeltas, bitWidth, minD, blockDeltasAddr);
                }
            }

            // Cumulative sum from firstValue
            long cumulative = Unsafe.getUnsafe().getLong(firstValuesAddr + (long) b * Long.BYTES);
            Unsafe.getUnsafe().putLong(blockBufferAddr, cumulative);
            for (int i = 0; i < numDeltas; i++) {
                cumulative += Unsafe.getUnsafe().getLong(blockDeltasAddr + (long) i * Long.BYTES);
                Unsafe.getUnsafe().putLong(blockBufferAddr + (long) (i + 1) * Long.BYTES, cumulative);
            }

            blockBufferPos = count - 1; // start from last value
        }

        private void decodeNextFlatBatchReverse() {
            int batch = Math.min(flatRemaining, PostingIndexUtils.PACKED_BATCH_SIZE);
            // Unpack from the end of remaining values
            int batchStart = flatStartIdx - batch;
            BitpackUtils.unpackValuesFrom(flatDataBase, batchStart, batch, flatBitWidth, flatBaseValue, blockBufferAddr);
            flatStartIdx = batchStart;
            flatRemaining -= batch;
            blockBufferPos = batch - 1;
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
                long newValueCountsAddr = Unsafe.malloc((long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                long newBitWidthsAddr;
                try {
                    newBitWidthsAddr = Unsafe.malloc((long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                } catch (Throwable e) {
                    Unsafe.free(newValueCountsAddr, (long) newCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                    throw e;
                }
                if (valueCountsAddr != 0) {
                    Unsafe.free(valueCountsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                if (bitWidthsAddr != 0) {
                    Unsafe.free(bitWidthsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                }
                valueCountsAddr = newValueCountsAddr;
                bitWidthsAddr = newBitWidthsAddr;
                metadataCapacity = newCapacity;
                blockPackedAddrs = new long[newCapacity];
            }
        }

        void close() {
            if (blockBufferAddr != 0) {
                Unsafe.free(blockBufferAddr, (long) PostingIndexUtils.PACKED_BATCH_SIZE * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockBufferAddr = 0;
            }
            if (blockDeltasAddr != 0) {
                Unsafe.free(blockDeltasAddr, (long) PostingIndexUtils.BLOCK_CAPACITY * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                blockDeltasAddr = 0;
            }
            if (firstValuesAddr != 0) {
                Unsafe.free(firstValuesAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                firstValuesAddr = 0;
            }
            if (minDeltasAddr != 0) {
                Unsafe.free(minDeltasAddr, (long) metadataCapacity * Long.BYTES, MemoryTag.NATIVE_INDEX_READER);
                minDeltasAddr = 0;
            }
            if (valueCountsAddr != 0) {
                Unsafe.free(valueCountsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                valueCountsAddr = 0;
            }
            if (bitWidthsAddr != 0) {
                Unsafe.free(bitWidthsAddr, (long) metadataCapacity * Integer.BYTES, MemoryTag.NATIVE_INDEX_READER);
                bitWidthsAddr = 0;
            }
        }

        private void loadDenseGenerationCached(int gen) {
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
            // Fence ensures value file writes are visible after metadata reads (ARM)
            Unsafe.getUnsafe().loadFence();
            long genAddr = valueMem.addressOf(genFileOffset);

            this.flatMode = false;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
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

                this.flatMode = true;
                this.flatBitWidth = bitWidth;
                this.flatBaseValue = baseValue;
                this.flatDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = -1;

                int batch = Math.min(effectiveCount, PostingIndexUtils.PACKED_BATCH_SIZE);
                int batchStart = effectiveStart + effectiveCount - batch;
                BitpackUtils.unpackValuesFrom(dataAddr, batchStart, batch, bitWidth, baseValue, blockBufferAddr);
                this.blockBufferPos = batch - 1;
                this.flatStartIdx = batchStart;
                this.flatRemaining = effectiveCount - batch;
                return;
            }

            // Delta mode
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int deltaHeaderSize = PostingIndexUtils.strideDeltaHeaderSize(ks);
            long encodedAddr = strideAddr + deltaHeaderSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            readDeltaBlockMetadata(encodedAddr, totalValueCount);
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
            int totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            long encodedAddr = genAddr + headerSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            readDeltaBlockMetadata(encodedAddr, totalValueCount);
        }

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
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                this.flatMode = false;
                return;
            }

            this.flatMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            long encodedAddr = genAddr + headerSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            readDeltaBlockMetadata(encodedAddr, totalValueCount);
        }

        private void readDeltaBlockMetadata(long encodedAddr, int totalValueCount) {
            long pos = encodedAddr;
            int blockCount = Unsafe.getUnsafe().getInt(pos);
            if (blockCount < 0 || blockCount > (totalValueCount + PostingIndexUtils.BLOCK_CAPACITY - 1) / PostingIndexUtils.BLOCK_CAPACITY) {
                throw CairoException.critical(0).put("corrupt posting index: invalid block count [blockCount=")
                        .put(blockCount).put(", totalValues=").put(totalValueCount).put(']');
            }
            pos += 4;

            ensureMetadataCapacity(blockCount);

            for (int b = 0; b < blockCount; b++) {
                Unsafe.getUnsafe().putInt(valueCountsAddr + (long) b * Integer.BYTES, Unsafe.getUnsafe().getByte(pos + b) & 0xFF);
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
                Unsafe.getUnsafe().putInt(bitWidthsAddr + (long) b * Integer.BYTES, Unsafe.getUnsafe().getByte(pos + b) & 0xFF);
            }
            pos += blockCount;

            // Pre-compute packed data addresses for each block (needed for reverse iteration)
            for (int b = 0; b < blockCount; b++) {
                blockPackedAddrs[b] = pos;
                int numDeltas = Unsafe.getUnsafe().getInt(valueCountsAddr + (long) b * Integer.BYTES) - 1;
                pos += BitpackUtils.packedDataSize(numDeltas, Unsafe.getUnsafe().getInt(bitWidthsAddr + (long) b * Integer.BYTES));
            }

            // Trim trailing blocks (highest values) above maxValue.
            // Since we iterate in reverse, these are the first blocks we'd decode.
            int endBlock = blockCount;
            if (maxValue < Long.MAX_VALUE && blockCount > 1) {
                // Binary search: find last block with firstValue <= maxValue
                int lo = 0, hi = blockCount - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getUnsafe().getLong(firstValuesAddr + (long) mid * Long.BYTES) <= maxValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                // lo is the last block that might contain values <= maxValue
                endBlock = lo + 1;
            }

            // Trim leading blocks (lowest values) below minValue.
            int startBlock = 0;
            if (minValue > 0 && blockCount > 1) {
                // Find first block whose firstValue could contain values >= minValue.
                // All blocks before this one have all values < minValue.
                int lo = 0, hi = blockCount - 1;
                while (lo < hi) {
                    int mid = (lo + hi + 1) >>> 1;
                    if (Unsafe.getUnsafe().getLong(firstValuesAddr + (long) mid * Long.BYTES) <= minValue) {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
                startBlock = lo;
            }

            this.encodedBlockCount = endBlock;
            this.currentBlock = endBlock - 1;
            this.minBlock = startBlock;
            this.blockBufferPos = -1;
        }
    }
}
