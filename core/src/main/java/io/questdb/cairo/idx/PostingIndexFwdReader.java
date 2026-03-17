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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Forward reader for Delta + FoR64 BitPacking (BP) bitmap index.
 * <p>
 * Block-buffered decode: unpacks 64 values at a time from FoR64 blocks.
 * Generation iteration uses PostingGenLookup for tiered gen-to-key mapping.
 */
public class PostingIndexFwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(PostingIndexFwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
    private final PostingGenLookup genLookup = new PostingGenLookup();
    protected MillisecondClock clock;
    protected long columnTop;
    protected int keyCount;
    protected long spinLockTimeoutMs;
    private long columnTxn;
    private int genCount;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private long valueMemSize = -1;

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
        genLookup.close();
        Misc.free(keyMem);
        Misc.free(valueMem);
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
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

    @Override
    public long getKeyBaseAddress() {
        return keyMem.addressOf(0);
    }

    @Override
    public int getKeyCount() {
        return keyCountIncludingNulls;
    }

    @Override
    public long getKeyMemorySize() {
        return keyMem.size();
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public long getValueBaseAddress() {
        return valueMem.addressOf(0);
    }

    /**
     * Returns 0 because PostingIndex does not use the legacy block-linked-list
     * value file layout. The only consumer (GeoHashNative.latestByAndFilterPrefix)
     * expects that layout and cannot operate on PostingIndex data regardless.
     */
    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return valueMem.size();
    }

    @Override
    public boolean isOpen() {
        return keyMem.getFd() != -1;
    }

    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        this.columnTop = columnTop;
        this.columnTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        final int plen = path.size();
        this.spinLockTimeoutMs = configuration.getSpinLockTimeout();

        try {
            FilesFacade ff = configuration.getFilesFacade();
            LPSZ name = PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    PostingIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            if (keyMem.getByte(PostingIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != PostingIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            int version = keyMem.getInt(PostingIndexUtils.KEY_RESERVED_OFFSET_FORMAT_VERSION);
            if (version != 0 && version != PostingIndexUtils.FORMAT_VERSION) {
                throw CairoException.critical(0).put("Unsupported Posting index version: ").put(version);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    PostingIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
                    valueMemSize,
                    valueMemSize,
                    MemoryTag.MMAP_INDEX_READER
            );
        } catch (Throwable e) {
            close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void reloadConditionally() {
        long seq = keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            long keyFileSize = PostingIndexUtils.getGenDirOffset(genCount);
            this.keyMem.extend(keyFileSize);
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        int genCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(PostingIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                genCount = keyMem.getInt(PostingIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);
                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                    break;
                }
            }
            if (clock.getTicks() > deadline) {
                this.keyCount = 0;
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }

        if (keyCount > this.keyCount) {
            this.keyCount = keyCount;
            this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
            this.genCount = genCount;
            long keyFileSize = PostingIndexUtils.getGenDirOffset(genCount);
            keyMem.extend(keyFileSize);
        }
    }

    /**
     * Prefetches all generation data pages into the OS page cache.
     * Call before a full scan to ensure sequential gen-major access
     * pattern results in cache hits rather than page faults.
     */
    public void prefetchGenData() {
        ensureGenLookup();
        if (genCount == 0 || keyCount == 0) {
            return;
        }
        // Touch each gen's data region sequentially (gen-major order)
        // to bring pages into L3/page cache
        long checksum = 0;
        for (int g = 0; g < genCount; g++) {
            long genFileOffset = genLookup.getGenFileOffset(g);
            int genDataSize = genLookup.getGenDataSize(g);
            if (genDataSize > 0) {
                valueMem.extend(genFileOffset + genDataSize);
                long addr = valueMem.addressOf(genFileOffset);
                // Read one byte per 4KB page to trigger page-in
                for (int off = 0; off < genDataSize; off += 4096) {
                    checksum += Unsafe.getUnsafe().getByte(addr + off);
                }
            }
        }
        // Volatile store to prevent dead code elimination
        if (checksum == Long.MIN_VALUE) {
            throw new IllegalStateException("unreachable");
        }
    }

    private void ensureGenLookup() {
        if (genCount == 0 || keyCount == 0) {
            return;
        }
        genLookup.buildIncremental(keyMem, valueMem, keyCount, genCount);
    }

    private void readIndexMetadataAtomically() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                int keyCount = keyMem.getInt(PostingIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                long valueMemSize = keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                int genCount = keyMem.getInt(PostingIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(PostingIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.genCount = genCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;

                    long keyFileSize = PostingIndexUtils.getGenDirOffset(genCount);
                    keyMem.extend(keyFileSize);
                    break;
                }
            }

            if (clock.getTicks() > deadline) {
                LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms]").$();
                throw CairoException.critical(0).put(INDEX_CORRUPT);
            }
            Os.pause();
        }
    }

    private class Cursor implements RowCursor {
        private final long[] blockBuffer = new long[PostingIndexUtils.BLOCK_CAPACITY];
        private final long[] blockDeltas = new long[PostingIndexUtils.BLOCK_CAPACITY];
        protected long next;
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
        private int metadataCapacity;
        private int[] valueCounts = new int[4];
        private long[] firstValues = new long[4];
        private long[] minDeltas = new long[4];
        private int[] bitWidths = new int[4];
        private long packedDataAddr;
        // Packed mode batch state (for count > BLOCK_CAPACITY)
        private boolean packedMode;
        private int packedBitWidth;
        private long packedBaseValue;
        private long packedDataBase;
        private int packedStartIdx;
        private int packedRemaining;
        // Inverted index cursor state — jumps directly to relevant sparse gens
        private int lookupPos;
        private int lookupEnd;
        private boolean exhausted;

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
                        return true;
                    }
                }

                // Try to decode next block in current generation
                if (currentBlock < encodedBlockCount) {
                    decodeNextBlock();
                    continue;
                }

                // Packed mode: decode next batch if remaining
                if (packedMode && packedRemaining > 0) {
                    decodeNextPackedBatch();
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
                exhausted = true;
                return;
            }

            ensureGenLookup();

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.currentGen = -1; // will be advanced by first advanceToNextRelevantGen()
            this.exhausted = false;

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
            if (!advanceToNextRelevantGen()) {
                exhausted = true;
            }
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
                if (totalValueCount > 0 || encodedBlockCount > 0 || packedMode) {
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
                if (totalValueCount > 0 || encodedBlockCount > 0 || packedMode) {
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
                    FORBitmapIndexUtils.unpackAllValues(packedDataAddr, numDeltas, bitWidth, minDeltas[b], blockDeltas);
                }
            }
            packedDataAddr += FORBitmapIndexUtils.packedDataSize(numDeltas, bitWidth);

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

        private void decodeNextPackedBatch() {
            int batch = Math.min(packedRemaining, PostingIndexUtils.BLOCK_CAPACITY);
            FORBitmapIndexUtils.unpackValuesFrom(packedDataBase, packedStartIdx, batch, packedBitWidth, packedBaseValue, blockBuffer);
            packedStartIdx += batch;
            packedRemaining -= batch;
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
            int genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                clearBlockState();
                return;
            }

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            this.packedMode = false;

            int stride = requestedKey / PostingIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingIndexUtils.DENSE_STRIDE;
            int siSize = PostingIndexUtils.strideIndexSize(genKeyCount);
            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingIndexUtils.keysInStride(genKeyCount, stride);
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

            if (mode == PostingIndexUtils.STRIDE_MODE_PACKED) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long baseValue = Unsafe.getUnsafe().getLong(strideAddr + PostingIndexUtils.STRIDE_PACKED_BASE_OFFSET);
                long prefixAddr = strideAddr + PostingIndexUtils.STRIDE_PACKED_PREFIX_COUNTS_OFFSET;
                int startCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startCount;

                if (count == 0) {
                    clearBlockState();
                    return;
                }

                int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                long dataAddr = strideAddr + packedHeaderSize;

                this.packedMode = true;
                this.packedBitWidth = bitWidth;
                this.packedBaseValue = baseValue;
                this.packedDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = 0;

                int batch = Math.min(count, PostingIndexUtils.BLOCK_CAPACITY);
                FORBitmapIndexUtils.unpackValuesFrom(dataAddr, startCount, batch, bitWidth, baseValue, blockBuffer);
                this.blockBufferPos = 0;
                this.blockBufferEnd = batch;
                this.packedStartIdx = startCount + batch;
                this.packedRemaining = count - batch;
                return;
            }

            // BP mode
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
            this.encodedAddr = strideAddr + bpHeaderSize + dataOffset;

            readBPBlockMetadata();
        }

        /**
         * Loads a sparse generation using the known position from the inverted index,
         * bypassing binary search entirely.
         */
        private void loadSparseGenDirect(int gen, int idx) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            int genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            this.packedMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readBPBlockMetadata();
        }

        /**
         * Loads a sparse generation using binary search (Tier 2/3 fallback).
         */
        private void loadSparseGenWithBinarySearch(int gen) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            int genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, requestedKey);
            if (idx < 0) {
                clearBlockState();
                totalValueCount = 0;
                return;
            }

            this.packedMode = false;

            int headerSize = PostingIndexUtils.genHeaderSizeSparse(activeKeyCount);
            long countsBase = genAddr + (long) activeKeyCount * Integer.BYTES;
            long offsetsBase = countsBase + (long) activeKeyCount * Integer.BYTES;
            this.totalValueCount = Unsafe.getUnsafe().getInt(countsBase + (long) idx * Integer.BYTES);
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) idx * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            readBPBlockMetadata();
        }

        private void readBPBlockMetadata() {
            if (totalValueCount == 0) {
                clearBlockState();
                return;
            }

            long pos = encodedAddr;
            this.encodedBlockCount = Unsafe.getUnsafe().getShort(pos) & 0xFFFF;
            pos += 2;

            ensureMetadataCapacity(encodedBlockCount);

            for (int b = 0; b < encodedBlockCount; b++) {
                valueCounts[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
            }
            pos += encodedBlockCount;

            for (int b = 0; b < encodedBlockCount; b++) {
                firstValues[b] = Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES);
            }
            pos += (long) encodedBlockCount * Long.BYTES;

            for (int b = 0; b < encodedBlockCount; b++) {
                minDeltas[b] = Unsafe.getUnsafe().getLong(pos + (long) b * Long.BYTES);
            }
            pos += (long) encodedBlockCount * Long.BYTES;

            for (int b = 0; b < encodedBlockCount; b++) {
                bitWidths[b] = Unsafe.getUnsafe().getByte(pos + b) & 0xFF;
            }
            pos += encodedBlockCount;

            this.packedDataAddr = pos;
            this.currentBlock = 0;
            this.blockBufferPos = 0;
            this.blockBufferEnd = 0;
        }
    }
}
