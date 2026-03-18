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
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Backward reader for Delta + FoR64 BitPacking (BP) bitmap index.
 * <p>
 * Iterates generations in reverse, blocks within each generation in reverse,
 * and values within each block in reverse — producing values in descending order.
 * Uses PostingGenLookup for tiered gen-to-key mapping with cached metadata.
 */
public class PostingIndexBwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(PostingIndexBwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
    private final PostingGenLookup genLookup = new PostingGenLookup();
    protected MillisecondClock clock;
    protected long columnTop;
    protected int keyCount;
    protected long spinLockTimeoutMs;
    private long activePageOffset;
    private long columnTxn;
    private int genCount;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private long valueMemSize = -1;

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

            readIndexMetadataFromBestPage();

            int version = keyMem.getInt(activePageOffset + PostingIndexUtils.PAGE_OFFSET_FORMAT_VERSION);
            if (version != 0 && version != PostingIndexUtils.FORMAT_VERSION) {
                throw CairoException.critical(0).put("Unsupported Posting index version: ").put(version);
            }

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
        // Check both pages for a higher sequence than cached
        long seqA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long seqB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
        long maxSeq = Math.max(seqA, seqB);
        if (maxSeq != keyFileSequence) {
            readIndexMetadataFromBestPage();
            if (valueMemSize > 0) {
                ((MemoryCMR) this.valueMem).changeSize(valueMemSize);
            }
        }
    }

    public void updateKeyCount() {
        for (int attempt = 0; attempt < 2; attempt++) {
            long seqStartA = keyMem.getLong(PostingIndexUtils.PAGE_A_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
            long seqStartB = keyMem.getLong(PostingIndexUtils.PAGE_B_OFFSET + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);

            long bestPage = (seqStartB > seqStartA) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
            long otherPage = (bestPage == PostingIndexUtils.PAGE_A_OFFSET) ? PostingIndexUtils.PAGE_B_OFFSET : PostingIndexUtils.PAGE_A_OFFSET;
            long tryPage = (attempt == 0) ? bestPage : otherPage;

            long seqStart = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
            if (seqStart <= keyFileSequence) {
                if (attempt == 0) continue;
                return;
            }
            Unsafe.getUnsafe().loadFence();

            int keyCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
            int genCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);
            long valueMemSize = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
            genLookup.snapshotMetadata(keyMem, genCount, tryPage);

            Unsafe.getUnsafe().loadFence();
            long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

            if (seqStart == seqEnd && keyCount >= this.keyCount) {
                if (valueMemSize > 0) {
                    ((MemoryCMR) valueMem).changeSize(valueMemSize);
                }
                this.activePageOffset = tryPage;
                this.keyFileSequence = seqStart;
                this.valueMemSize = valueMemSize;
                this.keyCount = keyCount;
                this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                this.genCount = genCount;
                return;
            }
        }
    }

    private void ensureGenLookup() {
        if (genCount == 0 || keyCount == 0) {
            return;
        }
        genLookup.buildLookupIfNeeded(valueMem, keyCount, genCount);
    }

    private void readIndexMetadataFromBestPage() {
        for (int attempt = 0; attempt < 2; attempt++) {
            long pageA = PostingIndexUtils.PAGE_A_OFFSET;
            long pageB = PostingIndexUtils.PAGE_B_OFFSET;
            long seqStartA = keyMem.getLong(pageA + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
            long seqStartB = keyMem.getLong(pageB + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);

            long bestPage = (seqStartB > seqStartA) ? pageB : pageA;
            long otherPage = (bestPage == pageA) ? pageB : pageA;
            long tryPage = (attempt == 0) ? bestPage : otherPage;

            long seqStart = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_START);
            Unsafe.getUnsafe().loadFence();

            long valueMemSize = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_VALUE_MEM_SIZE);
            int keyCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_KEY_COUNT);
            int genCount = keyMem.getInt(tryPage + PostingIndexUtils.PAGE_OFFSET_GEN_COUNT);

            genLookup.snapshotMetadata(keyMem, genCount, tryPage);

            Unsafe.getUnsafe().loadFence();
            long seqEnd = keyMem.getLong(tryPage + PostingIndexUtils.PAGE_OFFSET_SEQUENCE_END);

            if (seqStart == seqEnd && seqStart > 0) {
                this.activePageOffset = tryPage;
                this.keyFileSequence = seqStart;
                this.valueMemSize = valueMemSize;
                this.keyCount = keyCount;
                this.genCount = genCount;
                this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                return;
            }
        }
        this.keyFileSequence = 0;
        this.valueMemSize = 0;
        this.keyCount = 0;
        this.genCount = 0;
    }

    private class Cursor implements RowCursor {
        private final long[] blockBuffer = new long[PostingIndexUtils.BLOCK_CAPACITY];
        private final long[] blockDeltas = new long[PostingIndexUtils.BLOCK_CAPACITY];
        protected long next;
        private int blockBufferPos;
        private int currentGen;
        private int encodedBlockCount;
        private int currentBlock;
        private long maxValue;
        private long minValue;
        private int requestedKey;
        // Block metadata arrays (pre-allocated, grown as needed)
        private int metadataCapacity = 256;
        private int[] valueCounts = new int[256];
        private long[] firstValues = new long[256];
        private long[] minDeltas = new long[256];
        private int[] bitWidths = new int[256];
        private long[] blockPackedAddrs = new long[256];
        // Packed mode batch state (for count > BLOCK_CAPACITY)
        private boolean packedMode;
        private int packedBitWidth;
        private long packedBaseValue;
        private long packedDataBase;
        private int packedStartIdx; // start index of remaining values (going backwards)
        private int packedRemaining;

        @Override
        public boolean hasNext() {
            while (true) {
                // Serve from block buffer in reverse
                while (blockBufferPos >= 0) {
                    long value = blockBuffer[blockBufferPos];
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
                if (currentBlock >= 0) {
                    decodeBlock(currentBlock);
                    currentBlock--;
                    continue;
                }

                // Packed mode: decode previous batch if remaining
                if (packedMode && packedRemaining > 0) {
                    decodeNextPackedBatchReverse();
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
            if (!advanceToPrevRelevantGen()) {
                currentGen = -1;
                encodedBlockCount = 0;
                currentBlock = -1;
                blockBufferPos = -1;
            }
        }

        private boolean advanceToPrevRelevantGen() {
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

                // Use SBBF if available (Tier 2)
                if (genLookup.getTier() == PostingGenLookup.TIER_SBBF) {
                    if (!genLookup.mightContainKey(currentGen, requestedKey)) {
                        currentGen--;
                        continue;
                    }
                }

                // Binary search in sparse gen
                loadSparseGenWithBinarySearch(currentGen);
                if (encodedBlockCount > 0 || packedMode) {
                    return true;
                }
                currentGen--;
            }
            return false;
        }

        private void decodeBlock(int b) {
            int count = valueCounts[b];
            int bitWidth = bitWidths[b];
            int numDeltas = count - 1;

            if (numDeltas > 0) {
                if (bitWidth == 0) {
                    for (int i = 0; i < numDeltas; i++) {
                        blockDeltas[i] = minDeltas[b];
                    }
                } else {
                    FORBitmapIndexUtils.unpackAllValues(blockPackedAddrs[b], numDeltas, bitWidth, minDeltas[b], blockDeltas);
                }
            }

            // Cumulative sum from firstValue
            long cumulative = firstValues[b];
            blockBuffer[0] = cumulative;
            for (int i = 0; i < numDeltas; i++) {
                cumulative += blockDeltas[i];
                blockBuffer[i + 1] = cumulative;
            }

            blockBufferPos = count - 1; // start from last value
        }

        private void decodeNextPackedBatchReverse() {
            int batch = Math.min(packedRemaining, PostingIndexUtils.BLOCK_CAPACITY);
            // Unpack from the end of remaining values
            int batchStart = packedStartIdx - batch;
            for (int i = 0; i < batch; i++) {
                blockBuffer[i] = FORBitmapIndexUtils.unpackValue(packedDataBase, batchStart + i, packedBitWidth, packedBaseValue);
            }
            packedStartIdx = batchStart;
            packedRemaining -= batch;
            blockBufferPos = batch - 1;
        }

        private void ensureMetadataCapacity(int needed) {
            if (needed > metadataCapacity) {
                metadataCapacity = Math.max(needed, metadataCapacity * 2);
                valueCounts = new int[metadataCapacity];
                firstValues = new long[metadataCapacity];
                minDeltas = new long[metadataCapacity];
                bitWidths = new int[metadataCapacity];
                blockPackedAddrs = new long[metadataCapacity];
            }
        }

        private void loadDenseGenerationCached(int gen) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            int genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);

            if (requestedKey >= genKeyCount) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
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
                    this.encodedBlockCount = 0;
                    this.currentBlock = -1;
                    this.blockBufferPos = -1;
                    return;
                }

                int packedHeaderSize = PostingIndexUtils.stridePackedHeaderSize(ks);
                long dataAddr = strideAddr + packedHeaderSize;

                this.packedMode = true;
                this.packedBitWidth = bitWidth;
                this.packedBaseValue = baseValue;
                this.packedDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = -1;

                int batch = Math.min(count, PostingIndexUtils.BLOCK_CAPACITY);
                int batchStart = startCount + count - batch;
                for (int i = 0; i < batch; i++) {
                    blockBuffer[i] = FORBitmapIndexUtils.unpackValue(dataAddr, batchStart + i, bitWidth, baseValue);
                }
                this.blockBufferPos = batch - 1;
                this.packedStartIdx = batchStart;
                this.packedRemaining = count - batch;
                return;
            }

            // BP mode
            long countsAddr = strideAddr + PostingIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int bpHeaderSize = PostingIndexUtils.strideBPHeaderSize(ks);
            long encodedAddr = strideAddr + bpHeaderSize + dataOffset;

            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            readBPBlockMetadata(encodedAddr);
        }

        private void loadSparseGenWithBinarySearch(int gen) {
            long genFileOffset = genLookup.getGenFileOffset(gen);
            int genDataSize = genLookup.getGenDataSize(gen);
            int genKeyCount = genLookup.getGenKeyCount(gen);
            int activeKeyCount = -genKeyCount;

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);

            int idx = PostingIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, requestedKey);
            if (idx < 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            this.packedMode = false;

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

            readBPBlockMetadata(encodedAddr);
        }

        private void readBPBlockMetadata(long encodedAddr) {
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

            // Pre-compute packed data addresses for each block (needed for reverse iteration)
            for (int b = 0; b < encodedBlockCount; b++) {
                blockPackedAddrs[b] = pos;
                int numDeltas = valueCounts[b] - 1;
                pos += FORBitmapIndexUtils.packedDataSize(numDeltas, bitWidths[b]);
            }

            this.currentBlock = encodedBlockCount - 1;
            this.blockBufferPos = -1; // will be set by decodeBlock
        }
    }
}
