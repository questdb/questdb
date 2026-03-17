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
 * Backward reader for Delta + FoR64 BitPacking (BP) bitmap index.
 * <p>
 * Iterates generations in reverse, blocks within each generation in reverse,
 * and values within each block in reverse — producing values in descending order.
 * Uses PostingsGenLookup for tiered gen-to-key mapping with cached metadata.
 */
public class PostingsIndexBwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(PostingsIndexBwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
    private final PostingsGenLookup genLookup = new PostingsGenLookup();
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

    public PostingsIndexBwdReader(
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
            LPSZ name = PostingsIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    PostingsIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            if (keyMem.getByte(PostingsIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != PostingsIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    PostingsIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
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
        long seq = keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            long keyFileSize = PostingsIndexUtils.getGenDirOffset(genCount);
            this.keyMem.extend(keyFileSize);
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        int genCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(PostingsIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                genCount = keyMem.getInt(PostingsIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);
                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
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
            long keyFileSize = PostingsIndexUtils.getGenDirOffset(genCount);
            keyMem.extend(keyFileSize);
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
            long seq = keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                int keyCount = keyMem.getInt(PostingsIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                long valueMemSize = keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                int genCount = keyMem.getInt(PostingsIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(PostingsIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.genCount = genCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;

                    long keyFileSize = PostingsIndexUtils.getGenDirOffset(genCount);
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
        private final long[] blockBuffer = new long[PostingsIndexUtils.BLOCK_CAPACITY];
        private final long[] blockDeltas = new long[PostingsIndexUtils.BLOCK_CAPACITY];
        protected long next;
        private int blockBufferPos;
        private int currentGen;
        private int encodedBlockCount;
        private int currentBlock;
        private long maxValue;
        private long minValue;
        private int requestedKey;
        // Block metadata arrays (pre-allocated, grown as needed)
        private int metadataCapacity;
        private int[] valueCounts = new int[4];
        private long[] firstValues = new long[4];
        private long[] minDeltas = new long[4];
        private int[] bitWidths = new int[4];
        private long[] blockPackedAddrs = new long[4];
        // Packed mode batch state (for count > BLOCK_CAPACITY)
        private boolean packedMode;
        private int packedBitWidth;
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
                if (genLookup.getTier() == PostingsGenLookup.TIER_SBBF) {
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
            int batch = Math.min(packedRemaining, PostingsIndexUtils.BLOCK_CAPACITY);
            // Unpack from the end of remaining values
            int batchStart = packedStartIdx - batch;
            for (int i = 0; i < batch; i++) {
                blockBuffer[i] = FORBitmapIndexUtils.unpackValue(packedDataBase, batchStart + i, packedBitWidth, 0);
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

            int stride = requestedKey / PostingsIndexUtils.DENSE_STRIDE;
            int localKey = requestedKey % PostingsIndexUtils.DENSE_STRIDE;
            int siSize = PostingsIndexUtils.strideIndexSize(genKeyCount);
            int strideOff = Unsafe.getUnsafe().getInt(genAddr + (long) stride * Integer.BYTES);
            long strideAddr = genAddr + siSize + strideOff;
            int ks = PostingsIndexUtils.keysInStride(genKeyCount, stride);
            byte mode = Unsafe.getUnsafe().getByte(strideAddr);

            if (mode == PostingsIndexUtils.STRIDE_MODE_PACKED) {
                int bitWidth = Unsafe.getUnsafe().getByte(strideAddr + 1) & 0xFF;
                long prefixAddr = strideAddr + PostingsIndexUtils.STRIDE_MODE_PREFIX_SIZE;
                int startCount = Unsafe.getUnsafe().getInt(prefixAddr + (long) localKey * Integer.BYTES);
                int count = Unsafe.getUnsafe().getInt(prefixAddr + (long) (localKey + 1) * Integer.BYTES) - startCount;

                if (count == 0) {
                    this.encodedBlockCount = 0;
                    this.currentBlock = -1;
                    this.blockBufferPos = -1;
                    return;
                }

                int packedHeaderSize = PostingsIndexUtils.stridePackedHeaderSize(ks);
                long dataAddr = strideAddr + packedHeaderSize;

                this.packedMode = true;
                this.packedBitWidth = bitWidth;
                this.packedDataBase = dataAddr;
                this.encodedBlockCount = 0;
                this.currentBlock = -1;

                int batch = Math.min(count, PostingsIndexUtils.BLOCK_CAPACITY);
                int batchStart = startCount + count - batch;
                for (int i = 0; i < batch; i++) {
                    blockBuffer[i] = FORBitmapIndexUtils.unpackValue(dataAddr, batchStart + i, bitWidth, 0);
                }
                this.blockBufferPos = batch - 1;
                this.packedStartIdx = batchStart;
                this.packedRemaining = count - batch;
                return;
            }

            // BP mode
            long countsAddr = strideAddr + PostingsIndexUtils.STRIDE_MODE_PREFIX_SIZE;
            int totalValueCount = Unsafe.getUnsafe().getInt(countsAddr + (long) localKey * Integer.BYTES);
            long offsetsBase = countsAddr + (long) ks * Integer.BYTES;
            int dataOffset = Unsafe.getUnsafe().getInt(offsetsBase + (long) localKey * Integer.BYTES);
            int bpHeaderSize = PostingsIndexUtils.strideBPHeaderSize(ks);
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

            int idx = PostingsIndexUtils.binarySearchKeyId(genAddr, activeKeyCount, requestedKey);
            if (idx < 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = -1;
                this.blockBufferPos = -1;
                return;
            }

            this.packedMode = false;

            int headerSize = PostingsIndexUtils.genHeaderSizeSparse(activeKeyCount);
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
