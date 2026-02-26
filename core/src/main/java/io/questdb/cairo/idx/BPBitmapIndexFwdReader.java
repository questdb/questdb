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
 * Generation iteration follows the same pattern as FSSTBitmapIndexFwdReader.
 */
public class BPBitmapIndexFwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(BPBitmapIndexFwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
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

    public BPBitmapIndexFwdReader(
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
            LPSZ name = BPBitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    BPBitmapIndexUtils.KEY_FILE_RESERVED,
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            if (keyMem.getByte(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != BPBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    BPBitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
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
        long seq = keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            long keyFileSize = BPBitmapIndexUtils.getGenDirOffset(genCount);
            this.keyMem.extend(keyFileSize);
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
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
            this.genCount = keyMem.getInt(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);
            long keyFileSize = BPBitmapIndexUtils.getGenDirOffset(genCount);
            keyMem.extend(keyFileSize);
        }
    }

    private void readIndexMetadataAtomically() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                int keyCount = keyMem.getInt(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                long valueMemSize = keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                int genCount = keyMem.getInt(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_GEN_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(BPBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.genCount = genCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;

                    long keyFileSize = BPBitmapIndexUtils.getGenDirOffset(genCount);
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
        private final long[] blockBuffer = new long[BPBitmapIndexUtils.BLOCK_CAPACITY];
        private final long[] blockDeltas = new long[BPBitmapIndexUtils.BLOCK_CAPACITY];
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
        private int valuesEmitted;
        // Block metadata arrays (pre-allocated, grown as needed)
        private int metadataCapacity;
        private int[] valueCounts = new int[4];
        private long[] firstValues = new long[4];
        private long[] minDeltas = new long[4];
        private int[] bitWidths = new int[4];
        private long packedDataAddr;

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
                    valuesEmitted++;
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

                // Move to next generation
                currentGen++;
                if (currentGen >= genCount) {
                    return false;
                }
                loadGeneration();
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

            this.requestedKey = key;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.currentGen = 0;
            this.valuesEmitted = 0;
            loadGeneration();
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

        private void ensureMetadataCapacity(int needed) {
            if (needed > metadataCapacity) {
                metadataCapacity = Math.max(needed, metadataCapacity * 2);
                valueCounts = new int[metadataCapacity];
                firstValues = new long[metadataCapacity];
                minDeltas = new long[metadataCapacity];
                bitWidths = new int[metadataCapacity];
            }
        }

        private void loadGeneration() {
            long dirOffset = BPBitmapIndexUtils.getGenDirOffset(currentGen);
            keyMem.extend(dirOffset + BPBitmapIndexUtils.GEN_DIR_ENTRY_SIZE);
            long genFileOffset = keyMem.getLong(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_FILE_OFFSET);
            int genDataSize = keyMem.getInt(dirOffset + BPBitmapIndexUtils.GEN_DIR_OFFSET_SIZE);

            valueMem.extend(genFileOffset + genDataSize);
            long genAddr = valueMem.addressOf(genFileOffset);
            int headerSize = BPBitmapIndexUtils.genHeaderSize(keyCount);

            this.totalValueCount = Unsafe.getUnsafe().getInt(genAddr + (long) requestedKey * Integer.BYTES);
            if (totalValueCount == 0) {
                this.encodedBlockCount = 0;
                this.currentBlock = 0;
                this.blockBufferPos = 0;
                this.blockBufferEnd = 0;
                return;
            }

            int dataOffset = Unsafe.getUnsafe().getInt(genAddr + (long) keyCount * Integer.BYTES + (long) requestedKey * Integer.BYTES);
            this.encodedAddr = genAddr + headerSize + dataOffset;

            // Read block metadata from encoded data (reuse pre-allocated arrays)
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
            this.valuesEmitted = 0;
        }
    }
}
