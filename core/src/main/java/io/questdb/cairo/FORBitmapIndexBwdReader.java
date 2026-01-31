/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \|_| |_| | |_) |
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

package io.questdb.cairo;

import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Backward reader for Frame of Reference (FOR) bitmap index.
 * Reads values block by block in reverse order.
 */
public class FORBitmapIndexBwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(FORBitmapIndexBwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();
    protected MillisecondClock clock;
    protected long columnTop;
    protected int keyCount;
    protected long spinLockTimeoutMs;
    private long columnTxn;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private long valueMemSize = -1;

    public FORBitmapIndexBwdReader() {
    }

    public FORBitmapIndexBwdReader(
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

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            // Return actual index values first (in reverse), then nulls
            final NullCursor nc = getNullCursor(cachedInstance);
            nc.nullPos = Math.min(columnTop, maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1) - 1;
            nc.nullMin = minValue;
            nc.of(key, minValue, maxValue, keyCount);
            return nc;
        }

        if (key < keyCount) {
            final Cursor c = getCursor(cachedInstance);
            c.of(key, minValue, maxValue, keyCount);
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
        return FORBitmapIndexUtils.BLOCK_CAPACITY;
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
            LPSZ name = FORBitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    FORBitmapIndexUtils.getKeyEntryOffset(0),
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            if (keyMem.getByte(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != FORBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    FORBitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
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
        long seq = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            this.keyMem.extend(FORBitmapIndexUtils.getKeyEntryOffset(keyCount));
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
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
            keyMem.extend(FORBitmapIndexUtils.getKeyEntryOffset(keyCount));
        }
    }

    private Cursor getCursor(boolean cachedInstance) {
        return cachedInstance ? cursor : new Cursor();
    }

    private NullCursor getNullCursor(boolean cachedInstance) {
        return cachedInstance ? nullCursor : new NullCursor();
    }

    private void readIndexMetadataAtomically() {
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            int keyCount;
            long valueMemSize;

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                valueMemSize = keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(FORBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.keyCount = keyCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    keyMem.extend(FORBitmapIndexUtils.getKeyEntryOffset(keyCount));
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

    /**
     * Backward cursor that reads FOR blocks and iterates in reverse.
     * Buffers all values in memory for reverse iteration.
     */
    private class Cursor implements RowCursor {
        // Buffered values for reverse iteration
        private final LongList allValues = new LongList();
        // Output
        protected long next;
        private int currentIndex;
        // Query range
        private long minValue;

        @Override
        public boolean hasNext() {
            while (currentIndex >= 0) {
                long value = allValues.getQuick(currentIndex--);
                if (value >= minValue) {
                    this.next = value;
                    return true;
                }
            }
            return false;
        }

        @Override
        public long next() {
            return next;
        }

        void of(int key, long minValue, long maxValue, long keyCount) {
            allValues.clear();
            this.minValue = minValue;

            if (keyCount == 0) {
                this.currentIndex = -1;
                return;
            }

            long offset = FORBitmapIndexUtils.getKeyEntryOffset(key);
            keyMem.extend(offset + FORBitmapIndexUtils.KEY_ENTRY_SIZE);

            long valueCount;
            long firstBlockOffset;
            int blockCount;
            final long deadline = clock.getTicks() + spinLockTimeoutMs;

            while (true) {
                valueCount = keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                Unsafe.getUnsafe().loadFence();
                int countCheck = keyMem.getInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK);
                if (countCheck == (int) valueCount) {
                    firstBlockOffset = keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK);
                    blockCount = keyMem.getInt(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_BLOCK_COUNT);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(offset + FORBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                        break;
                    }
                }

                if (clock.getTicks() > deadline) {
                    LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, key=").$(key).$("]").$();
                    throw CairoException.critical(0).put(INDEX_CORRUPT);
                }
            }

            if (valueCount == 0 || blockCount == 0) {
                this.currentIndex = -1;
                return;
            }

            // Read all blocks and collect values within range
            long baseAddress = valueMem.addressOf(0);
            long currentBlockOffset = firstBlockOffset;
            long[] blockValues = new long[FORBitmapIndexUtils.BLOCK_CAPACITY];

            for (int blockIdx = 0; blockIdx < blockCount; blockIdx++) {
                long blockAddr = baseAddress + currentBlockOffset;

                // Read block header
                long blockMinValue = Unsafe.getUnsafe().getLong(blockAddr + FORBitmapIndexUtils.BLOCK_OFFSET_MIN_VALUE);
                int bitWidth = Unsafe.getUnsafe().getByte(blockAddr + FORBitmapIndexUtils.BLOCK_OFFSET_BIT_WIDTH) & 0xFF;
                int count = Unsafe.getUnsafe().getShort(blockAddr + FORBitmapIndexUtils.BLOCK_OFFSET_VALUE_COUNT) & 0xFFFF;

                // Unpack values
                long dataAddr = blockAddr + FORBitmapIndexUtils.BLOCK_OFFSET_DATA;
                FORBitmapIndexUtils.unpackAllValues(dataAddr, count, bitWidth, blockMinValue, blockValues);

                // Add values within range
                for (int i = 0; i < count; i++) {
                    long value = blockValues[i];
                    if (value > maxValue) {
                        // Values are sorted, no need to continue this block
                        break;
                    }
                    if (value >= minValue) {
                        allValues.add(value);
                    }
                }

                // Calculate next block offset
                int packedSize = FORBitmapIndexUtils.packedDataSize(count, bitWidth);
                currentBlockOffset += FORBitmapIndexUtils.BLOCK_HEADER_SIZE + packedSize;
            }

            // Position at the end for reverse iteration
            this.currentIndex = allValues.size() - 1;
        }
    }

    /**
     * Cursor that returns values in reverse, then nulls (for columnTop).
     */
    private class NullCursor extends Cursor {
        private long nullMin;
        private long nullPos;

        @Override
        public boolean hasNext() {
            // First return actual values in reverse
            if (super.hasNext()) {
                return true;
            }
            // Then return nulls in reverse
            if (nullPos >= nullMin) {
                next = nullPos--;
                return true;
            }
            return false;
        }
    }
}
