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

import io.questdb.cairo.*;
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
 * Backward reader for delta-encoded bitmap index with linked blocks.
 * Uses buffered decode (decodes all values into LongList for reverse iteration).
 */
public class DeltaBitmapIndexBwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(DeltaBitmapIndexBwdReader.class);

    protected final MemoryMR keyMem = Vm.getCMRInstance();
    protected final MemoryMR valueMem = Vm.getCMRInstance();
    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();
    protected MillisecondClock clock;
    protected long columnTop;
    protected int keyCount;
    protected long spinLockTimeoutMs;
    private int blockCapacity;
    private long columnTxn;
    private int keyCountIncludingNulls;
    private long keyFileSequence = -1;
    private long partitionTxn;
    private long valueMemSize = -1;

    public DeltaBitmapIndexBwdReader() {
    }

    public DeltaBitmapIndexBwdReader(
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
        assert minValue <= maxValue;

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            // Return actual index values first (in reverse), then nulls
            final NullCursor nc = getNullCursor(cachedInstance);
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nc.nullCount = Math.min(columnTop, hi);
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
        // Delta-encoded index doesn't use fixed block capacity
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
            LPSZ name = DeltaBitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            keyMem.of(
                    ff,
                    name,
                    ff.getMapPageSize(),
                    DeltaBitmapIndexUtils.getKeyEntryOffset(0),
                    MemoryTag.MMAP_INDEX_READER,
                    CairoConfiguration.O_NONE,
                    -1
            );
            this.clock = configuration.getMillisecondClock();

            if (keyMem.getByte(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != DeltaBitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            readIndexMetadataAtomically();

            this.valueMem.of(
                    configuration.getFilesFacade(),
                    DeltaBitmapIndexUtils.valueFileName(path.trimTo(plen), columnName, columnNameTxn),
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
        long seq = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK);
        if (seq != keyFileSequence) {
            readIndexMetadataAtomically();
            this.keyMem.extend(DeltaBitmapIndexUtils.getKeyEntryOffset(keyCount));
            this.valueMem.extend(valueMemSize);
        }
    }

    public void updateKeyCount() {
        int keyCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            long seq = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (seq == keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
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
            keyMem.extend(DeltaBitmapIndexUtils.getKeyEntryOffset(keyCount));
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
            long seq = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE);
            int keyCount;
            long valueMemSize;
            int blockCapacity;

            Unsafe.getUnsafe().loadFence();
            if (keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) == seq) {
                keyCount = keyMem.getInt(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
                valueMemSize = keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                blockCapacity = keyMem.getInt(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_CAPACITY);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(DeltaBitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) == seq) {
                    this.keyFileSequence = seq;
                    this.valueMemSize = valueMemSize;
                    this.blockCapacity = blockCapacity;
                    this.keyCount = keyCount;
                    this.keyCountIncludingNulls = columnTop > 0 ? keyCount + 1 : keyCount;
                    keyMem.extend(DeltaBitmapIndexUtils.getKeyEntryOffset(keyCount));
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
     * Backward cursor with buffered decode using linked blocks.
     * Decodes all values into a LongList and iterates in reverse.
     * Uses Unsafe for fast direct memory access.
     */
    private class Cursor implements RowCursor {
        private final LongList values = new LongList();
        protected long minValue;
        protected long next;
        private int position;

        @Override
        public boolean hasNext() {
            while (position >= 0) {
                long value = values.getQuick(position--);
                if (value >= minValue) {
                    this.next = value;
                    return true;
                }
                // value < minValue, no more valid values since list is ordered
                return false;
            }
            return false;
        }

        @Override
        public long next() {
            return next - minValue;
        }

        /**
         * Decode all values from a block chain into the values list.
         */
        private void decodeAllBlocks(long firstBlockOffset, long maxValue, long memSize) {
            long blockOffset = firstBlockOffset;

            while (blockOffset >= 0) {
                // Ensure block header is within mapped memory
                if (blockOffset < 0 || blockOffset + DeltaBitmapIndexUtils.BLOCK_HEADER_SIZE > memSize) {
                    break;
                }

                int blockCount = valueMem.getInt(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_COUNT);
                int dataLen = valueMem.getInt(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_DATA_LEN);
                long firstValue = valueMem.getLong(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_FIRST_VALUE);
                long nextBlock = valueMem.getLong(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_NEXT);

                // Sanity checks
                if (blockCount <= 0 || dataLen < 0) {
                    break;
                }

                // First value in block is stored in header
                long value = firstValue;
                if (value > maxValue) {
                    break;
                }
                values.add(value);

                // Decode remaining values from delta data
                long dataOffset = blockOffset + DeltaBitmapIndexUtils.BLOCK_HEADER_SIZE;
                long dataEnd = dataOffset + dataLen;
                int count = 1;

                // Bounds check for data area - use actual memory size
                if (dataEnd > memSize) {
                    dataEnd = memSize;
                }

                while (dataOffset < dataEnd && count < blockCount) {
                    // Ensure we have at least 1 byte to read
                    if (dataOffset >= memSize) {
                        break;
                    }

                    // Peek at first byte to determine how many bytes we need
                    int firstByte = valueMem.getByte(dataOffset) & 0xFF;
                    int bytesNeeded;
                    if ((firstByte & 0x80) == 0) {
                        bytesNeeded = 1;
                    } else if ((firstByte & 0xC0) == 0x80) {
                        bytesNeeded = 2;
                    } else if ((firstByte & 0xE0) == 0xC0) {
                        bytesNeeded = 4;
                    } else {
                        bytesNeeded = 9;
                    }

                    // Ensure we have enough bytes
                    if (dataOffset + bytesNeeded > memSize) {
                        break;
                    }

                    long[] result = new long[2];
                    DeltaBitmapIndexUtils.decodeDelta(valueMem, dataOffset, result);
                    value += result[0];
                    dataOffset += result[1];
                    count++;

                    if (value > maxValue) {
                        return; // Stop decoding, we've passed maxValue
                    }
                    values.add(value);
                }

                // Move to next block
                blockOffset = nextBlock;
            }
        }

        void of(int key, long minValue, long maxValue, long keyCount) {
            values.clear();

            if (keyCount == 0) {
                position = -1;
                return;
            }

            assert key >= 0 : "key must be non-negative: " + key;
            long offset = DeltaBitmapIndexUtils.getKeyEntryOffset(key);
            keyMem.extend(offset + DeltaBitmapIndexUtils.KEY_ENTRY_SIZE);

            long valueCount;
            long firstBlockOffset;
            final long deadline = clock.getTicks() + spinLockTimeoutMs;

            while (true) {
                valueCount = keyMem.getLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                Unsafe.getUnsafe().loadFence();
                int countCheck = keyMem.getInt(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK);
                if (countCheck == (int) valueCount) {
                    firstBlockOffset = keyMem.getLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_BLOCK);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(offset + DeltaBitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                        break;
                    }
                }

                if (clock.getTicks() > deadline) {
                    LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, key=").$(key).$(", offset=").$(offset).$(']').$();
                    throw CairoException.critical(0).put(INDEX_CORRUPT);
                }
            }

            if (valueCount > 0 && firstBlockOffset >= 0) {
                valueMem.extend(valueMemSize);
                decodeAllBlocks(firstBlockOffset, maxValue, valueMemSize);
            }

            this.minValue = minValue;
            this.position = values.size() - 1;
        }
    }

    /**
     * Cursor that returns actual values first (in reverse), then nulls (in reverse).
     */
    private class NullCursor extends Cursor {
        private long nullCount;

        @Override
        public boolean hasNext() {
            if (super.hasNext()) {
                return true;
            }

            if (--nullCount >= minValue) {
                next = nullCount;
                return true;
            }
            return false;
        }
    }
}
