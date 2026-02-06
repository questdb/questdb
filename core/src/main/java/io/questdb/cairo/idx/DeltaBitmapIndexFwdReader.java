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
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Transient;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Forward reader for delta-encoded bitmap index with linked blocks.
 * Uses streaming decode for memory efficiency, following block chains.
 */
public class DeltaBitmapIndexFwdReader implements BitmapIndexReader {
    private static final String INDEX_CORRUPT = "cursor could not consistently read index header [corrupt?]";
    private static final Log LOG = LogFactory.getLog(DeltaBitmapIndexFwdReader.class);

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

    public DeltaBitmapIndexFwdReader(
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
            // Return nulls first, then actual index values
            final NullCursor nc = getNullCursor(cachedInstance);
            nc.nullPos = minValue;
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
     * Forward cursor with streaming delta decode using linked blocks.
     */
    private class Cursor implements RowCursor {
        protected long next;
        protected long totalValueCount;
        private final long[] decodeResult = new long[2];
        private int blockCount;          // values in current block
        private long blockDataEnd;       // end of data area in current block
        private long blockDataOffset;    // current read position in block data
        private long currentBlockOffset; // offset of current block
        private long currentValue;
        private long maxValue;
        private long minValue;
        private long nextBlockOffset;    // offset of next block (-1 if none)
        private int positionInBlock;     // position within current block
        private long totalPosition;      // position across all blocks

        @Override
        public boolean hasNext() {
            final long memSize = valueMemSize;

            while (totalPosition < totalValueCount) {
                // Check if we need to move to next block
                if (positionInBlock >= blockCount) {
                    if (!moveToNextBlock()) {
                        return false;
                    }
                }

                long value;
                if (positionInBlock == 0) {
                    // First value in block is stored in header
                    if (currentBlockOffset < 0 || currentBlockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_FIRST_VALUE + 8 > memSize) {
                        return false;
                    }
                    value = valueMem.getLong(currentBlockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_FIRST_VALUE);
                } else {
                    // Subsequent values are delta-encoded
                    if (blockDataOffset >= blockDataEnd || blockDataOffset >= memSize) {
                        return false;
                    }

                    // Peek at first byte to check bounds
                    int firstByte = valueMem.getByte(blockDataOffset) & 0xFF;
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

                    if (blockDataOffset + bytesNeeded > memSize) {
                        return false;
                    }

                    DeltaBitmapIndexUtils.decodeDelta(valueMem, blockDataOffset, decodeResult);
                    value = currentValue + decodeResult[0];
                    blockDataOffset += decodeResult[1];
                }

                currentValue = value;
                positionInBlock++;
                totalPosition++;

                if (value > maxValue) {
                    totalValueCount = 0;
                    return false;
                }

                if (value >= minValue) {
                    this.next = value;
                    return true;
                }
            }
            return false;
        }

        @Override
        public long next() {
            return next - minValue;
        }

        void of(int key, long minValue, long maxValue, long keyCount) {
            if (keyCount == 0) {
                totalValueCount = 0;
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

            this.totalValueCount = valueCount;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.totalPosition = 0;

            if (valueCount > 0 && firstBlockOffset >= 0) {
                valueMem.extend(valueMemSize);
                initBlock(firstBlockOffset);
            } else {
                this.currentBlockOffset = -1;
                this.blockCount = 0;
                this.positionInBlock = 0;
            }
        }

        private void initBlock(long blockOffset) {
            // Bounds check
            if (blockOffset < 0 || blockOffset + DeltaBitmapIndexUtils.BLOCK_HEADER_SIZE > valueMemSize) {
                this.currentBlockOffset = -1;
                this.blockCount = 0;
                this.positionInBlock = 0;
                this.nextBlockOffset = -1;
                return;
            }

            this.currentBlockOffset = blockOffset;
            this.blockCount = valueMem.getInt(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_COUNT);
            int dataLen = valueMem.getInt(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_DATA_LEN);
            this.blockDataOffset = blockOffset + DeltaBitmapIndexUtils.BLOCK_HEADER_SIZE;
            this.blockDataEnd = this.blockDataOffset + dataLen;
            // Ensure dataEnd doesn't exceed valueMemSize
            if (this.blockDataEnd > valueMemSize) {
                this.blockDataEnd = valueMemSize;
            }
            this.nextBlockOffset = valueMem.getLong(blockOffset + DeltaBitmapIndexUtils.BLOCK_OFFSET_NEXT);
            this.positionInBlock = 0;
        }

        private boolean moveToNextBlock() {
            if (nextBlockOffset < 0 || nextBlockOffset >= valueMemSize) {
                return false;
            }
            initBlock(nextBlockOffset);
            return blockCount > 0;
        }
    }

    /**
     * Cursor that returns nulls (for columnTop) before actual values.
     */
    private class NullCursor extends Cursor {
        private long nullCount;
        private long nullPos;

        @Override
        public boolean hasNext() {
            if (nullPos < nullCount) {
                next = nullPos++;
                return true;
            }
            return super.hasNext();
        }
    }
}
