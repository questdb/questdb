/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class BitmapIndexWriter implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(BitmapIndexWriter.class);
    private static final long MAX_VALUE_OFFSET = 37L;
    private final CairoConfiguration configuration;
    private final Cursor cursor = new Cursor();
    private final FilesFacade ff;
    private final MemoryMARW keyMem = Vm.getMARWInstance();
    private final MemoryMARW valueMem = Vm.getMARWInstance();
    private int blockCapacity;
    private int blockValueCountMod;
    private int keyCount = -1;
    private long seekValueBlockOffset;
    private long seekValueCount;
    private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seek;
    private long valueMemSize = -1;

    @TestOnly
    public BitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name, long columnNameTxn) {
        this(configuration);
        of(path, name, columnNameTxn);
    }

    public BitmapIndexWriter(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
    }

    public static void initKeyMemory(MemoryMA keyMem, int blockValueCount) {
        // block value count must be power of 2
        assert blockValueCount == Numbers.ceilPow2(blockValueCount);
        keyMem.jumpTo(0);
        keyMem.truncate();
        keyMem.putByte(BitmapIndexUtils.SIGNATURE);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE MEM SIZE
        keyMem.putInt(blockValueCount); // BLOCK VALUE COUNT
        keyMem.putLong(0); // KEY COUNT
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE CHECK
        assert keyMem.getAppendOffset() == MAX_VALUE_OFFSET;
        keyMem.putLong(-1); // maxRow. It's inclusive, -1 means no rows
        keyMem.skip(BitmapIndexUtils.KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    /**
     * Adds key-value pair to index. If key already exists, value is appended to end of list of existing values. Otherwise,
     * new value list is associated with the key.
     * <p>
     * Index is updated atomically as far as concurrent reading is concerned. Please refer to notes on classes that
     * are responsible for reading bitmap indexes, such as {@link BitmapIndexBwdReader}.
     *
     * @param key   int key
     * @param value long value
     */
    public void add(int key, long value) {
        assert key > -1 : "key must be positive integer: " + key;
        final long offset = BitmapIndexUtils.getKeyEntryOffset(key);
        if (key < keyCount) {
            // when key exists we have possible outcomes in regard to the values
            // 1. last value block has space if value cell index is not the last in block
            // 2. value block is full, and we have to allocate a new one
            // 3. value count is 0. This means key was created as byproduct of adding sparse key value
            // second option is supposed to be less likely because we attempt to
            // configure block capacity to accommodate as many values as possible
            long valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
            long valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
            int valueCellIndex = (int) (valueCount & blockValueCountMod);
            if (valueCellIndex > 0) {
                // this is scenario #1: key exists and there is space in last block to add value
                // we don't need to allocate new block, just add value and update value count on key
                assert valueBlockOffset + blockCapacity <= valueMemSize;
                appendValue(offset, valueBlockOffset, valueCount, valueCellIndex, value);
            } else if (valueCount == 0) {
                // this is scenario #3: we are effectively adding a new key and creating new block
                initValueBlockAndStoreValue(offset, value);
            } else {
                // this is scenario #2: key exists but last block is full. We need to create new block and add value there
                assert valueBlockOffset + blockCapacity <= valueMemSize;
                addValueBlockAndStoreValue(offset, valueBlockOffset, valueCount, value);
            }
        } else {
            // This is a new key. Because index can have sparse keys whenever we think "key exists" we must deal
            // with holes left by this branch, which allocates new key. All key entries that have been
            // skipped during creation of new key will have been initialized with zeroes. This includes counts and
            // block offsets.
            initValueBlockAndStoreValue(offset, value);
            // here we also need to update key count
            // we don't just increment key count, in case this addition creates sparse key set
            updateKeyCount(key);
        }
    }

    @Override
    public void clear() {
        close();
    }

    @Override
    public void close() {
        if (keyMem.isOpen()) {
            if (keyCount > -1) {
                keyMem.setSize(keyMemSize());
            }
            Misc.free(keyMem);
        }

        if (valueMem.isOpen()) {
            if (valueMemSize > -1) {
                valueMem.setSize(valueMemSize);
            }
            Misc.free(valueMem);
        }
    }

    public void commit() {
        int commitMode = configuration.getCommitMode();
        if (commitMode != CommitMode.NOSYNC) {
            sync(commitMode == CommitMode.ASYNC);
        }
    }

    public RowCursor getCursor(int key) {
        if (key < keyCount) {
            cursor.of(key);
            return cursor;
        }
        return EmptyRowCursor.INSTANCE;
    }

    public int getKeyCount() {
        return keyCount;
    }

    public long getMaxValue() {
        return keyMem.getLong(MAX_VALUE_OFFSET);
    }

    @TestOnly
    public long getValueMemSize() {
        return valueMemSize;
    }

    public boolean isOpen() {
        return keyMem.isOpen();
    }

    public final void of(CairoConfiguration configuration, int keyFd, int valueFd, boolean init, int indexBlockCapacity) {
        close();
        final FilesFacade ff = configuration.getFilesFacade();
        boolean kFdUnassigned = true;
        boolean vFdUnassigned = true;
        final long keyAppendPageSize = configuration.getDataIndexKeyAppendPageSize();
        final long valueAppendPageSize = configuration.getDataIndexValueAppendPageSize();
        try {
            if (init) {
                if (ff.truncate(keyFd, 0)) {
                    kFdUnassigned = false;
                    this.keyMem.of(ff, keyFd, null, keyAppendPageSize, keyAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    initKeyMemory(this.keyMem, indexBlockCapacity);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(keyFd).put(']');
                }
            } else {
                kFdUnassigned = false;
                this.keyMem.of(ff, keyFd, null, ff.length(keyFd), MemoryTag.MMAP_INDEX_WRITER);
            }
            long keyMemSize = this.keyMem.getAppendOffset();
            // check if key file header is present
            if (keyMemSize < BitmapIndexUtils.KEY_FILE_RESERVED) {
                // Don't truncate the file on close.
                this.keyMem.close(false);
                LOG.error().$("file too short [corrupt] [fd=").$(keyFd).$(']').$();
                throw CairoException.critical(0).put("Index file too short (w): [fd=").put(keyFd).put(']');
            }

            // verify header signature
            if (this.keyMem.getByte(BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != BitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] [fd=").$(keyFd).$(']').$();
                throw CairoException.critical(0).put("Unknown format: [fd=").put(keyFd).put(']');
            }

            // verify key count
            this.keyCount = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
            if (keyMemSize < keyMemSize()) {
                LOG.error().$("key count does not match file length [corrupt] [fd=").$(keyFd).$(", keyCount=").$(this.keyCount).$(']').$();
                throw CairoException.critical(0).put("Key count does not match file length [fd=").put(keyFd).put(']');
            }

            // check if sequence is intact
            if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) != this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                LOG.error().$("sequence mismatch [corrupt] at [fd=").$(keyFd).$(']').$();
                throw CairoException.critical(0).put("Sequence mismatch [fd=").put(keyFd).put(']');
            }

            this.valueMemSize = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

            if (init) {
                if (ff.truncate(valueFd, 0)) {
                    vFdUnassigned = false;
                    this.valueMem.of(ff, valueFd, null, valueAppendPageSize, valueAppendPageSize, MemoryTag.MMAP_INDEX_WRITER);
                    this.valueMem.jumpTo(0);
                } else {
                    throw CairoException.critical(ff.errno()).put("Could not truncate [fd=").put(valueFd).put(']');
                }
            } else {
                vFdUnassigned = false;
                this.valueMem.of(ff, valueFd, null, valueAppendPageSize, this.valueMemSize, MemoryTag.MMAP_INDEX_WRITER);
            }

            // block value count is always a power of two
            // to calculate remainder we use faster 'x & (count-1)', which is equivalent to (x % count)
            this.blockValueCountMod = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
            assert blockValueCountMod > 0;
            this.blockCapacity = (this.blockValueCountMod + 1) * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
        } catch (Throwable e) {
            this.close();
            if (kFdUnassigned) {
                ff.close(keyFd);
            }
            if (vFdUnassigned) {
                ff.close(valueFd);
            }
            throw e;
        }
    }

    public final void of(Path path, CharSequence name, long columnNameTxn) {
        of(path, name, columnNameTxn, 0);
    }

    public final void of(Path path, CharSequence name, long columnNameTxn, int indexBlockCapacity) {
        close();
        final int plen = path.length();
        try {
            boolean init = indexBlockCapacity > 0;
            BitmapIndexUtils.keyFileName(path, name, columnNameTxn);
            if (init) {
                this.keyMem.of(ff, path, configuration.getDataIndexKeyAppendPageSize(), 0L, MemoryTag.MMAP_INDEX_WRITER);
                initKeyMemory(this.keyMem, indexBlockCapacity);
            } else {
                boolean exists = ff.exists(path);
                if (!exists) {
                    LOG.error().$(path).$(" not found").$();
                    throw CairoException.critical(0).put("Index does not exist: ").put(path);
                }
                this.keyMem.of(ff, path, configuration.getDataIndexKeyAppendPageSize(), ff.length(path), MemoryTag.MMAP_INDEX_WRITER);
            }

            long keyMemSize = this.keyMem.getAppendOffset();
            // check if key file header is present
            if (keyMemSize < BitmapIndexUtils.KEY_FILE_RESERVED) {
                LOG.error().$("file too short [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Index file too short (w): ").put(path);
            }

            // verify header signature
            if (this.keyMem.getByte(BitmapIndexUtils.KEY_RESERVED_OFFSET_SIGNATURE) != BitmapIndexUtils.SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.critical(0).put("Unknown format: ").put(path);
            }

            // verify key count
            this.keyCount = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT);
            if (keyMemSize < keyMemSize()) {
                LOG.error().$("key count does not match file length [corrupt] of ").$(path).$(" [keyCount=").$(this.keyCount).$(']').$();
                throw CairoException.critical(0).put("Key count does not match file length of ").put(path);
            }

            // check if sequence is intact
            if (this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK) != this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE)) {
                LOG.error().$("sequence mismatch [corrupt] at ").$(path).$();
                throw CairoException.critical(0).put("Sequence mismatch on ").put(path);
            }

            this.valueMemSize = this.keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            this.valueMem.of(
                    ff,
                    BitmapIndexUtils.valueFileName(path.trimTo(plen), name, columnNameTxn),
                    configuration.getDataIndexValueAppendPageSize(),
                    this.valueMemSize,
                    MemoryTag.MMAP_INDEX_WRITER
            );

            if (init) {
                assert valueMemSize == 0;
                this.valueMem.truncate();
            }

            // block value count is always a power of two
            // to calculate remainder we use faster 'x & (count-1)', which is equivalent to (x % count)
            this.blockValueCountMod = this.keyMem.getInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
            if (blockValueCountMod < 1) {
                LOG.error()
                        .$("corrupt file [name=").$(path)
                        .$(", valueMemSize=").$(this.valueMemSize)
                        .$(", blockValueCountMod=").$(this.blockValueCountMod)
                        .I$();
                throw CairoException.critical(0).put("corrupt file ").put(path);
            }
            this.blockCapacity = (this.blockValueCountMod + 1) * 8 + BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED;
        } catch (Throwable e) {
            this.close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    public void rollbackConditionally(long row) {
        final long currentMaxRow;
        if (row >= 0 && ((currentMaxRow = getMaxValue()) < 1 || currentMaxRow >= row)) {
            if (row == 0) {
                truncate();
            } else {
                rollbackValues(row - 1);
            }
        }
    }

    /**
     * Rolls values back. Remove values that are strictly greater than given maximum. Empty value blocks
     * will also be removed as well as blank space at end of value memory.
     *
     * @param maxValue maximum value allowed in index.
     */
    public void rollbackValues(long maxValue) {

        long maxValueBlockOffset = 0;
        for (int k = 0; k < keyCount; k++) {
            long offset = BitmapIndexUtils.getKeyEntryOffset(k);
            long valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

            // do we have anything for the key?
            if (valueCount > 0) {
                long blockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
                BitmapIndexUtils.seekValueBlockRTL(valueCount, blockOffset, valueMem, maxValue, blockValueCountMod, SEEKER);

                if (valueCount != seekValueCount || blockOffset != seekValueBlockOffset) {
                    // set new value count
                    keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT, seekValueCount);

                    if (blockOffset != seekValueBlockOffset) {
                        Unsafe.getUnsafe().storeFence();
                        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET, seekValueBlockOffset);
                        Unsafe.getUnsafe().storeFence();
                    }
                    keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, seekValueCount);
                }

                if (seekValueBlockOffset > maxValueBlockOffset) {
                    maxValueBlockOffset = seekValueBlockOffset;
                }
            }
        }
        valueMemSize = maxValueBlockOffset + blockCapacity;
        updateValueMemSize();
        setMaxValue(maxValue);
    }

    public void setMaxValue(long maxValue) {
        keyMem.putLong(MAX_VALUE_OFFSET, maxValue);
    }

    public void sync(boolean async) {
        keyMem.sync(async);
        valueMem.sync(async);
    }

    public void truncate() {
        initKeyMemory(keyMem, blockValueCountMod + 1);
        valueMem.truncate();
        keyCount = 0;
        valueMemSize = 0;
    }

    private void addValueBlockAndStoreValue(long offset, long valueBlockOffset, long valueCount, long value) {
        long newValueBlockOffset = allocateValueBlockAndStore(value);

        // update block linkage before we increase count
        // this is important to index readers, which will act on value count they read

        // we subtract 8 because we have just written long value
        // update this block reference to previous block
        valueMem.putLong(valueMemSize - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED, valueBlockOffset);

        // update previous block' "next" block reference to this block
        valueMem.putLong(valueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + 8, newValueBlockOffset);

        // update count and last value block offset for the key
        // in atomic fashion
        // we make sure count is always written _after_ new value block is added
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(offset, valueCount + 1);
        Unsafe.getUnsafe().storeFence();

        // don't set first block offset here
        // it would have been done when this key was first created

        // write last block offset because it changed in this scenario
        assert newValueBlockOffset < valueMemSize;
        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET, newValueBlockOffset);
        Unsafe.getUnsafe().storeFence();

        // write count check
        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, valueCount + 1);
        Unsafe.getUnsafe().storeFence();

        // we are done adding value to new block of values
    }

    private long allocateValueBlockAndStore(long value) {
        long newValueBlockOffset = valueMemSize;

        // store our value
        valueMem.putLong(newValueBlockOffset, value);

        // reserve memory for value block
        valueMem.skip(blockCapacity);

        // make sure we change value memory size after jump was successful
        valueMemSize += blockCapacity;

        // must update value mem size in key memory header
        // so that index can be opened correctly next time it loads
        updateValueMemSize();
        return newValueBlockOffset;
    }

    private void appendValue(long offset, long valueBlockOffset, long valueCount, int valueCellIndex, long value) {
        // first set value
        valueMem.putLong(valueBlockOffset + valueCellIndex * 8L, value);
        Unsafe.getUnsafe().storeFence();
        // update count and last value block offset for the key
        // in atomic fashion
        keyMem.putLong(offset, valueCount + 1);
        // write count check
        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, valueCount + 1);
    }

    private void initValueBlockAndStoreValue(long offset, long value) {
        long newValueBlockOffset = allocateValueBlockAndStore(value);

        // don't need to update linkage, value count is less than block size
        // index readers must not access linkage information in this case

        // now update key entry in atomic fashion
        // update count and last value block offset for the key
        // in atomic fashion
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(offset, 1);
        Unsafe.getUnsafe().storeFence();

        // first and last blocks are the same
        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET, newValueBlockOffset);
        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET, newValueBlockOffset);
        Unsafe.getUnsafe().storeFence();

        // write count check
        keyMem.putLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK, 1);
        Unsafe.getUnsafe().storeFence();
    }

    private long keyMemSize() {
        return this.keyCount * BitmapIndexUtils.KEY_ENTRY_SIZE + BitmapIndexUtils.KEY_FILE_RESERVED;
    }

    private void seek(long count, long offset) {
        this.seekValueCount = count;
        this.seekValueBlockOffset = offset;
    }

    private void updateValueMemSize() {
        long seq = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    void updateKeyCount(int key) {
        keyCount = key + 1;

        // also write key count to header of key memory
        long seq = keyMem.getLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(BitmapIndexUtils.KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(BitmapIndexUtils.KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private class Cursor implements RowCursor {
        private long valueBlockOffset;
        private long valueCount;

        @Override
        public boolean hasNext() {
            return valueCount > 0;
        }

        @Override
        public long next() {
            long cellIndex = getValueCellIndex(--valueCount);
            long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);
            if (cellIndex == 0 && valueCount > 0) {
                // we are at edge of block right now, next value will be in previous block
                jumpToPreviousValueBlock();
            }
            return result;
        }

        private long getPreviousBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            valueBlockOffset = getPreviousBlock(valueBlockOffset);
        }

        void of(int key) {
            assert key > -1 : "key must be positive integer: " + key;
            long offset = BitmapIndexUtils.getKeyEntryOffset(key);
            this.valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);
            assert valueCount > -1;
            this.valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
        }
    }
}
