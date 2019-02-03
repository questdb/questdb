/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.RowCursor;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Misc;
import com.questdb.std.Numbers;
import com.questdb.std.Unsafe;
import com.questdb.std.str.Path;

import java.io.Closeable;

import static com.questdb.cairo.BitmapIndexUtils.*;

public class BitmapIndexWriter implements Closeable {
    private static final Log LOG = LogFactory.getLog(BitmapIndexWriter.class);
    private final ReadWriteMemory keyMem = new ReadWriteMemory();
    private final ReadWriteMemory valueMem = new ReadWriteMemory();
    private final Cursor cursor = new Cursor();
    private int blockCapacity;
    private int blockValueCountMod;
    private long valueMemSize = -1;
    private int keyCount = -1;
    private long seekValueCount;
    private long seekValueBlockOffset;
    private final ValueBlockSeeker SEEKER = this::seek;

    public BitmapIndexWriter(CairoConfiguration configuration, Path path, CharSequence name) {
        of(configuration, path, name);
    }

    public BitmapIndexWriter() {
    }

    public static void initKeyMemory(VirtualMemory keyMem, int blockValueCount) {

        // block value count must be power of 2
        assert blockValueCount == Numbers.ceilPow2(blockValueCount);

        keyMem.putByte(SIGNATURE);
        keyMem.putLong(1); // SEQUENCE
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(0); // VALUE MEM SIZE
        keyMem.putInt(blockValueCount); // BLOCK VALUE COUNT
        keyMem.putLong(0); // KEY COUNT
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(1); // SEQUENCE CHECK
        keyMem.skip(KEY_FILE_RESERVED - keyMem.getAppendOffset());
    }

    /**
     * Adds key-value pair to index. If key already exists, value is appended to end of list of existing values. Otherwise
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
        final long offset = getKeyEntryOffset(key);
        if (key < keyCount) {
            // when key exists we have possible outcomes with regards to values
            // 1. last value block has space if value cell index is not the last in block
            // 2. value block is full and we have to allocate a new one
            // 3. value count is 0. This means key was created as byproduct of adding sparse key value
            // second option is supposed to be less likely because we attempt to
            // configure block capacity to accommodate as many values as possible
            long valueBlockOffset = keyMem.getLong(offset + KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
            long valueCount = keyMem.getLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT);
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
    public void close() {
        if (keyMem.isOpen() && keyCount > -1) {
            keyMem.jumpTo(keyMemSize());
        }
        Misc.free(keyMem);

        if (valueMem.isOpen() && valueMemSize > -1) {
            valueMem.jumpTo(valueMemSize);
        }
        Misc.free(valueMem);
    }

    public RowCursor getCursor(int key) {
        if (key < keyCount) {
            cursor.of(key);
            return cursor;
        }
        return EmptyRowCursor.INSTANCE;
    }

    final public void of(CairoConfiguration configuration, Path path, CharSequence name) {
        close();
        long pageSize = configuration.getFilesFacade().getMapPageSize();
        int plen = path.length();

        try {
            boolean exists = configuration.getFilesFacade().exists(keyFileName(path, name));
            this.keyMem.of(configuration.getFilesFacade(), path, pageSize);
            if (!exists) {
                LOG.error().$(path).$(" not found").$();
                throw CairoException.instance(0).put("Index does not exist: ").put(path);
            }

            long keyMemSize = this.keyMem.getAppendOffset();
            // check if key file header is present
            if (keyMemSize < KEY_FILE_RESERVED) {
                LOG.error().$("file too short [corrupt] ").$(path).$();
                throw CairoException.instance(0).put("Index file too short (w): ").put(path);
            }

            // verify header signature
            if (this.keyMem.getByte(KEY_RESERVED_OFFSET_SIGNATURE) != SIGNATURE) {
                LOG.error().$("unknown format [corrupt] ").$(path).$();
                throw CairoException.instance(0).put("Unknown format: ").put(path);
            }

            // verify key count
            this.keyCount = this.keyMem.getInt(KEY_RESERVED_OFFSET_KEY_COUNT);
            if (keyMemSize < keyMemSize()) {
                LOG.error().$("key count does not match file length [corrupt] of ").$(path).$(" [keyCount=").$(this.keyCount).$(']').$();
                throw CairoException.instance(0).put("Key count does not match file length of ").put(path);
            }

            // check if sequence is intact
            if (this.keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK) != this.keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE)) {
                LOG.error().$("sequence mismatch [corrupt] at ").$(path).$();
                throw CairoException.instance(0).put("Sequence mismatch on ").put(path);
            }

            this.valueMem.of(configuration.getFilesFacade(), valueFileName(path.trimTo(plen), name), pageSize);
            this.valueMemSize = this.keyMem.getLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);

            if (this.valueMem.getAppendOffset() < this.valueMemSize) {
                LOG.error().$("incorrect file size [corrupt] of ").$(path).$(" [expected=").$(this.valueMemSize).$(']').$();
                throw CairoException.instance(0).put("Incorrect file size of ").put(path);
            }

            // block value count is always a power of two
            // to calculate remainder we use faster 'x & (count-1)', which is equivalent to (x % count)
            this.blockValueCountMod = this.keyMem.getInt(KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
            assert blockValueCountMod > 0;
            this.blockCapacity = (this.blockValueCountMod + 1) * 8 + VALUE_BLOCK_FILE_RESERVED;
        } catch (CairoException e) {
            this.close();
            throw e;
        } finally {
            path.trimTo(plen);
        }
    }

    /**
     * Rolls values back. Removes values that are strictly greater than given maximum. Empty value blocks
     * will also be removed as well as blank space at end of value memory.
     *
     * @param maxValue maximum value allowed in index.
     */
    public void rollbackValues(long maxValue) {

        long maxValueBlockOffset = 0;
        for (int k = 0; k < keyCount; k++) {
            long offset = getKeyEntryOffset(k);
            long valueCount = keyMem.getLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT);

            // do we have anything for the key?
            if (valueCount > 0) {
                long blockOffset = keyMem.getLong(offset + KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
                seekValueBlockRTL(valueCount, blockOffset, valueMem, maxValue, blockValueCountMod, SEEKER);

                if (valueCount != seekValueCount || blockOffset != seekValueBlockOffset) {
                    // set new value count
                    keyMem.putLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT, seekValueCount);

                    if (blockOffset != seekValueBlockOffset) {
                        Unsafe.getUnsafe().storeFence();
                        keyMem.putLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT + 16, seekValueBlockOffset);
                        Unsafe.getUnsafe().storeFence();
                    }
                    keyMem.putLong(offset + KEY_ENTRY_OFFSET_COUNT_CHECK, seekValueCount);
                }

                if (seekValueBlockOffset > maxValueBlockOffset) {
                    maxValueBlockOffset = seekValueBlockOffset;
                }
            }
        }
        valueMemSize = maxValueBlockOffset + blockCapacity;
        updateValueMemSize();
    }

    private void addValueBlockAndStoreValue(long offset, long valueBlockOffset, long valueCount, long value) {
        long newValueBlockOffset = allocateValueBlockAndStore(value);

        // update block linkage before we increase count
        // this is important to index readers, which will act on value count they read

        // we subtract 8 because we just written long value
        // update this block reference to previous block
        valueMem.putLong(valueMemSize - VALUE_BLOCK_FILE_RESERVED, valueBlockOffset);

        // update previous block' "next" block reference to this block
        valueMem.putLong(valueBlockOffset + blockCapacity - VALUE_BLOCK_FILE_RESERVED + 8, newValueBlockOffset);

        // update count and last value block offset for the key
        // in atomic fashion
        // we make sure count is always written _after_ new value block is added
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(offset, valueCount + 1);
        Unsafe.getUnsafe().storeFence();

        // don't set first block offset here
        // it would have been done when this key was first created

        // write last block offset because it changed in this scenario
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET, newValueBlockOffset);
        Unsafe.getUnsafe().storeFence();

        // write count check
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_COUNT_CHECK, valueCount + 1);
        Unsafe.getUnsafe().storeFence();

        // we are done adding value to new block of values
    }

    private long allocateValueBlockAndStore(long value) {
        long newValueBlockOffset = valueMemSize;

        // store our value
        valueMem.putLong(newValueBlockOffset, value);

        // reserve memory for value block
        valueMem.jumpTo(valueMemSize + blockCapacity);

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
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_COUNT_CHECK, valueCount + 1);
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
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET, newValueBlockOffset);
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET, newValueBlockOffset);
        Unsafe.getUnsafe().storeFence();

        // write count check
        keyMem.putLong(offset + KEY_ENTRY_OFFSET_COUNT_CHECK, 1);
        Unsafe.getUnsafe().storeFence();
    }

    private long keyMemSize() {
        return this.keyCount * KEY_ENTRY_SIZE + KEY_FILE_RESERVED;
    }

    private void seek(long count, long offset) {
        this.seekValueCount = count;
        this.seekValueBlockOffset = offset;
    }

    private void updateKeyCount(int key) {
        keyCount = key + 1;

        // also write key count to header of key memory
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putInt(KEY_RESERVED_OFFSET_KEY_COUNT, keyCount);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
    }

    private void updateValueMemSize() {
        long seq = keyMem.getLong(KEY_RESERVED_OFFSET_SEQUENCE) + 1;
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE, seq);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, valueMemSize);
        Unsafe.getUnsafe().storeFence();
        keyMem.putLong(KEY_RESERVED_OFFSET_SEQUENCE_CHECK, seq);
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
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - VALUE_BLOCK_FILE_RESERVED);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            valueBlockOffset = getPreviousBlock(valueBlockOffset);
        }

        void of(int key) {
            assert key > -1 : "key must be positive integer: " + key;
            long offset = getKeyEntryOffset(key);
            this.valueCount = keyMem.getLong(offset + KEY_ENTRY_OFFSET_VALUE_COUNT);
            assert valueCount > -1;
            this.valueBlockOffset = keyMem.getLong(offset + KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
        }
    }
}
