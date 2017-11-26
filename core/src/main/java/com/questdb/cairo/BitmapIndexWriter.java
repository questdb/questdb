/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

import com.questdb.std.FilesFacade;
import com.questdb.std.Misc;
import com.questdb.std.Numbers;
import com.questdb.std.Unsafe;
import com.questdb.std.str.Path;

import java.io.Closeable;
import java.io.IOException;

public class BitmapIndexWriter implements Closeable {
    private final ReadWriteMemory keyMem;
    private final ReadWriteMemory valueMem;
    private final Path path = new Path();
    private final int blockCapacity;
    private final int blockValueCountMod;
    private final FilesFacade ff;
    private long valueMemSize = 0;
    private long keyCount;

    public BitmapIndexWriter(FilesFacade ff, CharSequence root, CharSequence name, int blockCapacity) {
        this.ff = ff;
        path.of(root).concat(name).$();
        long pageSize = TableUtils.getMapPageSize(ff);
        path.of(root).concat(name).put(".k").$();
        if (!ff.exists(path)) {
            createEmptyIndex(blockCapacity);
        }
        this.keyMem = new ReadWriteMemory(ff, path, pageSize);
        long keyMemSize = this.keyMem.size();
        // check if key file header is present
        if (keyMemSize < BitmapIndexConstants.KEY_FILE_RESERVED) {
            throw CairoException.instance(0).put("key file is too small");
        }

        // verify header signature
        if (this.keyMem.getByte(BitmapIndexConstants.KEY_RESERVED_OFFSET_SIGNATURE) != BitmapIndexConstants.SIGNATURE) {
            throw CairoException.instance(0).put("invalid header");
        }

        // verify key count
        this.keyCount = this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_OFFSET_KEY_COUNT);
        if (keyMemSize < keyMemSize()) {
            throw CairoException.instance(0).put("truncated key file");
        }

        this.valueMemSize = this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
        this.valueMem = new ReadWriteMemory(ff, path.of(root).concat(name).put(".v").$(), pageSize);

        if (this.valueMem.size() < this.valueMemSize) {
            throw CairoException.instance(0).put("truncated value file");
        }

        // block value count is always a power of two
        // to calculate remainder we use faster 'x & (count-1)', which is equivalent to (x % count)
        this.blockValueCountMod = this.keyMem.getInt(BitmapIndexConstants.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
        this.blockCapacity = this.blockValueCountMod + 1 + BitmapIndexConstants.VALUE_BLOCK_FILE_RESERVED;
    }

    /**
     * Adds key-value pair to index. If key already exists, value is appended to end of list of existing values. Otherwise
     * new value list is associated with the key.
     * <p>
     * Index is updated atomically as far as concurrent reading is concerned. Please refer to notes on classes that
     * are responsible for reading bitmap indexes, such as {@link BitmapIndexBackwardReader}.
     *
     * @param key   int key
     * @param value long value
     */
    public void add(int key, long value) {
        long offset = getKeyEntryOffset(key);
        long valueBlockOffset;
        long valueCount;
        int valueCellIndex;
        if (key < keyCount) {
            // when key exists we have possible outcomes with regards to values
            // 1. last value block has space if value cell index is not the last in block
            // 2. value block is full and we have to allocate a new one
            // 3. value count is 0. This means key was created as byproduct of adding sparse key value
            // second option is supposed to be less likely because we attempt to
            // configure block capacity to accommodate as many values as possible
            valueBlockOffset = keyMem.getLong(offset + BitmapIndexConstants.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
            valueCount = keyMem.getLong(offset + BitmapIndexConstants.KEY_ENTRY_OFFSET_VALUE_COUNT);
            valueCellIndex = (int) (valueCount & blockValueCountMod);

            if (valueCount == 0) {
                // we are effectively adding a new key and creating new block
                long newValueBlockOffset = valueMemSize;
                valueMemSize += blockCapacity;

                // must update value mem size in key memory header
                // so that index can be opened correctly next time it loads
                keyMem.jumpTo(BitmapIndexConstants.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                keyMem.putLong(valueMemSize);

                // store our value
                valueMem.jumpTo(newValueBlockOffset);
                valueMem.putLong(value);

                // don't need to update linkage, value count is less than block size
                // index readers must not access linkage information in this case

                // now update key entry in atomic fashion
                // update count and last value block offset for the key
                // in atomic fashion
                keyMem.jumpTo(offset);
                keyMem.putLong(1);
                Unsafe.getUnsafe().storeFence();

                // first and last blocks are the same
                keyMem.putLong(newValueBlockOffset);
                keyMem.putLong(newValueBlockOffset);
                Unsafe.getUnsafe().storeFence();

                // write count check
                keyMem.putLong(1);
                Unsafe.getUnsafe().storeFence();

            } else if (valueCellIndex == 0) {
                // this is scenario #2: key exists but last block is full. We need to create new block and add
                // value there

                // append block at end of value memory
                // we don't need to allocate explicitly, VirtualMemory does that
                // so our new
                long newValueBlockOffset = valueMemSize;
                valueMemSize += blockCapacity;

                // must write out valueMemSize to key memory header
                keyMem.jumpTo(BitmapIndexConstants.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
                keyMem.putLong(valueMemSize);

                // write out value
                valueMem.jumpTo(newValueBlockOffset);
                valueMem.putLong(value);

                // update block linkage before we increase count
                // this is important to index readers, which will act on value count they read

                // we subtract 8 because we just written long value
                valueMem.skip(blockCapacity - 8 - BitmapIndexConstants.VALUE_BLOCK_FILE_RESERVED);
                // set previous block offset on new block
                valueMem.putLong(valueBlockOffset);
                // set next block offset on previous block
                valueMem.jumpTo(valueBlockOffset - BitmapIndexConstants.VALUE_BLOCK_FILE_RESERVED + 8);
                valueMem.putLong(newValueBlockOffset);

                // update count and last value block offset for the key
                // in atomic fashion
                keyMem.jumpTo(offset);
                keyMem.putLong(valueCount + 1);
                Unsafe.getUnsafe().storeFence();

                // don't set first block offset here
                // it would have been done when this key was first created
                keyMem.skip(8);

                // write last block offset because it changed in this scenario
                keyMem.putLong(valueBlockOffset);
                Unsafe.getUnsafe().storeFence();

                // write count check
                keyMem.putLong(valueCount + 1);
                Unsafe.getUnsafe().storeFence();

                // we are done adding value to new block of values
            } else {
                // this is scenario #1: key exists and there is space in last block to add value
                // we don't need to allocate new block, just add value and update value count on key

                // first set value
                valueMem.jumpTo(valueBlockOffset + valueCellIndex * 8);
                valueMem.putLong(value);

                // update count and last value block offset for the key
                // in atomic fashion
                keyMem.jumpTo(offset);
                keyMem.putLong(valueCount + 1);

                // don't change block offsets here
                keyMem.skip(16);

                // write count check
                keyMem.putLong(valueCount + 1);

                // we only set fence at the end because we don't care in which order these counts are updated
                // nothing in between has changed anyway
                Unsafe.getUnsafe().storeFence();
            }
        } else {
            // This is a new key. Because index can have sparse keys whenever we think "key exists" we must deal
            // with holes left by this branch, which allocates new key. All key entries that have been
            // skipped during creation of new key will have been initialized with zeroes. This includes counts and
            // block offsets.

            long newValueBlockOffset = valueMemSize;
            valueMemSize += blockCapacity;

            // must write out valueMemSize to key memory header
            keyMem.jumpTo(BitmapIndexConstants.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE);
            keyMem.putLong(valueMemSize);

            // store our value
            valueMem.jumpTo(newValueBlockOffset);
            valueMem.putLong(value);

            // don't need to update linkage, value count is less than block size
            // index readers must not access linkage information in this case

            // now update key entry in atomic fashion
            // update count and last value block offset for the key
            // in atomic fashion
            keyMem.jumpTo(offset);
            keyMem.putLong(1);
            Unsafe.getUnsafe().storeFence();

            // first and last blocks are the same
            keyMem.putLong(newValueBlockOffset);
            keyMem.putLong(newValueBlockOffset);
            Unsafe.getUnsafe().storeFence();

            // write count check
            keyMem.putLong(1);
            Unsafe.getUnsafe().storeFence();

            // here we also need to update key count
            // we don't just increment key count, in case this addition creates sparse key set
            keyCount = key + 1;
        }
    }

    @Override
    public void close() throws IOException {
        keyMem.jumpTo(keyMemSize());
        valueMem.jumpTo(valueMemSize);
        Misc.free(keyMem);
        Misc.free(valueMem);
        Misc.free(path);
    }

    private void createEmptyIndex(int blockCapacity) {
        // we assume that "path" had already been set for key file name
        //
        // note that we don't have to create value file because it doesn't have mandatory header
        long fd = ff.openRW(path);
        try {
            if (fd < 0) {
                throw CairoException.instance(ff.errno()).put("cannot create ").put(path);
            }

            // this memory will be initialized with zeros already
            // and our intent to write 0L value at start of key file
            long tmp8b = Unsafe.malloc(BitmapIndexConstants.KEY_FILE_RESERVED);
            try {
                Unsafe.getUnsafe().putByte(tmp8b + BitmapIndexConstants.KEY_RESERVED_OFFSET_SIGNATURE, BitmapIndexConstants.SIGNATURE);
                Unsafe.getUnsafe().putLong(tmp8b + BitmapIndexConstants.KEY_RESERVED_OFFSET_KEY_COUNT, 0);
                Unsafe.getUnsafe().putLong(tmp8b + BitmapIndexConstants.KEY_RESERVED_OFFSET_VALUE_MEM_SIZE, 0);
                Unsafe.getUnsafe().putInt(tmp8b + BitmapIndexConstants.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT, Numbers.ceilPow2(blockCapacity));
                Unsafe.getUnsafe().putLong(tmp8b + BitmapIndexConstants.KEY_RESERVED_OFFSET_COUNT_CHECK, 0);

                if (ff.write(fd, tmp8b, BitmapIndexConstants.KEY_FILE_RESERVED, 0) != BitmapIndexConstants.KEY_FILE_RESERVED) {
                    throw CairoException.instance(ff.errno()).put("cannot write header to ").put(path);
                }
            } finally {
                Unsafe.free(tmp8b, BitmapIndexConstants.KEY_FILE_RESERVED);
            }
        } finally {
            ff.close(fd);
        }
    }

    private long getKeyEntryOffset(int key) {
        return key * BitmapIndexConstants.KEY_ENTRY_SIZE + BitmapIndexConstants.KEY_FILE_RESERVED;
    }

    private long keyMemSize() {
        return this.keyCount * 8 + BitmapIndexConstants.KEY_FILE_RESERVED;
    }
}
