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
import com.questdb.std.Unsafe;
import com.questdb.std.str.Path;

import java.io.Closeable;
import java.util.concurrent.locks.LockSupport;

public class BitmapIndexBackwardReader implements Closeable {
    private final ReadOnlyMemory keyMem;
    private final ReadOnlyMemory valueMem;
    private final Cursor cursor = new Cursor();
    private final int blockValueCountMod;
    private final int blockCapacity;
    private long keyCount;

    public BitmapIndexBackwardReader(FilesFacade ff, CharSequence root, CharSequence name) {
        long pageSize = TableUtils.getMapPageSize(ff);

        try (Path path = new Path()) {
            BitmapIndexConstants.keyFileName(path, root, name);
            this.keyMem = new ReadOnlyMemory(ff, path, pageSize);

            // Read key memory header atomically, in that start and end sequence numbers
            // must be read orderly and their values must match. If they don't match - we must retry.
            // This is always necessary in case reader is created at the same time as index itself.

            int blockValueCountMod;
            long keyCount;
            while (true) {
                long seq = this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_SEQUENCE);
                Unsafe.getUnsafe().loadFence();

                blockValueCountMod = this.keyMem.getInt(BitmapIndexConstants.KEY_RESERVED_OFFSET_BLOCK_VALUE_COUNT) - 1;
                keyCount = this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_OFFSET_KEY_COUNT);
                Unsafe.getUnsafe().loadFence();

                if (this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_SEQUENCE_CHECK) == seq) {
                    break;
                }

                LockSupport.parkNanos(1);
            }

            this.blockValueCountMod = blockValueCountMod;
            this.blockCapacity = (blockValueCountMod + 1) * 8 + BitmapIndexConstants.VALUE_BLOCK_FILE_RESERVED;
            this.keyCount = keyCount;

            BitmapIndexConstants.valueFileName(path, root, name);
            this.valueMem = new ReadOnlyMemory(ff, path, pageSize);
        }
    }

    @Override
    public void close() {
        Misc.free(keyMem);
        Misc.free(valueMem);
    }

    public BitmapIndexCursor getCursor(int key) {

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            cursor.of(key);
            return cursor;
        }

        return BitmapIndexEmptyCursor.INSTANCE;
    }

    private void updateKeyCount() {
        long keyCount;
        while (true) {
            long seq = this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_SEQUENCE);
            Unsafe.getUnsafe().loadFence();

            keyCount = this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_OFFSET_KEY_COUNT);
            Unsafe.getUnsafe().loadFence();

            if (this.keyMem.getLong(BitmapIndexConstants.KEY_RESERVED_SEQUENCE_CHECK) == seq) {
                break;
            }

            LockSupport.parkNanos(1);
        }
        if (keyCount > this.keyCount) {
            this.keyCount = keyCount;
        }
    }

    private class Cursor implements BitmapIndexCursor {
        private long valueBlockOffset;
        private long valueCount;

        @Override
        public boolean hasNext() {
            return valueCount > 0;
        }

        @Override
        public long next() {
            long cellIndex = ((--valueCount) & blockValueCountMod);
            long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);
            if (cellIndex == 0) {
                // we are at edge of block right now, next value will be in previous block
                jumpToPreviousValueBlock();
            }
            return result;
        }

        private void jumpToPreviousValueBlock() {
            valueBlockOffset = valueMem.getLong(valueBlockOffset + blockCapacity - BitmapIndexConstants.VALUE_BLOCK_FILE_RESERVED);
        }

        void of(int key) {
            long offset = BitmapIndexConstants.getKeyEntryOffset(key);

            // Read value count and last block offset atomically. In that we must orderly read value count first and
            // value count check last. If they match - everything we read between those holds true. We must retry
            // should these values do not match.
            while (true) {
                valueCount = keyMem.getLong(offset + BitmapIndexConstants.KEY_ENTRY_OFFSET_VALUE_COUNT);
                Unsafe.getUnsafe().loadFence();

                valueBlockOffset = keyMem.getLong(offset + BitmapIndexConstants.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);
                Unsafe.getUnsafe().loadFence();

                if (keyMem.getLong(offset + BitmapIndexConstants.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                    break;
                }
                LockSupport.parkNanos(1);
            }
        }
    }
}
