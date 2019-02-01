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
import com.questdb.std.Unsafe;
import com.questdb.std.str.Path;

public class BitmapIndexBwdReader extends AbstractIndexReader {
    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();

    public BitmapIndexBwdReader() {
    }

    public BitmapIndexBwdReader(CairoConfiguration configuration, Path path, CharSequence name, long unIndexedNullCount) {
        of(configuration, path, name, unIndexedNullCount);
    }

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {

        assert minValue <= maxValue;

        if (key >= getKeyCount()) {
            updateKeyCount();
        }

        if (key == 0 && unIndexedNullCount > 0) {
            final NullCursor nullCursor = getNullCursor(cachedInstance);
            nullCursor.nullCount = unIndexedNullCount;
            nullCursor.of(key, minValue, maxValue);
            return nullCursor;
        }

        if (key < getKeyCount()) {
            final Cursor cursor = getCursor(cachedInstance);
            cursor.of(key, minValue, maxValue);
            return cursor;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private Cursor getCursor(boolean cachedInstance) {
        return cachedInstance ? cursor : new Cursor();
    }

    private NullCursor getNullCursor(boolean cachedInstance) {
        return cachedInstance ? nullCursor : new NullCursor();
    }

    private class Cursor implements RowCursor {
        protected long valueCount;
        protected long minValue;
        protected long next;
        private long valueBlockOffset;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;

        @Override
        public boolean hasNext() {
            if (valueCount > 0) {
                long cellIndex = getValueCellIndex(--valueCount);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);
                if (cellIndex == 0 && valueCount > 0) {
                    // we are at edge of block right now, next value will be in previous block
                    jumpToPreviousValueBlock();
                }

                if (result < minValue) {
                    valueCount = 0;
                    return false;
                }

                this.next = result;
                return true;
            }
            return false;
        }

        @Override
        public long next() {
            return next;
        }

        private long getPreviousBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            // we don't need to grow valueMem because we going from farthest block back to start of file
            // to closes, e.g. valueBlockOffset is decreasing.
            valueBlockOffset = getPreviousBlock(valueBlockOffset);
        }

        void of(int key, long minValue, long maxValue) {
            assert key > -1 : "key must be positive integer: " + key;
            long offset = BitmapIndexUtils.getKeyEntryOffset(key);
            keyMem.grow(offset + BitmapIndexUtils.KEY_ENTRY_SIZE);
            // Read value count and last block offset atomically. In that we must orderly read value count first and
            // value count check last. If they match - everything we read between those holds true. We must retry
            // should these values do not match.
            long valueCount;
            long valueBlockOffset;
            final long deadline = clock.getTicks() + spinLockTimeoutUs;
            while (true) {
                valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                    valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                        break;
                    }
                }

                if (clock.getTicks() > deadline) {
                    LOG.error().$("cursor failed to read index header consistently [corrupt?] [timeout=").$(spinLockTimeoutUs).utf8("μs, key=").$(key).$(", offset=").$(offset).$(']').$();
                    throw CairoException.instance(0).put("cursor failed to read index header consistently [corrupt?]");
                }
            }

            valueMem.grow(valueBlockOffset + blockCapacity);

            if (valueCount > 0) {
                BitmapIndexUtils.seekValueBlockRTL(valueCount, valueBlockOffset, valueMem, maxValue, blockValueCountMod, SEEKER);
            } else {
                seekValue(valueCount, valueBlockOffset);
            }
            this.minValue = minValue;
        }

        private void seekValue(long count, long offset) {
            this.valueCount = count;
            this.valueBlockOffset = offset;
        }
    }

    private class NullCursor extends Cursor {
        private long nullCount;

        @Override
        public boolean hasNext() {
            if (super.hasNext()) {
                return true;
            }

            if (--nullCount < minValue) {
                return false;
            }

            this.next = nullCount;
            return true;
        }
    }
}