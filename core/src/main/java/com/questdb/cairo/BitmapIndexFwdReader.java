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
import com.questdb.std.Unsafe;
import com.questdb.std.str.Path;

public class BitmapIndexFwdReader extends AbstractIndexReader {
    private final static Log LOG = LogFactory.getLog(BitmapIndexFwdReader.class);
    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();

    public BitmapIndexFwdReader(CairoConfiguration configuration, Path path, CharSequence name, long unIndexedNullCount) {
        of(configuration, path, name, unIndexedNullCount);
    }

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && unIndexedNullCount > 0 && minValue < unIndexedNullCount) {
            // we need to return some nulls and the whole set of actual index values
            final NullCursor nullCursor = getNullCursor(cachedInstance);
            nullCursor.nullPos = minValue;
            nullCursor.nullCount = unIndexedNullCount;
            nullCursor.of(key, 0, maxValue);
            return nullCursor;
        }

        if (key < keyCount) {
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
        protected long position;
        protected long valueCount;
        protected long next;
        private long valueBlockOffset;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;
        private long maxValue;

        @Override
        public boolean hasNext() {
            if (position < valueCount) {
                long cellIndex = getValueCellIndex(position++);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);

                if (result > maxValue) {
                    valueCount = 0;
                    return false;
                }

                if (cellIndex == blockValueCountMod && position < valueCount) {
                    // we are at edge of block right now, next value will be in previous block
                    jumpToPreviousValueBlock();
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

        private long getNextBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + 8);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            // we don't need to grow valueMem because we going from farthest block back to start of file
            // to closes, e.g. valueBlockOffset is decreasing.
            valueBlockOffset = getNextBlock(valueBlockOffset);
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
            long lastValueBlockOffset;
            final long deadline = clock.getTicks() + spinLockTimeoutUs;
            while (true) {
                valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                Unsafe.getUnsafe().loadFence();
                if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                    valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);
                    lastValueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);

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

            valueMem.grow(lastValueBlockOffset + blockCapacity);
            this.valueCount = valueCount;
            if (valueCount > 0) {
                BitmapIndexUtils.seekValueBlockLTR(valueCount, valueBlockOffset, valueMem, minValue, blockValueCountMod, SEEKER);
            } else {
                seekValue(valueCount, valueBlockOffset);
            }

            this.maxValue = maxValue;
        }

        private void seekValue(long count, long offset) {
            this.position = count;
            this.valueBlockOffset = offset;
        }
    }

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