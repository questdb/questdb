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

package io.questdb.cairo;

import io.questdb.cairo.sql.RowCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Cursors returned by this class are thread-safe since they never extend key and
 * value memories or mutate any shared variables.
 * <p>
 * The important assumption is that of() method should be called on a single thread
 * prior to any cursor operations. Once this happens, cursors can be obtained and
 * called concurrently.
 */
public class ConcurrentBitmapIndexFwdReader extends AbstractIndexReader {
    private final static Log LOG = LogFactory.getLog(ConcurrentBitmapIndexFwdReader.class);
    private final Cursor cursor = new Cursor();

    public ConcurrentBitmapIndexFwdReader() {
    }

    // test only
    public ConcurrentBitmapIndexFwdReader(
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
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        return initCursor(cachedInstance ? this.cursor : null, key, minValue, maxValue);
    }

    /**
     * Allows reusing cursor objects, if that's possible.
     *
     * @param rowCursor cursor to reuse, or null
     * @param key       key to search for
     * @param minValue  minimum value to search for
     * @param maxValue  maximum value to search for
     * @return initialised cursor
     */
    public RowCursor initCursor(RowCursor rowCursor, int key, long minValue, long maxValue) {
        Cursor cursor = null;
        if (rowCursor != null && rowCursor != EmptyRowCursor.INSTANCE) {
            cursor = (Cursor) rowCursor;
            assert cursor.owner() == this;
        }

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            // we need to return some nulls and the whole set of actual index values
            if (cursor == null) {
                cursor = new Cursor();
            }
            cursor.of(key, minValue, maxValue, keyCount, minValue, columnTop);
            return cursor;
        }

        if (key < keyCount) {
            if (cursor == null) {
                cursor = new Cursor();
            }
            cursor.of(key, minValue, maxValue, keyCount, 0, 0);
            return cursor;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor implements RowCursor {
        protected long next;
        protected long position;
        protected long valueCount;
        private long maxValue;
        private long minValue;
        private long nullCount;
        private long nullPos;
        private long valueBlockOffset;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;

        @Override
        public boolean hasNext() {
            if (nullPos < nullCount) {
                next = nullPos++;
                return true;
            }
            if (position < valueCount) {
                long cellIndex = getValueCellIndex(position++);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);

                if (result > maxValue) {
                    valueCount = 0;
                    return false;
                }

                if (cellIndex == blockValueCountMod && position < valueCount) {
                    // We are at edge of block right now, next value will be in next block.
                    if (!jumpToNextValueBlock()) {
                        // We've reached the memory boundary which means that no values left.
                        valueCount = 0;
                        return false;
                    }
                }

                this.next = result;
                return true;
            }
            return false;
        }

        @Override
        public long next() {
            return next - minValue;
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private boolean jumpToNextValueBlock() {
            long nextBlock = valueMem.getLong(valueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + 8);
            if (nextBlock >= valueMem.size()) {
                return false;
            }
            valueBlockOffset = nextBlock;
            return true;
        }

        private ConcurrentBitmapIndexFwdReader owner() {
            return ConcurrentBitmapIndexFwdReader.this;
        }

        private void seekValue(long count, long offset) {
            this.position = count;
            this.valueBlockOffset = offset;
        }

        void of(int key, long minValue, long maxValue, long keyCount, long nullPos, long nullCount) {
            this.nullPos = nullPos;
            this.nullCount = nullCount;
            if (keyCount == 0) {
                valueCount = 0;
            } else {
                assert key > -1 : "key must be positive integer: " + key;
                assert key < keyCount : "key must be within the last known boundary: " + key + ", " + keyCount;

                long offset = BitmapIndexUtils.getKeyEntryOffset(key);
                // Read value count and first block offset atomically. In that we must orderly read value count first and
                // value count check last. If they match - everything we read between those holds true. We must retry
                // should these values do not match.
                long valueCount;
                long valueBlockOffset;
                final long deadline = clock.getTicks() + spinLockTimeoutMs;
                while (true) {
                    valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                    Unsafe.getUnsafe().loadFence();
                    if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                        valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);

                        Unsafe.getUnsafe().loadFence();
                        if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                            break;
                        }
                    }

                    if (clock.getTicks() > deadline) {
                        LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, key=").$(key).$(", offset=").$(offset).$(']').$();
                        throw CairoException.critical(0).put(INDEX_CORRUPT);
                    }
                }

                this.valueCount = valueCount;
                if (valueCount > 0) {
                    // Since we don't remap the value memory, the seekValueBlockLTR method needs to consider
                    // the valueMem size boundary as a part of the value search. Luckily, it does that.
                    BitmapIndexUtils.seekValueBlockLTR(valueCount, valueBlockOffset, valueMem, minValue, blockValueCountMod, SEEKER);
                } else {
                    seekValue(valueCount, valueBlockOffset);
                }

                this.minValue = minValue;
                this.maxValue = maxValue;
            }
        }
    }
}
