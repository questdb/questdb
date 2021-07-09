/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

public class BitmapIndexBwdReader extends AbstractIndexReader {
    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();

    public BitmapIndexBwdReader() {
    }

    public BitmapIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long unIndexedNullCount,
            long partitionTxn
    ) {
        of(configuration, path, name, unIndexedNullCount, partitionTxn);
    }

    // test only
    public BitmapIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long unIndexedNullCount
    ) {
        of(configuration, path, name, unIndexedNullCount, -1);
    }

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {

        assert minValue <= maxValue;

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && unIndexedNullCount > 0) {
            final NullCursor nullCursor = getNullCursor(cachedInstance);
            nullCursor.nullCount = unIndexedNullCount;
            nullCursor.of(key, minValue, maxValue, keyCount);
            return nullCursor;
        }

        if (key < keyCount) {
            final Cursor cursor = getCursor(cachedInstance);
            cursor.of(key, minValue, maxValue, keyCount);
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

    private class Cursor implements RowCursor, IndexFrameCursor {
        protected long valueCount;
        protected long minValue;
        protected long next;
        private long valueBlockOffset;
        private final IndexFrame indexFrame = new IndexFrame();
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;

        @Override
        public IndexFrame getNext() {
            // See BitmapIndexFwdReader if needs implementing
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            if (valueCount > 0) {
                long cellIndex = getValueCellIndex(--valueCount);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);
                if (cellIndex == 0 && valueCount > 0) {
                    // we are at edge of block right now, next value will be in previous block
                    jumpToPreviousValueBlock();
                }

                if (result >= minValue) {
                    this.next = result;
                    return true;
                }

                valueCount = 0;
                return false;

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

        void of(int key, long minValue, long maxValue, long keyCount) {
            if (keyCount == 0) {
                valueCount = 0;
            } else {
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
                        LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutUs).utf8("μs, key=").$(key).$(", offset=").$(offset).$(']').$();
                        throw CairoException.instance(0).put(INDEX_CORRUPT);
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

            if (--nullCount >= minValue) {
                this.next = nullCount;
                return true;
            }
            return false;
        }
    }
}