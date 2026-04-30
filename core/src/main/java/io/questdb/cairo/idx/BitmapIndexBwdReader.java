/*+*****************************************************************************
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Cursors returned by this class are not thread-safe.
 */
public class BitmapIndexBwdReader extends AbstractBitmapIndexReader {
    private final ObjList<Cursor> freeCursors = new ObjList<>();
    private final ObjList<NullCursor> freeNullCursors = new ObjList<>();

    public BitmapIndexBwdReader(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop, null, null, 0);
    }

    @Override
    public void close() {
        super.close();
        Misc.clear(freeCursors);
        Misc.clear(freeNullCursors);
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        assert minValue <= maxValue;

        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            NullCursor nc;
            if (freeNullCursors.size() > 0) {
                nc = freeNullCursors.popLast();
                nc.isPooled = false;
            } else {
                nc = new NullCursor();
            }
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nc.nullCount = Math.min(columnTop, hi);
            nc.of(key, minValue, maxValue, keyCount);
            return nc;
        }

        if (key < keyCount) {
            Cursor c;
            if (freeCursors.size() > 0) {
                c = freeCursors.popLast();
                c.isPooled = false;
            } else {
                c = new Cursor();
            }
            c.of(key, minValue, maxValue, keyCount);
            return c;
        }

        return EmptyRowCursor.INSTANCE;
    }

    private class Cursor implements RowCursor, IndexFrameCursor {
        protected long minValue;
        protected long next;
        protected long valueCount;
        boolean isPooled;
        private long valueBlockOffset;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;

        @Override
        public void close() {
            if (!isPooled && freeCursors.size() < MAX_CACHED_FREE_CURSORS) {
                isPooled = true;
                freeCursors.add(this);
            }
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
            return next - minValue;
        }

        @Override
        public IndexFrame nextIndexFrame() {
            // See BitmapIndexFwdReader if it needs implementing
            throw new UnsupportedOperationException();
        }

        private long getPreviousBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToPreviousValueBlock() {
            // We don't need to extend valueMem because we're going from the farthest block back to start of file
            // to closest, e.g. valueBlockOffset is decreasing.
            valueBlockOffset = getPreviousBlock(valueBlockOffset);
        }

        private void seekValue(long count, long offset) {
            this.valueCount = count;
            this.valueBlockOffset = offset;
        }

        void of(int key, long minValue, long maxValue, long keyCount) {
            if (keyCount == 0) {
                valueCount = 0;
            } else {
                assert key > -1 : "key must be positive integer: " + key;
                long offset = BitmapIndexUtils.getKeyEntryOffset(key);
                keyMem.extend(offset + BitmapIndexUtils.KEY_ENTRY_SIZE);
                // Read value count and last block offset atomically. In that we must orderly read value count first and
                // value count check last. If they match - everything we read between those holds true. We must retry
                // should these values do not match.
                long valueCount;
                long valueBlockOffset;
                final long deadline = clock.getTicks() + spinLockTimeoutMs;
                while (true) {
                    valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                    Unsafe.loadFence();
                    if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                        valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);

                        Unsafe.loadFence();
                        if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT) == valueCount) {
                            break;
                        }
                    }

                    if (clock.getTicks() > deadline) {
                        LOG.error().$(INDEX_CORRUPT).$(" [timeout=").$(spinLockTimeoutMs).$("ms, key=").$(key).$(", offset=").$(offset).$(']').$();
                        throw CairoException.critical(0).put(INDEX_CORRUPT);
                    }
                }

                valueMem.extend(valueBlockOffset + blockCapacity);

                if (valueCount > 0) {
                    BitmapIndexUtils.seekValueBlockRTL(valueCount, valueBlockOffset, valueMem, maxValue, blockValueCountMod, SEEKER);
                } else {
                    seekValue(valueCount, valueBlockOffset);
                }

                this.minValue = minValue;
            }
        }
    }

    private class NullCursor extends Cursor {
        private long nullCount;

        @Override
        public void close() {
            if (!isPooled && freeNullCursors.size() < MAX_CACHED_FREE_CURSORS) {
                isPooled = true;
                freeNullCursors.add(this);
            }
        }

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