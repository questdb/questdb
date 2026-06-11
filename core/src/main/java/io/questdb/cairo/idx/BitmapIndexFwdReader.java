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

import io.questdb.NullIndexFrameCursor;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Cursors returned by this class are not thread-safe.
 */
public class BitmapIndexFwdReader extends AbstractBitmapIndexReader {
    private static final Log LOG = LogFactory.getLog(BitmapIndexFwdReader.class);
    private final ObjList<Cursor> freeCursors = new ObjList<>();
    private final ObjList<NullCursor> freeNullCursors = new ObjList<>();

    public BitmapIndexFwdReader(
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
            nc.nullPos = minValue;
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

    @Override
    public IndexFrameCursor getFrameCursor(int key, long minRowId, long maxRowId) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            Cursor c;
            if (freeCursors.size() > 0) {
                c = freeCursors.popLast();
                c.isPooled = false;
            } else {
                c = new Cursor();
            }
            c.of(key, minRowId, maxRowId, keyCount);
            return c;
        }

        return NullIndexFrameCursor.INSTANCE;
    }

    private class Cursor implements RowCursor, IndexFrameCursor {
        private final IndexFrame indexFrame = new IndexFrame();
        protected long next;
        protected long position;
        protected long valueCount;
        boolean isPooled;
        private long maxValue;
        private long minValue;
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
            if (position < valueCount) {
                long cellIndex = getValueCellIndex(position++);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * 8);

                if (result > maxValue) {
                    valueCount = 0;
                    return false;
                }

                if (cellIndex == blockValueCountMod && position < valueCount) {
                    // we are at edge of block right now, next value will be in next block
                    jumpToNextValueBlock();
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

        @Override
        public IndexFrame nextIndexFrame() {
            if (position < valueCount) {
                long cellIndex = getValueCellIndex(position);
                long address = valueMem.addressOf(valueBlockOffset + cellIndex * Long.BYTES);

                long pageSize = Math.min(valueCount - position, blockValueCountMod - cellIndex + 1);
                position += pageSize;
                if (position < valueCount) {
                    // we are at edge of block right now, next value will be in next block
                    jumpToNextValueBlock();
                }

                return indexFrame.of(address, pageSize);
            }

            return IndexFrame.NULL_INSTANCE;
        }

        private long getNextBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + 8);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToNextValueBlock() {
            // We don't need to extend valueMem because all calls to this method are protected
            // with a position < valueCount check.
            valueBlockOffset = getNextBlock(valueBlockOffset);
        }

        private void seekValue(long count, long offset) {
            this.position = count;
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
                long lastValueBlockOffset;
                final long deadline = clock.getTicks() + spinLockTimeoutMs;
                while (true) {
                    valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

                    Unsafe.loadFence();
                    if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                        valueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);
                        lastValueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);

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

                valueMem.extend(lastValueBlockOffset + blockCapacity);
                this.valueCount = valueCount;
                if (valueCount > 0) {
                    BitmapIndexUtils.seekValueBlockLTR(valueCount, valueBlockOffset, valueMem, minValue, blockValueCountMod, SEEKER);
                } else {
                    seekValue(valueCount, valueBlockOffset);
                }

                this.minValue = minValue;
                this.maxValue = maxValue;
            }
        }
    }

    private class NullCursor extends Cursor {
        private long nullCount;
        private long nullPos;

        @Override
        public void close() {
            if (!isPooled && freeNullCursors.size() < MAX_CACHED_FREE_CURSORS) {
                isPooled = true;
                freeNullCursors.add(this);
            }
        }

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