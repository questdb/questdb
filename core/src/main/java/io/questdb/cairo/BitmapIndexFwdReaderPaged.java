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

import io.questdb.NullIndexFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.vm.MemoryPMRImpl;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/**
 * Forward bitmap index reader that uses paged value memory mapping.
 * Cursors returned by this class are not thread-safe.
 */
public class BitmapIndexFwdReaderPaged extends AbstractIndexReader {
    private static final Log LOG = LogFactory.getLog(BitmapIndexFwdReaderPaged.class);
    private final Cursor cursor = new Cursor();
    // Reuse frame cursor to avoid accumulating pinned pages from abandoned cursor instances.
    private final Cursor frameCursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();
    private final MemoryPMRImpl valueMemPaged;

    public BitmapIndexFwdReaderPaged(
            CairoConfiguration configuration,
            Path path,
            CharSequence name,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        this(configuration.getBitmapIndexReaderPagedPageSize(), configuration.getBitmapIndexReaderPagedMaxPages());
        of(configuration, path, name, columnNameTxn, partitionTxn, columnTop);
    }

    private BitmapIndexFwdReaderPaged(long pageSize, int maxMappedPages) {
        this(new MemoryPMRImpl(pageSize, maxMappedPages));
    }

    private BitmapIndexFwdReaderPaged(MemoryPMRImpl valueMemPaged) {
        super(valueMemPaged);
        this.valueMemPaged = valueMemPaged;
    }

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            // we need to return some nulls and the whole set of actual index values
            final NullCursor nullCursor = getNullCursor(cachedInstance);
            nullCursor.nullPos = minValue;
            final long hi = maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1;
            nullCursor.nullCount = Math.min(columnTop, hi);
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

    @Override
    public IndexFrameCursor getFrameCursor(int key, long minRowId, long maxRowId) {
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key < keyCount) {
            frameCursor.of(key, minRowId, maxRowId, keyCount);
            return frameCursor;
        }

        frameCursor.clear();
        return NullIndexFrameCursor.INSTANCE;
    }

    @Override
    public long getValueBaseAddress() {
        throw new UnsupportedOperationException("paged bitmap index reader does not expose contiguous value memory");
    }

    private Cursor getCursor(boolean cachedInstance) {
        return cachedInstance ? cursor : new Cursor();
    }

    private NullCursor getNullCursor(boolean cachedInstance) {
        return cachedInstance ? nullCursor : new NullCursor();
    }

    private class Cursor implements RowCursor, IndexFrameCursor {
        private final IndexFrame indexFrame = new IndexFrame();
        protected long next;
        protected long position;
        protected long valueCount;
        private long maxValue;
        private long minValue;
        private int pinnedFrameSlot = -1;
        private long valueBlockOffset;
        private final BitmapIndexUtils.ValueBlockSeeker SEEKER = this::seekValue;

        @Override
        public boolean hasNext() {
            if (position < valueCount) {
                long cellIndex = getValueCellIndex(position++);
                long result = valueMem.getLong(valueBlockOffset + cellIndex * Long.BYTES);

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
            releasePinnedFramePage();
            if (position < valueCount) {
                final long cellIndex = getValueCellIndex(position);
                final long frameOffset = valueBlockOffset + cellIndex * Long.BYTES;
                final int frameSlot = valueMemPaged.pin(frameOffset);
                final long requestedLongs = Math.min(valueCount - position, blockValueCountMod - cellIndex + 1);
                final long bytesInPage = valueMemPaged.getMappedLength(frameSlot) - valueMem.offsetInPage(frameOffset);
                final long frameLongs = Math.min(requestedLongs, bytesInPage / Long.BYTES);
                if (frameLongs <= 0) {
                    valueMemPaged.unpin(frameSlot);
                    throw CairoException.critical(0)
                            .put("index frame could not be mapped [offset=").put(frameOffset)
                            .put(", valueCount=").put(valueCount)
                            .put(", position=").put(position)
                            .put(']');
                }

                pinnedFrameSlot = frameSlot;
                final long frameAddress = valueMem.addressOf(frameOffset);
                position += frameLongs;
                if (position < valueCount && getValueCellIndex(position) == 0) {
                    // we are at edge of block right now, next value will be in next block
                    jumpToNextValueBlock();
                }

                return indexFrame.of(frameAddress, frameLongs);
            }

            return IndexFrame.NULL_INSTANCE;
        }

        private long getNextBlock(long currentValueBlockOffset) {
            return valueMem.getLong(currentValueBlockOffset + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + Long.BYTES);
        }

        private long getValueCellIndex(long absoluteValueIndex) {
            return absoluteValueIndex & blockValueCountMod;
        }

        private void jumpToNextValueBlock() {
            // We don't need to extend valueMem because all calls to this method are protected
            // with a position < valueCount check.
            valueBlockOffset = getNextBlock(valueBlockOffset);
        }

        private void releasePinnedFramePage() {
            if (pinnedFrameSlot > -1) {
                valueMemPaged.unpin(pinnedFrameSlot);
                pinnedFrameSlot = -1;
            }
        }

        private void seekValue(long count, long offset) {
            this.position = count;
            this.valueBlockOffset = offset;
        }

        void clear() {
            releasePinnedFramePage();
        }

        void of(int key, long minValue, long maxValue, long keyCount) {
            clear();
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
        public boolean hasNext() {
            if (nullPos < nullCount) {
                next = nullPos++;
                return true;
            }
            return super.hasNext();
        }
    }
}
