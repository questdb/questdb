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

package io.questdb.cairo.idx;

import io.questdb.NullIndexFrameCursor;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.IndexFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import static io.questdb.cairo.RoaringBitmapIndexUtils.*;

/**
 * Forward (ascending) reader for Roaring Bitmap Index.
 * Iterates row IDs from low to high.
 */
public class RoaringBitmapIndexFwdReader extends AbstractRoaringBitmapIndexReader {

    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();

    public RoaringBitmapIndexFwdReader() {
    }

    public RoaringBitmapIndexFwdReader(
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
        if (key >= keyCount) {
            updateKeyCount();
        }

        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            final NullCursor nc = cachedInstance ? nullCursor : new NullCursor();
            nc.nullPos = minValue;
            nc.nullCount = Math.min(columnTop, maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1);
            nc.of(key, minValue, maxValue);
            return nc;
        }

        if (key < keyCount) {
            final Cursor c = cachedInstance ? cursor : new Cursor();
            c.of(key, minValue, maxValue);
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
            final Cursor c = new Cursor();
            c.of(key, minRowId, maxRowId);
            return c;
        }

        return NullIndexFrameCursor.INSTANCE;
    }

    /**
     * Forward cursor implementation with optimized iteration.
     * Uses incremental state tracking to avoid re-scanning containers.
     */
    private class Cursor implements RowCursor, IndexFrameCursor {
        private final IndexFrame indexFrame = new IndexFrame();
        // Reusable objects to avoid allocations in hot path
        private final KeyEntryInfo keyInfo = new KeyEntryInfo();
        private final ChunkInfo currentChunk = new ChunkInfo();

        // Range limits
        private long maxValue;

        // Chunk iteration state
        private int chunkCount;
        private long chunkDirOffset;
        private int currentChunkIndex;

        // Flag to track if currentChunk is valid
        private boolean hasCurrentChunk;

        // Pre-computed values to avoid per-value overhead
        private long chunkBase;           // (chunkId << CHUNK_BITS) - computed once per chunk
        private long containerOffset;     // cached container offset
        private long containerAddress;    // direct memory address for Unsafe access

        // Container iteration state (maintained across calls for efficiency)
        // For array containers
        private int arrayIndex;
        private int arrayCardinality;     // cached cardinality for array bounds check

        // For bitmap containers
        private int bitmapWordIndex;
        private long bitmapCurrentWord;

        // For run containers
        private int runIndex;
        private int runCurrentStart;
        private int runCurrentEnd;
        private int runCurrentPos;

        // For hasNext/next pattern
        private long nextValue;
        private boolean hasNextValue;

        @Override
        public boolean hasNext() {
            if (hasNextValue) {
                return true;
            }
            return advance();
        }

        @Override
        public long next() {
            hasNextValue = false;
            return nextValue;
        }

        @Override
        public IndexFrame nextIndexFrame() {
            if (!hasNext()) {
                return IndexFrame.NULL_INSTANCE;
            }
            // For array containers, we can return direct memory reference
            if (hasCurrentChunk && currentChunk.containerType == CONTAINER_TYPE_ARRAY) {
                long addr = valueMem.addressOf(containerOffset + (long) arrayIndex * Short.BYTES);
                return indexFrame.of(addr, 1);
            }
            return IndexFrame.NULL_INSTANCE;
        }

        void of(int key, long minValue, long maxValue) {
            this.maxValue = maxValue;
            this.hasNextValue = false;
            this.hasCurrentChunk = false;

            if (key >= keyCount) {
                chunkCount = 0;
                return;
            }

            if (!readKeyEntryAtomically(key, keyInfo)) {
                chunkCount = 0;
                return;
            }

            this.chunkCount = keyInfo.chunkCount;
            this.chunkDirOffset = keyInfo.chunkDirOffset;

            // Extend to cover the chunk directory we're about to read
            // Use data from keyInfo, not the potentially stale valueMemSize
            long requiredSize = keyInfo.chunkDirOffset + (long) keyInfo.chunkCount * CHUNK_DIR_ENTRY_SIZE;
            valueMem.extend(requiredSize);

            // Find the first chunk that could contain values >= minValue
            int minChunkId = getChunkId(minValue);
            currentChunkIndex = findFirstChunk(minChunkId);

            if (currentChunkIndex >= chunkCount) {
                chunkCount = 0;
                return;
            }

            // Load first chunk and position within it
            loadChunk(currentChunkIndex);
            initializeContainerPosition(minValue);
        }

        private int findFirstChunk(int minChunkId) {
            int low = 0;
            int high = chunkCount - 1;

            while (low < high) {
                int mid = (low + high) >>> 1;
                long dirOffset = chunkDirOffset + (long) mid * CHUNK_DIR_ENTRY_SIZE;
                int chunkId = valueMem.getShort(dirOffset + CHUNK_DIR_OFFSET_CHUNK_ID) & 0xFFFF;

                if (chunkId < minChunkId) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }

            return low;
        }

        private void loadChunk(int chunkIndex) {
            long dirOffset = chunkDirOffset + (long) chunkIndex * CHUNK_DIR_ENTRY_SIZE;
            readChunkInfo(dirOffset, currentChunk);
            hasCurrentChunk = true;
            // Pre-compute values to avoid per-value overhead
            chunkBase = (long) currentChunk.chunkId << CHUNK_BITS;
            containerOffset = currentChunk.containerOffset;
            containerAddress = valueMem.addressOf(containerOffset);
            arrayCardinality = currentChunk.cardinality;
        }

        private void initializeContainerPosition(long minValue) {
            if (!hasCurrentChunk || currentChunk.cardinality == 0) {
                return;
            }

            int minChunkId = getChunkId(minValue);
            short minOffset = (currentChunk.chunkId == minChunkId) ? getOffsetInChunk(minValue) : 0;

            if (currentChunk.containerType == CONTAINER_TYPE_ARRAY) {
                initArrayPosition(minOffset);
            } else if (currentChunk.containerType == CONTAINER_TYPE_BITMAP) {
                initBitmapPosition(minOffset);
            } else {
                initRunPosition(minOffset);
            }
        }

        private void initArrayPosition(short minOffset) {
            // Binary search for first value >= minOffset
            arrayIndex = lowerBound(
                    valueMem.addressOf(containerOffset),
                    currentChunk.cardinality,
                    minOffset
            );
        }

        private void initBitmapPosition(short minOffset) {
            int uMinOffset = minOffset & 0xFFFF;
            bitmapWordIndex = uMinOffset >>> 6;
            int startBit = uMinOffset & 63;

            // Load current word and mask off bits before startBit
            if (bitmapWordIndex < 1024) {
                bitmapCurrentWord = valueMem.getLong(containerOffset + (long) bitmapWordIndex * 8);
                // Clear bits before startBit
                bitmapCurrentWord &= ~((1L << startBit) - 1);
                // Find next set bit or advance to next word
                advanceToNextBitmapBit();
            } else {
                bitmapCurrentWord = 0;
            }
        }

        private void advanceToNextBitmapBit() {
            // Direct memory access via Unsafe
            while (bitmapCurrentWord == 0 && bitmapWordIndex < 1023) {
                bitmapWordIndex++;
                bitmapCurrentWord = Unsafe.getUnsafe().getLong(containerAddress + (long) bitmapWordIndex * 8);
            }
        }

        private void initRunPosition(short minOffset) {
            int uMinOffset = minOffset & 0xFFFF;
            int runCount = valueMem.getShort(containerOffset) & 0xFFFF;

            runIndex = 0;
            runCurrentPos = 0;

            // Find the run containing or after minOffset
            for (int r = 0; r < runCount; r++) {
                long runOffset = containerOffset + RUN_CONTAINER_HEADER_SIZE + (long) r * 4;
                runCurrentStart = valueMem.getShort(runOffset) & 0xFFFF;
                int length = valueMem.getShort(runOffset + 2) & 0xFFFF;
                runCurrentEnd = runCurrentStart + length;

                if (uMinOffset < runCurrentEnd) {
                    runIndex = r;
                    runCurrentPos = Math.max(runCurrentStart, uMinOffset);
                    return;
                }
            }

            // Past all runs
            runIndex = runCount;
            runCurrentPos = 0;
            runCurrentEnd = 0;
        }

        private boolean advance() {
            while (currentChunkIndex < chunkCount) {
                if (hasCurrentChunk) {
                    // Type-specialized advance to avoid per-value type check
                    long rowId;
                    switch (currentChunk.containerType) {
                        case CONTAINER_TYPE_ARRAY:
                            rowId = advanceInArray();
                            break;
                        case CONTAINER_TYPE_BITMAP:
                            rowId = advanceInBitmap();
                            break;
                        default:
                            rowId = advanceInRun();
                            break;
                    }
                    if (rowId >= 0) {
                        if (rowId > maxValue) {
                            chunkCount = 0;
                            return false;
                        }
                        nextValue = rowId;
                        hasNextValue = true;
                        return true;
                    }
                }

                // Move to next chunk
                currentChunkIndex++;
                if (currentChunkIndex < chunkCount) {
                    loadChunk(currentChunkIndex);
                    initializeContainerAtStart();

                    // Check if this chunk is past maxValue
                    if (maxValue < ((long) Integer.MAX_VALUE << CHUNK_BITS)) {
                        int maxChunkId = getChunkId(maxValue);
                        if (currentChunk.chunkId > maxChunkId) {
                            chunkCount = 0;
                            return false;
                        }
                    }
                }
            }

            return false;
        }

        private void initializeContainerAtStart() {
            switch (currentChunk.containerType) {
                case CONTAINER_TYPE_ARRAY:
                    arrayIndex = 0;
                    break;
                case CONTAINER_TYPE_BITMAP:
                    bitmapWordIndex = 0;
                    bitmapCurrentWord = Unsafe.getUnsafe().getLong(containerAddress);
                    advanceToNextBitmapBit();
                    break;
                default:
                    initRunAtStart();
                    break;
            }
        }

        private void initRunAtStart() {
            int runCount = valueMem.getShort(containerOffset) & 0xFFFF;
            if (runCount > 0) {
                runIndex = 0;
                long runOffset = containerOffset + RUN_CONTAINER_HEADER_SIZE;
                runCurrentStart = valueMem.getShort(runOffset) & 0xFFFF;
                int length = valueMem.getShort(runOffset + 2) & 0xFFFF;
                runCurrentEnd = runCurrentStart + length;
                runCurrentPos = runCurrentStart;
            } else {
                runIndex = 0;
                runCurrentEnd = 0;
                runCurrentPos = 0;
            }
        }

        private long advanceInArray() {
            if (arrayIndex < arrayCardinality) {
                // Direct memory access via Unsafe - bypasses Memory abstraction overhead
                int offset = Unsafe.getUnsafe().getShort(containerAddress + (long) arrayIndex * Short.BYTES) & 0xFFFF;
                arrayIndex++;
                return chunkBase + offset;
            }
            return -1;
        }

        private long advanceInBitmap() {
            if (bitmapCurrentWord != 0) {
                // Find lowest set bit using numberOfTrailingZeros (intrinsic - very fast)
                int bit = Long.numberOfTrailingZeros(bitmapCurrentWord);
                long rowId = chunkBase + (bitmapWordIndex << 6) + bit;

                // Clear lowest set bit
                bitmapCurrentWord &= bitmapCurrentWord - 1;

                // If word is now empty, advance to next non-empty word
                if (bitmapCurrentWord == 0) {
                    advanceToNextBitmapBit();
                }

                return rowId;
            }
            return -1;
        }

        private long advanceInRun() {
            if (runCurrentPos < runCurrentEnd) {
                long rowId = chunkBase + runCurrentPos;
                runCurrentPos++;

                // If we've exhausted this run, move to next
                if (runCurrentPos >= runCurrentEnd) {
                    advanceToNextRun();
                }

                return rowId;
            }
            return -1;
        }

        private void advanceToNextRun() {
            int runCount = valueMem.getShort(containerOffset) & 0xFFFF;
            runIndex++;
            if (runIndex < runCount) {
                long runOffset = containerOffset + RUN_CONTAINER_HEADER_SIZE + (long) runIndex * 4;
                runCurrentStart = valueMem.getShort(runOffset) & 0xFFFF;
                int length = valueMem.getShort(runOffset + 2) & 0xFFFF;
                runCurrentEnd = runCurrentStart + length;
                runCurrentPos = runCurrentStart;
            } else {
                runCurrentEnd = 0;
                runCurrentPos = 0;
            }
        }
    }

    /**
     * Cursor that includes null values at the beginning for column top.
     */
    private class NullCursor extends Cursor {
        long nullPos;
        long nullCount;

        @Override
        public boolean hasNext() {
            if (nullPos < nullCount) {
                return true;
            }
            return super.hasNext();
        }

        @Override
        public long next() {
            if (nullPos < nullCount) {
                return nullPos++;
            }
            return super.next();
        }
    }
}
