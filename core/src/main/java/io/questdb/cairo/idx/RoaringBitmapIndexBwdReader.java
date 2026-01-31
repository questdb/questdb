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
import io.questdb.std.str.Path;

import static io.questdb.cairo.RoaringBitmapIndexUtils.*;

/**
 * Backward (descending) reader for Roaring Bitmap Index.
 * Iterates row IDs from high to low.
 */
public class RoaringBitmapIndexBwdReader extends AbstractRoaringBitmapIndexReader {

    private final Cursor cursor = new Cursor();
    private final NullCursor nullCursor = new NullCursor();

    public RoaringBitmapIndexBwdReader() {
    }

    public RoaringBitmapIndexBwdReader(
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
            nc.of(key, minValue, maxValue);
            nc.nullPos = Math.min(columnTop, maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1) - 1;
            nc.nullMin = minValue;
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
     * Backward cursor implementation with optimized iteration.
     * Uses incremental state tracking to avoid re-scanning containers.
     */
    private class Cursor implements RowCursor, IndexFrameCursor {
        private final IndexFrame indexFrame = new IndexFrame();
        // Reusable objects to avoid allocations in hot path
        private final KeyEntryInfo keyInfo = new KeyEntryInfo();
        private final ChunkInfo currentChunk = new ChunkInfo();

        // Range limits
        private long minValue;

        // Chunk iteration state (iterate from last to first)
        private int chunkCount;
        private long chunkDirOffset;
        private int currentChunkIndex;

        // Flag to track if currentChunk is valid
        private boolean hasCurrentChunk;

        // Pre-computed values to avoid per-value overhead
        private long chunkBase;           // (chunkId << CHUNK_BITS) - computed once per chunk
        private long containerOffset;     // cached container offset

        // Container iteration state (maintained across calls for efficiency)
        // For array containers
        private int arrayIndex;

        // For bitmap containers - iterate from high to low
        private int bitmapWordIndex;
        private long bitmapCurrentWord;

        // For run containers - iterate from last run to first
        private int runIndex;
        private int runCurrentStart;
        private int runCurrentPos;  // current position within run (iterates down)

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
            if (hasCurrentChunk && currentChunk.containerType == CONTAINER_TYPE_ARRAY) {
                long addr = valueMem.addressOf(containerOffset + (long) arrayIndex * Short.BYTES);
                return indexFrame.of(addr, 1);
            }
            return IndexFrame.NULL_INSTANCE;
        }

        void of(int key, long minValue, long maxValue) {
            this.minValue = minValue;
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

            // Find the last chunk that could contain values <= maxValue
            int maxChunkId;
            if (maxValue >= ((long) Integer.MAX_VALUE << CHUNK_BITS)) {
                maxChunkId = Integer.MAX_VALUE;
            } else {
                maxChunkId = getChunkId(maxValue);
            }
            currentChunkIndex = findLastChunk(maxChunkId);

            if (currentChunkIndex < 0) {
                chunkCount = 0;
                return;
            }

            // Load last chunk and position at end
            loadChunk(currentChunkIndex);
            initializeContainerPosition(maxValue);
        }

        private int findLastChunk(int maxChunkId) {
            int low = 0;
            int high = chunkCount - 1;
            int result = -1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                long dirOffset = chunkDirOffset + (long) mid * CHUNK_DIR_ENTRY_SIZE;
                int chunkId = valueMem.getShort(dirOffset + CHUNK_DIR_OFFSET_CHUNK_ID) & 0xFFFF;

                if (chunkId <= maxChunkId) {
                    result = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }

            return result;
        }

        private void loadChunk(int chunkIndex) {
            long dirOffset = chunkDirOffset + (long) chunkIndex * CHUNK_DIR_ENTRY_SIZE;
            readChunkInfo(dirOffset, currentChunk);
            hasCurrentChunk = true;
            // Pre-compute values to avoid per-value overhead
            chunkBase = (long) currentChunk.chunkId << CHUNK_BITS;
            containerOffset = currentChunk.containerOffset;
        }

        private void initializeContainerPosition(long maxValue) {
            if (!hasCurrentChunk || currentChunk.cardinality == 0) {
                return;
            }

            int maxChunkId;
            if (maxValue >= ((long) Integer.MAX_VALUE << CHUNK_BITS)) {
                maxChunkId = Integer.MAX_VALUE;
            } else {
                maxChunkId = getChunkId(maxValue);
            }

            // If this chunk is entirely <= maxValue, start at the end
            short maxOffset = (currentChunk.chunkId == maxChunkId)
                    ? getOffsetInChunk(maxValue)
                    : (short) 0xFFFF;

            if (currentChunk.containerType == CONTAINER_TYPE_ARRAY) {
                initArrayPosition(maxOffset);
            } else if (currentChunk.containerType == CONTAINER_TYPE_BITMAP) {
                initBitmapPosition(maxOffset);
            } else {
                initRunPosition(maxOffset);
            }
        }

        private void initArrayPosition(short maxOffset) {
            // Binary search for last value <= maxOffset
            int idx = upperBound(
                    valueMem.addressOf(containerOffset),
                    currentChunk.cardinality,
                    maxOffset
            );
            arrayIndex = idx - 1;
        }

        private void initBitmapPosition(short maxOffset) {
            int uMaxOffset = maxOffset & 0xFFFF;
            bitmapWordIndex = uMaxOffset >>> 6;
            int endBit = uMaxOffset & 63;

            // Load current word and mask off bits after endBit
            if (bitmapWordIndex < 1024) {
                bitmapCurrentWord = valueMem.getLong(containerOffset + (long) bitmapWordIndex * 8);
                // Clear bits after endBit (keep bits 0..endBit inclusive)
                if (endBit < 63) {
                    bitmapCurrentWord &= (1L << (endBit + 1)) - 1;
                }
                // Find previous set bit or go to previous word
                advanceToPrevBitmapBit();
            } else {
                bitmapWordIndex = 1023;
                bitmapCurrentWord = valueMem.getLong(containerOffset + (long) bitmapWordIndex * 8);
                advanceToPrevBitmapBit();
            }
        }

        private void advanceToPrevBitmapBit() {
            // Skip empty words going backward
            while (bitmapCurrentWord == 0 && bitmapWordIndex > 0) {
                bitmapWordIndex--;
                bitmapCurrentWord = valueMem.getLong(containerOffset + (long) bitmapWordIndex * 8);
            }
        }

        private void initRunPosition(short maxOffset) {
            int uMaxOffset = maxOffset & 0xFFFF;
            int runCount = valueMem.getShort(containerOffset) & 0xFFFF;

            // Find the last run that starts <= maxOffset
            runIndex = -1;
            for (int r = runCount - 1; r >= 0; r--) {
                long runOffset = containerOffset + RUN_CONTAINER_HEADER_SIZE + (long) r * 4;
                int start = valueMem.getShort(runOffset) & 0xFFFF;
                int length = valueMem.getShort(runOffset + 2) & 0xFFFF;
                int end = start + length - 1;

                if (start <= uMaxOffset) {
                    runIndex = r;
                    runCurrentStart = start;
                    // Position at min(end, maxOffset)
                    runCurrentPos = Math.min(end, uMaxOffset);
                    return;
                }
            }

            // No valid run found
            runCurrentPos = -1;
        }

        private boolean advance() {
            while (currentChunkIndex >= 0) {
                if (hasCurrentChunk) {
                    // Type-specialized advance to avoid per-value type check
                    long rowId = switch (currentChunk.containerType) {
                        case CONTAINER_TYPE_ARRAY -> advanceInArray();
                        case CONTAINER_TYPE_BITMAP -> advanceInBitmap();
                        default -> advanceInRun();
                    };
                    if (rowId >= 0) {
                        if (rowId < minValue) {
                            chunkCount = 0;
                            return false;
                        }
                        nextValue = rowId;
                        hasNextValue = true;
                        return true;
                    }
                }

                // Move to previous chunk
                currentChunkIndex--;
                if (currentChunkIndex >= 0) {
                    loadChunk(currentChunkIndex);
                    initializeContainerAtEnd();

                    // Check if this chunk is before minValue
                    int minChunkId = getChunkId(minValue);
                    if (currentChunk.chunkId < minChunkId) {
                        chunkCount = 0;
                        return false;
                    }
                }
            }

            return false;
        }

        private void initializeContainerAtEnd() {
            switch (currentChunk.containerType) {
                case CONTAINER_TYPE_ARRAY:
                    arrayIndex = currentChunk.cardinality - 1;
                    break;
                case CONTAINER_TYPE_BITMAP:
                    // Start from last word
                    bitmapWordIndex = 1023;
                    bitmapCurrentWord = valueMem.getLong(containerOffset + (long) bitmapWordIndex * 8);
                    advanceToPrevBitmapBit();
                    break;
                default:
                    initRunAtEnd();
                    break;
            }
        }

        private void initRunAtEnd() {
            int runCount = valueMem.getShort(containerOffset) & 0xFFFF;
            if (runCount > 0) {
                runIndex = runCount - 1;
                long runOffset = containerOffset + RUN_CONTAINER_HEADER_SIZE + (long) runIndex * 4;
                runCurrentStart = valueMem.getShort(runOffset) & 0xFFFF;
                int length = valueMem.getShort(runOffset + 2) & 0xFFFF;
                runCurrentPos = runCurrentStart + length - 1;
            } else {
                runIndex = -1;
                runCurrentPos = -1;
            }
        }

        private long advanceInArray() {
            if (arrayIndex >= 0) {
                int offset = valueMem.getShort(containerOffset + (long) arrayIndex * Short.BYTES) & 0xFFFF;
                arrayIndex--;
                return chunkBase + offset;
            }
            return -1;
        }

        private long advanceInBitmap() {
            if (bitmapCurrentWord != 0) {
                // Find highest set bit using numberOfLeadingZeros (intrinsic - very fast)
                int bit = 63 - Long.numberOfLeadingZeros(bitmapCurrentWord);
                long rowId = chunkBase + (bitmapWordIndex << 6) + bit;

                // Clear highest set bit
                bitmapCurrentWord &= ~(1L << bit);

                // If word is now empty, go to previous non-empty word
                if (bitmapCurrentWord == 0) {
                    advanceToPrevBitmapBit();
                }

                return rowId;
            }
            return -1;
        }

        private long advanceInRun() {
            if (runCurrentPos >= runCurrentStart) {
                long rowId = chunkBase + runCurrentPos;
                runCurrentPos--;

                // If we've exhausted this run, move to previous
                if (runCurrentPos < runCurrentStart) {
                    advanceToPrevRun();
                }

                return rowId;
            }
            return -1;
        }

        private void advanceToPrevRun() {
            runIndex--;
            if (runIndex >= 0) {
                long runOffset = containerOffset + RUN_CONTAINER_HEADER_SIZE + (long) runIndex * 4;
                runCurrentStart = valueMem.getShort(runOffset) & 0xFFFF;
                int length = valueMem.getShort(runOffset + 2) & 0xFFFF;
                runCurrentPos = runCurrentStart + length - 1;
            } else {
                runCurrentPos = -1;
            }
        }
    }

    /**
     * Cursor that includes null values at the end for column top (iterating backward).
     */
    private class NullCursor extends Cursor {
        long nullPos;
        long nullMin;
        boolean inNulls = false;

        @Override
        public boolean hasNext() {
            if (inNulls) {
                return nullPos >= nullMin;
            }
            if (super.hasNext()) {
                return true;
            }
            // Switch to nulls after exhausting real values
            inNulls = true;
            return nullPos >= nullMin;
        }

        @Override
        public long next() {
            if (inNulls) {
                return nullPos--;
            }
            return super.next();
        }

        @Override
        void of(int key, long minValue, long maxValue) {
            super.of(key, minValue, maxValue);
            inNulls = false;
        }
    }
}
