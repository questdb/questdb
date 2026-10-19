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
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Cursors returned by this class are not thread-safe.
 */
public class BitmapIndexFwdReader extends AbstractBitmapIndexReader {
    // @TestOnly observability for what the range counts spend. Their block seeks and walks read no rows
    // and pull no index entries, so every counter one level up reports zero for them, yet each hop is
    // a random read into the mapped value file and the chain a hot key builds is arbitrarily long.
    // Only a count of the hops themselves shows whether admission costs the posting list or the range.
    // A plain static boolean guards it: the JIT folds the always-false production branch away, and the
    // tests that flip it drive their queries on the calling thread.
    @TestOnly
    public static boolean isBlockHopCounterEnabled = false;
    @TestOnly
    public static final AtomicLong testRangeCountBlockHops = new AtomicLong();
    private static final Log LOG = LogFactory.getLog(BitmapIndexFwdReader.class);
    private final ObjList<Cursor> freeCursors = new ObjList<>();
    private final ObjList<NullCursor> freeNullCursors = new ObjList<>();
    private final RangeCountSeeker rangeCountSeeker = new RangeCountSeeker();
    // The value block offsets of the key entry readKeyEntry() read last.
    private long entryFirstValueBlockOffset;
    private long entryLastValueBlockOffset;
    // The positions of the values locateValues() found last, and the value blocks that hold the first and the last.
    private long rangeHi;
    private long rangeHiBlock;
    private long rangeLo;
    private long rangeLoBlock;
    // What the most recent countMatchesInRange() call spent, in block hops. Exhaustion charges the
    // whole budget the call was given. Same single-owner discipline as the seeker instance itself.
    private int lastRangeCountBlockHops;

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

    /**
     * The exact number of rows {@code key} matches within {@code [minValue, maxValue]} -- the same
     * count the cursor from {@link #getCursor(int, long, long)} yields over that range, read from
     * index metadata instead of walked -- or {@link AbstractPostingIndexReader#ESTIMATE_REJECT} when
     * reaching that range would cost more than {@code maxBlockHops} block hops.
     * <p>
     * The key entry's stored value count is NOT that answer on its own: it counts every posting the
     * key holds anywhere in this partition, so it equals the range count only when the range covers
     * the key's whole posting list. A partition frame narrowed by an interval scan, or by a
     * transaction boundary, is exactly the case where the two differ. The two block seeks are what
     * make the result exact for an arbitrary sub-range: the forward seek counts postings strictly
     * below {@code minValue} and the backward seek counts postings at or below {@code maxValue}.
     * Their difference is precisely what the cursor emits, because the cursor starts at the first
     * posting at or above {@code minValue} and stops at the first one past {@code maxValue}.
     * <p>
     * Those two seeks are also what makes an exact answer expensive. They hop block by block along a
     * linked chain, so a range in the MIDDLE of a hot key's posting list makes the forward seek cross
     * every block below it and the backward seek cross every block above it: together they inspect
     * the whole chain however few rows the range selects, and the chain grows with the partition
     * while the range stays put. {@code maxBlockHops} caps the pair -- the backward seek gets what the
     * forward seek left -- and this reports ESTIMATE_REJECT instead of a count once the cap runs out.
     * There is no cheaper exact answer to fall back on: the key entry carries no rank or skip
     * metadata over the chain, so a caller that cannot afford the walk has to pick a plan that needs
     * no count. Cost is one block read per seek for a range that covers the whole posting list (both
     * seeks break out on the first block they inspect), and otherwise one read per value block lying
     * outside the range, up to the cap -- never a per-row walk.
     * <p>
     * Callers must respect the single-owner discipline the cursors follow (see
     * {@link AbstractBitmapIndexReader#isOperatingThread()}): this reuses one seeker instance and is
     * not safe to call concurrently against one reader.
     *
     * @param key          index key; a negative key matches nothing
     * @param minValue     inclusive lower bound
     * @param maxValue     inclusive upper bound
     * @param maxBlockHops how many value blocks the two seeks may hop across in total; pass
     *                     {@link BitmapIndexUtils#UNBOUNDED_BLOCK_HOPS} to always get a count
     * @return the exact match count, or {@link AbstractPostingIndexReader#ESTIMATE_REJECT} when the
     * hop budget ran out
     */
    public long countMatchesInRange(int key, long minValue, long maxValue, int maxBlockHops) {
        lastRangeCountBlockHops = 0;
        if (key < 0 || minValue > maxValue) {
            return 0;
        }
        final long total = getNullRowCount(key, minValue, maxValue);
        // Past the key count the index addresses nothing, so the null prefix is the whole answer.
        final long valueCount = readKeyEntry(key);
        if (valueCount == 0) {
            return total;
        }
        final int hopsBelowMin = BitmapIndexUtils.seekValueBlockLTR(
                valueCount,
                entryFirstValueBlockOffset,
                valueMem,
                minValue,
                blockValueCountMod,
                rangeCountSeeker,
                maxBlockHops
        );
        if (hopsBelowMin == BitmapIndexUtils.BLOCK_HOP_BUDGET_EXHAUSTED) {
            lastRangeCountBlockHops = maxBlockHops;
            countBlockHops(maxBlockHops);
            return AbstractPostingIndexReader.ESTIMATE_REJECT;
        }
        lastRangeCountBlockHops = hopsBelowMin;
        countBlockHops(hopsBelowMin);
        final long countBelowMin = rangeCountSeeker.count;
        final int hopsAtOrBelowMax = BitmapIndexUtils.seekValueBlockRTL(
                valueCount,
                entryLastValueBlockOffset,
                valueMem,
                maxValue,
                blockValueCountMod,
                rangeCountSeeker,
                maxBlockHops - hopsBelowMin
        );
        if (hopsAtOrBelowMax == BitmapIndexUtils.BLOCK_HOP_BUDGET_EXHAUSTED) {
            lastRangeCountBlockHops = maxBlockHops;
            countBlockHops(maxBlockHops - hopsBelowMin);
            return AbstractPostingIndexReader.ESTIMATE_REJECT;
        }
        lastRangeCountBlockHops = hopsBelowMin + hopsAtOrBelowMax;
        countBlockHops(hopsAtOrBelowMax);
        final long countAtOrBelowMax = rangeCountSeeker.count;
        // seekValueBlockLTR reports the whole value count when the posting list runs past the mapped
        // extent of the value file, which is its way of saying it found nothing at or above
        // minValue; the cursor degrades to empty in the same case, so clamp instead of going negative.
        return total + Math.max(0, countAtOrBelowMax - countBelowMin);
    }

    /**
     * How many block hops the most recent {@link #countMatchesInRange(int, long, long, int)} call
     * spent, including a call that exhausted its budget, which charges the whole budget it was
     * given. A caller sharing one hop budget across many counts subtracts this after each call.
     * Single-owner discipline applies, exactly as for the count itself.
     */
    public int getLastRangeCountBlockHops() {
        return lastRangeCountBlockHops;
    }

    /**
     * The number of rows {@code key} matches within {@code [minValue, maxValue]}, or any number above
     * {@code maxCount} once the count passes it.
     * <p>
     * Unlike {@link #countMatchesInRange(int, long, long, int)}, this never walks a posting list from one end to the
     * far bound of the range. It reaches the nearer bound from its end of the list, then walks the range itself by
     * whole value blocks, and stops at the far bound or after {@code maxCount} rows. Each of the two walks races a walk
     * from the other end of the list, one block at a time, so it reads at most twice the blocks of the shorter way to
     * each bound, and none of the blocks between the range and the far end of the list when the range is nearer.
     * <p>
     * Single-owner discipline applies, as for {@link #countMatchesInRange(int, long, long, int)}.
     */
    public long countRows(int key, long minValue, long maxValue, long maxCount) {
        if (key < 0 || minValue > maxValue) {
            return 0;
        }
        final long nullRowCount = getNullRowCount(key, minValue, maxValue);
        if (nullRowCount > maxCount) {
            return nullRowCount;
        }
        final long valueCount = readKeyEntry(key);
        return valueCount > 0 ? nullRowCount + locateValues(valueCount, minValue, maxValue, maxCount - nullRowCount, 0) : nullRowCount;
    }

    /**
     * The row a cursor in {@code direction} returns for {@code key} over {@code [minValue, maxValue]} after it
     * skips {@code rowIndex} rows. The key must have more than {@code rowIndex} rows there.
     * <p>
     * It locates the values of the range as {@link #countRows(int, long, long, long)} does, stopping once it passes
     * the requested row from the requested direction, then hops to the value block from the nearer located end.
     *
     * @param direction {@link IndexReader#DIR_FORWARD} or {@link IndexReader#DIR_BACKWARD}
     */
    public long getRow(int key, long minValue, long maxValue, int direction, long rowIndex) {
        final long nullRowCount = getNullRowCount(key, minValue, maxValue);
        // a forward cursor returns the NULL rows below the column top first, a backward one returns them last
        if (direction == IndexReader.DIR_FORWARD && rowIndex < nullRowCount) {
            return minValue + rowIndex;
        }
        final long valueRowIndex = direction == IndexReader.DIR_FORWARD ? rowIndex - nullRowCount : rowIndex;
        final long valueCount = readKeyEntry(key);
        final long valueRowCount = valueCount > 0 ? locateValues(valueCount, minValue, maxValue, valueRowIndex, direction) : 0;
        if (valueRowIndex < valueRowCount) {
            return getRangeValue(direction == IndexReader.DIR_FORWARD ? rangeLo + valueRowIndex : rangeHi - 1 - valueRowIndex);
        }
        return minValue + nullRowCount - 1 - (valueRowIndex - valueRowCount);
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        stampOperatingThread();
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
        stampOperatingThread();
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

    @TestOnly
    public static void resetTestCounters() {
        testRangeCountBlockHops.set(0);
    }

    private static void countBlockHops(long blockHops) {
        if (isBlockHopCounterEnabled) {
            testRangeCountBlockHops.addAndGet(blockHops);
        }
    }

    private long blockIndex(long position) {
        return position / (blockValueCountMod + 1);
    }

    // Rows before the column top predate the column, so the index holds no entry for them and getCursor()
    // synthesizes them as NULL rows of key 0 through NullCursor. This mirrors its nullCount exactly. Those row ids
    // all sit below the column top while every key-0 posting sits at or above it, so they never double-count a row.
    private long getNullRowCount(int key, long minValue, long maxValue) {
        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            return Math.max(0, Math.min(columnTop, maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1) - minValue);
        }
        return 0;
    }

    // The value at the position, which lies in the range locateValues() found, reached from the nearer end of the range.
    private long getRangeValue(long position) {
        final long blockIndex = blockIndex(position);
        final long loBlockHops = blockIndex - blockIndex(rangeLo);
        final long hiBlockHops = blockIndex(rangeHi - 1) - blockIndex;
        return getValue(loBlockHops <= hiBlockHops ? hopForward(rangeLoBlock, loBlockHops) : hopBackward(rangeHiBlock, hiBlockHops), position);
    }

    private long getValue(long block, long position) {
        return valueMem.getLong(block + (position & blockValueCountMod) * Long.BYTES);
    }

    private long hopBackward(long block, long blockHops) {
        for (long i = 0; i < blockHops; i++) {
            block = valueMem.getLong(block + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED);
        }
        countBlockHops(blockHops);
        return block;
    }

    private long hopForward(long block, long blockHops) {
        for (long i = 0; i < blockHops; i++) {
            block = valueMem.getLong(block + blockCapacity - BitmapIndexUtils.VALUE_BLOCK_FILE_RESERVED + 8);
        }
        countBlockHops(blockHops);
        return block;
    }

    // Locates the values of the key entry readKeyEntry() read last that lie in [minValue, maxValue] and returns their
    // number, or any number above maxCount once the count passes it from direction (0 allows either end). The located
    // values sit at positions rangeLo, inclusive, to rangeHi, exclusive. The blocks rangeLoBlock and rangeHiBlock hold
    // rangeLo and rangeHi - 1. A walk up from the first value block and a walk down from the last one race, a block at
    // a time, to the bound nearer their end. A walk inside the range from that bound then races the walk from the other
    // end to the other bound, retaining only a prefix or suffix when it stops after maxCount values.
    private long locateValues(long valueCount, long minValue, long maxValue, long maxCount, int direction) {
        final long blockStartMask = ~(long) blockValueCountMod;
        long headBlock = entryFirstValueBlockOffset;
        long headPosition = 0;
        long tailBlock = entryLastValueBlockOffset;
        long tailPosition = valueCount;
        boolean isLoFound;
        while (true) {
            final long headEnd = Math.min(valueCount, headPosition + blockValueCountMod + 1);
            if (getValue(headBlock, headEnd - 1) >= minValue) {
                rangeLo = getValue(headBlock, headPosition) >= minValue ? headPosition
                        : headPosition + BitmapIndexUtils.searchValueBlock(valueMem, headBlock, headEnd - headPosition, minValue - 1);
                rangeLoBlock = headBlock;
                isLoFound = true;
                break;
            }
            if (headEnd == valueCount) {
                return 0;
            }
            final long tailStart = tailPosition - 1 & blockStartMask;
            if (getValue(tailBlock, tailStart) <= maxValue) {
                rangeHi = getValue(tailBlock, tailPosition - 1) <= maxValue ? tailPosition
                        : tailStart + BitmapIndexUtils.searchValueBlock(valueMem, tailBlock, tailPosition - tailStart, maxValue);
                rangeHiBlock = tailBlock;
                isLoFound = false;
                break;
            }
            if (tailStart == 0) {
                return 0;
            }
            headBlock = hopForward(headBlock, 1);
            headPosition = headEnd;
            tailBlock = hopBackward(tailBlock, 1);
            tailPosition = tailStart;
        }
        long count = 0;
        if (isLoFound) {
            long block = rangeLoBlock;
            long position = rangeLo;
            long lowerBlock = block;
            while (true) {
                final long tailStart = tailPosition - 1 & blockStartMask;
                if (getValue(tailBlock, tailStart) <= maxValue) {
                    rangeHi = getValue(tailBlock, tailPosition - 1) <= maxValue ? tailPosition
                            : tailStart + BitmapIndexUtils.searchValueBlock(valueMem, tailBlock, tailPosition - tailStart, maxValue);
                    rangeHiBlock = tailBlock;
                    break;
                }
                final long end = Math.min(valueCount, (position | blockValueCountMod) + 1);
                if (getValue(block, end - 1) > maxValue) {
                    if (getValue(block, position) > maxValue) {
                        rangeHi = position;
                        rangeHiBlock = lowerBlock;
                    } else {
                        final long cell = position & blockValueCountMod;
                        rangeHi = position + BitmapIndexUtils.searchValueBlock(valueMem, block + cell * Long.BYTES, end - position, maxValue);
                        rangeHiBlock = block;
                    }
                    break;
                }
                count += end - position;
                if (end == valueCount) {
                    rangeHi = valueCount;
                    rangeHiBlock = block;
                    break;
                }
                if (count > maxCount && direction != IndexReader.DIR_BACKWARD) {
                    rangeHi = end;
                    rangeHiBlock = block;
                    return count;
                }
                lowerBlock = block;
                position = end;
                block = hopForward(block, 1);
                tailBlock = hopBackward(tailBlock, 1);
                tailPosition = tailStart;
            }
        } else {
            long block = rangeHiBlock;
            long position = rangeHi - 1;
            long upperBlock = block;
            while (true) {
                final long headEnd = Math.min(valueCount, headPosition + blockValueCountMod + 1);
                if (getValue(headBlock, headEnd - 1) >= minValue) {
                    rangeLo = getValue(headBlock, headPosition) >= minValue ? headPosition
                            : headPosition + BitmapIndexUtils.searchValueBlock(valueMem, headBlock, headEnd - headPosition, minValue - 1);
                    rangeLoBlock = headBlock;
                    break;
                }
                final long start = position & blockStartMask;
                if (getValue(block, start) < minValue) {
                    if (getValue(block, position) < minValue) {
                        rangeLo = position + 1;
                        rangeLoBlock = upperBlock;
                    } else {
                        rangeLo = start + BitmapIndexUtils.searchValueBlock(valueMem, block, position - start + 1, minValue - 1);
                        rangeLoBlock = block;
                    }
                    break;
                }
                count += position - start + 1;
                if (start == 0) {
                    rangeLo = 0;
                    rangeLoBlock = block;
                    break;
                }
                if (count > maxCount && direction != IndexReader.DIR_FORWARD) {
                    rangeLo = start;
                    rangeLoBlock = block;
                    return count;
                }
                upperBlock = block;
                position = start - 1;
                block = hopBackward(block, 1);
                headBlock = hopForward(headBlock, 1);
                headPosition = headEnd;
            }
        }
        return Math.max(0, rangeHi - rangeLo);
    }

    // The value count of the key, with the offsets of its first and last value blocks in the entry fields, or 0
    // when the key lies past the key count. Same seqlock protocol as Cursor.of(): read the value count first and
    // last and retry while the two disagree, so the block offsets read in between belong to one consistent entry.
    private long readKeyEntry(int key) {
        if (key >= keyCount) {
            updateKeyCount();
            if (key >= keyCount) {
                return 0;
            }
        }
        final long offset = BitmapIndexUtils.getKeyEntryOffset(key);
        keyMem.extend(offset + BitmapIndexUtils.KEY_ENTRY_SIZE);
        long valueCount;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

            Unsafe.loadFence();
            if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                entryFirstValueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);
                entryLastValueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_LAST_VALUE_BLOCK_OFFSET);

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
        if (valueCount > 0) {
            valueMem.extend(entryLastValueBlockOffset + blockCapacity);
        }
        return valueCount;
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
            // Re-pool only on the reader's operating thread; an off-thread close
            // skips pooling (cursor GC'd, no native memory to strand). See
            // AbstractBitmapIndexReader.isOperatingThread().
            if (!isPooled && isOperatingThread() && freeCursors.size() < MAX_CACHED_FREE_CURSORS) {
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
            // See Cursor.close(): re-pool only on the reader's operating thread.
            if (!isPooled && isOperatingThread() && freeNullCursors.size() < MAX_CACHED_FREE_CURSORS) {
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

    // Captures the count half of a value-block seek; countMatchesInRange() reuses one instance for
    // both of its seeks, reading the result out between them, so the count costs no allocation.
    private static final class RangeCountSeeker implements BitmapIndexUtils.ValueBlockSeeker {
        private long count;

        @Override
        public void seek(long count, long offset) {
            this.count = count;
        }
    }
}