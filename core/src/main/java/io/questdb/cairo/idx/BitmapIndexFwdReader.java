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
    // @TestOnly observability for what countMatchesInRange() spends. The two block seeks read no rows
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
        if (key < 0 || minValue > maxValue) {
            return 0;
        }
        if (key >= keyCount) {
            updateKeyCount();
        }

        long total = 0;
        if (key == 0 && columnTop > 0 && minValue < columnTop) {
            // Rows before columnTop predate the column, so the index holds no entry for them and
            // getCursor() synthesizes them through NullCursor. Mirror its nullCount exactly. Those
            // synthetic row ids all sit below columnTop while every key-0 posting sits at or above
            // it, so this term and the posting count below cannot double-count a row.
            final long nullCount = Math.min(columnTop, maxValue == Long.MAX_VALUE ? Long.MAX_VALUE : maxValue + 1);
            total += Math.max(0, nullCount - minValue);
        }
        if (key >= keyCount) {
            // Past the key count the index addresses nothing, so the null prefix is the whole answer.
            return total;
        }

        final long offset = BitmapIndexUtils.getKeyEntryOffset(key);
        keyMem.extend(offset + BitmapIndexUtils.KEY_ENTRY_SIZE);
        // Same seqlock protocol as Cursor.of(): read the value count first and last and retry while
        // the two disagree, so the block offsets read in between belong to one consistent entry.
        long valueCount;
        long firstValueBlockOffset;
        long lastValueBlockOffset;
        final long deadline = clock.getTicks() + spinLockTimeoutMs;
        while (true) {
            valueCount = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_VALUE_COUNT);

            Unsafe.loadFence();
            if (keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_COUNT_CHECK) == valueCount) {
                firstValueBlockOffset = keyMem.getLong(offset + BitmapIndexUtils.KEY_ENTRY_OFFSET_FIRST_VALUE_BLOCK_OFFSET);
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

        if (valueCount == 0) {
            return total;
        }
        valueMem.extend(lastValueBlockOffset + blockCapacity);
        final int hopsBelowMin = BitmapIndexUtils.seekValueBlockLTR(
                valueCount,
                firstValueBlockOffset,
                valueMem,
                minValue,
                blockValueCountMod,
                rangeCountSeeker,
                maxBlockHops
        );
        if (hopsBelowMin == BitmapIndexUtils.BLOCK_HOP_BUDGET_EXHAUSTED) {
            countBlockHops(maxBlockHops);
            return AbstractPostingIndexReader.ESTIMATE_REJECT;
        }
        countBlockHops(hopsBelowMin);
        final long countBelowMin = rangeCountSeeker.count;
        final int hopsAtOrBelowMax = BitmapIndexUtils.seekValueBlockRTL(
                valueCount,
                lastValueBlockOffset,
                valueMem,
                maxValue,
                blockValueCountMod,
                rangeCountSeeker,
                maxBlockHops - hopsBelowMin
        );
        if (hopsAtOrBelowMax == BitmapIndexUtils.BLOCK_HOP_BUDGET_EXHAUSTED) {
            countBlockHops(maxBlockHops - hopsBelowMin);
            return AbstractPostingIndexReader.ESTIMATE_REJECT;
        }
        countBlockHops(hopsAtOrBelowMax);
        final long countAtOrBelowMax = rangeCountSeeker.count;
        // seekValueBlockLTR reports the whole value count when the posting list runs past the mapped
        // extent of the value file, which is its way of saying it found nothing at or above
        // minValue; the cursor degrades to empty in the same case, so clamp instead of going negative.
        return total + Math.max(0, countAtOrBelowMax - countBelowMin);
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

    private static void countBlockHops(int blockHops) {
        if (isBlockHopCounterEnabled) {
            testRangeCountBlockHops.addAndGet(blockHops);
        }
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