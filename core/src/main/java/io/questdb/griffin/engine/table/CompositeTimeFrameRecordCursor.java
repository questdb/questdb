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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntLongSortedList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * Builds, in ONE lazy forward pass over the whole table, a per-DAY designated-timestamp-sorted
 * PERMUTATION over a composite table's sibling cells -- the foundation of a composite {@code
 * TimeFrameCursor} (the random-access method surface itself -- {@code next}/{@code prev}/{@code
 * jumpTo}/{@code open}/{@code seekEstimate} -- is added over this by a follow-up task; this class
 * stops at day enumeration + permutation).
 * <p>
 * A composite table stores rows in per-cell partition subdirectories WITHIN each time partition
 * ("day"); the underlying page-frame cursor arrives {@code (partition-timestamp ASC, cellKey ASC)}
 * -- all cells of one day contiguous, each cell internally designated-timestamp ordered, but the
 * cells of a day are SEPARATE on disk. This class REUSES the per-day cross-cell k-way merge
 * mechanics of {@link CompositeMergePartitionRecordCursor} (mirrored here, not extracted into a
 * shared helper, specifically so that class's live record-cursor behaviour is left byte-for-byte
 * untouched -- see its class doc): for each day, it groups the day's sibling cells into {@link
 * CellIter}s and drains a poll-and-replace min-heap ({@link IntLongSortedList}, keyed on designated
 * timestamp) -- but instead of yielding one row per {@code hasNext()} call to a downstream consumer,
 * it RECORDS each winner's {@code packed(cellFrameIndex, cellRowIndex)} (a single {@code long}) into
 * that day's slice of a table-wide {@link DirectLongList} permutation; the append index within the
 * day is the mergedOrdinal. Native page-frame column memory stays valid for the whole query
 * (addresses live in the query-lifetime {@link PageFrameAddressCache}), so a permutation entry can be
 * read back at any later time via {@link #recordAt(Record, int, long)} with zero row copies.
 * <p>
 * Per-day arrays (indexed by dayIndex) record: the permutation offset, the row count, the actual
 * observed first/last designated timestamp (tsLo/tsHi), and the day's ESTIMATED exclusive upper
 * timestamp bound ("ceiling", mirroring {@link TimeFrameCursorImpl#estimatePartitionHi}, reused
 * as-is). A day group that seeds zero rows (every sibling cell empty) is skipped, matching {@link
 * CompositeMergePartitionRecordCursor}'s own defensive skip. A single-cell day degenerates to a
 * heap-of-one, so its permutation slice is the IDENTITY permutation over that cell's own physical
 * row order.
 * <p>
 * Native partitions only -- a Parquet frame raises the same kind of {@link CairoException} as
 * {@link CompositeMergePartitionRecordCursor}. Forward (ASC) only: the permutation is built once
 * from a single ascending scan; a later task's backward/random access reads the SAME array in
 * reverse / by index -- it never rebuilds a max-heap (mirrors {@code TimeFrameCursorImpl}'s "the
 * only supported partition BUILD order is forward" contract).
 * <p>
 * Thread-unsafe, single query lifetime -- {@link #of} rearms the cursor for a fresh (lazy) build;
 * not safe to share across threads or queries concurrently.
 */
public class CompositeTimeFrameRecordCursor implements QuietCloseable {
    private final ObjList<CellIter> cellPool = new ObjList<>();
    // Per-day (indexed by dayIndex): the day's ESTIMATED exclusive upper timestamp bound.
    private final LongList dayCeiling = new LongList();
    // Per-day: offset of the day's first entry in `permutation`.
    private final LongList dayOffset = new LongList();
    // Per-day: number of entries (rows) belonging to the day.
    private final LongList dayRowCount = new LongList();
    // Per-day: the ACTUAL (observed) highest/lowest designated timestamp emitted for the day.
    private final LongList dayTsHi = new LongList();
    private final LongList dayTsLo = new LongList();
    private final PageFrameAddressCache frameAddressCache;
    private final PageFrameMemoryPool frameMemoryPool;
    // Min-ordered heap over the CURRENT day group: (cellSlot -> designated ts). Forward-only, so,
    // unlike CompositeMergePartitionRecordCursor's heap, the key is never negated.
    private final IntLongSortedList heap = new IntLongSortedList();
    private final RecordMetadata metadata;
    // Table-wide, ts-sorted-per-day permutation: packed(cellFrameIndex, cellRowIndex), appended in
    // mergedOrdinal order within each day's slice [dayOffset[i], dayOffset[i] + dayRowCount[i]).
    private final DirectLongList permutation;
    // Reads a cell's candidate designated timestamp while building; never handed to a caller
    // (mirrors CompositeMergePartitionRecordCursor's probeRecord / recordA split).
    private final PageFrameMemoryRecord probeRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
    private final PageFrameMemoryRecord recordA = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_A_LETTER);
    private final int timestampIndex;
    private int cellCount;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // The day group most recently loaded by loadNextDayGroup() -- its own partition timestamp.
    private long currentDayTs;
    private int dayCount;
    private int frameCount;
    private TablePageFrameCursor frameCursor;
    // True when pullFrame() already fetched the first frame of the NEXT day group (look-ahead),
    // mirroring CompositeMergePartitionRecordCursor's identical scratch fields below.
    private boolean hasPending;
    private boolean isPermutationBuilt;
    private int pulledFrameIndex;
    private long pulledFrameSize;
    private int pulledPartitionIndex;
    private long pulledTs;
    private TableReader reader;

    public CompositeTimeFrameRecordCursor(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata
    ) {
        this.metadata = metadata;
        this.timestampIndex = metadata.getTimestampIndex();
        try {
            this.frameAddressCache = new PageFrameAddressCache();
            this.frameMemoryPool = new PageFrameMemoryPool(configuration.getSqlParquetCacheMemorySize());
            // Deferred allocation (keepClosed=true); buildPermutation() reopen()s on first use,
            // mirroring TimeFrameCursorImpl's frameRowCounts/framePartitionIndexes.
            this.permutation = new DirectLongList(1024, MemoryTag.NATIVE_DEFAULT, true);
        } catch (Throwable th) {
            close();
            throw th;
        }
    }

    /**
     * Decodes the frame index packed by {@link #getPermutationEntry(int, long)}.
     */
    public static int unpackFrameIndex(long packedEntry) {
        return (int) (packedEntry >>> 32);
    }

    /**
     * Decodes the frame-relative row index packed by {@link #getPermutationEntry(int, long)}.
     */
    public static long unpackRowIndex(long packedEntry) {
        return packedEntry & 0xFFFFFFFFL;
    }

    @Override
    public void close() {
        Misc.free(frameMemoryPool);
        Misc.free(frameAddressCache);
        Misc.free(permutation);
        Misc.free(probeRecord);
        Misc.free(recordA);
    }

    /**
     * @return the day's ESTIMATED exclusive upper timestamp bound (mirrors a plain table's
     * partition ceiling; may be larger than {@link #getDayTsHi(int)} -- see {@code TimeFrame}'s
     * estimate-vs-actual distinction).
     */
    public long getDayCeiling(int dayIndex) {
        buildPermutation();
        return dayCeiling.getQuick(dayIndex);
    }

    /**
     * @return the number of days (merged time frames) in the table.
     */
    public int getDayCount() {
        buildPermutation();
        return dayCount;
    }

    /**
     * @return the offset of day {@code dayIndex}'s first entry in the permutation.
     */
    public long getDayOffset(int dayIndex) {
        buildPermutation();
        return dayOffset.getQuick(dayIndex);
    }

    /**
     * @return the number of rows (permutation entries) in day {@code dayIndex}.
     */
    public long getDayRowCount(int dayIndex) {
        buildPermutation();
        return dayRowCount.getQuick(dayIndex);
    }

    /**
     * @return the actual (observed) highest designated timestamp in day {@code dayIndex}, inclusive.
     */
    public long getDayTsHi(int dayIndex) {
        buildPermutation();
        return dayTsHi.getQuick(dayIndex);
    }

    /**
     * @return the actual (observed) lowest designated timestamp in day {@code dayIndex}, inclusive.
     */
    public long getDayTsLo(int dayIndex) {
        buildPermutation();
        return dayTsLo.getQuick(dayIndex);
    }

    /**
     * @return day {@code dayIndex}'s {@code ordinal}-th entry in ts-sorted (mergedOrdinal) order,
     * packed as {@code (cellFrameIndex << 32) | cellRowIndex}; decode with {@link
     * #unpackFrameIndex(long)} / {@link #unpackRowIndex(long)}, or read the row directly via {@link
     * #recordAt(Record, int, long)}.
     */
    public long getPermutationEntry(int dayIndex, long ordinal) {
        buildPermutation();
        return permutation.get(dayOffset.getQuick(dayIndex) + ordinal);
    }

    /**
     * @return the record {@link #recordAt(Record, int, long)} is typically called with.
     */
    public Record getRecord() {
        return recordA;
    }

    public CompositeTimeFrameRecordCursor of(TablePageFrameCursor frameCursor, SqlExecutionContext executionContext) {
        this.frameCursor = frameCursor;
        this.reader = frameCursor.getTableReader();
        frameAddressCache.of(metadata, frameCursor.getColumnMapping(), frameCursor.isExternal());
        frameMemoryPool.setMemoryTracker(executionContext.getMemoryTracker());
        frameMemoryPool.of(frameAddressCache);
        probeRecord.of(frameCursor);
        recordA.of(frameCursor);
        this.circuitBreaker = executionContext.getCircuitBreaker();
        isPermutationBuilt = false;
        return this;
    }

    /**
     * Positions {@code record} at the given frame/row: the read mechanism a permutation entry
     * decodes to (mirrors {@link CompositeMergePartitionRecordCursor#hasNext()}'s OUTPUT bind --
     * read-only, zero-copy, since native addresses live in the query-lifetime {@link
     * PageFrameAddressCache}).
     */
    public void recordAt(Record record, int frameIndex, long rowIndex) {
        final PageFrameMemoryRecord frameMemoryRecord = (PageFrameMemoryRecord) record;
        frameMemoryPool.navigateTo(frameIndex, frameMemoryRecord);
        frameMemoryRecord.setRowIndex(rowIndex);
    }

    private CellIter acquireCell() {
        if (cellCount == cellPool.size()) {
            cellPool.add(new CellIter());
        }
        final CellIter c = cellPool.getQuick(cellCount++);
        c.reset();
        return c;
    }

    /**
     * Builds the whole-table permutation in a single lazy forward pass, memoized until the next
     * {@link #of}. Mirrors {@link TimeFrameCursorImpl#buildFrameCache()}'s lazy-build contract.
     */
    private void buildPermutation() {
        if (isPermutationBuilt) {
            return;
        }
        permutation.reopen();
        permutation.clear();
        dayOffset.clear();
        dayRowCount.clear();
        dayTsLo.clear();
        dayTsHi.clear();
        dayCeiling.clear();
        dayCount = 0;
        cellCount = 0;
        heap.clear();
        hasPending = false;
        frameCount = 0;
        frameCursor.toTop();

        final TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(
                reader.getMetadata().getTimestampType(),
                reader.getPartitionedBy()
        );

        while (loadNextDayGroup()) {
            final long dayTs = currentDayTs;
            final long offset = permutation.size();
            long tsLo = Long.MIN_VALUE;
            long tsHi = Long.MIN_VALUE;
            boolean any = false;
            while (heap.hasNext()) {
                final int winner = heap.peekIndex();
                final CellIter cell = cellPool.getQuick(winner);
                final long ts = cell.currentTs;
                if (!any) {
                    tsLo = ts;
                    any = true;
                }
                tsHi = ts;
                permutation.add(pack(cell.currentFrameIndex, cell.currentFrameRow));
                cell.advance();
                if (cell.exhausted) {
                    heap.pollValue();
                } else {
                    heap.pollAndReplace(winner, cell.currentTs);
                }
            }
            if (any) {
                // hasPending/pulledTs are frozen by loadNextDayGroup() until its NEXT call, so it's
                // safe to read them any time before that next call -- including after draining above.
                final long nextDayTs = hasPending ? pulledTs : Long.MAX_VALUE;
                dayOffset.add(offset);
                dayRowCount.add(permutation.size() - offset);
                dayTsLo.add(tsLo);
                dayTsHi.add(tsHi);
                dayCeiling.add(TimeFrameCursorImpl.estimatePartitionHi(ceilMethod, dayTs, nextDayTs));
                dayCount++;
            }
            // !any: every sibling cell of this day group was empty -- skip, matching
            // CompositeMergePartitionRecordCursor.hasNext()'s do-while over loadNextDayGroup().
        }
        isPermutationBuilt = true;
    }

    /**
     * Loads the next day group's sibling cells and seeds the heap -- a forward-only mirror of
     * {@link CompositeMergePartitionRecordCursor#loadNextDayGroup()} (see that method for the
     * detailed contract); additionally records the group's own partition timestamp into {@link
     * #currentDayTs} so the caller can compute the day's ceiling.
     */
    private boolean loadNextDayGroup() {
        cellCount = 0;
        heap.clear();
        if (!hasPending) {
            if (!pullFrame()) {
                return false; // stream exhausted
            }
        }
        final long dayTs = pulledTs;
        currentDayTs = dayTs;
        int currentPartitionIndex = pulledPartitionIndex;
        CellIter cell = acquireCell();
        cell.addFrame(pulledFrameIndex, pulledFrameSize);
        hasPending = false;
        while (pullFrame()) {
            if (pulledTs != dayTs) {
                hasPending = true; // belongs to the next day group; buffered as one-frame look-ahead
                break;
            }
            if (pulledPartitionIndex != currentPartitionIndex) {
                currentPartitionIndex = pulledPartitionIndex; // sibling cell of the same day
                cell = acquireCell();
            }
            cell.addFrame(pulledFrameIndex, pulledFrameSize);
        }
        for (int s = 0; s < cellCount; s++) {
            final CellIter c = cellPool.getQuick(s);
            c.start();
            if (!c.exhausted) {
                heap.add(s, c.currentTs);
            }
        }
        return true;
    }

    // Fetches the next frame into the pulledXxx scratch, registering it in the address cache under
    // a fresh, monotonically increasing frame index. Mirrors
    // CompositeMergePartitionRecordCursor.pullFrame(); returns false at end of stream.
    private boolean pullFrame() {
        final PageFrame frame = frameCursor.next();
        if (frame == null) {
            return false;
        }
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        if (frame.getFormat() != PartitionFormat.NATIVE) {
            throw CairoException.critical(0)
                    .put("composite time-frame permutation supports native partitions only [table=")
                    .put(reader.getTableToken().getTableName())
                    .put(']');
        }
        pulledFrameIndex = frameCount;
        frameAddressCache.add(frameCount, frame);
        frameCount++;
        pulledPartitionIndex = frame.getPartitionIndex();
        pulledFrameSize = frame.getPartitionHi() - frame.getPartitionLo();
        pulledTs = reader.getPartitionTimestampByIndex(pulledPartitionIndex);
        return true;
    }

    // Packs a (frameIndex, frame-relative rowIndex) pair into one long: frameIndex occupies the
    // full high 32 bits (it's already an int -- no table is large enough to overflow that), and a
    // frame's row count -- bounded in practice by the page-frame-size configuration -- fits
    // comfortably in the low 32 bits.
    private static long pack(int frameIndex, long rowIndex) {
        return (((long) frameIndex) << 32) | (rowIndex & 0xFFFFFFFFL);
    }

    /**
     * Iterates the frames of ONE cell within the current day group, forward only, exposing one
     * "current" designated timestamp at a time for the heap. Forward-only mirror of {@link
     * CompositeMergePartitionRecordCursor.CellIter} (that class also supports a backward scan;
     * this one doesn't need to -- the permutation is always built by a single ascending pass).
     */
    private final class CellIter {
        // Flat pairs [frameIndex, frameSize] for this cell's frames in arrival order.
        private final LongList frames = new LongList();
        private int currentFrameIndex;
        private long currentFrameHi;  // size of the current frame
        private long currentFrameRow; // frame-relative row index within [0, currentFrameHi)
        private long currentTs;
        private boolean exhausted;
        private int frameSlot;        // index into frames (stride 2) of the current frame

        void addFrame(int frameIndex, long frameSize) {
            frames.add(frameIndex);
            frames.add(frameSize);
        }

        void advance() {
            currentFrameRow++;
            if (currentFrameRow >= currentFrameHi) {
                if (!moveToNextFrame()) {
                    exhausted = true;
                    return;
                }
                currentFrameRow = 0;
            }
            readTs();
        }

        void reset() {
            frames.clear();
            frameSlot = -2;
            currentFrameIndex = -1;
            currentFrameHi = 0;
            currentFrameRow = -1;
            currentTs = 0;
            exhausted = false;
        }

        void start() {
            frameSlot = -2;
            if (!moveToNextFrame()) {
                exhausted = true;
                return;
            }
            currentFrameRow = 0;
            readTs();
        }

        // Advances frameSlot to the next non-empty frame; returns false when the cell has no more frames.
        private boolean moveToNextFrame() {
            frameSlot += 2;
            while (frameSlot < frames.size()) {
                final long size = frames.getQuick(frameSlot + 1);
                if (size > 0) {
                    currentFrameIndex = (int) frames.getQuick(frameSlot);
                    currentFrameHi = size;
                    return true;
                }
                frameSlot += 2;
            }
            return false;
        }

        private void readTs() {
            frameMemoryPool.navigateTo(currentFrameIndex, probeRecord);
            probeRecord.setRowIndex(currentFrameRow);
            currentTs = probeRecord.getLong(timestampIndex);
        }
    }
}
