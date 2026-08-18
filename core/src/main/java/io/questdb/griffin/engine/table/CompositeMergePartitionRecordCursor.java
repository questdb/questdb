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
import io.questdb.cairo.TableReader;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntLongSortedList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Per-day k-way cross-cell merge record cursor for composite (time + non-time dimension) tables.
 * <p>
 * A composite table stores rows in per-cell partition subdirectories WITHIN each time partition. The
 * underlying page-frame cursor arrives {@code (partition-timestamp ASC, cellKey ASC)} (or fully reversed
 * for a backward scan): all cells of one day are contiguous, each cell internally designated-timestamp
 * ordered, but the cells of a day are SEPARATE on disk, so a plain scan emits, per day,
 * {@code cell0 ++ cell1 ++ ...} -- globally MISORDERED whenever cells interleave in time. This cursor
 * merges the sibling cells of one day (a "day group") by designated timestamp with a poll-and-replace heap
 * (mirroring {@link HeapRowCursor} + {@link IntLongSortedList}), so the emitted stream is genuinely
 * globally designated-timestamp ordered (ASC forward, DESC backward).
 * <p>
 * Native page-frame column memory stays valid for the whole query (the addresses live in the query-lifetime
 * {@link io.questdb.cairo.sql.PageFrameAddressCache}), so this cursor holds two {@link PageFrameMemoryRecord}s
 * against arbitrary frames simultaneously with zero row copies: {@link #recordA} (the OUTPUT record the
 * consumer reads, bound to the winning cell's current row) and {@link #probeRecord} (used only to read each
 * cell's candidate designated timestamp while advancing it). Parquet frames pin at most a couple of decode
 * slots, so composite-parquet is not supported here (and is gated on the write/convert side); a non-native
 * frame raises a clear {@link CairoException}.
 * <p>
 * A day group with a single cell (a never-routed composite, or a day that happens to have one dimension
 * value) is a heap of one, i.e. pass-through identity -- output-identical to the plain twin.
 */
class CompositeMergePartitionRecordCursor extends AbstractPageFrameRecordCursor {
    // Per-cell iterators of the CURRENT day group, indexed 0..cellCount-1; grown and reused across days.
    private final ObjList<CellIter> cellPool = new ObjList<>();
    private final boolean forward;
    // Min-ordered heap: (cellSlot -> heapKey). One live entry per non-exhausted sibling cell of the current
    // day group. heapKey is the designated ts (forward) or its negation (backward, so min-of-negated == max).
    private final IntLongSortedList heap = new IntLongSortedList();
    // Transient record used only to read a cell's candidate designated timestamp; never handed to the
    // consumer, so it never disturbs the OUTPUT record (recordA) or recordB.
    private final PageFrameMemoryRecord probeRecord = new PageFrameMemoryRecord(PageFrameMemoryRecord.RECORD_B_LETTER);
    private final int timestampIndex;
    private int cellCount;
    private SqlExecutionCircuitBreaker circuitBreaker;
    // True when pullFrame() has already fetched the first frame of the NEXT day group and buffered its
    // metadata in the pulledXxx scratch fields.
    private boolean hasPending;
    private int pulledFrameIndex;
    private long pulledFrameSize;
    private int pulledPartitionIndex;
    private long pulledTs;
    private TableReader reader;

    CompositeMergePartitionRecordCursor(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            int timestampIndex,
            boolean forward
    ) {
        super(configuration, metadata);
        this.timestampIndex = timestampIndex;
        this.forward = forward;
    }

    @Override
    public void close() {
        Misc.free(probeRecord);
        super.close();
    }

    @Override
    public boolean hasNext() {
        if (!heap.hasNext()) {
            // Drain complete for the current day group; advance to the next. The while-loop skips a
            // (defensive) day group that seeded no rows, e.g. every cell empty.
            do {
                if (!loadNextDayGroup()) {
                    return false;
                }
            } while (!heap.hasNext());
        }
        final int winner = heap.peekIndex();
        final CellIter cell = cellPool.getQuick(winner);
        // OUTPUT: bind recordA (returned by getRecord()) to the winner's current row. Native addresses come
        // from the query-lifetime address cache, so this binding survives the probeRecord navigation inside
        // advance() below -- no row copy.
        frameMemoryPool.navigateTo(cell.currentFrameIndex, recordA);
        recordA.setRowIndex(cell.currentFrameRow);
        // Advance the winner past the row just emitted, then re-heap its new candidate (or drop it).
        cell.advance();
        if (cell.exhausted) {
            heap.pollValue();
        } else {
            heap.pollAndReplace(winner, heapKey(cell.currentTs));
        }
        return true;
    }

    @Override
    public void of(PageFrameCursor frameCursor, SqlExecutionContext executionContext) throws SqlException {
        if (this.frameCursor != frameCursor) {
            close();
            this.frameCursor = frameCursor;
        }
        this.reader = ((TablePageFrameCursor) frameCursor).getTableReader();
        recordA.of(frameCursor);
        recordB.of(frameCursor);
        probeRecord.of(frameCursor);
        this.circuitBreaker = executionContext.getCircuitBreaker();
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        resetMergeState();
        // frameAddressCache.of(...) + frameMemoryPool.of(...) + frameCount=0 + frameCursor.toTop()
        super.init(executionContext.getMemoryTracker());
    }

    @Override
    public long preComputedStateSize() {
        return 0;
    }

    @Override
    public long size() {
        // The merge preserves the row count; the underlying frame cursor already knows it.
        return frameCursor.size();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Composite cross-cell merge scan");
    }

    @Override
    public void toTop() {
        super.toTop(); // frameCount=0; frameCursor.toTop()
        resetMergeState();
    }

    private CellIter acquireCell() {
        if (cellCount == cellPool.size()) {
            cellPool.add(new CellIter());
        }
        final CellIter c = cellPool.getQuick(cellCount++);
        c.reset();
        return c;
    }

    private long heapKey(long ts) {
        // IntLongSortedList is a MIN-ordered structure. Forward wants the smallest ts first; backward wants
        // the largest, so key on -ts. A designated timestamp is never Long.MIN_VALUE (that is the NULL
        // sentinel, disallowed for a designated timestamp column), so the negation never overflows.
        return forward ? ts : -ts;
    }

    private boolean loadNextDayGroup() {
        cellCount = 0;
        heap.clear();
        if (!hasPending) {
            if (!pullFrame()) {
                return false; // stream exhausted
            }
        }
        // The pulledXxx scratch holds this day group's first frame (either the buffered look-ahead or a
        // fresh pull). pulledTs is the day's calendar FLOOR (see pullFrame), so a split fragment of this
        // day groups with it rather than being mistaken for the next day.
        final long dayTs = pulledTs;
        int currentPartitionIndex = pulledPartitionIndex;
        CellIter cell = acquireCell();
        cell.addFrame(pulledFrameIndex, pulledFrameSize);
        hasPending = false;
        while (pullFrame()) {
            if (pulledTs != dayTs) {
                hasPending = true; // belongs to the next day group; buffer it as one-frame look-ahead
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
                heap.add(s, heapKey(c.currentTs));
            }
        }
        return true;
    }

    // Fetches the next frame from the underlying cursor into the pulledXxx scratch, registering it in the
    // address cache under a fresh, monotonically increasing frame index (mirroring
    // PageFrameRecordCursorImpl.hasNext). Returns false at end of stream.
    private boolean pullFrame() {
        final PageFrame frame = frameCursor.next();
        if (frame == null) {
            return false;
        }
        // Stay cancellable across a long multi-frame scan (time-throttled, as in the plain cursor).
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
        if (frame.getFormat() != PartitionFormat.NATIVE) {
            throw CairoException.critical(0)
                    .put("composite cross-cell merge supports native partitions only [table=")
                    .put(reader.getTableToken().getTableName())
                    .put(']');
        }
        pulledFrameIndex = frameCount;
        frameAddressCache.add(frameCount, frame);
        frameCount++;
        pulledPartitionIndex = frame.getPartitionIndex();
        pulledFrameSize = frame.getPartitionHi() - frame.getPartitionLo();
        // The CALENDAR FLOOR, not the raw partition timestamp -- this value exists only to decide which
        // frames belong to the same day group, and a SPLIT FRAGMENT of a day carries a different raw
        // timestamp while belonging to that same day. Grouping on the raw value closed the day group as
        // soon as a fragment appeared, emitting the fragment as its own group, which broke global
        // timestamp order in BOTH directions (measured 2026-08-18: `ORDER BY ts` on a 3-cell day with one
        // fragment returned 01:00, 21:00, 22:00, 10:00, 20:00). The fragment is simply one more iterator
        // in the day's heap, which is exactly what the merge below already knows how to handle.
        pulledTs = reader.getTxFile().getLogicalPartitionTimestamp(
                reader.getPartitionTimestampByIndex(pulledPartitionIndex)
        );
        return true;
    }

    private void resetMergeState() {
        cellCount = 0;
        heap.clear();
        hasPending = false;
    }

    /**
     * Iterates the frames of ONE cell within the current day group, in the scan direction, exposing one
     * "current" designated timestamp at a time for the heap. Frames arrive contiguous per cell and already
     * in the correct order for sequential iteration -- forward: low frame first, rows {@code 0..size-1};
     * backward: high frame first, rows {@code size-1..0}. Row indices are FRAME-RELATIVE (the frame's page
     * addresses are rebased to its partition-lo), matching {@link PageFrameFwdRowCursor}/
     * {@link PageFrameBwdRowCursor}.
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
            if (forward) {
                currentFrameRow++;
                if (currentFrameRow >= currentFrameHi) {
                    if (!moveToNextFrame()) {
                        exhausted = true;
                        return;
                    }
                    currentFrameRow = 0;
                }
            } else {
                currentFrameRow--;
                if (currentFrameRow < 0) {
                    if (!moveToNextFrame()) {
                        exhausted = true;
                        return;
                    }
                    currentFrameRow = currentFrameHi - 1;
                }
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
            currentFrameRow = forward ? 0 : currentFrameHi - 1;
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
