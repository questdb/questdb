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

package io.questdb.cairo;

import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.model.RuntimeIntrinsicIntervalModel;
import io.questdb.std.IntHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.std.Vect.BIN_SEARCH_SCAN_UP;

public abstract class AbstractIntervalPartitionFrameCursor implements PartitionFrameCursor {
    protected final IntervalPartitionFrame frame = new IntervalPartitionFrame();
    protected final RuntimeIntrinsicIntervalModel intervalModel;
    protected final ParquetPartitionDecoder parquetDecoder;
    protected final int timestampIndex;
    private final NativeTimestampFinder nativeTimestampFinder = new NativeTimestampFinder();
    private final ParquetTimestampFinder parquetTimestampFinder;
    // Task 5b: set by the owning factory (see PartitionFrameCursorFactory#setAllowedCellKeys) right
    // before this cursor is handed out; null means "no pruning" (every plain table, and every composite
    // query whose predicate was not resolved to a dimension cellKey set).
    protected @Nullable IntHashSet allowedCellKeys;
    protected LongList intervals;
    protected int intervalsHi;
    protected int intervalsLo;
    protected int partitionHi;
    // This is where begin binary search on partition. When there are more
    // than one searches to be performed we can use this variable to avoid
    // searching partition from top every time
    protected long partitionLimit;
    protected int partitionLo;
    // 9A day-run state. A "run" is the maximal set of partitions sharing one partition timestamp --
    // i.e. all cells of one day, which are CONTIGUOUS in partition-index order (asserted directly by
    // CompositeDayRunUnitTest). Both concrete cursors walk a run CELL-MAJOR: every cell restarts at
    // runIntervalLo, so every cell sees every interval, and runResume carries the one interval index
    // the run resumes the global walk at. For a PLAIN table every run is exactly one partition, so the
    // inner walk runs once and reduces to the pre-9A walk -- which is what keeps plain byte-identical
    // without a composite-detection branch. -1/-1 means "no run open".
    protected int runHi = -1;
    protected int runIntervalLo;
    protected int runLo = -1;
    protected int runResume;
    protected TableReader reader;
    protected long sizeSoFar = 0;
    private int initialIntervalsHi;
    private int initialIntervalsLo;
    private int initialPartitionHi;
    private int initialPartitionLo;

    public AbstractIntervalPartitionFrameCursor(CairoConfiguration configuration, RuntimeIntrinsicIntervalModel intervalModel, int timestampIndex) {
        assert timestampIndex > -1;
        this.intervalModel = intervalModel;
        this.timestampIndex = timestampIndex;
        this.parquetDecoder = configuration.newParquetPartitionDecoder();
        this.parquetTimestampFinder = new ParquetTimestampFinder(parquetDecoder);
    }

    @Override
    public void close() {
        Misc.free(parquetTimestampFinder);
        Misc.free(parquetDecoder);
        nativeTimestampFinder.clear();
        reader = Misc.free(reader);
    }

    /**
     * The WHOLE resolved interval list, deliberately - not the
     * {@code [intervalsLo, intervalsHi)} sub-range this cursor actually walks.
     * {@link #cullIntervals} narrows those bounds against the READER's timestamp range, so
     * they describe where this table's rows can be, not what the filter admits. A caller
     * applying the filter to rows of its own (a live view's in-memory tier holds output
     * rows the LV table has not been flushed yet, i.e. rows ABOVE the reader's maximum)
     * would drop every one of them by honouring the culled bounds. The cull is an
     * optimisation over one row source; the list is the filter.
     */
    @Override
    public LongList getIntervals() {
        return intervals;
    }

    @Override
    public SymbolMapReader getSymbolTable(int columnIndex) {
        return reader.getSymbolMapReader(columnIndex);
    }

    @Override
    public TableReader getTableReader() {
        return reader;
    }

    /**
     * Composite-partitioning (Task 6c review, Part A) LOUD scope boundary for the one interval-scan shape
     * these cursors cannot yet serve correctly: 2+ time intervals that each fall within (or reach into)
     * the SAME multi-cell day. Both concrete cursors walk partitions (cells) and intervals strictly
     * MONOTONICALLY, so once an earlier interval consumes a multi-cell day's cells a later interval can
     * never revisit that day's earlier cells -- their matching rows would be SILENTLY dropped. Commit
     * {@code d31aa88716} fixed the SINGLE-interval sibling visit; multiple sub-day intervals over one
     * multi-cell day remained broken. A real fix would have to iterate cells and intervals as a 2D grid
     * (per-cell interval reset) while still emitting frames in the day-contiguous, per-cell-contiguous
     * order the downstream {@code CompositeMergePartitionRecordCursor} requires -- too invasive to do
     * safely within this review without risking a subtly-wrong scan, so this shape is LOUD-GATED instead.
     * Each cursor throws this at the EXACT point it detects the drop is imminent: a fragmented cell of a
     * multi-cell day about to be abandoned for a sibling while a LATER interval still reaches into that
     * cell's own timestamp span (proven to fire on every actual drop, and never on a correct multi-DAY
     * date-list, single-interval, or non-interleaving scan). Never reachable for a plain table (its
     * neighbour partition is always a distinct day, so the same-timestamp sibling precondition is never
     * met) -- plain interval scans stay byte-identical. Workaround: issue one interval per query, or widen
     * the predicate to whole days.
     */
    protected CairoException multipleSubDayIntervalsOverMultiCellDayUnsupported() {
        return CairoException.critical(0)
                .put("composite partitioning does not yet support multiple sub-day time intervals over a single multi-cell day; ")
                .put("issue one interval per query, or widen the range to whole days [table=")
                .put(reader.getTableToken().getTableName())
                .put(']');
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public boolean hasIntervalFilter() {
        return true;
    }

    /**
     * Opens the day-run beginning at {@code partitionHi - 1} for a BACKWARD walk. Mirror of
     * {@link #beginForwardRun()}: the run is entered from its top, every cell of it is walked from
     * {@code runIntervalLo} downward, and {@code runResume} accumulates the MAXIMUM interval bound the
     * run's cells reach. The maximum, because walking downward an interval that reaches BELOW this day
     * must stay live for the next (earlier) day.
     */
    protected void beginBackwardRun() {
        runHi = partitionHi;
        runLo = backwardRunStart(partitionHi - 1, partitionLo);
        runIntervalLo = intervalsHi;
        runResume = intervalsLo;
    }

    /**
     * Opens the day-run beginning at {@code partitionLo} for a FORWARD walk. Every cell of the run is
     * walked from {@code runIntervalLo}, so each cell sees every interval -- the monotonic constraint
     * that produced this cursor family's three silent-wrong-answer defects is gone.
     * <p>
     * {@code runResume} accumulates the MINIMUM interval index the run's cells reach, and becomes the
     * global {@code intervalsLo} once the run completes. The minimum, not the last cell's index: an
     * interval reaching past this day must stay live for the next one, and taking the last cell's index
     * would retire it early and silently drop its rows -- exactly the defect class 9A exists to end.
     */
    protected void beginForwardRun() {
        runLo = partitionLo;
        runHi = forwardRunEnd(partitionLo, partitionHi);
        runIntervalLo = intervalsLo;
        runResume = intervalsHi;
    }

    /**
     * First partition index of the day-run containing {@code partitionIndex}, clamped at
     * {@code loBound}. Takes the bound explicitly so {@code calculateSize()} can call it with its own
     * local copy of {@code partitionLo} rather than the field.
     */
    protected int backwardRunStart(int partitionIndex, int loBound) {
        final long ts = reader.getPartitionTimestampByIndex(partitionIndex);
        int start = partitionIndex;
        while (start > loBound && reader.getPartitionTimestampByIndex(start - 1) == ts) {
            start--;
        }
        return start;
    }

    /**
     * One past the last partition index of the day-run containing {@code partitionIndex}, clamped at
     * {@code hiBound}. O(cells-in-day) and called once per run, not per frame. Takes the bound
     * explicitly for the same reason as {@link #backwardRunStart(int, int)}.
     */
    protected int forwardRunEnd(int partitionIndex, int hiBound) {
        final long ts = reader.getPartitionTimestampByIndex(partitionIndex);
        int end = partitionIndex + 1;
        while (end < hiBound && reader.getPartitionTimestampByIndex(end) == ts) {
            end++;
        }
        return end;
    }

    /**
     * Task 5b: {@code true} unless a composite dimension predicate was resolved to an allowed-cellKey
     * set AND this slot's cell is not in it. Every concrete {@code next()}/{@code calculateSize()} in
     * both {@link IntervalFwdPartitionFrameCursor} and {@link IntervalBwdPartitionFrameCursor} composes
     * this with their existing ts culling -- never replaces it. {@code allowedCellKeys == null} (no
     * pruning attempted, or a plain table) short-circuits to {@code true} unconditionally, so this is a
     * zero-cost no-op for every case this task does not touch.
     */
    protected boolean isCellAllowed(int partitionIndex) {
        return allowedCellKeys == null || allowedCellKeys.contains(reader.getPartitionCellKey(partitionIndex));
    }

    @Override
    public StaticSymbolTable newSymbolTable(int columnIndex) {
        return reader.newSymbolTable(columnIndex);
    }

    public AbstractIntervalPartitionFrameCursor of(TableReader reader, SqlExecutionContext sqlExecutionContext) throws SqlException {
        parquetTimestampFinder.setMemoryTracker(sqlExecutionContext != null ? sqlExecutionContext.getMemoryTracker() : null);
        this.intervals = intervalModel.calculateIntervals(sqlExecutionContext);
        calculateRanges(reader, intervals);
        this.reader = reader;
        return this;
    }

    /**
     * Task 5b: see {@link io.questdb.cairo.sql.PartitionFrameCursorFactory#setAllowedCellKeys}'s own doc.
     * Called by the owning factory on every {@code getCursor()}, not just once, since this cursor
     * instance is cached and reused across executions of the same compiled factory.
     */
    public void setAllowedCellKeys(@Nullable IntHashSet allowedCellKeys) {
        this.allowedCellKeys = allowedCellKeys;
    }

    @TestOnly
    @Override
    public boolean reload() {
        if (reader != null && reader.reload()) {
            calculateRanges(reader, intervals);
            return true;
        }
        return false;
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public boolean supportsSizeCalculation() {
        return true;
    }

    @Override
    public void toTop() {
        parquetTimestampFinder.clear();
        nativeTimestampFinder.clear();
        intervalsLo = initialIntervalsLo;
        intervalsHi = initialIntervalsHi;
        partitionLo = initialPartitionLo;
        partitionHi = initialPartitionHi;
        sizeSoFar = 0;
        // 9A: -1/-1 is "no run open" -- both concrete cursors open one lazily on the next call. Every
        // early return from next() is a resumption point, so this reset is what makes a re-scan from
        // the top identical to a first scan.
        runLo = -1;
        runHi = -1;
        runIntervalLo = 0;
        runResume = 0;
    }

    private void calculateRanges(TableReader reader, LongList intervals) {
        if (intervals.size() > 0) {
            if (PartitionBy.isPartitioned(reader.getPartitionedBy())) {
                cullIntervals(reader, intervals);
                if (initialIntervalsLo < initialIntervalsHi) {
                    cullPartitions(reader, intervals);
                }
            } else {
                initialIntervalsLo = 0;
                initialIntervalsHi = intervals.size() / 2;
                initialPartitionLo = 0;
                initialPartitionHi = reader.getPartitionCount();
            }
        } else {
            initialIntervalsLo = 0;
            initialIntervalsHi = 0;
            initialPartitionLo = 0;
            initialPartitionHi = 0;
        }
        toTop();
    }

    private void cullIntervals(TableReader reader, LongList intervals) {
        int intervalsLo = intervals.binarySearch(reader.getMinTimestamp(), BIN_SEARCH_SCAN_UP);

        // not a direct hit
        if (intervalsLo < 0) {
            intervalsLo = -intervalsLo - 1;
        }

        // normalise interval index
        this.initialIntervalsLo = intervalsLo / 2;

        if (reader.getMaxTimestamp() == intervals.getQuick(intervals.size() - 1)) {
            this.initialIntervalsHi = intervals.size() / 2;
        } else if (reader.getMaxTimestamp() == intervals.getQuick(0)) {
            this.initialIntervalsHi = 1;
        } else {
            int intervalsHi = intervals.binarySearch(reader.getMaxTimestamp(), BIN_SEARCH_SCAN_UP);
            if (intervalsHi < 0) { // negative value means inexact match
                intervalsHi = -intervalsHi - 1;

                // when interval index is "even" we scored just between two interval
                // in which case we chose previous interval
                if (intervalsHi % 2 == 0) {
                    this.initialIntervalsHi = intervalsHi / 2;
                } else {
                    this.initialIntervalsHi = intervalsHi / 2 + 1;
                }
            } else { // positive value means exact match
                this.initialIntervalsHi = intervalsHi / 2 + 1;
            }
        }
    }

    private void cullPartitions(TableReader reader, LongList intervals) {
        final long lo = intervals.getQuick(initialIntervalsLo * 2);
        long intervalLo;
        if (lo == Long.MIN_VALUE) {
            intervalLo = reader.floorToPartitionTimestamp(reader.getMinTimestamp());
        } else {
            intervalLo = reader.floorToPartitionTimestamp(lo);
        }
        this.initialPartitionLo = reader.getMinTimestamp() < intervalLo ? reader.getPartitionIndexByTimestamp(intervalLo) : 0;
        long intervalHi = reader.floorToPartitionTimestamp(intervals.getQuick((initialIntervalsHi - 1) * 2 + 1));
        // High boundary must resolve to the LAST (highest cellKey) partition sharing intervalHi's
        // timestamp, not the first -- a composite table's multi-cell day would otherwise have its
        // cellKey >= 1 siblings excluded by the "+1" below. getPartitionIndexByTimestampScanDown is
        // byte-identical to getPartitionIndexByTimestamp for a plain table (one cell/day) and for any
        // not-found (between-days) boundary -- see TableReader#getPartitionIndexByTimestampScanDown's
        // own javadoc.
        this.initialPartitionHi = Math.min(reader.getPartitionCount(), reader.getPartitionIndexByTimestampScanDown(intervalHi) + 1);
    }

    protected TimestampFinder initTimestampFinder(int partitionIndex, long rowCount) {
        if (reader.getPartitionFormatFromMetadata(partitionIndex) == PartitionFormat.PARQUET) {
            return parquetTimestampFinder.of(reader, partitionIndex, timestampIndex);
        }
        return nativeTimestampFinder.of(reader, partitionIndex, timestampIndex, rowCount);
    }

    protected static class IntervalPartitionFrame implements PartitionFrame {
        protected byte format;
        protected ParquetPartitionDecoder parquetMetaDecoder;
        protected int partitionIndex;
        protected long rowHi;
        protected long rowLo;

        @Override
        public ParquetPartitionDecoder getParquetMetaDecoder() {
            return parquetMetaDecoder;
        }

        @Override
        public byte getPartitionFormat() {
            return format;
        }

        @Override
        public int getPartitionIndex() {
            return partitionIndex;
        }

        @Override
        public long getRowHi() {
            return rowHi;
        }

        @Override
        public long getRowLo() {
            return rowLo;
        }
    }
}
