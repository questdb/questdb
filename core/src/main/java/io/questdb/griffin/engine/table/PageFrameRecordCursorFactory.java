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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.FullPartitionFrameCursorFactory;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.*;

public class PageFrameRecordCursorFactory extends AbstractPageFrameRecordCursorFactory {
    private final CairoConfiguration configuration;
    private final boolean followsOrderByAdvice;
    private final boolean framingSupported;
    private final boolean singleRowFactory;
    private final boolean supportsRandomAccess;
    protected FwdTableReaderPageFrameCursor fwdPageFrameCursor;
    private int bwdIndexedColumnIndex = -1;
    private PageFrameRecordCursor bwdIndexedRecordCursor;
    private SymbolIndexRowCursorFactory bwdIndexedRowCursorFactory;
    private BwdTableReaderPageFrameCursor bwdPageFrameCursor;
    private PageFrameRecordCursor bwdRecordCursor;
    private PageFrameRecordCursor cursor;
    private Function filter;
    private RowCursorFactory rowCursorFactory;
    private TimeFrameCursorImpl timeFrameCursor;

    public PageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            RecordMetadata metadata,
            PartitionFrameCursorFactory partitionFrameCursorFactory,
            RowCursorFactory rowCursorFactory,
            boolean followsOrderByAdvice,
            // filter included here only for lifecycle management of the latter
            @Nullable Function filter,
            boolean framingSupported,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts,
            boolean supportsRandomAccess,
            boolean singleRowFactory
    ) {
        super(metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);

        this.configuration = configuration;
        this.rowCursorFactory = rowCursorFactory;
        this.cursor = new PageFrameRecordCursorImpl(
                configuration,
                metadata,
                rowCursorFactory,
                rowCursorFactory.isEntity(),
                filter
        );
        this.followsOrderByAdvice = followsOrderByAdvice;
        this.filter = filter;
        this.framingSupported = framingSupported;
        this.supportsRandomAccess = supportsRandomAccess;
        this.singleRowFactory = singleRowFactory;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followsOrderByAdvice;
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        if (framingSupported) {
            PartitionFrameCursor partitionFrameCursor = partitionFrameCursorFactory.getCursor(executionContext, columnIndexes, order);
            if (order == ORDER_ASC || order == ORDER_ANY) {
                return initFwdPageFrameCursor(partitionFrameCursor, executionContext);
            }
            return initBwdPageFrameCursor(partitionFrameCursor, executionContext);
        }
        return null;
    }

    /**
     * Opens this full forward scan at an inclusive timestamp lower bound.
     */
    public RecordCursor getCursorFromTimestamp(SqlExecutionContext executionContext, long timestampLo) throws SqlException {
        return getCursorInTimestampRange(executionContext, timestampLo, Long.MAX_VALUE);
    }

    /**
     * Opens this full forward scan over the timestamp range
     * {@code [timestampLo, timestampHi]}, <b>inclusive of both edges</b>. The
     * underlying interval cursor culls the partitions outside the range and binary
     * searches each boundary partition, so a bounded caller pays for neither the
     * history below {@code timestampLo} nor the tail above {@code timestampHi}.
     * <p>
     * This is the read bound a localized live-view repair plans against: it must
     * not read above its proven convergence boundary {@code H}, and a record-level
     * stop filter would still have visited every partition above it. The caller
     * converts its exclusive {@code H} to the inclusive {@code timestampHi} this
     * takes, carrying an end-of-frame {@code H} as a tag rather than as a
     * timestamp - no {@code long} expresses an exclusive bound one past
     * {@code Long.MAX_VALUE}.
     * <p>
     * {@code timestampLo > timestampHi} is an empty range and yields no row.
     */
    public RecordCursor getCursorInTimestampRange(
            SqlExecutionContext executionContext,
            long timestampLo,
            long timestampHi
    ) throws SqlException {
        if (!(partitionFrameCursorFactory instanceof FullPartitionFrameCursorFactory fullFrameFactory)) {
            throw CairoException.nonCritical().put("timestamp range cursor requires a full partition scan");
        }
        final PartitionFrameCursor partitionFrameCursor = fullFrameFactory.getCursor(
                executionContext,
                columnIndexes,
                timestampLo,
                timestampHi
        );
        final PageFrameCursor frameCursor = initFwdPageFrameCursor(partitionFrameCursor, executionContext);
        try {
            return initRecordCursor(frameCursor, executionContext);
        } catch (Throwable th) {
            frameCursor.close();
            throw th;
        }
    }

    /**
     * Opens this full scan over the timestamp range {@code [timestampLo, timestampHi]},
     * <b>inclusive of both edges</b>, yielding rows in descending designated-timestamp
     * order - the exact reverse of
     * {@link #getCursorInTimestampRange(SqlExecutionContext, long, long)} over the same
     * bounds, ties included.
     * <p>
     * A localized live-view repair reads through this while discovering how far back its
     * dependency floor {@code L} sits: it counts qualifying predecessors per partition
     * key and stops on the row that satisfies the last key still short. Descending order
     * is what makes that stop meaningful - the row it stops on <i>is</i> {@code L}, and
     * the partitions below it were never opened.
     * <p>
     * The descending scan substitutes an entity row cursor of its own, so the factory
     * must be a plain full scan. An index-backed row cursor yields rows in index order
     * rather than in timestamp order, which the caller would read as a predecessor count
     * over the wrong rows. That requirement also keeps the two directions free of shared
     * mutable state - every construction carrying a factory-level filter builds an
     * index-backed row cursor - which is what lets a repair hold an ascending and a
     * descending cursor open at the same time.
     */
    public RecordCursor getCursorInTimestampRangeBackward(
            SqlExecutionContext executionContext,
            long timestampLo,
            long timestampHi
    ) throws SqlException {
        if (!(partitionFrameCursorFactory instanceof FullPartitionFrameCursorFactory fullFrameFactory)) {
            throw CairoException.nonCritical().put("timestamp range cursor requires a full partition scan");
        }
        if (!isBackwardTimestampRangeSupported()) {
            throw CairoException.nonCritical().put("backward timestamp range cursor requires an entity row cursor");
        }
        if (bwdRecordCursor == null) {
            bwdRecordCursor = new PageFrameRecordCursorImpl(
                    configuration,
                    getMetadata(),
                    new PageFrameRowCursorFactory(ORDER_DESC),
                    true,
                    filter
            );
        }
        final PartitionFrameCursor partitionFrameCursor = fullFrameFactory.getCursorBackward(
                executionContext,
                columnIndexes,
                timestampLo,
                timestampHi
        );
        final PageFrameCursor frameCursor = initBwdPageFrameCursor(partitionFrameCursor, executionContext);
        try {
            bwdRecordCursor.of(frameCursor, executionContext);
            if (filter != null) {
                filter.init(bwdRecordCursor, executionContext);
            }
            return bwdRecordCursor;
        } catch (Throwable th) {
            frameCursor.close();
            throw th;
        }
    }

    /**
     * Opens this full scan over the timestamp range {@code [timestampLo, timestampHi]},
     * <b>inclusive of both edges</b>, in descending designated-timestamp order and
     * restricted to the rows carrying one value of an indexed SYMBOL column. It yields
     * exactly the subsequence of
     * {@link #getCursorInTimestampRangeBackward(SqlExecutionContext, long, long)} whose
     * {@code columnIndex} column equals {@code symbolKey}, and reads the other values'
     * rows not at all.
     * <p>
     * This is what makes per-key predecessor discovery cost follow the frame width
     * rather than the key's sparsity. The unrestricted descending walk has to pull every
     * key's rows to count one key's, so a key whose rows sit far apart drags the whole
     * history through the scan; here the index names that key's row positions inside each
     * partition and the walk stops as soon as the count is met.
     * <p>
     * {@code symbolKey} is the table-local key the column stores, so
     * {@link io.questdb.cairo.sql.SymbolTable#VALUE_IS_NULL} selects the rows whose value
     * is null - a partition key like any other.
     * <p>
     * The scan substitutes an index-backed row cursor of its own, so the factory must be
     * a plain full scan over an indexed SYMBOL column;
     * {@link #isIndexedBackwardTimestampRangeSupported(int)} reports that without opening
     * anything.
     */
    public RecordCursor getCursorInTimestampRangeBackwardIndexed(
            SqlExecutionContext executionContext,
            long timestampLo,
            long timestampHi,
            int columnIndex,
            int symbolKey
    ) throws SqlException {
        if (!(partitionFrameCursorFactory instanceof FullPartitionFrameCursorFactory fullFrameFactory)) {
            throw CairoException.nonCritical().put("timestamp range cursor requires a full partition scan");
        }
        if (!isIndexedBackwardTimestampRangeSupported(columnIndex)) {
            throw CairoException.nonCritical()
                    .put("indexed backward timestamp range cursor requires an indexed symbol column [columnIndex=")
                    .put(columnIndex)
                    .put(']');
        }
        if (bwdIndexedColumnIndex != columnIndex) {
            // One factory serves every repair of one view, and a view keys on one column,
            // so this rebuild is a first-call cost rather than a per-key one.
            bwdIndexedRecordCursor = Misc.free(bwdIndexedRecordCursor);
            bwdIndexedRowCursorFactory = Misc.free(bwdIndexedRowCursorFactory);
            bwdIndexedRowCursorFactory = new SymbolIndexRowCursorFactory(
                    columnIndex,
                    symbolKey,
                    IndexReader.DIR_BACKWARD,
                    null
            );
            bwdIndexedColumnIndex = columnIndex;
        } else {
            bwdIndexedRowCursorFactory.of(symbolKey);
        }
        if (bwdIndexedRecordCursor == null) {
            bwdIndexedRecordCursor = new PageFrameRecordCursorImpl(
                    configuration,
                    getMetadata(),
                    bwdIndexedRowCursorFactory,
                    false,
                    filter
            );
        }
        final PartitionFrameCursor partitionFrameCursor = fullFrameFactory.getCursorBackward(
                executionContext,
                columnIndexes,
                timestampLo,
                timestampHi
        );
        final PageFrameCursor frameCursor = initBwdPageFrameCursor(partitionFrameCursor, executionContext);
        try {
            bwdIndexedRecordCursor.of(frameCursor, executionContext);
            if (filter != null) {
                filter.init(bwdIndexedRecordCursor, executionContext);
            }
            return bwdIndexedRecordCursor;
        } catch (Throwable th) {
            frameCursor.close();
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        if (singleRowFactory) {
            // we only return single row, sometimes we use backward scan to do that
            // even if we do, we mark single row factory to return data in ascending timestamp order.

            // there is validation in as-of and lt-join generator code, which checks that both left and
            // right factories are in ascending order. Without this change single row symbol search will fail to
            // participate in those joins.

            // There is additional consistency issue, single-row flag is to address. The issue arose from
            // single-symbol filter search. Without this condition factory scan would be "backward", which is
            // inconsistent with same SQL filtering on two or more symbol values. Where scan order will be
            // "forward".
            return SCAN_DIRECTION_FORWARD;
        }
        return switch (partitionFrameCursorFactory.getOrder()) {
            case ORDER_ASC -> SCAN_DIRECTION_FORWARD;
            case ORDER_DESC -> SCAN_DIRECTION_BACKWARD;
            default ->
                    throw CairoException.critical(0).put("Unexpected factory order [order=").put(partitionFrameCursorFactory.getOrder()).put("]");
        };
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        if (framingSupported) {
            TablePageFrameCursor pageFrameCursor = initPageFrameCursor(executionContext);
            if (timeFrameCursor == null) {
                timeFrameCursor = new TimeFrameCursorImpl(configuration, getMetadata());
            }
            return timeFrameCursor.of(
                    pageFrameCursor,
                    executionContext.getPageFrameMinRows(),
                    executionContext.getPageFrameMaxRows(),
                    1, // used for single-threaded exec plans
                    executionContext.getMemoryTracker()
            );
        }
        return null;
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        if (framingSupported) {
            return new ConcurrentTimeFrameCursorImpl(configuration, getMetadata());
        }
        return null;
    }

    public boolean hasFilter() {
        return filter != null;
    }

    /**
     * Reports, without opening anything, whether
     * {@link #getCursorInTimestampRangeBackward(SqlExecutionContext, long, long)} would
     * be accepted. A planner that walks down to discover a bound asks first: an
     * index-backed factory is a legitimate compile, so being unable to descend through
     * it is a reason to plan a different bound rather than to fail the query.
     */
    public boolean isBackwardTimestampRangeSupported() {
        return partitionFrameCursorFactory instanceof FullPartitionFrameCursorFactory
                && rowCursorFactory.isEntity()
                && !rowCursorFactory.isUsingIndex();
    }

    /**
     * Reports, without opening anything, whether
     * {@link #getCursorInTimestampRangeBackwardIndexed(SqlExecutionContext, long, long, int, int)}
     * would be accepted for this column. It adds to
     * {@link #isBackwardTimestampRangeSupported()} the requirement that the column is an
     * indexed SYMBOL: the index is what names one value's row positions per partition, and
     * without it the caller has no cheaper option than walking every key's rows.
     */
    public boolean isIndexedBackwardTimestampRangeSupported(int columnIndex) {
        return isBackwardTimestampRangeSupported()
                && columnIndex > -1
                && columnIndex < getMetadata().getColumnCount()
                && ColumnType.isSymbol(getMetadata().getColumnType(columnIndex))
                && getMetadata().isColumnIndexed(columnIndex);
    }

    public boolean isIntervalScan() {
        return partitionFrameCursorFactory.isIntervalScan();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return supportsRandomAccess;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return framingSupported;
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        // Time frames are supported only for full table scan cursors, i.e. "x" queries.
        return framingSupported && supportsRandomAccess
                && rowCursorFactory.isEntity() && !rowCursorFactory.isUsingIndex()
                && getMetadata().getTimestampIndex() != -1
                && partitionFrameCursorFactory.getOrder() == ORDER_ASC
                && filter == null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("PageFrame");
        toPlanInner(sink);
    }

    @Override
    public void toSink(@NotNull CharSink<?> sink) {
        sink.putAscii("{\"name\":\"PageFrameRecordCursorFactory\", \"cursorFactory\":");
        partitionFrameCursorFactory.toSink(sink);
        sink.putAscii('}');
    }

    @Override
    public boolean usesIndex() {
        return rowCursorFactory.isUsingIndex();
    }

    @Override
    protected void _close() {
        final PageFrameRecordCursor bwdIndexedRecordCursor = this.bwdIndexedRecordCursor;
        this.bwdIndexedRecordCursor = null;
        final SymbolIndexRowCursorFactory bwdIndexedRowCursorFactory = this.bwdIndexedRowCursorFactory;
        this.bwdIndexedRowCursorFactory = null;
        this.bwdIndexedColumnIndex = -1;
        final TablePageFrameCursor bwdPageFrameCursor = this.bwdPageFrameCursor;
        this.bwdPageFrameCursor = null;
        final PageFrameRecordCursor bwdRecordCursor = this.bwdRecordCursor;
        this.bwdRecordCursor = null;
        final PageFrameRecordCursor cursor = this.cursor;
        this.cursor = null;
        final Function filter = this.filter;
        this.filter = null;
        final TablePageFrameCursor fwdPageFrameCursor = this.fwdPageFrameCursor;
        this.fwdPageFrameCursor = null;
        final RowCursorFactory rowCursorFactory = this.rowCursorFactory;
        this.rowCursorFactory = null;
        final TimeFrameCursorImpl timeFrameCursor = this.timeFrameCursor;
        this.timeFrameCursor = null;

        Throwable failure = null;
        try {
            super._close();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeBestEffort(failure, bwdIndexedRecordCursor);
        failure = Misc.freeBestEffort(failure, bwdIndexedRowCursorFactory);
        failure = Misc.freeBestEffort(failure, bwdRecordCursor);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = Misc.freeBestEffort(failure, filter);
        failure = Misc.freeBestEffort(failure, fwdPageFrameCursor);
        if (bwdPageFrameCursor != fwdPageFrameCursor) {
            failure = Misc.freeBestEffort(failure, bwdPageFrameCursor);
        }
        failure = Misc.freeBestEffort(failure, timeFrameCursor);
        failure = Misc.freeBestEffort(failure, rowCursorFactory);
        CairoException.rethrowCleanupFailure(failure);
    }

    protected PageFrameCursor initBwdPageFrameCursor(
            PartitionFrameCursor partitionFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (bwdPageFrameCursor == null) {
            bwdPageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    partitionFrameCursorFactory.getPushdownFilterConditions(),
                    executionContext.getSharedQueryWorkerCount()
            );
        }
        return bwdPageFrameCursor.of(executionContext, partitionFrameCursor);
    }

    protected PageFrameCursor initFwdPageFrameCursor(
            PartitionFrameCursor partitionFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (fwdPageFrameCursor == null) {
            fwdPageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    partitionFrameCursorFactory.getPushdownFilterConditions(),
                    executionContext.getSharedQueryWorkerCount()
            );
        }
        return fwdPageFrameCursor.of(executionContext, partitionFrameCursor);
    }

    @Override
    protected RecordCursor initRecordCursor(
            PageFrameCursor frameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        cursor.of(frameCursor, executionContext);
        if (filter != null) {
            filter.init(cursor, executionContext);
        }
        return cursor;
    }

    protected void toPlanInner(PlanSink sink) {
        sink.child(rowCursorFactory);
        sink.child(partitionFrameCursorFactory);
    }
}
