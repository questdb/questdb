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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
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
    private final PageFrameRecordCursor cursor;
    private final Function filter;
    private final boolean followsOrderByAdvice;
    private final boolean framingSupported;
    private final RowCursorFactory rowCursorFactory;
    private final boolean singleRowFactory;
    private final boolean supportsRandomAccess;
    protected FwdTableReaderPageFrameCursor fwdPageFrameCursor;
    private BwdTableReaderPageFrameCursor bwdPageFrameCursor;
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
        super(configuration, metadata, partitionFrameCursorFactory, columnIndexes, columnSizeShifts);

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
            return timeFrameCursor.of(pageFrameCursor);
        }
        return null;
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        if (framingSupported) {
            return new ConcurrentTimeFrameCursor(configuration, getMetadata());
        }
        return null;
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
        super._close();
        Misc.free(cursor);
        Misc.free(filter);
        Misc.free(fwdPageFrameCursor);
        Misc.free(bwdPageFrameCursor);
        Misc.free(timeFrameCursor);
    }

    protected PageFrameCursor initBwdPageFrameCursor(
            PartitionFrameCursor partitionFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (bwdPageFrameCursor == null) {
            bwdPageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    pushdownFilterConditions,
                    executionContext.getSharedQueryWorkerCount()
            );
        }
        return bwdPageFrameCursor.of(executionContext, partitionFrameCursor, executionContext.getPageFrameMinRows(), executionContext.getPageFrameMaxRows());
    }

    protected PageFrameCursor initFwdPageFrameCursor(
            PartitionFrameCursor partitionFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        if (fwdPageFrameCursor == null) {
            fwdPageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizeShifts,
                    pushdownFilterConditions,
                    executionContext.getSharedQueryWorkerCount()
            );
        }
        return fwdPageFrameCursor.of(executionContext, partitionFrameCursor, executionContext.getPageFrameMinRows(), executionContext.getPageFrameMaxRows());
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
