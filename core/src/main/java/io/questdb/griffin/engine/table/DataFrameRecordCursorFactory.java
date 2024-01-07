/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSinkBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.*;

public class DataFrameRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    protected final DataFrameRecordCursor cursor;
    protected final int pageFrameMaxRows;
    protected final int pageFrameMinRows;
    protected final RowCursorFactory rowCursorFactory;
    private final IntList columnIndexes;
    private final IntList columnSizes;
    private final Function filter;
    private final boolean followsOrderByAdvice;
    private final boolean framingSupported;
    private final boolean supportsRandomAccess;
    protected BwdTableReaderPageFrameCursor bwdPageFrameCursor;
    protected FwdTableReaderPageFrameCursor fwdPageFrameCursor;

    public DataFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            RecordMetadata metadata,
            DataFrameCursorFactory dataFrameCursorFactory,
            RowCursorFactory rowCursorFactory,
            boolean followsOrderByAdvice,
            // filter included here only for lifecycle management of the latter
            @Nullable Function filter,
            boolean framingSupported,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizes,
            boolean supportsRandomAccess
    ) {
        super(metadata, dataFrameCursorFactory);

        this.rowCursorFactory = rowCursorFactory;
        cursor = new DataFrameRecordCursorImpl(rowCursorFactory, rowCursorFactory.isEntity(), filter, columnIndexes);
        this.followsOrderByAdvice = followsOrderByAdvice;
        this.filter = filter;
        this.framingSupported = framingSupported;
        this.columnIndexes = columnIndexes;
        this.columnSizes = columnSizes;
        pageFrameMinRows = configuration.getSqlPageFrameMinRows();
        pageFrameMaxRows = configuration.getSqlPageFrameMaxRows();
        this.supportsRandomAccess = supportsRandomAccess;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followsOrderByAdvice;
    }

    @Override
    public String getBaseColumnName(int idx) {
        return dataFrameCursorFactory.getMetadata().getColumnName(columnIndexes.getQuick(idx));
    }

    @Override
    public String getBaseColumnNameNoRemap(int idx) {
        return dataFrameCursorFactory.getMetadata().getColumnName(idx);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        DataFrameCursor dataFrameCursor = dataFrameCursorFactory.getCursor(executionContext, order);
        if (framingSupported) {
            if (order == ORDER_ASC || order == ORDER_ANY) {
                return initFwdPageFrameCursor(executionContext, dataFrameCursor);
            }
            return initBwdPageFrameCursor(executionContext, dataFrameCursor);
        }
        return null;
    }

    @Override
    public int getScanDirection() {
        switch (dataFrameCursorFactory.getOrder()) {
            case ORDER_ASC:
                return SCAN_DIRECTION_FORWARD;
            case ORDER_DESC:
                return SCAN_DIRECTION_BACKWARD;
            default:
                throw CairoException.critical(0).put("Unexpected factory order [order=").put(dataFrameCursorFactory.getOrder()).put("]");
        }
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return supportsRandomAccess;
    }

    @Override
    public boolean supportPageFrameCursor() {
        return framingSupported;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("DataFrame");
        toPlanInner(sink);
    }

    @Override
    public void toSink(@NotNull CharSinkBase<?> sink) {
        sink.putAscii("{\"name\":\"DataFrameRecordCursorFactory\", \"cursorFactory\":");
        dataFrameCursorFactory.toSink(sink);
        sink.putAscii('}');
    }

    @Override
    public boolean usesIndex() {
        return rowCursorFactory.isUsingIndex();
    }

    @Override
    protected void _close() {
        super._close();
        Misc.free(filter);
    }

    @Override
    protected RecordCursor getCursorInstance(
            DataFrameCursor dataFrameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException {
        cursor.of(dataFrameCursor, executionContext);
        if (filter != null) {
            filter.init(cursor, executionContext);
        }
        return cursor;
    }

    protected PageFrameCursor initBwdPageFrameCursor(
            SqlExecutionContext executionContext,
            DataFrameCursor dataFrameCursor
    ) {
        if (bwdPageFrameCursor == null) {
            bwdPageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizes,
                    executionContext.getSharedWorkerCount(),
                    pageFrameMinRows,
                    pageFrameMaxRows
            );
        }
        return bwdPageFrameCursor.of(dataFrameCursor);
    }

    protected PageFrameCursor initFwdPageFrameCursor(
            SqlExecutionContext executionContext,
            DataFrameCursor dataFrameCursor
    ) {
        if (fwdPageFrameCursor == null) {
            fwdPageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizes,
                    executionContext.getSharedWorkerCount(),
                    pageFrameMinRows,
                    pageFrameMaxRows
            );
        }
        return fwdPageFrameCursor.of(dataFrameCursor);
    }

    protected void toPlanInner(PlanSink sink) {
        sink.child(rowCursorFactory);
        sink.child(dataFrameCursorFactory);
    }
}
