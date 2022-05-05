/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.sql.*;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ANY;
import static io.questdb.cairo.sql.DataFrameCursorFactory.ORDER_ASC;

public class DataFrameRecordCursorFactory extends AbstractDataFrameRecordCursorFactory {
    protected final int pageFrameMinRows;
    protected final int pageFrameMaxRows;
    private final DataFrameRecordCursor cursor;
    private final boolean followsOrderByAdvice;
    private final Function filter;
    private final boolean framingSupported;
    private final IntList columnIndexes;
    private final IntList columnSizes;
    protected FwdTableReaderPageFrameCursor fwdPageFrameCursor;
    protected BwdTableReaderPageFrameCursor bwdPageFrameCursor;
    private final boolean supportsRandomAccess;

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

        this.cursor = new DataFrameRecordCursor(rowCursorFactory, rowCursorFactory.isEntity(), filter, columnIndexes);
        this.followsOrderByAdvice = followsOrderByAdvice;
        this.filter = filter;
        this.framingSupported = framingSupported;
        this.columnIndexes = columnIndexes;
        this.columnSizes = columnSizes;
        this.pageFrameMinRows = configuration.getSqlPageFrameMinRows();
        this.pageFrameMaxRows = configuration.getSqlPageFrameMaxRows();
        this.supportsRandomAccess = supportsRandomAccess;
    }

    @Override
    public void close() {
        Misc.free(filter);
        Misc.free(dataFrameCursorFactory);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return followsOrderByAdvice;
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
    public boolean recordCursorSupportsRandomAccess() {
        return supportsRandomAccess;
    }

    @Override
    public boolean supportPageFrameCursor() {
        return framingSupported;
    }

    @Override
    public boolean supportsUpdateRowId(CharSequence tableName) {
        return dataFrameCursorFactory.supportTableRowId(tableName);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put("{\"name\":\"DataFrameRecordCursorFactory\", \"cursorFactory\":");
        dataFrameCursorFactory.toSink(sink);
        sink.put('}');
    }

    public boolean hasDescendingOrder() {
        return dataFrameCursorFactory.getOrder() == DataFrameCursorFactory.ORDER_DESC;
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

    protected PageFrameCursor initFwdPageFrameCursor(
            SqlExecutionContext executionContext,
            DataFrameCursor dataFrameCursor
    ) {
        if (fwdPageFrameCursor == null) {
            fwdPageFrameCursor = new FwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizes,
                    executionContext.getWorkerCount(),
                    pageFrameMinRows,
                    pageFrameMaxRows
            );
        }
        return fwdPageFrameCursor.of(dataFrameCursor);
    }

    protected PageFrameCursor initBwdPageFrameCursor(
            SqlExecutionContext executionContext,
            DataFrameCursor dataFrameCursor
    ) {
        if (bwdPageFrameCursor == null) {
            bwdPageFrameCursor = new BwdTableReaderPageFrameCursor(
                    columnIndexes,
                    columnSizes,
                    executionContext.getWorkerCount(),
                    pageFrameMinRows,
                    pageFrameMaxRows
            );
        }
        return bwdPageFrameCursor.of(dataFrameCursor);
    }
}
