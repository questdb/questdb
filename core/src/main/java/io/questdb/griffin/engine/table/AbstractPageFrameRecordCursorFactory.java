/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

abstract class AbstractPageFrameRecordCursorFactory extends AbstractRecordCursorFactory {
    protected final IntList columnIndexes;
    protected final IntList columnSizeShifts;
    protected final int pageFrameMaxRows;
    protected final int pageFrameMinRows;
    protected final PartitionFrameCursorFactory partitionFrameCursorFactory;
    protected TablePageFrameCursor pageFrameCursor;

    public AbstractPageFrameRecordCursorFactory(
            @NotNull CairoConfiguration configuration,
            @NotNull RecordMetadata metadata,
            @NotNull PartitionFrameCursorFactory partitionFrameCursorFactory,
            @NotNull IntList columnIndexes,
            @NotNull IntList columnSizeShifts
    ) {
        super(metadata);
        this.partitionFrameCursorFactory = partitionFrameCursorFactory;
        this.columnIndexes = columnIndexes;
        this.columnSizeShifts = columnSizeShifts;
        pageFrameMinRows = configuration.getSqlPageFrameMinRows();
        pageFrameMaxRows = configuration.getSqlPageFrameMaxRows();
    }

    @Override
    public String getBaseColumnName(int columnIndex) {
        return partitionFrameCursorFactory.getMetadata().getColumnName(columnIndexes.getQuick(columnIndex));
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        PageFrameCursor frameCursor = initPageFrameCursor(executionContext);
        try {
            return initRecordCursor(frameCursor, executionContext);
        } catch (Throwable e) {
            frameCursor.close();
            throw e;
        }
    }

    @Override
    public TableToken getTableToken() {
        return partitionFrameCursorFactory.getTableToken();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return partitionFrameCursorFactory.supportsTableRowId(tableToken);
    }

    @Override
    protected void _close() {
        Misc.free(pageFrameCursor);
        Misc.free(partitionFrameCursorFactory);
    }

    protected TablePageFrameCursor initPageFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = partitionFrameCursorFactory.getOrder();
        PartitionFrameCursor partitionFrameCursor = partitionFrameCursorFactory.getCursor(executionContext, ORDER_ANY);
        if (pageFrameCursor == null) {
            if (order == ORDER_ASC || order == ORDER_ANY) {
                pageFrameCursor = new FwdTableReaderPageFrameCursor(
                        columnIndexes,
                        columnSizeShifts,
                        1, // used for single-threaded exec plans
                        pageFrameMinRows,
                        pageFrameMaxRows
                );
            } else {
                pageFrameCursor = new BwdTableReaderPageFrameCursor(
                        columnIndexes,
                        columnSizeShifts,
                        1, // used for single-threaded exec plans
                        pageFrameMinRows,
                        pageFrameMaxRows
                );
            }
        }
        return pageFrameCursor.of(partitionFrameCursor);
    }

    protected abstract RecordCursor initRecordCursor(
            PageFrameCursor frameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException;
}
