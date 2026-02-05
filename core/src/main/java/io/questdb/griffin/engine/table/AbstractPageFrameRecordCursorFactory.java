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
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ANY;
import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

/**
 * Abstract base class for page frame record cursor factories.
 */
abstract class AbstractPageFrameRecordCursorFactory extends AbstractRecordCursorFactory {
    /**
     * The column indexes.
     */
    protected final IntList columnIndexes;
    /**
     * The column size shifts.
     */
    protected final IntList columnSizeShifts;
    /**
     * The partition frame cursor factory.
     */
    protected final PartitionFrameCursorFactory partitionFrameCursorFactory;
    /**
     * The page frame cursor.
     */
    protected TablePageFrameCursor pageFrameCursor;

    protected @Nullable ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions;

    /**
     * Constructs a new page frame record cursor factory.
     *
     * @param configuration               the Cairo configuration
     * @param metadata                    the record metadata
     * @param partitionFrameCursorFactory the partition frame cursor factory
     * @param columnIndexes               the column indexes
     * @param columnSizeShifts            the column size shifts
     */
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

    public boolean mayHasParquetFormatPartition(SqlExecutionContext executionContext) {
        return partitionFrameCursorFactory.hasParquetFormatPartitions(executionContext);
    }

    @Override
    public void setPushdownFilterCondition(ObjList<PushdownFilterExtractor.PushdownFilterCondition> pushdownFilterConditions) {
        this.pushdownFilterConditions = pushdownFilterConditions;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return partitionFrameCursorFactory.supportsTableRowId(tableToken);
    }

    @Override
    protected void _close() {
        Misc.free(pageFrameCursor);
        Misc.free(partitionFrameCursorFactory);
        Misc.freeObjList(pushdownFilterConditions);
    }

    /**
     * Initializes the page frame cursor.
     *
     * @param executionContext the SQL execution context
     * @return the initialized page frame cursor
     * @throws SqlException if initialization fails
     */
    protected TablePageFrameCursor initPageFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        final int order = partitionFrameCursorFactory.getOrder();
        PartitionFrameCursor partitionFrameCursor = partitionFrameCursorFactory.getCursor(executionContext, columnIndexes, ORDER_ANY);
        if (pageFrameCursor == null) {
            if (order == ORDER_ASC || order == ORDER_ANY) {
                pageFrameCursor = new FwdTableReaderPageFrameCursor(
                        columnIndexes,
                        columnSizeShifts,
                        pushdownFilterConditions,
                        1 // used for single-threaded exec plans,
                );
            } else {
                pageFrameCursor = new BwdTableReaderPageFrameCursor(
                        columnIndexes,
                        columnSizeShifts,
                        pushdownFilterConditions,
                        1 // used for single-threaded exec plans
                );
            }
        }
        return pageFrameCursor.of(executionContext, partitionFrameCursor, executionContext.getPageFrameMinRows(), executionContext.getPageFrameMaxRows());
    }

    /**
     * Initializes the record cursor from the page frame cursor.
     *
     * @param frameCursor      the page frame cursor
     * @param executionContext the SQL execution context
     * @return the initialized record cursor
     * @throws SqlException if initialization fails
     */
    protected abstract RecordCursor initRecordCursor(
            PageFrameCursor frameCursor,
            SqlExecutionContext executionContext
    ) throws SqlException;
}
