/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.security.AllowAllCairoSecurityContext;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;

public class TableReplicationRecordCursorFactory extends AbstractRecordCursorFactory {
    private final TablePageFrameCursor cursor;
    private final CairoEngine engine;
    private final CharSequence tableName;
    private final long maxRowsPerFrame;
    private final IntList columnIndexes;
    private final IntList columnSizes;

    private static RecordMetadata createMetadata(CairoEngine engine, CharSequence tableName) {
        try (TableReader reader = engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName, -1)) {
            return GenericRecordMetadata.copyOf(reader.getMetadata());
        }
    }

    public TableReplicationRecordCursorFactory(CairoEngine engine, CharSequence tableName, long maxRowsPerFrame) {
        super(createMetadata(engine, tableName));
        this.maxRowsPerFrame = maxRowsPerFrame;
        this.cursor = new TablePageFrameCursor();
        this.engine = engine;
        this.tableName = tableName;

        int nCols = getMetadata().getColumnCount();
        columnIndexes = new IntList(nCols);
        columnSizes = new IntList(nCols);
        for (int columnIndex = 0; columnIndex < nCols; columnIndex++) {
            int type = getMetadata().getColumnType(columnIndex);
            int typeSize = ColumnType.sizeOf(type);
            columnIndexes.add(columnIndex);
            columnSizes.add((Numbers.msb(typeSize)));
        }
    }

    @Override
    public TablePageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext) {
        return cursor.of(engine.getReader(executionContext.getCairoSecurityContext(), tableName), maxRowsPerFrame, -1, columnIndexes, columnSizes);
    }

    public TablePageFrameCursor getPageFrameCursorFrom(SqlExecutionContext executionContext, int timestampColumnIndex, long nFirstRow) {
        TableReader reader = engine.getReader(executionContext.getCairoSecurityContext(), tableName);
        int partitionIndex = 0;
        int partitionCount = reader.getPartitionCount();
        while (partitionIndex < partitionCount) {
            long partitionRowCount = reader.openPartition(partitionIndex);
            if (nFirstRow < partitionRowCount) {
                break;
            }
            partitionIndex++;
            nFirstRow -= partitionRowCount;
        }
        return cursor.of(reader, maxRowsPerFrame, timestampColumnIndex, columnIndexes, columnSizes, partitionIndex, nFirstRow);
    }

    public TablePageFrameCursor getPageFrameCursor(int timestampColumnIndex, int partitionIndex, long partitionRowCount) {
        return cursor.of(engine.getReader(AllowAllCairoSecurityContext.INSTANCE, tableName), maxRowsPerFrame, timestampColumnIndex, columnIndexes, columnSizes, partitionIndex,
                partitionRowCount);
    }

    @Override
    public void close() {
        Misc.free(cursor);
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }
}
