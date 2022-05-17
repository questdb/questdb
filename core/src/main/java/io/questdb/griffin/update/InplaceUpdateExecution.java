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

package io.questdb.griffin.update;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.dFile;

public class InplaceUpdateExecution implements Closeable {
    private static final Log LOG = LogFactory.getLog(InplaceUpdateExecution.class);
    private final FilesFacade ff;
    private final int rootLen;
    private final IntList updateToColumnMap = new IntList();
    private final ObjList<MemoryCMARW> updateMemory = new ObjList<>();
    private final long dataAppendPageSize;
    private final long fileOpenOpts;
    private Path path;

    public InplaceUpdateExecution(CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot());
        rootLen = path.length();
        dataAppendPageSize = configuration.getDataAppendPageSize();
        this.fileOpenOpts = configuration.getWriterFileOpenOpts();
    }

    @Override
    public void close() {
        path = Misc.free(path);
    }

    // This is very hacky way to update in place for testing only
    // TODO: rewrite to production grade
    public void executeUpdate(TableWriter tableWriter, UpdateStatement updateStatement, SqlExecutionContext executionContext) throws SqlException {

        if (tableWriter.inTransaction()) {
            LOG.info().$("committing current transaction before UPDATE execution [table=").$(tableWriter.getTableName()).I$();
            tableWriter.commit();
        }

        TableWriterMetadata writerMetadata = tableWriter.getMetadata();

        // Check that table structure hasn't changed between planning and executing the UPDATE
        if (writerMetadata.getId() != updateStatement.getTableId() || tableWriter.getStructureVersion() != updateStatement.getTableVersion()) {
            throw ReaderOutOfDateException.of(tableWriter.getTableName());
        }

        RecordMetadata updateToMetadata = updateStatement.getUpdateToDataCursorFactory().getMetadata();
        int updateToColumnCount = updateToMetadata.getColumnCount();

        // Build index column map from table to update to values returned from the update statement row cursors
        updateToColumnMap.clear();
        for (int i = 0; i < updateToColumnCount; i++) {
            CharSequence columnName = updateToMetadata.getColumnName(i);
            int tableColumnIndex = writerMetadata.getColumnIndex(columnName);
            assert tableColumnIndex >= 0;
            updateToColumnMap.add(tableColumnIndex);
        }

        // Create update memory list of all columns to be updated
        initUpdateMemory(updateToColumnCount);

        // Start execution frame by frame
        RecordCursorFactory updateStatementDataCursorFactory = updateStatement.getUpdateToDataCursorFactory();

        // Track how many records updated
        long rowsUpdated = 0;

        // Start row by row updates
        try (RecordCursor recordCursor = updateStatementDataCursorFactory.getCursor(executionContext)) {

            Record masterRecord = recordCursor.getRecord();
            long currentPartitionIndex = -1L;

            long lastRowId = -1;
            while (recordCursor.hasNext()) {
                long rowId = masterRecord.getUpdateRowId();

                // Some joins expand results set and returns same row multiple times
                if (rowId == lastRowId) {
                    continue;
                }
                lastRowId = rowId;

                int partitionIndex = Rows.toPartitionIndex(rowId);
                long partitionRowId = Rows.toLocalRowID(rowId);
                if (partitionIndex != currentPartitionIndex) {
                    // Map columns to be updated for RW
                    openPartitionColumnsForUpdate(tableWriter, updateMemory, partitionIndex, updateToColumnMap);
                    currentPartitionIndex = partitionIndex;
                }

                // Update values in-place
                updateColumnValues(
                        writerMetadata,
                        updateToColumnMap,
                        updateToColumnCount,
                        updateMemory,
                        partitionRowId,
                        masterRecord);
                rowsUpdated++;
            }
        } finally {
            for (int i = 0; i < updateMemory.size(); i++) {
                updateMemory.getQuick(i).close(false);
            }
        }
        if (rowsUpdated > 0) {
            tableWriter.commit();
        }
    }

    private void updateColumnValues(
            RecordMetadata metadata,
            IntList updateToColumnMap,
            int columnCount,
            ObjList<MemoryCMARW> updateMemory,
            long rowId,
            Record record
    ) throws SqlException {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            int columnType = metadata.getColumnType(updateToColumnMap.get(columnIndex));
            MemoryCMARW primaryColMem = updateMemory.get(columnIndex);

            switch (ColumnType.tagOf(columnType)) {
                case ColumnType.INT:
                    primaryColMem.putInt(rowId << 2, record.getInt(columnIndex));
                    break;
                case ColumnType.FLOAT:
                    primaryColMem.putFloat(rowId << 2, record.getFloat(columnIndex));
                    break;
                case ColumnType.LONG:
                    primaryColMem.putLong(rowId << 3, record.getLong(columnIndex));
                    break;
                case ColumnType.TIMESTAMP:
                    primaryColMem.putLong(rowId << 3, record.getTimestamp(columnIndex));
                    break;
                case ColumnType.DATE:
                    primaryColMem.putLong(rowId << 3, record.getDate(columnIndex));
                    break;
                case ColumnType.DOUBLE:
                    primaryColMem.putDouble(rowId << 3, record.getDouble(columnIndex));
                    break;
                case ColumnType.SHORT:
                    primaryColMem.putShort(rowId << 1, record.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    primaryColMem.putChar(rowId << 1, record.getChar(columnIndex));
                    break;
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                    primaryColMem.putByte(rowId, record.getByte(columnIndex));
                    break;
                case ColumnType.GEOBYTE:
                    primaryColMem.putByte(rowId, record.getGeoByte(columnIndex));
                    break;
                case ColumnType.GEOSHORT:
                    primaryColMem.putShort(rowId << 1, record.getGeoShort(columnIndex));
                    break;
                case ColumnType.GEOINT:
                    primaryColMem.putInt(rowId << 2, record.getGeoInt(columnIndex));
                    break;
                case ColumnType.GEOLONG:
                    primaryColMem.putLong(rowId << 3, record.getGeoLong(columnIndex));
                    break;
                default:
                    throw SqlException.$(0, "Column type ")
                            .put(ColumnType.nameOf(columnType))
                            .put(" not supported for updates");
            }
        }
    }

    private void initUpdateMemory(int columnCount) {
        for(int i = updateMemory.size(); i < columnCount; i++) {
            updateMemory.add(Vm.getCMARWInstance());
        }
    }

    private void openPartitionColumnsForUpdate(TableWriter tableWriter, ObjList<MemoryCMARW> updateMemory, int partitionIndex, IntList columnMap) {
        long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        RecordMetadata metadata = tableWriter.getMetadata();
        try {
            path.concat(tableWriter.getTableName());
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
            int pathTrimToLen = path.length();

            for (int i = 0, n = columnMap.size(); i < n; i++) {
                int columnIndex = columnMap.get(i);
                CharSequence name = metadata.getColumnName(columnMap.get(i));
                MemoryCMARW colMem = updateMemory.get(i);
                colMem.close(false);
                long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                colMem.of(
                        ff,
                        dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                        dataAppendPageSize,
                        -1,
                        MemoryTag.MMAP_TABLE_WRITER,
                        fileOpenOpts
                );
            }
        } finally {
            path.trimTo(rootLen);
        }
    }
}
