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
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
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

public class UpdateExecution implements Closeable {
    private static final Log LOG = LogFactory.getLog(UpdateExecution.class);
    private final FilesFacade ff;
    private final int rootLen;
    private final IntList updateColumnIndexes = new IntList();
    private final ObjList<MemoryCMARW> baseMemory = new ObjList<>();
    private final ObjList<MemoryCMARW> updateMemory = new ObjList<>();
    private final long dataAppendPageSize;
    private final long fileOpenOpts;
    private Path path;

    public UpdateExecution(CairoConfiguration configuration) {
        ff = configuration.getFilesFacade();
        path = new Path().of(configuration.getRoot());
        rootLen = path.length();
        dataAppendPageSize = configuration.getDataAppendPageSize();
        fileOpenOpts = configuration.getWriterFileOpenOpts();
    }

    @Override
    public void close() {
        path = Misc.free(path);
    }

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

        RecordMetadata updateMetadata = updateStatement.getUpdateToDataCursorFactory().getMetadata();
        int updateColumnCount = updateMetadata.getColumnCount();

        // Build index column map from table to update to values returned from the update statement row cursors
        updateColumnIndexes.clear();
        for (int i = 0; i < updateColumnCount; i++) {
            CharSequence columnName = updateMetadata.getColumnName(i);
            int tableColumnIndex = writerMetadata.getColumnIndex(columnName);
            assert tableColumnIndex >= 0;
            updateColumnIndexes.add(tableColumnIndex);
        }

        // Create update memory list of all columns to be updated
        initUpdateMemory(updateColumnCount);

        // Start execution frame by frame
        RecordCursorFactory updateStatementDataCursorFactory = updateStatement.getUpdateToDataCursorFactory();

        // Partition to update
        long currentPartitionIndex = -1L;

        // Row by row updates
        // This should happen parallel per file (partition and column)
        try (RecordCursor recordCursor = updateStatementDataCursorFactory.getCursor(executionContext)) {
            Record masterRecord = recordCursor.getRecord();

            long startPartitionRowId = 0;
            long lastRowId = -1;
            while (recordCursor.hasNext()) {
                long rowId = masterRecord.getUpdateRowId();

                // Some joins expand results set and returns same row multiple times
                if (rowId == lastRowId) {
                    continue;
                }
                lastRowId = rowId;

                int partitionIndex = Rows.toPartitionIndex(rowId);
                if (partitionIndex != currentPartitionIndex) {
                    updateColumnValues(
                            writerMetadata,
                            updateColumnIndexes,
                            updateColumnCount,
                            baseMemory,
                            updateMemory,
                            startPartitionRowId);

                    openPartitionColumnsForRead(tableWriter, baseMemory, partitionIndex, updateColumnIndexes);
                    openPartitionColumnsForWrite(tableWriter, updateMemory, partitionIndex, updateColumnIndexes);
                    currentPartitionIndex = partitionIndex;
                    startPartitionRowId = 0;
                }

                long partitionRowId = Rows.toLocalRowID(rowId);
                updateColumnValues(
                        writerMetadata,
                        updateColumnIndexes,
                        updateColumnCount,
                        baseMemory,
                        updateMemory,
                        startPartitionRowId,
                        partitionRowId,
                        masterRecord);

                startPartitionRowId = ++partitionRowId;
            }

            if (currentPartitionIndex > -1) {
                updateColumnValues(
                        writerMetadata,
                        updateColumnIndexes,
                        updateColumnCount,
                        baseMemory,
                        updateMemory,
                        startPartitionRowId);
            }
        } finally {
            for (int i = 0; i < updateMemory.size(); i++) {
                baseMemory.getQuick(i).close(false);
                updateMemory.getQuick(i).close(true);
            }
        }
        if (currentPartitionIndex > -1) {
            tableWriter.commit();
            tableWriter.openLastPartition();
        }
    }

    private void updateColumnValues(
            RecordMetadata metadata,
            IntList updateColumnIndexes,
            int columnCount,
            ObjList<MemoryCMARW> baseMemory,
            ObjList<MemoryCMARW> updateMemory,
            long startPartitionRowId
    ) throws SqlException {
        updateColumnValues(metadata, updateColumnIndexes, columnCount, baseMemory, updateMemory, startPartitionRowId, -1, null);
    }

    private void updateColumnValues(
            RecordMetadata metadata,
            IntList updateColumnIndexes,
            int columnCount,
            ObjList<MemoryCMARW> baseMemory,
            ObjList<MemoryCMARW> updateMemory,
            long startPartitionRowId,
            long partitionRowId,
            Record masterRecord
    ) throws SqlException {
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            MemoryCMARW baseColumn = baseMemory.get(columnIndex);
            MemoryCMARW updatedColumn = updateMemory.get(columnIndex);

            int columnType = metadata.getColumnType(updateColumnIndexes.get(columnIndex));
            int typeSize = ColumnType.sizeOf(columnType);
            if (typeSize > 0) {
                long addr = baseColumn.addressOf(startPartitionRowId * typeSize);
                if (masterRecord != null) {
                    updatedColumn.putBlockOfBytes(addr, (partitionRowId - startPartitionRowId) * typeSize);
                    switch (ColumnType.tagOf(columnType)) {
                        case ColumnType.INT:
                            updatedColumn.putInt(masterRecord.getInt(columnIndex));
                            break;
                        case ColumnType.FLOAT:
                            updatedColumn.putFloat(masterRecord.getFloat(columnIndex));
                            break;
                        case ColumnType.LONG:
                            updatedColumn.putLong(masterRecord.getLong(columnIndex));
                            break;
                        case ColumnType.TIMESTAMP:
                            updatedColumn.putLong(masterRecord.getTimestamp(columnIndex));
                            break;
                        case ColumnType.DATE:
                            updatedColumn.putLong(masterRecord.getDate(columnIndex));
                            break;
                        case ColumnType.DOUBLE:
                            updatedColumn.putDouble(masterRecord.getDouble(columnIndex));
                            break;
                        case ColumnType.SHORT:
                            updatedColumn.putShort(masterRecord.getShort(columnIndex));
                            break;
                        case ColumnType.CHAR:
                            updatedColumn.putChar(masterRecord.getChar(columnIndex));
                            break;
                        case ColumnType.BYTE:
                        case ColumnType.BOOLEAN:
                            updatedColumn.putByte(masterRecord.getByte(columnIndex));
                            break;
                        case ColumnType.GEOBYTE:
                            updatedColumn.putByte(masterRecord.getGeoByte(columnIndex));
                            break;
                        case ColumnType.GEOSHORT:
                            updatedColumn.putShort(masterRecord.getGeoShort(columnIndex));
                            break;
                        case ColumnType.GEOINT:
                            updatedColumn.putInt(masterRecord.getGeoInt(columnIndex));
                            break;
                        case ColumnType.GEOLONG:
                            updatedColumn.putLong(masterRecord.getGeoLong(columnIndex));
                            break;
                        default:
                            throw SqlException.$(0, "Column type ")
                                    .put(ColumnType.nameOf(columnType))
                                    .put(" not supported for updates");
                    }
                } else {
                    updatedColumn.putBlockOfBytes(addr, baseColumn.size() - (startPartitionRowId * typeSize));
                }
            } else {
                // add support for variable length types
                throw SqlException.$(0, "Column type ")
                        .put(ColumnType.nameOf(columnType))
                        .put(" is not fixed length, UPDATE is not supported");
            }
        }
    }

    private void initUpdateMemory(int columnCount) {
        for(int i = updateMemory.size(); i < columnCount; i++) {
            baseMemory.add(Vm.getCMARWInstance());
            updateMemory.add(Vm.getCMARWInstance());
        }
    }

    private void openPartitionColumnsForWrite(TableWriter tableWriter, ObjList<MemoryCMARW> updateMemory, int partitionIndex, IntList updateColumnIndexes) {
        openPartitionColumns(tableWriter, updateMemory, partitionIndex, updateColumnIndexes, true);
    }

    private void openPartitionColumnsForRead(TableWriter tableWriter, ObjList<MemoryCMARW> baseMemory, int partitionIndex, IntList updateColumnIndexes) {
        openPartitionColumns(tableWriter, baseMemory, partitionIndex, updateColumnIndexes, false);
    }

    private void openPartitionColumns(TableWriter tableWriter, ObjList<MemoryCMARW> memList, int partitionIndex, IntList updateColumnIndexes, boolean forWrite) {
        long partitionTimestamp = tableWriter.getPartitionTimestamp(partitionIndex);
        RecordMetadata metadata = tableWriter.getMetadata();
        try {
            path.concat(tableWriter.getTableName());
            TableUtils.setPathForPartition(path, tableWriter.getPartitionBy(), partitionTimestamp, false);
            int pathTrimToLen = path.length();

            for (int i = 0, n = updateColumnIndexes.size(); i < n; i++) {
                int columnIndex = updateColumnIndexes.get(i);
                CharSequence name = metadata.getColumnName(columnIndex);
                MemoryCMARW colMem = memList.get(i);
                colMem.close(forWrite);
                if (forWrite) {
                    tableWriter.upsertColumnVersion(partitionTimestamp, columnIndex);
                }
                long columnNameTxn = tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
                colMem.of(
                        ff,
                        dFile(path.trimTo(pathTrimToLen), name, columnNameTxn),
                        dataAppendPageSize,
                        -1,
                        MemoryTag.MMAP_UPDATE,
                        fileOpenOpts
                );
            }
        } finally {
            path.trimTo(rootLen);
        }
    }
}
